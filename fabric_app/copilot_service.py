"""
Fabric equivalent of the *orchestration* half of
backend/app/services/ai_service.py (the Snowflake version).

`ai_service.py` in this fabric_app package already provides the low-level
Azure OpenAI plumbing (generate_sql, cortex_complete, schema-prompt loading).
That's the Cortex Analyst / Cortex Complete replacement. What it does NOT yet
have is the higher-level "Copilot" behavior the frontend actually calls:
chat, quick analyses, saved insights, question history, and the Shortage
Agent actions. This file adds that layer, calling into `ai_service.py` for
the AI parts and `service.py` for the data parts, and returns dicts shaped
to match `frontend/src/api/aiApi.ts` exactly (CopilotChatResponse,
QuickAnalysisResult, SavedInsight, FrequentQuestion, ShortageQueueItem, etc.)

TABLES - GENIE_QUESTION_HISTORY, SAVED_INSIGHTS:
  These live in the Lakehouse (information_mart), not the Warehouse, and
  Fabric's SQL analytics endpoint cannot write to Lakehouse tables at all
  (confirmed: DML statements are not supported for this table type). So:
    - READS  go through service.run_query() against the Lakehouse, same as
      every other table in service.py.
    - WRITES go through lakehouse_writer.py, which writes the underlying
      Delta files directly on OneLake via the `deltalake` package,
      authenticated with the same Service Principal used for SQL. See that
      file's module docstring for the full rationale and caveats.

  shortage_alert_dt / work_order_dt follow the same WRITE pattern for their
  mutating actions (shortage_agent_mark_resolved, prioritize_work_order):
  a real Delta UPDATE via lakehouse_writer.py rather than a SQL-endpoint
  write, since the SQL endpoint restriction above applies to them too.
  lakehouse_writer.py also still provides an AGENT_REVIEW_LOG audit-trail
  writer (log_agent_review) for general use, it's just no longer part of
  the Shortage Agent's mark-resolved flow specifically.
"""

from __future__ import annotations

import copy
import hashlib
import logging
import re
import threading
import time
from typing import Optional

from config import Config
import ai_service
import lakehouse_writer
import service

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# copilot_chat result cache - mirrors Snowflake's app/services/ai_service.py
# in-process TTL cache exactly (_COPILOT_CHAT_CACHE / _cache_get / _cache_set),
# which this file never had. Keyed on message + short_memory + long_memory,
# same as Snowflake, so a repeated question (or a UI re-render re-asking the
# same thing) returns instantly instead of re-invoking Azure OpenAI twice for
# every question (once for SQL, once for the narrative) each time.
# ---------------------------------------------------------------------------

_COPILOT_CHAT_CACHE_TTL_SECONDS = 300  # 5 minutes, same as Snowflake
_COPILOT_CHAT_CACHE: dict[str, tuple[float, dict]] = {}
_COPILOT_CHAT_CACHE_LOCK = threading.Lock()
_COPILOT_CHAT_CACHE_VERSION = "v1"


def _cache_get(key: str) -> dict | None:
    now = time.time()
    with _COPILOT_CHAT_CACHE_LOCK:
        ent = _COPILOT_CHAT_CACHE.get(key)
        if not ent:
            return None
        ts, value = ent
        if now - ts > _COPILOT_CHAT_CACHE_TTL_SECONDS:
            _COPILOT_CHAT_CACHE.pop(key, None)
            return None
        return copy.deepcopy(value)


def _cache_set(key: str, value: dict) -> None:
    with _COPILOT_CHAT_CACHE_LOCK:
        _COPILOT_CHAT_CACHE[key] = (time.time(), copy.deepcopy(value))


HISTORY_TABLE_NAME = Config.HISTORY_TABLE_NAME
SAVED_INSIGHTS_TABLE_NAME = Config.SAVED_INSIGHTS_TABLE_NAME
# SQL-endpoint schema for these tables. Defaults to the same schema
# every other Lakehouse table in service.py reads through (information_mart,
# per the original OneLake paths); override with FABRIC_LAKEHOUSE_TABLE_SCHEMA
# if these specifically turn out to sit under a different schema (the
# tested Spark notebook reference was schema-less, e.g. "ReadyToBuild.
# SAVED_INSIGHTS" - see lakehouse_writer.py caveat #2).
_INSIGHTS_SQL_SCHEMA = Config.FABRIC_LAKEHOUSE_TABLE_SCHEMA or service.SCHEMA


def _sql_escape(value) -> str:
    if value is None:
        return ""
    return str(value).replace("'", "''")


# ---------------------------------------------------------------------------
# Quick analyses (fixed, hardcoded SQL - same pattern as Snowflake's
# `copilot_quick_analyses` / `run_quick_analysis`, adapted to this schema)
# ---------------------------------------------------------------------------

_QUICK_ANALYSES = {
    "ctb_readiness": {
        "title": "CTB Readiness",
        "desc": "Ready vs partial vs blocked work orders right now.",
        "sql": f"""
            SELECT ctb.CTB_STATUS AS "status", COUNT(*) AS "count"
            FROM {service.DB}.{service.SCHEMA}.ctb_result_dt ctb
            JOIN {service.DB}.{service.SCHEMA}.work_order_dt wo
              ON ctb.WORK_ORDER_ID = wo.WORK_ORDER_ID
            WHERE wo.WORK_ORDER_STATUS NOT IN ('COMPLETED','CANCELLED','CLOSED')
            GROUP BY ctb.CTB_STATUS
            ORDER BY COUNT(*) DESC
        """,
    },
    "part_shortages": {
        "title": "Top Part Shortages",
        "desc": "Parts with the largest shortage quantity across open work orders.",
        "sql": f"""
            SELECT TOP (10)
                   p.DEMAND_PART_NUMBER AS "part_number",
                   SUM(p.SHORTAGE_QTY) AS "shortage_qty",
                   COUNT(DISTINCT p.WORK_ORDER_ID) AS "affected_orders"
            FROM {service.DB}.{service.SCHEMA}.pegging_dt p
            JOIN {service.DB}.{service.SCHEMA}.work_order_dt wo
              ON p.WORK_ORDER_ID = wo.WORK_ORDER_ID
            WHERE p.SHORTAGE_QTY > 0
              AND wo.WORK_ORDER_STATUS NOT IN ('COMPLETED','CANCELLED','CLOSED')
            GROUP BY p.DEMAND_PART_NUMBER
            ORDER BY SUM(p.SHORTAGE_QTY) DESC
        """,
    },
    "supplier_performance": {
        "title": "Supplier On-Time Performance",
        "desc": "Suppliers ranked by worst on-time delivery percentage.",
        "sql": f"""
            SELECT TOP (20)
                   SUPPLIER_ID,
                   COUNT(*) AS "total_orders",
                   SUM(CASE WHEN IS_LATE = 1 THEN 1 ELSE 0 END) AS "late_orders",
                   ROUND(100.0 * SUM(CASE WHEN IS_LATE = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) AS "on_time_pct"
            FROM {service.DB}.{service.SCHEMA}.procurement_summary_dt
            GROUP BY SUPPLIER_ID
            ORDER BY "on_time_pct" ASC
        """,
    },
    "bom_completeness": {
        "title": "BOM Completeness",
        "desc": "Active BOM line counts per parent product.",
        "sql": f"""
            SELECT TOP (20)
                   PARENT_PART_NUMBER AS "product",
                   COUNT(*) AS "bom_line_count"
            FROM {service.DB}.{service.SCHEMA}.bom_dt
            WHERE BOM_STATUS = 'ACTIVE'
            GROUP BY PARENT_PART_NUMBER
            ORDER BY COUNT(*) DESC
        """,
    },
}


def copilot_quick_analyses() -> list[dict]:
    return [
        {"key": key, "title": v["title"], "desc": v["desc"], "question": v["desc"]}
        for key, v in _QUICK_ANALYSES.items()
    ]


def run_quick_analysis(key: str, user: str | None = None) -> dict:
    definition = _QUICK_ANALYSES.get(key)
    if not definition:
        return {"key": key, "metrics": {}, "rows": [], "sql": "", "descriptive": "",
                "prescriptive": "", "error": f"Unknown analysis key: {key}"}
    if user:
        save_question_history(definition["desc"], user, qtype=key)
    try:
        rows = service.run_query(definition["sql"])
        descriptive = f"{len(rows)} rows returned for {definition['title']}."
        prescriptive = ai_service.cortex_complete(
            f"Give a 2-3 sentence prescriptive recommendation for a manufacturing "
            f"planner based on this {definition['title']} data:\n{rows[:20]}"
        )
        return {
            "key": key,
            "metrics": {"row_count": len(rows)},
            "rows": rows,
            "sql": definition["sql"].strip(),
            "descriptive": descriptive,
            "prescriptive": prescriptive,
        }
    except Exception as exc:
        logger.exception("run_quick_analysis failed for key=%s", key)
        return {"key": key, "metrics": {}, "rows": [], "sql": definition["sql"].strip(),
                "descriptive": "", "prescriptive": "", "error": str(exc)}


# ---------------------------------------------------------------------------
# Free-form chat (Azure OpenAI text-to-SQL, replacing Cortex Analyst + Cortex
# Complete)
# ---------------------------------------------------------------------------

def _infer_chart(rows: list[dict]) -> dict | None:
    """Ported verbatim (logic-for-logic) from Snowflake's
    app/services/ai_service.py::copilot_chat - pure Python, no SQL-dialect
    dependency, so there was no reason this differed on Fabric; it just
    never existed here, so chat answers never got the automatic bar-chart
    the frontend (Copilot.tsx's chart rendering) already knows how to draw
    for QuickAnalysisResult.chart. Picks the first non-numeric column as
    the x-axis and the first numeric column as the y-axis."""
    if not rows:
        return None
    keys = list(rows[0].keys())

    def _is_number(v: object) -> bool:
        try:
            if v is None:
                return False
            float(v)
            return True
        except Exception:
            return False

    x_key = None
    y_key = None
    for k in keys:
        vals = [r.get(k) for r in rows[:20]]
        if any(v is not None for v in vals) and not all(_is_number(v) for v in vals if v is not None):
            x_key = k
            break
    for k in keys:
        vals = [r.get(k) for r in rows[:20]]
        if any(_is_number(v) for v in vals):
            y_key = k
            break

    if x_key and y_key:
        horizontal = len(rows) > 8
        return {
            "type": "bar_horizontal" if horizontal else "bar",
            "xKey": x_key,
            "yKey": y_key,
            "color": "#3B82F6",
            "title": "Visualization",
        }
    return None


# Fallback SQL for common vague questions the LLM sometimes can't turn into
# useful SQL on its own - ported from Snowflake's app/services/ai_service.py
# copilot_chat fallback blocks (wo_blocking_parts / safety_stock), translated
# to this schema's table names (shortage_alert_dt/work_order_dt/inventory_dt/
# procurement_summary_dt, per fabric_app/service.py's existing conventions).
#
# TRIGGER DIFFERENCE FROM SNOWFLAKE, is worth calling out explicitly:
# Snowflake only uses these when Cortex Analyst returns literally no SQL at
# all - a clean signal Cortex Analyst's structured response gives for free.
# Azure OpenAI here almost always returns SOME SQL (even a placeholder
# "SELECT 'please ask...'" one - see ai_service.generate_sql), so there is
# no equivalent "no SQL produced" signal to gate on. Instead, these are
# checked against the raw question BEFORE calling generate_sql at all, the
# same regex patterns Snowflake uses to decide whether a question matches
# one of these fallback topics. This is a deliberate, reasoned deviation
# from Snowflake's trigger mechanism, not an accidental one - it gives the
# same practical guarantee (a reliable, deterministic answer for these
# specific common questions) without depending on a signal Fabric's LLM
# call doesn't provide.
_WO_BLOCKING_RE = re.compile(r"\bwo0*\d+\b", re.IGNORECASE)


def _try_fallback_sql(message: str) -> tuple[str | None, str, list[dict]]:
    """Returns (fallback_kind, sql_used, rows) - fallback_kind is None if no
    fallback pattern matched (caller should fall through to generate_sql)."""
    msg_l = message.lower()

    if ("work order" in msg_l or _WO_BLOCKING_RE.search(msg_l)) and (
        "block" in msg_l or "shortage" in msg_l
    ):
        wo_match = re.search(r"\b(wo0*\d+)\b", msg_l, re.IGNORECASE)
        if wo_match:
            wo_id = wo_match.group(1).upper()
            sql = f"""
                WITH po AS (
                  SELECT
                    PART_NUMBER,
                    SUM(COALESCE(QTY_OUTSTANDING, 0)) AS incoming_qty,
                    MIN(COALESCE(CONFIRMED_DELIVERY_DATE, REQUESTED_DELIVERY_DATE)) AS next_supply_date
                  FROM {service.DB}.{service.SCHEMA}.procurement_summary_dt
                  WHERE PO_STATUS NOT IN ('COMPLETED', 'CANCELLED')
                    AND COALESCE(QTY_OUTSTANDING, 0) > 0
                  GROUP BY PART_NUMBER
                )
                SELECT
                  sa.WORK_ORDER_ID AS "work_order_id",
                  sa.PART_NUMBER AS "part_number",
                  COALESCE(sa.PART_DESCRIPTION, '') AS "part_description",
                  COALESCE(sa.SHORTAGE_QTY, 0) AS "shortage_qty",
                  COALESCE(inv.QTY_AVAILABLE, 0) AS "qty_available",
                  COALESCE(po.incoming_qty, 0) AS "incoming_qty",
                  po.next_supply_date AS "next_supply_date",
                  CASE
                    WHEN COALESCE(po.incoming_qty, 0) > 0 THEN 'OPEN_PO'
                    ELSE 'NO_OPEN_SUPPLY'
                  END AS "supply_status"
                FROM {service.DB}.{service.SCHEMA}.shortage_alert_dt sa
                LEFT JOIN {service.DB}.{service.SCHEMA}.inventory_dt inv
                  ON inv.PART_NUMBER = sa.PART_NUMBER
                LEFT JOIN po
                  ON po.PART_NUMBER = sa.PART_NUMBER
                WHERE sa.WORK_ORDER_ID = ?
                  AND COALESCE(sa.SHORTAGE_QTY, 0) > 0
                  AND sa.ALERT_STATUS = 'OPEN'
                ORDER BY "shortage_qty" DESC, sa.PART_NUMBER
            """
            try:
                rows = service.run_query(sql, [wo_id])
            except Exception:
                logger.warning("WO blocking-parts fallback execution failed", exc_info=True)
                rows = []
            return "wo_blocking_parts", sql, rows

    if "safety stock" in msg_l:
        sql = f"""
            WITH inv AS (
              SELECT
                PART_NUMBER AS "part_number",
                SUM(COALESCE(QTY_AVAILABLE, 0)) AS "qty_available",
                SUM(COALESCE(SAFETY_STOCK, 0)) AS "safety_stock"
              FROM {service.DB}.{service.SCHEMA}.inventory_dt
              GROUP BY PART_NUMBER
            ),
            incoming AS (
              SELECT
                PART_NUMBER AS "part_number",
                SUM(COALESCE(QTY_OUTSTANDING, 0)) AS "incoming_qty"
              FROM {service.DB}.{service.SCHEMA}.procurement_summary_dt
              WHERE PO_STATUS NOT IN ('COMPLETED', 'CANCELLED')
                AND COALESCE(QTY_OUTSTANDING, 0) > 0
              GROUP BY PART_NUMBER
            )
            SELECT TOP (50)
              inv."part_number",
              inv."qty_available",
              inv."safety_stock",
              (inv."qty_available" - inv."safety_stock") AS "qty_vs_safety_stock",
              CASE
                WHEN inv."safety_stock" > 0 THEN ROUND(inv."qty_available" * 1.0 / inv."safety_stock", 2)
                ELSE NULL
              END AS "safety_stock_coverage_ratio",
              COALESCE(incoming."incoming_qty", 0) AS "incoming_qty"
            FROM inv
            LEFT JOIN incoming ON inv."part_number" = incoming."part_number"
            WHERE inv."safety_stock" > 0
            ORDER BY "qty_vs_safety_stock" ASC
        """
        try:
            rows = service.run_query(sql)
        except Exception:
            logger.warning("Safety-stock fallback execution failed", exc_info=True)
            rows = []
        return "safety_stock", sql, rows

    return None, "", []


def _fallback_descriptive(fallback_kind: str, wo_id: str | None, rows: list[dict]) -> str:
    """Ported from Snowflake's hand-written fallback descriptive text -
    deterministic, data-driven, not LLM-generated (same as Snowflake)."""
    if fallback_kind == "wo_blocking_parts":
        if not rows:
            return (
                f"**Work order check:** No open shortage rows were found for {wo_id} in "
                f"`{service.DB}.{service.SCHEMA}.shortage_alert_dt` at this moment."
            )
        return f"**{len(rows)} part(s)** are currently blocking work order **{wo_id}** due to open shortages."

    if fallback_kind == "safety_stock":
        below = [r for r in rows if (r.get("qty_vs_safety_stock") or 0) < 0]
        below_cnt = len(below)
        min_cov, worst_part = None, None
        for r in rows:
            cov = r.get("safety_stock_coverage_ratio")
            if cov is None:
                continue
            try:
                cov_f = float(cov)
            except (TypeError, ValueError):
                continue
            if min_cov is None or cov_f < min_cov:
                min_cov, worst_part = cov_f, r.get("part_number")
        worst_str = f"{worst_part} (coverage {min_cov:.2f}x)" if worst_part and min_cov is not None else "n/a"
        top_below = ", ".join(
            f"{r.get('part_number')} ({r.get('qty_vs_safety_stock')})" for r in below[:5]
        ) or "n/a"
        return (
            f"**Safety stock coverage (current state):** {below_cnt} of the top {len(rows)} parts are "
            f"**below safety stock**. Worst coverage is {worst_str}. "
            f"Examples of parts below safety stock (qty vs safety stock): {top_below}."
        )
    return ""


def _generate_narrative(message: str, rows: list[dict], context_note: str) -> tuple[str, str]:
    """One Azure OpenAI call that produces BOTH a numbers-citing descriptive
    summary and a 4-5 bullet prescriptive recommendation in one shot, in the
    same strict, parseable format Snowflake's Cortex Analyst instruction +
    separate prescriptive_prompt together produce - see
    app/services/ai_service.py::call_cortex_analyst's `instruction` string
    and copilot_chat's `prescriptive_prompt`. Fabric doesn't have a
    Cortex-Analyst-equivalent that returns formatted prose alongside SQL in
    the same call, so this does it as one dedicated completion instead of
    Fabric's previous "N rows returned." template + a generic 2-4 sentence
    ask - matching Snowflake's OUTPUT QUALITY even though the underlying
    call architecture differs.

    Returns (descriptive, prescriptive); on any failure, returns a safe
    templated fallback rather than raising, since a chat answer with only
    weak narrative text is still far more useful than a hard error.
    """
    data_summary = str(rows[:20])
    if len(data_summary) > 6000:
        data_summary = data_summary[:6000] + "... (truncated)"

    prompt = f"""You are a manufacturing supply chain analyst. Answer the question using ONLY the data below.

Question asked: {message}{context_note}

Data returned ({len(rows)} rows):
{data_summary}

Respond in EXACTLY this format (including the literal markers DESCRIPTIVE: and PRESCRIPTIVE:):

DESCRIPTIVE:
[1-3 sentences citing SPECIFIC numbers, names, and IDs from the data above. If the question is a yes/no question, start with a clear Yes or No.]

PRESCRIPTIVE:
Here are the bullet points with specific findings, concrete actions, and explanations:

- **[Topic 1]**: [Specific finding citing actual values from the data above]. **Action:** [Concrete action to take]. **Why it matters:** [Business impact].

- **[Topic 2]**: [Specific finding citing actual values from the data above]. **Action:** [Concrete action to take]. **Why it matters:** [Business impact].

- **[Topic 3]**: [Specific finding citing actual values from the data above]. **Action:** [Concrete action to take]. **Why it matters:** [Business impact].

- **[Topic 4]**: [Specific finding citing actual values from the data above]. **Action:** [Concrete action to take]. **Why it matters:** [Business impact].

IMPORTANT: Use ACTUAL part numbers, supplier IDs, quantities, and percentages from the data. Do NOT use generic placeholders."""

    try:
        full_text = ai_service.cortex_complete(prompt)
    except Exception:
        logger.warning("Narrative generation failed", exc_info=True)
        return f"{len(rows)} rows returned.", ""

    descriptive, prescriptive = full_text, ""
    marker = "PRESCRIPTIVE:"
    if marker in full_text:
        desc_part, _, pres_part = full_text.partition(marker)
        descriptive = desc_part.replace("DESCRIPTIVE:", "").strip()
        prescriptive = pres_part.strip()
    else:
        descriptive = full_text.replace("DESCRIPTIVE:", "").strip()

    if not descriptive:
        descriptive = f"{len(rows)} rows returned."
    return descriptive, prescriptive


def copilot_chat(
    message: str,
    short_memory: Optional[str] = None,
    long_memory: Optional[str] = None,
    user: Optional[str] = None,
) -> dict:
    """
    Mirrors Snowflake's app/services/ai_service.py::copilot_chat as closely
    as the two platforms' underlying AI services allow:
      - short_memory/long_memory are threaded into SQL generation itself
        (ai_service.generate_sql), not just the narrative step, so follow-up
        questions can use prior conversation context the same way Cortex
        Analyst does.
      - A real, working in-process TTL cache (this module's _cache_get/
        _cache_set) short-circuits repeated identical questions, mirroring
        Snowflake's _COPILOT_CHAT_CACHE exactly (previously this file had no
        cache at all, while the "cache" it appeared to have - db.cache_get,
        called inside generate_sql - could never hit, since its write side
        is never called anywhere in this codebase).
      - Deterministic fallback SQL for common vague questions (blocking
        work orders, safety stock), ported from Snowflake's fallback blocks.
      - descriptive/prescriptive are both real generated analysis (see
        _generate_narrative), not a hardcoded "N rows returned." template.
      - A chart config is inferred from the result shape (see _infer_chart),
        so chat answers can render a bar chart the same way Snowflake's do.

    NOTE: `ai_service.generate_sql` still has no semantic-model-enforced
    guardrail beyond the system prompt itself, unlike Cortex Analyst's
    structural binding to its semantic model - this is a real, documented
    difference between the platforms, not something fixable purely in this
    file. The SELECT/WITH guard below is the same basic safety net both
    platforms use on top of that.
    """
    base_key = message.strip().lower()
    short_sig = hashlib.md5(((short_memory or "")[:200]).encode("utf-8")).hexdigest()[:10]
    long_sig = hashlib.md5(((long_memory or "")[:200]).encode("utf-8")).hexdigest()[:10]
    cache_key = f"{base_key}|s:{short_sig}|l:{long_sig}|{_COPILOT_CHAT_CACHE_VERSION}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    if user:
        save_question_history(message, user, qtype="custom")

    context_note = ""
    if short_memory:
        context_note += f"\nRecent conversation: {short_memory}"
    if long_memory:
        context_note += f"\nKnown user context: {long_memory}"

    fallback_kind, sql_used, rows = _try_fallback_sql(message)
    if fallback_kind is None:
        try:
            sql_used = ai_service.generate_sql(message, short_memory=short_memory, long_memory=long_memory)
        except Exception as exc:
            logger.exception("SQL generation failed")
            return {"response": f"I couldn't generate a query for that: {exc}"}

        sql_upper = sql_used.lstrip(";").strip().upper()
        if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
            return {"response": sql_used}

        try:
            rows = service.run_query(sql_used)
        except Exception as exc:
            logger.exception("Generated SQL failed to execute: %s", sql_used)
            return {"response": f"That query didn't run successfully: {exc}"}

    if not rows and fallback_kind != "wo_blocking_parts":
        # wo_blocking_parts still wants a descriptive "no blockers found"
        # answer even with 0 rows (matches Snowflake's behavior); every
        # other path treats 0 rows as "nothing to say".
        return {"response": "No matching data was found for that question."}

    wo_match = re.search(r"\b(wo0*\d+)\b", message, re.IGNORECASE)
    wo_id = wo_match.group(1).upper() if wo_match else None

    if fallback_kind:
        descriptive = _fallback_descriptive(fallback_kind, wo_id, rows)
        prescriptive = ""
        if rows or fallback_kind == "safety_stock":
            _, prescriptive = _generate_narrative(message, rows, context_note)
    else:
        descriptive, prescriptive = _generate_narrative(message, rows, context_note)

    chart = _infer_chart(rows)

    result = {
        "key": "chat",
        "metrics": {"row_count": len(rows)},
        "rows": rows,
        "sql": sql_used,
        "descriptive": descriptive,
        "prescriptive": prescriptive,
        "chart": chart,
    }
    _cache_set(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Saved insights / question history
#
# NOTE: like the Snowflake version, `user` here should be the actual
# logged-in app user (threaded in from the JWT), NOT a shared service
# identity - see the "shared user" bug flagged for the Snowflake AI
# Copilot. Callers (routers/ai.py) must pass the real username through.
# ---------------------------------------------------------------------------

def save_question_history(question: str, user: str, qtype: str = "custom") -> None:
    """Upserts into GENIE_QUESTION_HISTORY via OneLake Delta merge (the SQL
    endpoint can't write Lakehouse tables - see lakehouse_writer.py)."""
    try:
        lakehouse_writer.upsert_question_history(
            normalized_query=question,
            qtype=qtype,
            persona="",
            user_name=user,
        )
    except Exception:
        # Was logger.debug(), which is invisible under Python's default
        # logging level (WARNING) - a failure here produced ZERO console
        # output, making "why isn't anything showing up" undiagnosable.
        # logger.exception() logs at ERROR, which always prints.
        logger.exception("save_question_history failed")


def load_saved_insights(user: str) -> list[dict]:
    # Reads via OneLake directly (lakehouse_writer.read_saved_insights), NOT
    # the SQL analytics endpoint - see that function's docstring: the
    # endpoint cannot see this table's columns at all, confirmed even
    # immediately after a successful write, so a SQL SELECT here would
    # never work regardless of how the column names are spelled/cased.
    try:
        return lakehouse_writer.read_saved_insights(created_by=user)
    except Exception:
        logger.exception("load_saved_insights failed")
        return []


def save_insight(title: str, question: str, user: str, sql_text: str = "") -> None:
    """Inserts into SAVED_INSIGHTS via OneLake Delta append (see
    lakehouse_writer.py)."""
    try:
        lakehouse_writer.insert_saved_insight(
            title=title,
            question=question,
            sql_text=sql_text,
            created_by=user,
            sql_run_query=service.run_query,
        )
    except Exception:
        logger.exception("save_insight failed")


def delete_insight(insight_id: int, user: str) -> None:
    try:
        lakehouse_writer.delete_saved_insight(insight_id, user)
    except Exception:
        logger.exception("delete_insight failed")


def load_frequent_questions(user: str) -> list[dict]:
    # Reads via OneLake directly - same reasoning as load_saved_insights above.
    try:
        return lakehouse_writer.read_question_history(user_name=user, top_n=10)
    except Exception:
        logger.exception("load_frequent_questions failed")
        return []


def load_most_frequent_all() -> list[dict]:
    # Reads via OneLake directly - same reasoning as load_saved_insights above.
    try:
        return lakehouse_writer.read_question_history(user_name=None, top_n=10)
    except Exception:
        logger.exception("load_most_frequent_all failed")
        return []


# ---------------------------------------------------------------------------
# Shortage Agent
# ---------------------------------------------------------------------------

def shortage_agent_queue() -> list[dict]:
    """Open shortages, ranked as ACT_NOW / PLAN_THIS_WEEK / MONITOR.

    Fabric/T-SQL STRING_AGG does not support the Snowflake-style DISTINCT
    argument, so distinct products/plants are pre-aggregated in CTEs.

    Same as Snowflake's shortage_agent_queue: relies entirely on
    `WHERE sa.ALERT_STATUS = 'OPEN'` below. A part marked resolved via
    shortage_agent_mark_resolved has its ALERT_STATUS actually updated on
    shortage_alert_dt (see lakehouse_writer.update_shortage_alert_status),
    so it stops matching this filter on its own - no separate post-filter
    needed here.
    """
    rows = service.run_query(f"""
        WITH base AS (
            SELECT
                sa.PART_NUMBER,
                sa.WORK_ORDER_ID,
                sa.PLANT_ID,
                sa.SHORTAGE_QTY,
                wo.PRODUCT_ID,
                wo.PRIORITY,
                wo.PLANNED_START_DATE
            FROM {service.DB}.{service.SCHEMA}.shortage_alert_dt sa
            JOIN {service.DB}.{service.SCHEMA}.work_order_dt wo
              ON sa.WORK_ORDER_ID = wo.WORK_ORDER_ID
            WHERE sa.ALERT_STATUS = 'OPEN'
        ),
        agg AS (
            SELECT
                PART_NUMBER,
                COUNT(DISTINCT WORK_ORDER_ID) AS affected_wo_count,
                SUM(SHORTAGE_QTY) AS shortage_qty,
                MIN(PRIORITY) AS highest_priority,
                MIN(PLANNED_START_DATE) AS shortage_date_raw
            FROM base
            GROUP BY PART_NUMBER
        ),
        products AS (
            SELECT
                PART_NUMBER,
                STRING_AGG(PRODUCT_ID, ', ') WITHIN GROUP (ORDER BY PRODUCT_ID) AS affected_products
            FROM (SELECT DISTINCT PART_NUMBER, PRODUCT_ID FROM base) d
            GROUP BY PART_NUMBER
        ),
        plants AS (
            SELECT
                PART_NUMBER,
                STRING_AGG(PLANT_ID, ', ') WITHIN GROUP (ORDER BY PLANT_ID) AS affected_plants
            FROM (SELECT DISTINCT PART_NUMBER, PLANT_ID FROM base) d
            GROUP BY PART_NUMBER
        )
        SELECT
            a.PART_NUMBER AS "part_number",
            'CURRENT' AS "shortage_type",
            a.affected_wo_count AS "affected_wo_count",
            a.shortage_qty AS "shortage_qty",
            a.highest_priority AS "highest_priority",
            p.affected_products AS "affected_products",
            pl.affected_plants AS "affected_plants",
            CONVERT(varchar(10), a.shortage_date_raw, 120) AS "shortage_date",
            DATEDIFF(day, GETDATE(), a.shortage_date_raw) AS "days_until_shortage",
            0.0 AS "unit_cost",
            a.shortage_qty * 1.0 AS "business_impact_score",
            CASE
                WHEN a.highest_priority = 0 THEN 'ACT_NOW'
                WHEN DATEDIFF(day, GETDATE(), a.shortage_date_raw) <= 7 THEN 'PLAN_THIS_WEEK'
                ELSE 'MONITOR'
            END AS "action_group"
        FROM agg a
        LEFT JOIN products p ON p.PART_NUMBER = a.PART_NUMBER
        LEFT JOIN plants pl ON pl.PART_NUMBER = a.PART_NUMBER
        ORDER BY a.highest_priority, a.shortage_qty DESC
    """)
    return rows


def shortage_agent_recommendation(payload: dict) -> dict:
    part_number = payload.get("part_number", "")
    rows = service.run_query(f"""
        SELECT TOP (5) SUPPLIER_ID, PART_NUMBER, UNIT_PRICE, LEAD_TIME_DAYS, MIN_ORDER_QTY
        FROM {service.DB}.{service.SCHEMA}.part_supplier_dt
        WHERE PART_NUMBER = '{_sql_escape(part_number)}'
        ORDER BY LEAD_TIME_DAYS ASC
    """)
    recommendation = ai_service.cortex_complete(
        f"A manufacturing planner needs to resolve a shortage of part "
        f"{part_number}. Available suppliers: {rows}. Give a short "
        f"(2-3 sentence) recommendation on which supplier to use and why."
    )
    return {"response": recommendation}


def shortage_agent_po_preview(payload: dict) -> list[dict]:
    """
    Mirrors Snowflake's app/services/ai_service.py::shortage_agent_po_preview:
    one allocation row per plant with an OPEN shortage for this part
    (falling back to payload["affected_plants"] if none exist), quantities
    scaled to sum to the requested shortage_qty, and a real, non-blank
    plant_id per row.

    Previously this hardcoded '' AS "plant_id" for every row, which is what
    made SP_CREATE_PO fail downstream with "Incorrect syntax near ''" for
    @PlantId - there was never a real plant_id anywhere in the flow for the
    frontend to pass back on create-PO.
    """
    part_number = str(payload.get("part_number", "") or "").strip()
    if not part_number:
        return []
    shortage_qty = int(float(payload.get("shortage_qty", 0) or 0))

    supplier_rows = service.run_query(f"""
        SELECT TOP (3)
            ps.SUPPLIER_ID AS "supplier_id",
            COALESCE(s.SUPPLIER_NAME, ps.SUPPLIER_ID) AS "supplier_name",
            COALESCE(ps.LEAD_TIME_DAYS, 14) AS "lead_time_days",
            COALESCE(ps.UNIT_PRICE, 0) AS "unit_price",
            COALESCE(ps.MIN_ORDER_QTY, 1) AS "min_order_qty"
        FROM {service.DB}.{service.SCHEMA}.part_supplier_dt ps
        LEFT JOIN {service.DB}.{service.SCHEMA}.supplier_summary_dt s
          ON ps.SUPPLIER_ID = s.SUPPLIER_ID
        WHERE ps.PART_NUMBER = '{_sql_escape(part_number)}'
        ORDER BY
            CASE WHEN ps.PRIORITY_RANK IS NULL THEN 1 ELSE 0 END, ps.PRIORITY_RANK,
            CASE WHEN ps.RELIABILITY_SCORE IS NULL THEN 1 ELSE 0 END, ps.RELIABILITY_SCORE DESC
    """)
    best_supplier = supplier_rows[0] if supplier_rows else {
        "supplier_id": "SUP001",
        "supplier_name": "Default Supplier",
        "lead_time_days": 14,
        "unit_price": 0,
        "min_order_qty": 1,
    }

    plant_short_rows = service.run_query(f"""
        SELECT sa.PLANT_ID AS "plant_id", SUM(COALESCE(sa.SHORTAGE_QTY, 0)) AS "short_qty"
        FROM {service.DB}.{service.SCHEMA}.shortage_alert_dt sa
        WHERE sa.PART_NUMBER = '{_sql_escape(part_number)}'
          AND sa.ALERT_STATUS = 'OPEN'
        GROUP BY sa.PLANT_ID
        ORDER BY SUM(COALESCE(sa.SHORTAGE_QTY, 0)) DESC
    """)

    # Fallback to affected_plants payload if no open current shortage row exists.
    if not plant_short_rows:
        affected_plants = str(payload.get("affected_plants", "") or "")
        plants = [p.strip() for p in affected_plants.split(",") if p.strip()]
        if not plants:
            plants = ["PL001"]
        split_qty = max(1, shortage_qty // max(1, len(plants)))
        plant_short_rows = [{"plant_id": p, "short_qty": split_qty} for p in plants]

    moq = int(float(best_supplier.get("min_order_qty") or 1))
    lead = int(float(best_supplier.get("lead_time_days") or 14))
    supplier_id = str(best_supplier.get("supplier_id") or "SUP001")
    supplier_name = str(best_supplier.get("supplier_name") or supplier_id)
    unit_price = float(best_supplier.get("unit_price") or 0)

    # Reconcile quantities to exactly match selected shortage qty.
    raw_alloc = []
    sum_short = 0
    for pr in plant_short_rows:
        q = max(0, int(float(pr.get("short_qty") or 0)))
        raw_alloc.append({"plant_id": str(pr.get("plant_id") or "PL001"), "short_qty": q})
        sum_short += q
    if sum_short <= 0:
        raw_alloc = [{"plant_id": "PL001", "short_qty": max(1, shortage_qty)}]
        sum_short = max(1, shortage_qty)

    target = max(1, shortage_qty)
    scaled = []
    running = 0
    for i, a in enumerate(raw_alloc):
        if i == len(raw_alloc) - 1:
            sq = target - running
        else:
            sq = max(0, int(round((a["short_qty"] / sum_short) * target)))
            running += sq
        scaled.append({"plant_id": a["plant_id"], "short_qty": sq})
    diff = target - sum(s["short_qty"] for s in scaled)
    if scaled and diff != 0:
        scaled[0]["short_qty"] += diff

    out: list[dict] = []
    for pr in scaled:
        rec_qty = max(1, int(pr["short_qty"]))
        out.append(
            {
                "supplier_id": supplier_id,
                "supplier_name": supplier_name,
                "lead_time_days": lead,
                "unit_price": unit_price,
                "min_order_qty": moq,
                "recommended_qty": rec_qty,
                "plant_id": pr["plant_id"],
                "delivery_date": f"+{lead} days",
            }
        )
    # Snowflake caps defensively too - one allocation card per impacted plant.
    return out[:20]


def shortage_agent_create_po(payload: dict) -> dict:
    """Creates a real PO via the Warehouse stored procedure. See the
    IMPORTANT note on service.create_po - SP_CREATE_PO must exist in Fabric.

    Mirrors the same server-side defaulting Snowflake's
    app/services/ai_service.py::shortage_agent_create_po has, which this
    Fabric path was missing: plant_id and supplier_id are resolved here
    if the caller didn't supply real values (po-preview's allocations
    normally do, but this keeps behavior correct if create-PO is ever
    called without going through preview first, and prevents SP_CREATE_PO
    from being called with @PlantId = '' / no supplier again)."""
    try:
        part_number = str(payload.get("part_number", "") or "").strip()
        if not part_number:
            return {"ok": False, "message": "part_number is required"}

        qty = int(float(payload.get("recommended_qty") or payload.get("shortage_qty") or 0))
        qty = max(qty, 1)

        plant_id = str(payload.get("plant_id", "") or "").strip()
        if not plant_id:
            affected_plants = str(payload.get("affected_plants", "") or "")
            plant_id = affected_plants.split(",")[0].strip() if affected_plants else "PL001"

        supplier_id = str(payload.get("supplier_id", "") or "").strip()
        if not supplier_id:
            sup_rows = service.run_query(f"""
                SELECT TOP (1) SUPPLIER_ID AS "supplier_id"
                FROM {service.DB}.{service.SCHEMA}.part_supplier_dt
                WHERE PART_NUMBER = '{_sql_escape(part_number)}'
                ORDER BY
                    CASE WHEN PRIORITY_RANK IS NULL THEN 1 ELSE 0 END, PRIORITY_RANK,
                    CASE WHEN RELIABILITY_SCORE IS NULL THEN 1 ELSE 0 END, RELIABILITY_SCORE DESC
            """)
            supplier_id = str(sup_rows[0].get("supplier_id") or "SUP001") if sup_rows else "SUP001"

        result = service.create_po(
            part_number=part_number,
            qty=qty,
            supplier_id=supplier_id,
            plant_id=plant_id,
            delivery_date=payload.get("delivery_date", ""),
        )
        po_id = result.get("PO_ID") or result.get("po_id")
        return {"ok": True, "message": "Purchase order created.", "po_id": po_id}
    except Exception as exc:
        logger.exception("shortage_agent_create_po failed")
        return {"ok": False, "message": str(exc)}


def shortage_agent_email_draft(payload: dict) -> dict:
    part_number = payload.get("part_number", "")
    supplier_name = payload.get("supplier_id", "the supplier")
    qty = payload.get("recommended_qty") or payload.get("shortage_qty") or 0
    body = ai_service.cortex_complete(
        f"Write a brief, professional email to a supplier ({supplier_name}) "
        f"requesting an expedited purchase order for {qty} units of part "
        f"{part_number} due to a production shortage. Keep it under 120 words."
    )
    return {
        "to": "",
        "subject": f"Urgent PO Request - Part {part_number}",
        "body": body,
    }


def shortage_agent_mark_resolved(payload: dict) -> dict:
    """
    Real equivalent of Snowflake's shortage_agent_mark_resolved
    (app/services/ai_service.py):

        UPDATE SHORTAGE_ALERT SET ALERT_STATUS = 'RESOLVED'
        WHERE PART_NUMBER = %s AND ALERT_STATUS = 'OPEN'

    Applied directly to shortage_alert_dt via a Delta UPDATE over OneLake
    (lakehouse_writer.update_shortage_alert_status) - the same mechanism
    already used for work_order_dt.PRIORITY. This actually mutates
    ALERT_STATUS on the Lakehouse table itself, so shortage_agent_queue's
    existing `WHERE sa.ALERT_STATUS = 'OPEN'` filter is all that's needed
    for a resolved part to stop appearing in the queue on the next read -
    no separate audit-log table or Python-side post-filter required.
    """
    part_number = str(payload.get("part_number", "") or "").strip()
    if not part_number:
        return {"ok": False, "message": "part_number is required"}
    try:
        result = lakehouse_writer.update_shortage_alert_status(
            part_number=part_number,
            new_status="RESOLVED",
            match_status="OPEN",
        )
        rows_updated = result.get("rows_updated")
        # deltalake's DeltaTable.update() never throws for a predicate that
        # matches nothing - it just returns num_updated_rows: 0 - so this
        # was previously silent: the caller got {"ok": True, "message":
        # "...marked as resolved"} even when literally nothing changed on
        # shortage_alert_dt (e.g. ALERT_STATUS on that row isn't the exact
        # string 'OPEN'). Surface that explicitly instead of pretending.
        if rows_updated == 0:
            logger.warning(
                "shortage_agent_mark_resolved: 0 rows matched PART_NUMBER=%r AND ALERT_STATUS='OPEN' "
                "on shortage_alert_dt - nothing was actually updated",
                part_number,
            )
            return {
                "ok": False,
                "message": (
                    f"No OPEN shortage_alert_dt row found for {part_number} - nothing was changed. "
                    "Check ALERT_STATUS's actual value for this part (it may not be the exact "
                    "string 'OPEN')."
                ),
            }
        return {
            "ok": True,
            "message": f"Shortage {part_number} marked as resolved."
            + (f" ({rows_updated} row(s) updated.)" if rows_updated is not None else ""),
        }
    except Exception as exc:
        logger.exception("shortage_agent_mark_resolved failed")
        return {"ok": False, "message": f"Failed to mark resolved: {exc}"}