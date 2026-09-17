from __future__ import annotations

import logging
import json
import re
import time
import threading
import copy
import hashlib

import requests
from app.db import run_query, run_query_on_conn, get_connection, DB, SCHEMA

log = logging.getLogger(__name__)

SEMANTIC_MODEL = f"@{DB}.{SCHEMA}.CORTEX_STAGE/clear_to_build_semantic_model.yaml"

_COPILOT_CHAT_CACHE_TTL_SECONDS = 300  # 5 minutes
_COPILOT_CHAT_CACHE: dict[str, tuple[float, dict]] = {}
_COPILOT_CHAT_CACHE_LOCK = threading.Lock()
_COPILOT_CHAT_CACHE_VERSION = "v2"


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
        # Avoid accidental mutation across requests.
        return copy.deepcopy(value)


def _cache_set(key: str, value: dict) -> None:
    with _COPILOT_CHAT_CACHE_LOCK:
        _COPILOT_CHAT_CACHE[key] = (time.time(), copy.deepcopy(value))


def call_cortex_analyst(question: str, conn=None) -> dict:
    """Call Cortex Analyst REST API using a session token from the Snowflake connector."""
    instruction = (
        "Do NOT start with 'This is our interpretation of your question.' "
        "Start directly with **Descriptive**: then **Prescriptive**:. "
        "For ANY YES/NO question: Start the Descriptive section with a clear **Yes** or **No** answer first, then explain with specific numbers. "
        "(1) **Descriptive**: What the data shows with specific numbers and evidence. "
        "(2) **Prescriptive**: Provide a header 'Here are the bullet points with specific findings, concrete actions, and explanations:' then list 4-5 SPECIFIC bullet points. "
        "Each bullet must follow this EXACT format: "
        "- **[Finding Title]**: [Specific finding with actual numbers from the data]. **Action:** [Concrete action to take]. **Why it matters:** [Business impact explanation]. "
        "NEVER use vague phrases like 'review the data below' without citing specific numbers. "
        "Use actual values, counts, percentages, and supplier/part names from the query results. "
        "Answer the following question:\n\n"
    )
    body = {
        "messages": [{"role": "user", "content": [{"type": "text", "text": instruction + question}]}],
        "semantic_model_file": SEMANTIC_MODEL,
    }

    if conn is None:
        with get_connection() as c:
            return call_cortex_analyst(question, conn=c)

    token = conn.rest.token
    # Use the host that the connector already resolved (avoids underscore/SSL issues)
    host = conn.host

    url = f"https://{host}/api/v2/cortex/analyst/message"
    headers = {
        "Authorization": f'Snowflake Token="{token}"',
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    try:
        resp = requests.post(url, json=body, headers=headers, timeout=120)
    except requests.exceptions.SSLError:
        # Retry with underscore→hyphen conversion for legacy account identifiers
        alt_host = conn.account.replace("_", "-")
        if not alt_host.endswith(".snowflakecomputing.com"):
            alt_host = f"{alt_host}.snowflakecomputing.com"
        url = f"https://{alt_host}/api/v2/cortex/analyst/message"
        resp = requests.post(url, json=body, headers=headers, timeout=120)

    if resp.status_code >= 400:
        return {"error": f"Cortex Analyst HTTP {resp.status_code}: {resp.text[:500]}"}

    return resp.json()


def get_current_user() -> str:
    try:
        rows = run_query("SELECT CURRENT_USER() AS USERNAME")
        if rows:
            return str(rows[0].get("USERNAME", "UNKNOWN"))
    except Exception:
        log.warning("Could not fetch current user", exc_info=True)
    return "UNKNOWN"


def save_question_history(question: str, qtype: str = "custom", user: str | None = None) -> None:
    # `user` should be the actual logged-in app user (threaded in from the
    # JWT by the router). Falls back to the shared Snowflake connection
    # identity only if the caller doesn't pass one, for backward compat.
    user = user or get_current_user()
    try:
        safe_q = question.replace("'", "''")
        run_query(f"""
            MERGE INTO {DB}.{SCHEMA}.GENIE_QUESTION_HISTORY t
            USING (SELECT '{safe_q}' AS NORMALIZED_QUERY, '{qtype}' AS TYPE, '{user}' AS "USER") s
            ON t.NORMALIZED_QUERY = s.NORMALIZED_QUERY AND t."USER" = s."USER"
            WHEN MATCHED THEN UPDATE SET FREQUENCY = t.FREQUENCY + 1, LAST_ASKED_AT = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN INSERT (NORMALIZED_QUERY, TYPE, "USER", FREQUENCY) VALUES (s.NORMALIZED_QUERY, s.TYPE, s."USER", 1)
        """)
    except Exception:
        log.warning("Could not save question history", exc_info=True)


def load_saved_insights(user: str | None = None) -> list[dict]:
    user = user or get_current_user()
    try:
        return run_query(f"""
            SELECT INSIGHT_ID, TITLE, QUESTION, SQL_TEXT
            FROM {DB}.{SCHEMA}.SAVED_INSIGHTS
            WHERE CREATED_BY = '{user}'
            ORDER BY CREATED_AT DESC
            LIMIT 20
        """)
    except Exception:
        log.warning("Could not load saved insights", exc_info=True)
    return []


def save_insight(title: str, question: str, sql_text: str = "", user: str | None = None) -> None:
    user = user or get_current_user()
    try:
        safe_title = title.replace("'", "''")
        safe_q = question.replace("'", "''")
        safe_sql = sql_text.replace("'", "''") if sql_text else ""
        run_query(f"""
            INSERT INTO {DB}.{SCHEMA}.SAVED_INSIGHTS (CREATED_BY, PAGE, TITLE, QUESTION, SQL_TEXT)
            VALUES ('{user}', 'genie', '{safe_title}', '{safe_q}', '{safe_sql}')
        """)
    except Exception:
        log.warning("Could not save insight", exc_info=True)


def delete_insight(insight_id: int, user: str | None = None) -> None:
    user = user or get_current_user()
    try:
        run_query(f"DELETE FROM {DB}.{SCHEMA}.SAVED_INSIGHTS WHERE INSIGHT_ID = {insight_id} AND CREATED_BY = '{user}'")
    except Exception:
        log.warning("Could not delete insight", exc_info=True)


def load_frequent_questions(user: str | None = None) -> list[dict]:
    user = user or get_current_user()
    try:
        return run_query(f"""
            SELECT NORMALIZED_QUERY, TYPE, FREQUENCY
            FROM {DB}.{SCHEMA}.GENIE_QUESTION_HISTORY
            WHERE "USER" = '{user}'
            ORDER BY FREQUENCY DESC, LAST_ASKED_AT DESC
            LIMIT 10
        """)
    except Exception:
        log.warning("Could not load frequent questions", exc_info=True)
    return []


def load_most_frequent_all() -> list[dict]:
    try:
        return run_query(f"""
            SELECT NORMALIZED_QUERY, TYPE, SUM(FREQUENCY) AS TOTAL_FREQ
            FROM {DB}.{SCHEMA}.GENIE_QUESTION_HISTORY
            GROUP BY NORMALIZED_QUERY, TYPE
            ORDER BY TOTAL_FREQ DESC
            LIMIT 10
        """)
    except Exception:
        log.warning("Could not load most frequent questions", exc_info=True)
    return []


def cortex_complete(model: str, prompt: str) -> str:
    rows = run_query(
        "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s) AS \"response\"",
        (model, prompt),
    )
    if not rows:
        return ""
    return str(rows[0].get("response", "") or "")


COPILOT_QUICK_ANALYSES: dict[str, dict[str, str]] = {
    "ctb_readiness": {
        "title": "Clear-to-Build Readiness",
        "desc": "Overall CTB status across work orders",
        "question": "What is the current clear-to-build readiness percentage?",
    },
    "part_shortages": {
        "title": "Part Shortages",
        "desc": "Critical parts blocking production and work orders",
        "question": "What parts have shortages blocking work orders?",
    },
    "supplier_performance": {
        "title": "Supplier Delivery",
        "desc": "On-time delivery performance and lead time risks",
        "question": "What is supplier on-time delivery performance?",
    },
    "bom_completeness": {
        "title": "BOM Completeness",
        "desc": "Work orders with missing/short components",
        "question": "Which work orders have incomplete BOMs and what components are missing?",
    },
}


def copilot_quick_analyses() -> list[dict]:
    return [{"key": k, **v} for k, v in COPILOT_QUICK_ANALYSES.items()]


def run_quick_analysis(key: str, user: str | None = None) -> dict:
    qa = COPILOT_QUICK_ANALYSES.get(key)
    if qa:
        save_question_history(qa["question"], key, user=user)

    result: dict = {"key": key, "metrics": {}, "rows": [], "sql": "", "descriptive": "", "prescriptive": ""}
    if key == "ctb_readiness":
        sql = f"""
        SELECT CTB_STATUS AS status, COUNT(*) AS count
        FROM {DB}.{SCHEMA}.CTB_RESULT_VW
        GROUP BY CTB_STATUS
        ORDER BY COUNT(*) DESC
        """
        rows = run_query(sql)
        total = sum(int(r.get("COUNT") or r.get("count") or 0) for r in rows)
        ready = partial = blocked = 0
        for r in rows:
            s = str(r.get("STATUS") or r.get("status") or "").upper()
            v = int(r.get("COUNT") or r.get("count") or 0)
            if s == "READY":
                ready = v
            elif s == "PARTIAL":
                partial = v
            elif s == "BLOCKED":
                blocked = v
        pct = (ready / total * 100.0) if total else 0.0
        result["metrics"] = {"summary": f"{pct:.1f}% orders ready to build", "overall_pct": pct, "total_wo": total, "ready_wo": ready, "partial_wo": partial, "blocked_wo": blocked}
        result["rows"] = rows
        result["sql"] = sql
        result["descriptive"] = (
            f"**Readiness Status:** {pct:.1f}% of orders ({ready:,} of {total:,}) are ready to build. "
            f"{blocked:,} orders have components with shortages or risk issues."
        )
        result["prescriptive"] = (
            f"Here are the bullet points with specific findings, concrete actions, and explanations:\n\n"
            f"- **Ready Orders Execution:** {ready:,} work orders ({pct:.1f}%) are clear-to-build across all production lines. "
            f"**Action:** Prioritize releasing these orders to production immediately to maximize output. "
            f"**Why it matters:** These orders have all materials available and can generate revenue without delays.\n\n"
            f"- **Partial Orders Optimization:** {partial:,} work orders are in partial status with some materials available. "
            f"**Action:** Review partial orders to identify if partial builds can proceed while waiting for remaining components. "
            f"**Why it matters:** Partial builds can reduce WIP and improve cash flow while maintaining production momentum.\n\n"
            f"- **Blocked Orders Resolution:** {blocked:,} work orders are blocked due to material shortages. "
            f"**Action:** Focus procurement efforts on the most common blocking parts identified in the shortage analysis. "
            f"**Why it matters:** Unblocking these orders can increase overall readiness percentage and reduce delivery delays.\n\n"
            f"- **Overall Readiness:** The overall CTB readiness percentage is {pct:.1f}%. "
            f"**Action:** Continue to monitor and analyze readiness data to identify trends and areas for improvement. "
            f"**Why it matters:** By regularly reviewing readiness data, we can identify opportunities to optimize production processes and improve overall efficiency."
        )
        result["chart"] = {"type": "bar", "xKey": "status", "yKey": "count", "color": "#22C55E", "title": "CTB Status Distribution"}
        return result

    if key == "part_shortages":
        sql = f"""
        SELECT PART_NUMBER AS part_number,
               COUNT(DISTINCT WORK_ORDER_ID) AS affected_orders,
               SUM(SHORTAGE_QTY) AS shortage_qty
        FROM {DB}.{SCHEMA}.SHORTAGE_ALERT_VW
        WHERE ALERT_STATUS = 'OPEN'
        GROUP BY PART_NUMBER
        ORDER BY shortage_qty DESC
        LIMIT 20
        """
        rows = run_query(sql)
        cnt = len(rows)
        wo = sum(int(r.get("affected_orders") or r.get("AFFECTED_ORDERS") or 0) for r in rows)
        total_short = sum(int(r.get("shortage_qty") or r.get("SHORTAGE_QTY") or 0) for r in rows)
        top_parts = [str(r.get("part_number") or r.get("PART_NUMBER") or "") for r in rows[:3]]
        top_str = ", ".join(top_parts) if top_parts else "see table below"
        result["metrics"] = {"summary": f"{cnt} parts with shortages affecting {wo} work orders", "shortage_count": cnt, "affected_wo": wo, "total_shortage": total_short}
        result["rows"] = rows
        result["sql"] = sql
        result["descriptive"] = (
            f"**Shortage Impact:** {cnt} parts have shortages totaling {total_short:,} units, "
            f"affecting {wo} work orders. These shortages are blocking production readiness."
        )
        result["prescriptive"] = (
            f"Here are the bullet points with specific findings, concrete actions, and explanations:\n\n"
            f"- **Critical Shortages:** The top shortage parts are {top_str}. "
            f"**Action:** Escalate procurement for these parts immediately and contact suppliers for expedited delivery options. "
            f"**Why it matters:** These parts are blocking the most work orders and resolving them will have the biggest impact on production readiness.\n\n"
            f"- **Affected Work Orders:** {wo} work orders are impacted by part shortages. "
            f"**Action:** Review affected work orders by priority and delivery date to determine which should be addressed first. "
            f"**Why it matters:** Prioritizing high-value or time-sensitive orders ensures customer commitments are met.\n\n"
            f"- **Alternative Sourcing:** {cnt} unique parts have shortages. "
            f"**Action:** Identify alternative suppliers or substitute parts for components with recurring shortages. "
            f"**Why it matters:** Reducing single-source dependency decreases future shortage risk and improves supply chain resilience.\n\n"
            f"- **Safety Stock Review:** Total shortage quantity is {total_short:,} units. "
            f"**Action:** Review and adjust safety stock levels for high-impact parts to prevent future shortages. "
            f"**Why it matters:** Proper safety stock levels buffer against supply variability and demand fluctuations."
        )
        result["chart"] = {"type": "bar_horizontal", "xKey": "part_number", "yKey": "shortage_qty", "color": "#EF4444", "title": "Top Parts with Shortages"}
        return result

    if key == "supplier_performance":
        sql = f"""
        SELECT SUPPLIER_ID,
               COUNT(*) AS total_orders,
               SUM(CASE WHEN IS_LATE = 1 THEN 1 ELSE 0 END) AS late_orders,
               ROUND(100.0 * SUM(CASE WHEN IS_LATE = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) AS on_time_pct
        FROM {DB}.{SCHEMA}.PROCUREMENT_SUMMARY_VW
        GROUP BY SUPPLIER_ID
        ORDER BY on_time_pct ASC
        LIMIT 20
        """
        rows = run_query(sql)
        total_suppliers = len(rows)
        total_late = sum(int(r.get("late_orders") or r.get("LATE_ORDERS") or 0) for r in rows)
        avg_otp = 0.0
        if rows:
            otps = [float(r.get("on_time_pct") or r.get("ON_TIME_PCT") or 0) for r in rows]
            avg_otp = sum(otps) / len(otps) if otps else 0.0
        poor_suppliers = [str(r.get("SUPPLIER_ID") or r.get("supplier_id") or "") for r in rows[:3]]
        poor_str = ", ".join(poor_suppliers) if poor_suppliers else "see table below"
        result["metrics"] = {"summary": f"{total_suppliers} suppliers, {avg_otp:.1f}% avg on-time", "supplier_count": total_suppliers, "avg_on_time_pct": avg_otp, "late_deliveries": total_late}
        result["rows"] = rows
        result["sql"] = sql
        result["descriptive"] = (
            f"**Supplier Reliability:** {avg_otp:.1f}% average on-time delivery rate. "
            f"There have been {total_late:,} late deliveries impacting production schedules."
        )
        result["prescriptive"] = (
            f"Here are the bullet points with specific findings, concrete actions, and explanations:\n\n"
            f"- **Underperforming Suppliers:** Suppliers {poor_str} have the most late deliveries. "
            f"**Action:** Schedule urgent review meetings with these suppliers to understand root causes and develop improvement plans. "
            f"**Why it matters:** Late deliveries from these suppliers are directly impacting production schedules and customer commitments.\n\n"
            f"- **On-Time Performance:** The average on-time delivery rate is {avg_otp:.1f}%. "
            f"**Action:** Set a target of 95% on-time delivery and implement supplier scorecards to track progress. "
            f"**Why it matters:** Higher on-time delivery rates reduce production disruptions and improve planning accuracy.\n\n"
            f"- **Late Delivery Impact:** {total_late:,} deliveries have been late. "
            f"**Action:** Implement early warning systems and require suppliers to provide ASN updates for in-transit shipments. "
            f"**Why it matters:** Early visibility into delivery delays allows for proactive mitigation and alternative sourcing.\n\n"
            f"- **Supplier Diversification:** Review single-source components from low-performing suppliers. "
            f"**Action:** Develop backup supplier relationships for critical components. "
            f"**Why it matters:** Reducing dependency on unreliable suppliers decreases supply chain risk and improves resilience."
        )
        result["chart"] = {"type": "bar_horizontal", "xKey": "SUPPLIER_ID", "yKey": "on_time_pct", "color": "#3B82F6", "title": "Supplier On-Time Delivery %"}
        return result

    if key == "bom_completeness":
        sql = f"""
        SELECT p.WORK_ORDER_ID,
               w.PRODUCT_ID,
               w.PRIORITY,
               COUNT(DISTINCT p.DEMAND_PART_NUMBER) AS missing_parts,
               SUM(p.SHORTAGE_QTY) AS total_shortage
        FROM {DB}.{SCHEMA}.PEGGING_VW p
        JOIN {DB}.{SCHEMA}.WORK_ORDER w ON p.WORK_ORDER_ID = w.WORK_ORDER_ID
        WHERE p.PEGGING_STATUS = 'SHORT'
        GROUP BY p.WORK_ORDER_ID, w.PRODUCT_ID, w.PRIORITY
        ORDER BY w.PRIORITY, total_shortage DESC
        LIMIT 20
        """
        rows = run_query(sql)
        wo_cnt = len(rows)
        total_missing = sum(int(r.get("missing_parts") or r.get("MISSING_PARTS") or 0) for r in rows)
        high_priority = sum(1 for r in rows if int(r.get("PRIORITY") or r.get("priority") or 99) <= 2)
        result["metrics"] = {"summary": f"{wo_cnt} work orders with {total_missing} missing component types", "affected_wo": wo_cnt, "missing_parts": total_missing}
        result["rows"] = rows
        result["sql"] = sql
        result["descriptive"] = (
            f"**BOM Status:** {wo_cnt} work orders have incomplete BOMs with {total_missing} missing component types total. "
            f"These gaps are preventing production release."
        )
        result["prescriptive"] = (
            f"Here are the bullet points with specific findings, concrete actions, and explanations:\n\n"
            f"- **High Priority Orders:** {high_priority} of the affected work orders are high priority (Priority 1-2). "
            f"**Action:** Focus resolution efforts on high-priority orders first to minimize impact on key customers. "
            f"**Why it matters:** High-priority orders typically have firm customer commitments and delays can result in penalties or lost business.\n\n"
            f"- **Missing Components:** {total_missing} unique component types are missing across affected work orders. "
            f"**Action:** Cross-reference missing parts with open purchase orders and ASNs to identify expected delivery dates. "
            f"**Why it matters:** Understanding when parts will arrive enables better production planning and customer communication.\n\n"
            f"- **Inventory Optimization:** {wo_cnt} work orders are blocked by material gaps. "
            f"**Action:** Review pegging and allocation rules to ensure available inventory is optimally distributed across work orders. "
            f"**Why it matters:** Better allocation can unblock some orders without requiring additional procurement.\n\n"
            f"- **Substitute Parts:** Review engineering change orders for approved substitutes. "
            f"**Action:** Work with engineering to identify and approve substitute parts for missing components where possible. "
            f"**Why it matters:** Using approved substitutes can unblock production without waiting for original parts."
        )
        result["chart"] = {"type": "bar_horizontal", "xKey": "WORK_ORDER_ID", "yKey": "missing_parts", "color": "#8B5CF6", "title": "Work Orders with Missing Parts"}
        return result

    return {"error": f"Unknown analysis key: {key}"}


def copilot_chat(message: str, short_memory: str | None = None, long_memory: str | None = None, user: str | None = None) -> dict:
    """Use Cortex Analyst (same as Streamlit) for custom questions."""
    save_question_history(message, "custom", user=user)

    base_key = message.strip().lower()
    short_sig = hashlib.md5(((short_memory or "")[:200]).encode("utf-8")).hexdigest()[:10]
    long_sig = hashlib.md5(((long_memory or "")[:200]).encode("utf-8")).hexdigest()[:10]
    cache_key = f"{base_key}|s:{short_sig}|l:{long_sig}|{_COPILOT_CHAT_CACHE_VERSION}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    with get_connection() as conn:
        try:
            mem_parts: list[str] = []
            if short_memory:
                mem_parts.append(f"Short memory (recent conversation):\n{short_memory}")
            if long_memory:
                mem_parts.append(f"Long memory (important context):\n{long_memory}")
            analyst_input = message
            if mem_parts:
                analyst_input = f"{message}\n\n" + "\n\n".join(mem_parts)

            analyst_resp = call_cortex_analyst(analyst_input, conn=conn)
        except Exception as exc:
            log.error("Cortex Analyst call failed completely", exc_info=True)
            analyst_resp = {"error": str(exc)}

        if "error" in analyst_resp:
            return {
                "key": "custom",
                "metrics": {"summary": "Analysis failed"},
                "rows": [],
                "sql": "",
                "descriptive": f"Could not analyze: {analyst_resp['error']}",
                "prescriptive": "",
            }

        # Parse Cortex Analyst response (same structure as Streamlit handles)
        full_text = ""
        sql_stmts: list[str] = []
        content = []
        if "message" in analyst_resp and "content" in analyst_resp["message"]:
            content = analyst_resp["message"]["content"]
        for block in content:
            if block.get("type") == "text":
                full_text += block.get("text", "") + "\n"
            elif block.get("type") == "sql":
                sql_stmts.append(block.get("statement", ""))

        # If Analyst returns no SQL (common when the question is too vague),
        # fall back to a targeted query for common CTB topics.
        msg_l = message.lower()
        fallback_used = False
        fallback_kind: str | None = None
        fallback_rows: list[dict] | None = None
        fallback_sql_used = ""
        if not sql_stmts:
            if ("work order" in msg_l or re.search(r"\bwo0*\d+\b", msg_l)) and ("block" in msg_l or "shortage" in msg_l):
                wo_match = re.search(r"\b(wo0*\d+)\b", msg_l, re.IGNORECASE)
                if wo_match:
                    fallback_used = True
                    fallback_kind = "wo_blocking_parts"
                    wo_id = wo_match.group(1).upper()
                    fallback_sql_used = f"""
                    WITH po AS (
                      SELECT
                        PART_NUMBER,
                        SUM(COALESCE(QTY_OUTSTANDING, 0)) AS incoming_qty,
                        MIN(COALESCE(CONFIRMED_DELIVERY_DATE, REQUESTED_DELIVERY_DATE)) AS next_supply_date
                      FROM {DB}.{SCHEMA}.PROCUREMENT_SUMMARY_VW
                      WHERE PO_STATUS NOT IN ('COMPLETED', 'CANCELLED')
                        AND COALESCE(QTY_OUTSTANDING, 0) > 0
                      GROUP BY PART_NUMBER
                    )
                    SELECT
                      sa.WORK_ORDER_ID,
                      sa.PART_NUMBER AS part_number,
                      COALESCE(sa.PART_DESCRIPTION, '') AS part_description,
                      COALESCE(sa.SHORTAGE_QTY, 0) AS shortage_qty,
                      COALESCE(inv.QTY_AVAILABLE, 0) AS qty_available,
                      COALESCE(po.incoming_qty, 0) AS incoming_qty,
                      po.next_supply_date AS next_supply_date,
                      CASE
                        WHEN COALESCE(po.incoming_qty, 0) > 0 THEN 'OPEN_PO'
                        ELSE 'NO_OPEN_SUPPLY'
                      END AS supply_status
                    FROM {DB}.{SCHEMA}.SHORTAGE_ALERT_VW sa
                    LEFT JOIN {DB}.{SCHEMA}.INVENTORY inv
                      ON inv.PART_NUMBER = sa.PART_NUMBER
                    LEFT JOIN po
                      ON po.PART_NUMBER = sa.PART_NUMBER
                    WHERE sa.WORK_ORDER_ID = %s
                      AND COALESCE(sa.SHORTAGE_QTY, 0) > 0
                      AND sa.ALERT_STATUS = 'OPEN'
                    ORDER BY shortage_qty DESC, sa.PART_NUMBER
                    """
                    try:
                        fallback_rows = run_query_on_conn(conn, fallback_sql_used, (wo_id,))
                    except Exception:
                        log.warning("WO blocking-parts fallback execution failed", exc_info=True)
                        fallback_rows = []
                    sql_stmts = [fallback_sql_used]
                    if not full_text.strip():
                        full_text = (
                            f"**Descriptive**: Interpreting your question as: which parts are currently blocking work order {wo_id}, "
                            "based on open shortages and available/incoming supply.\n\n"
                            "**Prescriptive**: Recommendations are based on shortage quantities, available inventory, and open supply by part.\n"
                        )
            elif "safety stock" in msg_l:
                fallback_used = True
                fallback_kind = "safety_stock"
                sql_stmts = [
                    f"""
                    WITH inv AS (
                      SELECT
                        PART_NUMBER AS part_number,
                        SUM(COALESCE(QTY_AVAILABLE, 0)) AS qty_available,
                        SUM(COALESCE(SAFETY_STOCK, 0)) AS safety_stock
                      FROM {DB}.{SCHEMA}.INVENTORY
                      GROUP BY PART_NUMBER
                    ),
                    incoming AS (
                      SELECT
                        PART_NUMBER AS part_number,
                        SUM(COALESCE(QTY_OUTSTANDING, 0)) AS incoming_qty
                      FROM {DB}.{SCHEMA}.PROCUREMENT_SUMMARY_VW
                      WHERE PO_STATUS NOT IN ('COMPLETED', 'CANCELLED')
                        AND COALESCE(QTY_OUTSTANDING, 0) > 0
                      GROUP BY PART_NUMBER
                    )
                    SELECT
                      inv.part_number,
                      inv.qty_available,
                      inv.safety_stock,
                      (inv.qty_available - inv.safety_stock) AS qty_vs_safety_stock,
                      CASE
                        WHEN inv.safety_stock > 0 THEN ROUND(inv.qty_available / inv.safety_stock, 2)
                        ELSE NULL
                      END AS safety_stock_coverage_ratio,
                      COALESCE(incoming.incoming_qty, 0) AS incoming_qty
                    FROM inv
                    LEFT JOIN incoming ON inv.part_number = incoming.part_number
                    WHERE inv.safety_stock > 0
                    ORDER BY qty_vs_safety_stock ASC
                    LIMIT 50
                    """
                ]
                if not full_text.strip():
                    full_text = (
                        "**Descriptive**: Interpreting your question as: which parts are below safety stock today, "
                        "and what incoming PO quantity could help recover coverage.\n\n"
                        "**Prescriptive**: Recommendations are derived from parts below safety stock, their coverage ratio, "
                        "and open incoming quantities.\n"
                    )

        # Execute the SQL statements returned by Cortex Analyst in the SAME session/connection
        rows: list[dict] = []
        sql_used = ""
        if fallback_rows is not None:
            rows = fallback_rows
            sql_used = fallback_sql_used
        for stmt in sql_stmts:
            if rows:
                break
            try:
                tmp_rows = run_query_on_conn(conn, stmt)
                if not sql_used:
                    sql_used = stmt
                if tmp_rows:
                    rows = tmp_rows
                    sql_used = stmt
                    break
            except Exception:
                log.warning("Cortex Analyst SQL execution failed", exc_info=True)

        # Build descriptive text from the analyst response
        desc_text = full_text.replace("This is our interpretation of your question:", "").strip()

        # Generate prescriptive recommendations using Cortex Complete (same as Streamlit)
        data_summary = ""
        row_count = len(rows)
        wo_match_final = re.search(r"\b(wo0*\d+)\b", message, re.IGNORECASE)
        pres_text = ""
        if rows:
            # Keep the prompt small to reduce Cortex Complete latency.
            rows_for_prompt = rows[:10]
            data_summary = json.dumps(rows_for_prompt, default=str, ensure_ascii=False)
            if len(data_summary) > 6000:
                data_summary = data_summary[:6000] + "... (truncated)"

        if fallback_kind == "wo_blocking_parts" and wo_match_final and row_count == 0:
            wo_id_final = wo_match_final.group(1).upper()
            desc_text = (
                f"**Work order check:** No open shortage rows were found for {wo_id_final} in "
                f"`{DB}.{SCHEMA}.SHORTAGE_ALERT_VW` at this moment."
            )
            pres_text = (
                "Here are the bullet points with specific findings, concrete actions, and explanations:\n\n"
                f"- **No Current Open Shortages**: The shortage view returned 0 open blocking part rows for {wo_id_final}. "
                "**Action:** Verify whether this WO is blocked for a reason other than part shortage (for example pegging/allocation timing). "
                "**Why it matters:** It prevents false escalation to procurement.\n\n"
                "- **Validate Pegging Snapshot**: Shortage status can differ by run timestamp and allocation state. "
                "**Action:** Refresh the CTB/pegging snapshot and re-check this WO in the latest cycle. "
                "**Why it matters:** Ensures decisions are based on current availability.\n\n"
                "- **Check Incoming Supply Timing**: A WO may appear buildable after deliveries but blocked now. "
                "**Action:** Compare required date vs next confirmed supply date for all BOM parts. "
                "**Why it matters:** Distinguishes temporary blockages from true shortages.\n\n"
                "- **Investigate Non-Material Constraints**: Capacity or scheduling rules may block release even without shortages. "
                "**Action:** Review lane priority/scheduling constraints for this WO. "
                "**Why it matters:** Identifies the real bottleneck and avoids wrong corrective action."
            )

        if fallback_kind == "safety_stock":
            try:
                below = [r for r in rows if (r.get("QTY_VS_SAFETY_STOCK") or r.get("qty_vs_safety_stock") or 0) < 0]
                below_cnt = len(below)
                min_cov = None
                worst_part = None
                for r in rows:
                    cov = r.get("SAFETY_STOCK_COVERAGE_RATIO") if "SAFETY_STOCK_COVERAGE_RATIO" in r else r.get("safety_stock_coverage_ratio")
                    try:
                        if cov is None:
                            continue
                        cov_f = float(cov)
                        if min_cov is None or cov_f < min_cov:
                            min_cov = cov_f
                            worst_part = r.get("PART_NUMBER") or r.get("part_number")
                    except Exception:
                        continue

                top_below = []
                for r in below[:5]:
                    pn = r.get("PART_NUMBER") or r.get("part_number")
                    delta = r.get("QTY_VS_SAFETY_STOCK") or r.get("qty_vs_safety_stock")
                    top_below.append(f"{pn} ({delta})")
                top_below_str = ", ".join(top_below) if top_below else "n/a"

                worst_str = f"{worst_part} (coverage {min_cov:.2f}x)" if worst_part and min_cov is not None else "n/a"
                desc_text = (
                    f"**Safety stock coverage (current state):** {below_cnt} of the top {row_count} parts are **below safety stock**. "
                    f"Worst coverage is {worst_str}. "
                    f"Examples of parts below safety stock (qty vs safety stock): {top_below_str}."
                )
            except Exception:
                log.warning("Failed to build fallback descriptive text", exc_info=True)

        prescriptive_prompt = f"""You are a manufacturing supply chain analyst. Based on this question and data, provide EXACTLY 4-5 specific prescriptive recommendations.

Question asked: {message}

Data returned ({row_count} rows):
{data_summary}

You MUST format your response EXACTLY like this with bullet points:

Here are the bullet points with specific findings, concrete actions, and explanations:

- **[Topic 1]**: [Specific finding citing actual values from the data above]. **Action:** [Concrete action to take]. **Why it matters:** [Business impact].

- **[Topic 2]**: [Specific finding citing actual values from the data above]. **Action:** [Concrete action to take]. **Why it matters:** [Business impact].

- **[Topic 3]**: [Specific finding citing actual values from the data above]. **Action:** [Concrete action to take]. **Why it matters:** [Business impact].

- **[Topic 4]**: [Specific finding citing actual values from the data above]. **Action:** [Concrete action to take]. **Why it matters:** [Business impact].

IMPORTANT: Use ACTUAL supplier IDs, part numbers, quantities, and percentages from the data. Do NOT use generic placeholders."""

        if not (fallback_kind == "wo_blocking_parts" and row_count == 0):
            try:
                pres_rows = run_query_on_conn(
                    conn,
                    "SELECT SNOWFLAKE.CORTEX.COMPLETE(%s, %s) AS \"response\"",
                    ("llama3.1-8b", prescriptive_prompt),
                )
                pres_text = str(pres_rows[0].get("response", "") or "") if pres_rows else ""
            except Exception:
                log.warning("Failed to generate prescriptive text", exc_info=True)

        # Infer chart config from the returned data
        chart: dict | None = None
        if rows:
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
                chart = {
                    "type": "bar_horizontal" if horizontal else "bar",
                    "xKey": x_key,
                    "yKey": y_key,
                    "color": "#3B82F6",
                    "title": "Visualization",
                }

        # Override visualization for safety-stock fallback so it never comes out empty.
        if fallback_used and rows:
            chart = {
                "type": "bar_horizontal",
                "xKey": "part_number",
                "yKey": "qty_vs_safety_stock",
                "color": "#EF4444",
                "title": "Parts below safety stock (deficit)",
            }

        result = {
            "key": "custom",
            "metrics": {"summary": f"{row_count} rows returned"},
            "rows": rows,
            "sql": sql_used,
            "descriptive": desc_text,
            "prescriptive": pres_text,
            "chart": chart,
        }
        _cache_set(cache_key, result)
        return result


def shortage_agent_queue() -> list[dict]:
    sql = f"""
    WITH current_shortages AS (
        SELECT 
            sa.PART_NUMBER AS part_number,
            'CURRENT' AS shortage_type,
            COUNT(DISTINCT sa.WORK_ORDER_ID) as affected_wo_count,
            SUM(sa.SHORTAGE_QTY) as shortage_qty,
            MIN(wo.PRIORITY) as highest_priority,
            LISTAGG(DISTINCT wo.PRODUCT_ID, ', ') WITHIN GROUP (ORDER BY wo.PRODUCT_ID) as affected_products,
            LISTAGG(DISTINCT sa.PLANT_ID, ', ') WITHIN GROUP (ORDER BY sa.PLANT_ID) as affected_plants,
            MIN(wo.PLANNED_START_DATE) as shortage_date,
            MIN(DATEDIFF('day', CURRENT_DATE, wo.PLANNED_START_DATE)) as days_until_shortage,
            AVG(COALESCE(i.STANDARD_COST, 100)) as unit_cost
        FROM {DB}.{SCHEMA}.SHORTAGE_ALERT_VW sa
        JOIN {DB}.{SCHEMA}.WORK_ORDER wo ON sa.WORK_ORDER_ID = wo.WORK_ORDER_ID
        LEFT JOIN {DB}.{SCHEMA}.INVENTORY i ON sa.PART_NUMBER = i.PART_NUMBER
        WHERE sa.ALERT_STATUS = 'OPEN'
        GROUP BY sa.PART_NUMBER
    ),
    weeks AS (
        SELECT 
            ROW_NUMBER() OVER (ORDER BY SEQ4()) as week_num,
            DATEADD('WEEK', ROW_NUMBER() OVER (ORDER BY SEQ4()), DATE_TRUNC('WEEK', CURRENT_DATE)) as week_start
        FROM TABLE(GENERATOR(ROWCOUNT => 12))
    ),
    planned_consumption AS (
        SELECT 
            b.CHILD_PART_NUMBER AS part_number,
            w.week_num,
            w.week_start,
            SUM(b.QTY_PER_ASSEMBLY * wo.PLANNED_QTY) AS weekly_demand
        FROM {DB}.{SCHEMA}.WORK_ORDER wo
        JOIN {DB}.{SCHEMA}.BOM b ON wo.PRODUCT_ID = b.PARENT_PART_NUMBER AND b.BOM_STATUS = 'ACTIVE'
        JOIN weeks w ON DATE_TRUNC('WEEK', wo.PLANNED_START_DATE) = w.week_start
        GROUP BY b.CHILD_PART_NUMBER, w.week_num, w.week_start
    ),
    cumulative_demand AS (
        SELECT 
            part_number,
            week_num,
            week_start,
            weekly_demand,
            SUM(weekly_demand) OVER (PARTITION BY part_number ORDER BY week_num) AS cum_demand
        FROM planned_consumption
    ),
    current_supply AS (
        SELECT 
            part_number,
            SUM(COALESCE(QTY_AVAILABLE, 0)) AS qty_available,
            SUM(COALESCE(SAFETY_STOCK, 0)) AS safety_stock
        FROM {DB}.{SCHEMA}.INVENTORY
        GROUP BY part_number
    ),
    incoming_po AS (
        SELECT 
            PART_NUMBER AS part_number,
            DATE_TRUNC('WEEK', COALESCE(CONFIRMED_DELIVERY_DATE, REQUESTED_DELIVERY_DATE)) AS delivery_week,
            SUM(COALESCE(QTY_OUTSTANDING, 0)) AS incoming_qty
        FROM {DB}.{SCHEMA}.PROCUREMENT_SUMMARY_VW
        WHERE PO_STATUS NOT IN ('COMPLETED', 'CANCELLED') AND QTY_OUTSTANDING > 0
        GROUP BY PART_NUMBER, DATE_TRUNC('WEEK', COALESCE(CONFIRMED_DELIVERY_DATE, REQUESTED_DELIVERY_DATE))
    ),
    cumulative_supply AS (
        SELECT 
            cd.part_number,
            cd.week_num,
            cd.week_start,
            cd.cum_demand,
            COALESCE(cs.qty_available, 0) + COALESCE(cs.safety_stock, 0) +
              COALESCE(SUM(ip.incoming_qty) OVER (PARTITION BY cd.part_number ORDER BY cd.week_num), 0) AS total_supply
        FROM cumulative_demand cd
        LEFT JOIN current_supply cs ON cd.part_number = cs.part_number
        LEFT JOIN incoming_po ip ON cd.part_number = ip.part_number AND ip.delivery_week <= cd.week_start
    ),
    first_predicted_shortage AS (
        SELECT part_number, MIN(week_num) AS first_short_week
        FROM cumulative_supply
        WHERE cum_demand > total_supply
        GROUP BY part_number
    ),
    predicted_shortages AS (
        SELECT 
            cs.part_number,
            'PREDICTED' AS shortage_type,
            0 as affected_wo_count,
            cs.cum_demand - cs.total_supply as shortage_qty,
            5 as highest_priority,
            NULL as affected_products,
            NULL as affected_plants,
            cs.week_start as shortage_date,
            cs.week_num * 7 as days_until_shortage,
            COALESCE(i.STANDARD_COST, 100) as unit_cost
        FROM cumulative_supply cs
        JOIN first_predicted_shortage fps ON cs.part_number = fps.part_number AND cs.week_num = fps.first_short_week
        LEFT JOIN {DB}.{SCHEMA}.INVENTORY i ON cs.part_number = i.PART_NUMBER
        WHERE cs.cum_demand > cs.total_supply AND cs.week_num >= 2
    ),
    combined AS (
        SELECT * FROM current_shortages
        UNION ALL
        SELECT * FROM predicted_shortages
        WHERE part_number NOT IN (SELECT part_number FROM current_shortages)
    )
    SELECT 
        part_number,
        shortage_type,
        affected_wo_count,
        shortage_qty,
        highest_priority,
        affected_products,
        affected_plants,
        shortage_date::VARCHAR AS shortage_date,
        days_until_shortage,
        unit_cost,
        (6 - highest_priority) * shortage_qty * unit_cost as business_impact_score,
        CASE 
            WHEN days_until_shortage <= 7 THEN 'ACT_NOW'
            WHEN days_until_shortage <= 14 THEN 'PLAN_THIS_WEEK'
            ELSE 'MONITOR'
        END as action_group
    FROM combined
    QUALIFY ROW_NUMBER() OVER (PARTITION BY part_number, shortage_type ORDER BY shortage_date) = 1
    ORDER BY 
        CASE 
            WHEN days_until_shortage <= 7 THEN 1
            WHEN days_until_shortage <= 14 THEN 2
            ELSE 3
        END,
        business_impact_score DESC
    """
    return run_query(sql)


def shortage_agent_recommendation(payload: dict) -> dict:
    part_number = str(payload.get("part_number", ""))
    shortage_qty = payload.get("shortage_qty", 0)
    days_until = payload.get("days_until_shortage", 0)
    shortage_type = str(payload.get("shortage_type", "CURRENT"))
    affected_wo = payload.get("affected_wo_count", 0)
    affected_products = payload.get("affected_products", "")
    affected_plants = payload.get("affected_plants", "")
    impact = payload.get("business_impact_score", 0)
    safe_part = part_number.replace("'", "''")
    vendor_rows = run_query(
        f"""
        SELECT psu.SUPPLIER_ID,
               COALESCE(s.SUPPLIER_NAME, psu.SUPPLIER_ID) AS SUPPLIER_NAME,
               psu.LEAD_TIME_DAYS, psu.RELIABILITY_SCORE, psu.UNIT_PRICE,
               psu.MIN_ORDER_QTY, psu.IS_PREFERRED, psu.PRIORITY_RANK,
               DATEADD('day', psu.LEAD_TIME_DAYS, CURRENT_DATE()) AS EST_DELIVERY
        FROM {DB}.{SCHEMA}.PART_SUPPLIER_VW psu
        LEFT JOIN {DB}.{SCHEMA}.SUPPLIER_SUMMARY_VW s ON psu.SUPPLIER_ID = s.SUPPLIER_ID
        WHERE psu.PART_NUMBER = '{safe_part}'
        ORDER BY psu.PRIORITY_RANK NULLS LAST, psu.RELIABILITY_SCORE DESC NULLS LAST
        LIMIT 5
        """
    )
    inv_rows = run_query(
        f"""
        SELECT PLANT_ID, QTY_ON_HAND, QTY_AVAILABLE, SAFETY_STOCK,
               QTY_AVAILABLE - COALESCE(SAFETY_STOCK, 0) AS TRANSFERABLE_QTY
        FROM {DB}.{SCHEMA}.INVENTORY
        WHERE PART_NUMBER = '{safe_part}'
        ORDER BY TRANSFERABLE_QTY DESC
        """
    )
    po_rows = run_query(
        f"""
        SELECT po.PO_ID, po.SUPPLIER_ID, po.QTY_OUTSTANDING,
               COALESCE(po.CONFIRMED_DELIVERY_DATE, po.REQUESTED_DELIVERY_DATE) AS DELIVERY_DATE,
               DATEDIFF('day', CURRENT_DATE, COALESCE(po.CONFIRMED_DELIVERY_DATE, po.REQUESTED_DELIVERY_DATE)) AS DAYS_TO_DELIVERY
        FROM {DB}.{SCHEMA}.PROCUREMENT_SUMMARY_VW po
        WHERE po.PART_NUMBER = '{safe_part}'
          AND po.PO_STATUS NOT IN ('COMPLETED', 'CANCELLED')
          AND po.QTY_OUTSTANDING > 0
        ORDER BY DAYS_TO_DELIVERY
        LIMIT 5
        """
    )

    wo_info = f"{affected_wo} work orders" if int(float(affected_wo or 0)) > 0 else "Predicted shortage"
    urgency = str(payload.get("action_group", "ACT_NOW") or "ACT_NOW")

    # Deterministic scoring for consistent recommendations across parts.
    suppliers: list[dict] = []
    for r in vendor_rows:
        sid = str(r.get("SUPPLIER_ID") or "")
        if not sid:
            continue
        lead = int(float(r.get("LEAD_TIME_DAYS") or 999))
        rel = float(r.get("RELIABILITY_SCORE") or 0)
        price = float(r.get("UNIT_PRICE") or 0)
        moq = int(float(r.get("MIN_ORDER_QTY") or 1))
        if urgency == "ACT_NOW":
            score = (1000 - lead * 10) + (rel * 4) - (price * 0.2)
        else:
            score = (rel * 8) - (price * 0.4) + (300 - lead * 2)
        suppliers.append(
            {
                "supplier_id": sid,
                "supplier_name": str(r.get("SUPPLIER_NAME") or sid),
                "lead_time_days": lead,
                "reliability": rel,
                "unit_price": price,
                "min_order_qty": moq,
                "score": score,
            }
        )
    suppliers.sort(key=lambda x: x["score"], reverse=True)
    best = suppliers[0] if suppliers else {
        "supplier_id": "SUP001",
        "supplier_name": "Default Supplier",
        "lead_time_days": 14,
        "reliability": 70.0,
        "unit_price": 0.0,
        "min_order_qty": 1,
        "score": 0.0,
    }

    transferable = 0
    transfer_plant = "N/A"
    for ir in inv_rows:
        tq = float(ir.get("TRANSFERABLE_QTY") or 0)
        if tq > transferable:
            transferable = tq
            transfer_plant = str(ir.get("PLANT_ID") or "N/A")

    affected_plants_list = [p.strip() for p in str(affected_plants or "").split(",") if p.strip()]
    # Pick a destination plant different from the "from" plant, when possible.
    # This helps the UI show: "transfer X units from A to B" and what B covers.
    dest_plant = "N/A"
    if affected_plants_list:
        if transfer_plant != "N/A":
            dest_plant = next((p for p in affected_plants_list if p != transfer_plant), affected_plants_list[0])
        else:
            dest_plant = affected_plants_list[0]

    expedite_po = None
    if po_rows:
        po_rows_sorted = sorted(
            po_rows,
            key=lambda p: int(float(p.get("DAYS_TO_DELIVERY") or 999)),
        )
        expedite_po = po_rows_sorted[0]

    recommended_qty = max(int(float(shortage_qty or 0)), int(best["min_order_qty"]))
    moq = int(best["min_order_qty"])
    if moq > 0 and recommended_qty % moq != 0:
        recommended_qty = ((recommended_qty + moq - 1) // moq) * moq

    base_response = f"""<strong>SITUATION ASSESSMENT</strong>
Part {part_number} has a {shortage_type.lower()} shortage of {int(float(shortage_qty or 0)):,} units across {wo_info}, with impact ${float(impact or 0):,.0f} and timeline {int(float(days_until or 0))} days to shortage.

<strong>IMMEDIATE ACTIONS (ranked by speed)</strong>
1. INTER-PLANT TRANSFER: {
        (
            f'Transfer up to {int(transferable)} units from {transfer_plant} to {dest_plant} to cover the near-term shortage at {dest_plant}.'
            if transferable > 0 and dest_plant != "N/A"
            else (
                f'Transfer up to {int(transferable)} units from {transfer_plant} (destination plant not available in the shortage scope).'
                if transferable > 0
                else "No transferable inventory available across plants right now."
            )
        )
    }
2. EXPEDITE OPEN PO: {('Expedite PO ' + str(expedite_po.get('PO_ID')) + ' from ' + str(expedite_po.get('SUPPLIER_ID')) + ' (' + str(int(float(expedite_po.get('QTY_OUTSTANDING') or 0))) + ' units, ETA ' + str(int(float(expedite_po.get('DAYS_TO_DELIVERY') or 0))) + ' days).') if expedite_po else 'No open PO exists for this part; create a new emergency PO immediately.'}
3. NEW EMERGENCY ORDER: Place emergency PO to {best['supplier_id']} ({best['supplier_name']}) for {recommended_qty:,} units. Lead time {best['lead_time_days']} days, reliability {best['reliability']:.1f}, unit price ${best['unit_price']:.2f}.

<strong>RECOMMENDED SUPPLIER</strong>
Supplier: {best['supplier_name']} ({best['supplier_id']})
Lead Time: {best['lead_time_days']} days
Reliability: {best['reliability']:.1f}
Unit Price: ${best['unit_price']:.2f}
Min Order Qty: {best['min_order_qty']}
Recommended Order Qty: {recommended_qty:,}
Why this supplier: {"Fastest-response supplier prioritized for ACT_NOW shortage, with acceptable reliability and cost." if urgency == "ACT_NOW" else "Best reliability-cost balance while still meeting the shortage timeline."}

<strong>VENDOR COMMUNICATION DRAFT</strong>
To: {best['supplier_name']}
Subject: URGENT - Expedite Request for {part_number}
Dear {best['supplier_name']} Team,
We have an urgent shortage of {int(float(shortage_qty or 0)):,} units for part {part_number} impacting {wo_info}. Please confirm immediate availability and earliest delivery for {recommended_qty:,} units.
Please also share expedite options and any premium freight/fees.
Regards,
Supply Chain Team

<strong>TIMELINE & RESOLUTION</strong>
If emergency PO is placed now with {best['supplier_id']}, expected coverage starts in about {best['lead_time_days']} days. The inter-plant transfer helps bridge near-term demand at {dest_plant} (subject to inventory availability). Combine transfer/expedite actions to bridge overall risk."""
    return {"response": base_response}


def shortage_agent_create_po(payload: dict) -> dict:
    part_number = str(payload.get("part_number", "") or "").strip()
    if not part_number:
        return {"ok": False, "message": "part_number is required"}

    shortage_qty = int(float(payload.get("shortage_qty", 0) or 0))
    order_qty = max(shortage_qty, 1)

    plant_id = str(payload.get("plant_id", "") or "").strip()
    if not plant_id:
        affected_plants = str(payload.get("affected_plants", "") or "")
        plant_id = affected_plants.split(",")[0].strip() if affected_plants else "PL001"

    supplier_id = str(payload.get("supplier_id", "") or "").strip()
    if not supplier_id:
        sup_rows = run_query(
            f"""
            SELECT SUPPLIER_ID
            FROM {DB}.{SCHEMA}.PART_SUPPLIER_VW
            WHERE PART_NUMBER = %s
            ORDER BY PRIORITY_RANK NULLS LAST, RELIABILITY_SCORE DESC NULLS LAST
            LIMIT 1
            """,
            (part_number,),
        )
        supplier_id = str(sup_rows[0].get("SUPPLIER_ID") or "SUP001") if sup_rows else "SUP001"

    try:
        result_rows = run_query(
            f"""
            CALL {DB}.{SCHEMA}.SP_CREATE_PO(
                %s, %s, %s, %s, DATEADD('day', 14, CURRENT_DATE())::DATE, 100.00, 'AI_AGENT'
            )
            """,
            (part_number, order_qty, supplier_id, plant_id),
        )
        po_id = None
        if result_rows:
            first = result_rows[0]
            # Stored proc often returns JSON-like payload in first column.
            if first:
                any_val = next(iter(first.values()))
                if isinstance(any_val, str):
                    try:
                        parsed = json.loads(any_val)
                        po_id = parsed.get("po_id")
                    except Exception:
                        po_id = None
        msg = f"PO created for {part_number} ({order_qty:,} units, {supplier_id}, {plant_id})."
        return {"ok": True, "message": msg, "po_id": po_id}
    except Exception as exc:
        log.warning("Create PO failed", exc_info=True)
        return {"ok": False, "message": f"Failed to create PO: {exc}"}


def shortage_agent_email_draft(payload: dict) -> dict:
    part_number = str(payload.get("part_number", "") or "").strip()
    if not part_number:
        return {"to": "", "subject": "", "body": "part_number is required"}

    shortage_type = str(payload.get("shortage_type", "CURRENT") or "CURRENT")
    action_group = str(payload.get("action_group", "ACT_NOW") or "ACT_NOW")
    shortage_qty = int(float(payload.get("shortage_qty", 0) or 0))
    days_until = int(float(payload.get("days_until_shortage", 0) or 0))
    affected_wo_count = int(float(payload.get("affected_wo_count", 0) or 0))
    affected_products = str(payload.get("affected_products", "N/A") or "N/A")
    affected_plants = str(payload.get("affected_plants", "N/A") or "N/A")

    supplier_rows = run_query(
        f"""
        SELECT ps.SUPPLIER_ID,
               COALESCE(ss.SUPPLIER_NAME, ps.SUPPLIER_ID) AS SUPPLIER_NAME
        FROM {DB}.{SCHEMA}.PART_SUPPLIER_VW ps
        LEFT JOIN {DB}.{SCHEMA}.SUPPLIER_SUMMARY_VW ss ON ss.SUPPLIER_ID = ps.SUPPLIER_ID
        WHERE ps.PART_NUMBER = %s
        ORDER BY ps.PRIORITY_RANK NULLS LAST, ps.RELIABILITY_SCORE DESC NULLS LAST
        LIMIT 1
        """,
        (part_number,),
    )
    supplier_id = str(supplier_rows[0].get("SUPPLIER_ID") or "SUP001") if supplier_rows else "SUP001"
    supplier_name = str(supplier_rows[0].get("SUPPLIER_NAME") or supplier_id) if supplier_rows else supplier_id

    to_email = "vendor@supplier.com"
    try:
        mail_rows = run_query(
            f"""
            SELECT SUPPLIER_NAME, CONTACT_EMAIL
            FROM {DB}.SAP_STG.LFA1
            WHERE SUPPLIER_ID = %s
            LIMIT 1
            """,
            (supplier_id,),
        )
        if mail_rows:
            supplier_name = str(mail_rows[0].get("SUPPLIER_NAME") or supplier_name)
            to_email = str(mail_rows[0].get("CONTACT_EMAIL") or to_email)
    except Exception:
        log.warning("Could not load supplier email from LFA1", exc_info=True)

    subject = f"URGENT: Expedited Delivery Request - {part_number}"
    body = f"""Dear {supplier_name} Team,

We are experiencing a {'critical' if action_group == 'ACT_NOW' else 'projected'} shortage of part {part_number} at our {affected_plants} facility.

Current Situation:
- Type: {shortage_type} shortage
- Quantity Required: {shortage_qty:,} units
- Action Urgency: {action_group.replace('_', ' ')}
- Days Until Impact: {days_until} days
- Impact: {affected_wo_count} work orders affected
- Products: {affected_products}

Please confirm:
1. Availability of {shortage_qty:,} units
2. Fastest possible delivery date
3. Any expedite fees if applicable

Best regards,
Supply Chain Team
""".strip()
    return {"to": to_email, "subject": subject, "body": body, "supplier_id": supplier_id}


def shortage_agent_po_preview(payload: dict) -> list[dict]:
    part_number = str(payload.get("part_number", "") or "").strip()
    if not part_number:
        return []
    shortage_qty = int(float(payload.get("shortage_qty", 0) or 0))

    supplier_rows = run_query(
        f"""
        SELECT ps.SUPPLIER_ID,
               COALESCE(ss.SUPPLIER_NAME, ps.SUPPLIER_ID) AS SUPPLIER_NAME,
               COALESCE(ps.LEAD_TIME_DAYS, 14) AS LEAD_TIME_DAYS,
               COALESCE(ps.UNIT_PRICE, 0) AS UNIT_PRICE,
               COALESCE(ps.MIN_ORDER_QTY, 1) AS MIN_ORDER_QTY
        FROM {DB}.{SCHEMA}.PART_SUPPLIER_VW ps
        LEFT JOIN {DB}.{SCHEMA}.SUPPLIER_SUMMARY_VW ss ON ss.SUPPLIER_ID = ps.SUPPLIER_ID
        WHERE ps.PART_NUMBER = %s
        ORDER BY ps.PRIORITY_RANK NULLS LAST, ps.RELIABILITY_SCORE DESC NULLS LAST
        LIMIT 3
        """,
        (part_number,),
    )
    best_supplier = supplier_rows[0] if supplier_rows else {
        "SUPPLIER_ID": "SUP001",
        "SUPPLIER_NAME": "Default Supplier",
        "LEAD_TIME_DAYS": 14,
        "UNIT_PRICE": 0,
        "MIN_ORDER_QTY": 1,
    }

    plant_short_rows = run_query(
        f"""
        SELECT sa.PLANT_ID, SUM(COALESCE(sa.SHORTAGE_QTY, 0)) AS SHORT_QTY
        FROM {DB}.{SCHEMA}.SHORTAGE_ALERT_VW sa
        WHERE sa.PART_NUMBER = %s
          AND sa.ALERT_STATUS = 'OPEN'
        GROUP BY sa.PLANT_ID
        ORDER BY SHORT_QTY DESC
        """,
        (part_number,),
    )

    # Fallback to affected_plants payload if no open current shortage row exists.
    if not plant_short_rows:
        affected_plants = str(payload.get("affected_plants", "") or "")
        plants = [p.strip() for p in affected_plants.split(",") if p.strip()]
        if not plants:
            plants = ["PL001"]
        split_qty = max(1, shortage_qty // max(1, len(plants)))
        plant_short_rows = [{"PLANT_ID": p, "SHORT_QTY": split_qty} for p in plants]

    moq = int(float(best_supplier.get("MIN_ORDER_QTY") or 1))
    lead = int(float(best_supplier.get("LEAD_TIME_DAYS") or 14))
    supplier_id = str(best_supplier.get("SUPPLIER_ID") or "SUP001")
    supplier_name = str(best_supplier.get("SUPPLIER_NAME") or supplier_id)
    unit_price = float(best_supplier.get("UNIT_PRICE") or 0)

    # Reconcile quantities to exactly match selected shortage qty.
    raw_alloc = []
    sum_short = 0
    for pr in plant_short_rows:
        q = max(0, int(float(pr.get("SHORT_QTY") or 0)))
        raw_alloc.append({"plant_id": str(pr.get("PLANT_ID") or "PL001"), "short_qty": q})
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
            sq = int(round((a["short_qty"] / sum_short) * target))
            sq = max(0, sq)
            running += sq
        scaled.append({"plant_id": a["plant_id"], "short_qty": sq})
    diff = target - sum(s["short_qty"] for s in scaled)
    if scaled and diff != 0:
        scaled[0]["short_qty"] += diff

    out: list[dict] = []
    for pr in scaled:
        plant_id = pr["plant_id"]
        base_qty = int(pr["short_qty"])
        rec_qty = max(1, base_qty)
        out.append(
            {
                "supplier_id": supplier_id,
                "supplier_name": supplier_name,
                "lead_time_days": lead,
                "unit_price": unit_price,
                "min_order_qty": moq,
                "recommended_qty": rec_qty,
                "plant_id": plant_id,
                "delivery_date": f"+{lead} days",
            }
        )
    # Do not truncate allocations to 5; the UI expects one allocation card per impacted plant.
    # Cap defensively to avoid very large responses.
    return out[:20]


def shortage_agent_mark_resolved(payload: dict) -> dict:
    part_number = str(payload.get("part_number", "") or "").strip()
    shortage_type = str(payload.get("shortage_type", "CURRENT") or "CURRENT")
    if not part_number:
        return {"ok": False, "message": "part_number is required"}

    try:
        if shortage_type == "CURRENT":
            run_query(
                f"""
                UPDATE {DB}.{SCHEMA}.SHORTAGE_ALERT
                SET ALERT_STATUS = 'RESOLVED'
                WHERE PART_NUMBER = %s AND ALERT_STATUS = 'OPEN'
                """,
                (part_number,),
            )
        return {"ok": True, "message": f"Shortage {part_number} marked as resolved."}
    except Exception as exc:
        log.warning("Mark resolved failed", exc_info=True)
        return {"ok": False, "message": f"Failed to mark resolved: {exc}"}
