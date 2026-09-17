from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

from config import Config

logger = logging.getLogger(__name__)


def _storage_options() -> dict[str, str]:
    missing = [
        name for name, value in [
            ("AZURE_TENANT_ID", Config.AZURE_TENANT_ID),
            ("AZURE_CLIENT_ID", Config.AZURE_CLIENT_ID),
            ("AZURE_CLIENT_SECRET", Config.AZURE_CLIENT_SECRET),
        ] if not value
    ]
    if missing:
        raise RuntimeError(
            "OneLake write not configured. Missing: " + ", ".join(missing)
        )
    return {
        "azure_tenant_id": Config.AZURE_TENANT_ID,
        "azure_client_id": Config.AZURE_CLIENT_ID,
        "azure_client_secret": Config.AZURE_CLIENT_SECRET,
        "use_fabric_endpoint": "true",
    }


def _now_utc() -> datetime:
    """Timezone-aware UTC 'now'.

    IMPORTANT: this used to strip tzinfo (naive) on the theory that "Delta
    timestamps are naive UTC". That's backwards for THIS write path: a
    bare pyarrow timestamp("us") with no tz maps to Delta's newer
    TIMESTAMP_NTZ logical type, which is a protocol table feature
    ("timestampNtz") that must be explicitly enabled on the table before
    delta-rs will write it - confirmed by write_deltalake failing against
    a pre-existing table (created outside this writer, e.g. via Spark/
    Fabric with the classic UTC-instant TIMESTAMP type) with:
        CommitFailedError: Table features must be specified,
        please specify: TimestampWithoutTimezone
    Keeping tzinfo=UTC here (paired with pa.timestamp("us", tz="UTC") in
    every schema below) maps to Delta's classic TIMESTAMP type instead,
    which every table protocol version already supports - no feature
    negotiation needed.
    """
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Table schemas - copied verbatim from the tested Fabric notebook's
# spark.table("ReadyToBuild.<TABLE>").schema output. Column order matters
# for write_deltalake's schema-matching.
#
# All timestamp columns use tz="UTC" (see _now_utc's docstring above) -
# NOT the naive pa.timestamp("us") one might otherwise reach for.
# ---------------------------------------------------------------------------

SAVED_INSIGHTS_SCHEMA = pa.schema([
    ("insight_id", pa.int32()),
    ("created_at", pa.timestamp("us", tz="UTC")),
    ("created_by", pa.string()),
    ("page", pa.string()),
    ("title", pa.string()),
    ("question", pa.string()),
    ("verified_query_name", pa.string()),
    ("sql_text", pa.string()),
    ("tags", pa.string()),
])

GENIE_QUESTION_HISTORY_SCHEMA = pa.schema([
    ("normalized_query", pa.string()),
    ("type", pa.string()),
    ("user", pa.string()),
    ("frequency", pa.int32()),
    ("last_asked_at", pa.timestamp("us", tz="UTC")),
])

AGENT_REVIEW_LOG_SCHEMA = pa.schema([
    ("REVIEW_ID", pa.int32()),
    ("AGENT_KEY", pa.string()),
    ("ENTITY_ID", pa.string()),
    ("ENTITY_TYPE", pa.string()),
    ("ACTION_TAKEN", pa.string()),
    ("NOTES", pa.string()),
    ("REVIEWED_BY", pa.string()),
    ("REVIEWED_AT", pa.timestamp("us", tz="UTC")),
])

# NOTE ON THE TWO SCHEMAS BELOW: unlike the three schemas above (copied
# verbatim from a tested notebook's spark.table(...).schema), these were
# inferred from the columns referenced by service.py's SELECT queries
# against procurement_summary_dt / work_order_dt (get_bom_open_pos,
# get_supplier_performance, get_work_order_detail, etc.), NOT confirmed
# against a live spark.table(...).schema call. Column NAMES should be
# right; column TYPES (esp. QTY_ORDERED/QTY_OUTSTANDING as float64 vs
# decimal/int, IS_LATE as int32 vs bool) are a best guess and must be
# verified the same way the other three were before relying on this in
# production - write_deltalake's schema-matching will raise a clear error
# at write time if a type is wrong, so this will fail loudly, not silently,
# if it's off. Run once against a test row and adjust as needed.
PROCUREMENT_SUMMARY_SCHEMA = pa.schema([
    ("PO_ID", pa.string()),
    ("PART_NUMBER", pa.string()),
    ("SUPPLIER_ID", pa.string()),
    ("RECEIVING_PLANT_ID", pa.string()),
    ("PO_STATUS", pa.string()),
    ("PO_TYPE", pa.string()),
    ("QTY_ORDERED", pa.float64()),
    ("QTY_OUTSTANDING", pa.float64()),
    ("TOTAL_VALUE", pa.float64()),
    ("REQUESTED_DELIVERY_DATE", pa.date32()),
    ("CONFIRMED_DELIVERY_DATE", pa.date32()),
    ("ACTUAL_DELIVERY_DATE", pa.date32()),
    ("IS_LATE", pa.int32()),
    ("QUOTED_LEAD_TIME_DAYS", pa.int32()),
    ("UPDATED_AT", pa.timestamp("us", tz="UTC")),
])


def _table_uri(table_name: str) -> str:
    return Config.onelake_table_uri(table_name)


def _find_col(df, *candidates: str) -> str | None:
    """Case-insensitive column lookup: returns the actual column name in
    df matching any of the given candidate names, or None if none match.

    Written after this exact bug (a table's real column casing not
    matching what the code assumed) recurred more than once, including
    cases where the SAME table's schema apparently differed across two
    otherwise-identical deployments of this exact code (e.g. a table
    that's been dropped/recreated between sessions). Rather than keep
    chasing the current casing by hand, resolve it dynamically at read
    time instead."""
    lower_map = {str(c).lower(): c for c in df.columns}
    for cand in candidates:
        actual = lower_map.get(cand.lower())
        if actual is not None:
            return actual
    return None


def _next_id(table_name: str, id_column: str, sql_run_query=None) -> int:
    """MAX(id_column) + 1, read directly via OneLake - NOT the SQL
    analytics endpoint. `sql_run_query` is still accepted for backward
    compatibility with existing call sites but is intentionally unused now.

    WHY THE CHANGE: the SQL endpoint has now proven, on separate
    occasions, for BOTH SAVED_INSIGHTS ('Invalid column name
    insight_id'/'CREATED_BY') and GENIE_QUESTION_HISTORY, a persistent
    inability to see these tables' columns at all - under any casing,
    even immediately after a confirmed-good write - which is exactly what
    read_saved_insights/read_question_history already had to work around
    for reads. This was the one remaining function still going through
    the SQL endpoint for these two tables; it hits the identical wall.

    Uses the same case-insensitive column resolution (_find_col) as the
    read_* functions, since these tables' real column casing has also
    proven to differ between deployments/recreations of the same table.

    Same caveat as before: read-then-write, not concurrency-safe under
    simultaneous writers.
    """
    del sql_run_query  # intentionally unused - see docstring
    table_uri = _table_uri(table_name)
    storage_options = _storage_options()
    try:
        dt = DeltaTable(table_uri, storage_options=storage_options)
        df = dt.to_pandas()
    except Exception:
        logger.exception("_next_id (OneLake Delta read) failed for %s.%s", table_name, id_column)
        return 1
    if df is None or df.empty:
        return 1
    col = _find_col(df, id_column)
    if not col:
        logger.error(
            "_next_id: could not resolve id column %r on %s; found columns=%s",
            id_column, table_name, list(df.columns),
        )
        return 1
    try:
        max_val = df[col].max()
        return int(max_val) + 1 if pd.notna(max_val) else 1
    except Exception:
        logger.exception("_next_id: failed computing max for %s.%s", table_name, id_column)
        return 1


# ---------------------------------------------------------------------------
# GENIE_QUESTION_HISTORY - upsert (mirrors Snowflake's MERGE: increments
# FREQUENCY and bumps LAST_ASKED_AT on an existing (NORMALIZED_QUERY, TYPE,
# USER) row, inserts a new FREQUENCY=1 row otherwise).
#
# CONFIRMED against the real Delta table (from a live DeltaError's "Valid
# fields are ..." listing - NOT a guess like the procurement/work_order
# schemas' caveat): the actual columns are lowercase normalized_query,
# type, user, frequency, last_asked_at. There is NO persona column at all,
# and the user column is literally named "user", not "user_name" - our
# original schema guessed both of those wrong. `persona` is kept as a
# parameter here only so callers (copilot_service.save_question_history)
# don't need to change their call signature; it is intentionally ignored
# and never written, since the real table has nowhere to put it.
# ---------------------------------------------------------------------------

def upsert_question_history(normalized_query: str, qtype: str, persona: str, user_name: str) -> None:
    """
    WHY THIS DISCOVERS CASING AT RUNTIME instead of hardcoding it: this
    table's real column casing has now been directly observed as BOTH
    lowercase (an earlier session's live DeltaError field listing) AND
    uppercase (a later session's live DeltaError field listing) for what
    is otherwise the same table in the same codebase - almost certainly
    because the table gets dropped/recreated between sessions by
    something outside this backend's control, each time potentially with
    different casing. Hardcoding either one is guaranteed to break again
    the next time that happens. Instead, read the table's OWN schema via
    dt.schema() right before writing and match whatever casing is
    actually there - self-healing across recreations instead of guessing.
    """
    del persona  # intentionally unused - no such column on the real table

    table_uri = _table_uri(Config.HISTORY_TABLE_NAME)
    storage_options = _storage_options()
    now = _now_utc()

    try:
        dt = DeltaTable(table_uri, storage_options=storage_options)
    except Exception:
        logger.exception("upsert_question_history: could not open table")
        raise

    lower_map = {f.name.lower(): f.name for f in dt.schema().fields}
    col_query = lower_map.get("normalized_query", "normalized_query")
    col_type = lower_map.get("type", "type")
    col_user = lower_map.get("user", "user")
    col_freq = lower_map.get("frequency", "frequency")
    col_last = lower_map.get("last_asked_at", "last_asked_at")

    source = pa.table({
        col_query: [normalized_query],
        col_type: [qtype],
        col_user: [user_name],
        col_freq: pa.array([1], type=pa.int32()),
        col_last: pa.array([now], type=pa.timestamp("us", tz="UTC")),
    })

    try:
        (
            dt.merge(
                source=source,
                predicate=(
                    f'target."{col_query}" = source."{col_query}" '
                    f'AND target."{col_type}" = source."{col_type}" '
                    f'AND target."{col_user}" = source."{col_user}"'
                ),
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update(
                updates={
                    col_freq: f'target."{col_freq}" + 1',
                    col_last: f'source."{col_last}"',
                }
            )
            .when_not_matched_insert_all()
            .execute()
        )
    except Exception:
        logger.exception("upsert_question_history (OneLake Delta merge) failed")
        raise


def read_question_history(user_name: str | None = None, top_n: int = 10) -> list[dict]:
    """Reads GENIE_QUESTION_HISTORY directly over OneLake, for the same
    reason read_saved_insights below does - see that function's docstring
    for the full "why not just query it via SQL" explanation; it applies
    identically here (same 'Invalid column name' on every column, even
    right after a confirmed-successful merge).

    user_name=None returns the top `top_n` (normalized_query, type)
    combinations by summed frequency across ALL users (load_most_frequent_all's
    job); user_name='...' returns that one user's own top `top_n` questions
    by frequency (load_frequent_questions's job) - one function covers both
    reads since they're the same table, just aggregated differently.
    """
    table_uri = _table_uri(Config.HISTORY_TABLE_NAME)
    storage_options = _storage_options()
    try:
        dt = DeltaTable(table_uri, storage_options=storage_options)
        df = dt.to_pandas()
    except Exception:
        logger.exception("read_question_history (OneLake Delta read) failed")
        return []
    if df is None or df.empty:
        return []

    col_query = _find_col(df, "normalized_query")
    col_type = _find_col(df, "type")
    col_freq = _find_col(df, "frequency")
    if not all([col_query, col_type, col_freq]):
        logger.error(
            "read_question_history: could not resolve expected columns on "
            "the real table; found columns=%s", list(df.columns),
        )
        return []

    if user_name is None:
        grouped = (
            df.groupby([col_query, col_type], as_index=False)[col_freq]
            .sum()
            .sort_values(col_freq, ascending=False)
        )
        # Keys MUST be uppercase to match aiApi.ts's MostFrequentQuestion
        # type / Copilot.tsx's direct item.NORMALIZED_QUERY / item.TOTAL_FREQ
        # access - see read_saved_insights's comment for the full context.
        return [
            {
                "NORMALIZED_QUERY": r[col_query],
                "TYPE": r[col_type],
                "TOTAL_FREQ": int(r[col_freq]),
            }
            for r in grouped.head(top_n).to_dict("records")
        ]

    col_user = _find_col(df, "user", "user_name")
    if not col_user:
        logger.error(
            "read_question_history: could not resolve a user column on the "
            "real table; found columns=%s", list(df.columns),
        )
        return []

    filtered = df[df[col_user] == user_name].sort_values(col_freq, ascending=False)
    # Keys MUST be uppercase to match aiApi.ts's FrequentQuestion type /
    # Copilot.tsx's direct item.NORMALIZED_QUERY / item.FREQUENCY access.
    return [
        {
            "NORMALIZED_QUERY": r[col_query],
            "TYPE": r[col_type],
            "FREQUENCY": int(r[col_freq]),
        }
        for r in filtered.head(top_n)[[col_query, col_type, col_freq]].to_dict("records")
    ]


# ---------------------------------------------------------------------------
# SAVED_INSIGHTS - insert (save) + delete
#
# CONFIRMED against the real Delta table (you gave us the actual column
# list directly): insight_id, created_at, created_by, page, title,
# question, verified_query_name, sql_text, tags - all lowercase, and NO
# persona column, the exact same pattern as GENIE_QUESTION_HISTORY above.
# `persona` is kept as a parameter only so callers don't need to change;
# it's intentionally ignored and never written.
# ---------------------------------------------------------------------------

def insert_saved_insight(
    title: str,
    question: str,
    sql_text: str,
    created_by: str,
    persona: str = "",
    page: str = "",
    verified_query_name: str | None = None,
    tags: str = "",
    sql_run_query=None,
) -> int:
    """
    Discovers the real column casing from the table's own schema before
    writing, same reasoning as upsert_question_history's docstring: this
    class of table has proven its casing can differ across
    deployments/recreations, so match whatever's actually there instead
    of hardcoding an assumption.
    """
    del sql_run_query  # intentionally unused - _next_id no longer needs it
    del persona  # intentionally unused - no such column on the real table

    table_uri = _table_uri(Config.SAVED_INSIGHTS_TABLE_NAME)
    storage_options = _storage_options()

    try:
        dt = DeltaTable(table_uri, storage_options=storage_options)
        lower_map = {f.name.lower(): f.name for f in dt.schema().fields}
    except Exception:
        logger.exception("insert_saved_insight: could not open table to discover schema")
        raise

    col_id = lower_map.get("insight_id", "insight_id")
    col_created_at = lower_map.get("created_at", "created_at")
    col_created_by = lower_map.get("created_by", "created_by")
    col_page = lower_map.get("page", "page")
    col_title = lower_map.get("title", "title")
    col_question = lower_map.get("question", "question")
    col_vqn = lower_map.get("verified_query_name", "verified_query_name")
    col_sql_text = lower_map.get("sql_text", "sql_text")
    col_tags = lower_map.get("tags", "tags")

    insight_id = _next_id(Config.SAVED_INSIGHTS_TABLE_NAME, col_id)
    now = _now_utc()

    row = pa.table({
        col_id: pa.array([insight_id], type=pa.int32()),
        col_created_at: pa.array([now], type=pa.timestamp("us", tz="UTC")),
        col_created_by: [created_by],
        col_page: [page],
        col_title: [title],
        col_question: [question],
        col_vqn: [verified_query_name],
        col_sql_text: [sql_text],
        col_tags: [tags],
    })

    try:
        write_deltalake(table_uri, row, mode="append", storage_options=storage_options)
    except Exception:
        logger.exception("insert_saved_insight (OneLake Delta append) failed")
        raise
    return insight_id


def delete_saved_insight(insight_id: int, created_by: str) -> None:
    table_uri = _table_uri(Config.SAVED_INSIGHTS_TABLE_NAME)
    storage_options = _storage_options()
    try:
        dt = DeltaTable(table_uri, storage_options=storage_options)
        lower_map = {f.name.lower(): f.name for f in dt.schema().fields}
        col_id = lower_map.get("insight_id", "insight_id")
        col_created_by = lower_map.get("created_by", "created_by")
        dt.delete(
            predicate=(
                f'"{col_id}" = {int(insight_id)} '
                f"AND \"{col_created_by}\" = '{created_by.replace(chr(39), chr(39) * 2)}'"
            )
        )
    except Exception:
        logger.exception("delete_saved_insight (OneLake Delta delete) failed")
        raise


def read_saved_insights(created_by: str) -> list[dict]:
    """Reads SAVED_INSIGHTS directly over OneLake (via the same `deltalake`
    access used to write it), bypassing the SQL analytics endpoint
    entirely.

    WHY: the SQL endpoint has proven, repeatedly and even immediately
    after a confirmed-successful write, that it cannot see ANY column on
    this table - not INSIGHT_ID, not CREATED_BY, nothing - always
    'Invalid column name', even though T-SQL identifier lookups are
    normally case-insensitive and would have matched regardless of case.
    That rules out a transient schema-sync lag (which would resolve after
    a retry or a wait); this is a persistent gap between what the SQL
    endpoint's cached metadata sees and what's actually in the Delta log,
    for a table whose schema was established by a write from OUTSIDE
    Fabric's own native write path (see this file's module docstring).
    Reading through `deltalake` directly sidesteps that endpoint's
    metadata cache altogether - the exact mechanism that already proved
    it can see this table's real schema correctly, twice (the merge
    error's field list, and the successful save that preceded this read).
    """
    table_uri = _table_uri(Config.SAVED_INSIGHTS_TABLE_NAME)
    storage_options = _storage_options()
    try:
        dt = DeltaTable(table_uri, storage_options=storage_options)
        df = dt.to_pandas()
    except Exception:
        logger.exception("read_saved_insights (OneLake Delta read) failed")
        return []
    if df is None or df.empty:
        return []

    # Column casing has proven to differ between otherwise-identical
    # deployments of this exact code (e.g. a recreated table) - resolve
    # dynamically instead of assuming, so a schema-casing drift shows up as
    # an explicit log message instead of a KeyError crash.
    col_id = _find_col(df, "insight_id")
    col_created_by = _find_col(df, "created_by")
    col_created_at = _find_col(df, "created_at")
    col_title = _find_col(df, "title")
    col_question = _find_col(df, "question")
    col_sql_text = _find_col(df, "sql_text")
    if not all([col_id, col_created_by, col_created_at, col_title, col_question, col_sql_text]):
        logger.error(
            "read_saved_insights: could not resolve expected columns on the "
            "real table; found columns=%s", list(df.columns),
        )
        return []

    df = df[df[col_created_by] == created_by].sort_values(col_created_at, ascending=False)
    # Keys MUST be uppercase: frontend/src/api/aiApi.ts's SavedInsight type
    # and Copilot.tsx access these as item.INSIGHT_ID / item.TITLE /
    # item.QUESTION directly, with no case-insensitive fallback (unlike
    # ShortageAgent.tsx's pick() helper). This mismatch predates this
    # OneLake-read change - the original SQL query aliased everything
    # lowercase too (`INSIGHT_ID AS "insight_id"`) - so it was already
    # broken; matching the frontend's contract exactly fixes it here.
    return [
        {
            "INSIGHT_ID": int(row[col_id]),
            "TITLE": row[col_title],
            "QUESTION": row[col_question],
            "SQL_TEXT": row[col_sql_text],
        }
        for _, row in df.iterrows()
    ]


# ---------------------------------------------------------------------------
# AGENT_REVIEW_LOG - insert-only audit trail.
#
# Used as the write-side workaround for actions that would otherwise need
# an UPDATE on a Lakehouse table Fabric's SQL layer won't allow (e.g.
# "mark shortage resolved" against shortage_alert_dt). The read side
# (copilot_service.shortage_agent_queue) must separately exclude/annotate
# entities that have a matching RESOLVED row here, since shortage_alert_dt
# itself is never mutated.
# ---------------------------------------------------------------------------

def log_agent_review(
    agent_key: str,
    entity_id: str,
    entity_type: str,
    action_taken: str,
    reviewed_by: str,
    notes: str = "",
    sql_run_query=None,
) -> int:
    if sql_run_query is None:
        raise ValueError("sql_run_query is required to compute the next REVIEW_ID")

    table_uri = _table_uri(Config.AGENT_REVIEW_LOG_TABLE_NAME)
    storage_options = _storage_options()
    review_id = _next_id(Config.AGENT_REVIEW_LOG_TABLE_NAME, "REVIEW_ID", sql_run_query)
    now = _now_utc()

    row = pa.table({
        "REVIEW_ID": pa.array([review_id], type=pa.int32()),
        "AGENT_KEY": [agent_key],
        "ENTITY_ID": [entity_id],
        "ENTITY_TYPE": [entity_type],
        "ACTION_TAKEN": [action_taken],
        "NOTES": [notes],
        "REVIEWED_BY": [reviewed_by],
        "REVIEWED_AT": pa.array([now], type=pa.timestamp("us", tz="UTC")),
    }, schema=AGENT_REVIEW_LOG_SCHEMA)

    try:
        write_deltalake(table_uri, row, mode="append", storage_options=storage_options)
    except Exception:
        logger.exception("log_agent_review (OneLake Delta append) failed")
        raise
    return review_id


# ---------------------------------------------------------------------------
# PROCUREMENT_SUMMARY_DT / WORK_ORDER_DT
#
# Both tables are information_mart LAKEHOUSE tables (not Warehouse), so
# they go through the same OneLake Delta path as everything else in this
# file rather than the SQL analytics endpoint (which is read-only for
# Lakehouse tables) or a Warehouse stored procedure (which would require
# SP_CREATE_PO / SP_UPDATE_WORK_ORDER_PRIORITY to exist as real T-SQL
# procs somewhere they don't - these tables live in the Lakehouse, not the
# Warehouse). service.py's create_po / prioritize_work_order call these.
# ---------------------------------------------------------------------------

def _next_po_id(sql_run_query) -> str:
    """Mirrors the Spark version's PO numbering: 'PO' + zero-padded
    (MAX(existing numeric suffix) + 1), starting from 200000 if the table
    is empty. Same caveat as _next_id above - read-then-write, not
    concurrency-safe under simultaneous writers."""
    rows = sql_run_query(f"""
        SELECT MAX(CAST(SUBSTRING(PO_ID, 3, LEN(PO_ID) - 2) AS INT)) AS "max_po_num"
        FROM {Config.FABRIC_READYTOBUILD_DATABASE}.{Config.SCHEMA}.procurement_summary_dt
    """)
    max_po_num = rows[0].get("max_po_num") if rows else None
    if max_po_num is None:
        max_po_num = 200000
    return "PO" + str(int(max_po_num) + 1).rjust(6, "0")


def insert_procurement_po(
    part_number: str,
    qty_ordered: float,
    supplier_id: str,
    receiving_plant_id: str,
    requested_delivery_date,  # datetime.date, or 'YYYY-MM-DD' string
    unit_price: float = 100.00,
    created_by: str = "AI_AGENT",
    sql_run_query=None,
) -> dict:
    """Appends a new PO row directly to procurement_summary_dt in OneLake.
    Mirrors SP_CREATE_PO's logic (see the Spark reference implementation):
    PO_ID auto-numbered, PO_STATUS='PLACED', PO_TYPE='STANDARD',
    QTY_OUTSTANDING starts equal to QTY_ORDERED, TOTAL_VALUE computed,
    IS_LATE=0, QUOTED_LEAD_TIME_DAYS=14, no confirmed/actual delivery date
    yet (those get filled in later by whatever process confirms/receives
    the PO - not this insert)."""
    if sql_run_query is None:
        raise ValueError("sql_run_query is required to compute the next PO_ID")

    if isinstance(requested_delivery_date, str):
        requested_delivery_date = datetime.strptime(
            requested_delivery_date.strip()[:10], "%Y-%m-%d"
        ).date()

    table_uri = _table_uri("procurement_summary_dt")
    storage_options = _storage_options()
    po_id = _next_po_id(sql_run_query)
    qty_ordered = float(qty_ordered)
    total_value = qty_ordered * float(unit_price)
    now = _now_utc()

    row = pa.table({
        "PO_ID": [po_id],
        "PART_NUMBER": [part_number],
        "SUPPLIER_ID": [supplier_id],
        "RECEIVING_PLANT_ID": [receiving_plant_id],
        "PO_STATUS": ["PLACED"],
        "PO_TYPE": ["STANDARD"],
        "QTY_ORDERED": pa.array([qty_ordered], type=pa.float64()),
        "QTY_OUTSTANDING": pa.array([qty_ordered], type=pa.float64()),
        "TOTAL_VALUE": pa.array([total_value], type=pa.float64()),
        "REQUESTED_DELIVERY_DATE": pa.array([requested_delivery_date], type=pa.date32()),
        "CONFIRMED_DELIVERY_DATE": pa.array([None], type=pa.date32()),
        "ACTUAL_DELIVERY_DATE": pa.array([None], type=pa.date32()),
        "IS_LATE": pa.array([0], type=pa.int32()),
        "QUOTED_LEAD_TIME_DAYS": pa.array([14], type=pa.int32()),
        "UPDATED_AT": pa.array([now], type=pa.timestamp("us", tz="UTC")),
    }, schema=PROCUREMENT_SUMMARY_SCHEMA)

    try:
        write_deltalake(table_uri, row, mode="append", storage_options=storage_options)
    except Exception:
        logger.exception("insert_procurement_po (OneLake Delta append) failed")
        raise

    return {
        "status": "SUCCESS",
        "po_id": po_id,
        "part_number": part_number,
        "qty_ordered": qty_ordered,
        "supplier_id": supplier_id,
        "plant_id": receiving_plant_id,
        "requested_delivery_date": str(requested_delivery_date),
        "created_by": created_by,
        "message": f"PO created in {Config.FABRIC_LAKEHOUSE_TABLE_SCHEMA}.procurement_summary_dt",
    }


def update_work_order_priority(
    work_order_id: str,
    new_priority: int = 0,
    new_planned_start_date=None,  # datetime.date / 'YYYY-MM-DD' string / None
    updated_by: str = "PRODUCTION_BOARD",
    sql_run_query=None,
) -> dict:
    """Updates PRIORITY / PLANNED_START_DATE on work_order_dt via a direct
    Delta UPDATE (DeltaTable.update), the write-side counterpart of the
    read-only shortage_alert_dt problem documented above: the SQL
    analytics endpoint rejects UPDATE/DELETE against Lakehouse tables
    outright, so this bypasses it the same way every other write in this
    file does - by talking to the Delta table itself over OneLake instead
    of through the SQL endpoint. Mirrors SP_UPDATE_WORK_ORDER_PRIORITY:
    defaults the target date to the start of the current week if none is
    given, exactly as before."""
    if sql_run_query is None:
        raise ValueError("sql_run_query is required to read the work order's current state")

    current_rows = sql_run_query(
        f"""
        SELECT PRIORITY AS "priority", PLANNED_START_DATE AS "planned_start_date"
        FROM {Config.FABRIC_READYTOBUILD_DATABASE}.{Config.SCHEMA}.work_order_dt
        WHERE WORK_ORDER_ID = ?
        """,
        [work_order_id],
    )
    if not current_rows:
        return {"status": "ERROR", "message": f"Work Order not found: {work_order_id}"}

    previous_priority = current_rows[0].get("priority")
    previous_start_date = current_rows[0].get("planned_start_date")

    if new_planned_start_date is not None:
        if isinstance(new_planned_start_date, str):
            target_date = datetime.strptime(new_planned_start_date.strip()[:10], "%Y-%m-%d").date()
        else:
            target_date = new_planned_start_date
    else:
        today = _now_utc().date()
        target_date = today - timedelta(days=today.weekday())  # Monday of the current week

    table_uri = _table_uri("work_order_dt")
    storage_options = _storage_options()
    now = _now_utc()
    safe_wo_id = work_order_id.replace("'", "''")

    try:
        dt = DeltaTable(table_uri, storage_options=storage_options)
        # now.isoformat() would include a '+00:00' offset suffix, which the
        # CAST(... AS TIMESTAMP) expression evaluator (datafusion, via
        # DeltaTable.update) is not guaranteed to parse - strip it down to
        # a plain 'YYYY-MM-DD HH:MM:SS.ffffff' literal, which it always
        # accepts, and which is unambiguous anyway since the whole table
        # writes in UTC.
        now_literal = now.strftime("%Y-%m-%d %H:%M:%S.%f")
        dt.update(
            predicate=f"WORK_ORDER_ID = '{safe_wo_id}'",
            updates={
                "PRIORITY": str(int(new_priority)),
                "PLANNED_START_DATE": f"CAST('{target_date.isoformat()}' AS DATE)",
                "UPDATED_AT": f"CAST('{now_literal}' AS TIMESTAMP)",
            },
        )
    except Exception:
        logger.exception("update_work_order_priority (OneLake Delta update) failed")
        raise

    return {
        "status": "SUCCESS",
        "work_order_id": work_order_id,
        "previous_priority": previous_priority,
        "new_priority": new_priority,
        "previous_start_date": str(previous_start_date) if previous_start_date is not None else None,
        "new_start_date": target_date.isoformat(),
        "updated_by": updated_by,
        "message": f"Work order updated in {Config.FABRIC_LAKEHOUSE_TABLE_SCHEMA}.work_order_dt",
    }


def update_shortage_alert_status(
    part_number: str,
    new_status: str = "RESOLVED",
    match_status: str = "OPEN",
) -> dict:
    """Direct Delta UPDATE on shortage_alert_dt - the Fabric equivalent of
    Snowflake's:

        UPDATE SHORTAGE_ALERT SET ALERT_STATUS = 'RESOLVED'
        WHERE PART_NUMBER = %s AND ALERT_STATUS = 'OPEN'

    (see app/services/ai_service.py::shortage_agent_mark_resolved on the
    Snowflake side). This is a REAL state mutation on the row(s) matching
    part_number AND the current match_status - not an audit-log workaround
    - so shortage_agent_queue's existing `WHERE sa.ALERT_STATUS = 'OPEN'`
    filter picks the change up on its very next read, the same way it does
    against Snowflake's SHORTAGE_ALERT_VW. Uses DeltaTable.update() to
    bypass the SQL analytics endpoint's read-only restriction on Lakehouse
    tables, the same mechanism as update_work_order_priority above.

    Matches ALL rows for this part_number currently in match_status, same
    as the Snowflake WHERE clause - if the part has shortages logged
    against multiple plants/work orders, all of them flip at once. This is
    inherited from the Snowflake behavior being mirrored, not a Fabric-only
    choice.
    """
    table_uri = _table_uri("shortage_alert_dt")
    storage_options = _storage_options()
    safe_part = part_number.replace("'", "''")
    safe_new_status = new_status.replace("'", "''")
    safe_match_status = match_status.replace("'", "''")

    try:
        dt = DeltaTable(table_uri, storage_options=storage_options)
        result = dt.update(
            predicate=f"PART_NUMBER = '{safe_part}' AND ALERT_STATUS = '{safe_match_status}'",
            updates={"ALERT_STATUS": f"'{safe_new_status}'"},
        )
    except Exception:
        logger.exception("update_shortage_alert_status (OneLake Delta update) failed")
        raise

    # Confirmed against deltalake==1.6.3: DeltaTable.update() always returns
    # a metrics dict with a 'num_updated_rows' key, including when the
    # predicate matches nothing (it's then just 0, not absent - it never
    # returns None and never omits the key). Read it with an explicit
    # None-check, NOT `... or result.get(...)` - that treats a genuine
    # 0 as falsy and silently discards it, which defeats the entire point
    # of surfacing "0 rows matched" to the caller.
    num_updated = None
    if isinstance(result, dict):
        if "num_updated_rows" in result:
            num_updated = result["num_updated_rows"]
        elif "num_rows_updated" in result:  # older/alternate key, just in case
            num_updated = result["num_rows_updated"]

    return {
        "status": "SUCCESS",
        "part_number": part_number,
        "previous_status": match_status,
        "new_status": new_status,
        "rows_updated": num_updated,
        "message": f"{part_number}: {match_status} -> {new_status} in "
                   f"{Config.FABRIC_LAKEHOUSE_TABLE_SCHEMA}.shortage_alert_dt",
    }


# ---------------------------------------------------------------------------
# CTB_RESULT_DT / SHORTAGE_ALERT_DT - refresh (service.refresh_ctb).
#
# These are MERGE upserts, not appends/full overwrites: an overwrite would
# risk silently dropping real columns this file has no visibility into (the
# same class of mistake already made and fixed once for SAVED_INSIGHTS/
# GENIE_QUESTION_HISTORY's schemas). A merge only ever touches the columns
# named in `updates`, and .when_not_matched_insert_all() only fires for
# genuinely new keys - if the real table has additional required columns
# this doesn't know about, that INSERT path will fail loudly with a exact
# field-list error (delta-rs's standard behavior, seen several times this
# session), not silently corrupt the schema.
# ---------------------------------------------------------------------------

def upsert_ctb_results(rows: list[dict]) -> dict:
    """Merges service._compute_ctb_and_shortages()'s per-work-order
    CTB_STATUS/CAN_BUILD_QTY into ctb_result_dt, keyed on WORK_ORDER_ID.
    Only CTB_STATUS and CAN_BUILD_QTY are updated on a match - any other
    real column on this table (whatever Snowflake's version might also
    track) is left untouched. New work orders are inserted with
    PRODUCTION_STATUS='PENDING' (see _compute_ctb_and_shortages's
    docstring for why that specific default)."""
    if not rows:
        return {"status": "SUCCESS", "rows": 0}

    table_uri = _table_uri("ctb_result_dt")
    storage_options = _storage_options()
    source = pa.table({
        "WORK_ORDER_ID": [r["WORK_ORDER_ID"] for r in rows],
        "CTB_STATUS": [r["CTB_STATUS"] for r in rows],
        "CAN_BUILD_QTY": pa.array([float(r["CAN_BUILD_QTY"]) for r in rows], type=pa.float64()),
        "PRODUCTION_STATUS": [r["PRODUCTION_STATUS"] for r in rows],
    })

    try:
        dt = DeltaTable(table_uri, storage_options=storage_options)
        (
            dt.merge(
                source=source,
                predicate="target.WORK_ORDER_ID = source.WORK_ORDER_ID",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update(
                updates={
                    "CTB_STATUS": "source.CTB_STATUS",
                    "CAN_BUILD_QTY": "source.CAN_BUILD_QTY",
                }
            )
            .when_not_matched_insert_all()
            .execute()
        )
    except Exception:
        logger.exception("upsert_ctb_results (OneLake Delta merge) failed")
        raise
    return {"status": "SUCCESS", "rows": len(rows)}


def upsert_shortage_alerts(rows: list[dict]) -> dict:
    """Merges service._compute_ctb_and_shortages()'s freshly-computed
    shortage rows into shortage_alert_dt, keyed on
    (WORK_ORDER_ID, PART_NUMBER). Updates SHORTAGE_QTY and reopens
    ALERT_STATUS='OPEN' on a match; inserts a new OPEN row on no match.

    Deliberately does NOT auto-resolve shortages that no longer appear in
    the freshly computed set. A blanket "not matched by source" close
    would touch every row in the WHOLE table not present in this
    recompute's source - including rows for work orders outside this
    recompute's scope (e.g. anything already excluded upstream in
    _compute_ctb_and_shortages's WHERE clause) - which risks incorrectly
    resolving real, unrelated open shortages. Scoping that correctly needs
    more thought than a plain merge clause can express safely, so it's
    left as a known gap rather than guessed at."""
    if not rows:
        return {"status": "SUCCESS", "rows": 0}

    table_uri = _table_uri("shortage_alert_dt")
    storage_options = _storage_options()
    source = pa.table({
        "WORK_ORDER_ID": [r["WORK_ORDER_ID"] for r in rows],
        "PART_NUMBER": [r["PART_NUMBER"] for r in rows],
        "PLANT_ID": [r["PLANT_ID"] for r in rows],
        "SHORTAGE_QTY": pa.array([float(r["SHORTAGE_QTY"]) for r in rows], type=pa.float64()),
        "ALERT_STATUS": ["OPEN"] * len(rows),
    })

    try:
        dt = DeltaTable(table_uri, storage_options=storage_options)
        (
            dt.merge(
                source=source,
                predicate=(
                    "target.WORK_ORDER_ID = source.WORK_ORDER_ID "
                    "AND target.PART_NUMBER = source.PART_NUMBER"
                ),
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update(
                updates={
                    "SHORTAGE_QTY": "source.SHORTAGE_QTY",
                    "ALERT_STATUS": "source.ALERT_STATUS",
                }
            )
            .when_not_matched_insert_all()
            .execute()
        )
    except Exception:
        logger.exception("upsert_shortage_alerts (OneLake Delta merge) failed")
        raise
    return {"status": "SUCCESS", "rows": len(rows)}