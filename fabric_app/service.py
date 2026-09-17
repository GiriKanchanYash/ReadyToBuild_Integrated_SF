"""
Fabric (Microsoft Fabric Lakehouse/Warehouse, T-SQL) equivalent of
backend/app/services/snowflake_service.py.

Every public function here has the SAME NAME and SAME RETURN SHAPE (list[dict]
of lowercase-ish keys matching the frontend's TypeScript types) as its
Snowflake counterpart, so `routers/ctb.py` can call either module
interchangeably via a data-source factory (see `service_factory.py`).

Translation notes (Snowflake SQL -> Fabric T-SQL):
  - DATE_TRUNC('WEEK', x)         -> DATETRUNC(week, x)
  - x::VARCHAR / x::DATE / etc.  -> CAST(x AS VARCHAR) / CAST(x AS DATE)
  - LIMIT n                      -> TOP (n)           (placed after SELECT)
  - IFF(cond, a, b)               -> IIF(cond, a, b)
  - GREATEST(a, b) / LEAST(a, b) -> IIF(a > b, a, b) / IIF(a < b, a, b)
    (inlined with IIF since not every Fabric Warehouse version has these;
    3-argument versions are nested)
  - CEIL(x)                      -> CEILING(x)
  - a || b                       -> a + b              (string concat)
  - LISTAGG(DISTINCT x, ', ')     -> scalar subquery + STRING_AGG (T-SQL's
    WITHIN GROUP (ORDER BY x)       STRING_AGG has no DISTINCT keyword, so the
                                     DISTINCT is done in a derived table first)
  - ANY_VALUE(x)                  -> MAX(x)             (values are constant
                                                          within the GROUP BY
                                                          key in every case
                                                          this is used, so
                                                          MAX is equivalent)
  - TABLE(GENERATOR(ROWCOUNT=>16))
    + SEQ4()                     -> a `VALUES(...)` literal tally CTE + ROW_NUMBER()
                                     (NOT sys.all_objects / sys.objects - Fabric
                                     Warehouse's distributed engine rejects system
                                     catalog views in a query that also touches
                                     Lakehouse tables: "The query references an
                                     object that is not supported in distributed
                                     processing mode" (error 15816). A literal
                                     VALUES row source has no such restriction.)
  - WITH RECURSIVE cte AS (...)   -> WITH cte AS (...)  (T-SQL never uses the
                                                          RECURSIVE keyword)
  - CALL db.schema.SP_XXX(...)    -> EXEC [wh].[schema].[SP_XXX] ... for
    SP_REFRESH_CTB_DATA only (refresh_ctb) - that proc must exist as a real
    Warehouse stored procedure with matching parameters; creating it is a
    database task, not something this Python file can do. create_po and
    prioritize_work_order do NOT use EXEC/stored procs: PROCUREMENT_SUMMARY
    and WORK_ORDER are Lakehouse tables (information_mart schema), not
    Warehouse tables, so those two write directly to their Delta files over
    OneLake instead - see lakehouse_writer.py.
  - %s parameter placeholders      -> ? (pyodbc's paramstyle)

Everything here executes as read-only Lakehouse queries via `run_query()`
EXCEPT refresh_ctb (Warehouse stored proc via run_exec) and create_po /
prioritize_work_order (direct OneLake Delta writes via lakehouse_writer.py),
which need write
access and go through the Warehouse (`run_exec()`).

NOTE ON VALIDATION: this file was translated from the Snowflake source
without a live Fabric endpoint to test against. The straightforward
functions (filters, KPIs, shortages, BOM lookups) follow standard T-SQL and
should run as-is. The heavier, window-function-based functions
(get_parts_inventory, get_work_order_detail, simulate_prioritization_impact)
are the most likely to need small syntax adjustments once run against real
Fabric data - flagged inline below.
"""

from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

import lakehouse_writer
from config import Config
from db import execute_query, run_df, run_warehouse_df, run_warehouse_non_query

# ---------------------------------------------------------------------------
# Database / schema targets
# ---------------------------------------------------------------------------
DB = Config.FABRIC_READYTOBUILD_DATABASE          # Lakehouse (read-only analytics)
SCHEMA = Config.SCHEMA                             # e.g. "information_mart"
WH_DB = Config.FABRIC_READYTOBUILD_WAREHOUSE_DATABASE  # Warehouse (read + write)
WH_SCHEMA = Config.DEFAULT_SCHEMA                  # e.g. "dbo"


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def _to_records(df: pd.DataFrame | None) -> list[dict]:
    """Convert a query result DataFrame into JSON-safe dicts.

    NaN is replaced with None (as before). +/-Infinity is ALSO replaced
    with None here: Starlette's default JSONResponse calls
    json.dumps(..., allow_nan=False), so a leftover float('inf')/float('-inf')
    value (e.g. from a divide-by-zero or an out-of-range numeric column)
    raises "ValueError: Out of range float values are not JSON compliant"
    and turns the whole endpoint into a 500, even though NaN alone was
    already handled. Replacing inf -> NaN first lets the existing
    pd.notnull() pass catch both in one step.
    """
    if df is None or df.empty:
        return []
    df = df.replace([np.inf, -np.inf], np.nan)
    return df.where(pd.notnull(df), None).to_dict("records")


def run_query(sql: str, params: list | tuple | None = None) -> list[dict]:
    """Read query against the Lakehouse. Mirrors app/db.py's `run_query`."""
    df = execute_query(sql, list(params) if params else None)
    return _to_records(df)


def run_exec(sql: str) -> list[dict]:
    """EXEC a stored procedure against the Warehouse (read+write endpoint).
    Mirrors Snowflake's `CALL proc(...)` usage - the proc is expected to
    return its result via an internal SELECT, same as Snowflake CALL does."""
    df = run_warehouse_df(sql)
    return _to_records(df)


def _lower_keys(row: dict) -> dict:
    return {str(k).lower(): v for k, v in row.items()}


def _sql_escape(value) -> str:
    if value is None:
        return ""
    return str(value).replace("'", "''")


# ---------------------------------------------------------------------------
# Shared helpers (ported 1:1 in logic from snowflake_service.py)
# ---------------------------------------------------------------------------

def _weekly_build_summary(rows: list[dict]) -> dict[str, dict]:
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        key = f"{row['build_week']}|{row['build_stage']}"
        groups[key].append(row)

    summaries: dict[str, dict] = {}
    for key, parts in groups.items():
        planned = float(parts[0].get("planned_qty") or 0)
        can_builds = [float(p.get("can_build") or 0) for p in parts]
        week_can_build = max(0, min(min(can_builds) if can_builds else 0, planned))
        shortage_cnt = sum(1 for p in parts if p.get("status") == "SHORTAGE")
        partial_cnt = sum(1 for p in parts if p.get("status") == "PARTIAL")
        safety_cnt = sum(1 for p in parts if p.get("status") == "USING SAFETY")
        if week_can_build >= planned:
            week_status = "READY"
        elif week_can_build == 0:
            week_status = "BLOCKED"
        else:
            week_status = "PARTIAL"
        summaries[key] = {
            "planned_qty": planned,
            "week_can_build": week_can_build,
            "week_status": week_status,
            "part_count": len(parts),
            "shortage_count": shortage_cnt,
            "partial_count": partial_cnt,
            "safety_count": safety_cnt,
        }
    return summaries


def _wo_where(product: str | None, plant: str | None, week: str | None) -> str:
    clause = " AND wo.WORK_ORDER_STATUS NOT IN ('COMPLETED','CANCELLED','CLOSED')"
    if product:
        clause += f" AND wo.PRODUCT_ID = '{_sql_escape(product)}'"
    if plant:
        clause += f" AND wo.PLANT_ID = '{_sql_escape(plant)}'"
    if week:
        clause += f" AND DATETRUNC(week, wo.PLANNED_START_DATE) = '{_sql_escape(week)}'"
    return clause


# ---------------------------------------------------------------------------
# Filters / KPIs
# ---------------------------------------------------------------------------

def get_filter_options() -> dict:
    products = run_query(f"""
        SELECT DISTINCT PRODUCT_ID FROM {DB}.{SCHEMA}.work_order_dt
        WHERE PRODUCT_ID IS NOT NULL ORDER BY PRODUCT_ID
    """)
    plants = run_query(f"""
        WITH wo_plants AS (
            SELECT DISTINCT PLANT_ID
            FROM {DB}.{SCHEMA}.work_order_dt
            WHERE PLANT_ID IS NOT NULL
        ),
        plant_names AS (
            SELECT
                PLANT_ID,
                MAX(NULLIF(TRIM(PLANT_NAME), '')) AS PLANT_NAME
            FROM {DB}.{SCHEMA}.plant_summary_dt
            GROUP BY PLANT_ID
        )
        SELECT
            wp.PLANT_ID AS "id",
            COALESCE(pn.PLANT_NAME, wp.PLANT_ID) AS "name"
        FROM wo_plants wp
        LEFT JOIN plant_names pn ON pn.PLANT_ID = wp.PLANT_ID
        ORDER BY wp.PLANT_ID
    """)
    weeks = run_query(f"""
        SELECT DISTINCT CONVERT(varchar(10), DATETRUNC(week, PLANNED_START_DATE), 120) AS WEEK_START
        FROM {DB}.{SCHEMA}.work_order_dt
        WHERE PLANNED_START_DATE IS NOT NULL ORDER BY 1
    """)
    return {
        "products": [r["PRODUCT_ID"] for r in products],
        "plants": plants,
        "weeks": [r["WEEK_START"][:10] for r in weeks],
    }


def get_kpis(product: str | None = None, plant: str | None = None, week: str | None = None) -> dict:
    where = _wo_where(product, plant, week)
    rows = run_query(f"""
    WITH wo_ctb AS (
        SELECT wo.WORK_ORDER_ID, wo.PRODUCT_ID, wo.PLANNED_QTY,
               ctb.CTB_STATUS, ctb.CAN_BUILD_QTY
        FROM {DB}.{SCHEMA}.work_order_dt wo
        LEFT JOIN {DB}.{SCHEMA}.ctb_result_dt ctb ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
        WHERE 1=1 {where}
    ),
    shortage_parts AS (
        SELECT COUNT(DISTINCT p.DEMAND_PART_NUMBER) AS CNT
        FROM {DB}.{SCHEMA}.pegging_dt p
        JOIN {DB}.{SCHEMA}.work_order_dt wo ON p.WORK_ORDER_ID = wo.WORK_ORDER_ID
        WHERE p.SHORTAGE_QTY > 0 {where}
    )
    SELECT
        COUNT(DISTINCT wc.WORK_ORDER_ID)                                     AS "total_work_orders",
        COALESCE(SUM(CASE WHEN wc.CTB_STATUS='READY'   THEN 1 ELSE 0 END),0) AS "ready_count",
        COALESCE(SUM(CASE WHEN wc.CTB_STATUS='PARTIAL' THEN 1 ELSE 0 END),0) AS "partial_count",
        COALESCE(SUM(CASE WHEN wc.CTB_STATUS='BLOCKED' THEN 1 ELSE 0 END),0) AS "blocked_count",
        COALESCE(SUM(wc.CAN_BUILD_QTY),0)                                     AS "can_build_units",
        (SELECT CNT FROM shortage_parts)                                      AS "shortage_parts"
    FROM wo_ctb wc
    """)
    return rows[0] if rows else {}


def get_ctb_status_distribution(product: str | None = None, plant: str | None = None, week: str | None = None) -> list[dict]:
    where = _wo_where(product, plant, week)
    return run_query(f"""
        SELECT ctb.CTB_STATUS AS "status", COUNT(*) AS "count"
        FROM {DB}.{SCHEMA}.ctb_result_dt ctb
        JOIN {DB}.{SCHEMA}.work_order_dt wo ON ctb.WORK_ORDER_ID = wo.WORK_ORDER_ID
        WHERE 1=1 {where}
        GROUP BY ctb.CTB_STATUS ORDER BY COUNT(*) DESC
    """)


def get_top_shortages(product: str | None = None, plant: str | None = None, week: str | None = None, limit: int = 10) -> list[dict]:
    where = _wo_where(product, plant, week)
    return run_query(f"""
        SELECT TOP ({int(limit)})
               p.DEMAND_PART_NUMBER AS "part_number",
               SUM(p.SHORTAGE_QTY) AS "shortage_qty",
               COUNT(DISTINCT p.WORK_ORDER_ID) AS "affected_orders"
        FROM {DB}.{SCHEMA}.pegging_dt p
        JOIN {DB}.{SCHEMA}.work_order_dt wo ON p.WORK_ORDER_ID = wo.WORK_ORDER_ID
        WHERE p.SHORTAGE_QTY > 0 {where}
        GROUP BY p.DEMAND_PART_NUMBER
        ORDER BY SUM(p.SHORTAGE_QTY) DESC
    """)


def get_ctb_by_priority(product: str | None = None, plant: str | None = None, week: str | None = None) -> list[dict]:
    where = _wo_where(product, plant, week)
    return run_query(f"""
        SELECT wo.PRIORITY AS "PRIORITY",
               SUM(CASE WHEN ctb.CTB_STATUS='READY'   THEN 1 ELSE 0 END) AS "ready",
               SUM(CASE WHEN ctb.CTB_STATUS='PARTIAL' THEN 1 ELSE 0 END) AS "partial",
               SUM(CASE WHEN ctb.CTB_STATUS='BLOCKED' THEN 1 ELSE 0 END) AS "blocked"
        FROM {DB}.{SCHEMA}.work_order_dt wo
        JOIN {DB}.{SCHEMA}.ctb_result_dt ctb ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
        WHERE 1=1 {where}
        GROUP BY wo.PRIORITY ORDER BY wo.PRIORITY
    """)


# ---------------------------------------------------------------------------
# Parts inventory (16-week rolling balance grid)
# ---------------------------------------------------------------------------

def get_parts_inventory(
    product: str | None = None,
    plant: str | None = None,
    week: str | None = None,
    include_deliveries: bool = True,
    search: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """
    Next 16 weeks running ending-balance per part (demand from open WOs).
    suggest_reorder marks the calendar week to place a PO: first projected
    risk week minus lead time. Risk week = earliest week where balance goes
    below safety stock (if SS > 0) or below zero.
    """
    where = _wo_where(product, plant, week)
    where_demand = _wo_where(product, plant, None)

    plant_inv_filter = f" AND PLANT_ID = '{_sql_escape(plant)}'" if plant else ""
    plant_asn_filter = f" AND RECEIVING_PLANT_ID = '{_sql_escape(plant)}'" if plant else ""

    safe_search = _sql_escape(search).strip() if search else ""
    search_filter = ""
    if safe_search:
        search_filter = (
            f" AND (pl.PART_NUMBER LIKE '%{safe_search}%' "
            f"OR COALESCE(pl.DESCRIPTION,'') LIKE '%{safe_search}%')"
        )

    inc_expr = "COALESCE(dlv.DELIVERY_QTY, 0)" if include_deliveries else "0"

    return run_query(f"""
        WITH weeks_base AS (
            SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS week_idx
            FROM (VALUES (0),(0),(0),(0),(0),(0),(0),(0),
                         (0),(0),(0),(0),(0),(0),(0),(0)) AS tally(n)
        ),
        weeks AS (
            SELECT
                week_idx,
                CAST(DATEADD(week, week_idx, DATETRUNC(week, CAST(GETDATE() AS DATE))) AS DATE) AS week_start
            FROM weeks_base
        ),
        part_desc AS (
            SELECT
                PART_NUMBER,
                MAX(NULLIF(TRIM(PART_DESCRIPTION), '')) AS PART_DESCRIPTION
            FROM {DB}.{SCHEMA}.inventory_dt
            GROUP BY PART_NUMBER
        ),
        parts_list AS (
            SELECT TOP ({int(limit)}) PART_NUMBER, DESCRIPTION
            FROM (
                SELECT
                    b.CHILD_PART_NUMBER AS PART_NUMBER,
                    COALESCE(
                        MAX(NULLIF(TRIM(b.CHILD_PART_DESCRIPTION), '')),
                        MAX(pd.PART_DESCRIPTION),
                        b.CHILD_PART_NUMBER
                    ) AS DESCRIPTION
                FROM {DB}.{SCHEMA}.work_order_dt wo
                JOIN {DB}.{SCHEMA}.bom_dt b
                  ON wo.PRODUCT_ID = b.PARENT_PART_NUMBER
                 AND b.BOM_STATUS = 'ACTIVE'
                LEFT JOIN part_desc pd
                  ON pd.PART_NUMBER = b.CHILD_PART_NUMBER
                WHERE 1=1 {where}
                GROUP BY b.CHILD_PART_NUMBER
            ) x
            ORDER BY PART_NUMBER
        ),
        inv AS (
            SELECT
                PART_NUMBER,
                SUM(COALESCE(QTY_AVAILABLE, 0)) AS QTY_AVAILABLE,
                SUM(COALESCE(SAFETY_STOCK, 0)) AS SAFETY_STOCK
            FROM {DB}.{SCHEMA}.inventory_dt
            WHERE 1=1 {plant_inv_filter}
            GROUP BY PART_NUMBER
        ),
        demand_by_week AS (
            SELECT
                b.CHILD_PART_NUMBER AS PART_NUMBER,
                CAST(DATETRUNC(week, wo.PLANNED_START_DATE) AS DATE) AS WEEK_START,
                SUM(wo.PLANNED_QTY * b.QTY_PER_ASSEMBLY) AS DEMAND_QTY
            FROM {DB}.{SCHEMA}.work_order_dt wo
            JOIN {DB}.{SCHEMA}.bom_dt b
              ON wo.PRODUCT_ID = b.PARENT_PART_NUMBER
             AND b.BOM_STATUS = 'ACTIVE'
            WHERE wo.PLANNED_START_DATE IS NOT NULL
              AND wo.WORK_ORDER_STATUS NOT IN ('COMPLETED', 'CANCELLED', 'CLOSED')
              {where_demand}
            GROUP BY b.CHILD_PART_NUMBER, CAST(DATETRUNC(week, wo.PLANNED_START_DATE) AS DATE)
        ),
        delivery_by_week AS (
            SELECT
                PART_NUMBER,
                CAST(DATETRUNC(week, EXPECTED_ARRIVAL_DATE) AS DATE) AS WEEK_START,
                SUM(QTY_SHIPPED) AS DELIVERY_QTY
            FROM {DB}.{SCHEMA}.asn_in_transit_dt
            WHERE ASN_STATUS IN ('SHIPPED', 'IN_TRANSIT')
              AND EXPECTED_ARRIVAL_DATE IS NOT NULL
              AND QTY_SHIPPED > 0
              {plant_asn_filter}
            GROUP BY PART_NUMBER, CAST(DATETRUNC(week, EXPECTED_ARRIVAL_DATE) AS DATE)
        ),
        grid AS (
            SELECT
                pl.PART_NUMBER,
                pl.DESCRIPTION,
                COALESCE(i.QTY_AVAILABLE, 0) AS INV,
                COALESCE(i.SAFETY_STOCK, 0) AS SS,
                w.week_idx,
                w.week_start,
                COALESCE(dem.DEMAND_QTY, 0) AS DEMAND_QTY,
                COALESCE(dlv.DELIVERY_QTY, 0) AS DELIVERY_QTY,
                {inc_expr} AS INC_QTY
            FROM parts_list pl
            LEFT JOIN inv i ON pl.PART_NUMBER = i.PART_NUMBER
            CROSS JOIN weeks w
            LEFT JOIN demand_by_week dem ON dem.PART_NUMBER = pl.PART_NUMBER AND dem.WEEK_START = w.week_start
            LEFT JOIN delivery_by_week dlv ON dlv.PART_NUMBER = pl.PART_NUMBER AND dlv.WEEK_START = w.week_start
            WHERE 1=1 {search_filter}
        ),
        bal AS (
            SELECT
                PART_NUMBER,
                DESCRIPTION,
                INV,
                SS,
                week_idx,
                week_start,
                DEMAND_QTY,
                INC_QTY AS INCOMING_QTY,
                (INV + SUM(INC_QTY - DEMAND_QTY) OVER (
                    PARTITION BY PART_NUMBER ORDER BY week_idx
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )) AS BALANCE,
                CASE WHEN INC_QTY > 0 THEN 1 ELSE 0 END AS HAS_DELIVERY
            FROM grid
        ),
        part_lead AS (
            SELECT
                PART_NUMBER,
                CAST(COALESCE(MAX(AVG_LEAD_TIME_DAYS), 7) AS FLOAT) AS LEAD_DAYS
            FROM {DB}.{SCHEMA}.part_supply_dt
            GROUP BY PART_NUMBER
        ),
        risk_weeks AS (
            SELECT
                PART_NUMBER,
                MIN(week_idx) AS risk_week_idx
            FROM bal
            WHERE (BALANCE < SS AND SS > 0) OR BALANCE < 0
            GROUP BY PART_NUMBER
        ),
        reorder_plan AS (
            SELECT
                rw.PART_NUMBER,
                rw.risk_week_idx,
                IIF(
                    (rw.risk_week_idx - CAST(CEILING(COALESCE(pl.LEAD_DAYS, 7) / 7.0) AS INT)) > 0,
                    (rw.risk_week_idx - CAST(CEILING(COALESCE(pl.LEAD_DAYS, 7) / 7.0) AS INT)),
                    0
                ) AS place_order_week_idx
            FROM risk_weeks rw
            LEFT JOIN part_lead pl ON pl.PART_NUMBER = rw.PART_NUMBER
        )
        SELECT
            bal.PART_NUMBER AS "part_number",
            COALESCE(bal.DESCRIPTION, '') AS "description",
            bal.INV AS "inv",
            bal.SS AS "ss",
            FORMAT(bal.week_start, 'MM/dd') AS "week_label",
            CONVERT(varchar(10), bal.week_start, 120) AS "week_start",
            bal.BALANCE AS "balance",
            bal.HAS_DELIVERY AS "has_delivery",
            CASE WHEN bal.BALANCE >= 0 AND bal.BALANCE < bal.SS THEN 1 ELSE 0 END AS "using_safety",
            CASE
                WHEN rp.PART_NUMBER IS NOT NULL AND bal.week_idx = rp.place_order_week_idx THEN 1
                ELSE 0
            END AS "suggest_reorder"
        FROM bal
        LEFT JOIN reorder_plan rp ON rp.PART_NUMBER = bal.PART_NUMBER
        ORDER BY bal.PART_NUMBER, bal.week_start
    """)


# ---------------------------------------------------------------------------
# Work orders
# ---------------------------------------------------------------------------

def get_work_orders(product: str | None = None, plant: str | None = None, week: str | None = None) -> list[dict]:
    where = _wo_where(product, plant, week)
    return run_query(f"""
        SELECT wo.WORK_ORDER_ID, wo.PRODUCT_ID, wo.PLANT_ID,
               wo.PLANNED_QTY, wo.PRIORITY, wo.WORK_ORDER_STATUS,
               CONVERT(varchar(10), wo.PLANNED_START_DATE, 120) AS "planned_start_date",
               COALESCE(ctb.CTB_STATUS,'UNKNOWN')  AS "ctb_status",
               COALESCE(ctb.CAN_BUILD_QTY,0)       AS "can_build_qty",
               COALESCE(ctb.REQUESTED_QTY,0)       AS "requested_qty"
        FROM {DB}.{SCHEMA}.work_order_dt wo
        LEFT JOIN {DB}.{SCHEMA}.ctb_result_dt ctb ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
        WHERE 1=1 {where}
        ORDER BY wo.PRIORITY, wo.PLANNED_START_DATE
    """)


def get_work_order_detail(wo_id: str) -> dict:
    rows = run_query(f"""
        SELECT wo.WORK_ORDER_ID, wo.PRODUCT_ID, wo.PLANT_ID,
               wo.WORK_ORDER_TYPE, wo.WORK_ORDER_STATUS,
               wo.PLANNED_QTY, wo.PRIORITY,
               CONVERT(varchar(10), wo.PLANNED_START_DATE, 120) AS "planned_start_date",
               CONVERT(varchar(10), wo.PLANNED_END_DATE, 120)   AS "planned_end_date",
               COALESCE(ctb.CTB_STATUS,'UNKNOWN') AS "ctb_status",
               COALESCE(ctb.PRODUCTION_STATUS,'PENDING') AS "production_status",
               COALESCE(ctb.CAN_BUILD_QTY,0)      AS "can_build_qty"
        FROM {DB}.{SCHEMA}.work_order_dt wo
        LEFT JOIN {DB}.{SCHEMA}.ctb_result_dt ctb ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
        WHERE wo.WORK_ORDER_ID = ?
    """, (wo_id,))
    if not rows:
        return {}
    wo = rows[0]
    planned_qty = wo.get("PLANNED_QTY", 0) or 0

    parts = run_query(f"""
        SELECT b.CHILD_PART_NUMBER AS "part_number",
               b.QTY_PER_ASSEMBLY AS "qty_per_assembly",
               b.QTY_PER_ASSEMBLY * {planned_qty} AS "required_qty",
               COALESCE(inv.qty_available, 0)  AS "available_qty",
               COALESCE(inv.safety_stock, 0)   AS "safety_stock",
               COALESCE(del.incoming_qty, 0)   AS "incoming_qty",
               CASE
                 WHEN COALESCE(inv.qty_available,0) >= b.QTY_PER_ASSEMBLY * {planned_qty} THEN 'OK'
                 WHEN COALESCE(inv.qty_available,0) + COALESCE(del.incoming_qty,0) >= b.QTY_PER_ASSEMBLY * {planned_qty} THEN 'WITH_DELIVERY'
                 ELSE 'SHORT'
               END AS "part_status"
        FROM {DB}.{SCHEMA}.bom_dt b
        LEFT JOIN (SELECT PART_NUMBER, SUM(QTY_AVAILABLE) AS qty_available, SUM(SAFETY_STOCK) AS safety_stock
                   FROM {DB}.{SCHEMA}.inventory_dt GROUP BY PART_NUMBER) inv
          ON inv.PART_NUMBER = b.CHILD_PART_NUMBER
        LEFT JOIN (SELECT PART_NUMBER, SUM(QTY_SHIPPED) AS incoming_qty
                   FROM {DB}.{SCHEMA}.asn_in_transit_dt
                   WHERE ASN_STATUS IN ('SHIPPED','IN_TRANSIT') GROUP BY PART_NUMBER) del
          ON del.PART_NUMBER = b.CHILD_PART_NUMBER
        WHERE b.PARENT_PART_NUMBER = ? AND b.BOM_STATUS = 'ACTIVE'
        ORDER BY "part_status" DESC, "required_qty" DESC
    """, (wo.get("PRODUCT_ID", ""),))

    # NOTE: this weekly build-sequence query is the most complex translation
    # in the file. Logic mirrors the Snowflake version 1:1; validate against
    # real Fabric data before relying on it in production.
    weekly_rows = run_query(f"""
        WITH ts AS (
            SELECT
                w.WORK_ORDER_ID,
                w.PRODUCT_ID,
                w.PLANT_ID,
                w.PRIORITY,
                w.PLANNED_QTY,
                w.PLANNED_START_DATE
            FROM {DB}.{SCHEMA}.work_order_dt w
            WHERE w.WORK_ORDER_ID = ?
        ),
        seq_max AS (
            SELECT COALESCE(MAX(bs.BUILD_WEEK), 4) AS MAX_WEEK
            FROM {DB}.{SCHEMA}.build_sequence_dt bs
            CROSS JOIN ts t
            WHERE bs.PRODUCT_ID = t.PRODUCT_ID
        ),
        seq_rows AS (
            SELECT
                bs.BUILD_WEEK,
                bs.BUILD_STAGE,
                bs.PART_NUMBER,
                COALESCE(be.EXTENDED_QTY, bs.QTY_REQUIRED) AS QTY_PER_UNIT,
                MAX(COALESCE(bs.USING_SAFETY_STOCK, 0)) AS USING_SAFETY_STOCK
            FROM {DB}.{SCHEMA}.build_sequence_dt bs
            CROSS JOIN ts t
            LEFT JOIN {DB}.{SCHEMA}.bom_explode_dt be
              ON be.ROOT_PRODUCT = t.PRODUCT_ID
             AND be.CHILD_PART_NUMBER = bs.PART_NUMBER
            WHERE bs.PRODUCT_ID = t.PRODUCT_ID
            GROUP BY bs.BUILD_WEEK, bs.BUILD_STAGE, bs.PART_NUMBER,
                     COALESCE(be.EXTENDED_QTY, bs.QTY_REQUIRED)
        ),
        missing_bom AS (
            SELECT
                IIF(be.BOM_LEVEL < sm.MAX_WEEK, be.BOM_LEVEL, sm.MAX_WEEK) AS BUILD_WEEK,
                CASE be.BOM_LEVEL
                    WHEN 1 THEN 'Frame & Foundation'
                    WHEN 2 THEN 'Power Installation'
                    WHEN 3 THEN 'Systems Integration'
                    ELSE 'Final Assembly'
                END AS BUILD_STAGE,
                be.CHILD_PART_NUMBER AS PART_NUMBER,
                be.EXTENDED_QTY AS QTY_PER_UNIT,
                0 AS USING_SAFETY_STOCK
            FROM {DB}.{SCHEMA}.bom_explode_dt be
            CROSS JOIN ts t
            CROSS JOIN seq_max sm
            WHERE be.ROOT_PRODUCT = t.PRODUCT_ID
              AND NOT EXISTS (
                  SELECT 1
                  FROM seq_rows sr
                  WHERE sr.PART_NUMBER = be.CHILD_PART_NUMBER
              )
        ),
        parts AS (
            SELECT * FROM seq_rows
            UNION ALL
            SELECT * FROM missing_bom
        ),
        inv AS (
            SELECT
                PART_NUMBER,
                SUM(COALESCE(QTY_AVAILABLE, 0)) AS QTY_AVAILABLE,
                SUM(COALESCE(SAFETY_STOCK, 0)) AS SAFETY_STOCK
            FROM {DB}.{SCHEMA}.inventory_dt
            GROUP BY PART_NUMBER
        ),
        asn_before AS (
            SELECT
                a.PART_NUMBER,
                SUM(COALESCE(a.QTY_SHIPPED, 0)) AS INCOMING_QTY
            FROM {DB}.{SCHEMA}.asn_in_transit_dt a
            CROSS JOIN ts t
            WHERE a.ASN_STATUS IN ('SHIPPED', 'IN_TRANSIT')
              AND a.EXPECTED_ARRIVAL_DATE < DATETRUNC(week, t.PLANNED_START_DATE)
            GROUP BY a.PART_NUMBER
        ),
        ranked_wo AS (
            SELECT
                wo.WORK_ORDER_ID,
                wo.PRODUCT_ID,
                wo.PLANNED_QTY,
                ROW_NUMBER() OVER (
                    ORDER BY DATETRUNC(week, wo.PLANNED_START_DATE),
                             wo.PRIORITY,
                             wo.WORK_ORDER_ID
                ) AS WO_RANK
            FROM {DB}.{SCHEMA}.work_order_dt wo
            LEFT JOIN {DB}.{SCHEMA}.ctb_result_dt ctb
              ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
            WHERE COALESCE(ctb.CTB_STATUS, 'PENDING') NOT IN ('COMPLETE', 'CANCELLED')
        ),
        hp AS (
            SELECT
                bs.PART_NUMBER,
                SUM(bs.QTY_REQUIRED * hp_wo.PLANNED_QTY) AS HP_USED
            FROM ranked_wo hp_wo
            JOIN {DB}.{SCHEMA}.build_sequence_dt bs
              ON bs.PRODUCT_ID = hp_wo.PRODUCT_ID
            CROSS JOIN ts t
            CROSS JOIN ranked_wo cur
            WHERE cur.WORK_ORDER_ID = t.WORK_ORDER_ID
              AND hp_wo.WO_RANK < cur.WO_RANK
            GROUP BY bs.PART_NUMBER
        ),
        part_desc AS (
            SELECT PART_NUMBER, MAX(PART_DESCRIPTION) AS PART_DESCRIPTION
            FROM {DB}.{SCHEMA}.inventory_dt
            GROUP BY PART_NUMBER
        )
        SELECT
            p.BUILD_WEEK AS "build_week",
            p.BUILD_STAGE AS "build_stage",
            p.PART_NUMBER AS "part_number",
            COALESCE(pd.PART_DESCRIPTION, p.PART_NUMBER) AS "description",
            p.QTY_PER_UNIT AS "qty_per_unit",
            ts.PLANNED_QTY AS "planned_qty",
            COALESCE(i.QTY_AVAILABLE, 0) AS "on_hand",
            COALESCE(i.SAFETY_STOCK, 0) AS "safety_stock",
            COALESCE(ab.INCOMING_QTY, 0) AS "deliveries",
            COALESCE(hp.HP_USED, 0) AS "hp_used",
            IIF(COALESCE(i.QTY_AVAILABLE, 0) > 0, COALESCE(i.QTY_AVAILABLE, 0), 0) AS "eff_avail_qty",
            p.QTY_PER_UNIT * ts.PLANNED_QTY AS "qty_required",
            CASE
                WHEN p.QTY_PER_UNIT > 0 THEN FLOOR(
                    IIF(COALESCE(i.QTY_AVAILABLE, 0) > 0, COALESCE(i.QTY_AVAILABLE, 0), 0)
                    / NULLIF(p.QTY_PER_UNIT, 0)
                )
                ELSE 0
            END AS "can_build",
            CASE
                WHEN COALESCE(i.QTY_AVAILABLE, 0) >= p.QTY_PER_UNIT * ts.PLANNED_QTY THEN 'OK'
                WHEN COALESCE(i.QTY_AVAILABLE, 0) + COALESCE(ab.INCOMING_QTY, 0)
                     >= p.QTY_PER_UNIT * ts.PLANNED_QTY THEN 'WITH_DELIVERY'
                WHEN p.QTY_PER_UNIT > 0
                     AND FLOOR(IIF(COALESCE(i.QTY_AVAILABLE, 0) > 0, COALESCE(i.QTY_AVAILABLE, 0), 0) / p.QTY_PER_UNIT)
                         >= ts.PLANNED_QTY THEN 'OK'
                WHEN p.QTY_PER_UNIT > 0
                     AND FLOOR(IIF(COALESCE(i.QTY_AVAILABLE, 0) > 0, COALESCE(i.QTY_AVAILABLE, 0), 0) / p.QTY_PER_UNIT) > 0
                     AND FLOOR(IIF(COALESCE(i.QTY_AVAILABLE, 0) > 0, COALESCE(i.QTY_AVAILABLE, 0), 0) / p.QTY_PER_UNIT)
                         < ts.PLANNED_QTY THEN 'PARTIAL'
                WHEN COALESCE(i.QTY_AVAILABLE, 0) + COALESCE(i.SAFETY_STOCK, 0)
                     >= p.QTY_PER_UNIT * ts.PLANNED_QTY
                     AND p.QTY_PER_UNIT > 0
                     AND FLOOR(IIF(COALESCE(i.QTY_AVAILABLE, 0) > 0, COALESCE(i.QTY_AVAILABLE, 0), 0) / p.QTY_PER_UNIT) > 0
                     THEN 'USING SAFETY'
                ELSE 'SHORTAGE'
            END AS "status"
        FROM parts p
        CROSS JOIN ts
        LEFT JOIN inv i ON i.PART_NUMBER = p.PART_NUMBER
        LEFT JOIN asn_before ab ON ab.PART_NUMBER = p.PART_NUMBER
        LEFT JOIN hp ON hp.PART_NUMBER = p.PART_NUMBER
        LEFT JOIN part_desc pd ON pd.PART_NUMBER = p.PART_NUMBER
        ORDER BY p.BUILD_WEEK, p.PART_NUMBER
    """, (wo_id,))

    weekly_rows = [_lower_keys(r) for r in weekly_rows]
    wo["parts"] = parts
    wo["weekly_build_sequence"] = weekly_rows
    wo["weekly_build_summary"] = _weekly_build_summary(weekly_rows)
    return wo


# ---------------------------------------------------------------------------
# BOM explosion / stats / lineage / where-used
# ---------------------------------------------------------------------------

def get_bom_explosion(product_id: str) -> list[dict]:
    return run_query(f"""
        SELECT ROOT_PRODUCT, PARENT_PART_NUMBER, CHILD_PART_NUMBER,
               CHILD_PART_DESCRIPTION, CHILD_PART_TYPE,
               QTY_PER_ASSEMBLY, EXTENDED_QTY, BOM_LEVEL, BOM_PATH
        FROM {DB}.{SCHEMA}.bom_explode_dt
        WHERE ROOT_PRODUCT = ?
        ORDER BY BOM_LEVEL, PARENT_PART_NUMBER, CHILD_PART_NUMBER
    """, (product_id,))


def get_bom_stats(product_id: str, work_order_id: str | None = None) -> dict:
    """
    BOM Explosion KPIs using BOM_EXPLODE_VW extended quantities x planned WO
    qty so low-inventory counts match scaled demand. work_order_id optional
    (defaults to x1 FG). Returns: max_depth, total_parts, low_inv_cnt,
    total_qty_needed.
    """
    safe_product = _sql_escape(product_id)
    safe_wo = _sql_escape(work_order_id) if work_order_id else ""
    rows = run_query(f"""
        WITH wo_mult AS (
            SELECT
                CASE
                    WHEN NULLIF(TRIM('{safe_wo}'), '') IS NULL THEN 1
                    ELSE COALESCE(
                        (
                            SELECT TOP (1) PLANNED_QTY
                            FROM {DB}.{SCHEMA}.work_order_dt
                            WHERE WORK_ORDER_ID = '{safe_wo}'
                              AND PRODUCT_ID = '{safe_product}'
                        ),
                        1
                    )
                END AS PQ,
                CASE
                    WHEN NULLIF(TRIM('{safe_wo}'), '') IS NULL THEN NULL
                    ELSE (
                        SELECT TOP (1) PLANT_ID
                        FROM {DB}.{SCHEMA}.work_order_dt
                        WHERE WORK_ORDER_ID = '{safe_wo}'
                          AND PRODUCT_ID = '{safe_product}'
                    )
                END AS PLANT_ID
        ),
        exploded AS (
            SELECT
                CHILD_PART_NUMBER,
                MAX(BOM_LEVEL) AS [LEVEL],
                SUM(EXTENDED_QTY) AS EXT_PER_FG
            FROM {DB}.{SCHEMA}.bom_explode_dt
            WHERE ROOT_PRODUCT = '{safe_product}'
            GROUP BY CHILD_PART_NUMBER
        ),
        bom_parts AS (
            SELECT
                e.CHILD_PART_NUMBER,
                e.[LEVEL],
                e.EXT_PER_FG * w.PQ AS QTY_NEEDED
            FROM exploded e
            CROSS JOIN wo_mult w
        ),
        inv_agg AS (
            SELECT
                i.PART_NUMBER,
                SUM(COALESCE(i.QTY_AVAILABLE, 0)) AS QTY_AVAILABLE
            FROM {DB}.{SCHEMA}.inventory_dt i
            CROSS JOIN wo_mult wm
            WHERE wm.PLANT_ID IS NULL OR i.PLANT_ID = wm.PLANT_ID
            GROUP BY i.PART_NUMBER
        ),
        hp_alloc AS (
            SELECT
                p.DEMAND_PART_NUMBER AS PART_NUMBER,
                SUM(COALESCE(p.SUPPLY_QTY, 0)) AS HP_ALLOCATED
            FROM {DB}.{SCHEMA}.pegging_dt p
            JOIN {DB}.{SCHEMA}.work_order_dt wo ON p.WORK_ORDER_ID = wo.WORK_ORDER_ID
            WHERE wo.PRIORITY < (
                SELECT COALESCE(MIN(w2.PRIORITY), 999)
                FROM {DB}.{SCHEMA}.work_order_dt w2
                WHERE w2.PRODUCT_ID = '{safe_product}'
            )
              AND (
                  NULLIF(TRIM('{safe_wo}'), '') IS NULL
                  OR wo.PLANT_ID = (
                      SELECT TOP (1) w0.PLANT_ID
                      FROM {DB}.{SCHEMA}.work_order_dt w0
                      WHERE w0.WORK_ORDER_ID = '{safe_wo}'
                        AND w0.PRODUCT_ID = '{safe_product}'
                  )
              )
            GROUP BY p.DEMAND_PART_NUMBER
        ),
        inv_check AS (
            SELECT
                bp.CHILD_PART_NUMBER,
                bp.[LEVEL],
                bp.QTY_NEEDED,
                IIF(
                    (COALESCE(i.QTY_AVAILABLE, 0) - COALESCE(h.HP_ALLOCATED, 0)) > 0,
                    (COALESCE(i.QTY_AVAILABLE, 0) - COALESCE(h.HP_ALLOCATED, 0)),
                    0
                ) AS EFF_AVAILABLE
            FROM bom_parts bp
            LEFT JOIN inv_agg i ON bp.CHILD_PART_NUMBER = i.PART_NUMBER
            LEFT JOIN hp_alloc h ON bp.CHILD_PART_NUMBER = h.PART_NUMBER
        )
        SELECT
            COALESCE(MAX([LEVEL]), 0) AS "max_depth",
            COALESCE(COUNT(DISTINCT CHILD_PART_NUMBER), 0) AS "total_parts",
            COALESCE(SUM(CASE WHEN EFF_AVAILABLE < QTY_NEEDED THEN 1 ELSE 0 END), 0) AS "low_inv_cnt",
            COALESCE(SUM(QTY_NEEDED), 0) AS "total_qty_needed"
        FROM inv_check
    """)
    return rows[0] if rows else {}


def get_bom_lineage(product_id: str, work_order_id: str | None = None) -> list[dict]:
    """
    Lineage for BOM Explosion diagram. Includes BUILD_NEED_QTY (extended BOM
    qty x planned WO) so shortage coloring matches KPI / constraint scale.
    work_order_id optional (x1 FG).

    NOTE: Snowflake's recursive CTE (bom_tree UNION ALL ... WHERE LEVEL < 4)
    cannot be used as-is here - Fabric's SQL endpoint (Synapse SQL) does not
    support recursive CTEs at all ("Recursive CTEs are unsupported in this
    version of Synapse SQL", error 8768). Since the recursion was always
    capped at 4 levels, it's unrolled below into level1..level4 CTEs, each
    self-joining bom_dt one level deeper than the last, then UNION ALL'd
    together as bom_tree - functionally identical output, no recursion.
    If the max depth ever needs to change, add/remove a levelN block here.
    """
    safe_product = _sql_escape(product_id)
    safe_wo = _sql_escape(work_order_id) if work_order_id else ""
    return run_query(f"""
        WITH level1 AS (
            SELECT
                PARENT_PART_NUMBER,
                CHILD_PART_NUMBER,
                CHILD_PART_DESCRIPTION,
                QTY_PER_ASSEMBLY,
                1 AS [LEVEL]
            FROM {DB}.{SCHEMA}.bom_dt
            WHERE PARENT_PART_NUMBER = '{safe_product}' AND BOM_STATUS = 'ACTIVE'
        ),
        level2 AS (
            SELECT
                b.PARENT_PART_NUMBER,
                b.CHILD_PART_NUMBER,
                b.CHILD_PART_DESCRIPTION,
                b.QTY_PER_ASSEMBLY,
                2 AS [LEVEL]
            FROM {DB}.{SCHEMA}.bom_dt b
            JOIN level1 l1 ON b.PARENT_PART_NUMBER = l1.CHILD_PART_NUMBER
            WHERE b.BOM_STATUS = 'ACTIVE'
        ),
        level3 AS (
            SELECT
                b.PARENT_PART_NUMBER,
                b.CHILD_PART_NUMBER,
                b.CHILD_PART_DESCRIPTION,
                b.QTY_PER_ASSEMBLY,
                3 AS [LEVEL]
            FROM {DB}.{SCHEMA}.bom_dt b
            JOIN level2 l2 ON b.PARENT_PART_NUMBER = l2.CHILD_PART_NUMBER
            WHERE b.BOM_STATUS = 'ACTIVE'
        ),
        level4 AS (
            SELECT
                b.PARENT_PART_NUMBER,
                b.CHILD_PART_NUMBER,
                b.CHILD_PART_DESCRIPTION,
                b.QTY_PER_ASSEMBLY,
                4 AS [LEVEL]
            FROM {DB}.{SCHEMA}.bom_dt b
            JOIN level3 l3 ON b.PARENT_PART_NUMBER = l3.CHILD_PART_NUMBER
            WHERE b.BOM_STATUS = 'ACTIVE'
        ),
        bom_tree AS (
            SELECT * FROM level1
            UNION ALL
            SELECT * FROM level2
            UNION ALL
            SELECT * FROM level3
            UNION ALL
            SELECT * FROM level4
        ),
        wo_mult AS (
            SELECT
                CASE
                    WHEN NULLIF(TRIM('{safe_wo}'), '') IS NULL THEN 1
                    ELSE COALESCE(
                        (
                            SELECT TOP (1) PLANNED_QTY
                            FROM {DB}.{SCHEMA}.work_order_dt
                            WHERE WORK_ORDER_ID = '{safe_wo}'
                              AND PRODUCT_ID = '{safe_product}'
                        ),
                        1
                    )
                END AS PQ,
                CASE
                    WHEN NULLIF(TRIM('{safe_wo}'), '') IS NULL THEN NULL
                    ELSE (
                        SELECT TOP (1) PLANT_ID
                        FROM {DB}.{SCHEMA}.work_order_dt
                        WHERE WORK_ORDER_ID = '{safe_wo}'
                          AND PRODUCT_ID = '{safe_product}'
                    )
                END AS PLANT_ID
        ),
        exploded AS (
            SELECT
                CHILD_PART_NUMBER,
                SUM(EXTENDED_QTY) AS EXT_PER_FG
            FROM {DB}.{SCHEMA}.bom_explode_dt
            WHERE ROOT_PRODUCT = '{safe_product}'
            GROUP BY CHILD_PART_NUMBER
        ),
        part_need AS (
            SELECT
                e.CHILD_PART_NUMBER,
                e.EXT_PER_FG * w.PQ AS BUILD_NEED_QTY
            FROM exploded e
            CROSS JOIN wo_mult w
        ),
        inv_agg AS (
            SELECT
                i.PART_NUMBER,
                SUM(COALESCE(i.QTY_AVAILABLE, 0)) AS QTY_AVAILABLE,
                SUM(COALESCE(i.SAFETY_STOCK, 0)) AS SAFETY_STOCK
            FROM {DB}.{SCHEMA}.inventory_dt i
            CROSS JOIN wo_mult wm
            WHERE wm.PLANT_ID IS NULL OR i.PLANT_ID = wm.PLANT_ID
            GROUP BY i.PART_NUMBER
        ),
        hp_alloc AS (
            SELECT
                p.DEMAND_PART_NUMBER AS PART_NUMBER,
                SUM(COALESCE(p.SUPPLY_QTY, 0)) AS HP_ALLOCATED
            FROM {DB}.{SCHEMA}.pegging_dt p
            JOIN {DB}.{SCHEMA}.work_order_dt wo ON p.WORK_ORDER_ID = wo.WORK_ORDER_ID
            WHERE wo.PRIORITY < (
                SELECT COALESCE(MIN(w2.PRIORITY), 999)
                FROM {DB}.{SCHEMA}.work_order_dt w2
                WHERE w2.PRODUCT_ID = '{safe_product}'
            )
              AND (
                  NULLIF(TRIM('{safe_wo}'), '') IS NULL
                  OR wo.PLANT_ID = (
                      SELECT TOP (1) w0.PLANT_ID
                      FROM {DB}.{SCHEMA}.work_order_dt w0
                      WHERE w0.WORK_ORDER_ID = '{safe_wo}'
                        AND w0.PRODUCT_ID = '{safe_product}'
                  )
              )
            GROUP BY p.DEMAND_PART_NUMBER
        )
        SELECT
            bt.PARENT_PART_NUMBER,
            bt.CHILD_PART_NUMBER,
            bt.CHILD_PART_DESCRIPTION,
            bt.QTY_PER_ASSEMBLY,
            bt.[LEVEL],
            COALESCE(i.QTY_AVAILABLE, 0) AS QTY_AVAILABLE,
            COALESCE(i.SAFETY_STOCK, 0) AS SAFETY_STOCK,
            COALESCE(h.HP_ALLOCATED, 0) AS HP_ALLOCATED,
            IIF(
                (COALESCE(i.QTY_AVAILABLE, 0) - COALESCE(h.HP_ALLOCATED, 0)) > 0,
                (COALESCE(i.QTY_AVAILABLE, 0) - COALESCE(h.HP_ALLOCATED, 0)),
                0
            ) AS EFF_AVAILABLE,
            COALESCE(pn.BUILD_NEED_QTY, bt.QTY_PER_ASSEMBLY * wm.PQ) AS BUILD_NEED_QTY
        FROM bom_tree bt
        CROSS JOIN wo_mult wm
        LEFT JOIN part_need pn ON bt.CHILD_PART_NUMBER = pn.CHILD_PART_NUMBER
        LEFT JOIN inv_agg i ON bt.CHILD_PART_NUMBER = i.PART_NUMBER
        LEFT JOIN hp_alloc h ON bt.CHILD_PART_NUMBER = h.PART_NUMBER
        ORDER BY bt.[LEVEL], bt.PARENT_PART_NUMBER, bt.CHILD_PART_NUMBER
    """)


def get_bom_where_used(constraint_part: str) -> list[dict]:
    """Streamlit parity for the 'Part Where-Used' KPIs + table."""
    safe_constraint = _sql_escape(constraint_part)
    return run_query(f"""
        SELECT
            b.PARENT_PART_NUMBER AS PRODUCT,
            b.BOM_LEVEL,
            SUM(b.QTY_PER_ASSEMBLY) AS QTY_PER_UNIT,
            COUNT(DISTINCT wo.WORK_ORDER_ID) AS WORK_ORDERS,
            SUM(wo.PLANNED_QTY * b.QTY_PER_ASSEMBLY) AS TOTAL_REQUIRED,
            COALESCE(MAX(i.QTY_AVAILABLE), 0) AS AVAILABLE,
            COALESCE(SUM(wo.PLANNED_QTY * b.QTY_PER_ASSEMBLY), 0) - COALESCE(MAX(i.QTY_AVAILABLE), 0) AS GAP
        FROM {DB}.{SCHEMA}.bom_dt b
        LEFT JOIN {DB}.{SCHEMA}.work_order_dt wo ON b.PARENT_PART_NUMBER = wo.PRODUCT_ID
        LEFT JOIN {DB}.{SCHEMA}.inventory_dt i ON b.CHILD_PART_NUMBER = i.PART_NUMBER
        WHERE b.CHILD_PART_NUMBER LIKE '%{safe_constraint}%' AND b.BOM_STATUS = 'ACTIVE'
        GROUP BY b.PARENT_PART_NUMBER, b.BOM_LEVEL
        ORDER BY GAP DESC
    """)


def get_bom_open_pos(constraint_part: str) -> list[dict]:
    """Streamlit parity for the 'Open POs for this Part' table in Part Where-Used."""
    safe_constraint = _sql_escape(constraint_part)
    return run_query(f"""
        SELECT
            po.PO_ID,
            po.SUPPLIER_ID + ' - ' + COALESCE(s.SUPPLIER_NAME, '') AS SUPPLIER,
            po.QTY_ORDERED,
            po.QTY_OUTSTANDING,
            po.CONFIRMED_DELIVERY_DATE,
            po.PO_STATUS
        FROM {DB}.{SCHEMA}.procurement_summary_dt po
        LEFT JOIN {DB}.{SCHEMA}.supplier_summary_dt s ON po.SUPPLIER_ID = s.SUPPLIER_ID
        WHERE po.PART_NUMBER LIKE '%{safe_constraint}%'
          AND po.PO_STATUS NOT IN ('COMPLETED', 'CANCELLED')
        ORDER BY po.CONFIRMED_DELIVERY_DATE
    """)


# ---------------------------------------------------------------------------
# Shortages / suppliers
# ---------------------------------------------------------------------------

def get_shortage_alerts(product: str | None = None, plant: str | None = None, week: str | None = None) -> list[dict]:
    where = _wo_where(product, plant, week)
    return run_query(f"""
        SELECT sa.PART_NUMBER, sa.WORK_ORDER_ID, sa.PLANT_ID,
               sa.SHORTAGE_QTY, sa.ALERT_STATUS,
               wo.PRODUCT_ID, wo.PRIORITY,
               CONVERT(varchar(10), wo.PLANNED_START_DATE, 120) AS "planned_start_date"
        FROM {DB}.{SCHEMA}.shortage_alert_dt sa
        JOIN {DB}.{SCHEMA}.work_order_dt wo ON sa.WORK_ORDER_ID = wo.WORK_ORDER_ID
        WHERE sa.ALERT_STATUS = 'OPEN' {where}
        ORDER BY wo.PRIORITY, sa.SHORTAGE_QTY DESC
    """)


def get_supplier_performance() -> list[dict]:
    return run_query(f"""
        SELECT TOP (20)
               SUPPLIER_ID,
               COUNT(*) AS "total_orders",
               SUM(CASE WHEN IS_LATE = 1 THEN 1 ELSE 0 END) AS "late_orders",
               ROUND(100.0 * SUM(CASE WHEN IS_LATE = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) AS "on_time_pct"
        FROM {DB}.{SCHEMA}.procurement_summary_dt
        GROUP BY SUPPLIER_ID ORDER BY "on_time_pct" ASC
    """)


# ---------------------------------------------------------------------------
# Write actions
#
# refresh_ctb is a genuine Warehouse stored procedure call (SP_REFRESH_CTB_DATA
# must exist in {WH_DB}.{WH_SCHEMA} - a database-side task, not something
# this file can do). create_po and prioritize_work_order write directly to
# Lakehouse Delta tables over OneLake instead - see lakehouse_writer.py and
# the docstrings on each function below for why.
# ---------------------------------------------------------------------------

def create_po(part_number: str, qty: int, supplier_id: str, plant_id: str, delivery_date: str) -> dict:
    """
    Writes the PO directly to the Lakehouse's procurement_summary_dt Delta
    table over OneLake (fabric_app/lakehouse_writer.py::insert_procurement_po),
    instead of calling a Warehouse stored procedure: procurement_summary_dt
    is a Lakehouse table (information_mart schema), not a Warehouse table,
    so there was never a SQL-endpoint write path for it and no SP_CREATE_PO
    to call in the first place - that EXEC would only have worked if the
    proc AND the table both actually lived in the Warehouse.

    Still mirrors Snowflake's defaulting behavior in
    app/services/ai_service.py::shortage_agent_create_po:
      - plant_id: falls back to 'PL001' if blank (Snowflake does the same).
      - delivery_date: NEVER trusts a client-supplied delivery date as-is -
        parses it if it looks like 'YYYY-MM-DD', otherwise defaults to
        14 days from today, same as before.
    """
    safe_plant = plant_id.strip() if plant_id else "PL001"

    parsed_date = None
    if delivery_date:
        try:
            parsed_date = datetime.strptime(str(delivery_date).strip()[:10], "%Y-%m-%d").date()
        except ValueError:
            parsed_date = None  # not a real date (e.g. preview's "+14 days" text) - use default below
    if parsed_date is None:
        parsed_date = (datetime.utcnow() + timedelta(days=14)).date()

    return lakehouse_writer.insert_procurement_po(
        part_number=part_number,
        qty_ordered=float(qty),
        supplier_id=supplier_id,
        receiving_plant_id=safe_plant,
        requested_delivery_date=parsed_date,
        unit_price=100.00,
        created_by="REACT_APP",
        sql_run_query=run_query,
    )


def _compute_ctb_and_shortages() -> tuple[list[dict], list[dict]]:
    """
    Recomputes per-work-order CAN_BUILD_QTY/CTB_STATUS and per-part
    shortage rows, mimicking what Snowflake's SP_REFRESH_CTB_DATA
    (ALTER DYNAMIC TABLE CTB_RESULT_DT / SHORTAGE_ALERT_DT REFRESH) does -
    using formulas ALREADY established elsewhere in this codebase, not
    invented here:
      - PEGGING_DT as the primary allocation/shortage source: SUPPLY_QTY
        and SHORTAGE_QTY per (WORK_ORDER_ID, DEMAND_PART_NUMBER) are
        exactly what get_kpis/get_top_shortages/the prioritization-impact
        logic elsewhere in this file already treat as authoritative -
        pegging_dt is the real allocation snapshot, so it already resolves
        which work order gets which unit of scarce inventory when multiple
        orders compete for the same part. This replaces the previous
        version's raw inventory_dt subtraction, which gave every competing
        work order full credit for the same shared pool - a real
        correctness gap, not just a stylistic one.
      - INVENTORY_DT as a fallback ONLY for a (work order, part) pair with
        no pegging_dt row at all (nothing pegged yet) - same available-qty
        sum get_work_order_detail/simulate_prioritization already use.
      - CAN_BUILD_QTY / READY-PARTIAL-BLOCKED derivation: the exact
        formula in this file's _weekly_build_summary (min of per-part
        can-build, capped at planned qty; READY if >= planned, BLOCKED
        if 0, else PARTIAL), applied per work order instead of per week.
      - Shortage rows now use pegging_dt's own SHORTAGE_QTY directly
        (already computed by whatever allocation engine populates
        pegging_dt) rather than re-deriving it via subtraction, wherever
        a pegging row exists; falls back to the subtraction only when it
        doesn't.

    HONEST LIMITATIONS - still a best-effort mimic, not a confirmed match
    to Snowflake's real dynamic-table SELECT (which isn't visible from
    this codebase):
      - PRODUCTION_STATUS has no established derivation rule anywhere in
        this file (it only ever appears read-side behind
        COALESCE(...,'PENDING')), so every recomputed row is written as
        'PENDING' - this will NOT preserve a real in-progress/production
        status Snowflake's version might be tracking.
      - Does not touch RELEASE_GATE_DT or PLANT_SUMMARY_DT at all - no
        derivation logic for either exists anywhere in this codebase, and
        neither is actually an input to CTB_STATUS/shortage math (they're
        plant master/rollup data, not build-feasibility data).
    """
    rows = run_query(f"""
        SELECT
            wo.WORK_ORDER_ID AS "work_order_id",
            wo.PLANT_ID AS "plant_id",
            wo.PLANNED_QTY AS "planned_qty",
            b.CHILD_PART_NUMBER AS "part_number",
            b.QTY_PER_ASSEMBLY AS "qty_per_assembly",
            b.QTY_PER_ASSEMBLY * wo.PLANNED_QTY AS "required_qty",
            COALESCE(inv.qty_available, 0) AS "inventory_available_qty",
            peg.SUPPLY_QTY AS "pegged_supply_qty",
            peg.SHORTAGE_QTY AS "pegged_shortage_qty"
        FROM {DB}.{SCHEMA}.work_order_dt wo
        JOIN {DB}.{SCHEMA}.bom_dt b
          ON wo.PRODUCT_ID = b.PARENT_PART_NUMBER AND b.BOM_STATUS = 'ACTIVE'
        LEFT JOIN (
            SELECT PART_NUMBER, SUM(QTY_AVAILABLE) AS qty_available
            FROM {DB}.{SCHEMA}.inventory_dt
            GROUP BY PART_NUMBER
        ) inv ON inv.PART_NUMBER = b.CHILD_PART_NUMBER
        LEFT JOIN {DB}.{SCHEMA}.pegging_dt peg
          ON peg.WORK_ORDER_ID = wo.WORK_ORDER_ID AND peg.DEMAND_PART_NUMBER = b.CHILD_PART_NUMBER
        WHERE wo.WORK_ORDER_STATUS NOT IN ('COMPLETED', 'CANCELLED', 'CLOSED')
    """)

    by_wo: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_wo[r["work_order_id"]].append(r)

    ctb_rows: list[dict] = []
    shortage_rows: list[dict] = []
    for wo_id, parts in by_wo.items():
        planned = float(parts[0].get("planned_qty") or 0)
        plant_id = parts[0].get("plant_id")
        can_builds: list[float] = []

        for p in parts:
            qty_per = float(p.get("qty_per_assembly") or 0)
            pegged_supply = p.get("pegged_supply_qty")
            pegged_shortage = p.get("pegged_shortage_qty")
            has_pegging = pegged_supply is not None or pegged_shortage is not None

            if has_pegging:
                # pegging_dt already resolved allocation for this specific
                # work order's demand - use it directly rather than
                # re-deriving from the shared, unallocated inventory pool.
                supply = float(pegged_supply or 0)
                shortage = float(pegged_shortage or 0)
            else:
                # No pegging row for this (work order, part) - fall back to
                # the shared inventory pool (get_work_order_detail's logic).
                available = float(p.get("inventory_available_qty") or 0)
                required = float(p.get("required_qty") or 0)
                supply = available
                shortage = max(0.0, required - available)

            can_builds.append(math.floor(supply / qty_per) if qty_per > 0 else 0)

            if shortage > 0:
                shortage_rows.append({
                    "WORK_ORDER_ID": wo_id,
                    "PART_NUMBER": p["part_number"],
                    "PLANT_ID": plant_id,
                    "SHORTAGE_QTY": shortage,
                })

        wo_can_build = max(0, min(min(can_builds) if can_builds else 0, planned))
        if wo_can_build >= planned:
            wo_status = "READY"
        elif wo_can_build == 0:
            wo_status = "BLOCKED"
        else:
            wo_status = "PARTIAL"

        ctb_rows.append({
            "WORK_ORDER_ID": wo_id,
            "CTB_STATUS": wo_status,
            "CAN_BUILD_QTY": wo_can_build,
            "PRODUCTION_STATUS": "PENDING",
        })

    return ctb_rows, shortage_rows


def refresh_ctb() -> dict:
    """
    Fabric mimic of Snowflake's SP_REFRESH_CTB_DATA. Recomputes
    ctb_result_dt (CTB_STATUS/CAN_BUILD_QTY) and shortage_alert_dt (open
    shortage rows) directly via OneLake Delta merges - see
    _compute_ctb_and_shortages for the formulas used (now pegging_dt-aware,
    not just raw inventory subtraction) and their honest limitations, and
    lakehouse_writer.upsert_ctb_results / upsert_shortage_alerts for the
    merge mechanics.

    READS work_order_dt, bom_dt, inventory_dt, and pegging_dt as inputs.
    WRITES ctb_result_dt and shortage_alert_dt. Does NOT touch
    release_gate_dt or plant_summary_dt - neither has any derivation logic
    established in this codebase, and neither is actually an input to
    CTB_STATUS/shortage math (they're plant master/rollup data).

    Does NOT auto-resolve shortages that no longer compute as short -
    only adds/updates currently-computed shortages. See
    upsert_shortage_alerts's docstring for why that's a deliberate scope
    cut rather than an oversight.
    """
    ctb_rows, shortage_rows = _compute_ctb_and_shortages()
    lakehouse_writer.upsert_ctb_results(ctb_rows)
    lakehouse_writer.upsert_shortage_alerts(shortage_rows)
    return {
        "status": "SUCCESS",
        "message": (
            f"Recomputed CTB status for {len(ctb_rows)} open work orders and "
            f"upserted {len(shortage_rows)} open shortage rows "
            "(best-effort mimic of SP_REFRESH_CTB_DATA - see refresh_ctb()'s "
            "docstring for exact scope/limitations)."
        ),
        "work_orders_recomputed": len(ctb_rows),
        "shortage_rows_upserted": len(shortage_rows),
    }


def prioritize_work_order(wo_id: str) -> dict:
    """
    Updates PRIORITY / PLANNED_START_DATE directly on the Lakehouse's
    work_order_dt Delta table over OneLake
    (fabric_app/lakehouse_writer.py::update_work_order_priority), instead
    of a Warehouse stored procedure: work_order_dt is a Lakehouse table,
    and the SQL analytics endpoint rejects UPDATE/DELETE against Lakehouse
    tables outright ("Data Manipulation Language (DML) statements are not
    supported for this table type"), so a Warehouse EXEC was never going
    to reach this table regardless of whether the proc existed.
    """
    return lakehouse_writer.update_work_order_priority(
        work_order_id=wo_id,
        new_priority=0,
        new_planned_start_date=None,  # defaults to Monday of the current week
        updated_by="REACT_APP",
        sql_run_query=run_query,
    )


# ---------------------------------------------------------------------------
# What-if prioritization simulation
# ---------------------------------------------------------------------------

def simulate_prioritization(wo_id: str) -> list[dict]:
    safe_wo_id = _sql_escape(wo_id)
    rows = run_query(f"""
        WITH target_wo AS (
            SELECT wo.WORK_ORDER_ID, wo.PRODUCT_ID, wo.PLANNED_QTY,
                   wo.PLANNED_START_DATE,
                   COALESCE(ctb.CAN_BUILD_QTY, 0) AS CAN_BUILD_QTY,
                   COALESCE(ctb.CTB_STATUS, 'UNKNOWN') AS CTB_STATUS
            FROM {DB}.{SCHEMA}.work_order_dt wo
            LEFT JOIN {DB}.{SCHEMA}.ctb_result_dt ctb ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
            WHERE wo.WORK_ORDER_ID = '{safe_wo_id}'
        ),
        bom_requirements AS (
            SELECT
                b.CHILD_PART_NUMBER AS PART_NUMBER,
                b.QTY_PER_ASSEMBLY,
                tw.PLANNED_QTY,
                b.QTY_PER_ASSEMBLY * tw.PLANNED_QTY AS REQUIRED_QTY
            FROM target_wo tw
            JOIN {DB}.{SCHEMA}.bom_dt b ON tw.PRODUCT_ID = b.PARENT_PART_NUMBER AND b.BOM_STATUS = 'ACTIVE'
        ),
        current_inventory AS (
            SELECT PART_NUMBER, SUM(QTY_AVAILABLE) AS QTY_AVAILABLE
            FROM {DB}.{SCHEMA}.inventory_dt
            GROUP BY PART_NUMBER
        ),
        upcoming_deliveries AS (
            SELECT PART_NUMBER, SUM(QTY_SHIPPED) AS INCOMING_QTY
            FROM {DB}.{SCHEMA}.asn_in_transit_dt
            WHERE ASN_STATUS IN ('SHIPPED', 'IN_TRANSIT')
              AND EXPECTED_ARRIVAL_DATE <= DATEADD(week, 2, DATETRUNC(week, GETDATE()))
            GROUP BY PART_NUMBER
        ),
        in_production_consumption AS (
            SELECT
                b.CHILD_PART_NUMBER AS PART_NUMBER,
                SUM(b.QTY_PER_ASSEMBLY * wo.PLANNED_QTY) AS CONSUMED_QTY
            FROM {DB}.{SCHEMA}.work_order_dt wo
            JOIN {DB}.{SCHEMA}.ctb_result_dt ctb ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
            JOIN {DB}.{SCHEMA}.bom_dt b ON wo.PRODUCT_ID = b.PARENT_PART_NUMBER AND b.BOM_STATUS = 'ACTIVE'
            WHERE ctb.PRODUCTION_STATUS = 'IN_PRODUCTION'
            GROUP BY b.CHILD_PART_NUMBER
        ),
        high_priority_consumption AS (
            SELECT
                b.CHILD_PART_NUMBER AS PART_NUMBER,
                SUM(b.QTY_PER_ASSEMBLY * wo.PLANNED_QTY) AS CONSUMED_QTY
            FROM {DB}.{SCHEMA}.work_order_dt wo
            JOIN {DB}.{SCHEMA}.bom_dt b ON wo.PRODUCT_ID = b.PARENT_PART_NUMBER AND b.BOM_STATUS = 'ACTIVE'
            LEFT JOIN {DB}.{SCHEMA}.ctb_result_dt ctb ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
            WHERE wo.PRIORITY = 0
              AND DATETRUNC(week, wo.PLANNED_START_DATE) = DATETRUNC(week, GETDATE())
              AND COALESCE(ctb.PRODUCTION_STATUS, 'PENDING') != 'IN_PRODUCTION'
              AND wo.WORK_ORDER_ID != '{safe_wo_id}'
            GROUP BY b.CHILD_PART_NUMBER
        ),
        part_analysis AS (
            SELECT
                br.PART_NUMBER,
                br.REQUIRED_QTY,
                br.QTY_PER_ASSEMBLY,
                br.PLANNED_QTY,
                COALESCE(inv.QTY_AVAILABLE, 0) AS TOTAL_INVENTORY,
                COALESCE(del.INCOMING_QTY, 0) AS INCOMING_QTY,
                COALESCE(ip.CONSUMED_QTY, 0) AS IN_PRODUCTION_RESERVED,
                COALESCE(hp.CONSUMED_QTY, 0) AS HIGH_PRIORITY_RESERVED,
                IIF(
                    (COALESCE(inv.QTY_AVAILABLE, 0) - COALESCE(ip.CONSUMED_QTY, 0) - COALESCE(hp.CONSUMED_QTY, 0)) > 0,
                    (COALESCE(inv.QTY_AVAILABLE, 0) - COALESCE(ip.CONSUMED_QTY, 0) - COALESCE(hp.CONSUMED_QTY, 0)),
                    0
                ) AS AVAILABLE_NOW,
                IIF(
                    (COALESCE(inv.QTY_AVAILABLE, 0) + COALESCE(del.INCOMING_QTY, 0) - COALESCE(ip.CONSUMED_QTY, 0) - COALESCE(hp.CONSUMED_QTY, 0)) > 0,
                    (COALESCE(inv.QTY_AVAILABLE, 0) + COALESCE(del.INCOMING_QTY, 0) - COALESCE(ip.CONSUMED_QTY, 0) - COALESCE(hp.CONSUMED_QTY, 0)),
                    0
                ) AS AVAILABLE_AFTER_DELIVERY
            FROM bom_requirements br
            LEFT JOIN current_inventory inv ON br.PART_NUMBER = inv.PART_NUMBER
            LEFT JOIN upcoming_deliveries del ON br.PART_NUMBER = del.PART_NUMBER
            LEFT JOIN in_production_consumption ip ON br.PART_NUMBER = ip.PART_NUMBER
            LEFT JOIN high_priority_consumption hp ON br.PART_NUMBER = hp.PART_NUMBER
        )
        SELECT
            pa.PART_NUMBER AS "part_number",
            pa.REQUIRED_QTY AS "required_qty",
            tw.PLANNED_QTY AS "planned_qty",
            tw.CAN_BUILD_QTY AS "wo_can_build_qty",
            tw.CTB_STATUS AS "wo_ctb_status",
            pa.TOTAL_INVENTORY AS "total_inventory",
            pa.INCOMING_QTY AS "incoming_qty",
            pa.IN_PRODUCTION_RESERVED AS "in_production_reserved",
            pa.HIGH_PRIORITY_RESERVED AS "high_priority_reserved",
            pa.AVAILABLE_NOW AS "available_now",
            pa.AVAILABLE_AFTER_DELIVERY AS "available_after_delivery",
            IIF((pa.REQUIRED_QTY - pa.AVAILABLE_NOW) > 0, (pa.REQUIRED_QTY - pa.AVAILABLE_NOW), 0) AS "shortage_now",
            IIF((pa.REQUIRED_QTY - pa.AVAILABLE_AFTER_DELIVERY) > 0, (pa.REQUIRED_QTY - pa.AVAILABLE_AFTER_DELIVERY), 0) AS "shortage_after_delivery",
            CASE
                WHEN pa.AVAILABLE_NOW >= pa.REQUIRED_QTY THEN 'READY_NOW'
                WHEN pa.AVAILABLE_AFTER_DELIVERY >= pa.REQUIRED_QTY THEN 'READY_ON_DELIVERY'
                WHEN pa.AVAILABLE_NOW > 0 THEN 'PARTIAL_NOW'
                WHEN pa.AVAILABLE_AFTER_DELIVERY > 0 THEN 'PARTIAL_ON_DELIVERY'
                ELSE 'BLOCKED'
            END AS "part_status",
            CASE
                WHEN tw.CTB_STATUS = 'BLOCKED' AND tw.CAN_BUILD_QTY <= 0 THEN 0
                WHEN pa.QTY_PER_ASSEMBLY > 0 THEN
                    IIF(FLOOR(pa.AVAILABLE_NOW / pa.QTY_PER_ASSEMBLY) < tw.CAN_BUILD_QTY,
                        FLOOR(pa.AVAILABLE_NOW / pa.QTY_PER_ASSEMBLY), tw.CAN_BUILD_QTY)
                ELSE IIF(tw.PLANNED_QTY < tw.CAN_BUILD_QTY, tw.PLANNED_QTY, tw.CAN_BUILD_QTY)
            END AS "can_build_now",
            CASE
                WHEN tw.CTB_STATUS = 'BLOCKED' AND tw.CAN_BUILD_QTY <= 0 THEN 0
                WHEN pa.QTY_PER_ASSEMBLY > 0 THEN FLOOR(pa.AVAILABLE_AFTER_DELIVERY / pa.QTY_PER_ASSEMBLY)
                ELSE tw.PLANNED_QTY
            END AS "can_build_after_delivery"
        FROM part_analysis pa
        CROSS JOIN target_wo tw
        ORDER BY
            CASE WHEN pa.AVAILABLE_NOW < pa.REQUIRED_QTY THEN 0 ELSE 1 END,
            pa.AVAILABLE_NOW - pa.REQUIRED_QTY ASC
    """)
    return rows


def simulate_prioritization_impact(wo_id: str) -> list[dict]:
    """
    For the given target work order, return OTHER work orders that would be
    impacted if it is promoted to high priority. See snowflake_service.py's
    docstring for the 4-step algorithm this mirrors.
    """
    safe_wo_id = _sql_escape(wo_id)
    rows = run_query(f"""
        WITH target_wo AS (
            SELECT
                wo.WORK_ORDER_ID,
                wo.PRODUCT_ID,
                wo.PLANT_ID,
                wo.PLANNED_QTY
            FROM {DB}.{SCHEMA}.work_order_dt wo
            WHERE wo.WORK_ORDER_ID = '{safe_wo_id}'
        ),
        target_parts AS (
            SELECT
                b.CHILD_PART_NUMBER AS PART_NUMBER,
                b.QTY_PER_ASSEMBLY * tw.PLANNED_QTY AS TARGET_NEED
            FROM target_wo tw
            JOIN {DB}.{SCHEMA}.bom_dt b
              ON tw.PRODUCT_ID = b.PARENT_PART_NUMBER
             AND b.BOM_STATUS = 'ACTIVE'
        ),
        inv AS (
            SELECT PART_NUMBER, SUM(COALESCE(QTY_AVAILABLE, 0)) AS QTY_AVAILABLE
            FROM {DB}.{SCHEMA}.inventory_dt
            GROUP BY PART_NUMBER
        ),
        in_prod AS (
            SELECT
                b.CHILD_PART_NUMBER AS PART_NUMBER,
                SUM(b.QTY_PER_ASSEMBLY * wo.PLANNED_QTY) AS RESERVED_QTY
            FROM {DB}.{SCHEMA}.work_order_dt wo
            JOIN {DB}.{SCHEMA}.ctb_result_dt ctb
              ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
            JOIN {DB}.{SCHEMA}.bom_dt b
              ON wo.PRODUCT_ID = b.PARENT_PART_NUMBER
             AND b.BOM_STATUS = 'ACTIVE'
            WHERE ctb.PRODUCTION_STATUS = 'IN_PRODUCTION'
              AND wo.WORK_ORDER_ID != '{safe_wo_id}'
            GROUP BY b.CHILD_PART_NUMBER
        ),
        post_avail AS (
            SELECT
                tp.PART_NUMBER,
                tp.TARGET_NEED,
                COALESCE(i.QTY_AVAILABLE, 0) AS TOTAL_INV,
                COALESCE(ip.RESERVED_QTY, 0) AS IN_PROD_RESERVED,
                IIF(
                    (COALESCE(i.QTY_AVAILABLE, 0) - COALESCE(ip.RESERVED_QTY, 0) - tp.TARGET_NEED) > 0,
                    (COALESCE(i.QTY_AVAILABLE, 0) - COALESCE(ip.RESERVED_QTY, 0) - tp.TARGET_NEED),
                    0
                ) AS RESIDUAL_AVAIL
            FROM target_parts tp
            LEFT JOIN inv i ON i.PART_NUMBER = tp.PART_NUMBER
            LEFT JOIN in_prod ip ON ip.PART_NUMBER = tp.PART_NUMBER
        ),
        per_wo_part AS (
            SELECT
                other.WORK_ORDER_ID,
                other.PRODUCT_ID,
                other.PRIORITY,
                other.PLANNED_QTY,
                other.PLANNED_START_DATE,
                other.WORK_ORDER_STATUS,
                b.CHILD_PART_NUMBER AS PART_NUMBER,
                b.QTY_PER_ASSEMBLY,
                pa.TARGET_NEED,
                pa.RESIDUAL_AVAIL,
                FLOOR(pa.RESIDUAL_AVAIL / NULLIF(b.QTY_PER_ASSEMBLY, 0)) AS PART_NEW_CAN_BUILD
            FROM {DB}.{SCHEMA}.work_order_dt other
            JOIN {DB}.{SCHEMA}.bom_dt b
              ON other.PRODUCT_ID = b.PARENT_PART_NUMBER
             AND b.BOM_STATUS = 'ACTIVE'
            JOIN post_avail pa
              ON pa.PART_NUMBER = b.CHILD_PART_NUMBER
            CROSS JOIN target_wo tw
            WHERE other.WORK_ORDER_ID != tw.WORK_ORDER_ID
              AND other.WORK_ORDER_STATUS NOT IN ('COMPLETED', 'CANCELLED', 'CLOSED')
              AND other.PLANT_ID = tw.PLANT_ID
        ),
        agg AS (
            SELECT
                pwp.WORK_ORDER_ID,
                MAX(pwp.PRODUCT_ID) AS PRODUCT_ID,
                MAX(pwp.PRIORITY) AS PRIORITY,
                MAX(pwp.PLANNED_QTY) AS PLANNED_QTY,
                MAX(pwp.PLANNED_START_DATE) AS PLANNED_START_DATE,
                MAX(pwp.WORK_ORDER_STATUS) AS WORK_ORDER_STATUS,
                (
                    SELECT STRING_AGG(x.PART_NUMBER, ', ') WITHIN GROUP (ORDER BY x.PART_NUMBER)
                    FROM (
                        SELECT DISTINCT p2.PART_NUMBER
                        FROM per_wo_part p2
                        WHERE p2.WORK_ORDER_ID = pwp.WORK_ORDER_ID
                    ) x
                ) AS SHARED_PARTS,
                COUNT(DISTINCT pwp.PART_NUMBER) AS SHARED_PART_COUNT,
                MIN(pwp.PART_NEW_CAN_BUILD) AS NEW_CAN_BUILD_FROM_SHARED
            FROM per_wo_part pwp
            GROUP BY pwp.WORK_ORDER_ID
        )
        SELECT TOP (50)
            agg.WORK_ORDER_ID         AS "WORK_ORDER_ID",
            agg.PRODUCT_ID            AS "PRODUCT_ID",
            agg.PRIORITY              AS "PRIORITY",
            agg.PLANNED_QTY           AS "PLANNED_QTY",
            CONVERT(varchar(10), agg.PLANNED_START_DATE, 120) AS "planned_start_date",
            agg.WORK_ORDER_STATUS     AS "work_order_status",
            COALESCE(ctb.CAN_BUILD_QTY, 0) AS "current_can_build",
            COALESCE(ctb.CTB_STATUS, 'UNKNOWN') AS "current_status",
            new_can_build.val AS "new_can_build",
            IIF(
                (COALESCE(ctb.CAN_BUILD_QTY, 0) - new_can_build.val) > 0,
                (COALESCE(ctb.CAN_BUILD_QTY, 0) - new_can_build.val),
                0
            ) AS "lost_build_qty",
            agg.SHARED_PARTS          AS "shared_parts",
            agg.SHARED_PART_COUNT     AS "shared_part_count"
        FROM agg
        LEFT JOIN {DB}.{SCHEMA}.ctb_result_dt ctb
          ON ctb.WORK_ORDER_ID = agg.WORK_ORDER_ID
        CROSS APPLY (
            SELECT
                CASE
                    WHEN agg.PLANNED_QTY <= COALESCE(ctb.CAN_BUILD_QTY, 0)
                     AND agg.PLANNED_QTY <= COALESCE(agg.NEW_CAN_BUILD_FROM_SHARED, 0) THEN agg.PLANNED_QTY
                    WHEN COALESCE(ctb.CAN_BUILD_QTY, 0) <= COALESCE(agg.NEW_CAN_BUILD_FROM_SHARED, 0) THEN COALESCE(ctb.CAN_BUILD_QTY, 0)
                    ELSE COALESCE(agg.NEW_CAN_BUILD_FROM_SHARED, 0)
                END AS val
        ) new_can_build
        WHERE COALESCE(ctb.CAN_BUILD_QTY, 0) > new_can_build.val
        ORDER BY "lost_build_qty" DESC, "PRIORITY" ASC, "WORK_ORDER_ID"
    """)
    return rows


# ---------------------------------------------------------------------------
# Production board / BOM constraints
# ---------------------------------------------------------------------------

def get_production_board(product: str | None = None, plant: str | None = None) -> list[dict]:
    where = _wo_where(product, plant, None)
    rows = run_query(f"""
        SELECT
            wo.WORK_ORDER_ID,
            wo.PRODUCT_ID,
            wo.PLANT_ID,
            wo.PRIORITY,
            wo.PLANNED_QTY,
            CONVERT(varchar(10), wo.PLANNED_START_DATE, 120) AS "planned_start_date",
            wo.WORK_ORDER_STATUS,
            COALESCE(ctb.CTB_STATUS, 'UNKNOWN') AS "ctb_status",
            CASE
                WHEN COALESCE(ctb.CAN_BUILD_QTY, 0) >= COALESCE(wo.PLANNED_QTY, 0) THEN 'READY_NOW'
                WHEN DATETRUNC(week, wo.PLANNED_START_DATE) = DATETRUNC(week, GETDATE())
                     AND COALESCE(ctb.CAN_BUILD_QTY, 0) > 0 THEN 'PARTIAL'
                WHEN COALESCE(ctb.CAN_BUILD_WITH_DELIVERIES, 0) >= COALESCE(ctb.CAN_BUILD_FULL, 0)
                     AND COALESCE(ctb.CAN_BUILD_FULL, 0) > 0 THEN 'READY_ON_DELIVERY'
                ELSE 'BLOCKED'
            END AS "planning_status",
            COALESCE(ctb.PRODUCTION_STATUS, 'PENDING') AS "production_status",
            COALESCE(ctb.CAN_BUILD_QTY, 0) AS "can_build_qty",
            COALESCE(ctb.CAN_BUILD_BASE, 0) AS "parts_ready_now",
            COALESCE(ctb.CAN_BUILD_WITH_DELIVERIES, 0) AS "parts_ready_with_del",
            COALESCE(ctb.CAN_BUILD_FULL, 0) AS "total_parts",
            CONVERT(varchar(10), DATETRUNC(week, wo.PLANNED_START_DATE), 120) AS "week_start",
            CASE
                WHEN wo.PRIORITY = 0 AND DATETRUNC(week, wo.PLANNED_START_DATE) = DATETRUNC(week, GETDATE()) THEN 'HIGH_PRIORITY'
                WHEN COALESCE(ctb.CTB_STATUS, 'UNKNOWN') = 'READY'
                     AND DATETRUNC(week, wo.PLANNED_START_DATE) = DATETRUNC(week, GETDATE())
                     AND wo.PLANNED_START_DATE <= GETDATE() THEN 'IN_PRODUCTION'
                WHEN DATETRUNC(week, wo.PLANNED_START_DATE) = DATETRUNC(week, GETDATE()) THEN 'YET_TO_START'
                WHEN DATETRUNC(week, wo.PLANNED_START_DATE) = DATEADD(week, 1, DATETRUNC(week, GETDATE())) THEN 'NEXT_WEEK'
                ELSE 'OTHER'
            END AS "lane"
        FROM {DB}.{SCHEMA}.work_order_dt wo
        LEFT JOIN {DB}.{SCHEMA}.ctb_result_dt ctb ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
        WHERE 1=1 {where}
          AND DATETRUNC(week, wo.PLANNED_START_DATE) >= DATETRUNC(week, GETDATE())
          AND DATETRUNC(week, wo.PLANNED_START_DATE) <= DATEADD(week, 1, DATETRUNC(week, GETDATE()))
        ORDER BY wo.PRIORITY, wo.PLANNED_START_DATE
    """)
    return rows


def get_bom_constraints(product: str | None = None, plant: str | None = None, week: str | None = None) -> list[dict]:
    where = _wo_where(product, plant, week)
    return run_query(f"""
        WITH shortage_parts AS (
            SELECT
                wo.WORK_ORDER_ID,
                wo.PRODUCT_ID,
                wo.PLANT_ID,
                wo.PRIORITY,
                wo.PLANNED_QTY,
                wo.PLANNED_START_DATE,
                DATETRUNC(week, wo.PLANNED_START_DATE) AS WEEK_START,
                p.DEMAND_PART_NUMBER AS CONSTRAINT_PART,
                p.BOM_LEVEL,
                p.DEMAND_QTY AS REQUIRED_QTY,
                p.SUPPLY_QTY AS SUPPLY_QTY,
                p.SHORTAGE_QTY
            FROM {DB}.{SCHEMA}.work_order_dt wo
            JOIN {DB}.{SCHEMA}.pegging_dt p
              ON wo.WORK_ORDER_ID = p.WORK_ORDER_ID
            WHERE p.PEGGING_STATUS = 'SHORT'
              AND p.SHORTAGE_QTY > 0
              {where}
        ),
        bom_component AS (
            SELECT
                ROOT_PRODUCT,
                CHILD_PART_NUMBER AS PART_NUMBER,
                MIN(BOM_LEVEL) AS BOM_LEVEL,
                MAX(EXTENDED_QTY) AS EXTENDED_QTY
            FROM {DB}.{SCHEMA}.bom_explode_dt
            GROUP BY ROOT_PRODUCT, CHILD_PART_NUMBER
        ),
        inv AS (
            SELECT
                PART_NUMBER,
                PLANT_ID,
                SUM(COALESCE(QTY_AVAILABLE, 0)) AS qty_available,
                SUM(COALESCE(SAFETY_STOCK, 0)) AS safety_stock
            FROM {DB}.{SCHEMA}.inventory_dt
            GROUP BY PART_NUMBER, PLANT_ID
        ),
        del AS (
            SELECT
                PART_NUMBER,
                RECEIVING_PLANT_ID AS PLANT_ID,
                SUM(COALESCE(QTY_SHIPPED, 0)) AS total_deliveries
            FROM {DB}.{SCHEMA}.asn_in_transit_dt
            WHERE ASN_STATUS IN ('SHIPPED', 'IN_TRANSIT')
            GROUP BY PART_NUMBER, RECEIVING_PLANT_ID
        )
        SELECT
            sp.WORK_ORDER_ID,
            sp.PRODUCT_ID,
            sp.PRIORITY,
            sp.PLANNED_QTY,
            CONVERT(varchar(10), sp.PLANNED_START_DATE, 120) AS "planned_start_date",
            sp.CONSTRAINT_PART AS "constraint_part",
            COALESCE(bc.BOM_LEVEL, sp.BOM_LEVEL) AS BOM_LEVEL,
            COALESCE(
                bc.EXTENDED_QTY,
                IIF(sp.PLANNED_QTY > 0, sp.REQUIRED_QTY / NULLIF(sp.PLANNED_QTY, 0), 0)
            ) AS "qty_per_unit",
            sp.REQUIRED_QTY AS "required_qty",
            COALESCE(i.qty_available, 0) AS "available_qty",
            COALESCE(i.safety_stock, 0) AS "safety_stock",
            COALESCE(d.total_deliveries, 0) AS "total_deliveries",
            sp.SHORTAGE_QTY AS "gap",
            CASE
              WHEN (COALESCE(i.qty_available, 0) + COALESCE(i.safety_stock, 0) + COALESCE(d.total_deliveries, 0)) < sp.REQUIRED_QTY THEN 'CRITICAL'
              WHEN (COALESCE(i.qty_available, 0) + COALESCE(i.safety_stock, 0)) >= sp.REQUIRED_QTY THEN 'HIGH'
              WHEN (COALESCE(i.qty_available, 0) + COALESCE(d.total_deliveries, 0)) >= sp.REQUIRED_QTY THEN 'MODERATE'
              ELSE 'CRITICAL'
            END AS "severity"
        FROM shortage_parts sp
        LEFT JOIN bom_component bc
          ON bc.ROOT_PRODUCT = sp.PRODUCT_ID
         AND bc.PART_NUMBER = sp.CONSTRAINT_PART
        LEFT JOIN inv i
          ON sp.CONSTRAINT_PART = i.PART_NUMBER
         AND sp.PLANT_ID = i.PLANT_ID
        LEFT JOIN del d
          ON sp.CONSTRAINT_PART = d.PART_NUMBER
         AND sp.PLANT_ID = d.PLANT_ID
        ORDER BY "gap" DESC, sp.PRIORITY, sp.WORK_ORDER_ID
    """)