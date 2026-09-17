"""
Fabric (Microsoft Fabric Lakehouse/Warehouse, T-SQL) equivalent of
backend/app/services/transfer_service.py - the Cross-Site Inventory
Transfer page's Snowflake service.

MISSING PIECE THIS FILE FIXES: backend/app/routers/transfer.py previously
imported `app.services.transfer_service` directly (bypassing
service_factory), so every Transfer-page request went to Snowflake even
when the frontend's Data Source dropdown was set to "fabric". This module
is the Fabric implementation that lets the router route through
service_factory.get_transfer_service(data_source), exactly like
routers/ctb.py and routers/ai.py already do.

Same translation conventions as backend/fabric_app/service.py:
  - Snowflake `_VW` views            -> Fabric `_dt` Lakehouse tables,
                                         read via run_query() (Lakehouse,
                                         read-only)
  - LIMIT n                          -> TOP (n)               (placed
                                         right after SELECT)
  - ANY_VALUE(x)                     -> MAX(x)                (constant
                                         within the GROUP BY key here, so
                                         MAX is equivalent)
  - ORDER BY x NULLS LAST             -> ORDER BY CASE WHEN x IS NULL
                                         THEN 1 ELSE 0 END, x   (T-SQL has
                                         no NULLS LAST syntax)
  - IS_ACTIVE = TRUE                  -> IS_ACTIVE = 1          (T-SQL BIT)
  - Snowflake anonymous scripting
    block (DECLARE ... BEGIN ... END,
    OBJECT_CONSTRUCT) for create_sto  -> a Warehouse stored procedure
                                         (SP_CREATE_STO), called the same
                                         way service.create_po() calls
                                         SP_CREATE_PO
  - ALTER DYNAMIC TABLE ... REFRESH
    (x4, one per transfer table)      -> SP_REFRESH_TRANSFER_DATA
  - unescaped f-string literals       -> _sql_escape() on every
                                         user-supplied filter (this
                                         package's existing, safer
                                         convention - see service.py)

IMPORTANT - database objects this file assumes exist:
  Lakehouse ({Config.FABRIC_READYTOBUILD_DATABASE}.{Config.SCHEMA}):
    - plant_dt, lane_dt, transfer_candidates_dt, sto_dt
    with the same columns as Snowflake's PLANT_VW / LANE_VW /
    TRANSFER_CANDIDATES_VW / STO_VW.
  Warehouse ({Config.FABRIC_READYTOBUILD_WAREHOUSE_DATABASE}.{Config.DEFAULT_SCHEMA}):
    - SP_CREATE_STO, SP_REFRESH_TRANSFER_DATA
  None of these are created by this Python file - see
  fabric_warehouse_setup.sql for the stored-procedure templates, and the
  "IMPORTANT" note in service.py for the same caveat that already applies
  to SP_CREATE_PO / SP_REFRESH_CTB_DATA / SP_UPDATE_WORK_ORDER_PRIORITY.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from config import Config
from db import execute_query, run_warehouse_df, run_warehouse_non_query

# ---------------------------------------------------------------------------
# Database / schema targets (same pattern as service.py)
# ---------------------------------------------------------------------------
DB = Config.FABRIC_READYTOBUILD_DATABASE          # Lakehouse (read-only analytics)
SCHEMA = Config.SCHEMA                             # e.g. "information_mart"
WH_DB = Config.FABRIC_READYTOBUILD_WAREHOUSE_DATABASE  # Warehouse (read + write)
WH_SCHEMA = Config.DEFAULT_SCHEMA                  # e.g. "dbo"


# ---------------------------------------------------------------------------
# Query helpers (mirrors service.py's run_query/run_exec exactly so this
# file behaves identically whether service.py's helpers are reused or
# duplicated - duplicated here to keep this module self-contained/importable
# on its own, same as service.py itself does relative to db.py)
# ---------------------------------------------------------------------------

def _to_records(df: pd.DataFrame | None) -> list[dict]:
    """Convert a query result DataFrame into JSON-safe dicts.

    See service.py's _to_records for why +/-Infinity must be scrubbed here
    too, not just NaN (Starlette's JSONResponse uses allow_nan=False).
    """
    if df is None or df.empty:
        return []
    df = df.replace([np.inf, -np.inf], np.nan)
    return df.where(pd.notnull(df), None).to_dict("records")


def run_query(sql: str) -> list[dict]:
    """Read query against the Lakehouse."""
    df = execute_query(sql)
    return _to_records(df)


def run_exec(sql: str) -> list[dict]:
    """EXEC a stored procedure against the Warehouse (read+write endpoint)."""
    df = run_warehouse_df(sql)
    return _to_records(df)


def _sql_escape(value) -> str:
    if value is None:
        return ""
    return str(value).replace("'", "''")


def _f(v: Any) -> float:
    """Fabric/T-SQL numeric columns can come back as Decimal, which doesn't
    mix with float in arithmetic; coerce to float (treating None as 0)."""
    if v is None:
        return 0.0
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------------------
# Reads (Lakehouse)
# ---------------------------------------------------------------------------

def get_plants() -> list[dict]:
    """Plants with geo coordinates so the UI can render the facilities map."""
    return run_query(f"""
        SELECT
            PLANT_ID        AS "plant_id",
            PLANT_NAME      AS "plant_name",
            PLANT_TYPE      AS "plant_type",
            CITY            AS "city",
            STATE_PROVINCE  AS "state",
            COUNTRY_CODE    AS "country_code",
            COUNTRY_NAME    AS "country_name",
            REGION          AS "region",
            LATITUDE        AS "latitude",
            LONGITUDE       AS "longitude"
        FROM {DB}.{SCHEMA}.plant_dt
        WHERE IS_ACTIVE = 1
        ORDER BY PLANT_ID
    """)


def get_lanes(origin: str | None = None, dest: str | None = None) -> list[dict]:
    """Active transfer lanes; optional filter by origin/destination plant."""
    where = ["IS_ACTIVE = 1"]
    if origin:
        where.append(f"ORIGIN_PLANT_ID = '{_sql_escape(origin)}'")
    if dest:
        where.append(f"DEST_PLANT_ID = '{_sql_escape(dest)}'")
    return run_query(f"""
        SELECT
            LANE_ID            AS "lane_id",
            ORIGIN_PLANT_ID    AS "origin_plant_id",
            ORIGIN_PLANT_NAME  AS "origin_plant_name",
            ORIGIN_LATITUDE    AS "origin_lat",
            ORIGIN_LONGITUDE   AS "origin_lon",
            DEST_PLANT_ID      AS "dest_plant_id",
            DEST_PLANT_NAME    AS "dest_plant_name",
            DEST_LATITUDE      AS "dest_lat",
            DEST_LONGITUDE     AS "dest_lon",
            TRANSPORT_MODE     AS "transport_mode",
            DISTANCE_KM        AS "distance_km",
            TRANSIT_DAYS       AS "transit_days",
            TRANSIT_COST_USD   AS "transit_cost_usd",
            COST_PER_UNIT_USD  AS "cost_per_unit_usd",
            CARRIER_NAME       AS "carrier_name",
            SERVICE_LEVEL      AS "service_level",
            CO2_KG_PER_UNIT    AS "co2_kg_per_unit",
            RELIABILITY_PCT    AS "reliability_pct"
        FROM {DB}.{SCHEMA}.lane_dt
        WHERE {' AND '.join(where)}
        ORDER BY ORIGIN_PLANT_ID, DEST_PLANT_ID, TRANSPORT_MODE
    """)


def get_shortage_parts(dest_plant: str | None = None, limit: int = 50) -> list[dict]:
    """Parts that have a destination-plant shortage and at least one source plant
    with available inventory + an active lane back to the destination."""
    where = ["DEST_GAP_QTY > 0"]
    if dest_plant:
        where.append(f"DEST_PLANT_ID = '{_sql_escape(dest_plant)}'")
    return run_query(f"""
        SELECT TOP ({int(limit)})
               PART_NUMBER         AS "part_number",
               MAX(PART_DESCRIPTION) AS "part_description",
               DEST_PLANT_ID       AS "dest_plant_id",
               MAX(DEST_PLANT_NAME)  AS "dest_plant_name",
               MAX(DEST_REQUIRED_QTY) AS "required_qty",
               MAX(DEST_AVAILABLE_QTY) AS "available_qty",
               MAX(DEST_GAP_QTY)   AS "gap_qty",
               MIN(EARLIEST_NEED_DATE) AS "earliest_need_date",
               COUNT(DISTINCT ORIGIN_PLANT_ID) AS "source_plant_count",
               SUM(PROTECTABLE_QTY) AS "total_protectable_qty"
        FROM {DB}.{SCHEMA}.transfer_candidates_dt
        WHERE {' AND '.join(where)}
        GROUP BY PART_NUMBER, DEST_PLANT_ID
        ORDER BY MAX(DEST_GAP_QTY) DESC, PART_NUMBER
    """)


def get_transfer_candidates(
    part_number: str,
    dest_plant: str,
    weight_impact: float = 0.5,
    weight_speed: float = 0.3,
    weight_cost: float = 0.2,
) -> dict:
    """For (part_number, dest_plant), return KPIs + ranked transfer candidates.

    Score = weight_impact * impact% - weight_speed * speed_penalty - weight_cost * cost_penalty
    where impact% is normalized to 100 (full coverage of gap), and the speed/cost
    penalties are normalized to typical maximums.

    NOTE: the scoring/ranking logic below is plain Python (no SQL dialect
    dependency) and is ported 1:1 from
    app/services/transfer_service.py::get_transfer_candidates - only the
    initial SELECT differs (Snowflake -> T-SQL).
    """
    candidates = run_query(f"""
        SELECT
            PART_NUMBER          AS "part_number",
            PART_DESCRIPTION     AS "part_description",
            DEST_PLANT_ID        AS "dest_plant_id",
            DEST_PLANT_NAME      AS "dest_plant_name",
            DEST_LATITUDE        AS "dest_lat",
            DEST_LONGITUDE       AS "dest_lon",
            DEST_AVAILABLE_QTY   AS "dest_available_qty",
            DEST_REQUIRED_QTY    AS "dest_required_qty",
            DEST_GAP_QTY         AS "dest_gap_qty",
            QTY_PER_ASSEMBLY     AS "qty_per_assembly",
            EARLIEST_NEED_DATE   AS "earliest_need_date",
            ORIGIN_PLANT_ID      AS "origin_plant_id",
            ORIGIN_PLANT_NAME    AS "origin_plant_name",
            ORIGIN_LATITUDE      AS "origin_lat",
            ORIGIN_LONGITUDE     AS "origin_lon",
            ORIGIN_AVAILABLE_QTY AS "origin_available_qty",
            ORIGIN_SAFETY_STOCK  AS "origin_safety_stock",
            TRANSPORT_MODE       AS "transport_mode",
            LANE_ID              AS "lane_id",
            CARRIER_NAME         AS "carrier_name",
            DISTANCE_KM          AS "distance_km",
            TRANSIT_DAYS         AS "transit_days",
            COST_PER_UNIT_USD    AS "cost_per_unit_usd",
            PROTECTABLE_QTY      AS "protectable_qty",
            PROTECTED_BUILDS     AS "protected_builds"
        FROM {DB}.{SCHEMA}.transfer_candidates_dt
        WHERE PART_NUMBER = '{_sql_escape(part_number)}'
          AND DEST_PLANT_ID = '{_sql_escape(dest_plant)}'
        ORDER BY ORIGIN_PLANT_ID, TRANSPORT_MODE
    """)

    if not candidates:
        return {
            "part_number": part_number,
            "dest_plant_id": dest_plant,
            "kpi": {
                "total_inventory": 0,
                "total_demand": 0,
                "coverage_pct": 0,
                "earliest_need_date": None,
                "destination_available": 0,
                "gap_qty": 0,
            },
            "candidates": [],
        }

    head = candidates[0]
    total_demand = _f(head["dest_required_qty"])
    dest_avail = _f(head["dest_available_qty"])
    earliest_need = head["earliest_need_date"]
    part_description = head["part_description"]
    dest_lat = _f(head["dest_lat"])
    dest_lon = _f(head["dest_lon"])
    dest_plant_name = head["dest_plant_name"]

    # Aggregate origin-side inventory (one origin can have multiple modes).
    origin_inventory: dict[str, float] = {}
    for c in candidates:
        oid = c["origin_plant_id"]
        if oid not in origin_inventory:
            origin_inventory[oid] = _f(c["origin_available_qty"])
    total_inventory = sum(origin_inventory.values()) + dest_avail
    coverage_pct = round(min(100.0, 100.0 * (total_inventory / total_demand)), 1) if total_demand else 0.0

    # Compute scored, mode-best candidate per origin plant.
    by_origin: dict[str, list[dict]] = {}
    for c in candidates:
        by_origin.setdefault(c["origin_plant_id"], []).append(c)

    scored: list[dict] = []
    for _origin_id, rows in by_origin.items():
        # Pick the "preferred" mode per origin (cheapest unit cost first, then fastest).
        rows_sorted = sorted(
            rows,
            key=lambda r: (_f(r["cost_per_unit_usd"]), _f(r["transit_days"])),
        )
        chosen = rows_sorted[0]
        gap = _f(head["dest_gap_qty"]) or 1.0
        protectable = _f(chosen["protectable_qty"])
        protected_builds = _f(chosen["protected_builds"])
        chosen_transit = _f(chosen["transit_days"])
        chosen_cost = _f(chosen["cost_per_unit_usd"])
        impact_pct = (protectable / gap) * 100.0 if gap else 0.0
        speed_penalty = min(100.0, chosen_transit / 14.0 * 100.0)
        cost_penalty = min(100.0, chosen_cost / 50.0 * 100.0)
        score = round(
            weight_impact * impact_pct
            - weight_speed * speed_penalty
            - weight_cost * cost_penalty,
            1,
        )
        # Normalize all numeric fields on the candidate record so the JSON
        # response and downstream React types are consistent.
        normalized = {
            "part_number": chosen["part_number"],
            "part_description": chosen["part_description"],
            "dest_plant_id": chosen["dest_plant_id"],
            "dest_plant_name": chosen["dest_plant_name"],
            "dest_lat": _f(chosen["dest_lat"]),
            "dest_lon": _f(chosen["dest_lon"]),
            "dest_available_qty": _f(chosen["dest_available_qty"]),
            "dest_required_qty": _f(chosen["dest_required_qty"]),
            "dest_gap_qty": _f(chosen["dest_gap_qty"]),
            "qty_per_assembly": _f(chosen["qty_per_assembly"]),
            "earliest_need_date": (
                str(chosen["earliest_need_date"])
                if chosen["earliest_need_date"]
                else None
            ),
            "origin_plant_id": chosen["origin_plant_id"],
            "origin_plant_name": chosen["origin_plant_name"],
            "origin_lat": _f(chosen["origin_lat"]),
            "origin_lon": _f(chosen["origin_lon"]),
            "origin_available_qty": _f(chosen["origin_available_qty"]),
            "origin_safety_stock": _f(chosen["origin_safety_stock"]),
            "transport_mode": chosen["transport_mode"],
            "lane_id": chosen["lane_id"],
            "carrier_name": chosen["carrier_name"],
            "distance_km": _f(chosen["distance_km"]),
            "transit_days": chosen_transit,
            "cost_per_unit_usd": chosen_cost,
            "protectable_qty": protectable,
            "protected_builds": int(protected_builds),
            "all_modes": [
                {
                    "transport_mode": r["transport_mode"],
                    "lane_id": r["lane_id"],
                    "carrier_name": r["carrier_name"],
                    "transit_days": _f(r["transit_days"]),
                    "cost_per_unit_usd": _f(r["cost_per_unit_usd"]),
                    "distance_km": _f(r["distance_km"]),
                }
                for r in rows_sorted
            ],
            "score": score,
            "impact_pct": round(impact_pct, 1),
        }
        scored.append(normalized)

    scored.sort(key=lambda r: r["score"], reverse=True)

    return {
        "part_number": part_number,
        "part_description": part_description,
        "dest_plant_id": dest_plant,
        "dest_plant_name": dest_plant_name,
        "dest_lat": dest_lat,
        "dest_lon": dest_lon,
        "kpi": {
            "total_inventory": int(total_inventory),
            "total_demand": int(total_demand),
            "coverage_pct": coverage_pct,
            "earliest_need_date": str(earliest_need) if earliest_need else None,
            "destination_available": int(dest_avail),
            "gap_qty": int(_f(head["dest_gap_qty"])),
            "qty_per_assembly": _f(head["qty_per_assembly"]) or 1.0,
        },
        "candidates": scored,
    }


def get_open_stos(dest_plant: str | None = None) -> list[dict]:
    """Active stock transfer orders (anything not yet delivered)."""
    where = ["STO_STATUS NOT IN ('DELIVERED', 'CLOSED')"]
    if dest_plant:
        where.append(f"DEST_PLANT_ID = '{_sql_escape(dest_plant)}'")
    return run_query(f"""
        SELECT TOP (200)
            STO_ID                    AS "sto_id",
            STO_LINE_ID               AS "sto_line_id",
            ORIGIN_PLANT_ID           AS "origin_plant_id",
            ORIGIN_PLANT_NAME         AS "origin_plant_name",
            DEST_PLANT_ID             AS "dest_plant_id",
            DEST_PLANT_NAME           AS "dest_plant_name",
            PART_NUMBER               AS "part_number",
            PART_DESCRIPTION          AS "part_description",
            QTY_REQUESTED             AS "qty_requested",
            QTY_SHIPPED               AS "qty_shipped",
            QTY_IN_TRANSIT            AS "qty_in_transit",
            QTY_RECEIVED              AS "qty_received",
            TRANSPORT_MODE            AS "transport_mode",
            CARRIER_NAME              AS "carrier_name",
            TRANSIT_DAYS              AS "transit_days",
            UNIT_TRANSFER_COST        AS "unit_transfer_cost",
            TOTAL_TRANSFER_COST       AS "total_transfer_cost",
            STO_STATUS                AS "sto_status",
            REASON_CODE               AS "reason_code",
            REQUESTED_DELIVERY_DATE   AS "requested_delivery_date",
            EXPECTED_ARRIVAL_DATE     AS "expected_arrival_date",
            REQUESTED_BY              AS "requested_by"
        FROM {DB}.{SCHEMA}.sto_dt
        WHERE {' AND '.join(where)}
        ORDER BY
            CASE WHEN EXPECTED_ARRIVAL_DATE IS NULL THEN 1 ELSE 0 END,
            EXPECTED_ARRIVAL_DATE,
            STO_ID
    """)


# ---------------------------------------------------------------------------
# Writes (Warehouse stored procedures)
#
# IMPORTANT: SP_CREATE_STO / SP_REFRESH_TRANSFER_DATA must exist in
# {WH_DB}.{WH_SCHEMA} with parameters matching below - see
# fabric_warehouse_setup.sql. This is a database-side task and cannot be
# done from this Python file alone (same caveat as service.py's
# SP_CREATE_PO / SP_REFRESH_CTB_DATA / SP_UPDATE_WORK_ORDER_PRIORITY).
# ---------------------------------------------------------------------------

def create_sto(
    part_number: str,
    qty: int,
    origin_plant_id: str,
    dest_plant_id: str,
    transport_mode: str,
    requested_delivery_date: str,
    requested_by: str = "AI_AGENT",
) -> dict:
    """Create a planned STO. Snowflake's version runs an inline scripting
    block that looks up the matching lane and inserts directly into the
    staging table; Fabric writes only go through Warehouse stored
    procedures (same pattern as create_po), so this calls SP_CREATE_STO,
    which is expected to perform the same lane lookup + insert server-side
    and return the new STO_ID."""
    rows = run_exec(f"""
        EXEC {WH_DB}.{WH_SCHEMA}.SP_CREATE_STO
            @PartNumber             = '{_sql_escape(part_number)}',
            @Qty                    = {int(qty)},
            @OriginPlantId          = '{_sql_escape(origin_plant_id)}',
            @DestPlantId            = '{_sql_escape(dest_plant_id)}',
            @TransportMode          = '{_sql_escape(transport_mode)}',
            @RequestedDeliveryDate  = CAST('{_sql_escape(requested_delivery_date)}' AS DATE),
            @RequestedBy            = '{_sql_escape(requested_by)}'
    """)
    if rows:
        row = rows[0]
        return {
            "status": row.get("STATUS") or row.get("status") or "SUCCESS",
            "sto_id": row.get("STO_ID") or row.get("sto_id"),
            "origin_plant_id": origin_plant_id,
            "dest_plant_id": dest_plant_id,
            "part_number": part_number,
            "qty": int(qty),
            "transport_mode": transport_mode,
        }
    return {"status": "ERROR", "message": "No rows returned"}


def refresh_transfer_dts() -> dict:
    """Refresh the transfer-related Lakehouse tables on demand."""
    rows = run_exec(f"EXEC {WH_DB}.{WH_SCHEMA}.SP_REFRESH_TRANSFER_DATA")
    if rows:
        return rows[0]
    return {"status": "SUCCESS", "message": "Refreshed transfer dynamic tables"}
