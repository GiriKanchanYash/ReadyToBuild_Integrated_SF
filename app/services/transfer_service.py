"""Service layer for the Cross-Site Inventory Transfer page.

All queries target the views over the dynamic tables in
`CLEAR_TO_BUILD_DEV.INFORMATION_MART` so they automatically reflect the
latest stock + WO + lane state.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from app.db import run_query, DB, SCHEMA


def _f(v: Any) -> float:
    """Snowflake numeric columns come back as Decimal which doesn't mix with
    float in arithmetic; coerce to float (treating None as 0)."""
    if v is None:
        return 0.0
    if isinstance(v, Decimal):
        return float(v)
    try:
        return float(v)
    except (TypeError, ValueError):
        return 0.0


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
        FROM {DB}.{SCHEMA}.PLANT_VW
        WHERE IS_ACTIVE = TRUE
        ORDER BY PLANT_ID
    """)


def get_lanes(origin: str | None = None, dest: str | None = None) -> list[dict]:
    """Active transfer lanes; optional filter by origin/destination plant."""
    where = ["IS_ACTIVE = TRUE"]
    if origin:
        where.append(f"ORIGIN_PLANT_ID = '{origin}'")
    if dest:
        where.append(f"DEST_PLANT_ID = '{dest}'")
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
        FROM {DB}.{SCHEMA}.LANE_VW
        WHERE {' AND '.join(where)}
        ORDER BY ORIGIN_PLANT_ID, DEST_PLANT_ID, TRANSPORT_MODE
    """)


def get_shortage_parts(dest_plant: str | None = None, limit: int = 50) -> list[dict]:
    """Parts that have a destination-plant shortage and at least one source plant
    with available inventory + an active lane back to the destination."""
    where = ["DEST_GAP_QTY > 0"]
    if dest_plant:
        where.append(f"DEST_PLANT_ID = '{dest_plant}'")
    return run_query(f"""
        SELECT
            PART_NUMBER         AS "part_number",
            ANY_VALUE(PART_DESCRIPTION) AS "part_description",
            DEST_PLANT_ID       AS "dest_plant_id",
            ANY_VALUE(DEST_PLANT_NAME)  AS "dest_plant_name",
            MAX(DEST_REQUIRED_QTY) AS "required_qty",
            MAX(DEST_AVAILABLE_QTY) AS "available_qty",
            MAX(DEST_GAP_QTY)   AS "gap_qty",
            MIN(EARLIEST_NEED_DATE) AS "earliest_need_date",
            COUNT(DISTINCT ORIGIN_PLANT_ID) AS "source_plant_count",
            SUM(PROTECTABLE_QTY) AS "total_protectable_qty"
        FROM {DB}.{SCHEMA}.TRANSFER_CANDIDATES_VW
        WHERE {' AND '.join(where)}
        GROUP BY PART_NUMBER, DEST_PLANT_ID
        ORDER BY MAX(DEST_GAP_QTY) DESC, PART_NUMBER
        LIMIT {int(limit)}
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
        FROM {DB}.{SCHEMA}.TRANSFER_CANDIDATES_VW
        WHERE PART_NUMBER = '{part_number}'
          AND DEST_PLANT_ID = '{dest_plant}'
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
        where.append(f"DEST_PLANT_ID = '{dest_plant}'")
    return run_query(f"""
        SELECT
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
        FROM {DB}.{SCHEMA}.STO_VW
        WHERE {' AND '.join(where)}
        ORDER BY EXPECTED_ARRIVAL_DATE NULLS LAST, STO_ID
        LIMIT 200
    """)


def create_sto(
    part_number: str,
    qty: int,
    origin_plant_id: str,
    dest_plant_id: str,
    transport_mode: str,
    requested_delivery_date: str,
    requested_by: str = "AI_AGENT",
) -> dict:
    """Insert a planned STO directly into SAP_STG.EBLG. The dynamic tables will
    pick it up on the next refresh; the API also issues an immediate refresh."""
    sql = f"""
        DECLARE
            v_sto_id VARCHAR;
            v_lane_id VARCHAR;
            v_transit_days NUMBER;
            v_carrier VARCHAR;
            v_unit_cost NUMBER;
        BEGIN
            SELECT 'STO' || LPAD((COALESCE(MAX(CAST(SUBSTR(STO_ID, 4) AS INTEGER)), 700000) + 1)::VARCHAR, 6, '0')
              INTO v_sto_id
              FROM {DB}.SAP_STG.EBLG;

            SELECT LANE_ID, TRANSIT_DAYS, CARRIER_NAME, COST_PER_UNIT_USD
              INTO v_lane_id, v_transit_days, v_carrier, v_unit_cost
              FROM {DB}.SAP_STG.UMLB
              WHERE ORIGIN_PLANT_ID = '{origin_plant_id}'
                AND DEST_PLANT_ID = '{dest_plant_id}'
                AND TRANSPORT_MODE = '{transport_mode}'
                AND IS_ACTIVE = TRUE
              LIMIT 1;

            INSERT INTO {DB}.SAP_STG.EBLG (
                STO_ID, STO_LINE_ID, LANE_ID,
                ORIGIN_PLANT_ID, DEST_PLANT_ID,
                PART_NUMBER, PART_DESCRIPTION, UNIT_OF_MEASURE,
                QTY_REQUESTED, QTY_SHIPPED, QTY_IN_TRANSIT, QTY_RECEIVED,
                TRANSPORT_MODE, CARRIER_NAME, TRANSIT_DAYS,
                UNIT_TRANSFER_COST, TOTAL_TRANSFER_COST, CURRENCY_CODE,
                STO_STATUS, PRIORITY, REASON_CODE,
                REQUESTED_DELIVERY_DATE, PROMISED_DELIVERY_DATE, SHIP_DATE,
                EXPECTED_ARRIVAL_DATE, ACTUAL_DELIVERY_DATE,
                REQUESTED_BY, APPROVED_BY,
                CREATED_AT, UPDATED_AT,
                LOAD_DTS, SOURCE_SYSTEM, BATCH_ID, RECORD_HASH
            ) VALUES (
                :v_sto_id, :v_sto_id || '-010', :v_lane_id,
                '{origin_plant_id}', '{dest_plant_id}',
                '{part_number}', NULL, 'EA',
                {int(qty)}, 0, 0, 0,
                '{transport_mode}', :v_carrier, :v_transit_days,
                :v_unit_cost, {int(qty)} * :v_unit_cost, 'USD',
                'PLANNED', 1, 'SHORTAGE_COVER',
                '{requested_delivery_date}', NULL, NULL, NULL, NULL,
                '{requested_by}', NULL,
                CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP(), 'AI_AGENT', 'AI_AGENT_LOAD',
                MD5_BINARY(:v_sto_id)
            );

            RETURN OBJECT_CONSTRUCT(
                'status', 'SUCCESS',
                'sto_id', :v_sto_id,
                'origin_plant_id', '{origin_plant_id}',
                'dest_plant_id',   '{dest_plant_id}',
                'part_number',     '{part_number}',
                'qty',             {int(qty)},
                'transport_mode',  '{transport_mode}'
            );
        END;
    """
    rows = run_query(sql)
    return rows[0] if rows else {"status": "ERROR", "message": "No rows returned"}


def refresh_transfer_dts() -> dict:
    """Refresh the transfer-related dynamic tables on demand."""
    run_query(f"ALTER DYNAMIC TABLE {DB}.{SCHEMA}.PLANT_DT REFRESH")
    run_query(f"ALTER DYNAMIC TABLE {DB}.{SCHEMA}.LANE_DT REFRESH")
    run_query(f"ALTER DYNAMIC TABLE {DB}.{SCHEMA}.STO_DT REFRESH")
    run_query(f"ALTER DYNAMIC TABLE {DB}.{SCHEMA}.TRANSFER_CANDIDATES_DT REFRESH")
    return {"status": "SUCCESS", "message": "Refreshed transfer dynamic tables"}
