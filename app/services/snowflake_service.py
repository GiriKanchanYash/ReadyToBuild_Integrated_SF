from collections import defaultdict

from app.db import run_query, DB, SCHEMA


def _lower_keys(row: dict) -> dict:
    return {str(k).lower(): v for k, v in row.items()}


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
        clause += f" AND wo.PRODUCT_ID = '{product}'"
    if plant:
        clause += f" AND wo.PLANT_ID = '{plant}'"
    if week:
        clause += f" AND DATE_TRUNC('WEEK', wo.PLANNED_START_DATE) = '{week}'"
    return clause


def get_filter_options() -> dict:
    products = run_query(f"""
        SELECT DISTINCT PRODUCT_ID FROM {DB}.{SCHEMA}.WORK_ORDER
        WHERE PRODUCT_ID IS NOT NULL ORDER BY PRODUCT_ID
    """)
    plants = run_query(f"""
        WITH wo_plants AS (
            SELECT DISTINCT PLANT_ID
            FROM {DB}.{SCHEMA}.WORK_ORDER
            WHERE PLANT_ID IS NOT NULL
        ),
        plant_names AS (
            SELECT
                PLANT_ID,
                MAX(NULLIF(TRIM(PLANT_NAME), '')) AS PLANT_NAME
            FROM {DB}.{SCHEMA}.PLANT_SUMMARY_VW
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
        SELECT DISTINCT DATE_TRUNC('WEEK', PLANNED_START_DATE)::VARCHAR AS WEEK_START
        FROM {DB}.{SCHEMA}.WORK_ORDER
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
        FROM {DB}.{SCHEMA}.WORK_ORDER wo
        LEFT JOIN {DB}.{SCHEMA}.CTB_RESULT_VW ctb ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
        WHERE 1=1 {where}
    ),
    shortage_parts AS (
        SELECT COUNT(DISTINCT p.DEMAND_PART_NUMBER) AS CNT
        FROM {DB}.{SCHEMA}.PEGGING_VW p
        JOIN {DB}.{SCHEMA}.WORK_ORDER wo ON p.WORK_ORDER_ID = wo.WORK_ORDER_ID
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
        FROM {DB}.{SCHEMA}.CTB_RESULT_VW ctb
        JOIN {DB}.{SCHEMA}.WORK_ORDER wo ON ctb.WORK_ORDER_ID = wo.WORK_ORDER_ID
        WHERE 1=1 {where}
        GROUP BY ctb.CTB_STATUS ORDER BY COUNT(*) DESC
    """)


def get_top_shortages(product: str | None = None, plant: str | None = None, week: str | None = None, limit: int = 10) -> list[dict]:
    where = _wo_where(product, plant, week)
    return run_query(f"""
        SELECT p.DEMAND_PART_NUMBER AS "part_number",
               SUM(p.SHORTAGE_QTY) AS "shortage_qty",
               COUNT(DISTINCT p.WORK_ORDER_ID) AS "affected_orders"
        FROM {DB}.{SCHEMA}.PEGGING_VW p
        JOIN {DB}.{SCHEMA}.WORK_ORDER wo ON p.WORK_ORDER_ID = wo.WORK_ORDER_ID
        WHERE p.SHORTAGE_QTY > 0 {where}
        GROUP BY p.DEMAND_PART_NUMBER
        ORDER BY SUM(p.SHORTAGE_QTY) DESC LIMIT {limit}
    """)


def get_ctb_by_priority(product: str | None = None, plant: str | None = None, week: str | None = None) -> list[dict]:
    where = _wo_where(product, plant, week)
    return run_query(f"""
        SELECT wo.PRIORITY AS "PRIORITY",
               SUM(CASE WHEN ctb.CTB_STATUS='READY'   THEN 1 ELSE 0 END) AS "ready",
               SUM(CASE WHEN ctb.CTB_STATUS='PARTIAL' THEN 1 ELSE 0 END) AS "partial",
               SUM(CASE WHEN ctb.CTB_STATUS='BLOCKED' THEN 1 ELSE 0 END) AS "blocked"
        FROM {DB}.{SCHEMA}.WORK_ORDER wo
        JOIN {DB}.{SCHEMA}.CTB_RESULT_VW ctb ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
        WHERE 1=1 {where}
        GROUP BY wo.PRIORITY ORDER BY wo.PRIORITY
    """)


def get_parts_inventory(
    product: str | None = None,
    plant: str | None = None,
    week: str | None = None,
    include_deliveries: bool = True,
    search: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """
    Next 16 weeks running ending-balance per part (not WO-priority-based; demand from open WOs).
    suggest_reorder marks the calendar week to place a PO: first projected risk week minus lead time.
    Risk week = earliest week where balance goes below safety stock (if SS > 0) or below zero.
    """
    where = _wo_where(product, plant, week)
    # Week filter must NOT apply to demand_by_week: the grid is 16 calendar weeks from today;
    # each column needs demand from WOs whose PLANNED_START falls in that week. Restricting WOs
    # to the dashboard week puts all demand in one bucket and leaves other weeks flat.
    where_demand = _wo_where(product, plant, None)

    plant_inv_filter = f" AND PLANT_ID = '{plant}'" if plant else ""
    plant_asn_filter = f" AND RECEIVING_PLANT_ID = '{plant}'" if plant else ""

    safe_search = (search or "").replace("'", "''").strip()
    search_filter = ""
    if safe_search:
        search_filter = f" AND (pl.PART_NUMBER ILIKE '%{safe_search}%' OR COALESCE(pl.DESCRIPTION,'') ILIKE '%{safe_search}%')"

    inc_expr = "COALESCE(dlv.delivery_qty, 0)" if include_deliveries else "0"

    return run_query(f"""
        WITH weeks AS (
            SELECT
                ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1 AS week_idx,
                DATEADD('WEEK', ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, DATE_TRUNC('WEEK', CURRENT_DATE))::DATE AS week_start
            FROM TABLE(GENERATOR(ROWCOUNT => 16))
        ),
        part_desc AS (
            SELECT
                PART_NUMBER,
                MAX(NULLIF(TRIM(PART_DESCRIPTION), '')) AS PART_DESCRIPTION
            FROM {DB}.{SCHEMA}.INVENTORY
            GROUP BY PART_NUMBER
        ),
        parts_list AS (
            SELECT
                PART_NUMBER,
                DESCRIPTION
            FROM (
                SELECT
                    b.CHILD_PART_NUMBER AS PART_NUMBER,
                    COALESCE(
                        MAX(NULLIF(TRIM(b.CHILD_PART_DESCRIPTION), '')),
                        MAX(pd.PART_DESCRIPTION),
                        b.CHILD_PART_NUMBER
                    ) AS DESCRIPTION
                FROM {DB}.{SCHEMA}.WORK_ORDER wo
                JOIN {DB}.{SCHEMA}.BOM b
                  ON wo.PRODUCT_ID = b.PARENT_PART_NUMBER
                 AND b.BOM_STATUS = 'ACTIVE'
                LEFT JOIN part_desc pd
                  ON pd.PART_NUMBER = b.CHILD_PART_NUMBER
                WHERE 1=1 {where}
                GROUP BY b.CHILD_PART_NUMBER
            )
            ORDER BY PART_NUMBER
            LIMIT {int(limit)}
        ),
        inv AS (
            SELECT
                PART_NUMBER,
                SUM(COALESCE(QTY_AVAILABLE, 0)) AS QTY_AVAILABLE,
                SUM(COALESCE(SAFETY_STOCK, 0)) AS SAFETY_STOCK
            FROM {DB}.{SCHEMA}.INVENTORY
            WHERE 1=1 {plant_inv_filter}
            GROUP BY PART_NUMBER
        ),
        demand_by_week AS (
            SELECT
                b.CHILD_PART_NUMBER AS PART_NUMBER,
                DATE_TRUNC('WEEK', wo.PLANNED_START_DATE)::DATE AS WEEK_START,
                SUM(wo.PLANNED_QTY * b.QTY_PER_ASSEMBLY) AS DEMAND_QTY
            FROM {DB}.{SCHEMA}.WORK_ORDER wo
            JOIN {DB}.{SCHEMA}.BOM b
              ON wo.PRODUCT_ID = b.PARENT_PART_NUMBER
             AND b.BOM_STATUS = 'ACTIVE'
            WHERE wo.PLANNED_START_DATE IS NOT NULL
              AND wo.WORK_ORDER_STATUS NOT IN ('COMPLETED', 'CANCELLED', 'CLOSED')
              {where_demand}
            GROUP BY b.CHILD_PART_NUMBER, DATE_TRUNC('WEEK', wo.PLANNED_START_DATE)::DATE
        ),
        delivery_by_week AS (
            SELECT
                PART_NUMBER,
                DATE_TRUNC('WEEK', EXPECTED_ARRIVAL_DATE)::DATE AS WEEK_START,
                SUM(QTY_SHIPPED) AS DELIVERY_QTY
            FROM {DB}.{SCHEMA}.ASN_IN_TRANSIT_VW
            WHERE ASN_STATUS IN ('SHIPPED', 'IN_TRANSIT')
              AND EXPECTED_ARRIVAL_DATE IS NOT NULL
              AND QTY_SHIPPED > 0
              {plant_asn_filter}
            GROUP BY PART_NUMBER, DATE_TRUNC('WEEK', EXPECTED_ARRIVAL_DATE)::DATE
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
                (INV + SUM(INC_QTY - DEMAND_QTY) OVER (PARTITION BY PART_NUMBER ORDER BY week_idx ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS BALANCE,
                CASE WHEN INC_QTY > 0 THEN 1 ELSE 0 END AS HAS_DELIVERY
            FROM grid
        ),
        part_lead AS (
            SELECT
                PART_NUMBER,
                COALESCE(MAX(AVG_LEAD_TIME_DAYS), 7)::FLOAT AS LEAD_DAYS
            FROM {DB}.{SCHEMA}.PART_SUPPLY_VW
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
                GREATEST(
                    0,
                    rw.risk_week_idx - CEIL(COALESCE(pl.LEAD_DAYS, 7) / 7)::INTEGER
                ) AS place_order_week_idx
            FROM risk_weeks rw
            LEFT JOIN part_lead pl ON pl.PART_NUMBER = rw.PART_NUMBER
        )
        SELECT
            bal.PART_NUMBER AS "part_number",
            COALESCE(bal.DESCRIPTION, '') AS "description",
            bal.INV AS "inv",
            bal.SS AS "ss",
            TO_CHAR(bal.week_start, 'MM/DD') AS "week_label",
            TO_CHAR(bal.week_start, 'YYYY-MM-DD') AS "week_start",
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


def get_work_orders(product: str | None = None, plant: str | None = None, week: str | None = None) -> list[dict]:
    where = _wo_where(product, plant, week)
    return run_query(f"""
        SELECT wo.WORK_ORDER_ID, wo.PRODUCT_ID, wo.PLANT_ID,
               wo.PLANNED_QTY, wo.PRIORITY, wo.WORK_ORDER_STATUS,
               wo.PLANNED_START_DATE::VARCHAR AS "planned_start_date",
               COALESCE(ctb.CTB_STATUS,'UNKNOWN')  AS "ctb_status",
               COALESCE(ctb.CAN_BUILD_QTY,0)       AS "can_build_qty",
               COALESCE(ctb.REQUESTED_QTY,0)       AS "requested_qty"
        FROM {DB}.{SCHEMA}.WORK_ORDER wo
        LEFT JOIN {DB}.{SCHEMA}.CTB_RESULT_VW ctb ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
        WHERE 1=1 {where}
        ORDER BY wo.PRIORITY, wo.PLANNED_START_DATE
    """)


def get_work_order_detail(wo_id: str) -> dict:
    rows = run_query(f"""
        SELECT wo.WORK_ORDER_ID, wo.PRODUCT_ID, wo.PLANT_ID,
               wo.WORK_ORDER_TYPE, wo.WORK_ORDER_STATUS,
               wo.PLANNED_QTY, wo.PRIORITY,
               wo.PLANNED_START_DATE::VARCHAR AS "planned_start_date",
               wo.PLANNED_END_DATE::VARCHAR   AS "planned_end_date",
               COALESCE(ctb.CTB_STATUS,'UNKNOWN') AS "ctb_status",
               COALESCE(ctb.PRODUCTION_STATUS,'PENDING') AS "production_status",
               COALESCE(ctb.CAN_BUILD_QTY,0)      AS "can_build_qty"
        FROM {DB}.{SCHEMA}.WORK_ORDER wo
        LEFT JOIN {DB}.{SCHEMA}.CTB_RESULT_VW ctb ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
        WHERE wo.WORK_ORDER_ID = %s
    """, (wo_id,))
    if not rows:
        return {}
    wo = rows[0]

    parts = run_query(f"""
        SELECT b.CHILD_PART_NUMBER AS "part_number",
               b.QTY_PER_ASSEMBLY AS "qty_per_assembly",
               b.QTY_PER_ASSEMBLY * {wo.get('PLANNED_QTY',0)} AS "required_qty",
               COALESCE(inv.qty_available, 0)  AS "available_qty",
               COALESCE(inv.safety_stock, 0)   AS "safety_stock",
               COALESCE(del.incoming_qty, 0)   AS "incoming_qty",
               CASE
                 WHEN COALESCE(inv.qty_available,0) >= b.QTY_PER_ASSEMBLY * {wo.get('PLANNED_QTY',0)} THEN 'OK'
                 WHEN COALESCE(inv.qty_available,0) + COALESCE(del.incoming_qty,0) >= b.QTY_PER_ASSEMBLY * {wo.get('PLANNED_QTY',0)} THEN 'WITH_DELIVERY'
                 ELSE 'SHORT'
               END AS "part_status"
        FROM {DB}.{SCHEMA}.BOM b
        LEFT JOIN (SELECT PART_NUMBER, SUM(QTY_AVAILABLE) AS qty_available, SUM(SAFETY_STOCK) AS safety_stock
                   FROM {DB}.{SCHEMA}.INVENTORY GROUP BY PART_NUMBER) inv
          ON inv.PART_NUMBER = b.CHILD_PART_NUMBER
        LEFT JOIN (SELECT PART_NUMBER, SUM(QTY_SHIPPED) AS incoming_qty
                   FROM {DB}.{SCHEMA}.ASN_IN_TRANSIT_VW
                   WHERE ASN_STATUS IN ('SHIPPED','IN_TRANSIT') GROUP BY PART_NUMBER) del
          ON del.PART_NUMBER = b.CHILD_PART_NUMBER
        WHERE b.PARENT_PART_NUMBER = %s AND b.BOM_STATUS = 'ACTIVE'
        ORDER BY "part_status" DESC, "required_qty" DESC
    """, (wo.get("PRODUCT_ID", ""),))

    weekly_rows = run_query(f"""
        WITH ts AS (
            SELECT
                w.WORK_ORDER_ID,
                w.PRODUCT_ID,
                w.PLANT_ID,
                w.PRIORITY,
                w.PLANNED_QTY,
                w.PLANNED_START_DATE
            FROM {DB}.{SCHEMA}.WORK_ORDER w
            WHERE w.WORK_ORDER_ID = %s
        ),
        seq_max AS (
            SELECT COALESCE(MAX(bs.BUILD_WEEK), 4) AS MAX_WEEK
            FROM {DB}.{SCHEMA}.BUILD_SEQUENCE bs
            CROSS JOIN ts t
            WHERE bs.PRODUCT_ID = t.PRODUCT_ID
        ),
        -- Schedule rows from BUILD_SEQUENCE; qty from BOM_EXPLODE (same as CTB).
        seq_rows AS (
            SELECT
                bs.BUILD_WEEK,
                bs.BUILD_STAGE,
                bs.PART_NUMBER,
                COALESCE(be.EXTENDED_QTY, bs.QTY_REQUIRED) AS QTY_PER_UNIT,
                MAX(COALESCE(bs.USING_SAFETY_STOCK, 0)) AS USING_SAFETY_STOCK
            FROM {DB}.{SCHEMA}.BUILD_SEQUENCE bs
            CROSS JOIN ts t
            LEFT JOIN {DB}.{SCHEMA}.BOM_EXPLODE_VW be
              ON be.ROOT_PRODUCT = t.PRODUCT_ID
             AND be.CHILD_PART_NUMBER = bs.PART_NUMBER
            WHERE bs.PRODUCT_ID = t.PRODUCT_ID
            GROUP BY bs.BUILD_WEEK, bs.BUILD_STAGE, bs.PART_NUMBER,
                     COALESCE(be.EXTENDED_QTY, bs.QTY_REQUIRED)
        ),
        -- Any exploded BOM part missing from BUILD_SEQUENCE (e.g. after a
        -- partial reload) still appears so CTB-blocking parts are visible.
        missing_bom AS (
            SELECT
                LEAST(be.BOM_LEVEL, sm.MAX_WEEK) AS BUILD_WEEK,
                CASE be.BOM_LEVEL
                    WHEN 1 THEN 'Frame & Foundation'
                    WHEN 2 THEN 'Power Installation'
                    WHEN 3 THEN 'Systems Integration'
                    ELSE 'Final Assembly'
                END AS BUILD_STAGE,
                be.CHILD_PART_NUMBER AS PART_NUMBER,
                be.EXTENDED_QTY AS QTY_PER_UNIT,
                0 AS USING_SAFETY_STOCK
            FROM {DB}.{SCHEMA}.BOM_EXPLODE_VW be
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
            FROM {DB}.{SCHEMA}.INVENTORY
            GROUP BY PART_NUMBER
        ),
        asn_before AS (
            SELECT
                a.PART_NUMBER,
                SUM(COALESCE(a.QTY_SHIPPED, 0)) AS INCOMING_QTY
            FROM {DB}.{SCHEMA}.ASN_IN_TRANSIT_VW a
            CROSS JOIN ts t
            WHERE a.ASN_STATUS IN ('SHIPPED', 'IN_TRANSIT')
              AND a.EXPECTED_ARRIVAL_DATE < DATE_TRUNC('WEEK', t.PLANNED_START_DATE)
            GROUP BY a.PART_NUMBER
        ),
        ranked_wo AS (
            SELECT
                wo.WORK_ORDER_ID,
                wo.PRODUCT_ID,
                wo.PLANNED_QTY,
                ROW_NUMBER() OVER (
                    ORDER BY DATE_TRUNC('WEEK', wo.PLANNED_START_DATE),
                             wo.PRIORITY,
                             wo.WORK_ORDER_ID
                ) AS WO_RANK
            FROM {DB}.{SCHEMA}.WORK_ORDER wo
            LEFT JOIN {DB}.{SCHEMA}.CTB_RESULT_VW ctb
              ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
            WHERE COALESCE(ctb.CTB_STATUS, 'PENDING') NOT IN ('COMPLETE', 'CANCELLED')
        ),
        hp AS (
            SELECT
                bs.PART_NUMBER,
                SUM(bs.QTY_REQUIRED * hp_wo.PLANNED_QTY) AS HP_USED
            FROM ranked_wo hp_wo
            JOIN {DB}.{SCHEMA}.BUILD_SEQUENCE bs
              ON bs.PRODUCT_ID = hp_wo.PRODUCT_ID
            CROSS JOIN ts t
            CROSS JOIN ranked_wo cur
            WHERE cur.WORK_ORDER_ID = t.WORK_ORDER_ID
              AND hp_wo.WO_RANK < cur.WO_RANK
            GROUP BY bs.PART_NUMBER
        ),
        part_desc AS (
            SELECT PART_NUMBER, MAX(PART_DESCRIPTION) AS PART_DESCRIPTION
            FROM {DB}.{SCHEMA}.INVENTORY
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
            -- Eff Avail Qty must equal the basis Can Build is derived from so the
            -- two columns agree. CTB_RESULT_DT (the WO header) is inventory-only
            -- and does NOT subtract higher-priority consumption, so we mirror that
            -- here. HP Used / Safety Stock / Deliveries remain informational.
            GREATEST(0, COALESCE(i.QTY_AVAILABLE, 0)) AS "eff_avail_qty",
            p.QTY_PER_UNIT * ts.PLANNED_QTY AS "qty_required",
            -- Match CTB_RESULT_DT: can-build is inventory-only (no safety / ASN).
            CASE
                WHEN p.QTY_PER_UNIT > 0 THEN FLOOR(
                    GREATEST(0, COALESCE(i.QTY_AVAILABLE, 0))
                    / NULLIF(p.QTY_PER_UNIT, 0)
                )
                ELSE 0
            END AS "can_build",
            -- Part status aligned with CTB can-build (inventory-only) + supply coverage.
            -- USING SAFETY only when on-hand can build >= 1 FG; if on-hand is 0 but
            -- safety covers the line, that still blocks can-build → SHORTAGE.
            CASE
                WHEN COALESCE(i.QTY_AVAILABLE, 0) >= p.QTY_PER_UNIT * ts.PLANNED_QTY THEN 'OK'
                WHEN COALESCE(i.QTY_AVAILABLE, 0) + COALESCE(ab.INCOMING_QTY, 0)
                     >= p.QTY_PER_UNIT * ts.PLANNED_QTY THEN 'WITH_DELIVERY'
                WHEN p.QTY_PER_UNIT > 0
                     AND FLOOR(GREATEST(0, COALESCE(i.QTY_AVAILABLE, 0)) / p.QTY_PER_UNIT)
                         >= ts.PLANNED_QTY THEN 'OK'
                WHEN p.QTY_PER_UNIT > 0
                     AND FLOOR(GREATEST(0, COALESCE(i.QTY_AVAILABLE, 0)) / p.QTY_PER_UNIT) > 0
                     AND FLOOR(GREATEST(0, COALESCE(i.QTY_AVAILABLE, 0)) / p.QTY_PER_UNIT)
                         < ts.PLANNED_QTY THEN 'PARTIAL'
                WHEN COALESCE(i.QTY_AVAILABLE, 0) + COALESCE(i.SAFETY_STOCK, 0)
                     >= p.QTY_PER_UNIT * ts.PLANNED_QTY
                     AND p.QTY_PER_UNIT > 0
                     AND FLOOR(GREATEST(0, COALESCE(i.QTY_AVAILABLE, 0)) / p.QTY_PER_UNIT) > 0
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


def get_bom_explosion(product_id: str) -> list[dict]:
    return run_query(f"""
        SELECT ROOT_PRODUCT, PARENT_PART_NUMBER, CHILD_PART_NUMBER,
               CHILD_PART_DESCRIPTION, CHILD_PART_TYPE,
               QTY_PER_ASSEMBLY, EXTENDED_QTY, BOM_LEVEL, BOM_PATH
        FROM {DB}.{SCHEMA}.BOM_EXPLODE_VW
        WHERE ROOT_PRODUCT = %s
        ORDER BY BOM_LEVEL, PARENT_PART_NUMBER, CHILD_PART_NUMBER
    """, (product_id,))


def get_bom_stats(product_id: str, work_order_id: str | None = None) -> dict:
    """
    BOM Explosion KPIs using BOM_EXPLODE_VW extended quantities × planned WO qty so low-inventory
    counts match scaled demand (same scale as BOM constraint cards). work_order_id optional (defaults ×1 FG).
    Returns: max_depth, total_parts, low_inv_cnt, total_qty_needed
    """
    safe_product = str(product_id).replace("'", "''")
    safe_wo = str(work_order_id).replace("'", "''") if work_order_id else ""
    rows = run_query(f"""
        WITH wo_mult AS (
            SELECT
                CASE
                    WHEN NULLIF(TRIM('{safe_wo}'), '') IS NULL THEN 1
                    ELSE COALESCE(
                        (
                            SELECT PLANNED_QTY
                            FROM {DB}.{SCHEMA}.WORK_ORDER
                            WHERE WORK_ORDER_ID = '{safe_wo}'
                              AND PRODUCT_ID = '{safe_product}'
                        ),
                        1
                    )
                END AS PQ,
                CASE
                    WHEN NULLIF(TRIM('{safe_wo}'), '') IS NULL THEN NULL
                    ELSE (
                        SELECT PLANT_ID
                        FROM {DB}.{SCHEMA}.WORK_ORDER
                        WHERE WORK_ORDER_ID = '{safe_wo}'
                          AND PRODUCT_ID = '{safe_product}'
                        LIMIT 1
                    )
                END AS PLANT_ID
        ),
        exploded AS (
            SELECT
                CHILD_PART_NUMBER,
                MAX(BOM_LEVEL) AS LEVEL,
                SUM(EXTENDED_QTY) AS EXT_PER_FG
            FROM {DB}.{SCHEMA}.BOM_EXPLODE_VW
            WHERE ROOT_PRODUCT = '{safe_product}'
            GROUP BY CHILD_PART_NUMBER
        ),
        bom_parts AS (
            SELECT
                e.CHILD_PART_NUMBER,
                e.LEVEL,
                e.EXT_PER_FG * w.PQ AS QTY_NEEDED
            FROM exploded e
            CROSS JOIN wo_mult w
        ),
        inv_agg AS (
            SELECT
                i.PART_NUMBER,
                SUM(COALESCE(i.QTY_AVAILABLE, 0)) AS QTY_AVAILABLE
            FROM {DB}.{SCHEMA}.INVENTORY i
            CROSS JOIN wo_mult wm
            WHERE wm.PLANT_ID IS NULL OR i.PLANT_ID = wm.PLANT_ID
            GROUP BY i.PART_NUMBER
        ),
        hp_alloc AS (
            SELECT
                p.DEMAND_PART_NUMBER AS PART_NUMBER,
                SUM(COALESCE(p.SUPPLY_QTY, 0)) AS HP_ALLOCATED
            FROM {DB}.{SCHEMA}.PEGGING_VW p
            JOIN {DB}.{SCHEMA}.WORK_ORDER wo ON p.WORK_ORDER_ID = wo.WORK_ORDER_ID
            WHERE wo.PRIORITY < (
                SELECT COALESCE(MIN(w2.PRIORITY), 999)
                FROM {DB}.{SCHEMA}.WORK_ORDER w2
                WHERE w2.PRODUCT_ID = '{safe_product}'
            )
              AND (
                  NULLIF(TRIM('{safe_wo}'), '') IS NULL
                  OR wo.PLANT_ID = (
                      SELECT w0.PLANT_ID
                      FROM {DB}.{SCHEMA}.WORK_ORDER w0
                      WHERE w0.WORK_ORDER_ID = '{safe_wo}'
                        AND w0.PRODUCT_ID = '{safe_product}'
                      LIMIT 1
                  )
              )
            GROUP BY p.DEMAND_PART_NUMBER
        ),
        inv_check AS (
            SELECT
                bp.CHILD_PART_NUMBER,
                bp.LEVEL,
                bp.QTY_NEEDED,
                GREATEST(
                    0,
                    COALESCE(i.QTY_AVAILABLE, 0) - COALESCE(h.HP_ALLOCATED, 0)
                ) AS EFF_AVAILABLE
            FROM bom_parts bp
            LEFT JOIN inv_agg i ON bp.CHILD_PART_NUMBER = i.PART_NUMBER
            LEFT JOIN hp_alloc h ON bp.CHILD_PART_NUMBER = h.PART_NUMBER
        )
        SELECT
            COALESCE(MAX(LEVEL), 0) AS "max_depth",
            COALESCE(COUNT(DISTINCT CHILD_PART_NUMBER), 0) AS "total_parts",
            COALESCE(SUM(CASE WHEN EFF_AVAILABLE < QTY_NEEDED THEN 1 ELSE 0 END), 0) AS "low_inv_cnt",
            COALESCE(SUM(QTY_NEEDED), 0) AS "total_qty_needed"
        FROM inv_check
    """)
    return rows[0] if rows else {}


def get_bom_lineage(product_id: str, work_order_id: str | None = None) -> list[dict]:
    """
    Lineage for BOM Explosion diagram. Includes BUILD_NEED_QTY (extended BOM qty × planned WO)
    so shortage coloring matches KPI / constraint scale. work_order_id optional (×1 FG).
    """
    safe_product = str(product_id).replace("'", "''")
    safe_wo = str(work_order_id).replace("'", "''") if work_order_id else ""
    return run_query(f"""
        WITH RECURSIVE bom_tree AS (
            SELECT 
                PARENT_PART_NUMBER,
                CHILD_PART_NUMBER,
                CHILD_PART_DESCRIPTION,
                QTY_PER_ASSEMBLY,
                1 AS LEVEL
            FROM {DB}.{SCHEMA}.BOM
            WHERE PARENT_PART_NUMBER = '{safe_product}' AND BOM_STATUS = 'ACTIVE'
            UNION ALL
            SELECT 
                b.PARENT_PART_NUMBER,
                b.CHILD_PART_NUMBER,
                b.CHILD_PART_DESCRIPTION,
                b.QTY_PER_ASSEMBLY,
                bt.LEVEL + 1
            FROM {DB}.{SCHEMA}.BOM b
            JOIN bom_tree bt ON b.PARENT_PART_NUMBER = bt.CHILD_PART_NUMBER
            WHERE b.BOM_STATUS = 'ACTIVE' AND bt.LEVEL < 4
        ),
        wo_mult AS (
            SELECT
                CASE
                    WHEN NULLIF(TRIM('{safe_wo}'), '') IS NULL THEN 1
                    ELSE COALESCE(
                        (
                            SELECT PLANNED_QTY
                            FROM {DB}.{SCHEMA}.WORK_ORDER
                            WHERE WORK_ORDER_ID = '{safe_wo}'
                              AND PRODUCT_ID = '{safe_product}'
                        ),
                        1
                    )
                END AS PQ,
                CASE
                    WHEN NULLIF(TRIM('{safe_wo}'), '') IS NULL THEN NULL
                    ELSE (
                        SELECT PLANT_ID
                        FROM {DB}.{SCHEMA}.WORK_ORDER
                        WHERE WORK_ORDER_ID = '{safe_wo}'
                          AND PRODUCT_ID = '{safe_product}'
                        LIMIT 1
                    )
                END AS PLANT_ID
        ),
        exploded AS (
            SELECT
                CHILD_PART_NUMBER,
                SUM(EXTENDED_QTY) AS EXT_PER_FG
            FROM {DB}.{SCHEMA}.BOM_EXPLODE_VW
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
            FROM {DB}.{SCHEMA}.INVENTORY i
            CROSS JOIN wo_mult wm
            WHERE wm.PLANT_ID IS NULL OR i.PLANT_ID = wm.PLANT_ID
            GROUP BY i.PART_NUMBER
        ),
        hp_alloc AS (
            SELECT 
                p.DEMAND_PART_NUMBER AS PART_NUMBER,
                SUM(COALESCE(p.SUPPLY_QTY, 0)) AS HP_ALLOCATED
            FROM {DB}.{SCHEMA}.PEGGING_VW p
            JOIN {DB}.{SCHEMA}.WORK_ORDER wo ON p.WORK_ORDER_ID = wo.WORK_ORDER_ID
            WHERE wo.PRIORITY < (
                SELECT COALESCE(MIN(w2.PRIORITY), 999)
                FROM {DB}.{SCHEMA}.WORK_ORDER w2
                WHERE w2.PRODUCT_ID = '{safe_product}'
            )
              AND (
                  NULLIF(TRIM('{safe_wo}'), '') IS NULL
                  OR wo.PLANT_ID = (
                      SELECT w0.PLANT_ID
                      FROM {DB}.{SCHEMA}.WORK_ORDER w0
                      WHERE w0.WORK_ORDER_ID = '{safe_wo}'
                        AND w0.PRODUCT_ID = '{safe_product}'
                      LIMIT 1
                  )
              )
            GROUP BY p.DEMAND_PART_NUMBER
        )
        SELECT 
            bt.PARENT_PART_NUMBER,
            bt.CHILD_PART_NUMBER,
            bt.CHILD_PART_DESCRIPTION,
            bt.QTY_PER_ASSEMBLY,
            bt.LEVEL,
            COALESCE(i.QTY_AVAILABLE, 0) AS QTY_AVAILABLE,
            COALESCE(i.SAFETY_STOCK, 0) AS SAFETY_STOCK,
            COALESCE(h.HP_ALLOCATED, 0) AS HP_ALLOCATED,
            GREATEST(0, COALESCE(i.QTY_AVAILABLE, 0) - COALESCE(h.HP_ALLOCATED, 0)) AS EFF_AVAILABLE,
            COALESCE(pn.BUILD_NEED_QTY, bt.QTY_PER_ASSEMBLY * wm.PQ) AS BUILD_NEED_QTY
        FROM bom_tree bt
        CROSS JOIN wo_mult wm
        LEFT JOIN part_need pn ON bt.CHILD_PART_NUMBER = pn.CHILD_PART_NUMBER
        LEFT JOIN inv_agg i ON bt.CHILD_PART_NUMBER = i.PART_NUMBER
        LEFT JOIN hp_alloc h ON bt.CHILD_PART_NUMBER = h.PART_NUMBER
        ORDER BY bt.LEVEL, bt.PARENT_PART_NUMBER, bt.CHILD_PART_NUMBER
    """)


def get_bom_where_used(constraint_part: str) -> list[dict]:
    """
    Streamlit parity for the 'Part Where-Used' KPIs + table.
    Returns rows for: PRODUCTS AFFECTED
    """
    safe_constraint = str(constraint_part).replace("'", "''")
    return run_query(f"""
        SELECT 
            b.PARENT_PART_NUMBER AS PRODUCT,
            b.BOM_LEVEL,
            SUM(b.QTY_PER_ASSEMBLY) AS QTY_PER_UNIT,
            COUNT(DISTINCT wo.WORK_ORDER_ID) AS WORK_ORDERS,
            SUM(wo.PLANNED_QTY * b.QTY_PER_ASSEMBLY) AS TOTAL_REQUIRED,
            COALESCE(MAX(i.QTY_AVAILABLE), 0) AS AVAILABLE,
            COALESCE(SUM(wo.PLANNED_QTY * b.QTY_PER_ASSEMBLY), 0) - COALESCE(MAX(i.QTY_AVAILABLE), 0) AS GAP
        FROM {DB}.{SCHEMA}.BOM b
        LEFT JOIN {DB}.{SCHEMA}.WORK_ORDER wo ON b.PARENT_PART_NUMBER = wo.PRODUCT_ID
        LEFT JOIN {DB}.{SCHEMA}.INVENTORY i ON b.CHILD_PART_NUMBER = i.PART_NUMBER
        WHERE b.CHILD_PART_NUMBER LIKE '%{safe_constraint}%' AND b.BOM_STATUS = 'ACTIVE'
        GROUP BY b.PARENT_PART_NUMBER, b.BOM_LEVEL
        ORDER BY GAP DESC
    """)


def get_bom_open_pos(constraint_part: str) -> list[dict]:
    """
    Streamlit parity for the 'Open POs for this Part' table in Part Where-Used.
    """
    safe_constraint = str(constraint_part).replace("'", "''")
    return run_query(f"""
        SELECT 
            po.PO_ID,
            po.SUPPLIER_ID || ' - ' || COALESCE(s.SUPPLIER_NAME, '') AS SUPPLIER,
            po.QTY_ORDERED,
            po.QTY_OUTSTANDING,
            po.CONFIRMED_DELIVERY_DATE,
            po.PO_STATUS
        FROM {DB}.{SCHEMA}.PROCUREMENT_SUMMARY_VW po
        LEFT JOIN {DB}.{SCHEMA}.SUPPLIER_SUMMARY_VW s ON po.SUPPLIER_ID = s.SUPPLIER_ID
        WHERE po.PART_NUMBER LIKE '%{safe_constraint}%' 
          AND po.PO_STATUS NOT IN ('COMPLETED', 'CANCELLED')
        ORDER BY po.CONFIRMED_DELIVERY_DATE
    """)


def get_shortage_alerts(product: str | None = None, plant: str | None = None, week: str | None = None) -> list[dict]:
    where = _wo_where(product, plant, week)
    return run_query(f"""
        SELECT sa.PART_NUMBER, sa.WORK_ORDER_ID, sa.PLANT_ID,
               sa.SHORTAGE_QTY, sa.ALERT_STATUS,
               wo.PRODUCT_ID, wo.PRIORITY,
               wo.PLANNED_START_DATE::VARCHAR AS "planned_start_date"
        FROM {DB}.{SCHEMA}.SHORTAGE_ALERT_VW sa
        JOIN {DB}.{SCHEMA}.WORK_ORDER wo ON sa.WORK_ORDER_ID = wo.WORK_ORDER_ID
        WHERE sa.ALERT_STATUS = 'OPEN' {where}
        ORDER BY wo.PRIORITY, sa.SHORTAGE_QTY DESC
    """)


def get_supplier_performance() -> list[dict]:
    return run_query(f"""
        SELECT SUPPLIER_ID,
               COUNT(*) AS "total_orders",
               SUM(CASE WHEN IS_LATE = 1 THEN 1 ELSE 0 END) AS "late_orders",
               ROUND(100.0 * SUM(CASE WHEN IS_LATE = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0), 1) AS "on_time_pct"
        FROM {DB}.{SCHEMA}.PROCUREMENT_SUMMARY_VW
        GROUP BY SUPPLIER_ID ORDER BY on_time_pct ASC LIMIT 20
    """)


def create_po(part_number: str, qty: int, supplier_id: str, plant_id: str, delivery_date: str) -> dict:
    rows = run_query(f"""
        CALL {DB}.{SCHEMA}.SP_CREATE_PO(
            '{part_number}', {qty}, '{supplier_id}', '{plant_id}',
            '{delivery_date}'::DATE, 100.00, 'REACT_APP'
        )
    """)
    return rows[0] if rows else {}


def refresh_ctb() -> dict:
    rows = run_query(f"CALL {DB}.{SCHEMA}.SP_REFRESH_CTB_DATA()")
    return rows[0] if rows else {}


def prioritize_work_order(wo_id: str) -> dict:
    rows = run_query(f"""
        CALL {DB}.{SCHEMA}.SP_UPDATE_WORK_ORDER_PRIORITY(
            '{wo_id}', 0, DATE_TRUNC('WEEK', CURRENT_DATE)::DATE, 'REACT_APP'
        )
    """)
    return rows[0] if rows else {}


def simulate_prioritization(wo_id: str) -> list[dict]:
    safe_wo_id = wo_id.replace("'", "''")
    rows = run_query(f"""
        WITH target_wo AS (
            SELECT wo.WORK_ORDER_ID, wo.PRODUCT_ID, wo.PLANNED_QTY,
                   wo.PLANNED_START_DATE,
                   COALESCE(ctb.CAN_BUILD_QTY, 0) AS CAN_BUILD_QTY,
                   COALESCE(ctb.CTB_STATUS, 'UNKNOWN') AS CTB_STATUS
            FROM {DB}.{SCHEMA}.WORK_ORDER wo
            LEFT JOIN {DB}.{SCHEMA}.CTB_RESULT_VW ctb ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
            WHERE wo.WORK_ORDER_ID = '{safe_wo_id}'
        ),
        bom_requirements AS (
            SELECT
                b.CHILD_PART_NUMBER AS PART_NUMBER,
                b.QTY_PER_ASSEMBLY,
                tw.PLANNED_QTY,
                b.QTY_PER_ASSEMBLY * tw.PLANNED_QTY AS REQUIRED_QTY
            FROM target_wo tw
            JOIN {DB}.{SCHEMA}.BOM b ON tw.PRODUCT_ID = b.PARENT_PART_NUMBER AND b.BOM_STATUS = 'ACTIVE'
        ),
        current_inventory AS (
            SELECT PART_NUMBER, SUM(QTY_AVAILABLE) AS QTY_AVAILABLE
            FROM {DB}.{SCHEMA}.INVENTORY
            GROUP BY PART_NUMBER
        ),
        upcoming_deliveries AS (
            SELECT PART_NUMBER, SUM(QTY_SHIPPED) AS INCOMING_QTY
            FROM {DB}.{SCHEMA}.ASN_IN_TRANSIT_VW
            WHERE ASN_STATUS IN ('SHIPPED', 'IN_TRANSIT')
              AND EXPECTED_ARRIVAL_DATE <= DATE_TRUNC('WEEK', CURRENT_DATE) + INTERVAL '2 WEEKS'
            GROUP BY PART_NUMBER
        ),
        in_production_consumption AS (
            SELECT
                b.CHILD_PART_NUMBER AS PART_NUMBER,
                SUM(b.QTY_PER_ASSEMBLY * wo.PLANNED_QTY) AS CONSUMED_QTY
            FROM {DB}.{SCHEMA}.WORK_ORDER wo
            JOIN {DB}.{SCHEMA}.CTB_RESULT_VW ctb ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
            JOIN {DB}.{SCHEMA}.BOM b ON wo.PRODUCT_ID = b.PARENT_PART_NUMBER AND b.BOM_STATUS = 'ACTIVE'
            WHERE ctb.PRODUCTION_STATUS = 'IN_PRODUCTION'
            GROUP BY b.CHILD_PART_NUMBER
        ),
        high_priority_consumption AS (
            SELECT
                b.CHILD_PART_NUMBER AS PART_NUMBER,
                SUM(b.QTY_PER_ASSEMBLY * wo.PLANNED_QTY) AS CONSUMED_QTY
            FROM {DB}.{SCHEMA}.WORK_ORDER wo
            JOIN {DB}.{SCHEMA}.BOM b ON wo.PRODUCT_ID = b.PARENT_PART_NUMBER AND b.BOM_STATUS = 'ACTIVE'
            LEFT JOIN {DB}.{SCHEMA}.CTB_RESULT_VW ctb ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
            WHERE wo.PRIORITY = 0
              AND DATE_TRUNC('WEEK', wo.PLANNED_START_DATE) = DATE_TRUNC('WEEK', CURRENT_DATE)
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
                GREATEST(0, COALESCE(inv.QTY_AVAILABLE, 0) - COALESCE(ip.CONSUMED_QTY, 0) - COALESCE(hp.CONSUMED_QTY, 0)) AS AVAILABLE_NOW,
                GREATEST(0, COALESCE(inv.QTY_AVAILABLE, 0) + COALESCE(del.INCOMING_QTY, 0) - COALESCE(ip.CONSUMED_QTY, 0) - COALESCE(hp.CONSUMED_QTY, 0)) AS AVAILABLE_AFTER_DELIVERY
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
            GREATEST(0, pa.REQUIRED_QTY - pa.AVAILABLE_NOW) AS "shortage_now",
            GREATEST(0, pa.REQUIRED_QTY - pa.AVAILABLE_AFTER_DELIVERY) AS "shortage_after_delivery",
            CASE
                WHEN pa.AVAILABLE_NOW >= pa.REQUIRED_QTY THEN 'READY_NOW'
                WHEN pa.AVAILABLE_AFTER_DELIVERY >= pa.REQUIRED_QTY THEN 'READY_ON_DELIVERY'
                WHEN pa.AVAILABLE_NOW > 0 THEN 'PARTIAL_NOW'
                WHEN pa.AVAILABLE_AFTER_DELIVERY > 0 THEN 'PARTIAL_ON_DELIVERY'
                ELSE 'BLOCKED'
            END AS "part_status",
            CASE
                WHEN tw.CTB_STATUS = 'BLOCKED' AND tw.CAN_BUILD_QTY <= 0 THEN 0
                WHEN pa.QTY_PER_ASSEMBLY > 0 THEN LEAST(FLOOR(pa.AVAILABLE_NOW / pa.QTY_PER_ASSEMBLY), tw.CAN_BUILD_QTY)
                ELSE LEAST(tw.PLANNED_QTY, tw.CAN_BUILD_QTY)
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
    """For the given target work order, return OTHER work orders that would be
    impacted if it is promoted to high priority.

    1. Compute the parts the target WO needs (BOM x planned qty).
    2. Subtract reservations for currently in-production WOs from total
       inventory and give the target WO its full requirement next.
    3. For each other active WO at the same plant that uses any of the same
       parts, compute its new can-build limited by the residual inventory.
    4. Return only WOs whose can-build would actually decrease.
    """
    safe_wo_id = wo_id.replace("'", "''")
    rows = run_query(f"""
        WITH target_wo AS (
            SELECT
                wo.WORK_ORDER_ID,
                wo.PRODUCT_ID,
                wo.PLANT_ID,
                wo.PLANNED_QTY
            FROM {DB}.{SCHEMA}.WORK_ORDER wo
            WHERE wo.WORK_ORDER_ID = '{safe_wo_id}'
        ),
        target_parts AS (
            SELECT
                b.CHILD_PART_NUMBER AS PART_NUMBER,
                b.QTY_PER_ASSEMBLY * tw.PLANNED_QTY AS TARGET_NEED
            FROM target_wo tw
            JOIN {DB}.{SCHEMA}.BOM b
              ON tw.PRODUCT_ID = b.PARENT_PART_NUMBER
             AND b.BOM_STATUS = 'ACTIVE'
        ),
        inv AS (
            SELECT PART_NUMBER, SUM(COALESCE(QTY_AVAILABLE, 0)) AS QTY_AVAILABLE
            FROM {DB}.{SCHEMA}.INVENTORY
            GROUP BY PART_NUMBER
        ),
        in_prod AS (
            SELECT
                b.CHILD_PART_NUMBER AS PART_NUMBER,
                SUM(b.QTY_PER_ASSEMBLY * wo.PLANNED_QTY) AS RESERVED_QTY
            FROM {DB}.{SCHEMA}.WORK_ORDER wo
            JOIN {DB}.{SCHEMA}.CTB_RESULT_VW ctb
              ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
            JOIN {DB}.{SCHEMA}.BOM b
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
                GREATEST(
                    0,
                    COALESCE(i.QTY_AVAILABLE, 0)
                    - COALESCE(ip.RESERVED_QTY, 0)
                    - tp.TARGET_NEED
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
            FROM {DB}.{SCHEMA}.WORK_ORDER other
            JOIN {DB}.{SCHEMA}.BOM b
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
                ANY_VALUE(pwp.PRODUCT_ID) AS PRODUCT_ID,
                ANY_VALUE(pwp.PRIORITY) AS PRIORITY,
                ANY_VALUE(pwp.PLANNED_QTY) AS PLANNED_QTY,
                ANY_VALUE(pwp.PLANNED_START_DATE) AS PLANNED_START_DATE,
                ANY_VALUE(pwp.WORK_ORDER_STATUS) AS WORK_ORDER_STATUS,
                LISTAGG(DISTINCT pwp.PART_NUMBER, ', ')
                    WITHIN GROUP (ORDER BY pwp.PART_NUMBER) AS SHARED_PARTS,
                COUNT(DISTINCT pwp.PART_NUMBER) AS SHARED_PART_COUNT,
                MIN(pwp.PART_NEW_CAN_BUILD) AS NEW_CAN_BUILD_FROM_SHARED
            FROM per_wo_part pwp
            GROUP BY pwp.WORK_ORDER_ID
        )
        SELECT
            agg.WORK_ORDER_ID         AS "WORK_ORDER_ID",
            agg.PRODUCT_ID            AS "PRODUCT_ID",
            agg.PRIORITY              AS "PRIORITY",
            agg.PLANNED_QTY           AS "PLANNED_QTY",
            agg.PLANNED_START_DATE::VARCHAR AS "planned_start_date",
            agg.WORK_ORDER_STATUS     AS "work_order_status",
            COALESCE(ctb.CAN_BUILD_QTY, 0) AS "current_can_build",
            COALESCE(ctb.CTB_STATUS, 'UNKNOWN') AS "current_status",
            LEAST(
                agg.PLANNED_QTY,
                COALESCE(ctb.CAN_BUILD_QTY, 0),
                COALESCE(agg.NEW_CAN_BUILD_FROM_SHARED, 0)
            ) AS "new_can_build",
            GREATEST(
                0,
                COALESCE(ctb.CAN_BUILD_QTY, 0)
                - LEAST(
                    agg.PLANNED_QTY,
                    COALESCE(ctb.CAN_BUILD_QTY, 0),
                    COALESCE(agg.NEW_CAN_BUILD_FROM_SHARED, 0)
                  )
            ) AS "lost_build_qty",
            agg.SHARED_PARTS          AS "shared_parts",
            agg.SHARED_PART_COUNT     AS "shared_part_count"
        FROM agg
        LEFT JOIN {DB}.{SCHEMA}.CTB_RESULT_VW ctb
          ON ctb.WORK_ORDER_ID = agg.WORK_ORDER_ID
        WHERE COALESCE(ctb.CAN_BUILD_QTY, 0) > LEAST(
                agg.PLANNED_QTY,
                COALESCE(ctb.CAN_BUILD_QTY, 0),
                COALESCE(agg.NEW_CAN_BUILD_FROM_SHARED, 0)
              )
        ORDER BY "lost_build_qty" DESC, "PRIORITY" ASC, "WORK_ORDER_ID"
        LIMIT 50
    """)
    return rows


def get_production_board(product: str | None = None, plant: str | None = None) -> list[dict]:
    where = _wo_where(product, plant, None)
    rows = run_query(f"""
        SELECT 
            wo.WORK_ORDER_ID,
            wo.PRODUCT_ID,
            wo.PLANT_ID,
            wo.PRIORITY,
            wo.PLANNED_QTY,
            wo.PLANNED_START_DATE::VARCHAR AS "planned_start_date",
            wo.WORK_ORDER_STATUS,
            COALESCE(ctb.CTB_STATUS, 'UNKNOWN') AS "ctb_status",
            CASE
                WHEN COALESCE(ctb.CAN_BUILD_QTY, 0) >= COALESCE(wo.PLANNED_QTY, 0) THEN 'READY_NOW'
                WHEN DATE_TRUNC('WEEK', wo.PLANNED_START_DATE) = DATE_TRUNC('WEEK', CURRENT_DATE)
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
            DATE_TRUNC('WEEK', wo.PLANNED_START_DATE)::VARCHAR AS "week_start",
            CASE 
                WHEN wo.PRIORITY = 0 AND DATE_TRUNC('WEEK', wo.PLANNED_START_DATE) = DATE_TRUNC('WEEK', CURRENT_DATE) THEN 'HIGH_PRIORITY'
                WHEN COALESCE(ctb.CTB_STATUS, 'UNKNOWN') = 'READY' 
                     AND DATE_TRUNC('WEEK', wo.PLANNED_START_DATE) = DATE_TRUNC('WEEK', CURRENT_DATE) 
                     AND wo.PLANNED_START_DATE <= CURRENT_DATE THEN 'IN_PRODUCTION'
                WHEN DATE_TRUNC('WEEK', wo.PLANNED_START_DATE) = DATE_TRUNC('WEEK', CURRENT_DATE) THEN 'YET_TO_START'
                WHEN DATE_TRUNC('WEEK', wo.PLANNED_START_DATE) = DATE_TRUNC('WEEK', CURRENT_DATE) + INTERVAL '1 WEEK' THEN 'NEXT_WEEK'
                ELSE 'OTHER'
            END AS "lane"
        FROM {DB}.{SCHEMA}.WORK_ORDER wo
        LEFT JOIN {DB}.{SCHEMA}.CTB_RESULT_VW ctb ON wo.WORK_ORDER_ID = ctb.WORK_ORDER_ID
        WHERE 1=1 {where}
          AND DATE_TRUNC('WEEK', wo.PLANNED_START_DATE) >= DATE_TRUNC('WEEK', CURRENT_DATE)
          AND DATE_TRUNC('WEEK', wo.PLANNED_START_DATE) <= DATE_TRUNC('WEEK', CURRENT_DATE) + INTERVAL '1 WEEK'
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
                DATE_TRUNC('WEEK', wo.PLANNED_START_DATE) AS WEEK_START,
                p.DEMAND_PART_NUMBER AS CONSTRAINT_PART,
                p.BOM_LEVEL,
                p.DEMAND_QTY AS REQUIRED_QTY,
                p.SUPPLY_QTY AS SUPPLY_QTY,
                p.SHORTAGE_QTY
            FROM {DB}.{SCHEMA}.WORK_ORDER wo
            JOIN {DB}.{SCHEMA}.PEGGING_VW p
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
            FROM {DB}.{SCHEMA}.BOM_EXPLODE_VW
            GROUP BY ROOT_PRODUCT, CHILD_PART_NUMBER
        ),
        inv AS (
            SELECT
                PART_NUMBER,
                PLANT_ID,
                SUM(COALESCE(QTY_AVAILABLE, 0)) AS qty_available,
                SUM(COALESCE(SAFETY_STOCK, 0)) AS safety_stock
            FROM {DB}.{SCHEMA}.INVENTORY
            GROUP BY PART_NUMBER, PLANT_ID
        ),
        del AS (
            SELECT
                PART_NUMBER,
                RECEIVING_PLANT_ID AS PLANT_ID,
                SUM(COALESCE(QTY_SHIPPED, 0)) AS total_deliveries
            FROM {DB}.{SCHEMA}.ASN_IN_TRANSIT_VW
            WHERE ASN_STATUS IN ('SHIPPED', 'IN_TRANSIT')
            GROUP BY PART_NUMBER, RECEIVING_PLANT_ID
        )
        SELECT
            sp.WORK_ORDER_ID,
            sp.PRODUCT_ID,
            sp.PRIORITY,
            sp.PLANNED_QTY,
            sp.PLANNED_START_DATE::VARCHAR AS "planned_start_date",
            sp.CONSTRAINT_PART AS "constraint_part",
            COALESCE(bc.BOM_LEVEL, sp.BOM_LEVEL) AS BOM_LEVEL,
            COALESCE(
                bc.EXTENDED_QTY,
                IFF(sp.PLANNED_QTY > 0, sp.REQUIRED_QTY / NULLIF(sp.PLANNED_QTY, 0), 0)
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
