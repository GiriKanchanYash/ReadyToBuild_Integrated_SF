from fastapi import APIRouter, Query, Depends
from app.auth import require_roles
from app.service_factory import get_ctb_service

router = APIRouter(prefix="/api/ctb", tags=["CTB"])


def _svc(data_source: str | None):
    return get_ctb_service(data_source)


@router.get("/filters")
def filters(data_source: str | None = Query(None)):
    return _svc(data_source).get_filter_options()


@router.get("/kpis")
def kpis(
    product: str | None = Query(None),
    plant: str | None = Query(None),
    week: str | None = Query(None),
    data_source: str | None = Query(None),
):
    return _svc(data_source).get_kpis(product, plant, week)


@router.get("/status-distribution")
def status_distribution(
    product: str | None = Query(None),
    plant: str | None = Query(None),
    week: str | None = Query(None),
    data_source: str | None = Query(None),
):
    return _svc(data_source).get_ctb_status_distribution(product, plant, week)


@router.get("/top-shortages")
def top_shortages(
    product: str | None = Query(None),
    plant: str | None = Query(None),
    week: str | None = Query(None),
    limit: int = Query(10),
    data_source: str | None = Query(None),
):
    return _svc(data_source).get_top_shortages(product, plant, week, limit)


@router.get("/by-priority")
def by_priority(
    product: str | None = Query(None),
    plant: str | None = Query(None),
    week: str | None = Query(None),
    data_source: str | None = Query(None),
):
    return _svc(data_source).get_ctb_by_priority(product, plant, week)


@router.get("/work-orders")
def work_orders(
    product: str | None = Query(None),
    plant: str | None = Query(None),
    week: str | None = Query(None),
    data_source: str | None = Query(None),
):
    return _svc(data_source).get_work_orders(product, plant, week)


@router.get("/work-orders/{wo_id}")
def work_order_detail(wo_id: str, data_source: str | None = Query(None)):
    return _svc(data_source).get_work_order_detail(wo_id)


@router.get("/work-orders/{wo_id}/prioritization-sim")
def prioritization_sim(wo_id: str, data_source: str | None = Query(None)):
    return _svc(data_source).simulate_prioritization(wo_id)


@router.get("/work-orders/{wo_id}/prioritization-impact")
def prioritization_impact(wo_id: str, data_source: str | None = Query(None)):
    return _svc(data_source).simulate_prioritization_impact(wo_id)


@router.get("/shortage-alerts")
def shortage_alerts(
    product: str | None = Query(None),
    plant: str | None = Query(None),
    week: str | None = Query(None),
    data_source: str | None = Query(None),
):
    return _svc(data_source).get_shortage_alerts(product, plant, week)


@router.get("/supplier-performance")
def supplier_performance(data_source: str | None = Query(None)):
    return _svc(data_source).get_supplier_performance()


@router.get("/bom/{product_id}")
def bom_explosion(product_id: str, data_source: str | None = Query(None)):
    return _svc(data_source).get_bom_explosion(product_id)


@router.get("/bom-stats/{product_id}")
def bom_stats(product_id: str, wo: str | None = Query(None), data_source: str | None = Query(None)):
    return _svc(data_source).get_bom_stats(product_id, wo)


@router.get("/bom-lineage/{product_id}")
def bom_lineage(product_id: str, wo: str | None = Query(None), data_source: str | None = Query(None)):
    return _svc(data_source).get_bom_lineage(product_id, wo)


@router.get("/bom-where-used/{constraint_part}")
def bom_where_used(constraint_part: str, data_source: str | None = Query(None)):
    return _svc(data_source).get_bom_where_used(constraint_part)


@router.get("/bom-open-pos/{constraint_part}")
def bom_open_pos(constraint_part: str, data_source: str | None = Query(None)):
    return _svc(data_source).get_bom_open_pos(constraint_part)


@router.get("/production-board")
def production_board(
    product: str | None = Query(None),
    plant: str | None = Query(None),
    data_source: str | None = Query(None),
):
    return _svc(data_source).get_production_board(product, plant)


@router.get("/bom-constraints")
def bom_constraints(
    product: str | None = Query(None),
    plant: str | None = Query(None),
    week: str | None = Query(None),
    data_source: str | None = Query(None),
):
    return _svc(data_source).get_bom_constraints(product, plant, week)


@router.get("/parts-inventory")
def parts_inventory(
    product: str | None = Query(None),
    plant: str | None = Query(None),
    week: str | None = Query(None),
    include_deliveries: bool = Query(True),
    search: str | None = Query(None),
    limit: int = Query(200),
    data_source: str | None = Query(None),
):
    return _svc(data_source).get_parts_inventory(
        product=product,
        plant=plant,
        week=week,
        include_deliveries=include_deliveries,
        search=search,
        limit=limit,
    )


@router.post("/refresh")
def refresh(data_source: str | None = Query(None), _=Depends(require_roles("admin"))):
    return _svc(data_source).refresh_ctb()


@router.post("/work-orders/{wo_id}/prioritize")
def prioritize(wo_id: str, data_source: str | None = Query(None), _=Depends(require_roles("admin", "planner"))):
    return _svc(data_source).prioritize_work_order(wo_id)


@router.post("/create-po")
def create_po(body: dict, data_source: str | None = Query(None), _=Depends(require_roles("admin", "sourcing"))):
    return _svc(data_source).create_po(
        part_number=body["part_number"],
        qty=body["qty"],
        supplier_id=body["supplier_id"],
        plant_id=body["plant_id"],
        delivery_date=body["delivery_date"],
    )
