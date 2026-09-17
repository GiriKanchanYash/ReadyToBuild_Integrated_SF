from fastapi import APIRouter, Depends, Query

from app.auth import require_roles
from app.service_factory import get_transfer_service

router = APIRouter(prefix="/api/transfer", tags=["Transfer"])


def _svc(data_source: str | None):
    return get_transfer_service(data_source)


@router.get("/plants")
def plants(data_source: str | None = Query(None)):
    return _svc(data_source).get_plants()


@router.get("/lanes")
def lanes(
    origin: str | None = Query(None),
    dest: str | None = Query(None),
    data_source: str | None = Query(None),
):
    return _svc(data_source).get_lanes(origin, dest)


@router.get("/shortage-parts")
def shortage_parts(
    dest_plant: str | None = Query(None),
    limit: int = Query(50),
    data_source: str | None = Query(None),
):
    return _svc(data_source).get_shortage_parts(dest_plant, limit)


@router.get("/candidates")
def candidates(
    part_number: str = Query(..., description="Part number with shortage"),
    dest_plant: str = Query(..., description="Destination plant ID"),
    weight_impact: float = Query(0.5, ge=0.0, le=1.0),
    weight_speed: float = Query(0.3, ge=0.0, le=1.0),
    weight_cost: float = Query(0.2, ge=0.0, le=1.0),
    data_source: str | None = Query(None),
):
    return _svc(data_source).get_transfer_candidates(
        part_number=part_number,
        dest_plant=dest_plant,
        weight_impact=weight_impact,
        weight_speed=weight_speed,
        weight_cost=weight_cost,
    )


@router.get("/open-stos")
def open_stos(
    dest_plant: str | None = Query(None),
    data_source: str | None = Query(None),
):
    return _svc(data_source).get_open_stos(dest_plant)


@router.post("/create-sto")
def create_sto(
    body: dict,
    data_source: str | None = Query(None),
    _=Depends(require_roles("admin", "planner", "sourcing")),
):
    return _svc(data_source).create_sto(
        part_number=body["part_number"],
        qty=int(body["qty"]),
        origin_plant_id=body["origin_plant_id"],
        dest_plant_id=body["dest_plant_id"],
        transport_mode=body.get("transport_mode", "TRUCK"),
        requested_delivery_date=body["requested_delivery_date"],
        requested_by=body.get("requested_by", "AI_AGENT"),
    )


@router.post("/refresh")
def refresh(data_source: str | None = Query(None), _=Depends(require_roles("admin"))):
    return _svc(data_source).refresh_transfer_dts()
