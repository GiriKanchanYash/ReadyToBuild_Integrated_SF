from fastapi import APIRouter, Depends, Query

from app.auth import require_roles, get_current_user, AuthUser
from app.service_factory import get_copilot_service

router = APIRouter(prefix="/api/ai", tags=["AI"])


def _svc(data_source: str | None):
    return get_copilot_service(data_source)


@router.get("/copilot/quick-analyses")
def quick_analyses(data_source: str | None = Query(None), _user: AuthUser = Depends(get_current_user)):
    return _svc(data_source).copilot_quick_analyses()


@router.post("/copilot/run-analysis")
def run_analysis(body: dict, data_source: str | None = Query(None), user: AuthUser = Depends(get_current_user)):
    return _svc(data_source).run_quick_analysis(body.get("key", ""), user=user.name)


@router.post("/copilot/chat")
def copilot_chat(body: dict, data_source: str | None = Query(None), user: AuthUser = Depends(get_current_user)):
    return _svc(data_source).copilot_chat(
        str(body.get("message", "")),
        body.get("short_memory") if body else None,
        body.get("long_memory") if body else None,
        user=user.name,
    )


@router.get("/copilot/saved-insights")
def saved_insights(data_source: str | None = Query(None), user: AuthUser = Depends(get_current_user)):
    return _svc(data_source).load_saved_insights(user=user.name)


@router.post("/copilot/save-insight")
def save_insight(body: dict, data_source: str | None = Query(None), user: AuthUser = Depends(get_current_user)):
    _svc(data_source).save_insight(
        title=str(body.get("title", "")),
        question=str(body.get("question", "")),
        sql_text=str(body.get("sql_text", "")),
        user=user.name,
    )
    return {"ok": True}


@router.post("/copilot/delete-insight")
def delete_insight(body: dict, data_source: str | None = Query(None), user: AuthUser = Depends(get_current_user)):
    _svc(data_source).delete_insight(int(body.get("insight_id", 0)), user=user.name)
    return {"ok": True}


@router.get("/copilot/frequent-questions")
def frequent_questions(data_source: str | None = Query(None), user: AuthUser = Depends(get_current_user)):
    return _svc(data_source).load_frequent_questions(user=user.name)


@router.get("/copilot/most-frequent")
def most_frequent(data_source: str | None = Query(None), _user: AuthUser = Depends(get_current_user)):
    return _svc(data_source).load_most_frequent_all()


@router.get("/shortage-agent/queue")
def shortage_queue(data_source: str | None = Query(None), _user: AuthUser = Depends(get_current_user)):
    return _svc(data_source).shortage_agent_queue()


@router.post("/shortage-agent/recommend")
def shortage_recommend(body: dict, data_source: str | None = Query(None), _=Depends(require_roles("admin", "sourcing"))):
    return _svc(data_source).shortage_agent_recommendation(body)


@router.post("/shortage-agent/create-po")
def shortage_create_po(body: dict, data_source: str | None = Query(None), _=Depends(require_roles("admin", "sourcing"))):
    return _svc(data_source).shortage_agent_create_po(body)


@router.post("/shortage-agent/po-preview")
def shortage_po_preview(body: dict, data_source: str | None = Query(None), _=Depends(require_roles("admin", "sourcing"))):
    return _svc(data_source).shortage_agent_po_preview(body)


@router.post("/shortage-agent/email-draft")
def shortage_email_draft(body: dict, data_source: str | None = Query(None), _=Depends(require_roles("admin", "sourcing"))):
    return _svc(data_source).shortage_agent_email_draft(body)


@router.post("/shortage-agent/mark-resolved")
def shortage_mark_resolved(body: dict, data_source: str | None = Query(None), _=Depends(require_roles("admin", "sourcing"))):
    return _svc(data_source).shortage_agent_mark_resolved(body)
