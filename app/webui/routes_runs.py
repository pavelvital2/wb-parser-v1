from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .dependencies import get_current_user, get_services, get_templates
from .services import WebUIServices


router = APIRouter()


@router.get("/runs", response_class=HTMLResponse)
def runs_page(
    request: Request,
    limit: int = Query(default=50, ge=1, le=500),
    user: str = Depends(get_current_user),
    services: WebUIServices = Depends(get_services),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    rows = services.list_runs(limit=limit)
    return templates.TemplateResponse(
        request,
        "runs.html",
        {
            "title": "Run History",
            "user": user,
            "runs": rows,
            "limit": limit,
        },
    )
