from __future__ import annotations

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .dependencies import get_current_user, get_services, get_templates
from .services import WebUIServices


router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def dashboard(
    request: Request,
    user: str = Depends(get_current_user),
    services: WebUIServices = Depends(get_services),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    runs = services.list_runs(limit=10)
    latest_outputs = services.latest_outputs()
    return templates.TemplateResponse(
        request,
        "dashboard.html",
        {
            "user": user,
            "runs": runs,
            "latest_outputs": latest_outputs,
            "title": "Dashboard",
        },
    )
