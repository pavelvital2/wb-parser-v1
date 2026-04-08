from __future__ import annotations

from fastapi import APIRouter, Depends, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from .dependencies import get_current_user, get_services, get_templates
from .services import WebUIServices


router = APIRouter()


@router.get("/logs", response_class=HTMLResponse)
def logs_page(
    request: Request,
    file: str | None = Query(default=None),
    lines: int = Query(default=200, ge=10, le=5000),
    user: str = Depends(get_current_user),
    services: WebUIServices = Depends(get_services),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    files = services.list_log_files()
    selected = file or (files[0] if files else "")

    content_lines: list[str] = []
    error = ""
    if selected:
        try:
            content_lines = services.tail_log(selected, lines)
        except Exception as exc:
            error = str(exc)

    return templates.TemplateResponse(
        request,
        "logs.html",
        {
            "title": "Logs",
            "user": user,
            "files": files,
            "selected": selected,
            "lines": lines,
            "content": "\n".join(content_lines),
            "error": error,
        },
    )
