from __future__ import annotations

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .dependencies import get_current_user, get_services, get_templates
from .services import ALLOWED_ACTIONS, WebUIServices


router = APIRouter()


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)


@router.get("/actions", response_class=HTMLResponse)
def actions_page(
    request: Request,
    user: str = Depends(get_current_user),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    msg = request.query_params.get("msg", "")
    return templates.TemplateResponse(
        request,
        "actions.html",
        {
            "title": "Actions",
            "user": user,
            "actions": sorted(ALLOWED_ACTIONS),
            "message": msg,
        },
    )


@router.post("/actions/run")
def run_action(
    request: Request,
    target: str = Form(...),
    user: str = Depends(get_current_user),
    services: WebUIServices = Depends(get_services),
) -> RedirectResponse:
    ok, msg = services.start_action(target=target, user=user)
    prefix = "OK" if ok else "ERROR"
    return _redirect(f"/actions?msg={prefix}:%20{msg}")
