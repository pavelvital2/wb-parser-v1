from __future__ import annotations

from fastapi import HTTPException, Request
from fastapi.templating import Jinja2Templates

from .auth import AuthManager
from .services import WebUIServices


def get_services(request: Request) -> WebUIServices:
    return request.app.state.webui_services


def get_templates(request: Request) -> Jinja2Templates:
    return request.app.state.templates


def get_auth_manager(request: Request) -> AuthManager:
    return request.app.state.auth_manager


def get_current_user(request: Request) -> str:
    session = request.scope.get("session")
    if isinstance(session, dict):
        user = str(session.get("user", "")).strip()
        if user:
            return user
    raise HTTPException(status_code=303, headers={"Location": "/login"}, detail="Auth required")
