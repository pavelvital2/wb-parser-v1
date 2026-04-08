from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from app.common.config import load_config
from app.common.logging_setup import configure_logging, get_logger
from app.common.state_db import StateDB

from .auth import SESSION_USER_KEY, build_auth_manager
from .routes_actions import router as actions_router
from .routes_config import router as config_router
from .routes_dashboard import router as dashboard_router
from .routes_files import router as files_router
from .routes_logs import router as logs_router
from .routes_runs import router as runs_router
from .services import WebUIServices


def create_app(config_path: str | None = None) -> FastAPI:
    cfg_path = config_path or os.getenv("WB_PARSER_CONFIG", "config/config.yaml")
    config = load_config(cfg_path)
    configure_logging(config)

    db = StateDB(config.paths.SQLITE_DB)
    db.init_schema()

    auth_manager = build_auth_manager(config)
    services = WebUIServices(config)
    logger = get_logger("webui")

    app = FastAPI(title="WB Parser Web UI", version="1.0")
    app.add_middleware(SessionMiddleware, secret_key=auth_manager.secret_key, same_site="lax", https_only=False)

    templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

    app.state.config = config
    app.state.db = db
    app.state.templates = templates
    app.state.auth_manager = auth_manager
    app.state.webui_services = services

    static_dir = Path(__file__).parent / "static"
    static_dir.mkdir(parents=True, exist_ok=True)
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")


    @app.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/login", response_class=HTMLResponse)
    def login_page(request: Request) -> HTMLResponse:
        msg = request.query_params.get("msg", "")
        return templates.TemplateResponse(
            request,
            "login.html",
            {
                "title": "Login",
                "message": msg,
            },
        )

    @app.post("/login")
    def login_submit(request: Request, username: str = Form(...), password: str = Form(...)) -> RedirectResponse:
        if not auth_manager.is_configured:
            return RedirectResponse(url="/login?msg=Auth%20is%20not%20configured", status_code=303)

        if not auth_manager.verify(username=username, password=password):
            return RedirectResponse(url="/login?msg=Invalid%20credentials", status_code=303)

        request.session[SESSION_USER_KEY] = username
        services.log_ui_action(user=username, action="login")
        return RedirectResponse(url="/", status_code=303)

    @app.get("/logout")
    def logout(request: Request) -> RedirectResponse:
        user = str(request.session.get(SESSION_USER_KEY, ""))
        request.session.pop(SESSION_USER_KEY, None)
        if user:
            services.log_ui_action(user=user, action="logout")
        return RedirectResponse(url="/login?msg=Logged%20out", status_code=303)

    app.include_router(dashboard_router)
    app.include_router(runs_router)
    app.include_router(logs_router)
    app.include_router(files_router)
    app.include_router(config_router)
    app.include_router(actions_router)

    logger.info("webui_started", extra={"component": "webui", "status": "ready"})
    return app


app = create_app()

