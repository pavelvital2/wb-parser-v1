from __future__ import annotations

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from .dependencies import get_current_user, get_services, get_templates
from .services import WebUIServices


router = APIRouter()


def _redirect(url: str) -> RedirectResponse:
    return RedirectResponse(url=url, status_code=303)


@router.get("/config/prefixes", response_class=HTMLResponse)
def config_prefixes_page(
    request: Request,
    user: str = Depends(get_current_user),
    services: WebUIServices = Depends(get_services),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    content = services.load_text_config("prefixes")
    msg = request.query_params.get("msg", "")
    return templates.TemplateResponse(
        request,
        "config_prefixes.html",
        {
            "title": "Config Prefixes",
            "user": user,
            "content": content,
            "message": msg,
        },
    )


@router.post("/config/prefixes")
def config_prefixes_save(
    request: Request,
    content: str = Form(default=""),
    user: str = Depends(get_current_user),
    services: WebUIServices = Depends(get_services),
) -> RedirectResponse:
    services.save_text_config("prefixes", content)
    services.log_ui_action(user=user, action="save_prefixes")
    return _redirect("/config/prefixes?msg=Saved")


@router.get("/config/wordstat", response_class=HTMLResponse)
def config_wordstat_page(
    request: Request,
    user: str = Depends(get_current_user),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    msg = request.query_params.get("msg", "")
    return templates.TemplateResponse(
        request,
        "config_wordstat.html",
        {
            "title": "Wordstat Upload",
            "user": user,
            "message": msg,
        },
    )


@router.post("/config/wordstat")
async def config_wordstat_upload(
    request: Request,
    wordstat_file: UploadFile = File(...),
    user: str = Depends(get_current_user),
    services: WebUIServices = Depends(get_services),
) -> RedirectResponse:
    payload = await wordstat_file.read()
    saved = services.save_wordstat_upload(wordstat_file.filename or "wordstat_upload.csv", payload)
    services.log_ui_action(user=user, action="upload_wordstat", details=saved.name)
    return _redirect(f"/config/wordstat?msg=Uploaded%20{saved.name}")


@router.get("/config/query-rules", response_class=HTMLResponse)
def config_rules_page(
    request: Request,
    user: str = Depends(get_current_user),
    services: WebUIServices = Depends(get_services),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    content = services.load_text_config("query_rules")
    msg = request.query_params.get("msg", "")
    return templates.TemplateResponse(
        request,
        "config_rules.html",
        {
            "title": "Config Query Rules",
            "user": user,
            "content": content,
            "message": msg,
        },
    )


@router.post("/config/query-rules")
def config_rules_save(
    request: Request,
    content: str = Form(default=""),
    user: str = Depends(get_current_user),
    services: WebUIServices = Depends(get_services),
) -> RedirectResponse:
    try:
        services.save_text_config("query_rules", content)
    except Exception as exc:
        return _redirect(f"/config/query-rules?msg=YAML%20error:%20{exc}")

    services.log_ui_action(user=user, action="save_query_rules")
    return _redirect("/config/query-rules?msg=Saved")


@router.get("/config/main", response_class=HTMLResponse)
def config_main_page(
    request: Request,
    user: str = Depends(get_current_user),
    services: WebUIServices = Depends(get_services),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    content = services.load_text_config("main")
    msg = request.query_params.get("msg", "")
    return templates.TemplateResponse(
        request,
        "config_main.html",
        {
            "title": "Config Main",
            "user": user,
            "content": content,
            "message": msg,
        },
    )


@router.post("/config/main")
def config_main_save(
    request: Request,
    content: str = Form(default=""),
    user: str = Depends(get_current_user),
    services: WebUIServices = Depends(get_services),
) -> RedirectResponse:
    try:
        services.save_text_config("main", content)
    except Exception as exc:
        return _redirect(f"/config/main?msg=YAML%20error:%20{exc}")

    services.log_ui_action(user=user, action="save_main_config")
    return _redirect("/config/main?msg=Saved")
