from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.templating import Jinja2Templates

from .dependencies import get_current_user, get_services, get_templates
from .services import WebUIServices


router = APIRouter()


@router.get("/files", response_class=HTMLResponse)
def files_page(
    request: Request,
    root: str = Query(default="raw"),
    subdir: str = Query(default=""),
    user: str = Depends(get_current_user),
    services: WebUIServices = Depends(get_services),
    templates: Jinja2Templates = Depends(get_templates),
) -> HTMLResponse:
    error = ""
    rows: list[dict[str, str]] = []
    try:
        rows = services.list_files(root_key=root, subdir=subdir)
    except Exception as exc:
        error = str(exc)

    return templates.TemplateResponse(
        request,
        "files.html",
        {
            "title": "Files",
            "user": user,
            "roots": list(services.list_roots().keys()),
            "selected_root": root,
            "subdir": subdir,
            "rows": rows,
            "error": error,
        },
    )


@router.get("/files/download")
def download_file(
    root: str = Query(...),
    path: str = Query(...),
    user: str = Depends(get_current_user),
    services: WebUIServices = Depends(get_services),
) -> FileResponse:
    try:
        target = services.resolve_file_path(root_key=root, relative_path=path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    return FileResponse(path=target, filename=target.name, media_type="application/octet-stream")
