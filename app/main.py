"""FastAPI 入口：网页 + JSON API。"""
from __future__ import annotations

import os
import json
from contextlib import asynccontextmanager

from fastapi import Body, FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from . import core, importer
from .db import init_db, session

MAX_CSV_BYTES = 20 * 1024 * 1024
MAX_IMPORT_BYTES = importer.MAX_IMPORT_BYTES


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="环境监测小时观测数据平台", lifespan=lifespan)
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
)


@app.exception_handler(core.DomainError)
async def domain_error_handler(request: Request, exc: core.DomainError):
    if isinstance(exc, importer.ConflictError):
        status = 409
    elif isinstance(exc, core.NotFoundError):
        status = 404
    else:
        status = 400
    return JSONResponse(status_code=status,
                        content={"detail": exc.message, "errors": exc.errors})


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(request, "index.html")


@app.get("/api/versions")
def api_list_versions():
    with session() as conn:
        return {"versions": core.list_versions(conn)}


@app.get("/api/versions/current")
def api_current_version():
    with session() as conn:
        v = core.get_active_version(conn)
        if v is None:
            raise core.NotFoundError("尚未发布任何结构版本")
        return core.version_with_columns(conn, v)


@app.get("/api/versions/{version_no}")
def api_get_version(version_no: int):
    with session() as conn:
        v = core.get_version(conn, version_no)
        if v is None:
            raise core.NotFoundError(f"版本 v{version_no} 不存在")
        return core.version_with_columns(conn, v)


@app.post("/api/versions", status_code=201)
def api_publish_version(spec: dict = Body(...)):
    with session() as conn:
        return core.publish_version(conn, spec)


@app.post("/api/batches", status_code=201)
async def api_upload_batch(file: UploadFile = File(...)):
    raw = await file.read()
    if len(raw) > MAX_CSV_BYTES:
        raise core.DomainError("CSV 文件过大（上限 20MB）")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise core.DomainError("CSV 必须是 UTF-8 编码")
    with session() as conn:
        return core.upload_csv(conn, file.filename or "", text)


@app.get("/api/batches")
def api_list_batches():
    with session() as conn:
        return {"batches": core.list_batches(conn)}


@app.post("/api/imports/preview")
async def api_import_preview(
    file: UploadFile = File(...),
    mapping: str | None = Form(default=None),
):
    """补录批次 JSON 预览。

    第一次不传 mapping：返回字段清单与现役数据列供配对；
    带上 mapping（JSON 字符串，[{field_id, column_id}]）：完整校验并钉住预览快照。
    同库同号原样重传返回原入库批次号；内容有变返回 409。
    """
    raw = await file.read()
    if len(raw) > MAX_IMPORT_BYTES:
        raise core.DomainError("JSON 文件过大（上限 20MB）")
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise core.DomainError("JSON 必须是 UTF-8 编码")
    mapping_obj = None
    if mapping is not None and mapping.strip():
        try:
            mapping_obj = json.loads(mapping)
        except json.JSONDecodeError as e:
            raise core.DomainError(f"mapping 不是合法 JSON：{e}")
    with session() as conn:
        return importer.create_preview(conn, file.filename or "", text, mapping_obj)


@app.post("/api/imports/{preview_id}/commit")
def api_import_commit(preview_id: int):
    """按预览快照入库；现役结构换版则拒绝并要求重新预览。"""
    with session() as conn:
        result = importer.commit_preview(conn, preview_id)
    if result.get("idempotent"):
        return JSONResponse(status_code=200, content=result)
    return JSONResponse(status_code=201, content=result)


@app.get("/api/observations")
def api_observations(
    station: str | None = None,
    hour_from: str | None = None,
    hour_to: str | None = None,
    version_no: int | None = None,
    limit: int = 1000,
):
    limit = max(1, min(limit, 10000))
    try:
        hf = core.normalize_hour(hour_from) if hour_from else None
        ht = core.normalize_hour(hour_to) if hour_to else None
    except ValueError as e:
        raise core.DomainError(str(e))
    with session() as conn:
        return core.query_observations(
            conn, station=station, hour_from=hf, hour_to=ht,
            version_no=version_no, limit=limit,
        )
