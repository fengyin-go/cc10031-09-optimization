"""页面建模工作台接口：分页续做、已覆盖页、增量字段映射明细。

这些固定路径必须注册在通配资源路由之前，否则 /api/page_model/... 会被
/api/{module}/{entry_id} 抢先匹配成 entry_id 解析失败。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.schemas import ActionResult

router = APIRouter(prefix="/api", tags=["页面建模工作台"])


class ResumePayload(BaseModel):
    """续做请求：可指定观测到的最后一页、处理项编号和本次最多覆盖页数。"""
    observed_last_page: int | None = None
    max_pages: int | None = Field(default=None, ge=1, le=50)
    handle_ref: str | None = None
    recheck_pages: list[int] | None = None


class MappingPayload(BaseModel):
    """增量字段映射明细保存请求。"""
    model_id: int
    revision: int | None = None
    note: str = ""
    mappings: list[dict[str, Any]] = Field(default_factory=list)


class PaginationPayload(BaseModel):
    """分页配置保存请求：只改分页相关字段，不改已有的续做边界。"""
    pagination_mode: str = "页码分页"
    page_size: int = Field(default=10, ge=1, le=200)
    total_pages: int | None = Field(default=None, ge=0, le=1000)
    boundary_policy: str = "page_then_content"
    revision: int | None = Field(default=None, ge=1)


@router.post("/page_model/{model_id}/pagination", response_model=ActionResult)
def save_pagination_config(model_id: int, payload: PaginationPayload) -> ActionResult:
    """保存页面模型的分页配置（站点分页状态与续做策略）。"""
    from app.services.page_model import BOUNDARY_POLICIES, PageModelService
    from app.store import store

    entry = store.find("page_model", model_id)
    if entry is None:
        return ActionResult(ok=False, message=f"页面模型 {model_id} 不存在")
    if payload.boundary_policy not in BOUNDARY_POLICIES:
        return ActionResult(ok=False, message=f"未知边界策略：{payload.boundary_policy}")

    entry["分页模式"] = payload.pagination_mode
    entry["每页条数"] = payload.page_size
    entry["总页数"] = payload.total_pages or ""
    entry["边界策略"] = payload.boundary_policy
    if payload.revision is not None:
        entry["修订号"] = payload.revision
    view = PageModelService().resume_overview(model_id=model_id)
    return ActionResult(
        ok=True,
        message="分页配置已保存，续做边界保持不变",
        entry=view["models"][0] if view["models"] else None,
    )


@router.get("/page_model/resume", response_model=dict)
def page_model_resume(
    site: str | None = Query(default=None),
    model_id: int | None = Query(default=None),
) -> dict[str, Any]:
    """按站点查看分页状态、已覆盖页和已见内容数量。"""
    from app.services.page_model import PageModelService

    return PageModelService().resume_overview(site=site, model_id=model_id)


@router.post("/page_model/{model_id}/resume", response_model=dict)
def page_model_resume_crawl(model_id: int, payload: ResumePayload) -> dict[str, Any]:
    """继续未覆盖页：重新发布快照，按续做边界只消费未覆盖的页面内容。"""
    from app.services.page_model import PageModelService

    service = PageModelService()
    result = service.resume_crawl(
        model_id,
        observed_last_page=payload.observed_last_page,
        max_pages=payload.max_pages or 5,
        handle_ref=payload.handle_ref,
        recheck_pages=payload.recheck_pages,
    )
    if result is None:
        raise HTTPException(status_code=404, detail=f"页面模型 {model_id} 不存在或不可续做")
    return {**result, "ok": True}


@router.get("/page_model/mappings", response_model=dict)
def field_mapping_detail(
    model_id: int | None = Query(default=None),
    revision: int | None = Query(default=None),
) -> dict[str, Any]:
    """读取增量字段映射采集明细。"""
    from app.services.page_model import PageModelService

    return PageModelService().field_mapping_detail(model_id, revision)


@router.post("/page_model/mappings", response_model=ActionResult)
def save_field_mapping(payload: MappingPayload) -> ActionResult:
    """保存增量字段映射明细。"""
    from app.services.page_model import PageModelService

    result = PageModelService().save_field_mapping(
        payload.model_id, payload.mappings,
        revision=payload.revision, note=payload.note,
    )
    if result is None:
        return ActionResult(ok=False, message=f"页面模型 {payload.model_id} 不存在")
    return ActionResult(
        ok=True,
        message=f"已保存 {result['saved']} 条增量字段映射明细",
        entry=result,
    )


@router.get("/data_extract/summary", response_model=dict)
def data_extract_summary() -> dict[str, Any]:
    """数据提取侧的分页续做统计与核对结果。"""
    from app.services.data_extract import DataExtractService

    return DataExtractService().get_collection_stats()


@router.get("/data_extract/duplicate-check", response_model=dict)
def duplicate_check() -> dict[str, Any]:
    """核对重开页面不会重复累计同一行数据。"""
    from app.services.data_extract import DataExtractService

    return DataExtractService().check_duplicate_accumulation()
