"""页面建模领域服务 — 源端结构定义、分页边界与发布。

数据工程核心：数据标准化与字段映射转换 — 跨模块数据产物

数据流:
    page_model (定义源端结构 + 分页游标) → channel("rpa.source_configured")
    → data_extract 消费 → 只取尚未覆盖的页面 → channel("rpa.data_collected")

分页续做的两条边界（互斥，都不会重复输出同一行）：
- 页码边界：``covered_pages`` 只增不减，续做从 ``next_page`` 往后扩展；
- 内容边界：``seen`` 按内容指纹判重，同页重抓只补差量。

两套边界与已见内容指纹一起落在 ``store.page_cursors`` 里，
所以重开页面时不会从首页重新累计同一行数据。
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store

#: 分页模式关键词 → 规范化模式
PAGINATION_MODE_TOKENS = (
    ("下一页", "next"),
    ("next", "next"),
    ("页码", "page"),
    ("page", "page"),
    ("偏移", "offset"),
    ("offset", "offset"),
    ("游标", "cursor"),
    ("cursor", "cursor"),
    ("无分页", "none"),
    ("none", "none"),
    ("single", "none"),
)

#: 两套允许的续做边界 + 兼容既有配置的混合边界
BOUNDARY_POLICIES: dict[str, str] = {
    "content_first": "按内容指纹追加版本",
    "page_first": "等待页码推进后整页重抓",
    "page_then_content": "页码优先，同页按内容指纹补差",
}

#: 边界冲突落到的处理终态（9 号处理终态位）
CONFLICT_TERMINAL_STATUS = "结构失效"

DEFAULT_PAGE_SIZE = 10
MAX_PAGE_SIZE = 200


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


class PageModelService(BaseModuleService):
    MODULE = "page_model"
    ENTITY = "页面模型"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["模型名称", "目标站点", "页面结构", "动态元素", "分页模式", "加载策略", "版本号"]
    REQUIRED_FIELDS = ["模型名称", "目标站点", "页面结构"]
    STATUS_ORDER = ["草稿", "已验证", "已使用", "结构失效"]
    ACTION_RULES = {"验证模型": "草稿", "标记失效": "已验证"}
    NEGATIVE_ACTIONS = ["标记失效"]

    # ---- 分页配置解析 ----

    def pagination_config(self, entry: dict[str, Any]) -> dict[str, Any]:
        """读取页面模型的分页配置，容错解析历史/占位数据。

        Returns:
            {"mode", "page_size", "boundary_policy", "revision", "total_pages"}
        """
        raw_mode = str(entry.get("分页模式") or "").strip()
        mode = "auto"
        lowered = raw_mode.casefold()
        for token, normalized in PAGINATION_MODE_TOKENS:
            if token.casefold() in lowered:
                mode = normalized
                break

        page_size = _safe_int(entry.get("每页条数"), DEFAULT_PAGE_SIZE) or DEFAULT_PAGE_SIZE
        page_size = min(max(page_size, 1), MAX_PAGE_SIZE)

        raw_boundary = str(entry.get("边界策略") or "").strip()
        boundary = raw_boundary if raw_boundary in BOUNDARY_POLICIES else "page_then_content"

        revision = _safe_int(entry.get("修订号"), 1) or 1
        total_pages = _safe_int(entry.get("总页数"), 0)
        return {
            "mode": mode,
            "mode_label": dict((v, k) for k, v in PAGINATION_MODE_TOKENS).get(mode, "自动识别"),
            "page_size": page_size,
            "boundary_policy": boundary,
            "boundary_label": BOUNDARY_POLICIES[boundary],
            "revision": revision,
            "total_pages": total_pages or None,
        }

    @staticmethod
    def _config_version(entry: dict[str, Any], pagination: dict[str, Any]) -> str:
        """页面配置指纹：配置不变则版本不变，配置一变下游就能判定快照过期。"""
        material = "|".join(str(part) for part in (
            entry.get("目标站点"), entry.get("页面结构"), entry.get("动态元素"),
            pagination["mode"], pagination["page_size"],
            pagination["boundary_policy"], pagination["revision"],
        ))
        return hashlib.sha256(material.encode("utf-8")).hexdigest()[:8]

    def _cursor_for(self, entry: dict[str, Any]) -> dict[str, Any]:
        """按「站点 + 模型」取续做游标；已有游标只刷新配置、不回退边界。"""
        pagination = self.pagination_config(entry)
        return store.page_cursors.acquire(
            site=str(entry.get("目标站点") or "unknown"),
            model_id=_safe_int(entry.get("id")),
            page_size=pagination["page_size"],
            pagination_mode=pagination["mode"],
            boundary_policy=pagination["boundary_policy"],
        )

    # ---- 续做快照 ----

    def build_source_snapshot(
        self,
        model_id: int,
        *,
        revision: int | None = None,
        observed_last_page: int | None = None,
        max_pages: int | None = None,
    ) -> dict[str, Any] | None:
        """生成带分页版本的源端配置快照。

        读取分页配置 + 修订范围内已采集记录，解析已见内容与剩余页边界。
        快照本身不推进游标，可以安全地重复生成。

        Args:
            model_id: 页面模型 ID
            revision: 修订范围（默认取模型当前修订号）
            observed_last_page: 页面上观测到的最后一页，用于解析剩余边界
            max_pages: 本次快照最多带上的剩余页数

        Returns:
            源端配置快照，模型不存在时返回 None
        """
        entry = store.find(self.MODULE, model_id)
        if entry is None:
            return None

        pagination = self.pagination_config(entry)
        revision_no = _safe_int(revision, pagination["revision"]) or pagination["revision"]
        cursor = self._cursor_for(entry)

        site = str(entry.get("目标站点") or "unknown")
        collected = [
            record for record in store.collect_records.list_by_site(site)
            if record.revision_no == revision_no and record.fingerprint
        ]
        record_fingerprints = {record.fingerprint for record in collected}
        # 已见内容 = 游标持久化指纹 ∪ 修订范围内已采集记录指纹。
        # 即使游标丢失，修订内的记录也能把已见内容补回来，避免重复累计。
        seen_fingerprints = set(cursor.get("seen", {})) | record_fingerprints
        # 剩余页边界：优先用页面上观测到的最后一页，其次用页面模型的总页数；
        # 两者都没有时按单页处理，避免续做凭空造出并不存在的下一页。
        boundary_last = observed_last_page
        if boundary_last is None:
            boundary_last = pagination["total_pages"] or 1
        remaining = store.page_cursors.pending_pages(
            cursor, boundary_last, max_pages)

        covered = sorted(int(page) for page in cursor.get("covered_pages", []))
        # 分页版本只随页面配置（站点/结构/分页模式/每页条数/边界策略/修订号）变化，
        # 不随采集进度变化 —— 下游核对的是「这份配置快照还对得上吗」。
        config_version = self._config_version(entry, pagination)
        pagination_version = f"m{model_id}.r{revision_no}.{config_version}"

        snapshot = {
            "model_id": model_id,
            "model_name": entry.get("模型名称"),
            "target_site": site,
            "page_structure": entry.get("页面结构"),
            "dynamic_elements": entry.get("动态元素", ""),
            "pagination_mode": pagination["mode"],
            "load_strategy": entry.get("加载策略", "lazy"),
            "version": entry.get("版本号", "1.0"),
            "revision_no": revision_no,
            "fields_to_extract": self._infer_extractable_fields(entry),
            "pagination": {
                "mode": pagination["mode"],
                "page_size": pagination["page_size"],
                "boundary_policy": pagination["boundary_policy"],
                "boundary_label": pagination["boundary_label"],
                "revision": revision_no,
                "pagination_version": pagination_version,
                "covered_pages": covered,
                "next_page": cursor.get("next_page", 1),
                "remaining_pages": remaining,
                "seen_count": len(seen_fingerprints),
                "seen_fingerprints": sorted(seen_fingerprints),
                "observed_last_page": observed_last_page,
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "snapshot_version": store.page_cursors.SNAPSHOT_VERSION,
        }
        snapshot["pagination_version"] = pagination_version
        return snapshot

    # ---- 发布 ----

    def publish_source_config(
        self, model_id: int, observed_last_page: int | None = None,
    ) -> dict[str, Any] | None:
        """发布页面模型作为源端配置，供 data_extract 消费。

        快照带分页版本与剩余页边界；下游只消费尚未覆盖的页面内容。

        Args:
            model_id: 页面模型 ID
            observed_last_page: 页面上观测到的最后一页

        Returns:
            channel 消息概要或 None
        """
        entry = store.find(self.MODULE, model_id)
        if entry is None:
            return None

        current = entry.get("status")
        if current not in ("已验证", "草稿", "已使用"):
            return None

        snapshot = self.build_source_snapshot(
            model_id, observed_last_page=observed_last_page)
        if snapshot is None:
            return None

        entry["status"] = "已使用"
        entry["pending"] = bool(snapshot["pagination"]["remaining_pages"])

        msg = store.channel.publish(
            topic="rpa.source_configured",
            source=self.MODULE,
            payload=snapshot,
        )
        return {
            "model_id": model_id,
            "status": "已使用",
            "msg_id": msg.msg_id,
            "target_site": snapshot["target_site"],
            "inferred_fields": len(snapshot["fields_to_extract"]),
            "pagination_version": snapshot["pagination_version"],
            "covered_pages": snapshot["pagination"]["covered_pages"],
            "remaining_pages": snapshot["pagination"]["remaining_pages"],
            "seen_count": snapshot["pagination"]["seen_count"],
        }

    def _infer_extractable_fields(self, entry: dict[str, Any]) -> list[str]:
        """从页面结构推断可提取字段（基于结构描述关键词）。"""
        structure = str(entry.get("页面结构", "")) + str(entry.get("动态元素", ""))
        field_keywords = {
            "表格": ["序号", "名称", "状态", "金额", "日期"],
            "列表": ["标题", "链接", "摘要", "时间"],
            "表单": ["字段名", "字段值", "类型", "必填"],
            "详情": ["标题", "内容", "标签", "发布日期"],
        }
        for keyword, fields in field_keywords.items():
            if keyword in structure:
                return fields
        return ["数据项1", "数据项2", "数据项3"]  # 默认字段

    # ---- 工作台视图 ----

    def resume_overview(
        self,
        *,
        model_id: int | None = None,
        site: str | None = None,
    ) -> dict[str, Any]:
        """按站点查看分页状态、已覆盖页和已见内容数量。"""
        rows = store.rows(self.MODULE)
        if model_id is not None:
            rows = [row for row in rows if _safe_int(row.get("id")) == int(model_id)]

        entries: list[dict[str, Any]] = []
        for entry in rows:
            entry_site = str(entry.get("目标站点") or "unknown")
            if site and site.casefold() not in entry_site.casefold():
                continue
            cursor = self._cursor_for(entry)
            view = store.page_cursors.resume_snapshot(cursor)
            pagination = self.pagination_config(entry)
            entries.append({
                "model_id": entry.get("id"),
                "model_name": entry.get("模型名称"),
                "status": entry.get("status"),
                "site": entry_site,
                "pagination_mode": pagination["mode"],
                "boundary_policy": pagination["boundary_policy"],
                "page_size": pagination["page_size"],
                "revision": pagination["revision"],
                "pagination_version": (
                    f"m{entry.get('id')}.r{pagination['revision']}"
                    f".{self._config_version(entry, pagination)}"
                ),
                "covered_pages": view["covered_pages"],
                "next_page": view["next_page"],
                "seen_count": view["seen_count"],
                "versions": view["versions"],
                "uncovered": bool(
                    pagination["total_pages"]
                    and view["next_page"] <= int(pagination["total_pages"] or 0)
                ),
                "interrupted": cursor.get("interrupted"),
                "deferred_pages": sorted(cursor.get("deferred_pages", {})),
                "updated_at": view["updated_at"],
            })

        sites: dict[str, dict[str, Any]] = {}
        for item in entries:
            bucket = sites.setdefault(item["site"], {
                "site": item["site"],
                "model_count": 0,
                "covered_pages": set(),
                "seen_rows": 0,
                "next_pages": [],
            })
            bucket["model_count"] += 1
            bucket["covered_pages"].update(item["covered_pages"])
            bucket["seen_rows"] += item["seen_count"]
            bucket["next_pages"].append(item["next_page"])

        return {
            "sites": [
                {
                    "site": bucket["site"],
                    "model_count": bucket["model_count"],
                    "covered_pages": sorted(bucket["covered_pages"]),
                    "seen_rows": bucket["seen_rows"],
                    "next_page": min(bucket["next_pages"]) if bucket["next_pages"] else 1,
                }
                for bucket in sites.values()
            ],
            "models": entries,
            "summary": {
                "model_count": len(entries),
                "covered_pages": sum(len(item["covered_pages"]) for item in entries),
                "seen_rows": sum(item["seen_count"] for item in entries),
            },
        }

    def refresh_todos(self) -> list[dict[str, Any]]:
        """业务入口联动：按游标刷新页面模型的待办标记。"""
        todos: list[dict[str, Any]] = []
        for entry in store.rows(self.MODULE):
            cursor = self._cursor_for(entry)
            view = store.page_cursors.resume_snapshot(cursor)
            pagination = self.pagination_config(entry)
            pending = bool(
                cursor.get("interrupted")
                or cursor.get("deferred_pages")
                or (
                    pagination["total_pages"]
                    and view["next_page"] <= int(pagination["total_pages"] or 0)
                )
            )
            entry["pending"] = pending
            if pending:
                todos.append({
                    "model_id": entry.get("id"),
                    "model_name": entry.get("模型名称"),
                    "site": entry.get("目标站点"),
                    "next_page": view["next_page"],
                    "seen_count": view["seen_count"],
                    "reason": (
                        cursor.get("interrupted", {}).get("reason")
                        if cursor.get("interrupted")
                        else "存在未覆盖页面"
                    ),
                })
        return todos

    # ---- 增量字段映射明细 ----

    def save_field_mapping(
        self,
        model_id: int,
        mappings: list[dict[str, Any]],
        *,
        revision: int | None = None,
        note: str = "",
    ) -> dict[str, Any] | None:
        """保存增量字段映射明细，供工作台核对与中断后续做。

        Args:
            model_id: 页面模型 ID
            mappings: [{"source_field","target_field","data_type","format"}...]
            revision: 修订范围（默认当前修订号）
            note: 备注
        """
        entry = store.find(self.MODULE, model_id)
        if entry is None:
            return None

        pagination = self.pagination_config(entry)
        revision_no = _safe_int(revision, pagination["revision"]) or pagination["revision"]
        normalized: list[dict[str, Any]] = []
        for item in mappings:
            source_field = str(item.get("source_field") or item.get("源字段") or "").strip()
            target_field = str(item.get("target_field") or item.get("目标字段") or "").strip()
            if not source_field or not target_field:
                continue
            normalized.append({
                "source_field": source_field,
                "target_field": target_field,
                "data_type": str(item.get("data_type") or "string"),
                "format": str(item.get("format") or ""),
            })

        payload = {
            "model_id": model_id,
            "model_name": entry.get("模型名称"),
            "site": entry.get("目标站点"),
            "revision": revision_no,
            "mappings": normalized,
            "note": note,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        journal_entry = store.record_mapping(payload)
        entry["field_mappings"] = normalized
        entry["mapping_revision"] = revision_no
        return {
            "mapping_id": journal_entry.get("id"),
            "model_id": model_id,
            "revision": revision_no,
            "saved": len(normalized),
            "total_mappings": len(normalized),
        }

    def field_mapping_detail(
        self, model_id: int | None = None, revision: int | None = None,
    ) -> dict[str, Any]:
        entries = store.mapping_snapshot()["entries"]
        if model_id is not None:
            entries = [e for e in entries if _safe_int(e.get("model_id")) == int(model_id)]
        if revision is not None:
            entries = [e for e in entries if _safe_int(e.get("revision")) == int(revision)]
        return {
            "count": len(entries),
            "fields": sum(len(e.get("mappings", [])) for e in entries),
            "entries": entries,
        }

    # ---- 冲突终态 ----

    def resume_crawl(
        self,
        model_id: int,
        *,
        observed_last_page: int | None = None,
        max_pages: int = 5,
        handle_ref: str | None = None,
        recheck_pages: list[int] | None = None,
    ) -> dict[str, Any] | None:
        """工作台「继续未覆盖页」入口：发布快照并联动推进采集与待办。

        业务入口打通后：重新生成带分页版本的源端配置快照 → data_extract
        按边界消费未覆盖页 → 刷新页面模型待办。已完成页不会被回退重抓。

        Returns:
            {"model_id", "handle_ref", "publish", "extract", "todo"}
        """
        snapshot = self.build_source_snapshot(
            model_id, observed_last_page=observed_last_page)
        if snapshot is None:
            return None

        if not snapshot["pagination"]["remaining_pages"]:
            self.refresh_todos()
            return {
                "model_id": model_id,
                "handle_ref": handle_ref,
                "publish": None,
                "extract": None,
                "todo": None,
                "message": "没有未覆盖页，无需续做",
            }

        published = self.publish_source_config(
            model_id, observed_last_page=observed_last_page)
        if published is None:
            return None

        from app.services.data_extract import DataExtractService

        extract = DataExtractService().consume_source_and_extract(
            handle_ref=handle_ref,
            observed_last_page=observed_last_page,
            max_pages=max_pages,
            recheck_pages=recheck_pages,
        )
        todos = self.refresh_todos()
        todo = next((item for item in todos if item["model_id"] == model_id), None)
        return {
            "model_id": model_id,
            "handle_ref": extract["handle_ref"],
            "publish": published,
            "extract": extract,
            "todo": todo,
            "message": (
                "已在续做边界后继续未覆盖页"
                if extract["pages_covered"]
                else "无新增页面内容"
            ),
        }

    def mark_conflict(self, model_id: int, reason: str) -> dict[str, Any] | None:
        """分页边界冲突：落到页面源配置的 9 号处理终态并保留原因。"""
        entry = store.find(self.MODULE, model_id)
        if entry is None:
            return None
        entry["status"] = CONFLICT_TERMINAL_STATUS
        entry["pending"] = False
        entry["abnormal"] = True
        entry["conflict_reason"] = reason
        entry["conflict_at"] = datetime.now(timezone.utc).isoformat()
        return {
            "model_id": model_id,
            "terminal_status": CONFLICT_TERMINAL_STATUS,
            "reason": reason,
        }

    def list_active_models(self) -> list[dict[str, Any]]:
        """查询所有已使用状态的页面模型。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "model_id": r.get("id"), "name": r.get("模型名称"),
                "site": r.get("目标站点"), "structure": r.get("页面结构"),
                "pagination": r.get("分页模式"), "version": r.get("版本号"),
                "revision": self.pagination_config(r)["revision"],
            }
            for r in rows if r.get("status") == "已使用"
        ]

    def validate_model_structure(self, model_id: int) -> dict[str, Any]:
        """验证页面模型结构完整性，标记潜在问题。"""
        entry = store.find(self.MODULE, model_id)
        if entry is None:
            return {"model_id": model_id, "valid": False, "issues": ["模型不存在"]}

        issues = []
        if not str(entry.get("目标站点", "")).strip():
            issues.append("目标站点为空")
        if not str(entry.get("页面结构", "")).strip():
            issues.append("页面结构未定义")
        if self.pagination_config(entry)["mode"] == "none":
            issues.append("分页模式未配置")

        return {
            "model_id": model_id,
            "model_name": entry.get("模型名称"),
            "valid": len(issues) == 0,
            "issues": issues,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        active = self.list_active_models()
        channel_events = store.channel.snapshot(topic="rpa.source_configured")
        cursors = store.page_cursors.snapshot()
        return {
            **base,
            "active_models": len(active),
            "source_config_events": len(channel_events),
            "pagination": cursors,
            "mapping_journal": store.mapping_snapshot()["total"],
        }
