"""数据提取领域服务 — 核对源端配置快照，按分页边界增量采集。

数据工程核心：数据归档存储与快照管理 — 跨模块数据产物

数据流:
    channel("rpa.source_configured") ← page_model（带分页版本快照）
    → data_extract 核对快照 + 按处理边界只消费未覆盖页
    → CollectRecord + DirtyDataVault → channel("rpa.data_collected") → result_verify

边界规则:
    - 快照版本必须是当前页面模型生成的最新版本，否则拒绝消费（不拿旧边界采集）；
    - 只消费快照 ``remaining_pages`` 里的页码，已完成页一律不再重抓；
    - 每行内容指纹在提交时统一判重，重开页面不会重复累计同一行。
"""
from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import content_fingerprint, store


class SnapshotMismatch(Exception):
    """处理项对应的源端配置快照与当前页面模型不一致。"""


class DataExtractService(BaseModuleService):
    MODULE = "data_extract"
    ENTITY = "提取规则"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["规则名称", "源页面结构", "提取字段", "提取路径", "数据类型", "格式化", "去空策略"]
    REQUIRED_FIELDS = ["规则名称", "源页面结构", "提取字段"]
    STATUS_ORDER = ["草稿", "已测试", "已生效", "已过期"]
    ACTION_RULES = {"测试提取": "草稿", "启用规则": "已测试"}
    NEGATIVE_ACTIONS = []

    #: 一次采集最多消费多少页（按处理项边界切分）
    MAX_PAGES_PER_HANDLE = 5

    # ---- 跨模块业务方法 (field_mapping + data_extractor 角色) ----

    def consume_source_and_extract(
        self,
        *,
        handle_ref: str | None = None,
        observed_last_page: int | None = None,
        max_pages: int | None = None,
        resume: bool = True,
        recheck_pages: list[int] | None = None,
    ) -> dict[str, Any]:
        """消费 page_model 的源端配置快照，按边界执行增量采集。

        Args:
            handle_ref: 处理项编号；同一编号重复提交只算一次
            observed_last_page: 页面当前最后一页（None 时按快照里的剩余边界）
            max_pages: 本次处理项最多覆盖几页
            resume: True 表示续做（不重抓已完成页）
            recheck_pages: 重开页面后要复核内容的已完成页（页码没推进但内容可能变了）

        Returns:
            {"extracted", "dirty", "duplicates", "batches", "pages_covered",
             "remaining_pages", "handle_ref", "skipped"}
        """
        handle = handle_ref or f"HANDLE-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
        admitted, previous = store.begin_handle(handle)
        if not admitted:
            # 处理项边界：并发两次提交只算一次
            return {
                "extracted": 0, "dirty": 0, "duplicates": 0, "batches": [],
                "pages_covered": [], "remaining_pages": [],
                "handle_ref": handle, "skipped": "duplicate_handle",
                "handle_status": (previous or {}).get("status"),
            }

        try:
            msgs = store.channel.consume(
                consumer=self.MODULE,
                topic="rpa.source_configured",
            )
            if not msgs:
                store.finish_handle(handle, {
                    "extracted": 0, "pages_covered": [],
                    "note": "没有待消费的源端配置",
                })
                return {
                    "extracted": 0, "dirty": 0, "duplicates": 0, "batches": [],
                    "pages_covered": [], "remaining_pages": [],
                    "handle_ref": handle, "skipped": "no_source_config",
                    "handle_status": "已完成",
                }

            batches: list[dict[str, Any]] = []
            pages: list[dict[str, Any]] = []
            total_extracted = total_dirty = total_duplicates = 0

            budget = max(int(max_pages or self.MAX_PAGES_PER_HANDLE), 1)
            interrupted: str | None = None
            interruption_reason = ""
            for msg in msgs:
                try:
                    outcome = self._consume_handle(
                        msg.payload, handle, budget,
                        observed_last_page=observed_last_page,
                        resume=resume,
                        recheck_pages=recheck_pages,
                    )
                except SnapshotMismatch as exc:
                    # 快照对不上：说明原因并留待下次重试
                    store.channel.confirm(msg.msg_id, self.MODULE)
                    store.recovery.log(
                        module=self.MODULE, batch_ref=handle,
                        step="核对源端配置快照", error_detail=str(exc),
                    )
                    interrupted = "snapshot_mismatch"
                    interruption_reason = str(exc)
                    batches.append({
                        "batch_ref": None,
                        "site": msg.payload.get("target_site"),
                        "extracted": 0, "dirty": 0, "duplicates": 0,
                        "skipped": str(exc),
                    })
                    continue
                except Exception as exc:  # noqa: BLE001
                    store.channel.confirm(msg.msg_id, self.MODULE)
                    reason = f"采集中断：{exc}"
                    store.recovery.log(
                        module=self.MODULE, batch_ref=handle,
                        step="分页续做", error_detail=reason,
                    )
                    interrupted = "exception"
                    interruption_reason = reason
                    batches.append({
                        "batch_ref": None,
                        "site": msg.payload.get("target_site"),
                        "extracted": 0, "dirty": 0, "duplicates": 0,
                        "skipped": reason,
                    })
                    continue

                total_extracted += outcome["extracted"]
                total_dirty += outcome["dirty"]
                total_duplicates += outcome["duplicates"]
                pages.extend(outcome["pages"])
                if outcome["batch_ref"]:
                    batches.append({
                        "batch_ref": outcome["batch_ref"],
                        "site": outcome["site"],
                        "extracted": outcome["extracted"],
                        "dirty": outcome["dirty"],
                        "duplicates": outcome["duplicates"],
                        "pages_covered": outcome["pages_covered"],
                        "remaining_pages": outcome["remaining_pages"],
                        "pagination_version": outcome["pagination_version"],
                    })
                elif outcome["skipped"]:
                    batches.append({
                        "batch_ref": None,
                        "site": outcome["site"],
                        "extracted": 0, "dirty": 0, "duplicates": 0,
                        "skipped": outcome["skipped"],
                    })
                store.channel.confirm(msg.msg_id, self.MODULE)

            pages_covered: list[int] = []
            remaining: list[int] = []
            for page in pages:
                if page.get("advanced") and page["page_no"] not in pages_covered:
                    pages_covered.append(page["page_no"])
            for batch in batches:
                for page_no in batch.get("remaining_pages", []):
                    if page_no not in remaining:
                        remaining.append(page_no)

            summary = {
                "extracted": total_extracted,
                "pages_covered": sorted(pages_covered),
                "remaining_pages": sorted(remaining),
                "interrupted": interrupted,
                "interrupt_reason": interruption_reason or None,
            }
            if interrupted:
                store.fail_handle(handle, interruption_reason)
            else:
                store.finish_handle(handle, summary)

            return {
                "extracted": total_extracted,
                "dirty": total_dirty,
                "duplicates": total_duplicates,
                "batches": batches,
                "pages_covered": sorted(pages_covered),
                "remaining_pages": sorted(remaining),
                "handle_ref": handle,
                "handle_status": "已中断" if interrupted else "已完成",
                "skipped": interrupted or None,
                "interrupt_reason": interruption_reason or None,
            }
        except Exception as exc:
            # 重试中断异常说明原因：台账保留中断记录，下次续做不会回退边界
            store.fail_handle(handle, f"处理项异常中断：{exc}")
            raise

    # ---- 单个处理项 ----

    def _consume_handle(
        self,
        snapshot: dict[str, Any],
        handle_ref: str,
        budget: int,
        *,
        observed_last_page: int | None,
        resume: bool,
        recheck_pages: list[int] | None = None,
    ) -> dict[str, Any]:
        """核对一个源端配置快照，只消费未覆盖页。"""
        model_id = int(snapshot.get("model_id") or 0)
        site = snapshot.get("target_site", "unknown")
        pagination = dict(snapshot.get("pagination") or {})

        current = self._snapshot_for_handle(model_id)
        if current is None:
            raise SnapshotMismatch(f"页面模型 {model_id} 已不存在，源端配置快照无法核对")
        if current["pagination_version"] != snapshot.get("pagination_version"):
            raise SnapshotMismatch(
                "源端配置快照已过期："
                f"快照 {snapshot.get('pagination_version')}，"
                f"当前 {current['pagination_version']}"
            )

        cursor = store.page_cursors.acquire(
            site=site, model_id=model_id,
            page_size=int(pagination.get("page_size") or 10),
            pagination_mode=str(pagination.get("mode") or "auto"),
            boundary_policy=str(pagination.get("boundary_policy") or "page_then_content"),
        )
        boundary_policy = cursor.get("boundary_policy", "page_then_content")

        # 以游标为准重新解析剩余页：快照里的边界只用于展示，不许把已完成页拉回来
        allowed_last = observed_last_page
        if allowed_last is None:
            configured = pagination.get("observed_last_page")
            if configured is not None:
                allowed_last = int(configured)
            else:
                remaining = pagination.get("remaining_pages") or []
                # 快照里没有未覆盖页时不要再往后造页（边界外没有内容可采）
                allowed_last = max(remaining) if remaining else 0
        if not resume:
            # 非续做模式也从续做边界起算，避免回退已完成边界
            allowed_last = max(int(allowed_last), int(cursor["next_page"]))
        planned = store.page_cursors.pending_pages(
            cursor, allowed_last, max_pages=budget)
        # 延后页优先整页重抓：页码优先边界下挂起的内容变化在这里消化
        deferred = sorted(int(p) for p in cursor.get("deferred_pages", {}))
        planned = deferred + [p for p in planned if p not in deferred]
        # 重开页面时复核已完成页：页码没推进但内容变了，交给对应边界处理
        for page_no in sorted(set(int(p) for p in (recheck_pages or []))):
            if page_no not in planned:
                planned.append(page_no)
        # 只依赖未被占用的页：并发处理项抢到同一页时后到者让位
        planned = [
            page_no for page_no in planned
            if page_no not in cursor.get("covered_pages", [])
            or page_no in cursor.get("deferred_pages", {})
            or page_no in (recheck_pages or [])
        ]

        batch_ref = (
            f"BATC-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
            f"-{handle_ref[-6:]}"
        )
        rules = self._get_active_rules_for_site(site)
        fields_to_extract = self._merge_fields(
            snapshot.get("fields_to_extract") or [], rules)

        extracted_total = dirty_total = duplicates_total = 0
        pages_detail: list[dict[str, Any]] = []
        accepted_all: list[dict[str, Any]] = []
        covered_before = set(cursor.get("covered_pages", []))

        for page_no in planned:
            if not store.page_cursors.claim_page(cursor, page_no):
                # 另一个处理项正在抓这一页，本次让位
                duplicates_total += 0
                continue
            try:
                rows = self._render_page_rows(
                    snapshot, fields_to_extract, page_no,
                    page_size=int(pagination.get("page_size") or 10),
                )
                rows = store.page_cursors.page_rows(cursor, page_no, rows)
                outcome = self._commit_page(
                    cursor, page_no, rows, batch_ref, boundary_policy,
                    model_id=model_id, revision=int(snapshot.get("revision_no") or 1),
                    pagination_version=snapshot.get("pagination_version", ""),
                    handle_ref=handle_ref,
                )
            finally:
                store.page_cursors.release_page(cursor, page_no)
            extracted_total += outcome["extracted"]
            dirty_total += outcome["dirty"]
            duplicates_total += outcome["duplicates"]
            accepted_all.extend(outcome["accepted"])
            pages_detail.append({
                "page_no": page_no,
                "version": outcome["version"],
                "extracted": outcome["extracted"],
                "dirty": outcome["dirty"],
                "duplicates": outcome["duplicates"],
                "deferred": outcome.get("deferred", False),
                "conflict": outcome.get("conflict", False),
            })

        # 只把真正新覆盖的页算作本次覆盖；复核页/延后页不计入
        covered_now = set(cursor.get("covered_pages", []))
        advanced = [
            page["page_no"] for page in pages_detail
            if page["page_no"] in covered_now
            and page["page_no"] not in covered_before
            and not page["deferred"]
        ]
        for page in pages_detail:
            page["advanced"] = page["page_no"] in advanced
        if advanced:
            # 续做成功推进了边界：清除上一次中断标记
            store.page_cursors.clear_interrupt(cursor)

        remaining_pages = store.page_cursors.pending_pages(
            cursor, allowed_last)

        result_payload = {
            "batch_ref": batch_ref,
            "model_id": model_id,
            "site": site,
            "handle_ref": handle_ref,
            "revision_no": snapshot.get("revision_no"),
            "pagination_version": snapshot.get("pagination_version"),
            "pages_covered": advanced,
            "remaining_pages": remaining_pages,
            "extracted": extracted_total,
            "dirty": dirty_total,
            "duplicates": duplicates_total,
            "extract_rules_applied": len(rules),
            "sample_records": accepted_all[:3],
            "rows": accepted_all,
            "boundary_policy": boundary_policy,
        }
        store.channel.publish(
            topic="rpa.data_collected",
            source=self.MODULE,
            payload=result_payload,
        )

        return {
            "batch_ref": batch_ref,
            "site": site,
            "model_id": model_id,
            "extracted": extracted_total,
            "dirty": dirty_total,
            "duplicates": duplicates_total,
            "pages_covered": advanced,
            "remaining_pages": remaining_pages,
            "pagination_version": snapshot.get("pagination_version"),
            "pages": pages_detail,
            "skipped": None if pages_detail else "无未覆盖页，处理项已完成",
        }

    def _snapshot_for_handle(self, model_id: int) -> dict[str, Any] | None:
        """核对用：按页面模型重新生成当前快照（不推进游标）。"""
        from app.services.page_model import PageModelService

        if model_id <= 0:
            return None
        return PageModelService().build_source_snapshot(model_id)

    def _commit_page(
        self,
        cursor: dict[str, Any],
        page_no: int,
        rows: list[dict[str, Any]],
        batch_ref: str,
        boundary_policy: str,
        *,
        model_id: int,
        revision: int,
        pagination_version: str,
        handle_ref: str,
    ) -> dict[str, Any]:
        """按边界提交一页：页码优先时先把内容变化挂起，等页码推进后整页重抓。"""
        preview = store.page_cursors.preview(cursor, page_no, rows)
        deferred_before = cursor.get("deferred_pages", {}).get(page_no)
        if (
            boundary_policy == "page_first"
            and preview["covered_before"]
            and preview["new_rows"]
        ):
            # 「页码推进」指挂起之后游标确实往前走过了，而不是这一页本身早被覆盖
            boundary_advanced = bool(
                deferred_before
                and int(cursor.get("next_page", 1))
                > int(deferred_before.get("next_page_at_defer", page_no))
            )
            if deferred_before and not boundary_advanced:
                # 挂起期间内容又变了、页码仍没推进：两套边界无法自洽，
                # 落到页面源配置的冲突终态，不再继续输出该页数据。
                conflict = store.page_cursors.defer_page(
                    cursor, page_no,
                    f"第 {page_no} 页在页码推进前反复变化，分页边界冲突",
                )
                from app.services.page_model import PageModelService

                PageModelService().mark_conflict(model_id, conflict["reason"])
                return {
                    "accepted": [], "extracted": 0, "dirty": 0, "duplicates": 0,
                    "version": None, "deferred": True, "conflict": True,
                }
            if not boundary_advanced:
                # 页码没推进却变了内容：先挂起，等页码推进后整页重抓，
                # 不在这里按行追加，避免同一行重复输出。
                store.page_cursors.defer_page(
                    cursor, page_no,
                    f"第 {page_no} 页内容变化但页码未推进，等待整页重抓",
                )
                return {
                    "accepted": [], "extracted": 0, "dirty": 0, "duplicates": 0,
                    "version": None, "deferred": True, "conflict": False,
                }
            # 页码已推进：现在整页重抓，旧行仍按内容指纹去重

        outcome = store.page_cursors.commit(
            cursor, page_no, rows, batch_ref=batch_ref)
        accepted = outcome["accepted"]
        if boundary_policy == "page_first":
            store.page_cursors.release_deferred(cursor, page_no)
            store.page_cursors.clear_page_content(cursor, page_no)

        valid: list[dict[str, Any]] = []
        dirty_count = 0
        for row in accepted:
            fingerprint = row.pop("_fingerprint", "")
            fields = {key: value for key, value in row.items()
                      if key != "_page_no"}
            fields["_site"] = cursor.get("site")
            fields["_page_no"] = page_no
            if not self._row_quality_ok(fields):
                dirty_count += 1
                store.dirty_vault.quarantine(
                    source=self.MODULE, batch_ref=batch_ref,
                    raw_data=dict(fields), reason="字段缺失",
                )
                continue
            store.collect_records.add(
                batch_ref=batch_ref, source=self.MODULE, fields=fields,
                fingerprint=fingerprint, page_no=page_no,
                config_version=str(pagination_version),
                revision_no=revision,
            )
            valid.append(fields)

        return {
            "accepted": valid,
            "extracted": len(valid),
            "dirty": dirty_count,
            "duplicates": len(outcome["duplicates"]),
            "version": outcome["version"],
            "deferred": False,
        }

    # ---- 页面内容渲染（模拟站点分页） ----

    def _render_page_rows(
        self,
        snapshot: dict[str, Any],
        fields: list[str],
        page_no: int,
        page_size: int = 10,
    ) -> list[dict[str, Any]]:
        """渲染某一页的真实内容。

        行内容只由「站点 + 页码 + 行号」决定，重开同一页得到完全相同的行，
        因此内容指纹稳定，判重才能生效。
        """
        site = str(snapshot.get("target_site") or "unknown")
        model_id = int(snapshot.get("model_id") or 0)
        rows: list[dict[str, Any]] = []
        for offset in range(max(page_size, 1)):
            index = (page_no - 1) * page_size + offset
            rows.append(self._generate_mock_record(
                {"target_site": site, "model_id": model_id}, fields, index))
        return rows

    def _merge_fields(
        self, inferred: list[str], rules: list[dict[str, Any]],
    ) -> list[str]:
        fields = list(inferred)
        for rule in rules:
            rule_field = rule.get("提取字段", "")
            if rule_field and rule_field not in fields:
                fields.append(rule_field)
        return fields

    def _row_quality_ok(self, fields: dict[str, Any]) -> bool:
        """质量检查：同一行内容永远得到同一个结论，重抓不会忽好忽坏。

        约 10% 的行会被判为字段缺失；判脏只影响它进隔离区，
        内容指纹照样记入「已见」，重开页面不会把它当新行重复输出。
        """
        fingerprint = content_fingerprint(
            {key: value for key, value in fields.items() if not key.startswith("_")}
        )
        return int(fingerprint[:4], 16) % 10 != 0

    def _get_active_rules_for_site(self, site: str) -> list[dict[str, Any]]:
        """获取目标站点关联的已生效提取规则（field_mapping 语义）。"""
        rows = store.rows(self.MODULE)
        active = [
            r for r in rows if r.get("status") == "已生效"
        ]
        if site:
            active = [
                r for r in active
                if site[:2] in (r.get("源页面结构") or "")
                or r.get("源页面结构", "") == site
            ]
        # 如果没有关联规则，用默认规则
        if not active and rows:
            active = [r for r in rows if r.get("status") != "已过期"][:3]
        return active

    def _generate_mock_record(
        self,
        config: dict[str, Any],
        fields: list[str],
        index: int,
    ) -> dict[str, Any]:
        """生成一条页面行内容：同站点同页码同序号必然生成同一份内容。"""
        site = config.get("target_site", "unknown")
        seed = f"{config.get('model_id', 0)}:{site}:{index}"
        rng = random.Random(seed)
        record: dict[str, Any] = {"_seq": index + 1}

        sample_templates = {
            "序号": lambda i: i + 1,
            "名称": lambda i: f"{site}-产品-{1000 + i}",
            "标题": lambda i: f"关于{site}的第{i + 1}期报告",
            "状态": lambda i: rng.choice(["正常", "异常", "待审核"]),
            "金额": lambda i: round(rng.uniform(100, 99999), 2),
            "日期": lambda i: f"2026-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}",
            "链接": lambda i: f"https://{site}.example.com/item/{i + 1}",
            "摘要": lambda i: f"{site}数据采集摘要 #{i + 1}",
            "时间": lambda i: f"2026-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}T08:00:00Z",
            "字段名": lambda i: f"field_{i + 1}",
            "字段值": lambda i: f"value_{rng.randint(1, 100)}",
            "内容": lambda i: f"{site}采集内容行 {i + 1}",
            "标签": lambda i: rng.choice(["采集", "RPA", "自动化"]),
            "发布日期": lambda i: f"2026-{rng.randint(1, 12):02d}-{rng.randint(1, 28):02d}",
            "类型": lambda i: rng.choice(["文本", "数值", "日期"]),
            "必填": lambda i: rng.choice(["是", "否"]),
            "数据项1": lambda i: f"data1_{i}",
            "数据项2": lambda i: f"data2_{i}",
            "数据项3": lambda i: f"data3_{i}",
        }

        for field in fields:
            if field in sample_templates:
                record[field] = sample_templates[field](index)
            else:
                record[field] = f"{field}-{index + 1}"

        return record

    def execute_mapping_and_extract(
        self,
        model_id: int,
        target_fields: list[str] | None = None,
        *,
        observed_last_page: int | None = None,
        handle_ref: str | None = None,
    ) -> dict[str, Any]:
        """直接执行字段映射 + 数据提取（不通过 channel 的简化版）。

        同样走分页游标：重复调用只补未覆盖页，不会重复累计同一行。

        Args:
            model_id: 页面模型 ID
            target_fields: 额外目标字段（可选）
            observed_last_page: 页面最后一页（可选）
            handle_ref: 处理项编号（可选）

        Returns:
            采集结果概要
        """
        from app.services.page_model import PageModelService

        model_entry = store.find("page_model", model_id)
        if model_entry is None:
            return {"error": f"页面模型 {model_id} 不存在"}

        snapshot = PageModelService().build_source_snapshot(
            model_id, observed_last_page=observed_last_page)
        assert snapshot is not None
        if target_fields:
            snapshot["fields_to_extract"] = target_fields
        store.channel.publish(
            topic="rpa.source_configured", source="page_model", payload=snapshot)

        outcome = self.consume_source_and_extract(
            handle_ref=handle_ref,
            observed_last_page=observed_last_page,
        )
        batch_ref = next(
            (b["batch_ref"] for b in outcome["batches"] if b["batch_ref"]), None)
        return {
            "batch_ref": batch_ref,
            "model_id": model_id,
            "extracted": outcome["extracted"],
            "dirty": outcome["dirty"],
            "duplicates": outcome["duplicates"],
            "pages_covered": outcome["pages_covered"],
            "remaining_pages": outcome["remaining_pages"],
            "handle_ref": outcome["handle_ref"],
        }

    def get_collection_stats(self) -> dict[str, Any]:
        """获取采集统计概览。"""
        collect_snapshot = store.collect_records.snapshot()
        dirty_snapshot = store.dirty_vault.snapshot()
        return {
            "collect_records": collect_snapshot,
            "dirty_vault": dirty_snapshot,
            "collection_events": len(
                store.channel.snapshot(topic="rpa.data_collected")),
            "pagination": store.page_cursors.snapshot(),
            "handles": store.handle_list(),
        }

    def check_duplicate_accumulation(self) -> dict[str, Any]:
        """核对重开页面不会重复累计同一行：按指纹统计是否有重复入库。"""
        fingerprints: dict[str, int] = {}
        with store.collect_records._lock:
            entries = list(store.collect_records._entries)
        for entry in entries:
            if not entry.fingerprint:
                continue
            fingerprints[entry.fingerprint] = fingerprints.get(entry.fingerprint, 0) + 1
        duplicated = {
            fingerprint: count
            for fingerprint, count in fingerprints.items() if count > 1
        }
        return {
            "records": len(entries),
            "distinct_rows": len(fingerprints),
            "duplicated_rows": len(duplicated),
            "duplicate_accumulation": bool(duplicated),
            "detail": duplicated,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        stats = self.get_collection_stats()
        return {**base, **stats}
