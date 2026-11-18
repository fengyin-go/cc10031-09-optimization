"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

跨模块数据流能力：
- DataChannel: 服务间数据发布/消费通道，支持 topic 订阅、快照读、消费确认
- DirtyDataVault: 脏数据隔离区，存储校验不通过/解析失败的脏数据记录
- FailureRecovery: 失败恢复管理器，记录失败步骤并支持重试
- CollectRecord: 采集记录存储，记录每次采集产出的结构化数据
"""
from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.seed import SEED_ROWS

# ---------------------------------------------------------------------------
# 跨模块数据通道 (DataChannel) — 服务间发布/消费
# ---------------------------------------------------------------------------

@dataclass
class ChannelMessage:
    """数据通道中的单条消息，带 topic、来源、负载和确认状态。"""
    msg_id: int
    topic: str
    source_module: str
    payload: dict[str, Any]
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    consumed_by: list[str] = field(default_factory=list)
    confirmed: bool = False

    def mark_consumed(self, consumer: str) -> None:
        if consumer not in self.consumed_by:
            self.consumed_by.append(consumer)


class DataChannel:
    """跨模块数据通道：支持 topic 发布/订阅，服务间数据流转的桥梁。

    用法::

        store.channel.publish("rpa.bot_created", "bot_definition", bot_data)
        msgs = store.channel.consume("step_editor", "rpa.bot_created")
        store.channel.confirm(msg_id, "step_editor")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._messages: list[ChannelMessage] = []

    @property
    def total_messages(self) -> int:
        with self._lock:
            return len(self._messages)

    def publish(self, topic: str, source: str, payload: dict[str, Any]) -> ChannelMessage:
        with self._lock:
            self._seq += 1
            msg = ChannelMessage(
                msg_id=self._seq, topic=topic,
                source_module=source, payload=payload,
            )
            self._messages.append(msg)
            return msg

    def consume(self, consumer: str, topic: str | None = None,
                since_msg_id: int | None = None) -> list[ChannelMessage]:
        """获取指定 consumer 尚未消费的消息。"""
        with self._lock:
            results: list[ChannelMessage] = []
            for msg in self._messages:
                if topic and msg.topic != topic:
                    continue
                if since_msg_id is not None and msg.msg_id <= since_msg_id:
                    continue
                if consumer not in msg.consumed_by:
                    results.append(msg)
                    msg.mark_consumed(consumer)
            return results

    def confirm(self, msg_id: int, consumer: str) -> bool:
        with self._lock:
            for msg in self._messages:
                if msg.msg_id == msg_id:
                    msg.confirmed = True
                    return True
            return False

    def snapshot(self, topic: str | None = None) -> list[dict[str, Any]]:
        """返回消息快照，供跨模块分析/看板消费。"""
        with self._lock:
            msgs = self._messages if topic is None else [
                m for m in self._messages if m.topic == topic
            ]
            return [{
                "msg_id": m.msg_id, "topic": m.topic,
                "source": m.source_module, "payload": m.payload,
                "created_at": m.created_at, "consumed_by": m.consumed_by,
                "confirmed": m.confirmed,
            } for m in msgs]

    def latest(self, topic: str) -> dict[str, Any] | None:
        with self._lock:
            for msg in reversed(self._messages):
                if msg.topic == topic:
                    return {
                        "msg_id": msg.msg_id, "topic": msg.topic,
                        "source": msg.source_module, "payload": msg.payload,
                        "created_at": msg.created_at, "confirmed": msg.confirmed,
                    }
            return None

    def uncomfirmed_count(self) -> int:
        with self._lock:
            return sum(1 for m in self._messages if not m.confirmed)


# ---------------------------------------------------------------------------
# 脏数据隔离区 (DirtyDataVault)
# ---------------------------------------------------------------------------

@dataclass
class DirtyRecord:
    """脏数据记录：校验失败或解析异常的数据行。"""
    record_id: int
    source_module: str
    batch_ref: str
    raw_data: dict[str, Any]
    reason: str  # 如 "字段缺失", "格式非法", "解析超时"
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    quarantined: bool = True
    recovered_to: str | None = None


class DirtyDataVault:
    """脏数据隔离区：被校验规则或解析器标记为问题数据后移入此处。

    用法::

        vault = store.dirty_vault
        vault.quarantine("data_extract", "BATC-001", row, "缺少必填字段")
        dirty = vault.list_by_source("data_extract")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._records: list[DirtyRecord] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._records)

    def quarantine(self, source: str, batch_ref: str,
                   raw_data: dict[str, Any], reason: str) -> DirtyRecord:
        with self._lock:
            self._seq += 1
            rec = DirtyRecord(
                record_id=self._seq, source_module=source,
                batch_ref=batch_ref, raw_data=raw_data, reason=reason,
            )
            self._records.append(rec)
            return rec

    def list_by_source(self, source: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.source_module == source]

    def list_by_batch(self, batch_ref: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.batch_ref == batch_ref]

    def mark_recovered(self, record_id: int, recovered_to: str) -> bool:
        with self._lock:
            for r in self._records:
                if r.record_id == record_id:
                    r.quarantined = False
                    r.recovered_to = recovered_to
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            quarantined = sum(1 for r in self._records if r.quarantined)
            by_reason: dict[str, int] = {}
            for r in self._records:
                by_reason[r.reason] = by_reason.get(r.reason, 0) + 1
            return {
                "total": len(self._records),
                "quarantined": quarantined,
                "recovered": len(self._records) - quarantined,
                "by_reason": by_reason,
            }


# ---------------------------------------------------------------------------
# 采集记录 (CollectRecord) — 每次采集产出的结构化数据
# ---------------------------------------------------------------------------

@dataclass
class CollectEntry:
    entry_id: int
    batch_ref: str
    source_module: str
    fields: dict[str, Any]
    collected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    validated: bool = False
    quality_score: float = 1.0
    fingerprint: str = ""
    page_no: int = 0
    config_version: str = ""
    revision_no: int = 0


class CollectRecordStore:
    """采集记录存储：每次采集流程产出的结构化数据行。

    用法::

        rec = store.collect_records.add("BATC-001", "data_extract", {"字段1": "值1", ...})
        store.collect_records.mark_validated(rec.entry_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[CollectEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def add(self, batch_ref: str, source: str,
            fields: dict[str, Any], *, fingerprint: str = "",
            page_no: int = 0, config_version: str = "",
            revision_no: int = 0) -> CollectEntry:
        with self._lock:
            self._seq += 1
            entry = CollectEntry(
                entry_id=self._seq, batch_ref=batch_ref,
                source_module=source, fields=dict(fields),
                fingerprint=fingerprint, page_no=page_no,
                config_version=config_version, revision_no=revision_no,
            )
            self._entries.append(entry)
            return entry

    def list_by_batch(self, batch_ref: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.batch_ref == batch_ref]

    def list_by_source(self, source: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.source_module == source]

    def list_by_revision(self, revision_no: int) -> list[CollectEntry]:
        """读取某个修订范围内的已采集记录（续做核对用）。"""
        with self._lock:
            return [e for e in self._entries if e.revision_no == revision_no]

    def list_by_site(self, site: str) -> list[CollectEntry]:
        with self._lock:
            return [
                e for e in self._entries
                if e.fields.get("_site") == site
            ]

    def fingerprints(self, source: str | None = None) -> set[str]:
        with self._lock:
            return {
                e.fingerprint for e in self._entries
                if e.fingerprint and (source is None or e.source_module == source)
            }

    def mark_validated(self, entry_id: int, score: float = 1.0) -> bool:
        with self._lock:
            for e in self._entries:
                if e.entry_id == entry_id:
                    e.validated = True
                    e.quality_score = score
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            validated = sum(1 for e in self._entries if e.validated)
            by_source: dict[str, int] = {}
            for e in self._entries:
                by_source[e.source_module] = by_source.get(e.source_module, 0) + 1
            return {
                "total": len(self._entries),
                "validated": validated,
                "unvalidated": len(self._entries) - validated,
                "by_source": by_source,
            }


# ---------------------------------------------------------------------------
# 失败恢复管理 (FailureRecovery)
# ---------------------------------------------------------------------------

@dataclass
class RecoveryEntry:
    recovery_id: int
    module: str
    batch_ref: str
    step: str
    error_detail: str
    attempt: int = 0
    max_attempts: int = 3
    resolved: bool = False
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class FailureRecovery:
    """失败恢复管理器：记录失败步骤并支持自动重试。

    用法::

        rec = store.recovery.log("step_editor", "BATC-001", "元素定位",
                                  "选择器超时未匹配")
        store.recovery.retry(rec.recovery_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[RecoveryEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def log(self, module: str, batch_ref: str, step: str,
            error_detail: str, max_attempts: int = 3) -> RecoveryEntry:
        with self._lock:
            self._seq += 1
            entry = RecoveryEntry(
                recovery_id=self._seq, module=module,
                batch_ref=batch_ref, step=step,
                error_detail=error_detail, max_attempts=max_attempts,
            )
            self._entries.append(entry)
            return entry

    def retry(self, recovery_id: int) -> tuple[bool, str]:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    if e.resolved:
                        return False, "已恢复，无需重试"
                    e.attempt += 1
                    if e.attempt > e.max_attempts:
                        return False, f"已达最大重试次数 {e.max_attempts}"
                    return True, f"重试 {e.attempt}/{e.max_attempts}"
            return False, "恢复记录不存在"

    def resolve(self, recovery_id: int) -> bool:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    e.resolved = True
                    return True
            return False

    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for e in self._entries if not e.resolved)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            pending = sum(1 for e in self._entries if not e.resolved)
            return {
                "total": len(self._entries),
                "resolved": len(self._entries) - pending,
                "pending": pending,
            }


# ---------------------------------------------------------------------------
# 分页游标 (PaginationCursor) — 已见内容指纹 + 页码边界，续做时一起持久化
# ---------------------------------------------------------------------------

def content_fingerprint(payload: Any) -> str:
    """把一行页面内容压成稳定指纹：同内容同指纹，重开页面即可判重。"""
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


@dataclass
class SeenRow:
    """已见页面内容：按内容指纹记录它首次出现在哪一页、第几行。"""
    fingerprint: str
    page_no: int
    row_index: int
    first_seen_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    times_seen: int = 1
    content: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "fingerprint": self.fingerprint,
            "page_no": self.page_no,
            "row_index": self.row_index,
            "first_seen_at": self.first_seen_at,
            "times_seen": self.times_seen,
        }


@dataclass
class PageVersion:
    """同一页码下的内容版本：页面内容变了但页码没推进时按指纹追加。"""
    version: int
    page_no: int
    fingerprint: str
    row_count: int
    captured_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "page_no": self.page_no,
            "fingerprint": self.fingerprint,
            "row_count": self.row_count,
            "captured_at": self.captured_at,
        }


class PageCursorStore:
    """分页采集游标仓库：按站点 + 页面模型持久化已覆盖页码和已见内容指纹。

    续做规则（两套边界互斥，都保证同一行只输出一次）：
    - 页码边界：只允许向后推进 ``covered_pages``，不允许回退已完成边界；
    - 内容边界：同页重抓只补指纹未覆盖的行，内容变了追加 ``page_versions``。

    用法::

        cursor = store.page_cursors.acquire("example.com", 1, page_size=10)
        plan = cursor.pending_pages(observed_last_page=3)
        cursor.commit(page_no=1, rows=[...], batch_ref="BATC-1")
    """

    #: 快照版本号：页面配置变化时递增，下游据版本判断配置是否过期
    SNAPSHOT_VERSION = 1

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._cursors: dict[str, dict[str, Any]] = {}

    @staticmethod
    def key(site: str, model_id: int | None) -> str:
        return f"{site or 'unknown'}::{int(model_id or 0)}"

    # ---- 生命周期 ----

    def acquire(self, site: str, model_id: int | None = None,
                page_size: int = 10, pagination_mode: str = "auto",
                boundary_policy: str = "page_then_content") -> dict[str, Any]:
        """取出或新建游标。已有游标时只刷新配置，不回退任何已覆盖边界。"""
        key = self.key(site, model_id)
        with self._lock:
            cursor = self._cursors.get(key)
            if cursor is None:
                cursor = {
                    "cursor_key": key,
                    "site": site or "unknown",
                    "model_id": int(model_id or 0),
                    "page_size": max(int(page_size or 10), 1),
                    "pagination_mode": pagination_mode or "auto",
                    "boundary_policy": boundary_policy,
                    "status": "待续做",
                    "covered_pages": [],      # 已完成页码，单调递增
                    "next_page": 1,           # 下一页起点，>= 已完成边界 + 1
                    "seen": {},               # fingerprint -> SeenRow
                    "page_versions": {},      # page_no -> [PageVersion]
                    "batch_refs": [],         # 本游标产出过的批次
                    "deferred_pages": {},     # page_no -> 延后原因（页码优先边界）
                    "defer_history": {},      # page_no -> 累计延后次数
                    "claimed_pages": {},      # page_no -> 抢占时间（并发处理）
                    "content_overrides": {},  # page_no -> 页面内容更新后的行（模拟站点改版）
                    "interrupted": None,      # 最近一次中断原因，续做后清除
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
                self._cursors[key] = cursor
            else:
                # 续做：配置可刷新，边界只可扩展
                cursor["page_size"] = max(int(page_size or cursor["page_size"]), 1)
                cursor["pagination_mode"] = pagination_mode or cursor["pagination_mode"]
                cursor["boundary_policy"] = boundary_policy or cursor["boundary_policy"]
                cursor["updated_at"] = datetime.now(timezone.utc).isoformat()
            return cursor

    def get(self, site: str, model_id: int | None = None) -> dict[str, Any] | None:
        with self._lock:
            return self._cursors.get(self.key(site, model_id))

    def all_cursors(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._cursors.values())

    def reset(self) -> None:
        """清空游标（测试或换站点重开时使用，正常续做不要调用）。"""
        with self._lock:
            self._cursors.clear()

    # ---- 边界与计划 ----

    def pending_pages(self, cursor: dict[str, Any],
                      observed_last_page: int | None = None,
                      max_pages: int | None = None) -> list[int]:
        """解析剩余页边界：从 next_page 起算，绝不低于已完成边界。

        Args:
            cursor: acquire 返回的游标
            observed_last_page: 页面当前可见的最后一页（None 表示只做单页续做）
            max_pages: 本次最多带上的页数，防止一次拉爆

        Returns:
            待覆盖页码升序列表
        """
        with self._lock:
            start = max(int(cursor.get("next_page", 1)), 1)
            if observed_last_page is None:
                pages = [start]
            else:
                last = int(observed_last_page)
                # 已完成边界之后的页才算未覆盖；边界外（下一页 > 最后一页）不再产出
                pages = [] if last < start else list(range(start, last + 1))
            if max_pages is not None:
                pages = pages[:max(int(max_pages), 0)]
            return pages

    def seen_count(self, cursor: dict[str, Any]) -> int:
        return len(cursor.get("seen", {}))

    # ---- 提交与判重 ----

    def preview(self, cursor: dict[str, Any], page_no: int,
                rows: list[dict[str, Any]]) -> dict[str, Any]:
        """只读预判：这一页有没有新内容、是否落在已完成边界内。"""
        with self._lock:
            seen = cursor.get("seen", {})
            fresh = 0
            for row in rows:
                if content_fingerprint(dict(row)) not in seen:
                    fresh += 1
            return {
                "page_no": int(page_no),
                "row_count": len(rows),
                "new_rows": fresh,
                "duplicates": len(rows) - fresh,
                "covered_before": int(page_no) in cursor.get("covered_pages", []),
            }

    def admit_page(self, cursor: dict[str, Any], page_no: int) -> bool:
        """标记一页已完成（幂等）：并发提交同一页时只有第一次推进边界。"""
        with self._lock:
            covered = cursor.setdefault("covered_pages", [])
            if int(page_no) in covered:
                return False
            covered.append(int(page_no))
            covered.sort()
            next_page = 1
            for number in covered:
                if number == next_page:
                    next_page += 1
                elif number > next_page:
                    break
            cursor["next_page"] = next_page
            return True

    def claim_page(self, cursor: dict[str, Any], page_no: int) -> bool:
        """抢占一页的处理权，避免两个处理项同时抓同一页。"""
        with self._lock:
            claimed = cursor.setdefault("claimed_pages", {})
            if int(page_no) in claimed:
                return False
            claimed[int(page_no)] = datetime.now(timezone.utc).isoformat()
            return True

    def release_page(self, cursor: dict[str, Any], page_no: int) -> bool:
        with self._lock:
            return cursor.setdefault("claimed_pages", {}).pop(int(page_no), None) is not None

    def commit(self, cursor: dict[str, Any], page_no: int,
               rows: list[dict[str, Any]], batch_ref: str = "",
               ) -> dict[str, Any]:
        """提交一页采集结果：按内容指纹判重，只返回尚未覆盖的行。

        已见指纹直接跳过，所以同一行不会因为重开页面而重复累计；
        已完成边界内的页面只按内容补差，绝不回退 ``next_page``。

        Returns:
            {"page_no", "accepted", "duplicates", "version", "pages_advanced",
             "covered_before", "changed_covered_page"}
        """
        page_no = int(page_no)
        with self._lock:
            covered = cursor.setdefault("covered_pages", [])
            covered_before = page_no in covered

            seen: dict[str, SeenRow] = cursor.setdefault("seen", {})
            accepted: list[dict[str, Any]] = []
            duplicates: list[dict[str, Any]] = []
            page_fingerprints: list[str] = []

            for index, row in enumerate(rows):
                payload = dict(row)
                fingerprint = content_fingerprint(payload)
                page_fingerprints.append(fingerprint)
                existing = seen.get(fingerprint)
                if existing is not None:
                    existing.times_seen += 1
                    duplicates.append({
                        "fingerprint": fingerprint,
                        "page_no": page_no,
                        "row_index": index,
                        "first_page": existing.page_no,
                    })
                    continue
                seen[fingerprint] = SeenRow(
                    fingerprint=fingerprint, page_no=page_no,
                    row_index=index, content=payload,
                )
                accepted.append({**payload, "_fingerprint": fingerprint,
                                 "_page_no": page_no})

            page_fp = content_fingerprint(page_fingerprints)
            versions: dict[int, list[PageVersion]] = cursor.setdefault("page_versions", {})
            version_entry: PageVersion | None = None
            for existing in versions.get(page_no, []):
                if existing.fingerprint == page_fp:
                    version_entry = existing
                    break
            if version_entry is None and page_fingerprints:
                # 同页内容变了：按内容指纹追加新版本，不覆盖旧版本
                history = versions.setdefault(page_no, [])
                version_entry = PageVersion(
                    version=len(history) + 1, page_no=page_no,
                    fingerprint=page_fp, row_count=len(rows),
                )
                history.append(version_entry)

            pages_advanced = 0
            if page_no not in covered:
                covered.append(page_no)
                pages_advanced = 1
            covered.sort()
            # 下一页始终指向最小未覆盖页：既不会回退已完成边界，也不会漏掉被跳过的页
            next_page = 1
            for number in covered:
                if number == next_page:
                    next_page += 1
                elif number > next_page:
                    break
            cursor["next_page"] = next_page
            if batch_ref and batch_ref not in cursor.setdefault("batch_refs", []):
                cursor["batch_refs"].append(batch_ref)
            cursor["status"] = "已续做" if accepted else "无新增"
            cursor["updated_at"] = datetime.now(timezone.utc).isoformat()

            return {
                "page_no": page_no,
                "accepted": accepted,
                "duplicates": duplicates,
                "version": version_entry.version if version_entry else None,
                "pages_advanced": pages_advanced,
                "covered_before": covered_before,
                "changed_covered_page": covered_before and bool(accepted),
            }

    # ---- 页面内容变化（页码不变） ----

    def set_page_content(self, cursor: dict[str, Any], page_no: int,
                         rows: list[dict[str, Any]]) -> None:
        """模拟站点在同一页码下更新了内容，供两套边界分别处理。"""
        with self._lock:
            cursor.setdefault("content_overrides", {})[int(page_no)] = [
                dict(row) for row in rows
            ]

    def page_rows(self, cursor: dict[str, Any], page_no: int,
                  rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """取当前页内容：有内容更新时用更新后的行，否则用抓取到的行。"""
        with self._lock:
            override = cursor.get("content_overrides", {}).get(int(page_no))
            return [dict(row) for row in override] if override else rows

    def clear_page_content(self, cursor: dict[str, Any], page_no: int) -> None:
        with self._lock:
            cursor.setdefault("content_overrides", {}).pop(int(page_no), None)

    # ---- 中断 / 延后 ----

    def mark_interrupted(self, cursor: dict[str, Any], reason: str,
                         page_no: int | None = None) -> dict[str, Any]:
        """记录一次中断：保留原因和停在哪一页，重试时原样说明。"""
        with self._lock:
            cursor["interrupted"] = {
                "reason": reason,
                "page_no": page_no,
                "next_page": cursor.get("next_page", 1),
                "at": datetime.now(timezone.utc).isoformat(),
            }
            cursor["status"] = "已中断"
            return cursor["interrupted"]

    def clear_interrupt(self, cursor: dict[str, Any]) -> dict[str, Any] | None:
        with self._lock:
            previous = cursor.pop("interrupted", None)
            if previous is not None:
                cursor["status"] = "待续做"
            return previous

    def defer_page(self, cursor: dict[str, Any], page_no: int, reason: str) -> dict[str, Any]:
        """页码优先边界：先挂起内容变化的页，等页码推进后整页重抓。

        同一页反复要求延后时，说明两套边界已无法自洽，计数交给调用方落冲突终态。
        """
        with self._lock:
            deferred = cursor.setdefault("deferred_pages", {})
            history = cursor.setdefault("defer_history", {})
            count = int(history.get(int(page_no), 0)) + 1
            history[int(page_no)] = count
            record = {
                "reason": reason,
                "at": datetime.now(timezone.utc).isoformat(),
                "count": count,
                # 记下挂起时的页码边界：之后 next_page 超过它才算「页码推进了」
                "next_page_at_defer": int(cursor.get("next_page", 1)),
            }
            deferred[int(page_no)] = record
            return record

    def release_deferred(self, cursor: dict[str, Any], page_no: int) -> bool:
        with self._lock:
            return cursor.setdefault("deferred_pages", {}).pop(int(page_no), None) is not None

    # ---- 持久化：游标与已见指纹同存一份快照 ----

    def export_state(self) -> dict[str, Any]:
        """导出全部游标（含已见内容指纹），用于落盘/跨进程续做。"""
        with self._lock:
            return {
                "format": "rpa.page-cursors",
                "version": self.SNAPSHOT_VERSION,
                "saved_at": datetime.now(timezone.utc).isoformat(),
                "cursors": [
                    {
                        **cursor,
                        "seen": {
                            fingerprint: seen.to_dict()
                            for fingerprint, seen in cursor.get("seen", {}).items()
                        },
                        "page_versions": {
                            int(page): [v.to_dict() for v in items]
                            for page, items in cursor.get("page_versions", {}).items()
                        },
                    }
                    for cursor in self._cursors.values()
                ],
            }

    def import_state(self, state: dict[str, Any]) -> int:
        """从快照恢复游标；只恢复边界，不清理已有更新游标。"""
        restored = 0
        with self._lock:
            for raw in state.get("cursors", []):
                key = raw.get("cursor_key")
                if not key:
                    continue
                cursor = dict(raw)
                cursor["seen"] = {
                    fingerprint: SeenRow(**item)
                    for fingerprint, item in (raw.get("seen") or {}).items()
                }
                cursor["page_versions"] = {
                    int(page): [PageVersion(**item) for item in items]
                    for page, items in (raw.get("page_versions") or {}).items()
                }
                existing = self._cursors.get(key)
                if existing is not None:
                    # 已加载的游标只允许向前合并，不能回退
                    for seen_fp, seen in cursor["seen"].items():
                        existing["seen"].setdefault(seen_fp, seen)
                    for page, items in cursor["page_versions"].items():
                        bucket = existing["page_versions"].setdefault(page, [])
                        known = {v.fingerprint for v in bucket}
                        bucket.extend(v for v in items if v.fingerprint not in known)
                    for number in cursor.get("covered_pages", []):
                        if number not in existing["covered_pages"]:
                            existing["covered_pages"].append(number)
                    existing["covered_pages"].sort()
                    existing["next_page"] = (
                        max(existing["covered_pages"]) + 1
                        if existing["covered_pages"] else 1)
                else:
                    self._cursors[key] = cursor
                restored += 1
        return restored

    def save(self, path: str) -> str:
        """原子写入磁盘：已见指纹和分页游标在同一个文件里，不会各存一半。"""
        import os
        import tempfile

        state = self.export_state()
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        fd, temp_path = tempfile.mkstemp(dir=directory, suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(state, handle, ensure_ascii=False, indent=2, default=str)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, path)
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
        return path

    def load(self, path: str) -> int:
        import os

        if not os.path.exists(path):
            return 0
        with open(path, "r", encoding="utf-8") as handle:
            state = json.load(handle)
        return self.import_state(state)

    def resume_snapshot(self, cursor: dict[str, Any]) -> dict[str, Any]:
        """续做视图：已覆盖页、已见内容、剩余页边界一次性给前端。"""
        with self._lock:
            covered = sorted(int(p) for p in cursor.get("covered_pages", []))
            return {
                "cursor_key": cursor.get("cursor_key"),
                "site": cursor.get("site"),
                "model_id": cursor.get("model_id"),
                "status": cursor.get("status"),
                "covered_pages": covered,
                "next_page": cursor.get("next_page", 1),
                "seen_count": len(cursor.get("seen", {})),
                "page_size": cursor.get("page_size"),
                "pagination_mode": cursor.get("pagination_mode"),
                "boundary_policy": cursor.get("boundary_policy"),
                "versions": {
                    int(page): [v.to_dict() for v in items]
                    for page, items in cursor.get("page_versions", {}).items()
                },
                "batch_refs": list(cursor.get("batch_refs", [])),
                "updated_at": cursor.get("updated_at"),
                "snapshot_version": self.SNAPSHOT_VERSION,
            }

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            cursors = list(self._cursors.values())
            return {
                "total_cursors": len(cursors),
                "sites": sorted({c.get("site", "") for c in cursors}),
                "covered_pages": sum(len(c.get("covered_pages", [])) for c in cursors),
                "seen_rows": sum(len(c.get("seen", {})) for c in cursors),
            }


# ---------------------------------------------------------------------------
# 增强版 Store — 整合通道、脏数据、采集记录、恢复
# ---------------------------------------------------------------------------

class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块基础设施
        self.channel = DataChannel()
        self.dirty_vault = DirtyDataVault()
        self.collect_records = CollectRecordStore()
        self.recovery = FailureRecovery()
        # 分页续做基础设施：已见内容指纹 + 分页游标一起持久化
        self.page_cursors = PageCursorStore()
        # 处理项台账：同一处理项重复/并发提交只算一次
        self.handled_items: dict[str, dict[str, Any]] = {}
        self._handle_lock = threading.Lock()
        # 增量字段映射明细：每次续做落一份可核对的映射快照
        self.mapping_journal: list[dict[str, Any]] = []
        self._mapping_seq = 0
        self._journal_lock = threading.Lock()

    # ---- 处理项台账 ----

    def begin_handle(self, handle_ref: str) -> tuple[bool, dict[str, Any] | None]:
        """登记一个处理项：已完成或进行中的重复提交直接拒绝。

        Returns:
            (是否可以继续处理, 台账里的旧记录)
        """
        with self._handle_lock:
            previous = self.handled_items.get(handle_ref)
            if previous is not None and previous.get("status") in ("已完成", "进行中"):
                return False, dict(previous)
            record = {
                "handle_ref": handle_ref,
                "status": "进行中",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "retry_of": previous.get("status") if previous else None,
                "previous_interrupt": (previous or {}).get("interrupt_reason"),
            }
            self.handled_items[handle_ref] = record
            return True, previous

    def finish_handle(self, handle_ref: str, summary: dict[str, Any]) -> None:
        with self._handle_lock:
            record = self.handled_items.setdefault(handle_ref, {"handle_ref": handle_ref})
            record.update(summary)
            record["status"] = "已完成"
            record.pop("interrupt_reason", None)
            record["finished_at"] = datetime.now(timezone.utc).isoformat()

    def fail_handle(self, handle_ref: str, reason: str) -> None:
        with self._handle_lock:
            record = self.handled_items.setdefault(handle_ref, {"handle_ref": handle_ref})
            record["status"] = "已中断"
            record["interrupt_reason"] = reason
            record["interrupted_at"] = datetime.now(timezone.utc).isoformat()

    def handle_record(self, handle_ref: str) -> dict[str, Any] | None:
        with self._handle_lock:
            record = self.handled_items.get(handle_ref)
            return dict(record) if record else None

    def handle_list(self) -> list[dict[str, Any]]:
        with self._handle_lock:
            return [dict(record) for record in self.handled_items.values()]

    def reset_transient(self) -> None:
        """清空跨模块运行态（通道、游标、台账、采集记录），测试或换站点重开时用。"""
        with self.channel._lock:
            self.channel._messages.clear()
            self.channel._seq = 0
        self.page_cursors.reset()
        with self._handle_lock:
            self.handled_items.clear()
        with self.collect_records._lock:
            self.collect_records._entries.clear()
            self.collect_records._seq = 0
        with self.dirty_vault._lock:
            self.dirty_vault._records.clear()
            self.dirty_vault._seq = 0
        with self._journal_lock:
            self.mapping_journal.clear()
            self._mapping_seq = 0

    def record_mapping(self, payload: dict[str, Any]) -> dict[str, Any]:
        """登记一份增量字段映射明细，供工作台核对/导出。"""
        with self._journal_lock:
            self._mapping_seq += 1
            entry = {"id": self._mapping_seq, **payload}
            self.mapping_journal.append(entry)
            return entry

    def mapping_snapshot(self) -> dict[str, Any]:
        with self._journal_lock:
            return {
                "total": len(self.mapping_journal),
                "fields": sum(len(e.get("mappings", [])) for e in self.mapping_journal),
                "entries": list(self.mapping_journal),
            }

    # ---- 基础数据操作 ----

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()