"""分页续做测试：重开页面不会重复累计同一行，边界只扩展不回退。

覆盖：
1. 内容指纹 + 分页游标一起持久化（落盘再恢复边界不丢）
2. 重开页面续做只扩展未覆盖页，已完成边界不回退
3. 处理项重复/并发提交只算一次
4. 页码不变但内容更新：内容优先追加版本；页码优先挂起整页重抓
5. 边界冲突落到页面源配置的处理终态
6. 中断原因可说明，重试沿既有边界
7. 增量字段映射明细落库可查
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest

from app.services.data_extract import DataExtractService
from app.services.page_model import (
    CONFLICT_TERMINAL_STATUS,
    PageModelService,
)
from app.store import PageCursorStore, content_fingerprint, store


@pytest.fixture(autouse=True)
def _clean_runtime_state():
    """每个用例从干净的通道/游标/采集记录开始。"""
    store.reset_transient()
    for entry in store.rows("page_model"):
        entry.pop("conflict_reason", None)
        entry.pop("conflict_at", None)
    yield
    store.reset_transient()


def _model(model_id: int, **overrides):
    entry = store.find("page_model", model_id)
    assert entry is not None
    entry.update({
        "分页模式": "页码分页",
        "每页条数": "5",
        "总页数": "3",
        "边界策略": "content_first",
        "修订号": "1",
        "status": "已验证",
    })
    entry.update(overrides)
    return entry


def _records_for(model_id: int) -> int:
    entry = store.find("page_model", model_id)
    site = entry["目标站点"]
    return len([
        record for record in store.collect_records.list_by_site(site)
        if record.revision_no == 1
    ])


def test_content_fingerprint_is_stable():
    row = {"标题": "报告A", "金额": "12.5"}
    assert content_fingerprint(row) == content_fingerprint(dict(row))
    assert content_fingerprint(row) != content_fingerprint({**row, "金额": "13.0"})


def test_cursor_and_fingerprints_persist_together():
    cursor_store = PageCursorStore()
    cursor = cursor_store.acquire("example.com", 9, page_size=3)
    cursor_store.commit(cursor, 1, [{"序号": 1}, {"序号": 2}], batch_ref="B-1")
    cursor_store.commit(cursor, 2, [{"序号": 3}], batch_ref="B-1")

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "cursors.json")
        cursor_store.save(path)
        assert os.path.exists(path)

        restored = PageCursorStore()
        assert restored.load(path) == 1
        view = restored.resume_snapshot(restored.get("example.com", 9))
        assert view["covered_pages"] == [1, 2]
        assert view["next_page"] == 3
        assert view["seen_count"] == 3
        # 续做时只能扩展未覆盖页
        assert restored.pending_pages(restored.get("example.com", 9), 4) == [3, 4]

        # 再次提交已完成页的行：不新增，也不回退边界
        again = restored.commit(restored.get("example.com", 9), 1, [{"序号": 1}])
        assert again["accepted"] == []
        assert restored.get("example.com", 9)["next_page"] == 3


def test_reopening_pages_does_not_duplicate_rows():
    _model(1)
    page_svc = PageModelService()
    extract_svc = DataExtractService()

    first = page_svc.resume_crawl(1, max_pages=1, handle_ref="H-1")
    assert first["extract"]["pages_covered"] == [1]
    after_first = _records_for(1)
    assert after_first > 0

    # 重开页面（再次续做）只推进到下一页，不重抓已完成页
    second = page_svc.resume_crawl(1, max_pages=1, handle_ref="H-2")
    assert second["extract"]["pages_covered"] == [2]
    after_second = _records_for(1)
    assert after_second == after_first + int(store.find("page_model", 1).get("每页条数"))

    # 重开页面只回看到已完成的第 2 页：没有新增记录
    snapshot = page_svc.build_source_snapshot(1, observed_last_page=2)
    store.channel.publish("rpa.source_configured", "page_model", snapshot)
    replay = extract_svc.consume_source_and_extract(
        handle_ref="H-3", observed_last_page=2, max_pages=1)
    assert replay["extracted"] == 0
    assert replay["pages_covered"] == []
    assert _records_for(1) == after_second

    check = extract_svc.check_duplicate_accumulation()
    assert check["duplicate_accumulation"] is False
    assert check["duplicated_rows"] == 0


def test_completed_boundary_never_rolls_back():
    _model(1)
    page_svc = PageModelService()
    extract_svc = DataExtractService()

    page_svc.resume_crawl(1, max_pages=2, handle_ref="R-1")
    cursor = store.page_cursors.get(store.find("page_model", 1)["目标站点"], 1)
    assert cursor["covered_pages"] == [1, 2]
    assert cursor["next_page"] == 3

    # 页面回退到第 1 页重开：快照里的边界不能把已完成页拉回来
    snapshot = page_svc.build_source_snapshot(1, observed_last_page=1)
    assert snapshot["pagination"]["remaining_pages"] == []
    store.channel.publish("rpa.source_configured", "page_model", snapshot)
    replay = extract_svc.consume_source_and_extract(handle_ref="R-2")
    assert replay["pages_covered"] == []
    assert cursor["covered_pages"] == [1, 2]
    assert cursor["next_page"] == 3


def test_duplicate_handle_only_counts_once():
    _model(1)
    page_svc = PageModelService()
    page_svc.publish_source_config(1)
    extract_svc = DataExtractService()

    first = extract_svc.consume_source_and_extract(handle_ref="DUP-1", max_pages=1)
    baseline = _records_for(1)
    assert first["extracted"] > 0

    page_svc.publish_source_config(1)
    second = extract_svc.consume_source_and_extract(handle_ref="DUP-1", max_pages=1)
    assert second["skipped"] == "duplicate_handle"
    assert second["extracted"] == 0
    assert _records_for(1) == baseline


def test_concurrent_handle_submissions_count_once():
    _model(1)
    page_svc = PageModelService()
    page_svc.publish_source_config(1)
    extract_svc = DataExtractService()

    results: list[dict] = []
    lock = threading.Lock()

    def submit() -> None:
        outcome = extract_svc.consume_source_and_extract(
            handle_ref="CONCURRENT-1", max_pages=1)
        with lock:
            results.append(outcome)

    threads = [threading.Thread(target=submit) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    winners = [r for r in results if r["skipped"] is None]
    losers = [r for r in results if r["skipped"] == "duplicate_handle"]
    assert len(winners) == 1
    assert len(losers) == 1
    assert sum(r["extracted"] for r in results) > 0
    # 并发两次提交只算一次：记录数等于一个处理项的产出
    assert _records_for(1) == winners[0]["extracted"]


def test_content_change_same_page_appends_version():
    _model(1)
    page_svc = PageModelService()
    page_svc.resume_crawl(1, max_pages=1, handle_ref="V-1")

    cursor = store.page_cursors.get(store.find("page_model", 1)["目标站点"], 1)
    assert cursor["page_versions"][1][0].version == 1

    # 同页内容变化、页码未推进：内容优先边界按指纹追加新版本
    overridden = [
        {"序号": index, "标题": f"新增行-{index}"} for index in range(1, 4)
    ]
    store.page_cursors.set_page_content(cursor, 1, overridden)
    result = page_svc.resume_crawl(
        1, max_pages=1, handle_ref="V-2", recheck_pages=[1])

    assert result["extract"]["extracted"] > 0
    assert 1 in cursor["covered_pages"]  # 边界没有回退
    assert cursor["next_page"] >= 2
    versions = cursor["page_versions"][1]
    assert len(versions) >= 2
    assert "第 1 页" not in str(cursor.get("deferred_pages", {}))


def test_page_first_defers_until_page_advances():
    _model(3, 边界策略="page_first", 每页条数="2")
    page_svc = PageModelService()
    page_svc.resume_crawl(3, max_pages=1, handle_ref="P-1")

    cursor = store.page_cursors.get(store.find("page_model", 3)["目标站点"], 3)
    baseline = _records_for(3)
    store.page_cursors.set_page_content(
        cursor, 1, [{"序号": 1, "标题": "更新行A"}, {"序号": 2, "标题": "更新行B"},
                    {"序号": 3, "标题": "新增行C"}])

    deferred = page_svc.resume_crawl(
        3, max_pages=1, handle_ref="P-2", recheck_pages=[1])
    assert 1 in cursor["deferred_pages"]
    # 挂起页没有输出任何行：基线记录里不含新内容
    assert _records_for(3) >= baseline
    page_one_rows = [
        record for record in store.collect_records.list_by_site(
            store.find("page_model", 3)["目标站点"])
        if record.page_no == 1
    ]
    assert all(r.fields.get("标题") != "新增行C" for r in page_one_rows)

    # 页码推进后再整页重抓：原来已见的行仍然不会重复输出
    store.page_cursors.clear_page_content(cursor, 1)
    advanced = page_svc.resume_crawl(3, max_pages=1, handle_ref="P-3")
    assert advanced["extract"]["pages_covered"]
    assert cursor["deferred_pages"] == {}
    assert store.find("page_model", 3)["status"] != CONFLICT_TERMINAL_STATUS
    assert DataExtractService().check_duplicate_accumulation()[
        "duplicate_accumulation"] is False


def test_repeated_content_conflict_hits_terminal_status():
    _model(3, 边界策略="page_first", 每页条数="2")
    page_svc = PageModelService()
    page_svc.resume_crawl(3, max_pages=1, handle_ref="X-1")

    cursor = store.page_cursors.get(store.find("page_model", 3)["目标站点"], 3)
    store.page_cursors.set_page_content(
        cursor, 1, [{"序号": 1, "标题": "更新A"}, {"序号": 2, "标题": "更新B"},
                    {"序号": 3, "标题": "新增C"}])
    page_svc.resume_crawl(3, max_pages=1, handle_ref="X-2", recheck_pages=[1])

    store.page_cursors.set_page_content(
        cursor, 1, [{"序号": 1, "标题": "更新A"}, {"序号": 2, "标题": "更新B"},
                    {"序号": 3, "标题": "新增C"}, {"序号": 4, "标题": "新增D"}])
    page_svc.resume_crawl(3, max_pages=1, handle_ref="X-3", recheck_pages=[1])

    entry = store.find("page_model", 3)
    assert entry["status"] == CONFLICT_TERMINAL_STATUS
    assert entry["conflict_reason"]
    assert "分页边界冲突" in entry["conflict_reason"]

    # 冲突终态下不再输出同一行
    check = DataExtractService().check_duplicate_accumulation()
    assert check["duplicate_accumulation"] is False


def test_stale_snapshot_is_rejected_with_reason():
    entry = _model(1)
    page_svc = PageModelService()
    stale = page_svc.build_source_snapshot(1)
    entry["每页条数"] = "7"  # 页面配置变了，旧快照失效
    store.channel.publish("rpa.source_configured", "page_model", stale)

    outcome = DataExtractService().consume_source_and_extract(handle_ref="STALE-1")
    assert outcome["extracted"] == 0
    assert outcome["handle_status"] == "已中断"
    assert "快照已过期" in (outcome["interrupt_reason"] or "")


def test_interrupted_retry_keeps_existing_boundary():
    _model(1)
    page_svc = PageModelService()
    extract_svc = DataExtractService()
    page_svc.resume_crawl(1, max_pages=1, handle_ref="I-1")

    cursor = store.page_cursors.get(store.find("page_model", 1)["目标站点"], 1)
    store.page_cursors.mark_interrupted(cursor, "翻页按钮定位超时", page_no=2)

    overview = page_svc.resume_overview(model_id=1)
    model_view = overview["models"][0]
    assert model_view["interrupted"]["reason"] == "翻页按钮定位超时"
    assert model_view["covered_pages"] == [1]

    # 重试沿既有边界继续：只做下一页
    retried = page_svc.resume_crawl(1, max_pages=1, handle_ref="I-2")
    assert retried["extract"]["pages_covered"] == [2]
    check = extract_svc.check_duplicate_accumulation()
    assert check["duplicate_accumulation"] is False


def test_empty_state_has_no_failures():
    _model(1)
    page_svc = PageModelService()
    page_svc.resume_crawl(1, max_pages=5, handle_ref="E-1")
    again = page_svc.resume_crawl(1, max_pages=5, handle_ref="E-2")
    assert again["extract"] is None
    assert again["message"] == "没有未覆盖页，无需续做"
    assert page_svc.field_mapping_detail(model_id=1)["count"] == 0


def test_field_mapping_journal_is_saved():
    _model(1)
    page_svc = PageModelService()
    saved = page_svc.save_field_mapping(1, [
        {"source_field": "标题", "target_field": "title"},
        {"source_field": "金额", "target_field": "amount", "data_type": "number"},
        {"source_field": "", "target_field": "ignored"},
    ])
    assert saved["saved"] == 2

    detail = page_svc.field_mapping_detail(model_id=1)
    assert detail["count"] == 1
    assert detail["fields"] == 2
    entry = detail["entries"][0]
    assert entry["revision"] == 1
    assert entry["mappings"][1]["target_field"] == "amount"


def test_source_snapshot_carries_pagination_version():
    _model(1)
    page_svc = PageModelService()
    snapshot = page_svc.build_source_snapshot(1)
    assert snapshot["pagination_version"] == snapshot["pagination"]["pagination_version"]
    assert snapshot["snapshot_version"] >= 1
    assert snapshot["pagination"]["boundary_policy"] == "content_first"
    assert snapshot["pagination"]["page_size"] == 5

    published = page_svc.publish_source_config(1)
    assert published["pagination_version"] == snapshot["pagination_version"]
    assert published["remaining_pages"] == [1, 2, 3]
