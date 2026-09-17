"""预约改期接口测试：乐观锁版本、排除自身的冲突检测、原子替换、并发与迁移。"""

from __future__ import annotations

import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app import db
from app import reservations as core
from app.main import app


@pytest.fixture()
def db_path(tmp_path, monkeypatch) -> str:
    """每个测试用独立的 SQLite 文件。"""
    path = str(tmp_path / "reservations.db")
    monkeypatch.setenv("RESERVATIONS_DB_PATH", path)
    return path


@pytest.fixture()
def client(db_path) -> TestClient:
    return TestClient(app)


def _create_payload(**overrides):
    payload = {
        "request_id": "req-1",
        "start_utc": "2026-03-09T13:00:00Z",
        "end_utc": "2026-03-09T14:00:00Z",
        "participant_ids": ["alice", "bob"],
    }
    payload.update(overrides)
    return payload


def _put_payload(**overrides):
    payload = {
        "expected_version": 1,
        "start_utc": "2026-03-10T15:00:00Z",
        "end_utc": "2026-03-10T16:00:00Z",
        "participant_ids": ["alice", "carol"],
    }
    payload.update(overrides)
    return payload


def _utc(y, mo, d, h, mi=0):
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


def _create(client: TestClient, **overrides) -> dict:
    resp = client.post("/api/v1/reservations", json=_create_payload(**overrides))
    assert resp.status_code == 201, resp.text
    return resp.json()


# ------------------------------------------------------------------ 成功改期

def test_create_response_carries_version_1(client):
    data = _create(client)
    assert data["version"] == 1


def test_reschedule_replaces_reservation_and_bumps_version(client):
    created = _create(client)
    rid = created["reservation_id"]

    resp = client.put(f"/api/v1/reservations/{rid}", json=_put_payload(
        participant_ids=["carol", "alice", "carol"],  # 去重 + 字典序
    ))
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["reservation_id"] == rid
    assert data["request_id"] == created["request_id"]      # 不变
    assert data["created_at"] == created["created_at"]      # 不变
    assert data["start_utc"] == "2026-03-10T15:00:00Z"
    assert data["end_utc"] == "2026-03-10T16:00:00Z"
    assert data["participants"] == ["alice", "carol"]
    assert data["version"] == 2

    # 再次改期需要携带新版本号
    resp = client.put(f"/api/v1/reservations/{rid}", json=_put_payload(
        expected_version=2,
        start_utc="2026-03-11T09:00:00Z",
        end_utc="2026-03-11T10:00:00Z",
        participant_ids=["bob"],
    ))
    assert resp.status_code == 200, resp.text
    assert resp.json()["version"] == 3
    assert resp.json()["participants"] == ["bob"]


def test_reschedule_normalizes_equivalent_timezone_offsets(client):
    created = _create(client)
    rid = created["reservation_id"]
    # +09:00 表示的同一物理时刻，规范化后等价于 UTC 文本
    resp = client.put(f"/api/v1/reservations/{rid}", json=_put_payload(
        start_utc="2026-03-11T00:00:00+09:00",
        end_utc="2026-03-11T01:00:00+09:00",
    ))
    assert resp.status_code == 200, resp.text
    assert resp.json()["start_utc"] == "2026-03-10T15:00:00Z"
    assert resp.json()["end_utc"] == "2026-03-10T16:00:00Z"


def test_reschedule_frees_old_occupancy(client):
    created = _create(client)
    rid = created["reservation_id"]
    assert client.put(f"/api/v1/reservations/{rid}",
                      json=_put_payload()).status_code == 200
    # 旧时段已释放：其他预约可以立即占用
    resp = client.post("/api/v1/reservations", json=_create_payload(
        request_id="req-2", participant_ids=["alice"],
    ))
    assert resp.status_code == 201, resp.text


# ------------------------------------------------------------------ 404 / 409

def test_reschedule_missing_reservation_returns_404(client):
    resp = client.put("/api/v1/reservations/999", json=_put_payload())
    assert resp.status_code == 404
    assert resp.json()["detail"]


def test_stale_version_returns_409_with_current_version(client):
    created = _create(client)
    rid = created["reservation_id"]
    assert client.put(f"/api/v1/reservations/{rid}",
                      json=_put_payload()).status_code == 200  # -> version 2

    resp = client.put(f"/api/v1/reservations/{rid}",
                      json=_put_payload(expected_version=1))  # 过期版本
    assert resp.status_code == 409
    data = resp.json()
    assert data["current_version"] == 2

    # 失败的改期不改变任何状态：当前版本仍可用新版本号改期
    resp = client.put(f"/api/v1/reservations/{rid}", json=_put_payload(
        expected_version=2,
        start_utc="2026-03-12T09:00:00Z",
        end_utc="2026-03-12T10:00:00Z",
    ))
    assert resp.status_code == 200, resp.text
    assert resp.json()["version"] == 3


def test_reschedule_conflict_returns_409_details_and_changes_nothing(client):
    first = _create(client)  # alice+bob 13:00-14:00
    second = _create(client, request_id="req-2",
                     start_utc="2026-03-09T15:00:00Z",
                     end_utc="2026-03-09T16:00:00Z",
                     participant_ids=["carol"])
    rid = second["reservation_id"]

    # 把 second 改到与 first 重叠的时段，并引入新参与者 dave
    resp = client.put(f"/api/v1/reservations/{rid}", json=_put_payload(
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T14:30:00Z",
        participant_ids=["alice", "dave"],
    ))
    assert resp.status_code == 409
    assert resp.json()["conflicts"] == [
        {"reservation_id": first["reservation_id"], "participant_id": "alice"}
    ]

    # 整体回滚：second 的占用保持 15:00-16:00 / carol，dave 没有留下任何占用
    resp = client.post("/api/v1/reservations", json=_create_payload(
        request_id="req-3",
        start_utc="2026-03-09T15:30:00Z",
        end_utc="2026-03-09T16:30:00Z",
        participant_ids=["carol"],
    ))
    assert resp.status_code == 409  # carol 仍被 second 占用
    resp = client.post("/api/v1/reservations", json=_create_payload(
        request_id="req-4",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T14:30:00Z",
        participant_ids=["dave"],
    ))
    assert resp.status_code == 201, resp.text  # dave 无残留占用

    # second 版本未变，仍可用 version=1 改期成功
    resp = client.put(f"/api/v1/reservations/{rid}", json=_put_payload())
    assert resp.status_code == 200, resp.text
    assert resp.json()["version"] == 2


# ------------------------------------------------------------ 排除自身的冲突检查

def test_reschedule_overlapping_own_old_slot_is_allowed(client):
    created = _create(client, participant_ids=["alice"])
    rid = created["reservation_id"]
    # 新时段与自身旧时段重叠：排除自身后无冲突
    resp = client.put(f"/api/v1/reservations/{rid}", json=_put_payload(
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T14:30:00Z",
        participant_ids=["alice"],
    ))
    assert resp.status_code == 200, resp.text
    assert resp.json()["version"] == 2


def test_reschedule_adjacent_to_other_reservation_is_allowed(client):
    _create(client, participant_ids=["alice"])  # 13:00-14:00
    second = _create(client, request_id="req-2",
                     start_utc="2026-03-09T16:00:00Z",
                     end_utc="2026-03-09T17:00:00Z",
                     participant_ids=["alice"])
    # 改为紧邻第一条预约（半开区间，end == start 不冲突）
    resp = client.put(f"/api/v1/reservations/{second['reservation_id']}",
                      json=_put_payload(
                          start_utc="2026-03-09T14:00:00Z",
                          end_utc="2026-03-09T15:00:00Z",
                          participant_ids=["alice"],
                      ))
    assert resp.status_code == 200, resp.text


# ------------------------------------------------------------------ 参数校验

def test_reschedule_rejects_invalid_window_and_timestamps(client):
    created = _create(client)
    rid = created["reservation_id"]
    # end <= start
    resp = client.put(f"/api/v1/reservations/{rid}", json=_put_payload(
        end_utc="2026-03-10T15:00:00Z"))
    assert resp.status_code == 422
    # 缺时区偏移
    resp = client.put(f"/api/v1/reservations/{rid}", json=_put_payload(
        start_utc="2026-03-10T15:00:00"))
    assert resp.status_code == 422
    # 空参与者 / 空字符串参与者 / 非法版本号
    resp = client.put(f"/api/v1/reservations/{rid}",
                      json=_put_payload(participant_ids=[]))
    assert resp.status_code == 422
    resp = client.put(f"/api/v1/reservations/{rid}",
                      json=_put_payload(participant_ids=["  "]))
    assert resp.status_code == 422
    resp = client.put(f"/api/v1/reservations/{rid}",
                      json=_put_payload(expected_version=0))
    assert resp.status_code == 422


# ---------------------------------------------------------------------- 并发

def test_concurrent_reschedule_single_winner(db_path):
    """8 个并发改期同一预约（同一 expected_version）：恰好一个成功，其余 409 语义。"""
    reservation, _ = core.create_reservation(
        request_id="req-1",
        start_utc=_utc(2026, 3, 9, 13),
        end_utc=_utc(2026, 3, 9, 14),
        participant_ids=["alice"],
        db_path=db_path,
    )
    barrier = threading.Barrier(8)

    def worker(_: int):
        barrier.wait()
        return core.reschedule_reservation(
            reservation_id=reservation.id,
            expected_version=1,
            start_utc=_utc(2026, 3, 10, 13),
            end_utc=_utc(2026, 3, 10, 14),
            participant_ids=["alice"],
            db_path=db_path,
        )

    won, stale = [], []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(worker, i) for i in range(8)]
        for f in futures:
            try:
                won.append(f.result())
            except core.VersionConflictError as exc:
                stale.append(exc)

    assert len(won) == 1
    assert len(stale) == 7
    assert won[0].version == 2
    for exc in stale:
        # 所有失败者看到的都是赢家写入后的当前版本
        assert exc.current_version == 2


# ---------------------------------------------------------------------- 迁移

_OLD_SCHEMA = """\
CREATE TABLE reservations (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id       TEXT NOT NULL UNIQUE,
    start_utc        TEXT NOT NULL,
    end_utc          TEXT NOT NULL,
    participants_key TEXT NOT NULL,
    created_at       TEXT NOT NULL
);
CREATE TABLE reservation_participants (
    reservation_id INTEGER NOT NULL REFERENCES reservations(id) ON DELETE CASCADE,
    participant_id TEXT NOT NULL,
    start_utc      TEXT NOT NULL,
    end_utc        TEXT NOT NULL,
    PRIMARY KEY (reservation_id, participant_id)
);
"""


def test_existing_database_is_backfilled_with_version_1(db_path):
    """无 version 列的旧库：启动初始化后既有行无损补齐 version=1，且可正常改期。"""
    conn = sqlite3.connect(db_path)
    conn.executescript(_OLD_SCHEMA)
    conn.execute(
        "INSERT INTO reservations"
        " (request_id, start_utc, end_utc, participants_key, created_at)"
        " VALUES ('req-old', '2026-03-09T13:00:00.000000Z',"
        " '2026-03-09T14:00:00.000000Z', '[\"alice\"]', '2026-03-01T00:00:00.000000Z')"
    )
    conn.execute(
        "INSERT INTO reservation_participants"
        " (reservation_id, participant_id, start_utc, end_utc)"
        " VALUES (1, 'alice', '2026-03-09T13:00:00.000000Z', '2026-03-09T14:00:00.000000Z')"
    )
    conn.commit()
    conn.close()

    db.init_db(db_path)  # 应用启动时的初始化：对旧表做无损迁移

    conn = db.connect(db_path)
    try:
        row = conn.execute(
            "SELECT request_id, start_utc, version FROM reservations WHERE id = 1"
        ).fetchone()
        assert row["version"] == 1
        assert row["request_id"] == "req-old"  # 既有数据保持不变
        assert row["start_utc"] == "2026-03-09T13:00:00.000000Z"
    finally:
        conn.close()

    # 迁移后的库可以按 version=1 正常改期
    client = TestClient(app)
    resp = client.put("/api/v1/reservations/1", json=_put_payload(expected_version=1))
    assert resp.status_code == 200, resp.text
    assert resp.json()["version"] == 2
    assert resp.json()["request_id"] == "req-old"
