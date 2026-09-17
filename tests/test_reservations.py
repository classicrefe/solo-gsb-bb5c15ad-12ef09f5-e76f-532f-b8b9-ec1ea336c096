"""会议预约接口测试：半开区间冲突、幂等重放、事务原子性与并发。"""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

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


def _payload(**overrides):
    payload = {
        "request_id": "req-1",
        "start_utc": "2026-03-09T13:00:00Z",
        "end_utc": "2026-03-09T14:00:00Z",
        "participant_ids": ["alice", "bob"],
    }
    payload.update(overrides)
    return payload


def _utc(y, mo, d, h, mi=0):
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


# ------------------------------------------------------------------ 基本创建

def test_create_returns_201_and_normalizes_participants(client):
    resp = client.post("/api/v1/reservations",
                       json=_payload(participant_ids=["bob", "alice", "bob"]))
    assert resp.status_code == 201, resp.text
    data = resp.json()
    assert data["reservation_id"] == 1
    assert data["request_id"] == "req-1"
    assert data["start_utc"] == "2026-03-09T13:00:00Z"
    assert data["end_utc"] == "2026-03-09T14:00:00Z"
    assert data["participants"] == ["alice", "bob"]  # 去重 + 字典序
    assert data["created_at"]


def test_end_must_be_later_than_start(client):
    # 相等
    resp = client.post("/api/v1/reservations",
                       json=_payload(end_utc="2026-03-09T13:00:00Z"))
    assert resp.status_code == 422
    # 倒置
    resp = client.post("/api/v1/reservations",
                       json=_payload(end_utc="2026-03-09T12:00:00Z"))
    assert resp.status_code == 422


def test_naive_timestamp_rejected(client):
    resp = client.post("/api/v1/reservations",
                       json=_payload(start_utc="2026-03-09T13:00:00"))
    assert resp.status_code == 422


def test_empty_participants_rejected(client):
    resp = client.post("/api/v1/reservations", json=_payload(participant_ids=[]))
    assert resp.status_code == 422


# ------------------------------------------------------------------ 冲突检测

def test_adjacent_intervals_do_not_conflict(client):
    assert client.post("/api/v1/reservations", json=_payload()).status_code == 201
    # 紧邻其后（半开区间，end == start 不冲突）
    resp = client.post("/api/v1/reservations", json=_payload(
        request_id="req-2",
        start_utc="2026-03-09T14:00:00Z",
        end_utc="2026-03-09T15:00:00Z",
    ))
    assert resp.status_code == 201, resp.text
    # 紧邻其前
    resp = client.post("/api/v1/reservations", json=_payload(
        request_id="req-3",
        start_utc="2026-03-09T12:00:00Z",
        end_utc="2026-03-09T13:00:00Z",
    ))
    assert resp.status_code == 201, resp.text


@pytest.mark.parametrize("start,end", [
    ("2026-03-09T13:30:00Z", "2026-03-09T13:45:00Z"),  # 被包含
    ("2026-03-09T12:00:00Z", "2026-03-09T15:00:00Z"),  # 包含已有
    ("2026-03-09T12:30:00Z", "2026-03-09T13:30:00Z"),  # 左侧重叠
    ("2026-03-09T13:30:00Z", "2026-03-09T14:30:00Z"),  # 右侧重叠
    ("2026-03-09T13:00:00Z", "2026-03-09T14:00:00Z"),  # 完全相同
])
def test_any_half_open_overlap_conflicts(client, start, end):
    assert client.post("/api/v1/reservations", json=_payload()).status_code == 201
    resp = client.post("/api/v1/reservations", json=_payload(
        request_id="req-2", start_utc=start, end_utc=end,
        participant_ids=["alice"],
    ))
    assert resp.status_code == 409


def test_conflict_409_reports_reservation_and_participant(client):
    first = client.post("/api/v1/reservations", json=_payload()).json()
    resp = client.post("/api/v1/reservations", json=_payload(
        request_id="req-2",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T14:30:00Z",
        participant_ids=["bob", "carol"],
    ))
    assert resp.status_code == 409
    data = resp.json()
    assert data["conflicts"] == [
        {"reservation_id": first["reservation_id"], "participant_id": "bob"}
    ]


def test_conflict_leaves_no_partial_writes(client):
    assert client.post("/api/v1/reservations", json=_payload()).status_code == 201
    # alice 冲突 → carol 也不能被部分写入
    resp = client.post("/api/v1/reservations", json=_payload(
        request_id="req-2",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T14:30:00Z",
        participant_ids=["alice", "carol"],
    ))
    assert resp.status_code == 409
    # carol 在同一时段应能立即预约成功（证明没有残留占用）
    resp = client.post("/api/v1/reservations", json=_payload(
        request_id="req-3",
        start_utc="2026-03-09T13:30:00Z",
        end_utc="2026-03-09T14:30:00Z",
        participant_ids=["carol"],
    ))
    assert resp.status_code == 201, resp.text


def test_disjoint_participants_same_slot_ok(client):
    assert client.post("/api/v1/reservations", json=_payload()).status_code == 201
    resp = client.post("/api/v1/reservations",
                       json=_payload(request_id="req-2",
                                     participant_ids=["carol", "dave"]))
    assert resp.status_code == 201, resp.text


# ---------------------------------------------------------------------- 幂等

def test_idempotent_replay_returns_original_without_new_record(client):
    first = client.post("/api/v1/reservations",
                        json=_payload(participant_ids=["bob", "alice"]))
    assert first.status_code == 201
    # 同一 request_id + 同一规范化内容（参与者顺序/重复不同也算同一请求）
    replay = client.post("/api/v1/reservations",
                         json=_payload(participant_ids=["alice", "bob", "alice"]))
    assert replay.status_code == 200
    assert replay.json() == first.json()
    # 不新增记录：下一条新预约的 id 应紧接其后
    nxt = client.post("/api/v1/reservations", json=_payload(
        request_id="req-2",
        start_utc="2026-03-09T15:00:00Z",
        end_utc="2026-03-09T16:00:00Z",
    ))
    assert nxt.status_code == 201
    assert nxt.json()["reservation_id"] == first.json()["reservation_id"] + 1


def test_equivalent_timezone_offsets_are_the_same_request(client):
    first = client.post("/api/v1/reservations", json=_payload())
    assert first.status_code == 201
    # +09:00 表示的同一物理时刻，规范化后视为同一请求 → 幂等重放
    replay = client.post("/api/v1/reservations", json=_payload(
        start_utc="2026-03-09T22:00:00+09:00",
        end_utc="2026-03-09T23:00:00+09:00",
    ))
    assert replay.status_code == 200
    assert replay.json()["reservation_id"] == first.json()["reservation_id"]


def test_same_request_id_different_content_returns_409(client):
    assert client.post("/api/v1/reservations", json=_payload()).status_code == 201
    # 时间不同
    resp = client.post("/api/v1/reservations",
                       json=_payload(end_utc="2026-03-09T15:00:00Z"))
    assert resp.status_code == 409
    assert resp.json()["existing_reservation_id"] == 1
    # 参与者不同
    resp = client.post("/api/v1/reservations",
                       json=_payload(participant_ids=["alice"]))
    assert resp.status_code == 409


# ---------------------------------------------------------------------- 并发

def test_concurrent_conflicting_requests_single_winner(db_path):
    """8 个并发请求抢占同一参与者的同一时段：恰好一个成功，其余全部 409 语义。"""
    barrier = threading.Barrier(8)

    def worker(i: int):
        barrier.wait()
        return core.create_reservation(
            request_id=f"req-{i}",
            start_utc=_utc(2026, 3, 9, 13),
            end_utc=_utc(2026, 3, 9, 14),
            participant_ids=["alice"],
            db_path=db_path,
        )

    created, conflicts = [], []
    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(worker, i) for i in range(8)]
        for f in futures:
            try:
                created.append(f.result())
            except core.ReservationConflictError as exc:
                conflicts.append(exc)

    assert len(created) == 1
    assert len(conflicts) == 7
    winner = created[0][0]
    assert created[0][1] is True
    for exc in conflicts:
        # 所有冲突都指向赢家预约，且没有任何部分写入
        assert exc.conflicts == [
            {"reservation_id": winner.id, "participant_id": "alice"}
        ]


def test_concurrent_idempotent_replays_create_single_record(db_path):
    """同一 request_id + 同一内容并发重试：全部拿到同一条预约，只新建一次。"""
    barrier = threading.Barrier(8)
    kwargs = dict(
        request_id="req-same",
        start_utc=_utc(2026, 3, 9, 13),
        end_utc=_utc(2026, 3, 9, 14),
        participant_ids=["alice", "bob"],
        db_path=db_path,
    )

    def worker():
        barrier.wait()
        return core.create_reservation(**kwargs)

    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(lambda _: worker(), range(8)))

    ids = {r.id for r, _ in outcomes}
    assert len(ids) == 1                                    # 同一条预约
    assert sum(1 for _, created in outcomes if created) == 1  # 只新建一次
    # 数据库里确实只有这一条：下一条预约 id 紧接其后
    nxt, created = core.create_reservation(
        request_id="req-next",
        start_utc=_utc(2026, 3, 10, 13),
        end_utc=_utc(2026, 3, 10, 14),
        participant_ids=["alice"],
        db_path=db_path,
    )
    assert created is True
    assert nxt.id == ids.pop() + 1


def test_concurrent_requests_via_api(db_path):
    """API 层并发：同一参与者同一时段，恰好一个 201，其余 409。"""

    def worker(i: int):
        client = TestClient(app)
        return client.post("/api/v1/reservations", json=_payload(
            request_id=f"req-{i}",
            participant_ids=["alice"],
        ))

    with ThreadPoolExecutor(max_workers=6) as pool:
        responses = list(pool.map(worker, range(6)))

    assert sorted(r.status_code for r in responses) == [201, 409, 409, 409, 409, 409]


# ---------------------------------------------------------- 聚合接口回归

def test_aggregate_endpoint_still_works(client):
    resp = client.post("/api/v1/availability/aggregate", json={
        "start_date": "2026-03-09",
        "end_date": "2026-03-09",
        "meeting_minutes": 60,
        "min_attendees": 1,
        "participants": [
            {"id": "a", "timezone": "UTC",
             "availability": [{"weekday": 1, "start_local": "09:00", "end_local": "17:00"}]},
        ],
    })
    assert resp.status_code == 200
    assert len(resp.json()["segments"]) == 1
