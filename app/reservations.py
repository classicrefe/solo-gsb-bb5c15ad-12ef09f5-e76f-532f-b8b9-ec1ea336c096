"""会议预约核心逻辑：幂等创建 + 单事务冲突检测 + 乐观锁改期。

- 参与者 ID 去重并按字典序排序后，作为规范化请求内容参与幂等比较；
- 区间一律按半开 ``[start, end)`` 处理：首尾相接（相邻）不算冲突；
- 冲突检查与写入在同一个 ``BEGIN IMMEDIATE`` 事务内完成：
  SQLite 在事务开始时即取写锁，并发请求被串行化；
  任一参与者与已有预约重叠都会整体回滚，不会留下部分写入；
- 改期（reschedule）在同一类事务内依次校验“预约存在 → 版本匹配 →
  排除自身后的冲突检查”，全部通过才原子替换预约与参与者占用，
  并将 ``version`` 加 1；任一步失败整体回滚，预约与占用保持不变。
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from . import db

# 定宽 UTC 文本格式：字典序即时间序，可直接用于 SQL 范围比较
_CANON_FMT = "%Y-%m-%dT%H:%M:%S.%fZ"


class ReservationConflictError(Exception):
    """至少一位参与者与已有预约存在半开区间重叠。"""

    def __init__(self, conflicts: list[dict[str, object]]) -> None:
        self.conflicts = conflicts  # [{"reservation_id": ..., "participant_id": ...}]
        pretty = ", ".join(
            f"participant {c['participant_id']!r} overlaps reservation #{c['reservation_id']}"
            for c in conflicts
        )
        super().__init__(f"reservation conflict: {pretty}")


class RequestIdConflictError(Exception):
    """request_id 已被内容不同的请求占用。"""

    def __init__(self, existing_reservation_id: int) -> None:
        self.existing_reservation_id = existing_reservation_id
        super().__init__(
            f"request_id is already used by reservation #{existing_reservation_id} "
            "with different content"
        )


class ReservationNotFoundError(Exception):
    """改期目标预约不存在。"""

    def __init__(self, reservation_id: int) -> None:
        self.reservation_id = reservation_id
        super().__init__(f"reservation #{reservation_id} not found")


class VersionConflictError(Exception):
    """expected_version 与当前版本不一致（预约已被其他请求改过）。"""

    def __init__(self, reservation_id: int, current_version: int) -> None:
        self.reservation_id = reservation_id
        self.current_version = current_version
        super().__init__(
            f"reservation #{reservation_id} version mismatch: "
            f"current version is {current_version}"
        )


@dataclass(frozen=True)
class Reservation:
    id: int
    request_id: str
    start_utc: datetime
    end_utc: datetime
    participants: tuple[str, ...]
    created_at: datetime
    version: int


def _canon(dt: datetime) -> str:
    """统一成定宽 UTC 字符串，保证字典序与时间序一致。"""
    return dt.astimezone(timezone.utc).strftime(_CANON_FMT)


def _parse(text: str) -> datetime:
    return datetime.strptime(text, _CANON_FMT).replace(tzinfo=timezone.utc)


def normalize_participants(participant_ids: Iterable[str]) -> tuple[str, ...]:
    """去重 + 字典序排序，得到规范化参与者列表。"""
    return tuple(sorted(set(participant_ids)))


def _row_to_reservation(row: sqlite3.Row) -> Reservation:
    return Reservation(
        id=row["id"],
        request_id=row["request_id"],
        start_utc=_parse(row["start_utc"]),
        end_utc=_parse(row["end_utc"]),
        participants=tuple(json.loads(row["participants_key"])),
        created_at=_parse(row["created_at"]),
        version=row["version"],
    )


def _find_conflicts(
    conn: sqlite3.Connection,
    participants: tuple[str, ...],
    start_s: str,
    end_s: str,
    exclude_reservation_id: int | None = None,
) -> list[dict[str, object]]:
    """半开区间重叠检查，可按预约 ID 排除自身（改期时旧占用不算冲突）。

    重叠判定：existing.start < new.end 且 new.start < existing.end
    （相邻区间因等号不成立而不会命中）。
    """
    sql = (
        "SELECT reservation_id, participant_id"
        " FROM reservation_participants"
        " WHERE participant_id = ? AND start_utc < ? AND ? < end_utc"
    )
    params: list[object]
    if exclude_reservation_id is not None:
        sql += " AND reservation_id != ?"
    sql += " ORDER BY reservation_id"

    conflicts: list[dict[str, object]] = []
    for pid in participants:
        params = [pid, end_s, start_s]
        if exclude_reservation_id is not None:
            params.append(exclude_reservation_id)
        for hit in conn.execute(sql, params):
            conflicts.append({
                "reservation_id": hit["reservation_id"],
                "participant_id": hit["participant_id"],
            })
    return conflicts


def create_reservation(
    request_id: str,
    start_utc: datetime,
    end_utc: datetime,
    participant_ids: Iterable[str],
    db_path: str | None = None,
) -> tuple[Reservation, bool]:
    """创建预约，返回 ``(预约, 是否新建)``。

    - ``request_id`` 已存在且规范化内容（起止时刻 + 去重排序后的参与者）相同
      → 返回原预约，``created=False``，不新增记录；
    - ``request_id`` 已存在但内容不同 → :class:`RequestIdConflictError`；
    - 任一参与者与已有预约半开区间重叠 → :class:`ReservationConflictError`，
      整体回滚，不留下任何部分写入。
    """
    participants = normalize_participants(participant_ids)
    start_s = _canon(start_utc)
    end_s = _canon(end_utc)
    participants_key = json.dumps(list(participants), ensure_ascii=False)

    path = db.ensure_initialized(db_path)
    conn = db.connect(path)
    try:
        # BEGIN IMMEDIATE：事务开始即持有数据库写锁，
        # 使“检查 + 写入”对并发请求原子（并发写者被串行化）。
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT id, request_id, start_utc, end_utc, participants_key,"
                " created_at, version"
                " FROM reservations WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is not None:
                if (row["start_utc"] == start_s
                        and row["end_utc"] == end_s
                        and row["participants_key"] == participants_key):
                    conn.execute("COMMIT")
                    return _row_to_reservation(row), False
                raise RequestIdConflictError(row["id"])

            conflicts = _find_conflicts(conn, participants, start_s, end_s)
            if conflicts:
                raise ReservationConflictError(conflicts)

            now_s = _canon(datetime.now(timezone.utc))
            cursor = conn.execute(
                "INSERT INTO reservations"
                " (request_id, start_utc, end_utc, participants_key, created_at, version)"
                " VALUES (?, ?, ?, ?, ?, 1)",
                (request_id, start_s, end_s, participants_key, now_s),
            )
            reservation_id = int(cursor.lastrowid)
            conn.executemany(
                "INSERT INTO reservation_participants"
                " (reservation_id, participant_id, start_utc, end_utc)"
                " VALUES (?, ?, ?, ?)",
                [(reservation_id, pid, start_s, end_s) for pid in participants],
            )
            conn.execute("COMMIT")
            return Reservation(
                id=reservation_id,
                request_id=request_id,
                start_utc=_parse(start_s),
                end_utc=_parse(end_s),
                participants=participants,
                created_at=_parse(now_s),
                version=1,
            ), True
        except BaseException:
            # 无活动事务时 rollback 为 no-op；任何失败都保证整体回滚
            conn.rollback()
            raise
    finally:
        conn.close()


def reschedule_reservation(
    reservation_id: int,
    expected_version: int,
    start_utc: datetime,
    end_utc: datetime,
    participant_ids: Iterable[str],
    db_path: str | None = None,
) -> Reservation:
    """改期：单个 ``BEGIN IMMEDIATE`` 事务内完成全部校验与原子替换。

    事务内依次执行，任一步失败整体回滚，预约与参与者占用保持不变：

    1. 预约不存在 → :class:`ReservationNotFoundError`；
    2. ``expected_version`` 与当前版本不一致 → :class:`VersionConflictError`；
    3. 排除自身旧占用后，任一参与者与他人预约半开区间重叠
       → :class:`ReservationConflictError`。

    全部通过则原子替换起止时刻与参与者占用，``version`` 加 1 并返回更新后的预约。
    """
    participants = normalize_participants(participant_ids)
    start_s = _canon(start_utc)
    end_s = _canon(end_utc)
    participants_key = json.dumps(list(participants), ensure_ascii=False)

    path = db.ensure_initialized(db_path)
    conn = db.connect(path)
    try:
        # 与创建相同：BEGIN IMMEDIATE 使“校验 + 替换”对并发请求原子
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT id, request_id, start_utc, end_utc, participants_key,"
                " created_at, version"
                " FROM reservations WHERE id = ?",
                (reservation_id,),
            ).fetchone()
            if row is None:
                raise ReservationNotFoundError(reservation_id)
            if row["version"] != expected_version:
                raise VersionConflictError(reservation_id, row["version"])

            # 排除自身：本预约的旧参与者占用不参与冲突判定
            conflicts = _find_conflicts(
                conn, participants, start_s, end_s,
                exclude_reservation_id=reservation_id,
            )
            if conflicts:
                raise ReservationConflictError(conflicts)

            new_version = expected_version + 1
            conn.execute(
                "UPDATE reservations"
                " SET start_utc = ?, end_utc = ?, participants_key = ?, version = ?"
                " WHERE id = ?",
                (start_s, end_s, participants_key, new_version, reservation_id),
            )
            conn.execute(
                "DELETE FROM reservation_participants WHERE reservation_id = ?",
                (reservation_id,),
            )
            conn.executemany(
                "INSERT INTO reservation_participants"
                " (reservation_id, participant_id, start_utc, end_utc)"
                " VALUES (?, ?, ?, ?)",
                [(reservation_id, pid, start_s, end_s) for pid in participants],
            )
            conn.execute("COMMIT")
            return Reservation(
                id=reservation_id,
                request_id=row["request_id"],
                start_utc=_parse(start_s),
                end_utc=_parse(end_s),
                participants=participants,
                created_at=_parse(row["created_at"]),
                version=new_version,
            )
        except BaseException:
            # 无活动事务时 rollback 为 no-op；任何失败都保证整体回滚
            conn.rollback()
            raise
    finally:
        conn.close()
