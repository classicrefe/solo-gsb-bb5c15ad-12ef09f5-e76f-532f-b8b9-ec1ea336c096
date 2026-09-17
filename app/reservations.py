"""会议预约核心逻辑：幂等创建 + 单事务冲突检测。

- 参与者 ID 去重并按字典序排序后，作为规范化请求内容参与幂等比较；
- 区间一律按半开 ``[start, end)`` 处理：首尾相接（相邻）不算冲突；
- 冲突检查与写入在同一个 ``BEGIN IMMEDIATE`` 事务内完成：
  SQLite 在事务开始时即取写锁，并发请求被串行化；
  任一参与者与已有预约重叠都会整体回滚，不会留下部分写入。
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


@dataclass(frozen=True)
class Reservation:
    id: int
    request_id: str
    start_utc: datetime
    end_utc: datetime
    participants: tuple[str, ...]
    created_at: datetime


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
    )


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
                "SELECT id, request_id, start_utc, end_utc, participants_key, created_at"
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

            conflicts: list[dict[str, object]] = []
            for pid in participants:
                # 半开区间重叠：existing.start < new.end 且 new.start < existing.end
                # （相邻区间因等号不成立而不会命中）
                for hit in conn.execute(
                    "SELECT reservation_id, participant_id"
                    " FROM reservation_participants"
                    " WHERE participant_id = ? AND start_utc < ? AND ? < end_utc"
                    " ORDER BY reservation_id",
                    (pid, end_s, start_s),
                ):
                    conflicts.append({
                        "reservation_id": hit["reservation_id"],
                        "participant_id": hit["participant_id"],
                    })
            if conflicts:
                raise ReservationConflictError(conflicts)

            now_s = _canon(datetime.now(timezone.utc))
            cursor = conn.execute(
                "INSERT INTO reservations"
                " (request_id, start_utc, end_utc, participants_key, created_at)"
                " VALUES (?, ?, ?, ?, ?)",
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
            ), True
        except BaseException:
            # 无活动事务时 rollback 为 no-op；任何失败都保证整体回滚
            conn.rollback()
            raise
    finally:
        conn.close()
