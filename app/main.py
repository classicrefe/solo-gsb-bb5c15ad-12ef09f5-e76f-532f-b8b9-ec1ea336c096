"""FastAPI 应用：跨时区可用时段聚合 + SQLite 会议预约。

启动（容器内）：uvicorn app.main:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse

from . import db as db_module
from . import reservations as reservation_core
from . import scheduling
from .reservations import RequestIdConflictError, ReservationConflictError
from .scheduling import SchedulingError
from .schemas import (
    AggregationRequest,
    AggregationResponse,
    LocalTimeOut,
    ParticipantIn,
    ReservationIn,
    ReservationOut,
    SegmentOut,
    WEEKDAY_NAMES,
)
from .scheduling import Participant, Window as CoreWindow


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Compose 启动时自动初始化 SQLite（建表幂等）；
    # 数据库文件路径由 RESERVATIONS_DB_PATH 配置，默认落在挂载卷上
    db_module.init_db()
    yield


app = FastAPI(
    title="跨时区可用时段聚合 API",
    description=(
        "接收日期范围、会议时长、最低出席人数及每位参与者按本地星期/时刻填写的 "
        "重复可用区间与 IANA 时区，返回可安排会议的 UTC 候选时段及各自本地时刻。"
        "DST 按具体日期应用：不存在的本地时刻返回 400，歧义时刻统一取 fold=0。"
        "另提供 SQLite 持久化的会议预约接口：单事务冲突检测 + request_id 幂等。"
    ),
    version="1.1.0",
    lifespan=lifespan,
)


@app.exception_handler(SchedulingError)
async def scheduling_error_handler(request, exc: SchedulingError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"detail": str(exc)})


@app.exception_handler(ReservationConflictError)
async def reservation_conflict_handler(request, exc: ReservationConflictError) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": str(exc),
            "conflicts": exc.conflicts,
        },
    )


@app.exception_handler(RequestIdConflictError)
async def request_id_conflict_handler(request, exc: RequestIdConflictError) -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={
            "detail": str(exc),
            "existing_reservation_id": exc.existing_reservation_id,
        },
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


def _to_core_participant(p: ParticipantIn) -> Participant:
    return Participant(
        participant_id=p.id,
        timezone=p.timezone,
        windows=tuple(
            CoreWindow(weekday=w.weekday,
                       start_local=w.start_local,
                       end_local=w.end_local)
            for w in p.availability
        ),
    )


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _segment_to_out(seg: scheduling.Segment,
                    tz_by_id: dict[str, ZoneInfo]) -> SegmentOut:
    local_times: list[LocalTimeOut] = []
    for pid in seg.attendees:
        tzinfo = tz_by_id[pid]
        start_local = seg.start_utc.astimezone(tzinfo)
        end_local = seg.end_utc.astimezone(tzinfo)
        local_times.append(LocalTimeOut(
            participant_id=pid,
            timezone=str(tzinfo),
            start_local=start_local.isoformat(),
            end_local=end_local.isoformat(),
            start_weekday=WEEKDAY_NAMES[start_local.isoweekday()],
            end_weekday=WEEKDAY_NAMES[end_local.isoweekday()],
        ))
    return SegmentOut(
        start_utc=_iso_utc(seg.start_utc),
        end_utc=_iso_utc(seg.end_utc),
        duration_minutes=int(seg.duration.total_seconds() // 60),
        attendees=list(seg.attendees),
        attendee_count=len(seg.attendees),
        local_times=local_times,
    )


@app.post("/api/v1/availability/aggregate", response_model=AggregationResponse)
async def aggregate_availability(req: AggregationRequest) -> AggregationResponse:
    participants = [_to_core_participant(p) for p in req.participants]
    # 提前解析所有时区，未知名称统一返回 400
    tz_by_id = {p.participant_id: scheduling.get_timezone(p.timezone)
                for p in participants}

    segments = scheduling.aggregate(
        participants=participants,
        start_date=req.start_date,
        end_date=req.end_date,
        meeting_minutes=req.meeting_minutes,
        min_attendees=req.min_attendees,
    )

    return AggregationResponse(
        start_date=req.start_date,
        end_date=req.end_date,
        meeting_minutes=req.meeting_minutes,
        min_attendees=req.min_attendees,
        segments=[_segment_to_out(seg, tz_by_id) for seg in segments],
    )


# ---------------------------------------------------------------- reservations


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _reservation_to_out(r: reservation_core.Reservation) -> ReservationOut:
    return ReservationOut(
        reservation_id=r.id,
        request_id=r.request_id,
        start_utc=_iso_z(r.start_utc),
        end_utc=_iso_z(r.end_utc),
        participants=list(r.participants),
        created_at=_iso_z(r.created_at),
    )


@app.post("/api/v1/reservations", response_model=ReservationOut, status_code=201)
def create_reservation(req: ReservationIn, response: Response) -> ReservationOut:
    """创建会议预约（同步函数，在线程池中执行以避免阻塞事件循环）。

    - 任一参与者与已有预约半开区间重叠 → 409 + 冲突预约/参与者，整体不写库；
    - 相同 request_id + 相同规范化内容 → 200 返回原预约，不新增记录；
    - 相同 request_id + 不同内容 → 409。
    """
    reservation, created = reservation_core.create_reservation(
        request_id=req.request_id,
        start_utc=req.start_utc,
        end_utc=req.end_utc,
        participant_ids=req.participant_ids,
    )
    if not created:
        # 幂等重放：返回原预约
        response.status_code = 200
    return _reservation_to_out(reservation)
