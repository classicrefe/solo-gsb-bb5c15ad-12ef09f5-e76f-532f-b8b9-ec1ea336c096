# 跨时区可用时段聚合 API

基于 **Python + FastAPI + 标准库 `zoneinfo`** 的会议候选时段聚合服务，
并附带 **SQLite 持久化的会议预约接口**（幂等创建 + 单事务冲突检测）。

每位参与者按**本地星期与钟面时刻**填写每周重复的可用区间并给出 IANA 时区
（如 `Asia/Shanghai`），服务按具体日期应用夏令时（DST）规则，统一换算到 UTC，
用**扫描线**算法计算所有满足最低出席人数与会议时长的候选时段。

- 交互式文档（Swagger UI）：<http://localhost:8000/docs>
- ReDoc：<http://localhost:8000/redoc>
- 健康检查：`GET /health`

## 语义说明

- **搜索区间**：`[start_date 00:00 UTC, end_date + 1 天 00:00 UTC)`，起止日期均含（以 UTC 日期为界）。
  这意味着 UTC+14 时区里落在本地周一凌晨的窗口，其 UTC 时刻可能处于周日、从而被裁剪在区间之外。
- **跨午夜窗口**：`end_local <= start_local` 时视为跨午夜，自动拆成“当日起点 → 次日终点”的半开区间后换算 UTC。
- **DST 处理**（按每个具体日期分别计算）：
  - **不存在的本地时刻**（春季跳变被跳过的钟面时间，如美东 2026-03-08 02:30）→ 返回 `400` 错误；
  - **歧义时刻**（秋季回退出现两次的钟面时间）→ 统一选择 `fold=0`，即 UTC 偏移量较大、较早出现的那一次。
- **候选段**：出席者集合保持恒定的最大区间；时长必须 `>= meeting_minutes`。
- **排序**：出席人数**降序**，人数相同时按 UTC 开始时间**升序**（稳定排序）。
- 同一参与者互相重叠的窗口会先按人合并，扫描线中一人在任一时刻至多计一份出席。

## 目录结构

```
app/
  scheduling.py    # 聚合核心：本地时刻换算、跨午夜拆分、扫描线、排序
  reservations.py  # 预约核心：规范化、幂等创建、单事务冲突检测
  db.py            # SQLite 连接管理与建表（路径可配置）
  schemas.py       # Pydantic 请求/响应模型
  main.py          # FastAPI 路由
tests/             # pytest 测试（DST、跨午夜、排序、预约冲突/幂等/并发）
Dockerfile
docker-compose.yml
requirements.txt  # 运行依赖
requirements-dev.txt  # 测试依赖（含 pytest、httpx）
```

## 配置

| 配置项 | 位置 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `APP_PORT` | 容器环境变量 | `8000` | 容器内 uvicorn 监听端口 |
| `HOST_PORT` | 宿主机环境变量 / `.env` | `8000` | 映射到宿主机的端口 |
| `RESERVATIONS_DB_PATH` | 容器环境变量 / `.env` | `/data/reservations.db`（Compose）<br>`./data/reservations.db`（本地） | SQLite 数据库文件路径 |
| `TZ` | 容器环境变量 | `UTC` | 容器系统时区（不影响按 IANA 时区进行的换算） |

构建时镜像内会安装 `tzdata`（IANA 时区数据库），Python 的 `tzdata` pip 包也作为兜底依赖安装。

## 用 Docker Compose 一条命令启动

```bash
docker compose up --build
```

启动时应用会**自动初始化 SQLite 数据库**（建表幂等）；数据库文件落在命名卷
`reservations-data`（容器内 `/data`）上，容器重建后预约数据不丢失。

服务启动后访问 <http://localhost:8000>（Swagger UI 在 <http://localhost:8000/docs>）。
若 8000 端口被占用：

```bash
HOST_PORT=9000 docker compose up --build   # 访问 http://localhost:9000
```

后台运行与停止：

```bash
docker compose up -d --build
docker compose logs -f
docker compose down          # 加 -v 会连同数据库卷一起删除
```

## 本地开发（不使用 Docker）

需要 Python 3.11+。

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt

# 启动开发服务器（端口 8000）
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000

# 运行测试
pytest -q
```

## API

### `POST /api/v1/availability/aggregate`

请求体：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `start_date` / `end_date` | `YYYY-MM-DD` | 搜索日期范围，均含（UTC 日期为界） |
| `meeting_minutes` | 整数 > 0 | 会议时长（分钟） |
| `min_attendees` | 整数 > 0 | 最低出席人数（不能超过参与者总数） |
| `participants[].id` | 字符串 | 参与者唯一标识 |
| `participants[].timezone` | 字符串 | IANA 时区名，如 `Europe/Berlin` |
| `participants[].availability[].weekday` | `1..7` 或名称 | `1`=周一 … `7`=周日；也接受 `Monday`/`mon` |
| `...start_local` / `end_local` | `HH:MM[:SS]` | 本地钟面时刻；`end <= start` 表示跨午夜 |

请求示例（东京周一 22:00 跨午夜到次日 02:00，纽约周一 09:00–12:00）：

```bash
curl -s -X POST http://localhost:8000/api/v1/availability/aggregate \
  -H 'Content-Type: application/json' \
  -d '{
    "start_date": "2026-03-09",
    "end_date": "2026-03-09",
    "meeting_minutes": 60,
    "min_attendees": 2,
    "participants": [
      {"id": "tokyo", "timezone": "Asia/Tokyo",
       "availability": [{"weekday": 1, "start_local": "22:00", "end_local": "02:00"}]},
      {"id": "ny", "timezone": "America/New_York",
       "availability": [{"weekday": 1, "start_local": "09:00", "end_local": "12:00"}]}
    ]
  }'
```

响应（共同可用段为 UTC 13:00–16:00；东京本地已跨到周二凌晨）：

```json
{
  "start_date": "2026-03-09",
  "end_date": "2026-03-09",
  "meeting_minutes": 60,
  "min_attendees": 2,
  "segments": [
    {
      "start_utc": "2026-03-09T13:00:00Z",
      "end_utc": "2026-03-09T16:00:00Z",
      "duration_minutes": 180,
      "attendees": ["ny", "tokyo"],
      "attendee_count": 2,
      "local_times": [
        {
          "participant_id": "ny",
          "timezone": "America/New_York",
          "start_local": "2026-03-09T09:00:00-04:00",
          "end_local": "2026-03-09T12:00:00-04:00",
          "start_weekday": "Monday",
          "end_weekday": "Monday"
        },
        {
          "participant_id": "tokyo",
          "timezone": "Asia/Tokyo",
          "start_local": "2026-03-09T22:00:00+09:00",
          "end_local": "2026-03-10T01:00:00+09:00",
          "start_weekday": "Monday",
          "end_weekday": "Tuesday"
        }
      ]
    }
  ]
}
```

错误：

- `400`：本地时刻因 DST 春跳变不存在，或 IANA 时区名未知；
- `422`：请求结构校验失败（日期倒置、人数非法、id 重复、weekday 越界等）。

### `POST /api/v1/reservations`

创建会议预约（SQLite 持久化）。请求体：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `request_id` | 非空字符串 | 客户端幂等请求标识 |
| `start_utc` / `end_utc` | ISO 8601（须带时区偏移，如 `...Z`） | UTC 起止时刻；`end_utc` 必须晚于 `start_utc` |
| `participant_ids` | 非空字符串数组 | 参与者 ID；**去重并排序后**存储并参与幂等比较 |

语义：

- **原子占用**：检查与写入在单个 `BEGIN IMMEDIATE` 事务内完成，并发请求被串行化；
  只要任一参与者与已有预约存在**半开区间重叠**（`existing.start < new.end` 且
  `new.start < existing.end`），整个请求被拒绝，不会留下部分写入。
  **相邻区间不冲突**（`end == start` 首尾相接可以共存）。
- **幂等**：相同 `request_id` + 相同规范化内容（起止时刻、去重排序后的参与者）
  重试 → `200` 返回原预约，不新增记录；同一 `request_id` 内容不同 → `409`。

请求示例：

```bash
curl -s -X POST http://localhost:8000/api/v1/reservations \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id": "req-9f3a",
    "start_utc": "2026-03-09T13:00:00Z",
    "end_utc": "2026-03-09T14:00:00Z",
    "participant_ids": ["alice", "bob"]
  }'
```

成功响应（`201`，幂等重放为 `200`）：

```json
{
  "reservation_id": 1,
  "request_id": "req-9f3a",
  "start_utc": "2026-03-09T13:00:00Z",
  "end_utc": "2026-03-09T14:00:00Z",
  "participants": ["alice", "bob"],
  "created_at": "2026-09-17T20:42:39.634009Z"
}
```

冲突响应（`409`，含冲突预约 ID 与参与者）：

```json
{
  "detail": "reservation conflict: participant 'bob' overlaps reservation #1",
  "conflicts": [{"reservation_id": 1, "participant_id": "bob"}]
}
```

`request_id` 被不同内容占用时（`409`）：

```json
{
  "detail": "request_id is already used by reservation #1 with different content",
  "existing_reservation_id": 1
}
```

其他错误：`422`（`end_utc` 不晚于 `start_utc`、时间缺时区偏移、参与者列表为空等）。

## 测试覆盖要点

- **春季跳变**：不存在的钟面时刻报错；跨越跳变间隙的窗口 UTC 时长缩短（柏林 2026-03-29 00:00–04:00 实际只有 3 小时）。
- **秋季回退**：歧义窗口取 `fold=0`（美东 2025-11-02 01:00–02:00 → UTC 05:00–07:00，共 2 小时）。
- **跨午夜**：窗口拆分、与另一时区窗口求交、各自本地时刻回显。
- 扫描线排序（人数降序、开始时间升序）、同参与者重叠窗口合并、UTC 边界裁剪、时长过滤。
- **预约**：半开区间各方向重叠均 409、相邻区间不冲突、冲突无部分写入、
  幂等重放返回原预约且不新增记录、`request_id` 内容不同 409、
  多线程并发下同一时段仅一个赢家、并发重放只建一条记录。

```bash
pytest -q
```
