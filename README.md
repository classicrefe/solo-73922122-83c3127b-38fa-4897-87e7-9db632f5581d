# 公共采购公告采集系统

面向招标数据员的公告采集交付件：FastAPI 采集器 + 带限流/延迟开关的本地模拟采购站
+ 本地分析接收服务，Docker Compose 一键启动。浏览器即可启动采集、查看实时进度、
停止任务、下载 JSON，并把已结束的轮次作为批次经 HTTP 交付到分析库。

## 目录结构

```
.
├── docker-compose.yml          # 三服务编排：procurement-site + analysis-receiver + collector
├── collector/                  # 采集器（FastAPI + httpx + BeautifulSoup）
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # API、JSON 下载、交付 API、采购站同源代理、静态页挂载
│       ├── crawler.py          # 采集引擎：并发闸门、429 全站冷却、停止回收
│       ├── delivery.py         # 批次冻结、SQLite 持久化、交付/重试/回执核实/自动续办
│       ├── parser.py           # BeautifulSoup 列表/详情解析（失败抛 ParseError）
│       └── static/index.html   # 浏览器控制台（单文件，原生 JS）
├── site/                       # 本地模拟公共采购站
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # 栏目/详情页、滑动窗口限流、延迟开关、管理页
│       └── data.py             # 确定性内存数据集（含跨栏目重复编号与 1 个坏详情页）
├── receiver/                   # 本地分析接收服务（SQLite + 批次回执）
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       └── main.py             # POST /api/batches、回执重放、同号异内容 409、丢响应开关、明细页
└── tests/                      # 契约测试与解析器/接收端单测（标准库 + venv 依赖）
```

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 | 说明 |
| --- | --- | --- |
| 采集控制台 | http://localhost:8000/ | 启动/停止/进度/事件/下载 JSON/交付批次与状态 |
| 采购站（同源代理入口） | http://localhost:8000/site/ | 浏览器入口，经采集器反代到站点 |
| 采购站限流管理 | http://localhost:8000/site/admin | 限流阈值、窗口、延迟毫秒开关 |
| 采购站直连 | http://localhost:8080/ | 仅排障时使用 |
| 分析接收端 | http://localhost:8090/ | 已接收批次明细、回执、丢响应故障开关 |

数据持久化：`collector-data` 卷（`/data/collector.db`，冻结批次 + 交付状态）与
`receiver-data` 卷（`/data/receiver.db`，整批公告 + 完整性标记 + 回执）。
`docker compose down` 不会删除批次数据；需要清空时用 `docker compose down -v`。

## 采集规则与实现对照

| 需求 | 实现 |
| --- | --- |
| 固定站点两个栏目，遍历分页列表及详情 | `Crawler` 配置 `/tenders`、`/awards`；首页解析 `data-total-pages` 后入队其余列表页，再逐条入队详情页 |
| 按公告编号去重 | 集合 `_seen_codes` 跨栏目去重；同编号只采一次详情，跳过计数在分栏目进度中可见 |
| 提取标题、采购单位、截止日期 | `parser.parse_detail_page`，缺任一字段即 `ParseError` |
| 可下载 JSON | `GET /api/records/download`，附件文件名带轮次 ID |
| 单进程同时只采一轮 | 单 uvicorn worker + `round_lock`；重复启动返回 `409` |
| 最多两个请求在途 | 全站共享 `asyncio.Semaphore(2)`，两个栏目共用 |
| 429 按 Retry-After 整数秒全站暂停 | `_extend_cooldown` 维护单一冷却钟，`_pass_gate` 拦截所有后续请求（含另一栏目与重试）；后续 429 只延长不缩短；已在途请求允许收尾 |
| 每页最多尝试三次 | 列表/详情统一 `_get_parsed_page`，传输错误、4xx/5xx、解析失败均消耗尝试次数 |
| 耗尽/解析失败须列出未完成页，不能报整轮成功 | 失败页进入 `unfinished`；轮次结束状态为 `partial` 而非 `completed` |
| 停止阻止排队/冷却请求，关闭在途请求并回收连接后才显示已停止 | `stop_round` 先置停止位，再 `aclose()` 关闭在途请求与连接池，`await` 轮次协程收尾后状态才变 `stopped`，随后重建客户端允许新轮 |
| 旧响应不能写入新轮 | 每轮生成 `round_id`，冷却唤醒与回写前校验；新轮重建客户端、状态与记录容器 |

轮次状态机：`idle → running → completed | partial | stopped`（停止过程中短暂为 `stopping`）。

## 交付到分析库（实现对照）

| 需求 | 实现 |
| --- | --- |
| 已结束的 `completed`/`partial` 轮次固定为一个批次 | `POST /api/deliveries` 调 `DeliveryManager.freeze`；其他状态返回 `409`，未采集过返回 `404` |
| 批次含成功公告和未完成页，partial 标为不完整 | 批次内容含 `records`、`unfinished_pages`、`status`、`complete`（`complete == (status == "completed")`） |
| 重复点击沿用该轮原批次及内容 | 批次 ID 为 `B-<round_id>`，采集端 SQLite 中 `round_id` 唯一；冻结后 payload 永不重写 |
| 重试与重启沿用原批次 | 交付状态与冻结内容持久化在 `/data/collector.db`，重启后从库读取同一 payload |
| 开始新采集不能改变待交付数据 | 冻结时深拷贝为独立 JSON 存入 SQLite，与爬虫内存对象完全解耦；新轮只有新 `round_id` |
| 接收端整批公告 + 完整性标记 + 回执一起入库 | `POST /api/batches` 在单个 SQLite 事务中写入 `payload`、`complete`、`receipt` |
| 同批重传返回原回执，不重复写入 | 按 `batch_id` 主键命中且 `content_hash` 相同 → `200` + 原回执；`batch_id` 相同但内容不同 → `409` 且不写入 |
| 接收端提交后丢响应（开关） | 接收端 `/` 页或 `POST /api/admin/drop-response` 开启；提交先提交事务，再返回“头已发、体未发”的响应，连接被关闭 |
| 采集端确认前退出 / 丢响应后重启自动续办 | 启动时 `DeliveryManager.start_resume_loop()` 扫描 `pending` 批次并重试；重试会对每个批次 POST 后 `GET /api/batches/{id}` 核实已入库回执 |
| 核实回执后才显示“已交付” | `deliver()` 只有在回执 `batch_id`/`content_hash` 与本地冻结批次完全一致时才置 `delivered`；否则保持 `pending` |
| 网络失败保持待确认并可重试 | `httpx` 错误 → `pending` + `last_error`；UI“重试交付”按钮与后台续办循环均可再次尝试 |
| 网页可交付、重试、查批次状态及接收明细 | 采集台“交付批次”表格（状态/回执/错误/重试/核实/明细/批次 JSON）；接收端首页列出已接收批次与回执，`/batches/{id}` 展示整批公告、未完成页、完整性与回执 |

交付状态机：`pending → delivered`；接收端 `409` 冲突则为终态 `rejected`。

### 批次内容与哈希

批次内容（采集端与接收端必须逐字段一致，否则哈希不一致会被拒收）：

```json
{
  "batch_id": "B-<round_id>",
  "round_id": "...",
  "status": "completed | partial",
  "complete": true,
  "base_url": "...",
  "started_at": "...",
  "finished_at": "...",
  "records_count": N,
  "unfinished_count": M,
  "records": [ ... ],
  "unfinished_pages": [ ... ]
}
```

`content_hash` = SHA-256(按 key 排序、无空格、UTF-8 的 JSON)，由采集端计算并随批次
发送；接收端用同一规则复算校验。回执由接收端在首次入库时生成
（`receipt_id`、批次 ID、轮次 ID、`content_hash`、完整性标记、公告数、未完成页数、入库时间），
之后重放始终返回这份原始回执。

### 采集端批次 API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/deliveries` | 冻结当前已结束轮次并尝试交付（幂等） |
| `GET` | `/api/deliveries` | 批次列表（状态、尝试次数、错误、回执） |
| `GET` | `/api/deliveries/pending` | 待确认批次 ID 列表 |
| `GET` | `/api/deliveries/{batch_id}` | 单批次状态与回执 |
| `GET` | `/api/deliveries/{batch_id}/content` | 下载该批次冻结内容 JSON |
| `POST` | `/api/deliveries/{batch_id}/retry` | 手动重试（`rejected` 批次返回 `409`） |
| `POST` | `/api/deliveries/{batch_id}/refresh` | 只核实接收端已入库回执，不重复提交 |

### 接收端 API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/api/batches` | 接收整批；`201` 首次入库，`200` 原回执重放，`409` 同号异内容 |
| `GET` | `/api/batches` | 已接收批次摘要 |
| `GET` | `/api/batches/{batch_id}` | 整批内容、完整性标记与回执 |
| `GET/POST` | `/api/admin/drop-response` | 读取/切换“提交后断开响应”开关 |

## 模拟采购站说明

- 数据集：36 条招标公告（6 个列表页）+ 24 条中标公告（4 个列表页），
  中标公告复用前 24 个招标编号，用于验证跨栏目去重；
- `ZB-2026-010` 详情页故意缺少“采购单位/截止日期”，用于验证三次解析失败后
  进入未完成页面列表、整轮标记为 `partial`；
- 限流为滑动窗口，默认 10 秒窗口 12 个请求，超限返回 `429` 与整数 `Retry-After: 5`；
- `/admin` 可运行时切换限流开关、窗口容量、窗口秒数与每请求延迟毫秒，
  保存即重置计数，也可单独重置计数。

## 推荐演示流程

1. `docker compose up --build`，打开 http://localhost:8000/ ；
2. 直接点“启动一轮采集”：观察到 `partial`——35 条记录、1 个坏详情页在未完成列表中；
3. 点“交付本轮到分析库”：批次状态变为“已交付”，回执号出现在采集台表格中；
   打开 http://localhost:8090/ 可见整批公告、未完成页与“不完整”标记；
4. 再点一次“交付”：批次 ID、内容与回执号不变，接收端仍只有一条记录（不重复入库）；
5. 在 `/site/admin` 把窗口容量调到 3：再启动一轮，观察控制台“全站冷却剩余”倒计时，
   冷却期间两个栏目均无新请求完成；
6. 冷却进行中点“停止”：状态在 1 秒内变为 `stopped`，排队/冷却中的页面进入未完成列表；
7. 立即再启动一轮：状态与记录完全重置，证明旧轮响应不会写入新轮；
   此前已交付批次的 `content_hash` 与回执在刷新后保持不变；
8. 故障演练：在接收端首页勾选“提交后断开响应”→ 采集台交付新一轮，
   刷新后状态仍为“已交付”，因为采集端核实了已入库回执；
   若同时停掉接收端再交付，则状态为“待确认”，重启接收端后无需点击即自动续办；
9. 点“下载 JSON”仍可获取原始轮次结构化结果（含未完成页清单）。

## 本地测试（无需 Docker）

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r collector/requirements.txt -r receiver/requirements.txt
.venv/bin/python -I tests/test_parser.py
.venv/bin/python -I tests/test_crawler_contract.py
.venv/bin/python -I tests/test_receiver_store.py
.venv/bin/python -I tests/test_receiver_api.py
.venv/bin/python -I tests/test_delivery_contract.py
.venv/bin/python -I tests/test_delivery_api.py
```

- `test_crawler_contract.py` 用内存假上游验证四件事：在途请求峰值恰为 2、429 后整站
  等待不少于 Retry-After 且重试可恢复、冷却中停止后零请求泄漏、停止后新轮干净无旧数据。
- `test_receiver_store.py` 验证首次入库/同批重放/同号异内容 409/partial 不完整标记/
  重开数据库后回执不变。
- `test_receiver_api.py` 验证接收端 HTTP 契约：201/200/409、哈希与计数校验、
  完整性标记矛盾 400、丢响应开关切换、列表回执。
- `test_delivery_contract.py` 用真实接收端验证冻结幂等与不可变、交付前核实回执、
  重放不重复、网络失败待确认并在重启后自动续办、丢响应被核实而非重复入库。
- `test_delivery_api.py` 以 compose 相同方式启动采购站、采集端、接收端，走完整 HTTP
  浏览器流程（交付/重复点击/新采集不影响旧批次/丢响应/重启续办）。
