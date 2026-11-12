# 订单到清结算差异处理中心

跨境电商财务的订单 → 支付 → 退款/拒付 → 渠道结算 → 银行入账全链路差异处置系统。
FastAPI + PostgreSQL（四层数据架构）+ Vue 3，解决重复记账、乱序状态破坏、手续费/汇率后补、
合并拆分入账、关闭周期被静默改写等问题。

## 一、架构与关键约束

| 层 | 表 | 职责 |
|---|---|---|
| L1 原始事件层 | `raw_events` | CDC/webhook/轮询/人工消息原样留存，`(source, idempotency_key)` 去重 |
| L2 标准业务账本层 | `business_events` `ledger_entries` `fx_rates` `order_states` `payment_states` | 归一化业务事件、双分录三币种账本、历史汇率版本、按业务时间重放的状态投影 |
| L3 结算匹配层 | `settlement_batches/lines` `bank_transactions` `settlement_matches` `discrepancies(+actions)` | 两层匹配、五类差异、人工处置工作流 |
| L4 月度汇总层 | `monthly_reports` `monthly_report_versions` | 不可变快照与版本链，更正只追加新版本 |
| 支付状态机（特性 04） | `event_decisions` `unlinked_events` `state_conflicts` `replay_runs` | 接受/拒绝原因留痕、待关联队列、冲突双记录待裁决、隔离影子重放（不回写） |

关键机制：
- **两层幂等**：L1 来源消息去重 + L2 `business_key` 去重，CDC 重放/重复 webhook/轮询补拉均不重复进账本。
- **乱序安全**：状态投影按业务发生时间 `event_time` 重放，迟到的旧事件不能回退订单/支付状态。
- **支付状态机视图**：按订单展示授权/捕获/退款/拒付/反转及 webhook/轮询事件的**接受或拒绝原因**；无法关联的事件进待关联队列并可用原始报文隔离重放；同业务时间互斥事件**保留两条记录并标记待裁决**，不按到达顺序覆盖状态。
- **隔离影子重放**：重放只生成影子状态（`replay_runs` + `SHADOW` 决策）供生产态对比，`write_back` 恒为 false，绝不回写生产账本。
- **三币种留痕**：每笔金额保留交易币、结算币、记账币，以及汇率来源、适用时点、精度规则和原始金额。
- **只追加账本**：8 张证据表由 PostgreSQL 触发器拒绝 UPDATE/DELETE/TRUNCATE（详见 `db/DATA_LAYER.md`）。
- **自动任务只提示**：对账引擎只生成 `PROPOSED` 匹配与 `OPEN` 差异；`UNKNOWN` 未知差必须人工调账留痕后才能关闭。
- **关闭后更正**：结算修订、汇率更正、补单一律追加更正账与新报表版本，旧快照标记 `SUPERSEDED` 但原样保留。

## 二、一键启动（Docker Compose，推荐）

前置：Docker + Docker Compose。

```bash
cd /workspace/04
docker compose up -d --build          # 启动 db / backend / frontend
docker compose run --rm seed          # 装载 8 类模拟场景并跑完整处置/更正/版本链
```

访问：
- 前端（财务处置队列、批次对账、事件账本、支付状态机、报表版本）：http://localhost:8080
  - 支付状态机入口：左侧菜单「支付状态机」（路由 `/state-machine`），含按订单状态机视图、待关联队列、冲突待裁决、隔离重放记录四个子页。
- 后端 API：http://localhost:8000 ，健康检查 http://localhost:8000/health

停止与清库：
```bash
docker compose down            # 停止
docker compose down -v         # 停止并删除数据库卷（彻底重置）
```

## 三、本地开发启动（无 Docker 时）

需要 Python 3.11+、Node 20+、PostgreSQL 14+。

### 3.1 准备数据库
```bash
createdb recon   # 或在已有实例中建库，账号密码按需调整
```

### 3.2 后端
```bash
cd backend
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export RECON_DATABASE_URL="postgresql+asyncpg://<user>:<pwd>@<host>:5432/recon"
export PYTHONPATH="$(pwd)"
uvicorn app.main:app --reload --port 8000     # 启动时自动建表并安装只追加触发器
python scripts/seed.py --reset                 # 可选：装载模拟场景数据
```

### 3.3 前端
```bash
cd frontend
npm install
VITE_API_TARGET=http://localhost:8000 npm run dev    # http://localhost:5173，/api 自动代理
```

前端生产构建：`npm run build`（产物在 `frontend/dist`，容器内由 Nginx 托管并反代后端）。

## 四、测试账号 / 操作身份

系统为内部财务工具，处置动作以**操作人工号**标识（无独立登录体系）：
- 演示与种子脚本统一使用操作人：`finance.chen`
- 前端处置弹窗的“处理人”字段默认填充 `finance.chen`，可改为实际工号；所有认领、调账、关闭动作都带操作人留痕。
- 数据库连接（Compose）：用户 `recon` / 密码 `recon` / 库 `recon`。

## 五、验证方式

### 5.1 自动化验收（推荐）
```bash
cd backend && source .venv/bin/activate
export RECON_DATABASE_URL="postgresql+asyncpg://recon:recon@<host>:5432/recon"
bash scripts/verify.sh        # = pytest 全套 + seed 端到端
# 或分步：
python -m pytest tests/ -q    # 41 个用例
python scripts/seed.py --reset
```

测试覆盖（`backend/tests/`）：
| 文件 | 验收点 |
|---|---|
| `test_01_idempotency.py` | CDC 重放、重复 webhook、轮询补拉不重复进账本 |
| `test_02_ordering.py` | webhook 乱序下支付状态正确、旧订单状态迟到不回退 |
| `test_03_lifecycle.py` | 部分退款、拒付反转、账本借贷守恒、三币种与汇率溯源 |
| `test_04_reconciliation.py` | 两层匹配、五类差异归因、对账幂等、未知差禁止自动关闭、合并入账分配 |
| `test_05_workflow.py` | 认领/附件/调账建议/过账/关闭轨迹、关闭前置校验、快照生成 |
| `test_06_corrections.py` | 结算修订新版本、汇率更正账、报表版本链、历史快照不被改写、关闭后补单 |
| `test_07_append_only.py` | UPDATE/DELETE/TRUNCATE 触发器、证据列不可改、生命周期列可改 |
| `test_08_state_machine.py` | 接受/拒绝原因留痕、待关联入队不进账本、同业务时间冲突双记录待裁决、乱序按业务时间归位、隔离影子重放不回写、人工关联/裁决、状态机证据表只追加 |

### 5.2 模拟场景（`backend/app/demo/events.py`，结算周期 2026-10）
S1 CDC 重放 · S2 webhook 乱序与状态回退防护 · S3 支付后延迟入账（11-15 才到银行）·
S4 部分退款（ORD-005 退 40/100）· S5 拒付反转（ORD-006）· S6 结算单修订 v1→v2（手续费后补）·
S7 汇率更正 USD/CNY 7.20→7.25 · S8 多笔合并银行入账；另含金额差（ORD-007）、汇率差（ORD-009 EUR）、
幽灵结算行与银行残差两类未知差、关闭周期补单。
S9 支付状态机专项（`state_machine_events()`）：2 条无法关联事件进待关联队列、已捕获后再授权/退款超额 2 类拒绝、
ORD-010 同业务时间「捕获 ⟷ 失败」冲突双记录待裁决、ORD-011 多来源（POLL+WEBHOOK）接受。

### 5.3 本环境实际执行过的验证（结论如实记录）
- `python -m pytest tests/ -q`：**41 passed**（原 27 + 状态机 14；用户态 PostgreSQL 15，端口 5433）。
- `python scripts/seed.py --reset`：端到端跑通，42 条原始消息中 2 条重复被丢弃、生成 34 个业务事件；
  状态机决策分布 `ACCEPTED 21 / CONFLICT 2 / UNLINKED 2 / REJECTED 2 / DUPLICATE 1`，
  待关联队列 2 条、待裁决冲突 1 条；v1/v2 批次差异全部人工处置关闭，最终 4 个报表版本
  （v1~v3 SUPERSEDED、v4 CURRENT），全账本借贷各 18,197.01 CNY 平衡；只追加触发器成功拦截 UPDATE/DELETE；
  影子重放 `write_back` 恒为 false。
- API 冒烟（uvicorn :8011）：`/health`、`/api/state-machine/orders`（含仅支付态的 ORD-007/008/009）、
  `/orders/ORD-010`（冲突两条 CONFLICT 留痕、生产态不被覆盖）、`/unlinked`、`/conflicts`，
  以及 POST `replay/order`（MATCHED, write_back=false）、`unlinked/{id}/replay`（BLOCKED 带拒绝原因）、
  `conflicts/{id}/resolve`（选胜者裁决、败者留 REJECTED 记录保留），均验证通过。
- 前端 `npm run build` 构建成功（91 个模块，含新增「支付状态机」视图与四个子页）。
- 说明：本环境无 Docker 守护进程，**未实际执行镜像构建**，Dockerfile/compose 已按标准编写并通过 YAML 校验，
  请在具备 Docker 的环境按第二节验证。

## 六、主要 API（FastAPI，OpenAPI 见 `/docs`）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/events/ingest` `/api/events/ingest-batch` | 原始事件摄入（两层幂等） |
| POST | `/api/fx/rates` | 汇率发布/更正（自动版本号） |
| GET | `/api/events/raw` `/api/events/business` | L1/L2 事件查询 |
| GET | `/api/ledger/entries` `/api/ledger/balances` | 分录与守恒视图 |
| GET | `/api/states/orders` `/api/states/payments` | 状态投影 |
| POST | `/api/reconcile/run-all` | 自动对账（只生成建议） |
| GET/POST | `/api/batches`、`/api/batches/{id}/reconcile|close|snapshot` | 批次对账与关闭 |
| GET/POST | `/api/matches`、`/api/matches/{id}/confirm` | 两层匹配与人工确认 |
| GET/POST | `/api/discrepancies`、`/{id}/claim|attach|adjust-suggest|adjust-post|close` | 差异处置队列 |
| GET | `/api/reports`、`/api/reports/{id}/versions` | 月度报表与版本快照 |
| POST | `/api/reports/regenerate` | 关闭周期补单后重算新版本 |
| GET | `/api/state-machine/orders`、`/orders/{order_ref}` | 支付状态机视图（按订单的接受/拒绝时间线） |
| GET | `/api/state-machine/decisions` | 状态机决策留痕（可按订单/支付/结论过滤） |
| GET/POST | `/api/state-machine/unlinked`、`/{id}/link\|discard\|replay` | 待关联队列：人工关联、丢弃、隔离重放 |
| GET/POST | `/api/state-machine/conflicts`、`/{id}/resolve\|discard` | 同业务时间冲突的待裁决与人工裁决 |
| POST | `/api/state-machine/replay/order/{order_ref}`、`/replay/raw/{id}` | 隔离 SHADOW 影子重放（不回写） |
| GET | `/api/state-machine/replays`、`/replays/{id}` | 重放运行记录与影子步骤对比 |

## 七、目录结构

```
.
├── backend/
│   ├── app/
│   │   ├── models.py            # 四层表结构与只追加清单
│   │   ├── enums.py             # 事件/状态/差异/科目枚举
│   │   ├── init_db.py           # 建表 + 只追加触发器
│   │   ├── services/
│   │   │   ├── ledger.py        # 幂等摄入、双分录、状态投影（含状态机闸门接入）
│   │   │   ├── statemachine.py  # 支付状态机：接受/拒绝、待关联、冲突裁决、隔离影子重放
│   │   │   ├── fx.py            # 历史汇率时点取值
│   │   │   ├── reconciliation.py# 两层匹配与五类差异引擎
│   │   │   ├── workflow.py      # 人工处置与批次关闭
│   │   │   └── corrections.py   # 更正账与报表版本
│   │   ├── routers/             # events / reconciliation / state-machine API
│   │   └── demo/                # 9 类模拟事件（含状态机专项）与场景装载器
│   ├── tests/                   # 41 个 pytest 验收用例（含 test_08 状态机 14 个）
│   └── scripts/seed.py verify.sh
├── frontend/                    # Vue 3 + Vite + vue-router + axios（含 StateMachineView）
├── db/DATA_LAYER.md             # 数据层与触发器设计说明
└── docker-compose.yml           # db + backend + frontend(+seed profile)
```
