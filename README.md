# 批采编排台（batchsched）

**一句话定位：** 把一批采集作业按依赖关系、并发上限和重试策略编排成可执行计划，在本地执行、断点续跑，并留下可审计的执行台账。

`batchsched` 是一个**离线运行**的批量采集作业 DAG 编排与调度引擎。它读取 YAML 作业计划，计算拓扑执行前沿，在线程池中按并发上限派发作业，把 mock 数据源响应落盘，通过 SQLite 记录每次尝试和最终结果；进程中断后可以从台账恢复，已完成作业不重跑。

## 与同类项目的区别

| 已有项目 | 关注点 | 本项目 `batchsched` 的差异 |
| --- | --- | --- |
| `crawl-flow` | 单个数据源的列表→详情→解析→入库管道 | 本项目面向**多作业 DAG 编排与调度执行**；不做字段解析归一、不是单源翻页抓取器 |
| `fillbot` | 表单填报写入 RPA | 本项目只编排**读取型采集作业**，不执行页面写入 |
| `linkpatrol` | 站点链接健康巡检 | 本项目不做站点巡检，关注作业级依赖、并发、重试与续跑 |
| `pagewatch` | 单页/多页内容变更监测 | 本项目不比较内容是否变化，只负责批量采集作业的执行秩序 |
| `档采匠` | 离线处理静态 HTML | 本项目从内置的**本机 mock HTTP 站点**取数，重点不在解析，而在调度编排 |

一句话：它们回答“采集什么、怎么解析”，本项目回答“什么时候、按什么顺序、失败怎么办、中断了怎么续”。

## 快速开始

环境：Python 3.10+。依赖：Flask、requests、PyYAML、pytest。所有依赖都可以离线安装。

```bash
# 1. 启动内置 mock 数据源 + 只读 API（127.0.0.1:5600）
python -m batchsched serve

# 2. 执行演示计划（另开终端；run 也会在端口未占用时自动拉起本地 mock）
python -m batchsched run --plan plans/demo.yaml

# 3. 跑全部测试（测试只使用临时目录和本机端口）
pytest -q
```

校验计划：

```bash
python -m batchsched plan-validate plans/demo.yaml
python -m batchsched plan-validate plans/demo_invalid_cycle.yaml   # 返回非 0，并给出环所在行
```

查看台账与报告：

```bash
python -m batchsched report --run <run_id> --format json
python -m batchsched report --run <run_id> --format md
```

只读 API：

- `GET http://127.0.0.1:5600/api/runs`
- `GET http://127.0.0.1:5600/api/runs/<run_id>`

## YAML 计划样例

```yaml
name: demo-collection
concurrency: 3
jobs:
  - id: fetch_catalog
    source: /api/catalog
    depends_on: []
    retry:
      mode: exponential
      max_attempts: 3
      base_delay_ms: 50
    params:
      note: local fixture

  - id: summarize
    source: /api/summary
    depends_on: [fetch_catalog]
    retry:
      mode: fixed
      max_attempts: 2
      base_delay_ms: 100
    params:
      expect_content_type: application/json
      min_bytes: 10
      required_fields: [dataset]
      min_records: 1
```

字段规则：

- `id` 必须是 1–64 位字母、数字、`_` 或 `-`，以字母或数字开头，同一计划内唯一。
- `source` 必须是以 `/` 开头的本地路径，不能是完整 URL、不能含 `://`、`..` 或空白。
- `depends_on` 中的作业必须存在，且不能自依赖或重复。
- 计划不能有环；校验错误带 YAML 行号。
- 未知的顶层字段、作业字段和 retry 字段都会被拒绝。
- 作业数上限 200；并发上限 8；重试次数 1–5；退避基数 0–10000 ms；单作业累计退避不超过 10 s。
- `retryable` 可以显式覆盖默认可重试错误集合。
- `params.required_fields`（非空字段名列表）与 `params.min_records`（非负整数）是**上游产物契约**：声明后，作业在发起任何 HTTP 前会逐一校验所有直接上游的落盘产物。

## 执行语义

1. `plan-validate` 只读 YAML，不创建数据库。
2. `run` 创建唯一 `run_id`，按 `min(计划 concurrency, 全局上界 8)` 建线程池。
3. 作业只有在所有依赖均为 `done` 时才进入执行前沿；依赖 `failed`/`skipped` 的作业直接标记 `skipped`，原因固定为 `upstream_failed`，并继续向下传导。
4. 声明了上游产物契约的作业，在抓取前对**每个**直接上游的 `work/output/runs/<run_id>/<job_id>.raw` 做结构化校验：产物必须存在、非空、是合法 JSON；`required_fields` 中的字段必须全部出现在顶层对象；`min_records` 要求记录数（顶层数组长度，或对象内最长顶层数组）达标。所有上游逐一校验并聚合失败原因，任一不满足即以 `parse_precondition` 记录一次 attempt 与 job_result、写 `upstream_contract_blocked` 审计，**不重试、不发起抓取**。上游为 `failed`/`skipped` 时不进入该校验，仍按 `upstream_failed` 传导。
5. 单个作业内部的重试串行执行；不同作业可以并行。
6. 成功后原始响应写入 `work/output/runs/<run_id>/<job_id>.raw`，并记录 SHA-256、字节数和相对输出路径。
7. 每次尝试写入 `attempts`；每个作业在同一个 run 下只有一条 `job_results`（主键 `(run_id, job_id)`）。
8. run 的最终状态为 `completed`（全部 done）、`failed`（存在 failed/skipped）或 `interrupted`（收到中断）。
9. `run` 在 5600 端口不可用时会自动启动内置 mock 站点，结束后停止它；如果 `serve` 已在运行则复用，不会重复启动。

## 错误分类

| 错误类 | 触发示例 | 默认重试 |
| --- | --- | --- |
| `network` | 连接失败、目标被白名单拒绝、未知传输异常 | 是 |
| `http_5xx` | mock 站点前两次返回 500 | 是 |
| `http_4xx` | 404、3xx 重定向被拒绝 | 否 |
| `too_large` | 响应超过字节上限 | 否 |
| `timeout` | 连接或读取超时 | 是 |
| `empty_body` | 200 但响应体为空 | 是 |
| `parse_precondition` | Content-Type / min_bytes 不满足、上游产物契约不满足，或输出写入失败 | 否 |

`retryable` 可覆盖默认集合。每个失败 attempt 都会记录错误类、耗时和本次退避时长；退避采用 `fixed` 或 `exponential`，并受累计上限保护。

## 断点续跑语义

```bash
python -m batchsched resume --run <run_id>
```

- 读取同一 `run_id` 的台账，**不会创建新 run**。
- 已 `done` 的作业不重新执行、不重写输出文件，SHA-256 保持原值。
- 已 `failed` 的作业保持失败；其下游保持/补记 `skipped`，不会在 resume 时自动重试失败作业（保守续跑）。
- 尚未执行且依赖满足的作业进入前沿继续执行，attempt 序号在已有记录之后递增。
- 每个作业仍只有一条 `job_results` 记录，不会因续跑产生重复结果。
- 台账追加 `audit` 事件 `resumed`。
- 中断后输出 SHA-256 与不中断执行一致（测试覆盖）。

## 台账结构（SQLite）

数据库默认位于 `work/ledger.sqlite3`。所有 SQL 集中在 `batchsched/ledger.py`。

| 表 | 关键列 | 说明 |
| --- | --- | --- |
| `runs` | `run_id`, `plan_path`, `started_at`, `finished_at`, `status`, `summary_json` | 一次调度 run |
| `attempts` | `run_id`, `job_id`, `attempt_no`, `error_class`, `duration_ms`, `backoff_ms`, `started_at` | 每次尝试；主键阻止重复序号 |
| `job_results` | `run_id`, `job_id`, `status`, `sha256`, `size_bytes`, `output_path`, `reason` | 每个作业的终态；主键保证唯一 |
| `audit` | `run_id`, `event`, `detail`, `at` | 拒绝、重试、续跑、完成等事件 |

## 报告

- JSON 汇报：run 状态、done/failed/skipped 计数、总 attempt 数、累计 attempt 耗时、失败分类计数、输出文件列表、作业与 attempt 明细。
- `upstream_contract_blocked`：被上游产物契约阻断的作业数（JSON 字段与 Markdown 摘要行均有），与 `failure_class_counts` 中的 `parse_precondition` 计数互相印证。
- Markdown 复盘：按状态分组的作业表、失败作业的错误分类与 attempt 明细、续跑历史、审计事件摘要。
- 报告中的路径都转换为相对路径（典型为 `work/output/runs/<run_id>/...`），计划路径若是绝对路径会折叠为文件名或相对路径。
- 报告对固定台账内容可按相同顺序复现；唯一变化来源是 run 自身记录的时间和耗时。

## 离线与合规声明

- 运行期只允许访问 `127.0.0.1`、`localhost`、`::1`；白名单在建立连接之前校验，并拒绝重定向、URL 内嵌凭据和非 HTTP(S) scheme。
- 非白名单目标被拒绝时，运行期通过 `audit_hook` 写入 `audit` 表的 `fetch_rejected` 事件。
- mock 站点只由本地 Flask 提供，不解析 DNS，不访问外部网络。
- fixture 中只出现保留域名 `demo.local` 和 `example.invalid` 作为数据文本，不是可解析目标。
- 项目不读取密钥、不调用外部命令、不使用 `subprocess`。
- 所有运行期写入都落在显式 `--work-dir`（默认当前目录）下的 `work/`。

## 测试说明

```bash
pytest -q
```

当前共有 **97 个测试项**，覆盖：

- 计划校验：重复 id、悬空依赖、自依赖、环、未知字段、非法 retry、非法契约声明、作业上限、YAML 语法与行号。
- DAG：拓扑分层、执行前沿、失败/skipped 传导、下游计算。
- 调度：并发峰值、显式并发上限、失败跳过、已有 done 不重跑、中断、worker 异常隔离。
- 抓取与执行：白名单拒绝且零 HTTP 调用、500/404/超时/空 body/超大响应分类、固定/指数退避、4xx 不重试、退避上限、输出 SHA-256。
- 上游产物契约：全上游逐一校验并聚合失败、首个满足不放行、缺失/空/非法 JSON 产物、声明非法时 fail-closed、不重试、零 HTTP 抓取、续跑时 failed/skipped 上游仍按 `upstream_failed` 传导、已 done 上游产物稳定可读。
- 台账与续跑：唯一约束、attempt 序号、resume 不重复 job_result、中断后 SHA-256 与完整执行一致、`resumed` 审计。
- API 与报告：只读端点、非回环拒绝、JSON/Markdown 结构、`upstream_contract_blocked` 计数、报告无绝对路径。
- CLI：计划退出码、run/report 闭环。

测试全部使用 `tmp_path` 和临时端口，不污染仓库运行目录。

## 不实现清单

- 分布式 worker、消息队列、远程执行、Kubernetes。
- 通用爬虫能力：反爬、Cookie、代理、浏览器渲染。
- 数据解析归一、字段映射、入库建模（下游项目职责）。
- Web 前端管理界面（只有最小只读 API 与 CLI）。
- 鉴权、多租户、权限模型。
- cron/定时触发（由外部调度器调用 `run` 即可）。
- 增量变更监测（属于 pagewatch 领域）。
- DAG 可视化。

## 项目结构

```text
batchsched/
  __main__.py          CLI 入口
  cli.py               serve / run / resume / report / plan-validate
  config.py            白名单、路径、上限常量
  models.py            JobSpec / PlanDag / Attempt / JobResult
  plan_loader.py       YAML 计划入口与结构化校验
  plan_schema.py       字段级校验与行号辅助
  dag.py               拓扑分层、前沿、跳过传导
  scheduler.py         并发派发与终态收敛
  executor.py          单作业抓取、落盘、重试
  fetcher.py           白名单 + 本机 HTTP 抓取
  ledger.py            SQLite 台账（唯一 SQL 层）
  retry.py             退避算术与上限
  resume.py            断点续跑
  reporter.py          JSON / Markdown 报告
  api.py               只读 Flask API
  mocksite/            本地 mock 站点与 fixture
plans/
  demo.yaml
  demo_invalid_cycle.yaml
tests/                 pytest 测试
```

## 依赖与离线安装

`pyproject.toml` 声明了运行依赖和可选测试依赖。若目标机器已安装相同版本，可直接运行；若需要离线安装，可在有网络的机器上准备本地 wheel 目录，然后执行：

```bash
pip install --no-index --find-links /path/to/wheels -e .[test]
```

直接依赖的参考锁定见 `requirements-lock.txt`。项目不下载模型、不使用网络资源，也不要求数据库服务。
