# 批采编排台（batchsched）交付说明

## 1. 交付内容

已在当前目录交付可离线运行的批量采集 DAG 编排引擎，包含：

- 完整 Python 源码（`batchsched/`）
- 演示与非法计划（`plans/`）
- 本机 mock 站点与 fixture（`batchsched/mocksite/`）
- 110 个 pytest 测试项（`tests/`）
- `pyproject.toml`、直接依赖参考锁定（`requirements-lock.txt`）
- README 与可视化验收依据
- 运行期生成目录 `work/`（SQLite 台账与输出文件，已被 `.gitignore` 忽略）

## 2. 行数统计

统计数据来自最终交付版本的 `wc -l`。项目规模按 Python 源码 + 测试计为 **4345 行**，落在要求的 2500–4500 行区间；单文件最大 **299 行**，满足 ≤300 行限制。

| 文件 | 行数 |
| --- | ---: |
| `batchsched/__init__.py` | 6 |
| `batchsched/__main__.py` | 6 |
| `batchsched/api.py` | 68 |
| `batchsched/cli.py` | 237 |
| `batchsched/config.py` | 122 |
| `batchsched/dag.py` | 114 |
| `batchsched/executor.py` | 196 |
| `batchsched/fetcher.py` | 162 |
| `batchsched/ledger.py` | 290 |
| `batchsched/mocksite/__init__.py` | 5 |
| `batchsched/mocksite/app.py` | 137 |
| `batchsched/mocksite/server.py` | 111 |
| `batchsched/models.py` | 206 |
| `batchsched/plan_loader.py` | 164 |
| `batchsched/plan_schema.py` | 299 |
| `batchsched/reporter.py` | 236 |
| `batchsched/resume.py` | 94 |
| `batchsched/retry.py` | 54 |
| `batchsched/scheduler.py` | 240 |
| `tests/conftest.py` | 159 |
| `tests/test_api.py` | 88 |
| `tests/test_cli.py` | 76 |
| `tests/test_dag.py` | 113 |
| `tests/test_executor.py` | 186 |
| `tests/test_fetcher.py` | 89 |
| `tests/test_ledger.py` | 137 |
| `tests/test_plan_loader.py` | 221 |
| `tests/test_reporter.py` | 146 |
| `tests/test_resume.py` | 190 |
| `tests/test_retry.py` | 66 |
| `tests/test_scheduler.py` | 127 |
| **Python 合计** | **4345** |

其他文件：

| 文件/目录 | 行数/说明 |
| --- | ---: |
| `plans/demo.yaml` | 59 |
| `plans/demo_params.yaml` | 24 |
| `plans/demo_invalid_cycle.yaml` | 19 |
| `batchsched/mocksite/fixtures/*.json` | 33 |
| `pyproject.toml` | 31 |
| `requirements-lock.txt` | 6 |
| `README.md` | 220 |
| `.gitignore` | 8 |

代码规模只计 `.py` 源码与测试，不含 README、DELIVERY、配置和 fixture。

## 3. 测试结果

```text
$ pytest -q
........................................................................ [ 91%]
.......                                                                  [100%]
110 passed in 2.99s
```

覆盖重点：

- 非法计划：环、悬空依赖、重复 id、自依赖、未知字段、非法 retry、作业上限、YAML 行号。
- DAG：拓扑分层、执行前沿、失败/skipped 传导。
- 调度：并发峰值、计划/全局并发上限、worker 异常隔离、结束状态。
- 执行：500 三次重试、404 不重试、超时、空 body、超大响应、Content-Type 前置条件、退避上限、SHA-256。
- 台账：主键约束、job_result 唯一、attempt 序号、审计顺序。
- 续跑：中断后 resume、已完成作业不重跑、输出 SHA-256 一致、无重复 job_result。
- 抓取：非白名单目标零 HTTP 调用并写入 `audit`。
- API / 报告 / CLI：只读结构、非回环拒绝、无绝对路径、退出码与闭环。

测试只使用 `tmp_path` 与临时本地端口，未污染仓库。

## 4. 三条关键命令的实测输出

### 4.1 计划校验

```text
$ python -m batchsched plan-validate plans/demo.yaml
{"concurrency": 3, "job_count": 6, "layers": [["fetch_catalog", "fetch_flaky"], ["fetch_orders", "fetch_slow"], ["enrich_orders"], ["summarize"]], "name": "demo-collection", "ok": true, "plan": "plans/demo.yaml"}
exit=0

$ python -m batchsched plan-validate plans/demo_invalid_cycle.yaml
{"error_count": 1, "errors": [{"code": "cycle", "job_id": "fetch_alpha", "line": 5, "message": "dependency cycle detected: fetch_alpha -> fetch_gamma -> fetch_beta -> fetch_alpha"}], "ok": false}
exit=2
```

### 4.2 执行演示计划

```text
$ python -m batchsched run --plan plans/demo.yaml
{"job_counts": {"done": 6, "failed": 0, "recorded_total": 6, "skipped": 0}, "ok": true, "outputs": ["work/output/runs/run-20261113T023626Z-1ce18f/enrich_orders.raw", "work/output/runs/run-20261113T023626Z-1ce18f/fetch_catalog.raw", "work/output/runs/run-20261113T023626Z-1ce18f/fetch_flaky.raw", "work/output/runs/run-20261113T023626Z-1ce18f/fetch_orders.raw", "work/output/runs/run-20261113T023626Z-1ce18f/fetch_slow.raw", "work/output/runs/run-20261113T023626Z-1ce18f/summarize.raw"], "run_id": "run-20261113T023626Z-1ce18f", "status": "completed", "total_attempts": 8}
exit=0
```

该 run 的 8 次 attempt 中，`fetch_flaky` 的前两次命中 mock 500，第三次成功，验证了重试闭环。

### 4.3 报告

```text
$ python -m batchsched report --run run-20261113T023626Z-1ce18f --format json
status= completed
counts= {'done': 6, 'failed': 0, 'recorded_total': 6, 'skipped': 0}
attempts= 8 failure_classes= {'http_5xx': 2}
outputs= 6 first= work/output/runs/run-20261113T023626Z-1ce18f/enrich_orders.raw

$ python -m batchsched report --run run-20261113T023626Z-1ce18f --format md
# Batch run report: run-20261113T023626Z-1ce18f

## Summary
- Plan: `plans/demo.yaml`
- Status: **completed**
- Jobs: done=6 failed=0 skipped=0 recorded=6
- Total attempts: 8
- Failure classes: http_5xx=2

## Jobs by status
...
## Output files
- `work/output/runs/run-20261113T023626Z-1ce18f/enrich_orders.raw`
...
```

报告路径检查结果为不含 `C:` 盘符或临时测试路径；输出路径均为
`work/output/runs/<run_id>/...` 相对形式。

### 4.4 只读 API

```text
$ python -m batchsched serve
... 本地监听 127.0.0.1:5600 ...

GET /api/runs
status=200 count=1 latest_status=completed plan=plans/demo.yaml

GET /api/runs/run-20261113T023626Z-1ce18f
status=200 jobs=6 attempts=8
```

API 的 `before_request` 检查还提供非回环客户端拒绝（403），测试已覆盖。

### 4.5 白名单拒绝与审计

```text
rejected_class= network
http_calls= 0
audit= [{'event': 'fetch_rejected', 'detail': 'host=example.invalid reason=host_not_allowlisted', 'at': '2026-11-13T02:36:43Z'}]
```

目标在建立连接前被拒绝，未发生 HTTP 调用，拒绝事件通过注入的 audit hook 写入 SQLite `audit` 表。

### 4.6 中断与续跑

- 通过 `_thread.interrupt_main()` 在主线程制造真实 `KeyboardInterrupt`：run 状态为 `interrupted`，已完成作业保持 `done`，未完成作业保持 `pending`。
- 使用同一调度器的 `stop_event` 路径做自动化中断测试，再用 CLI 执行 `resume`：
  - 中断时 `fetch_catalog=done`、其余 pending。
  - `python -m batchsched resume --run run-resume-acceptance` 返回 `completed`。
  - 最终 3 个作业全部 done，`total_attempts=3`，`resume_history=1`。
  - 已完成作业的 SHA-256 与不中断的基线 run 完全一致，`job_results` 每作业仍只有一条，attempt 不重复写入。

## 5. 与已有项目的差异对照

| 项目 | 原有职责 | 本项目边界 |
| --- | --- | --- |
| `crawl-flow` | 单数据源列表→详情→解析→入库 | 多作业 DAG 编排与执行，不解析归一 |
| `fillbot` | 表单填报写入 RPA | 采集读取作业的调度，不写入页面 |
| `linkpatrol` | 链接健康巡检 | 无关站巡检；管理作业依赖和终态 |
| `pagewatch` | 页面变更监测 | 不做内容差异检测，只执行采集秩 |
| `档采匠` | 离线静态 HTML 处理 | 从本机 mock HTTP 站点取数，重点在调度 |

## 6. 验收清单

- [x] 1. `python -m batchsched serve` 启动 mock 站点 + 只读 API；`127.0.0.1:5600/api/runs` 返回 200。
- [x] 2. `plan-validate plans/demo.yaml` 返回 0；非法环计划返回 2，输出 `cycle` 和行号 5。
- [x] 3. `run --plan plans/demo.yaml` 跑通；`work/ledger.sqlite3` 有 runs/attempts/job_results/audit，输出目录有原始响应文件。
- [x] 4. 中断后 `resume --run <run_id>` 可续跑；完成作业不重跑，输出 SHA-256 与基线一致；真实 KeyboardInterrupt 路径和 stop_event 自动化测试均通过。
- [x] 5. `report --format json|md` 均可输出；报告路径检查无绝对路径。
- [x] 6. 抓取层拒绝 `example.invalid`，HTTP 调用为 0，并写入 `fetch_rejected` 审计事件。
- [x] 7. `pytest -q`：110 passed。
- [x] 8. Python 代码 4345 行，在 2500–4500 内；单文件最大 299 行，≤300。
- [x] 9. 无真实密钥、公司名、真实目标域名；仅使用 `demo.local`、`example.invalid` 保留域名和回环地址。
- [x] 10. README 包含定位、差异、快速开始、YAML 样例、错误分类、续跑语义、台账结构、离线合规、测试说明、不实现清单。

## 7. 依赖与离线安装说明

`pyproject.toml` 声明直接依赖：

- Flask 3.x
- requests 2.x
- PyYAML 6.x
- pytest 8.x（可选 test extra）

实际测试使用的直接版本见 `requirements-lock.txt`。离线安装时准备本地 wheel 仓库后执行：

```bash
pip install --no-index --find-links /path/to/wheels -e .[test]
```

项目运行期不需要外部网络、数据库服务或大模型下载。

## 8. 已知边界（刻意不实现）

- 无分布式 worker / 消息队列 / 远程任务执行。
- 无通用爬虫能力（反爬、Cookie、代理、浏览器渲染、DNS 外网访问）。
- 无解析归一、字段映射、入库建模。
- 无 Web 管理前端、鉴权、多租户。
- 无 cron 触发、DAG 可视化、页面变更监测。

这些边界保证本项目聚焦于“批量采集作业的执行编排、并发、重试、台账、续跑与审计”。
