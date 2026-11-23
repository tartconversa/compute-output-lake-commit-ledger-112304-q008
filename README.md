# 计算输出到可分析分区的提交账本与重试恢复

本仓库把本地合成的科研计算记录整理成可审计的数据湖分区：登记作业尝试与输出对象，
按幂等键接收完成回执，由作业负责人确认重跑版本，按提交完整性把分区标记为可见，
并支持提交恢复、引用查询和未完成输出的清理规划。数据目录完全为合成数据，
不包含真实实验室、研究人员或实验结果。仅使用 Python 3.11–3.14 与标准库，无外部服务。

```sh
python3 -m unittest discover -s tests -v
```

Windows PowerShell 使用 `python -m unittest discover -s tests -v`。

## 数据资料

| 文件 | 作用 |
| --- | --- |
| `data/jobs.csv` | 作业尝试、逻辑任务、代码版本、输入快照、参数哈希与任务状态 |
| `data/output_objects.csv` | 每次尝试写出的对象、大小、校验和与写入状态 |
| `data/completion_receipts.csv` | 成功/失败/重试通知及幂等键 |
| `data/lake_partitions.csv` | 存储侧分区快照（提交状态、清单摘要、消费方计数） |
| `data/artifact_manifests.csv` | 产物清单（内容校验和、采集时间） |
| `data/object_parts.csv` | 分片记录，按对象键数字后缀与输出对象对应 |
| `data/result_refs.csv` | 下游结果引用（有效、待复核、已撤回） |
| `data/parser_runs.csv` | 解析运行记录（合成资料中的附加背景） |
| `docs/资料契约.md` | 字段定义 |
| `docs/运行说明.md` | 中文操作说明与运维流程 |

### 资料中的对齐问题

三张执行表不是按 `(job_id, attempt)` 直接对齐的：`output_objects.csv` 与
`completion_receipts.csv` 的 `job_id` 标签比真正产生该记录的作业大 1，`attempt`
列与作业一致，记录时间与作业 `finished_at` 同一天（输出早 1 小时、回执晚 1 小时）。
装载器因此以 `(attempt, 日期)` 加时间距离做确定性匹配，并把整体标签偏移登记为一条
`labels_reconciled_by_time` 异常，而不是逐行制造噪声。下游看到的每条提交凭证都指向
真实的作业尝试。

## 业务规则

* **提交完整性**：只有任务状态为 `complete`、全部输出 `written`、存在成功回执，
  且实验版本已由负责人确认的尝试才可提交。`partial`/`pending` 输出、失败或缺失回执、
  待确认的重跑都阻止提交，并给出具体原因码。
* **版本确认**：不同指纹（代码、输入快照、参数）的重跑默认是“待确认重跑”，
  作业负责人调用 `confirm_version` 声明它是否构成新实验版本；判定为非新版本的
  尝试保持不可见，判定为新版本后才可提交。
* **可见性**：分区初始为 `staging`；提交后为 `committed` 且携带凭证。
  同一实验版本只保留最新尝试可见，旧尝试变为 `superseded`（历史保留）。
* **幂等与迟到通知**：幂等键在 `(job_id, attempt)` 作用域内生效；重复通知只留档，
  不生成第二个分区。迟到的失败通知不能撤销已成功的版本；失败之后到达的成功通知
  视为该尝试最终完成。
* **提交恢复**：`recover` 依据存储快照重放。记录为 `committed` 但实际不完整的
  分区被隔离；有更新且合格的同版本分区时旧分区被标记为被取代；记录为 `staging`
  但已合格的尝试可自动提升；快照缺失但账本中已合格的尝试可补建分区。重复执行
  结果稳定。
* **引用保护**：有效引用保护完整对象；有效引用指向不完整对象时保持待复核而不删除；
  待复核引用同样不删除。清理只删除未完成且无有效引用的对象，完整但未提交的对象
  进入隔离而非删除。
* **未完成输出**：只规划，不执行文件删除；`apply_cleanup(confirm=False)` 为演练，
  `confirm=True` 只移除账本登记。

## 代码结构

```
src/compute_output_lake_commit_ledger/
├── models.py   # 领域模型、枚举、异常
├── ledger.py   # 登记、幂等通知、版本、完整性、提交/恢复、可见性、引用、清理
├── loader.py   # 合成 CSV 的对齐装载与引用解析
└── cli.py      # 命令行入口
```

## 快速开始

```sh
PYTHONPATH=src python3 -m compute_output_lake_commit_ledger.cli inspect
PYTHONPATH=src python3 -m compute_output_lake_commit_ledger.cli recover
PYTHONPATH=src python3 -m compute_output_lake_commit_ledger.cli snapshot --run RUN-021
PYTHONPATH=src python3 -m compute_output_lake_commit_ledger.cli cleanup --run RUN-021
```

更完整的流程、输出字段说明与运维注意事项见 `docs/运行说明.md`。
