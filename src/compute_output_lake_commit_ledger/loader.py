"""从合成 CSV 资料装载提交账本。

资料中的三张执行表并非按 ``(job_id, attempt)`` 直接对齐：

* ``jobs.csv`` 的 ``attempt`` 是该作业当前的尝试序号；
* ``output_objects.csv`` 与 ``completion_receipts.csv`` 的 ``job_id`` 标签会
  比实际产生该记录的作业尝试大 1，且 ``attempt`` 列与作业尝试一致；
* 记录时间与作业 ``finished_at`` 同属一天（回执晚 1 小时，输出早 1 小时）。

因此装载器不以标签直接关联，而是用 ``(attempt, 日期)`` 与时间先后做确定
性匹配，并把标签偏移、幂等键复用、分区状态与账本事实冲突等登记为异常。
这样下游看到的提交凭证总是来自真实的作业尝试，而不是错位的标签。
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .ledger import LakeCommitLedger
from .models import (
    CompletionReceipt,
    JobAttempt,
    JobState,
    OutputObject,
    ReceiptType,
    RecordedPartition,
    ReferenceRecord,
    RecoveryReport,
    ReferenceState,
    ReferenceTarget,
    WriteState,
    parse_timestamp,
)

DEFAULT_MAX_LAG = timedelta(hours=6)


def _read(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as stream:
        return list(csv.DictReader(stream))


def _optional(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text or None


@dataclass
class JobIndex:
    """按 (尝试序号, 日期) 索引作业尝试，用于时间对齐。"""

    jobs: list[JobAttempt]

    def __post_init__(self) -> None:
        self._by_slot: dict[tuple[int, str], list[JobAttempt]] = {}
        for job in self.jobs:
            slot = (job.attempt, job.finished_at.date().isoformat())
            self._by_slot.setdefault(slot, []).append(job)
        self._sorted = sorted(self.jobs, key=lambda item: item.finished_at)

    def match(
        self, moment: datetime, attempt: int, max_lag: timedelta = DEFAULT_MAX_LAG
    ) -> JobAttempt | None:
        same_day = self._by_slot.get((int(attempt), moment.date().isoformat()), [])
        if same_day:
            # 同日同尝试出现多个候选时不猜测，交由异常清单暴露。
            if len(same_day) > 1:
                return None
            return same_day[0]
        candidates = [
            job
            for job in self._sorted
            if job.attempt == int(attempt) and abs(job.finished_at - moment) <= max_lag
        ]
        if len(candidates) != 1:
            return None
        return candidates[0]


@dataclass
class ReconcileReport:
    """装载时的对齐统计与异常清单。"""

    matched_outputs: int = 0
    unmatched_outputs: list[dict[str, Any]] = field(default_factory=list)
    matched_receipts: int = 0
    unmatched_receipts: list[dict[str, Any]] = field(default_factory=list)
    output_label_offsets: dict[str, int] = field(default_factory=dict)
    receipt_label_offsets: dict[str, int] = field(default_factory=dict)
    total_outputs: int = 0
    total_receipts: int = 0

    def note_output_offset(self, source: str, assigned: str) -> None:
        if source == assigned:
            return
        try:
            delta = int(source.split("-")[1]) - int(assigned.split("-")[1])
        except (IndexError, ValueError):
            delta = 0
        key = f"{delta:+d}"
        self.output_label_offsets[key] = self.output_label_offsets.get(key, 0) + 1

    def note_receipt_offset(self, source: str, assigned: str) -> None:
        if source == assigned:
            return
        try:
            delta = int(source.split("-")[1]) - int(assigned.split("-")[1])
        except (IndexError, ValueError):
            delta = 0
        key = f"{delta:+d}"
        self.receipt_label_offsets[key] = self.receipt_label_offsets.get(key, 0) + 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_outputs": self.total_outputs,
            "matched_outputs": self.matched_outputs,
            "unmatched_outputs": self.unmatched_outputs,
            "total_receipts": self.total_receipts,
            "matched_receipts": self.matched_receipts,
            "unmatched_receipts": self.unmatched_receipts,
            "output_label_offsets": self.output_label_offsets,
            "receipt_label_offsets": self.receipt_label_offsets,
        }


def load_data_dir(
    data_dir: Path | str,
    *,
    max_lag: timedelta = DEFAULT_MAX_LAG,
    ledger: LakeCommitLedger | None = None,
) -> LoadedDataset:
    """装载作业、输出、回执、分区、引用并做冲突登记。"""

    root = Path(data_dir)
    ledger = ledger or LakeCommitLedger()

    jobs: list[JobAttempt] = []
    for row in _read(root / "jobs.csv"):
        jobs.append(
            JobAttempt(
                job_id=row["job_id"],
                logical_run=row["logical_run"],
                code_version=row["code_version"],
                input_snapshot=row["input_snapshot"],
                parameter_hash=row["parameter_hash"],
                attempt=int(row["attempt"]),
                state=JobState(row["job_state"].strip().lower()),
                finished_at=row["finished_at"],
                resource_queue=_optional(row.get("resource_queue")),
                experiment_version=_optional(row.get("experiment_version")),
                recorded_state=row["job_state"],
            )
        )
    jobs.sort(key=lambda job: (job.logical_run, job.attempt))
    for job in jobs:
        ledger.register_job(job)

    index = JobIndex(jobs)
    report = ReconcileReport()

    for row in _read(root / "output_objects.csv"):
        report.total_outputs += 1
        moment = parse_timestamp(row["written_at"])
        owner = index.match(moment, int(row["attempt"]), max_lag=max_lag)
        if owner is None:
            report.unmatched_outputs.append(row)
            ledger.record_anomaly(
                "unmatched_output",
                "输出记录无法对应到任何作业尝试",
                {"row": row},
                key_suffix=row["object_key"],
            )
            continue
        report.matched_outputs += 1
        report.note_output_offset(row["job_id"], owner.job_id)
        ledger.register_output(
            OutputObject(
                job_id=owner.job_id,
                attempt=owner.attempt,
                object_key=row["object_key"],
                size_bytes=int(row["bytes"]),
                checksum=row["checksum"],
                write_state=WriteState(row["write_state"].strip().lower()),
                written_at=moment,
                source_job_id=row["job_id"],
                recorded_write_state=row["write_state"],
            ),
            note_label=False,
        )

    receipts: list[tuple[CompletionReceipt, dict[str, str]]] = []
    for row in _read(root / "completion_receipts.csv"):
        report.total_receipts += 1
        moment = parse_timestamp(row["received_at"])
        owner = index.match(moment, int(row["attempt"]), max_lag=max_lag)
        if owner is None:
            report.unmatched_receipts.append(row)
            ledger.record_anomaly(
                "unmatched_receipt",
                "完成回执无法对应到任何作业尝试",
                {"row": row},
                key_suffix=row["receipt_id"],
            )
            continue
        report.matched_receipts += 1
        report.note_receipt_offset(row["job_id"], owner.job_id)
        receipts.append(
            (
                CompletionReceipt(
                    receipt_id=row["receipt_id"],
                    job_id=owner.job_id,
                    attempt=owner.attempt,
                    receipt_type=ReceiptType(row["receipt_type"].strip().lower()),
                    received_at=moment,
                    idempotency_key=row["idempotency_key"],
                    source_job_id=row["job_id"],
                    recorded_receipt_type=row["receipt_type"],
                ),
                row,
            )
        )
    receipts.sort(key=lambda pair: (pair[0].received_at, pair[0].receipt_id))
    for receipt, _row in receipts:
        ledger.register_receipt(receipt)

    recorded: list[RecordedPartition] = []
    for row in _read(root / "lake_partitions.csv"):
        recorded.append(
            RecordedPartition(
                partition_id=row["partition_id"],
                logical_run=row["logical_run"],
                attempt=int(row["attempt"]),
                commit_state=row["commit_state"],
                committed_at=row["committed_at"],
                manifest_digest=row["manifest_digest"],
                consumer_refs=int(row["consumer_refs"]),
            )
        )

    references = load_references(root)
    for reference in references:
        ledger.register_reference(reference)

    shifted_labels = any(
        int(value) != 0 for value in report.output_label_offsets if value.startswith("+")
    ) or any(
        int(value) != 0 for value in report.receipt_label_offsets if value.startswith("+")
    )
    if shifted_labels:
        ledger.record_anomaly(
            "labels_reconciled_by_time",
            "输出与回执的作业标签整体偏移，已按尝试序号与记录时间归属",
            {
                "output_label_offsets": dict(report.output_label_offsets),
                "receipt_label_offsets": dict(report.receipt_label_offsets),
                "matched_outputs": report.matched_outputs,
                "matched_receipts": report.matched_receipts,
            },
            key_suffix="global",
        )

    return LoadedDataset(
        ledger=ledger,
        jobs=tuple(jobs),
        recorded_partitions=tuple(recorded),
        references=tuple(references),
        reconcile=report,
    )


def load_references(root: Path | str) -> list[ReferenceRecord]:
    """把结果引用与对象清单按对象键后缀对齐为内容引用。

    ``artifact_manifests.csv`` 与 ``object_parts.csv`` 通过对象键的数字后缀
    关联到 ``output_objects.csv`` 中的对象；``result_refs.csv`` 再引用清单。
    有效引用最终落到具体输出对象的校验和上。
    """

    root = Path(root)
    manifests = {row["manifest_id"]: row for row in _read(root / "artifact_manifests.csv")}
    refs = _read(root / "result_refs.csv")
    parts = _read(root / "object_parts.csv")

    def suffix(key: str) -> str:
        return key.rsplit("/", 1)[-1].lstrip("0") or "0"

    outputs = _read(root / "output_objects.csv")
    key_by_suffix = {suffix(row["object_key"]): row["object_key"] for row in outputs}
    keys_by_manifest: dict[str, set[str]] = {}
    for part in parts:
        ident = suffix(part["object_key"])
        object_key = key_by_suffix.get(ident)
        if object_key is None:
            continue
        keys_by_manifest.setdefault(part["manifest_id"], set()).add(object_key)

    references: list[ReferenceRecord] = []
    for row in refs:
        manifest = manifests.get(row["manifest_id"])
        if manifest is None:
            continue
        state = ReferenceState(row["reference_state"].strip().lower())
        object_keys = sorted(keys_by_manifest.get(row["manifest_id"], ()))
        if object_keys:
            for object_key in object_keys:
                references.append(
                    ReferenceRecord(
                        reference_id=f"{row['result_id']}:{object_key}",
                        target_kind=ReferenceTarget.OBJECT_KEY,
                        target=object_key,
                        reference_state=state,
                        usage_scope=row["usage_scope"],
                        access_class=row["access_class"],
                        source=f"{row['manifest_id']}|{row['result_id']}",
                    )
                )
        else:
            # 没有对象分片时退化为按清单校验和引用内容。
            references.append(
                ReferenceRecord(
                    reference_id=f"{row['result_id']}:{row['manifest_id']}",
                    target_kind=ReferenceTarget.CHECKSUM,
                    target=manifest["checksum"],
                    reference_state=state,
                    usage_scope=row["usage_scope"],
                    access_class=row["access_class"],
                    source=f"{row['manifest_id']}|{row['result_id']}",
                )
            )
    return references


@dataclass(frozen=True)
class LoadedDataset:
    ledger: LakeCommitLedger
    jobs: tuple[JobAttempt, ...]
    recorded_partitions: tuple[RecordedPartition, ...]
    references: tuple[ReferenceRecord, ...]
    reconcile: ReconcileReport

    def recover(self, **kwargs: Any) -> RecoveryReport:
        """用本数据集的分区快照重放提交恢复。"""

        return self.ledger.recover(self.recorded_partitions, **kwargs)
