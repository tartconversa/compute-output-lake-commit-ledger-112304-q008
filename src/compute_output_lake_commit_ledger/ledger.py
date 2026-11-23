"""提交账本核心：登记、幂等通知、版本判定、提交与恢复、可见性、引用与清理。

状态保存在内存中；``to_state`` / ``from_state`` 支持账本落盘与恢复。
本模块不依赖第三方库，也不执行任何网络或文件删除操作。
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .models import (
    Anomaly,
    AttemptOutcome,
    Certificate,
    CleanupAction,
    CleanupItem,
    CleanupPlan,
    CommitResult,
    CompletenessReport,
    CompletionReceipt,
    ConflictingRecordError,
    JobAttempt,
    JobState,
    LakeLedgerError,
    OutputObject,
    Partition,
    PartitionNotVisible,
    PartitionState,
    ReceiptApplication,
    ReceiptStatus,
    ReceiptType,
    RecordedPartition,
    RecoveryAction,
    RecoveryReport,
    ReferenceRecord,
    ReferenceState,
    ReferenceTarget,
    Snapshot,
    SnapshotNotFound,
    UnknownJobError,
    VersionDecision,
    VersionRecord,
    WriteState,
    format_timestamp,
    parse_timestamp,
)


def _digest(lines: Iterable[str]) -> str:
    hasher = hashlib.sha256()
    for line in lines:
        hasher.update(line.encode("utf-8"))
        hasher.update(b"\n")
    return hasher.hexdigest()


def _partition_digest(objects: Iterable[OutputObject]) -> str:
    ordered = sorted(objects, key=lambda item: item.object_key)
    return _digest(
        f"{item.object_key}\t{item.checksum}\t{item.size_bytes}" for item in ordered
    )


def _partition_sort_key(partition: Partition) -> tuple[int, int, str]:
    return (partition.version_ordinal, partition.attempt, partition.partition_id)


class LakeCommitLedger:
    """科研计算输出的提交账本。"""

    def __init__(self) -> None:
        self._jobs: dict[tuple[str, int], JobAttempt] = {}
        self._jobs_by_run: dict[str, list[JobAttempt]] = defaultdict(list)
        self._outputs: dict[tuple[str, int], list[OutputObject]] = defaultdict(list)
        self._outputs_by_key: dict[str, list[OutputObject]] = defaultdict(list)
        self._receipts_by_id: dict[str, CompletionReceipt] = {}
        self._receipts: dict[tuple[str, int], list[CompletionReceipt]] = defaultdict(list)
        self._receipt_applications: list[ReceiptApplication] = []
        self._application_by_receipt: dict[str, ReceiptApplication] = {}
        self._buffered: list[CompletionReceipt] = []
        self._idempotency_scoped: dict[tuple[str, int, str], CompletionReceipt] = {}
        self._idempotency_global: dict[str, set[tuple[str, int]]] = defaultdict(set)
        self._conflicting: dict[tuple[str, int], list[str]] = defaultdict(list)
        self._confirmations: dict[tuple[str, str], VersionDecision] = {}
        self._partitions: dict[tuple[str, int], Partition] = {}
        self._partitions_by_id: dict[str, Partition] = {}
        self._references: list[ReferenceRecord] = []
        self._anomaly_keys: set[tuple[str, str]] = set()
        self.anomalies: list[Anomaly] = []
        self._version_cache: dict[str, dict[str, VersionRecord]] = {}

    # ==================================================================
    # 登记
    # ==================================================================
    def register_job(self, job: JobAttempt) -> JobAttempt:
        existing = self._jobs.get(job.key)
        if existing is not None:
            if existing != job:
                raise ConflictingRecordError(
                    f"作业尝试 {job.job_id}/{job.attempt} 已登记且内容不一致"
                )
            return existing
        self._jobs[job.key] = job
        self._jobs_by_run[job.logical_run].append(job)
        self._version_cache.pop(job.logical_run, None)
        self._drain_buffered(job)
        return job

    def register_output(
        self, obj: OutputObject, *, note_label: bool = True
    ) -> OutputObject:
        self._require_job(obj.job_id, obj.attempt)
        bucket = self._outputs[(obj.job_id, obj.attempt)]
        for existing in bucket:
            if existing.object_key == obj.object_key:
                if existing != obj:
                    raise ConflictingRecordError(
                        f"对象 {obj.object_key} 在 {obj.job_id}/{obj.attempt} 下登记内容不一致"
                    )
                return existing
        bucket.append(obj)
        self._outputs_by_key[obj.object_key].append(obj)
        source = obj.source_job_id
        if note_label and source is not None and source != obj.job_id:
            self.record_anomaly(
                "output_label_reassigned",
                "输出记录的作业标签与按时间匹配到的作业尝试不同，按时间归属登记",
                {
                    "object_key": obj.object_key,
                    "source_job_id": source,
                    "assigned_job_id": obj.job_id,
                    "attempt": obj.attempt,
                },
                key_suffix=obj.object_key,
            )
        if obj.write_state is WriteState.UNKNOWN:
            self.record_anomaly(
                "unknown_write_state",
                "输出对象的写入状态无法识别",
                {"object_key": obj.object_key, "recorded": obj.recorded_write_state},
                key_suffix=obj.object_key,
            )
        return obj

    def register_receipt(self, receipt: CompletionReceipt) -> ReceiptApplication:
        existing = self._receipts_by_id.get(receipt.receipt_id)
        if existing is not None:
            if existing != receipt:
                raise ConflictingRecordError(
                    f"回执 {receipt.receipt_id} 已登记且内容不一致"
                )
            return ReceiptApplication(receipt, ReceiptStatus.DUPLICATE, "同一回执重复登记")
        self._receipts_by_id[receipt.receipt_id] = receipt
        attempts = self._idempotency_global[receipt.idempotency_key]
        attempts.add(receipt.key)
        if len(attempts) > 1:
            reused = {
                key: [list(item) for item in sorted(value)]
                for key, value in sorted(self._idempotency_global.items())
                if len(value) > 1
            }
            self.record_anomaly(
                "idempotency_key_reused",
                "幂等键在多个作业尝试上复用，按尝试作用域分别幂等",
                {"reused_keys": reused},
                key_suffix="global",
            )
        if receipt.key not in self._jobs:
            self._buffered.append(receipt)
            self._receipts[receipt.key].append(receipt)
            return ReceiptApplication(receipt, ReceiptStatus.BUFFERED, "作业尝试尚未登记，先缓冲")
        return self._accept_receipt(receipt)

    def register_reference(self, reference: ReferenceRecord) -> ReferenceRecord:
        for existing in self._references:
            if existing.reference_id == reference.reference_id:
                if existing != reference:
                    raise ConflictingRecordError(
                        f"引用 {reference.reference_id} 已登记且内容不一致"
                    )
                return existing
        self._references.append(reference)
        return reference

    def _drain_buffered(self, job: JobAttempt) -> None:
        pending = [item for item in self._buffered if item.key == job.key]
        if not pending:
            return
        self._buffered = [item for item in self._buffered if item.key != job.key]
        for receipt in pending:
            self._accept_receipt(receipt, already_listed=True)

    def _accept_receipt(
        self, receipt: CompletionReceipt, *, already_listed: bool = False
    ) -> ReceiptApplication:
        scope = (receipt.job_id, receipt.attempt, receipt.idempotency_key)
        previous = self._idempotency_scoped.get(scope)
        if previous is not None:
            if previous.receipt_type is receipt.receipt_type:
                return self._record(
                    ReceiptApplication(receipt, ReceiptStatus.DUPLICATE, "同一幂等键的重复通知")
                )
            self.record_anomaly(
                "idempotency_conflict",
                "同一幂等键对应不同通知类型，保留首个",
                {
                    "job_id": receipt.job_id,
                    "attempt": receipt.attempt,
                    "idempotency_key": receipt.idempotency_key,
                    "kept": previous.receipt_type.value,
                    "ignored": receipt.receipt_type.value,
                },
                key_suffix=receipt.idempotency_key,
            )
            return self._record(
                ReceiptApplication(receipt, ReceiptStatus.CONFLICT_IGNORED, "同一幂等键的通知类型冲突")
            )
        self._idempotency_scoped[scope] = receipt
        if not already_listed:
            self._receipts[receipt.key].append(receipt)

        if receipt.receipt_type is ReceiptType.UNKNOWN:
            return self._record(
                ReceiptApplication(receipt, ReceiptStatus.CONFLICT_IGNORED, "无法识别的通知类型")
            )
        if receipt.receipt_type is ReceiptType.RETRY:
            return self._record(
                ReceiptApplication(receipt, ReceiptStatus.RECORDED, "记录重试通知")
            )

        successes = sorted(
            (
                item
                for item in self._receipts[receipt.key]
                if item.receipt_type is ReceiptType.SUCCESS
            ),
            key=lambda item: (item.received_at, item.receipt_id),
        )
        failures = sorted(
            (
                item
                for item in self._receipts[receipt.key]
                if item.receipt_type is ReceiptType.FAILURE
            ),
            key=lambda item: (item.received_at, item.receipt_id),
        )

        if receipt.receipt_type is ReceiptType.SUCCESS:
            first_success = successes[0]
            if first_success.receipt_id == receipt.receipt_id:
                if failures and failures[0].received_at < receipt.received_at:
                    self.record_anomaly(
                        "late_failure_superseded_by_success",
                        "失败通知之后收到成功通知，以成功为终态",
                        {
                            "job_id": receipt.job_id,
                            "attempt": receipt.attempt,
                            "kept": receipt.receipt_id,
                            "ignored": [item.receipt_id for item in failures],
                        },
                        key_suffix=f"{receipt.job_id}:{receipt.attempt}",
                    )
                return self._record(
                    ReceiptApplication(receipt, ReceiptStatus.APPLIED, "接受成功终态")
                )
            later = receipt.received_at > first_success.received_at
            return self._record(
                ReceiptApplication(
                    receipt,
                    ReceiptStatus.LATE_IGNORED if later else ReceiptStatus.DUPLICATE,
                    "已有更早的成功终态，不重复生效",
                )
            )

        # 失败通知：成功终态具有粘性，迟到失败不能撤销已完成版本。
        if successes:
            self._conflicting[receipt.key].append(receipt.receipt_id)
            partition = self._partitions.get((receipt.job_id, receipt.attempt))
            committed = bool(partition and partition.visible)
            self.record_anomaly(
                "conflicting_terminal_receipts",
                "已成功的尝试收到失败通知，保留成功且不回滚已提交版本",
                {
                    "job_id": receipt.job_id,
                    "attempt": receipt.attempt,
                    "kept": successes[0].receipt_id,
                    "kept_type": "success",
                    "ignored": receipt.receipt_id,
                    "ignored_type": "failure",
                    "partition_committed": committed,
                },
                key_suffix=f"{receipt.job_id}:{receipt.attempt}",
            )
            return self._record(
                ReceiptApplication(
                    receipt,
                    ReceiptStatus.LATE_IGNORED,
                    "迟到失败不能撤销已完成的版本",
                )
            )
        first_failure = failures[0]
        if first_failure.receipt_id == receipt.receipt_id:
            return self._record(
                ReceiptApplication(receipt, ReceiptStatus.APPLIED, "接受失败终态")
            )
        later = receipt.received_at > first_failure.received_at
        return self._record(
            ReceiptApplication(
                receipt,
                ReceiptStatus.LATE_IGNORED if later else ReceiptStatus.DUPLICATE,
                "已有更早的失败终态，不重复生效",
            )
        )

    def _record(self, application: ReceiptApplication) -> ReceiptApplication:
        self._receipt_applications.append(application)
        if application.receipt.receipt_id not in self._application_by_receipt:
            self._application_by_receipt[application.receipt.receipt_id] = application
        return application

    def receipt_applications(self) -> list[ReceiptApplication]:
        return list(self._receipt_applications)

    def receipt_status(self, receipt_id: str) -> ReceiptStatus | None:
        application = self._application_by_receipt.get(receipt_id)
        return application.status if application is not None else None

    # ==================================================================
    # 版本与负责人确认
    # ==================================================================
    def confirm_version(
        self,
        logical_run: str,
        *,
        confirmed_by: str,
        job_id: str | None = None,
        attempt: int | None = None,
        version_id: str | None = None,
        fingerprint: tuple[str, str, str] | None = None,
        note: str | None = None,
        is_new_experiment: bool = True,
        confirmed_at: datetime | str | None = None,
    ) -> VersionRecord:
        """作业负责人确认某个重跑是否构成新的实验版本。"""

        if not confirmed_by or not confirmed_by.strip():
            raise ValueError("confirmed_by 不能为空")
        version = self._resolve_version(
            logical_run,
            job_id=job_id,
            attempt=attempt,
            version_id=version_id,
            fingerprint=fingerprint,
        )
        decision = VersionDecision(
            logical_run=logical_run,
            fingerprint_key=version.fingerprint_key,
            is_new_experiment=bool(is_new_experiment),
            confirmed_by=confirmed_by.strip(),
            confirmed_at=parse_timestamp(confirmed_at) if confirmed_at else None,
            note=note,
        )
        self._confirmations[(logical_run, version.fingerprint_key)] = decision
        self._version_cache.pop(logical_run, None)
        return next(
            item
            for item in self.versions(logical_run)
            if item.fingerprint_key == version.fingerprint_key
        )

    def _resolve_version(
        self,
        logical_run: str,
        *,
        job_id: str | None,
        attempt: int | None,
        version_id: str | None,
        fingerprint: tuple[str, str, str] | None,
    ) -> VersionRecord:
        versions = self.versions(logical_run)
        if version_id is not None:
            for version in versions:
                if version.version_id == version_id:
                    return version
            raise LakeLedgerError(f"未找到版本 {version_id}")
        if fingerprint is not None:
            key = "|".join(fingerprint)
            for version in versions:
                if version.fingerprint_key == key:
                    return version
            raise LakeLedgerError(f"版本指纹 {key} 不属于 {logical_run}")
        if job_id is not None:
            job = None
            if attempt is not None:
                job = self._jobs.get((job_id, int(attempt)))
            if job is None:
                for item in self._jobs_by_run.get(logical_run, []):
                    if item.job_id == job_id:
                        job = item
                        break
            if job is None:
                raise UnknownJobError(job_id, attempt)
            for version in versions:
                if job.key in version.job_keys:
                    return version
            raise LakeLedgerError(f"作业 {job_id} 没有对应版本")
        raise ValueError("必须提供 job_id、version_id 或 fingerprint 之一")

    def versions(self, logical_run: str) -> list[VersionRecord]:
        cached = self._version_cache.get(logical_run)
        if cached is not None:
            return [cached[key] for key in sorted(cached, key=lambda k: cached[k].ordinal)]

        # 版本按尝试序号排序：序号越大表示越晚的重跑。
        attempts = sorted(
            self._jobs_by_run.get(logical_run, []),
            key=lambda item: (item.attempt, item.finished_at, item.job_id),
        )
        grouped: dict[tuple[str, str, str], list[JobAttempt]] = {}
        order: list[tuple[str, str, str]] = []
        for job in attempts:
            if job.fingerprint not in grouped:
                grouped[job.fingerprint] = []
                order.append(job.fingerprint)
            grouped[job.fingerprint].append(job)

        versions: dict[str, VersionRecord] = {}
        for index, fingerprint in enumerate(order, start=1):
            jobs = grouped[fingerprint]
            key = "|".join(fingerprint)
            decision = self._confirmations.get((logical_run, key))
            if index == 1:
                is_new: bool | None = True
                confirmed = True
                label = "基线版本"
            elif decision is None:
                # 指纹不同的重跑默认不回写成既有版本，需负责人确认后才可见。
                is_new = None
                confirmed = False
                label = f"待确认重跑 #{index}"
            elif decision.is_new_experiment:
                is_new = True
                confirmed = True
                label = f"重跑版本 #{index}"
            else:
                is_new = False
                confirmed = False
                label = f"重跑（负责人判定非新版本 #{index}）"
            version_id = f"{logical_run}:V{index}"
            versions[version_id] = VersionRecord(
                version_id=version_id,
                lineage_id=f"{logical_run}:L{index}",
                logical_run=logical_run,
                label=label,
                ordinal=index,
                fingerprint=fingerprint,
                confirmed=confirmed,
                is_new_experiment=is_new,
                job_keys=tuple(job.key for job in jobs),
                confirmed_by=decision.confirmed_by if decision else None,
                confirmation_note=decision.note if decision else None,
            )
        self._version_cache[logical_run] = versions
        return [versions[key] for key in sorted(versions, key=lambda k: versions[k].ordinal)]

    def version_for_job(self, job: JobAttempt | None) -> VersionRecord | None:
        if job is None:
            return None
        for version in self.versions(job.logical_run):
            if job.key in version.job_keys:
                return version
        return None

    def version_for_partition(self, partition: Partition) -> VersionRecord | None:
        job = self._find_job(partition.logical_run, partition.attempt)
        return self.version_for_job(job)

    # ==================================================================
    # 尝试终态与完整性
    # ==================================================================
    def outcome(self, job_id: str, attempt: int) -> AttemptOutcome:
        key = (job_id, attempt)
        receipts = self._receipts.get(key, [])
        successes = sorted(
            (item for item in receipts if item.receipt_type is ReceiptType.SUCCESS),
            key=lambda item: (item.received_at, item.receipt_id),
        )
        failures = sorted(
            (item for item in receipts if item.receipt_type is ReceiptType.FAILURE),
            key=lambda item: (item.received_at, item.receipt_id),
        )
        # 成功一旦出现即为终态；否则取最早的失败。
        terminal = successes[0] if successes else (failures[0] if failures else None)
        retries = sum(
            1 for item in receipts if item.receipt_type is ReceiptType.RETRY
        )
        return AttemptOutcome(
            job_id=job_id,
            attempt=attempt,
            terminal=terminal.receipt_type if terminal else None,
            terminal_receipt_id=terminal.receipt_id if terminal else None,
            retry_count=retries,
            conflicting_receipt_ids=tuple(self._conflicting.get(key, [])),
        )

    def _require_job(self, job_id: str, attempt: int) -> JobAttempt:
        job = self._jobs.get((job_id, attempt))
        if job is None:
            raise UnknownJobError(job_id, attempt)
        return job

    def _find_job(self, logical_run: str, attempt: int) -> JobAttempt | None:
        for job in self._jobs_by_run.get(logical_run, []):
            if job.attempt == attempt:
                return job
        return None

    def outputs_for(self, job_id: str, attempt: int) -> list[OutputObject]:
        return sorted(
            self._outputs.get((job_id, attempt), []), key=lambda item: item.object_key
        )

    def completeness(self, logical_run: str, attempt: int) -> CompletenessReport:
        job = self._find_job(logical_run, attempt)
        if job is None:
            return CompletenessReport(
                job_id="",
                logical_run=logical_run,
                attempt=attempt,
                complete=False,
                reasons=("unknown_job",),
                job_state=JobState.UNKNOWN,
                success_receipt=False,
                failure_receipt=False,
                retry_count=0,
                output_count=0,
                written_count=0,
                partial_count=0,
                pending_count=0,
                total_bytes=0,
                output_digest=None,
                version_id=None,
                version_label=None,
                version_ordinal=0,
                version_confirmed=False,
            )
        outputs = self.outputs_for(job.job_id, job.attempt)
        written = [obj for obj in outputs if obj.write_state is WriteState.WRITTEN]
        partial = [obj for obj in outputs if obj.write_state is WriteState.PARTIAL]
        pending = [
            obj
            for obj in outputs
            if obj.write_state in (WriteState.PENDING, WriteState.UNKNOWN)
        ]
        outcome = self.outcome(job.job_id, job.attempt)
        version = self.version_for_job(job)
        reasons: list[str] = []

        if job.state is not JobState.COMPLETE:
            reasons.append("job_state")
        if not outputs:
            reasons.append("no_outputs")
        else:
            if pending:
                reasons.append("outputs_pending")
            if partial:
                reasons.append("outputs_partial")
            if any(obj.write_state is WriteState.UNKNOWN for obj in outputs):
                reasons.append("outputs_unknown")
        if outcome.terminal is None:
            reasons.append("missing_success_receipt")
        elif outcome.terminal is ReceiptType.FAILURE:
            reasons.append("receipt_failure")
        elif outcome.terminal is not ReceiptType.SUCCESS:
            reasons.append("missing_success_receipt")
        if version is None:
            reasons.append("version_missing")
        elif not version.confirmed:
            if version.is_new_experiment is False:
                reasons.append("version_declined")
            else:
                reasons.append("version_unconfirmed")

        partition = self._partitions.get((logical_run, attempt))
        if partition is not None and partition.state is PartitionState.QUARANTINED:
            reasons.append("partition_quarantined")

        if version is not None:
            self_key = _partition_sort_key(
                partition
                or Partition(
                    partition_id="",
                    logical_run=logical_run,
                    attempt=attempt,
                    version_ordinal=version.ordinal,
                )
            )
            for other in self._partitions.values():
                if (
                    other.logical_run == logical_run
                    and other.state is PartitionState.COMMITTED
                    and other.version_key == version.version_id
                    and _partition_sort_key(other) > self_key
                ):
                    reasons.append("attempt_superseded")
                    break

        digest = _partition_digest(written) if written else None
        return CompletenessReport(
            job_id=job.job_id,
            logical_run=logical_run,
            attempt=attempt,
            complete=not reasons,
            reasons=tuple(dict.fromkeys(reasons)),
            job_state=job.state,
            success_receipt=outcome.terminal is ReceiptType.SUCCESS,
            failure_receipt=outcome.terminal is ReceiptType.FAILURE,
            retry_count=outcome.retry_count,
            output_count=len(outputs),
            written_count=len(written),
            partial_count=len(partial),
            pending_count=len(pending),
            total_bytes=sum(obj.size_bytes for obj in written),
            output_digest=digest,
            version_id=version.version_id if version else None,
            version_label=version.label if version else None,
            version_ordinal=version.ordinal if version else 0,
            version_confirmed=bool(version and version.confirmed),
        )

    def eligible_attempts(self, logical_run: str | None = None) -> list[CompletenessReport]:
        jobs = sorted(
            self._jobs.values(),
            key=lambda job: (job.logical_run, job.attempt, job.job_id),
        )
        reports = []
        for job in jobs:
            if logical_run is not None and job.logical_run != logical_run:
                continue
            report = self.completeness(job.logical_run, job.attempt)
            if report.complete:
                reports.append(report)
        return reports

    def evaluate(self, logical_run: str, attempt: int) -> CompletenessReport:
        return self.completeness(logical_run, attempt)

    # ==================================================================
    # 分区、提交与可见性
    # ==================================================================
    def _build_certificate(self, job: JobAttempt, objects: list[OutputObject]) -> Certificate:
        version = self.version_for_job(job)
        assert version is not None
        ordered = sorted(objects, key=lambda item: item.object_key)
        digest = _partition_digest(ordered)
        return Certificate(
            job_id=job.job_id,
            logical_run=job.logical_run,
            attempt=job.attempt,
            code_version=job.code_version,
            input_snapshot=job.input_snapshot,
            parameter_hash=job.parameter_hash,
            resource_queue=job.resource_queue,
            version_key=version.version_id,
            version_label=version.label,
            version_ordinal=version.ordinal,
            object_count=len(ordered),
            total_bytes=sum(obj.size_bytes for obj in ordered),
            output_digest=f"sha256:{digest}",
        )

    @staticmethod
    def _manifest_from_digest(digest: str) -> str:
        return f"MAN-{digest.split(':', 1)[-1][:16].upper()}"

    def _ensure_partition(
        self,
        job: JobAttempt,
        *,
        partition_id: str | None = None,
        manifest_digest: str | None = None,
    ) -> Partition:
        key = (job.logical_run, job.attempt)
        existing = self._partitions.get(key)
        if existing is not None:
            if partition_id is not None and existing.partition_id != partition_id:
                self.record_anomaly(
                    "partition_id_conflict",
                    "同一逻辑任务尝试出现不同分区标识，保留首个",
                    {
                        "logical_run": job.logical_run,
                        "attempt": job.attempt,
                        "kept": existing.partition_id,
                        "ignored": partition_id,
                    },
                    key_suffix=f"{job.logical_run}:{job.attempt}",
                )
            if manifest_digest is not None and existing.manifest_digest is None:
                existing.manifest_digest = manifest_digest
            return existing
        version = self.version_for_job(job)
        identifier = partition_id or self._make_partition_id(job.logical_run, job.attempt)
        if identifier in self._partitions_by_id:
            raise ConflictingRecordError(f"分区标识 {identifier} 已存在")
        partition = Partition(
            partition_id=identifier,
            logical_run=job.logical_run,
            attempt=job.attempt,
            state=PartitionState.STAGING,
            version_key=version.version_id if version else "",
            version_label=version.label if version else "",
            version_ordinal=version.ordinal if version else 0,
            manifest_digest=manifest_digest,
            created_at=datetime.now(timezone.utc),
        )
        self._partitions[key] = partition
        self._partitions_by_id[identifier] = partition
        return partition

    @staticmethod
    def _make_partition_id(logical_run: str, attempt: int) -> str:
        token = hashlib.sha1(f"{logical_run}|{attempt}".encode("utf-8")).hexdigest()[:12]
        return f"P-{token.upper()}"

    def _certify(self, partition: Partition, job: JobAttempt, *, committed_at: datetime) -> None:
        objects = self.outputs_for(job.job_id, job.attempt)
        version = self.version_for_job(job)
        partition.certificate = self._build_certificate(job, objects)
        partition.committed_at = committed_at
        partition.blockers = ()
        partition.state = PartitionState.COMMITTED
        if version is not None:
            partition.version_key = version.version_id
            partition.version_label = version.label
            partition.version_ordinal = version.ordinal
        if not partition.manifest_digest:
            partition.manifest_digest = self._manifest_from_digest(
                partition.certificate.output_digest
            )
        self._supersede_older_same_version(partition)

    def _supersede_older_same_version(self, current: Partition) -> None:
        """同一实验版本只保留最新尝试可见，旧尝试分区转为历史记录。"""

        if not current.version_key:
            return
        for other in self._partitions.values():
            if (
                other is not current
                and other.logical_run == current.logical_run
                and other.version_key == current.version_key
                and other.state is PartitionState.COMMITTED
                and _partition_sort_key(other) < _partition_sort_key(current)
            ):
                other.state = PartitionState.SUPERSEDED
                other.blockers = (
                    f"同版本已有更新尝试 {current.partition_id}",
                )

    def commit(
        self,
        logical_run: str,
        attempt: int,
        *,
        partition_id: str | None = None,
        manifest_digest: str | None = None,
        committed_at: datetime | str | None = None,
    ) -> CommitResult:
        """把满足完整性要求的尝试标记为可见分区；重复调用不产生第二个分区。"""

        job = self._find_job(logical_run, attempt)
        if job is None:
            raise UnknownJobError(logical_run, attempt)
        partition = self._ensure_partition(
            job, partition_id=partition_id, manifest_digest=manifest_digest
        )
        report = self.completeness(logical_run, attempt)
        if not report.complete:
            # 被取代或隔离的分区保持原状态，不因一次失败的提交尝试被重置。
            if partition.state not in (
                PartitionState.COMMITTED,
                PartitionState.SUPERSEDED,
                PartitionState.QUARANTINED,
            ):
                partition.state = PartitionState.STAGING
            if partition.state is PartitionState.STAGING:
                partition.blockers = report.reasons
            return CommitResult(False, partition, "blocked", report.reasons)
        moment = (
            parse_timestamp(committed_at)
            if committed_at is not None
            else datetime.now(timezone.utc)
        )
        already = partition.state is PartitionState.COMMITTED
        self._certify(
            partition, job, committed_at=partition.committed_at or moment
        )
        return CommitResult(True, partition, "verified" if already else "committed", ())

    def supersede(self, partition_id: str, *, reason: str = "被更新的可见版本取代") -> Partition:
        """把分区转为历史记录；凭证保留以便追溯输入与代码版本。"""

        partition = self.get_partition(partition_id)
        partition.state = PartitionState.SUPERSEDED
        partition.blockers = (reason,)
        return partition

    def quarantine(self, partition_id: str, *, reasons: Iterable[str] = ()) -> Partition:
        partition = self.get_partition(partition_id)
        partition.state = PartitionState.QUARANTINED
        partition.certificate = None
        partition.committed_at = None
        partition.blockers = tuple(dict.fromkeys(reasons)) or ("manual_quarantine",)
        return partition

    def get_partition(self, partition_id: str) -> Partition:
        partition = self._partitions_by_id.get(partition_id)
        if partition is None:
            raise LakeLedgerError(f"未找到分区 {partition_id}")
        return partition

    def partition_for(self, logical_run: str, attempt: int) -> Partition | None:
        return self._partitions.get((logical_run, attempt))

    def partitions(self, logical_run: str | None = None) -> list[Partition]:
        return sorted(
            (
                item
                for item in self._partitions.values()
                if logical_run is None or item.logical_run == logical_run
            ),
            key=lambda item: (item.logical_run, *_partition_sort_key(item)),
        )

    def visible_partitions(self, logical_run: str | None = None) -> list[Partition]:
        return [item for item in self.partitions(logical_run) if item.visible]

    def current_partition(self, logical_run: str) -> Partition:
        """返回当前可消费分区：同一版本内优先最新尝试，跨版本优先已确认的最新版本。"""

        visible = self.visible_partitions(logical_run)
        if not visible:
            raise SnapshotNotFound(f"逻辑任务 {logical_run} 没有可见分区")
        confirmed = [
            partition
            for partition in visible
            if (version := self.version_for_partition(partition)) is not None
            and version.confirmed
        ]
        return max(confirmed or visible, key=_partition_sort_key)

    def snapshot_for_partition(self, partition_id: str) -> Snapshot:
        partition = self.get_partition(partition_id)
        if not partition.visible or partition.certificate is None:
            raise PartitionNotVisible(
                partition_id,
                (f"分区状态为 {partition.state.value}", *partition.blockers),
            )
        job = self._jobs.get((partition.certificate.job_id, partition.attempt))
        if job is None:
            raise UnknownJobError(partition.certificate.job_id, partition.attempt)
        version = self.version_for_job(job)
        if version is None:
            raise SnapshotNotFound(f"分区 {partition_id} 没有可解析的版本")
        return Snapshot(
            partition=partition,
            job=job,
            version=version,
            certificate=partition.certificate,
            objects=tuple(self.outputs_for(job.job_id, job.attempt)),
        )

    def current_snapshot(self, logical_run: str) -> Snapshot:
        return self.snapshot_for_partition(self.current_partition(logical_run).partition_id)

    def list_snapshots(self, logical_run: str | None = None) -> list[Snapshot]:
        return [
            self.snapshot_for_partition(partition.partition_id)
            for partition in self.visible_partitions(logical_run)
        ]

    def select_snapshot(
        self,
        logical_run: str,
        *,
        version_ordinal: int | None = None,
        require_confirmed: bool = True,
    ) -> Snapshot:
        """按提交账本选择可消费快照；未确认的重跑版本默认不可选。"""

        candidates = self.visible_partitions(logical_run)
        if require_confirmed:
            candidates = [
                partition
                for partition in candidates
                if (version := self.version_for_partition(partition)) is not None
                and version.confirmed
            ]
        if version_ordinal is not None:
            candidates = [
                partition
                for partition in candidates
                if partition.version_ordinal == version_ordinal
            ]
        if not candidates:
            raise SnapshotNotFound(
                f"逻辑任务 {logical_run} 没有满足条件的可见快照"
            )
        return self.snapshot_for_partition(
            max(candidates, key=_partition_sort_key).partition_id
        )

    # ==================================================================
    # 提交恢复
    # ==================================================================
    def recover(
        self,
        recorded_partitions: Iterable[RecordedPartition],
        *,
        auto_commit: bool = True,
        create_missing: bool = True,
        now: datetime | str | None = None,
    ) -> RecoveryReport:
        """依据存储快照与账本事实恢复提交状态，并保持幂等。"""

        moment = parse_timestamp(now) if now is not None else datetime.now(timezone.utc)
        actions: list[RecoveryAction] = []
        created: list[str] = []
        promoted: list[str] = []
        verified: list[str] = []
        quarantined: list[str] = []
        held: list[str] = []
        staged: list[str] = []
        superseded: list[str] = []
        skipped: list[str] = []

        normalised: dict[tuple[str, int], RecordedPartition] = {}
        for record in recorded_partitions:
            key = (record.logical_run, record.attempt)
            existing = normalised.get(key)
            if existing is None:
                normalised[key] = record
                continue
            if (
                existing.partition_id != record.partition_id
                or existing.commit_state.strip().lower()
                != record.commit_state.strip().lower()
            ):
                self.record_anomaly(
                    "conflicting_partition_records",
                    "同一逻辑任务尝试存在冲突的存储分区记录，保留首个",
                    {"kept": existing.to_dict(), "ignored": record.to_dict()},
                    key_suffix=f"{record.logical_run}:{record.attempt}",
                )

        for key, record in sorted(normalised.items()):
            job = self._find_job(*key)
            if job is None:
                self.record_anomaly(
                    "partition_without_job",
                    "存储分区记录找不到对应作业尝试",
                    {"record": record.to_dict()},
                    key_suffix=f"{key[0]}:{key[1]}",
                )
                skipped.append(record.partition_id)
                continue
            partition = self._ensure_partition(
                job,
                partition_id=record.partition_id,
                manifest_digest=record.manifest_digest,
            )
            partition.recorded_state = record.commit_state
            partition.recorded_manifest_digest = record.manifest_digest
            holder = self._partitions_by_id.get(record.partition_id)
            if holder is not None and holder is not partition:
                self.record_anomaly(
                    "partition_id_shared",
                    "多个逻辑任务尝试使用同一分区标识",
                    {
                        "partition_id": record.partition_id,
                        "kept": [holder.logical_run, holder.attempt],
                        "ignored": [record.logical_run, record.attempt],
                    },
                    key_suffix=record.partition_id,
                )

        # 两阶段处理以保证与快照顺序无关：
        # 1) 先接纳/提升所有满足完整性的分区（可能补建缺失分区）；
        # 2) 再对不合格的已提交分区判定“被取代”或“隔离”。
        conflicts: list[tuple[tuple[str, int], RecordedPartition]] = []
        # 由新到旧处理：先确认同一版本的最新尝试，旧尝试随后被判为被取代。
        for key, record in sorted(normalised.items(), reverse=True):
            job = self._find_job(*key)
            if job is None:
                continue
            partition = self._partitions[key]
            report = self.completeness(*key)
            state = record.commit_state.strip().lower()
            if state == "committed":
                if report.complete:
                    self._certify(partition, job, committed_at=record.committed_at)
                    if partition.manifest_digest is None:
                        partition.manifest_digest = record.manifest_digest
                    verified.append(partition.partition_id)
                    actions.append(
                        self._action(partition, record.commit_state, "verified", ())
                    )
                else:
                    conflicts.append((key, record))
            elif state == "staging":
                already_committed = partition.state is PartitionState.COMMITTED
                if report.complete and auto_commit:
                    self._certify(partition, job, committed_at=moment)
                    if already_committed:
                        verified.append(partition.partition_id)
                        actions.append(
                            self._action(
                                partition, record.commit_state, "verified", ()
                            )
                        )
                    else:
                        promoted.append(partition.partition_id)
                        actions.append(
                            self._action(
                                partition, record.commit_state, "promoted", ()
                            )
                        )
                elif report.complete:
                    partition.state = PartitionState.STAGING
                    partition.certificate = None
                    partition.blockers = ()
                    staged.append(partition.partition_id)
                    actions.append(
                        self._action(partition, record.commit_state, "staged", ())
                    )
                else:
                    partition.state = PartitionState.STAGING
                    partition.certificate = None
                    partition.blockers = report.reasons
                    held.append(partition.partition_id)
                    actions.append(
                        self._action(partition, record.commit_state, "held", report.reasons)
                    )
            elif state == "quarantined":
                partition.state = PartitionState.QUARANTINED
                partition.certificate = None
                partition.committed_at = None
                partition.blockers = report.reasons or partition.blockers or (
                    "recorded_quarantined",
                )
                skipped.append(partition.partition_id)
                actions.append(
                    self._action(
                        partition, record.commit_state, "kept_quarantined", report.reasons
                    )
                )
            else:
                partition.state = PartitionState.STAGING
                partition.certificate = None
                partition.blockers = ("unknown_commit_state",)
                held.append(partition.partition_id)
                self.record_anomaly(
                    "unknown_commit_state",
                    "存储分区记录包含无法识别的提交状态",
                    {"record": record.to_dict()},
                    key_suffix=record.partition_id,
                )
                actions.append(
                    self._action(
                        partition, record.commit_state, "held", ("unknown_commit_state",)
                    )
                )

        # 账本中已提交但存储快照未覆盖的分区。
        for job in sorted(
            self._jobs.values(), key=lambda item: (item.logical_run, item.attempt)
        ):
            key = (job.logical_run, job.attempt)
            if key in normalised:
                continue
            partition = self._partitions.get(key)
            if partition is None or partition.state is not PartitionState.COMMITTED:
                continue
            report = self.completeness(*key)
            if report.complete:
                if partition.state is PartitionState.SUPERSEDED:
                    superseded.append(partition.partition_id)
                    actions.append(
                        self._action(
                            partition, "ledger_committed", "superseded", partition.blockers
                        )
                    )
                else:
                    verified.append(partition.partition_id)
                    actions.append(
                        self._action(partition, "ledger_committed", "verified", ())
                    )
                continue
            fake = RecordedPartition(
                partition_id=partition.partition_id,
                logical_run=partition.logical_run,
                attempt=partition.attempt,
                commit_state="ledger_committed",
                committed_at=partition.committed_at or moment,
                manifest_digest=partition.manifest_digest or "",
                consumer_refs=0,
            )
            conflicts.append((key, fake))

        # 账本中合格但存储快照未覆盖、也没有分区记录：补建。
        if create_missing:
            for job in sorted(
                self._jobs.values(), key=lambda item: (item.logical_run, item.attempt)
            ):
                key = (job.logical_run, job.attempt)
                if key in normalised or key in self._partitions:
                    continue
                report = self.completeness(*key)
                if not report.complete:
                    continue
                partition = self._ensure_partition(job)
                created.append(partition.partition_id)
                if auto_commit:
                    self._certify(partition, job, committed_at=moment)
                    promoted.append(partition.partition_id)
                    actions.append(
                        self._action(partition, "missing", "created_committed", ())
                    )
                else:
                    staged.append(partition.partition_id)
                    actions.append(
                        self._action(partition, "missing", "created_staging", ())
                    )

        # 第二阶段：此时可见集合已经确定，再判定不合格的已提交分区。
        for key, record in sorted(conflicts):
            partition = self._partitions.get(key)
            if partition is None:
                continue
            report = self.completeness(*key)
            self._resolve_conflict(
                partition, record, report, actions, superseded, quarantined
            )

        return RecoveryReport(
            actions=tuple(actions),
            created=tuple(created),
            promoted=tuple(promoted),
            verified=tuple(verified),
            quarantined=tuple(quarantined),
            held=tuple(held),
            staged=tuple(staged),
            skipped=tuple(skipped),
            superseded=tuple(superseded),
        )

    def _resolve_conflict(
        self,
        partition: Partition,
        record: RecordedPartition,
        report: CompletenessReport,
        actions: list[RecoveryAction],
        superseded: list[str],
        quarantined: list[str],
    ) -> None:
        winner = self._visible_winner(partition.logical_run, exclude=partition.key)
        newer = winner is not None and _partition_sort_key(winner) > _partition_sort_key(
            partition
        )
        if newer:
            partition.state = PartitionState.SUPERSEDED
            partition.blockers = (
                f"同逻辑任务已有更新且合格的分区 {winner.partition_id}",  # type: ignore[union-attr]
                *report.reasons,
            )
            superseded.append(partition.partition_id)
            actions.append(
                self._action(partition, record.commit_state, "superseded", report.reasons)
            )
        else:
            partition.state = PartitionState.QUARANTINED
            partition.certificate = None
            partition.committed_at = None
            partition.blockers = report.reasons
            quarantined.append(partition.partition_id)
            actions.append(
                self._action(partition, record.commit_state, "quarantined", report.reasons)
            )

    def _visible_winner(
        self, logical_run: str, *, exclude: tuple[str, int] | None = None
    ) -> Partition | None:
        candidates = [
            partition
            for key, partition in self._partitions.items()
            if partition.logical_run == logical_run
            and partition.visible
            and key != exclude
        ]
        return max(candidates, key=_partition_sort_key) if candidates else None

    def _action(
        self,
        partition: Partition,
        recorded_state: str,
        action: str,
        reasons: Iterable[str],
    ) -> RecoveryAction:
        version = self.version_for_partition(partition)
        partition.recovery_action = action
        if action.startswith("created_"):
            partition.recovered_from = "missing"
        else:
            partition.recovered_from = recorded_state
        return RecoveryAction(
            partition_id=partition.partition_id,
            logical_run=partition.logical_run,
            attempt=partition.attempt,
            recorded_state=recorded_state,
            resulting_state=partition.state.value,
            action=action,
            reasons=tuple(dict.fromkeys(reasons)),
            version_id=version.version_id if version else None,
            version_confirmed=bool(version and version.confirmed),
        )

    # ==================================================================
    # 引用查询
    # ==================================================================
    def _referenced_checksums(self, object_key: str) -> set[str]:
        return {obj.checksum for obj in self._outputs_by_key.get(object_key, [])}

    def references_to_object(self, object_key: str) -> list[ReferenceRecord]:
        checksums = self._referenced_checksums(object_key)
        matched = []
        for reference in self._references:
            if reference.target_kind is ReferenceTarget.OBJECT_KEY:
                if reference.target == object_key:
                    matched.append(reference)
            elif reference.target in checksums:
                matched.append(reference)
        return matched

    def references_to_checksum(self, checksum: str) -> list[ReferenceRecord]:
        """按内容校验和查询引用（对象引用也会折算到其校验和）。"""

        matched = []
        for reference in self._references:
            if reference.target_kind is ReferenceTarget.CHECKSUM:
                if reference.target == checksum:
                    matched.append(reference)
            else:
                for obj in self._outputs_by_key.get(reference.target, []):
                    if obj.checksum == checksum:
                        matched.append(reference)
                        break
        return matched

    def references_to_partition(self, partition_id: str) -> list[ReferenceRecord]:
        """分区当前承载对象上的全部引用（含已撤回，便于审计）。"""

        partition = self.get_partition(partition_id)
        job = self._find_job(partition.logical_run, partition.attempt)
        if job is None:
            return []
        matched: list[ReferenceRecord] = []
        seen: set[str] = set()
        for obj in self._outputs.get(job.key, []):
            for reference in self.references_to_object(obj.object_key):
                if reference.reference_id not in seen:
                    seen.add(reference.reference_id)
                    matched.append(reference)
        return matched

    def references_to_run(self, logical_run: str) -> list[ReferenceRecord]:
        """逻辑任务所有尝试对象上的引用，按引用标识去重。"""

        matched: list[ReferenceRecord] = []
        seen: set[str] = set()
        for job in self._jobs_by_run.get(logical_run, []):
            for obj in self._outputs.get(job.key, []):
                for reference in self.references_to_object(obj.object_key):
                    if reference.reference_id not in seen:
                        seen.add(reference.reference_id)
                        matched.append(reference)
        return matched

    def objects_for_reference(self, reference_id: str) -> list[OutputObject]:
        reference = next(
            (item for item in self._references if item.reference_id == reference_id),
            None,
        )
        if reference is None:
            raise LakeLedgerError(f"未找到引用 {reference_id}")
        if reference.target_kind is ReferenceTarget.OBJECT_KEY:
            return list(self._outputs_by_key.get(reference.target, []))
        return [
            obj
            for bucket in self._outputs.values()
            for obj in bucket
            if obj.checksum == reference.target
        ]

    def is_object_protected(self, object_key: str) -> bool:
        """对象是否被有效或待复核的引用保护。"""

        return any(
            reference.reference_state
            in (ReferenceState.VALID, ReferenceState.REVIEW, ReferenceState.UNKNOWN)
            for reference in self.references_to_object(object_key)
        )

    # ==================================================================
    # 清理
    # ==================================================================
    def cleanup_plan(
        self,
        *,
        logical_run: str | None = None,
        include_committed: bool = False,
    ) -> CleanupPlan:
        """规划未完成输出的清理与隔离，保护仍被有效结果引用或已提交的对象。"""

        items: list[CleanupItem] = []
        protected_bytes = 0
        reclaimable_bytes = 0
        held_bytes = 0
        for job in sorted(
            self._jobs.values(), key=lambda item: (item.logical_run, item.attempt)
        ):
            if logical_run is not None and job.logical_run != logical_run:
                continue
            partition = self._partitions.get((job.logical_run, job.attempt))
            committed = bool(partition and partition.visible)
            report = self.completeness(job.logical_run, job.attempt)
            for obj in self.outputs_for(job.job_id, job.attempt):
                references = self.references_to_object(obj.object_key)
                valid = [
                    ref for ref in references if ref.reference_state is ReferenceState.VALID
                ]
                disputed = [
                    ref
                    for ref in references
                    if ref.reference_state in (ReferenceState.REVIEW, ReferenceState.UNKNOWN)
                ]
                if committed and not include_committed:
                    action, reason = CleanupAction.PROTECT, "属于已提交可见分区"
                elif valid:
                    if obj.write_state is WriteState.WRITTEN:
                        action, reason = CleanupAction.PROTECT, "被有效结果引用"
                    else:
                        action, reason = (
                            CleanupAction.HOLD,
                            "被有效结果引用但对象写入不完整，需复核",
                        )
                        self.record_anomaly(
                            "valid_reference_to_incomplete_object",
                            "有效结果引用了未完整写入的对象",
                            {
                                "object_key": obj.object_key,
                                "write_state": obj.write_state.value,
                                "references": [ref.reference_id for ref in valid],
                            },
                            key_suffix=obj.object_key,
                        )
                elif disputed:
                    action, reason = CleanupAction.HOLD, "存在待复核引用"
                elif report.complete:
                    action, reason = CleanupAction.PROTECT, "属于待提交的合格尝试"
                elif job.state in (JobState.RUNNING, JobState.RETRYING):
                    action, reason = CleanupAction.HOLD, "尝试仍在运行，暂不清理"
                elif obj.write_state is WriteState.WRITTEN:
                    action, reason = CleanupAction.QUARANTINE, "完整但未提交到可见分区"
                else:
                    action, reason = CleanupAction.DELETE, "未完成输出且无有效引用"

                items.append(
                    CleanupItem(
                        object_key=obj.object_key,
                        job_id=job.job_id,
                        attempt=job.attempt,
                        write_state=obj.write_state,
                        size_bytes=obj.size_bytes,
                        action=action,
                        reason=reason,
                    )
                )
                if action is CleanupAction.PROTECT:
                    protected_bytes += obj.size_bytes
                elif action is CleanupAction.DELETE:
                    reclaimable_bytes += obj.size_bytes
                else:
                    held_bytes += obj.size_bytes
        return CleanupPlan(
            items=tuple(items),
            protected_bytes=protected_bytes,
            reclaimable_bytes=reclaimable_bytes,
            held_bytes=held_bytes,
        )

    def apply_cleanup(
        self,
        plan: CleanupPlan,
        *,
        confirm: bool,
        now: datetime | str | None = None,
    ) -> dict[str, Any]:
        """执行清理计划：删除可回收对象，隔离而不删除完整输出。

        仅移除账本登记；不触碰任何真实文件。``confirm=False`` 时只返回演练结果。
        """

        moment = parse_timestamp(now) if now is not None else datetime.now(timezone.utc)
        removed: list[str] = []
        quarantined: list[str] = []
        protected: list[str] = []
        for item in plan.items:
            if item.action is CleanupAction.DELETE:
                if confirm:
                    self._remove_output(item.job_id, item.attempt, item.object_key)
                removed.append(item.object_key)
            elif item.action is CleanupAction.QUARANTINE:
                if confirm:
                    self._quarantine_object(item.job_id, item.attempt, item.object_key)
                quarantined.append(item.object_key)
            elif item.action is CleanupAction.PROTECT:
                protected.append(item.object_key)
        return {
            "applied": bool(confirm),
            "at": format_timestamp(moment),
            "removed": removed,
            "quarantined": quarantined,
            "protected": protected,
            "reclaimable_bytes": plan.reclaimable_bytes,
        }

    def _remove_output(self, job_id: str, attempt: int, object_key: str) -> None:
        bucket = self._outputs.get((job_id, attempt), [])
        self._outputs[(job_id, attempt)] = [
            obj for obj in bucket if obj.object_key != object_key
        ]
        self._outputs_by_key[object_key] = [
            obj for obj in self._outputs_by_key.get(object_key, [])
            if not (obj.job_id == job_id and obj.attempt == attempt)
        ]
        if not self._outputs_by_key[object_key]:
            del self._outputs_by_key[object_key]

    def _quarantine_object(self, job_id: str, attempt: int, object_key: str) -> None:
        bucket = self._outputs.get((job_id, attempt), [])
        for index, obj in enumerate(bucket):
            if obj.object_key != object_key:
                continue
            if obj.quarantined:
                return
            bucket[index] = replace(obj, quarantined=True)
            self.record_anomaly(
                "object_quarantined",
                "完整但未提交的对象已移入隔离区",
                {"object_key": object_key, "job_id": job_id, "attempt": attempt},
                key_suffix=object_key,
            )
            return

    # ==================================================================
    # 报告与持久化
    # ==================================================================
    def record_anomaly(
        self,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
        *,
        key_suffix: str = "",
    ) -> None:
        key = (code, key_suffix)
        if key in self._anomaly_keys:
            return
        self._anomaly_keys.add(key)
        self.anomalies.append(Anomaly(code, message, details or {}, key_suffix))

    def summary(self) -> dict[str, Any]:
        partition_states: dict[str, int] = defaultdict(int)
        for partition in self._partitions.values():
            partition_states[partition.state.value] += 1
        outcome_states: dict[str, int] = defaultdict(int)
        for job in self._jobs.values():
            outcome = self.outcome(job.job_id, job.attempt)
            outcome_states[outcome.terminal.value if outcome.terminal else "none"] += 1
        return {
            "counts": {
                "jobs": len(self._jobs),
                "logical_runs": len(self._jobs_by_run),
                "outputs": sum(len(bucket) for bucket in self._outputs.values()),
                "receipts": len(self._receipts_by_id),
                "partitions": len(self._partitions),
                "visible_partitions": len(self.visible_partitions()),
                "references": len(self._references),
            },
            "partition_states": dict(sorted(partition_states.items())),
            "attempt_outcomes": dict(sorted(outcome_states.items())),
            "eligible": [report.to_dict() for report in self.eligible_attempts()],
            "visible": [partition.to_dict() for partition in self.visible_partitions()],
            "anomalies": [anomaly.to_dict() for anomaly in self.anomalies],
        }

    def to_state(self) -> dict[str, Any]:
        return {
            "format": "compute-output-lake-commit-ledger/v1",
            "jobs": [job.to_dict() for job in self._jobs.values()],
            "outputs": [
                obj.to_dict() for bucket in self._outputs.values() for obj in bucket
            ],
            "receipts": [
                dict(receipt.to_dict(), status=application.status.value)
                for receipt, application in (
                    (receipt, self._application_for(receipt))
                    for receipt in self._receipts_by_id.values()
                )
            ],
            "references": [reference.to_dict() for reference in self._references],
            "confirmations": [
                {
                    "logical_run": decision.logical_run,
                    "fingerprint_key": decision.fingerprint_key,
                    "is_new_experiment": decision.is_new_experiment,
                    "confirmed_by": decision.confirmed_by,
                    "confirmed_at": format_timestamp(decision.confirmed_at),
                    "note": decision.note,
                }
                for decision in self._confirmations.values()
            ],
            "partitions": [
                partition.to_dict() for partition in self._partitions.values()
            ],
            "anomalies": [anomaly.to_dict() for anomaly in self.anomalies],
        }

    def _application_for(self, receipt: CompletionReceipt) -> ReceiptApplication:
        existing = self._application_by_receipt.get(receipt.receipt_id)
        if existing is not None:
            return existing
        return ReceiptApplication(receipt, ReceiptStatus.RECORDED, "已登记")

    def save_state(self, path: Path | str) -> Path:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(
            json.dumps(self.to_state(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return target

    @classmethod
    def from_state(cls, payload: dict[str, Any]) -> "LakeCommitLedger":
        ledger = cls()
        for row in payload.get("jobs", []):
            ledger.register_job(
                JobAttempt(
                    job_id=row["job_id"],
                    logical_run=row["logical_run"],
                    code_version=row["code_version"],
                    input_snapshot=row["input_snapshot"],
                    parameter_hash=row["parameter_hash"],
                    attempt=int(row["attempt"]),
                    state=row["state"],
                    finished_at=row["finished_at"],
                    resource_queue=row.get("resource_queue"),
                    experiment_version=row.get("experiment_version"),
                    recorded_state=row.get("recorded_state"),
                )
            )
        for row in payload.get("outputs", []):
            ledger.register_output(
                OutputObject(
                    job_id=row["job_id"],
                    attempt=int(row["attempt"]),
                    object_key=row["object_key"],
                    size_bytes=int(row["size_bytes"]),
                    checksum=row["checksum"],
                    write_state=row["write_state"],
                    written_at=row["written_at"],
                    source_job_id=row.get("source_job_id"),
                    recorded_write_state=row.get("recorded_write_state"),
                    quarantined=bool(row.get("quarantined", False)),
                ),
                note_label=False,
            )
        for row in payload.get("receipts", []):
            ledger.register_receipt(
                CompletionReceipt(
                    receipt_id=row["receipt_id"],
                    job_id=row["job_id"],
                    attempt=int(row["attempt"]),
                    receipt_type=row["receipt_type"],
                    received_at=row["received_at"],
                    idempotency_key=row["idempotency_key"],
                    source_job_id=row.get("source_job_id"),
                    recorded_receipt_type=row.get("recorded_receipt_type"),
                )
            )
        for row in payload.get("references", []):
            ledger.register_reference(
                ReferenceRecord(
                    reference_id=row["reference_id"],
                    target_kind=row["target_kind"],
                    target=row["target"],
                    reference_state=row["reference_state"],
                    usage_scope=row.get("usage_scope"),
                    access_class=row.get("access_class"),
                    source=row.get("source"),
                )
            )
        for row in payload.get("confirmations", []):
            ledger.confirm_version(
                row["logical_run"],
                fingerprint=tuple(row["fingerprint_key"].split("|")),
                confirmed_by=row["confirmed_by"],
                note=row.get("note"),
                is_new_experiment=bool(row.get("is_new_experiment", True)),
                confirmed_at=row.get("confirmed_at"),
            )
        for row in payload.get("partitions", []):
            certificate = row.get("certificate")
            partition = Partition(
                partition_id=row["partition_id"],
                logical_run=row["logical_run"],
                attempt=int(row["attempt"]),
                state=PartitionState(str(row["state"]).strip().lower()),
                version_key=row.get("version_key", ""),
                version_label=row.get("version_label", ""),
                version_ordinal=int(row.get("version_ordinal", 0)),
                manifest_digest=row.get("manifest_digest"),
                created_at=parse_timestamp(row["created_at"])
                if row.get("created_at")
                else None,
                committed_at=parse_timestamp(row["committed_at"])
                if row.get("committed_at")
                else None,
                blockers=tuple(row.get("blockers", ())),
                recorded_state=row.get("recorded_state"),
                recorded_manifest_digest=row.get("recorded_manifest_digest"),
                recovered_from=row.get("recovered_from"),
                recovery_action=row.get("recovery_action"),
            )
            if certificate is not None:
                partition.certificate = Certificate(
                    job_id=certificate["job_id"],
                    logical_run=certificate["logical_run"],
                    attempt=int(certificate["attempt"]),
                    code_version=certificate["code_version"],
                    input_snapshot=certificate["input_snapshot"],
                    parameter_hash=certificate["parameter_hash"],
                    resource_queue=certificate.get("resource_queue"),
                    version_key=certificate["version_key"],
                    version_label=certificate["version_label"],
                    version_ordinal=int(certificate["version_ordinal"]),
                    object_count=int(certificate["object_count"]),
                    total_bytes=int(certificate["total_bytes"]),
                    output_digest=certificate["output_digest"],
                )
            ledger._partitions[(partition.logical_run, partition.attempt)] = partition
            ledger._partitions_by_id[partition.partition_id] = partition
        for row in payload.get("anomalies", []):
            ledger.record_anomaly(
                row["code"],
                row["message"],
                row.get("details", {}),
                key_suffix=row.get("key_suffix", ""),
            )
        return ledger

    @classmethod
    def load_state(cls, path: Path | str) -> "LakeCommitLedger":
        return cls.from_state(json.loads(Path(path).read_text(encoding="utf-8")))
