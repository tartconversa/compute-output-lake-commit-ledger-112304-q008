"""领域模型：作业尝试、输出对象、完成回执、版本、分区、引用与清理计划。

设计约束：

* 所有时间统一为 UTC；输入可带 ``Z`` 后缀或显式时区。
* 枚举解析不静默猜测：无法识别的取值映射为 ``UNKNOWN``，由加载器记录异常。
* 已提交分区所对应的作业尝试、代码版本、输入快照和参数哈希保存在证书中。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class LakeLedgerError(Exception):
    """账本与分区管理的基类异常。"""


class UnknownJobError(LakeLedgerError):
    """引用了未登记的作业尝试。"""

    def __init__(self, job_id: str, attempt: int | None = None) -> None:
        self.job_id = job_id
        self.attempt = attempt
        suffix = f"（attempt={attempt}）" if attempt is not None else ""
        super().__init__(f"未登记的作业尝试：{job_id}{suffix}")


class ConflictingRecordError(LakeLedgerError):
    """同一标识被登记为不一致的内容。"""


class SnapshotNotFound(LakeLedgerError):
    """逻辑任务下没有满足条件的可见快照。"""


class PartitionNotVisible(LakeLedgerError):
    """消费方请求的分区尚不可见。"""

    def __init__(self, partition_id: str, reasons: tuple[str, ...] = ()) -> None:
        self.partition_id = partition_id
        self.reasons = reasons
        detail = f"分区 {partition_id} 尚不可消费"
        if reasons:
            detail += "：" + "、".join(reasons)
        super().__init__(detail)


class JobState(str, Enum):
    RUNNING = "running"
    COMPLETE = "complete"
    FAILED = "failed"
    RETRYING = "retrying"
    UNKNOWN = "unknown"


class WriteState(str, Enum):
    PENDING = "pending"
    PARTIAL = "partial"
    WRITTEN = "written"
    UNKNOWN = "unknown"


class ReceiptType(str, Enum):
    SUCCESS = "success"
    FAILURE = "failure"
    RETRY = "retry"
    UNKNOWN = "unknown"


class ReceiptStatus(str, Enum):
    """一条通知被账本接受、缓冲或忽略的结论。"""

    APPLIED = "applied"
    BUFFERED = "buffered"
    RECORDED = "recorded"
    DUPLICATE = "duplicate"
    CONFLICT_IGNORED = "conflict_ignored"
    LATE_IGNORED = "late_ignored"


class PartitionState(str, Enum):
    STAGING = "staging"
    COMMITTED = "committed"
    QUARANTINED = "quarantined"
    SUPERSEDED = "superseded"
    UNKNOWN = "unknown"


class ReferenceState(str, Enum):
    VALID = "valid"
    REVIEW = "review"
    WITHDRAWN = "withdrawn"
    UNKNOWN = "unknown"


class ReferenceTarget(str, Enum):
    CHECKSUM = "checksum"
    OBJECT_KEY = "object_key"


class CleanupAction(str, Enum):
    DELETE = "delete"
    QUARANTINE = "quarantine"
    HOLD = "hold"
    PROTECT = "protect"


def parse_timestamp(value: datetime | str) -> datetime:
    """解析 ISO-8601 时间并统一为 UTC。"""

    if isinstance(value, datetime):
        moment = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError("时间字段不能为空")
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        moment = datetime.fromisoformat(text)
    else:
        raise TypeError(f"无法解析时间：{value!r}")
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc)


def format_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return parse_timestamp(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def as_enum(enum_type: type[Enum], value: Any) -> Any:
    """解析枚举值；无法识别时返回该枚举的 UNKNOWN 成员（若存在）。"""

    if isinstance(value, enum_type):
        return value
    text = str(value).strip().lower()
    try:
        return enum_type(text)
    except ValueError:
        unknown = getattr(enum_type, "UNKNOWN", None)
        if unknown is not None:
            return unknown
        raise ValueError(f"{enum_type.__name__} 不接受取值 {value!r}") from None


@dataclass(frozen=True)
class JobAttempt:
    """一次具体执行（job_id + attempt），属于某个逻辑任务的实验版本。"""

    job_id: str
    logical_run: str
    code_version: str
    input_snapshot: str
    parameter_hash: str
    attempt: int
    state: JobState
    finished_at: datetime
    resource_queue: str | None = None
    experiment_version: str | None = None
    recorded_state: str | None = None

    def __post_init__(self) -> None:
        if not self.job_id or not self.logical_run:
            raise ValueError("job_id 与 logical_run 不能为空")
        if int(self.attempt) < 1:
            raise ValueError("attempt 必须从 1 开始")
        object.__setattr__(self, "attempt", int(self.attempt))
        object.__setattr__(self, "state", as_enum(JobState, self.state))
        object.__setattr__(self, "finished_at", parse_timestamp(self.finished_at))
        if self.recorded_state is None:
            object.__setattr__(self, "recorded_state", self.state.value)
        for name in ("resource_queue", "experiment_version"):
            value = getattr(self, name)
            if value is not None and not str(value).strip():
                object.__setattr__(self, name, None)

    @property
    def key(self) -> tuple[str, int]:
        return (self.job_id, self.attempt)

    @property
    def fingerprint(self) -> tuple[str, str, str]:
        """代码、输入快照与参数的组合，用于判断是否为同一实验版本。"""

        return (self.code_version, self.input_snapshot, self.parameter_hash)

    @property
    def fingerprint_key(self) -> str:
        return "|".join(self.fingerprint)

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "logical_run": self.logical_run,
            "code_version": self.code_version,
            "input_snapshot": self.input_snapshot,
            "parameter_hash": self.parameter_hash,
            "attempt": self.attempt,
            "state": self.state.value,
            "recorded_state": self.recorded_state,
            "finished_at": format_timestamp(self.finished_at),
            "resource_queue": self.resource_queue,
            "experiment_version": self.experiment_version,
        }


@dataclass(frozen=True)
class OutputObject:
    """作业尝试写出的对象；write_state 表示该对象是否已完整落盘。"""

    job_id: str
    attempt: int
    object_key: str
    size_bytes: int
    checksum: str
    write_state: WriteState
    written_at: datetime
    source_job_id: str | None = None
    recorded_write_state: str | None = None
    quarantined: bool = False

    def __post_init__(self) -> None:
        if not self.object_key:
            raise ValueError("object_key 不能为空")
        object.__setattr__(self, "attempt", int(self.attempt))
        if int(self.size_bytes) < 0:
            raise ValueError("size_bytes 不能为负")
        object.__setattr__(self, "size_bytes", int(self.size_bytes))
        object.__setattr__(self, "write_state", as_enum(WriteState, self.write_state))
        object.__setattr__(self, "written_at", parse_timestamp(self.written_at))
        if self.recorded_write_state is None:
            object.__setattr__(self, "recorded_write_state", self.write_state.value)
        if self.source_job_id is None:
            object.__setattr__(self, "source_job_id", self.job_id)

    @property
    def identity(self) -> tuple[str, int, str]:
        return (self.job_id, self.attempt, self.object_key)

    def to_dict(self) -> dict[str, Any]:
        return {
            "object_key": self.object_key,
            "job_id": self.job_id,
            "attempt": self.attempt,
            "size_bytes": self.size_bytes,
            "checksum": self.checksum,
            "write_state": self.write_state.value,
            "recorded_write_state": self.recorded_write_state,
            "written_at": format_timestamp(self.written_at),
            "source_job_id": self.source_job_id,
            "quarantined": self.quarantined,
        }


@dataclass(frozen=True)
class CompletionReceipt:
    """调度系统或工作进程发来的完成/失败/重试通知。"""

    receipt_id: str
    job_id: str
    attempt: int
    receipt_type: ReceiptType
    received_at: datetime
    idempotency_key: str
    source_job_id: str | None = None
    recorded_receipt_type: str | None = None

    def __post_init__(self) -> None:
        if not self.receipt_id or not self.idempotency_key:
            raise ValueError("receipt_id 与 idempotency_key 不能为空")
        object.__setattr__(self, "attempt", int(self.attempt))
        object.__setattr__(self, "receipt_type", as_enum(ReceiptType, self.receipt_type))
        object.__setattr__(self, "received_at", parse_timestamp(self.received_at))
        if self.recorded_receipt_type is None:
            object.__setattr__(self, "recorded_receipt_type", self.receipt_type.value)
        if self.source_job_id is None:
            object.__setattr__(self, "source_job_id", self.job_id)

    @property
    def key(self) -> tuple[str, int]:
        return (self.job_id, self.attempt)

    def to_dict(self) -> dict[str, Any]:
        return {
            "receipt_id": self.receipt_id,
            "job_id": self.job_id,
            "attempt": self.attempt,
            "receipt_type": self.receipt_type.value,
            "recorded_receipt_type": self.recorded_receipt_type,
            "received_at": format_timestamp(self.received_at),
            "idempotency_key": self.idempotency_key,
            "source_job_id": self.source_job_id,
        }


@dataclass(frozen=True)
class ReceiptApplication:
    """账本对一条通知的处理结论。"""

    receipt: CompletionReceipt
    status: ReceiptStatus
    reason: str

    @property
    def applied(self) -> bool:
        return self.status in (ReceiptStatus.APPLIED, ReceiptStatus.RECORDED)

    def to_dict(self) -> dict[str, Any]:
        return {
            "receipt": self.receipt.to_dict(),
            "status": self.status.value,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class AttemptOutcome:
    """某次尝试的终态判定：首个 success/failure 通知胜出。"""

    job_id: str
    attempt: int
    terminal: ReceiptType | None = None
    terminal_receipt_id: str | None = None
    retry_count: int = 0
    conflicting_receipt_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class VersionRecord:
    """逻辑任务下的一个实验版本（按代码、输入快照与参数指纹分组）。"""

    version_id: str
    lineage_id: str
    logical_run: str
    label: str
    ordinal: int
    fingerprint: tuple[str, str, str]
    confirmed: bool
    is_new_experiment: bool | None
    job_keys: tuple[tuple[str, int], ...] = ()
    confirmed_by: str | None = None
    confirmation_note: str | None = None

    @property
    def fingerprint_key(self) -> str:
        return "|".join(self.fingerprint)

    @property
    def is_baseline(self) -> bool:
        return self.ordinal == 1

    def to_dict(self) -> dict[str, Any]:
        return {
            "version_id": self.version_id,
            "lineage_id": self.lineage_id,
            "logical_run": self.logical_run,
            "label": self.label,
            "ordinal": self.ordinal,
            "fingerprint": list(self.fingerprint),
            "confirmed": self.confirmed,
            "is_new_experiment": self.is_new_experiment,
            "is_baseline": self.is_baseline,
            "job_keys": [list(key) for key in self.job_keys],
            "confirmed_by": self.confirmed_by,
            "confirmation_note": self.confirmation_note,
        }


@dataclass(frozen=True)
class VersionDecision:
    """作业负责人对某个实验版本是否成立的确认。"""

    logical_run: str
    fingerprint_key: str
    is_new_experiment: bool
    confirmed_by: str
    confirmed_at: datetime | None = None
    note: str | None = None


@dataclass(frozen=True)
class Certificate:
    """提交完整性凭证：下游可据此校验分区内容与来源版本。"""

    job_id: str
    logical_run: str
    attempt: int
    code_version: str
    input_snapshot: str
    parameter_hash: str
    resource_queue: str | None
    version_key: str
    version_label: str
    version_ordinal: int
    object_count: int
    total_bytes: int
    output_digest: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "logical_run": self.logical_run,
            "attempt": self.attempt,
            "code_version": self.code_version,
            "input_snapshot": self.input_snapshot,
            "parameter_hash": self.parameter_hash,
            "resource_queue": self.resource_queue,
            "version_key": self.version_key,
            "version_label": self.version_label,
            "version_ordinal": self.version_ordinal,
            "object_count": self.object_count,
            "total_bytes": self.total_bytes,
            "output_digest": self.output_digest,
        }


@dataclass
class Partition:
    """一个逻辑任务某次尝试的分区；同一 (logical_run, attempt) 至多一个。"""

    partition_id: str
    logical_run: str
    attempt: int
    state: PartitionState = PartitionState.STAGING
    version_key: str = ""
    version_label: str = ""
    version_ordinal: int = 0
    manifest_digest: str | None = None
    certificate: Certificate | None = None
    created_at: datetime | None = None
    committed_at: datetime | None = None
    blockers: tuple[str, ...] = ()
    recorded_state: str | None = None
    recorded_manifest_digest: str | None = None
    recovered_from: str | None = None
    recovery_action: str | None = None

    @property
    def visible(self) -> bool:
        return self.state is PartitionState.COMMITTED and self.certificate is not None

    @property
    def key(self) -> tuple[str, int]:
        return (self.logical_run, self.attempt)

    def to_dict(self, *, include_certificate: bool = True) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "partition_id": self.partition_id,
            "logical_run": self.logical_run,
            "attempt": self.attempt,
            "state": self.state.value,
            "visible": self.visible,
            "version_key": self.version_key,
            "version_label": self.version_label,
            "version_ordinal": self.version_ordinal,
            "manifest_digest": self.manifest_digest,
            "created_at": format_timestamp(self.created_at),
            "committed_at": format_timestamp(self.committed_at),
            "blockers": list(self.blockers),
            "recorded_state": self.recorded_state,
            "recorded_manifest_digest": self.recorded_manifest_digest,
            "recovered_from": self.recovered_from,
            "recovery_action": self.recovery_action,
        }
        if include_certificate:
            payload["certificate"] = (
                self.certificate.to_dict() if self.certificate is not None else None
            )
        return payload


@dataclass(frozen=True)
class RecordedPartition:
    """从存储快照读到的分区记录，未经账本确认。"""

    partition_id: str
    logical_run: str
    attempt: int
    commit_state: str
    committed_at: datetime
    manifest_digest: str
    consumer_refs: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "attempt", int(self.attempt))
        object.__setattr__(self, "committed_at", parse_timestamp(self.committed_at))
        object.__setattr__(self, "consumer_refs", int(self.consumer_refs))

    def to_dict(self) -> dict[str, Any]:
        return {
            "partition_id": self.partition_id,
            "logical_run": self.logical_run,
            "attempt": self.attempt,
            "commit_state": self.commit_state,
            "committed_at": format_timestamp(self.committed_at),
            "manifest_digest": self.manifest_digest,
            "consumer_refs": self.consumer_refs,
        }


@dataclass(frozen=True)
class ReferenceRecord:
    """下游对某内容（校验和）或某个对象键的引用。"""

    reference_id: str
    target_kind: ReferenceTarget
    target: str
    reference_state: ReferenceState
    usage_scope: str | None = None
    access_class: str | None = None
    source: str | None = None

    def __post_init__(self) -> None:
        if not self.reference_id or not self.target:
            raise ValueError("reference_id 与 target 不能为空")
        object.__setattr__(
            self, "target_kind", as_enum(ReferenceTarget, self.target_kind)
        )
        object.__setattr__(
            self, "reference_state", as_enum(ReferenceState, self.reference_state)
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "reference_id": self.reference_id,
            "target_kind": self.target_kind.value,
            "target": self.target,
            "reference_state": self.reference_state.value,
            "usage_scope": self.usage_scope,
            "access_class": self.access_class,
            "source": self.source,
        }


@dataclass(frozen=True)
class CompletenessReport:
    """提交完整性评估结果；reasons 为空才允许提交。"""

    job_id: str
    logical_run: str
    attempt: int
    complete: bool
    reasons: tuple[str, ...]
    job_state: JobState
    success_receipt: bool
    failure_receipt: bool
    retry_count: int
    output_count: int
    written_count: int
    partial_count: int
    pending_count: int
    total_bytes: int
    output_digest: str | None
    version_id: str | None
    version_label: str | None
    version_ordinal: int
    version_confirmed: bool

    @property
    def reason_text(self) -> tuple[str, ...]:
        return tuple(
            EXPLANATIONS.get(code.split(":", 1)[0], code) for code in self.reasons
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "logical_run": self.logical_run,
            "attempt": self.attempt,
            "complete": self.complete,
            "reasons": list(self.reasons),
            "reason_text": list(self.reason_text),
            "job_state": self.job_state.value,
            "success_receipt": self.success_receipt,
            "failure_receipt": self.failure_receipt,
            "retry_count": self.retry_count,
            "output_count": self.output_count,
            "written_count": self.written_count,
            "partial_count": self.partial_count,
            "pending_count": self.pending_count,
            "total_bytes": self.total_bytes,
            "output_digest": self.output_digest,
            "version_id": self.version_id,
            "version_label": self.version_label,
            "version_ordinal": self.version_ordinal,
            "version_confirmed": self.version_confirmed,
        }


EXPLANATIONS: dict[str, str] = {
    "unknown_job": "作业尝试未登记",
    "no_outputs": "没有登记任何输出对象",
    "outputs_pending": "存在仍待写入的输出对象",
    "outputs_partial": "存在只写出一部分的输出对象",
    "outputs_unknown": "存在写入状态未知的输出对象",
    "job_state": "任务状态不满足完成要求",
    "missing_success_receipt": "缺少成功完成回执",
    "receipt_failure": "完成回执为失败",
    "version_missing": "无法确定实验版本",
    "version_unconfirmed": "重跑版本尚未由作业负责人确认",
    "attempt_superseded": "同一版本已有更新的可见尝试",
    "partition_quarantined": "分区已被隔离",
}


@dataclass(frozen=True)
class Snapshot:
    """分析用户可消费的已提交快照。"""

    partition: Partition
    job: JobAttempt
    version: VersionRecord
    certificate: Certificate
    objects: tuple[OutputObject, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "partition": self.partition.to_dict(),
            "job": self.job.to_dict(),
            "version": self.version.to_dict(),
            "certificate": self.certificate.to_dict(),
            "objects": [obj.to_dict() for obj in self.objects],
        }


@dataclass(frozen=True)
class CommitResult:
    ok: bool
    partition: Partition
    action: str
    reasons: tuple[str, ...] = ()

    @property
    def visible(self) -> bool:
        return self.partition.visible

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "action": self.action,
            "reasons": list(self.reasons),
            "partition": self.partition.to_dict(),
        }


@dataclass(frozen=True)
class RecoveryAction:
    partition_id: str
    logical_run: str
    attempt: int
    recorded_state: str
    resulting_state: str
    action: str
    reasons: tuple[str, ...]
    version_id: str | None
    version_confirmed: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "partition_id": self.partition_id,
            "logical_run": self.logical_run,
            "attempt": self.attempt,
            "recorded_state": self.recorded_state,
            "resulting_state": self.resulting_state,
            "action": self.action,
            "reasons": list(self.reasons),
            "version_id": self.version_id,
            "version_confirmed": self.version_confirmed,
        }


@dataclass(frozen=True)
class RecoveryReport:
    actions: tuple[RecoveryAction, ...] = ()
    created: tuple[str, ...] = ()
    promoted: tuple[str, ...] = ()
    verified: tuple[str, ...] = ()
    quarantined: tuple[str, ...] = ()
    held: tuple[str, ...] = ()
    staged: tuple[str, ...] = ()
    skipped: tuple[str, ...] = ()
    superseded: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "actions": [action.to_dict() for action in self.actions],
            "counts": {
                "created": len(self.created),
                "promoted": len(self.promoted),
                "verified": len(self.verified),
                "quarantined": len(self.quarantined),
                "held": len(self.held),
                "staged": len(self.staged),
                "skipped": len(self.skipped),
                "superseded": len(self.superseded),
            },
            "created": list(self.created),
            "promoted": list(self.promoted),
            "verified": list(self.verified),
            "quarantined": list(self.quarantined),
            "held": list(self.held),
            "staged": list(self.staged),
            "skipped": list(self.skipped),
            "superseded": list(self.superseded),
        }


@dataclass(frozen=True)
class CleanupItem:
    object_key: str
    job_id: str
    attempt: int
    write_state: WriteState
    size_bytes: int
    action: CleanupAction
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "object_key": self.object_key,
            "job_id": self.job_id,
            "attempt": self.attempt,
            "write_state": self.write_state.value,
            "size_bytes": self.size_bytes,
            "action": self.action.value,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class CleanupPlan:
    items: tuple[CleanupItem, ...]
    protected_bytes: int
    reclaimable_bytes: int
    held_bytes: int

    def _of(self, action: CleanupAction) -> tuple[CleanupItem, ...]:
        return tuple(item for item in self.items if item.action is action)

    @property
    def deletable(self) -> tuple[CleanupItem, ...]:
        return self._of(CleanupAction.DELETE)

    @property
    def quarantined(self) -> tuple[CleanupItem, ...]:
        return self._of(CleanupAction.QUARANTINE)

    @property
    def protected(self) -> tuple[CleanupItem, ...]:
        return self._of(CleanupAction.PROTECT)

    @property
    def held(self) -> tuple[CleanupItem, ...]:
        return self._of(CleanupAction.HOLD)

    def to_dict(self) -> dict[str, Any]:
        return {
            "items": [item.to_dict() for item in self.items],
            "counts": {
                action.value: len(self._of(action)) for action in CleanupAction
            },
            "protected_bytes": self.protected_bytes,
            "reclaimable_bytes": self.reclaimable_bytes,
            "held_bytes": self.held_bytes,
        }


@dataclass(frozen=True)
class Anomaly:
    """需要人工关注的输入异常或状态冲突。"""

    code: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    key_suffix: str = field(default="", compare=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "details": dict(self.details),
            "key_suffix": self.key_suffix,
        }
