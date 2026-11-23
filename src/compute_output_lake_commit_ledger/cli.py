"""命令行：装载合成资料、恢复提交状态、输出可消费快照与清理计划。"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Sequence

from .ledger import LakeCommitLedger
from .loader import load_data_dir
from .models import (
    LakeLedgerError,
    PartitionNotVisible,
    SnapshotNotFound,
    UnknownJobError,
)

ROOT = Path(__file__).resolve().parents[2]


def _print(payload: Any) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _ensure_data_dir(path: str | None) -> Path:
    candidate = Path(path) if path else ROOT / "data"
    if not candidate.is_dir():
        raise SystemExit(f"资料目录不存在：{candidate}")
    return candidate


def _build_ledger(data_dir: Path, confirm_file: str | None) -> tuple[LakeCommitLedger, Any]:
    dataset = load_data_dir(data_dir)
    if confirm_file:
        confirmations = json.loads(Path(confirm_file).read_text(encoding="utf-8"))
        for item in confirmations:
            dataset.ledger.confirm_version(
                item["logical_run"],
                version_id=item.get("version_id"),
                job_id=item.get("job_id"),
                attempt=item.get("attempt"),
                fingerprint=tuple(item["fingerprint"]) if item.get("fingerprint") else None,
                confirmed_by=item["confirmed_by"],
                note=item.get("note"),
                is_new_experiment=bool(item.get("is_new_experiment", True)),
                confirmed_at=item.get("confirmed_at"),
            )
    dataset.recover()
    return dataset.ledger, dataset


def cmd_inspect(args: argparse.Namespace) -> int:
    _, dataset = _build_ledger(_ensure_data_dir(args.data), args.confirm)
    _print(
        {
            "reconcile": dataset.reconcile.to_dict(),
            "summary": dataset.ledger.summary(),
        }
    )
    return 0


def cmd_commit(args: argparse.Namespace) -> int:
    ledger, _ = _build_ledger(_ensure_data_dir(args.data), args.confirm)
    try:
        if args.partition:
            # 允许对存储快照中尚未建立的分区直接指定标识。
            result = ledger.commit(
                args.run, args.attempt, partition_id=args.partition
            )
        else:
            result = ledger.commit(args.run, args.attempt)
    except (UnknownJobError, LakeLedgerError) as exc:
        _print({"error": str(exc), "type": type(exc).__name__})
        return 1
    _print(result.to_dict())
    return 0 if result.ok else 1


def cmd_recover(args: argparse.Namespace) -> int:
    _, dataset = _build_ledger(_ensure_data_dir(args.data), args.confirm)
    # _build_ledger 已执行一次恢复；再次执行展示幂等性。
    report = dataset.recover()
    _print(report.to_dict())
    return 0


def cmd_partitions(args: argparse.Namespace) -> int:
    ledger, _ = _build_ledger(_ensure_data_dir(args.data), args.confirm)
    payload = {"partitions": [p.to_dict() for p in ledger.partitions(args.run)]}
    if args.output:
        Path(args.output).write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    _print(payload)
    return 0


def cmd_snapshot(args: argparse.Namespace) -> int:
    ledger, _ = _build_ledger(_ensure_data_dir(args.data), args.confirm)
    try:
        if args.partition:
            snapshot = ledger.snapshot_for_partition(args.partition)
        else:
            snapshot = ledger.select_snapshot(
                args.run,
                version_ordinal=args.version,
                require_confirmed=not args.include_unconfirmed,
            )
    except (PartitionNotVisible, SnapshotNotFound) as exc:
        _print({"error": str(exc)})
        return 1
    _print(snapshot.to_dict())
    return 0


def cmd_references(args: argparse.Namespace) -> int:
    ledger, _ = _build_ledger(_ensure_data_dir(args.data), args.confirm)
    if args.object:
        references = ledger.references_to_object(args.object)
    elif args.partition:
        references = ledger.references_to_partition(args.partition)
    elif args.run:
        references = ledger.references_to_run(args.run)
    else:
        raise SystemExit("请提供 --object、--partition 或 --run 之一")
    _print({"references": [reference.to_dict() for reference in references]})
    return 0


def cmd_cleanup(args: argparse.Namespace) -> int:
    ledger, _ = _build_ledger(_ensure_data_dir(args.data), args.confirm)
    plan = ledger.cleanup_plan(logical_run=args.run)
    payload = plan.to_dict()
    if args.apply and not args.yes:
        _print({"error": "实际执行清理需要同时提供 --yes"})
        return 1
    if args.apply:
        payload["applied"] = ledger.apply_cleanup(plan, confirm=True)
    _print(payload)
    return 0


def cmd_state(args: argparse.Namespace) -> int:
    if args.load:
        ledger = LakeCommitLedger.load_state(args.load)
    else:
        ledger, _ = _build_ledger(_ensure_data_dir(args.data), args.confirm)
    if args.save:
        ledger.save_state(args.save)
        _print({"saved": str(args.save)})
        return 0
    _print(ledger.summary())
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lake-ledger",
        description="科研计算输出的提交账本与分区可见性管理（本地合成资料）",
    )
    parser.add_argument("--data", help="资料目录，默认为项目 data/")
    parser.add_argument(
        "--confirm", help="实验版本确认 JSON 文件的路径（作业负责人填写）"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    inspect = sub.add_parser("inspect", help="装载资料并输出对齐统计与账本摘要")
    inspect.set_defaults(func=cmd_inspect)

    commit = sub.add_parser("commit", help="对指定逻辑任务尝试执行提交")
    commit.add_argument("run")
    commit.add_argument("attempt", type=int)
    commit.add_argument("--partition", help="显式指定分区标识（用于补建存储未知的分区）")
    commit.set_defaults(func=cmd_commit)

    recover = sub.add_parser("recover", help="重放存储快照的提交恢复流程")
    recover.set_defaults(func=cmd_recover)

    partitions = sub.add_parser("partitions", help="列出分区及其可见性")
    partitions.add_argument("--run")
    partitions.add_argument("--output", help="把结果另存为 JSON 文件")
    partitions.set_defaults(func=cmd_partitions)

    snapshot = sub.add_parser("snapshot", help="按提交账本选择可消费快照")
    snapshot.add_argument("--run", help="逻辑任务标识")
    snapshot.add_argument("--partition", help="直接指定分区标识")
    snapshot.add_argument("--version", type=int, help="限定版本序号")
    snapshot.add_argument(
        "--include-unconfirmed",
        action="store_true",
        help="允许选择尚未由负责人确认的重跑版本",
    )
    snapshot.set_defaults(func=cmd_snapshot)

    references = sub.add_parser("references", help="查询引用关系")
    references.add_argument("--object")
    references.add_argument("--partition")
    references.add_argument("--run")
    references.set_defaults(func=cmd_references)

    cleanup = sub.add_parser("cleanup", help="生成或执行未完成输出的清理计划")
    cleanup.add_argument("--run")
    cleanup.add_argument("--apply", action="store_true", help="执行清理（仅账本登记）")
    cleanup.add_argument("--yes", action="store_true", help="确认执行")
    cleanup.set_defaults(func=cmd_cleanup)

    state = sub.add_parser("state", help="保存或载入账本状态")
    state.add_argument("--save")
    state.add_argument("--load")
    state.set_defaults(func=cmd_state)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
