import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from compute_output_lake_commit_ledger import (  # noqa: E402
    CleanupAction,
    CompletionReceipt,
    JobAttempt,
    JobState,
    LakeCommitLedger,
    OutputObject,
    PartitionNotVisible,
    PartitionState,
    ReceiptStatus,
    ReceiptType,
    RecordedPartition,
    ReferenceRecord,
    ReferenceState,
    ReferenceTarget,
    SnapshotNotFound,
    WriteState,
    load_data_dir,
)

BASE = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)


def job(
    job_id="JOB-0001",
    run="RUN-001",
    attempt=1,
    code="GIT-001",
    snapshot="SS-001",
    params="PAR-1",
    state=JobState.COMPLETE,
    finished=None,
):
    return JobAttempt(
        job_id=job_id,
        logical_run=run,
        code_version=code,
        input_snapshot=snapshot,
        parameter_hash=params,
        attempt=attempt,
        state=state,
        finished_at=finished or (BASE + timedelta(hours=attempt)),
    )


def output(
    job_id="JOB-0001",
    attempt=1,
    key="result/01/00001",
    state=WriteState.WRITTEN,
    size=100,
    checksum="SHA-001",
    written=None,
):
    return OutputObject(
        job_id=job_id,
        attempt=attempt,
        object_key=key,
        size_bytes=size,
        checksum=checksum,
        write_state=state,
        written_at=written or BASE,
    )


def receipt(
    receipt_id="RC-0001",
    job_id="JOB-0001",
    attempt=1,
    kind=ReceiptType.SUCCESS,
    key="IDEM-001",
    at=None,
):
    return CompletionReceipt(
        receipt_id=receipt_id,
        job_id=job_id,
        attempt=attempt,
        receipt_type=kind,
        received_at=at or (BASE + timedelta(hours=1)),
        idempotency_key=key,
    )


def new_ledger_with_job(**kwargs):
    ledger = LakeCommitLedger()
    ledger.register_job(job(**kwargs))
    return ledger


class CommitAndVisibilityTests(unittest.TestCase):
    def test_commit_blocks_when_outputs_incomplete(self):
        ledger = new_ledger_with_job()
        ledger.register_output(output(state=WriteState.PARTIAL))
        ledger.register_receipt(receipt())

        result = ledger.commit("RUN-001", 1)

        self.assertFalse(result.ok)
        self.assertFalse(result.visible)
        self.assertIn("outputs_partial", result.reasons)
        self.assertEqual(result.partition.state, PartitionState.STAGING)

    def test_commit_blocks_without_success_receipt(self):
        ledger = new_ledger_with_job()
        ledger.register_output(output())
        ledger.register_receipt(receipt(kind=ReceiptType.FAILURE))

        result = ledger.commit("RUN-001", 1)

        self.assertFalse(result.ok)
        self.assertIn("receipt_failure", result.reasons)

    def test_commit_writes_certificate_and_becomes_visible(self):
        ledger = new_ledger_with_job()
        ledger.register_output(output())
        ledger.register_output(
            output(key="result/02/00002", checksum="SHA-002", size=250)
        )
        ledger.register_receipt(receipt())

        result = ledger.commit("RUN-001", 1)

        self.assertTrue(result.ok)
        self.assertTrue(result.visible)
        certificate = result.partition.certificate
        self.assertEqual(certificate.object_count, 2)
        self.assertEqual(certificate.total_bytes, 350)
        self.assertEqual(certificate.code_version, "GIT-001")
        self.assertEqual(certificate.input_snapshot, "SS-001")
        self.assertTrue(certificate.output_digest.startswith("sha256:"))

    def test_commit_is_idempotent_and_creates_one_partition(self):
        ledger = new_ledger_with_job()
        ledger.register_output(output())
        ledger.register_receipt(receipt())

        first = ledger.commit("RUN-001", 1)
        second = ledger.commit("RUN-001", 1)

        self.assertEqual(len(ledger.partitions()), 1)
        self.assertEqual(first.partition.partition_id, second.partition.partition_id)
        self.assertEqual(second.action, "verified")
        self.assertTrue(second.visible)

    def test_non_visible_partition_cannot_be_consumed(self):
        ledger = new_ledger_with_job()
        ledger.register_output(output(state=WriteState.PENDING))
        result = ledger.commit("RUN-001", 1)

        with self.assertRaises(PartitionNotVisible):
            ledger.snapshot_for_partition(result.partition.partition_id)

    def test_duplicate_success_receipts_do_not_create_second_partition(self):
        ledger = new_ledger_with_job()
        ledger.register_output(output())
        ledger.register_receipt(receipt())

        duplicate_same_key = ledger.register_receipt(
            receipt(receipt_id="RC-0002", at=BASE + timedelta(hours=2))
        )
        duplicate_new_key = ledger.register_receipt(
            receipt(
                receipt_id="RC-0003",
                key="IDEM-OTHER",
                at=BASE + timedelta(hours=3),
            )
        )
        ledger.commit("RUN-001", 1)
        ledger.register_receipt(
            receipt(receipt_id="RC-0004", key="IDEM-THIRD", at=BASE + timedelta(hours=4))
        )

        self.assertEqual(duplicate_same_key.status, ReceiptStatus.DUPLICATE)
        self.assertIn(
            duplicate_new_key.status,
            (ReceiptStatus.LATE_IGNORED, ReceiptStatus.DUPLICATE),
        )
        self.assertEqual(len(ledger.partitions()), 1)

    def test_late_failure_does_not_revoke_committed_version(self):
        ledger = new_ledger_with_job()
        ledger.register_output(output())
        ledger.register_receipt(receipt())
        committed = ledger.commit("RUN-001", 1)
        self.assertTrue(committed.visible)

        application = ledger.register_receipt(
            receipt(
                receipt_id="RC-LATE",
                kind=ReceiptType.FAILURE,
                key="IDEM-LATE",
                at=BASE + timedelta(days=5),
            )
        )

        self.assertEqual(application.status, ReceiptStatus.LATE_IGNORED)
        self.assertTrue(ledger.partition_for("RUN-001", 1).visible)
        self.assertEqual(
            ledger.outcome("JOB-0001", 1).terminal, ReceiptType.SUCCESS
        )

    def test_success_after_failure_becomes_terminal(self):
        ledger = new_ledger_with_job()
        ledger.register_output(output())
        ledger.register_receipt(
            receipt(receipt_id="RC-F", kind=ReceiptType.FAILURE, key="IDEM-F")
        )
        ledger.register_receipt(
            receipt(
                receipt_id="RC-S",
                kind=ReceiptType.SUCCESS,
                key="IDEM-S",
                at=BASE + timedelta(hours=2),
            )
        )

        self.assertTrue(ledger.commit("RUN-001", 1).ok)
        self.assertEqual(ledger.outcome("JOB-0001", 1).terminal, ReceiptType.SUCCESS)

    def test_idempotency_key_is_scoped_per_attempt(self):
        ledger = LakeCommitLedger()
        ledger.register_job(job(job_id="JOB-0001", attempt=1))
        ledger.register_job(job(job_id="JOB-0002", attempt=2, params="PAR-2"))
        ledger.register_output(output(job_id="JOB-0001", attempt=1))
        ledger.register_output(
            output(job_id="JOB-0002", attempt=2, key="result/02/00002")
        )

        first = ledger.register_receipt(receipt(job_id="JOB-0001", attempt=1))
        second = ledger.register_receipt(
            receipt(receipt_id="RC-0002", job_id="JOB-0002", attempt=2)
        )

        self.assertTrue(first.applied)
        self.assertTrue(second.applied)

    def test_same_version_later_attempt_supersedes_older(self):
        ledger = LakeCommitLedger()
        ledger.register_job(job(job_id="JOB-0001", attempt=1))
        ledger.register_job(job(job_id="JOB-0002", attempt=2))
        for attempt, key, receipt_id in (
            (1, "result/01/00001", "RC-1"),
            (2, "result/02/00002", "RC-2"),
        ):
            ledger.register_output(
                output(job_id=f"JOB-000{attempt}", attempt=attempt, key=key)
            )
            ledger.register_receipt(
                receipt(
                    receipt_id=receipt_id,
                    job_id=f"JOB-000{attempt}",
                    attempt=attempt,
                    key=f"IDEM-{attempt}",
                    at=BASE + timedelta(hours=attempt),
                )
            )

        self.assertTrue(ledger.commit("RUN-001", 2).ok)
        report = ledger.completeness("RUN-001", 1)

        self.assertFalse(report.complete)
        self.assertIn("attempt_superseded", report.reasons)

    def test_newer_version_does_not_supersede_baseline(self):
        ledger = LakeCommitLedger()
        ledger.register_job(job(job_id="JOB-0001", attempt=1))
        ledger.register_job(job(job_id="JOB-0002", attempt=2, params="PAR-2"))
        for attempt, key, receipt_id in (
            (1, "result/01/00001", "RC-1"),
            (2, "result/02/00002", "RC-2"),
        ):
            ledger.register_output(
                output(job_id=f"JOB-000{attempt}", attempt=attempt, key=key)
            )
            ledger.register_receipt(
                receipt(
                    receipt_id=receipt_id,
                    job_id=f"JOB-000{attempt}",
                    attempt=attempt,
                    key=f"IDEM-{attempt}",
                    at=BASE + timedelta(hours=attempt),
                )
            )
        ledger.confirm_version("RUN-001", job_id="JOB-0002", confirmed_by="负责人")
        ledger.commit("RUN-001", 2)

        baseline = ledger.completeness("RUN-001", 1)

        self.assertTrue(baseline.complete)
        self.assertTrue(ledger.commit("RUN-001", 1).ok)
        self.assertEqual(len(ledger.visible_partitions("RUN-001")), 2)


class VersionTests(unittest.TestCase):
    def _ledger_with_rerun(self):
        ledger = LakeCommitLedger()
        ledger.register_job(job(job_id="JOB-0001", attempt=1))
        ledger.register_job(
            job(job_id="JOB-0002", attempt=2, code="GIT-002", params="PAR-2")
        )
        for attempt, key, receipt_id in (
            (1, "result/01/00001", "RC-1"),
            (2, "result/02/00002", "RC-2"),
        ):
            ledger.register_output(
                output(
                    job_id=f"JOB-000{attempt}",
                    attempt=attempt,
                    key=key,
                    checksum=f"SHA-00{attempt}",
                )
            )
            ledger.register_receipt(
                receipt(
                    receipt_id=receipt_id,
                    job_id=f"JOB-000{attempt}",
                    attempt=attempt,
                    key=f"IDEM-{attempt}",
                    at=BASE + timedelta(hours=attempt),
                )
            )
        return ledger

    def test_unconfirmed_rerun_is_not_eligible(self):
        ledger = self._ledger_with_rerun()

        report = ledger.completeness("RUN-001", 2)

        self.assertFalse(report.complete)
        self.assertIn("version_unconfirmed", report.reasons)
        self.assertEqual(report.version_ordinal, 2)
        self.assertFalse(report.version_confirmed)

    def test_owner_confirmation_makes_rerun_eligible_as_new_version(self):
        ledger = self._ledger_with_rerun()

        version = ledger.confirm_version(
            "RUN-001", job_id="JOB-0002", confirmed_by="负责人甲", note="新参数"
        )
        ledger.commit("RUN-001", 2)

        self.assertTrue(version.confirmed)
        self.assertTrue(version.is_new_experiment)
        self.assertEqual(version.version_id, "RUN-001:V2")
        snapshot = ledger.select_snapshot("RUN-001")
        self.assertEqual(snapshot.version.version_id, "RUN-001:V2")
        self.assertEqual(snapshot.job.code_version, "GIT-002")

    def test_owner_can_declare_rerun_as_same_experiment(self):
        ledger = self._ledger_with_rerun()

        version = ledger.confirm_version(
            "RUN-001",
            job_id="JOB-0002",
            confirmed_by="负责人乙",
            is_new_experiment=False,
            note="只是重跑同一实验",
        )
        result = ledger.commit("RUN-001", 2)

        self.assertFalse(version.confirmed)
        self.assertFalse(version.is_new_experiment)
        self.assertFalse(result.ok)
        self.assertIn("version_declined", result.reasons)

    def test_previous_version_remains_selectable(self):
        ledger = self._ledger_with_rerun()
        ledger.commit("RUN-001", 1)
        ledger.confirm_version("RUN-001", job_id="JOB-0002", confirmed_by="负责人")
        ledger.commit("RUN-001", 2)

        latest = ledger.select_snapshot("RUN-001")
        baseline = ledger.select_snapshot("RUN-001", version_ordinal=1)

        self.assertEqual(latest.version.ordinal, 2)
        self.assertEqual(baseline.version.ordinal, 1)
        self.assertEqual(baseline.job.job_id, "JOB-0001")

    def test_select_snapshot_rejects_unconfirmed(self):
        ledger = self._ledger_with_rerun()
        ledger.commit("RUN-001", 1)

        with self.assertRaises(SnapshotNotFound):
            ledger.select_snapshot("RUN-001", version_ordinal=2)
        snapshot = ledger.select_snapshot("RUN-001", require_confirmed=False)
        self.assertEqual(snapshot.version.ordinal, 1)

    def test_commit_refuses_unconfirmed_rerun_partition(self):
        ledger = self._ledger_with_rerun()
        result = ledger.commit("RUN-001", 2)

        self.assertFalse(result.ok)
        self.assertFalse(result.visible)
        self.assertIn("version_unconfirmed", result.reasons)

    def test_versions_are_grouped_by_fingerprint(self):
        ledger = self._ledger_with_rerun()
        ledger.register_job(
            job(job_id="JOB-0003", attempt=3, code="GIT-002", params="PAR-2")
        )
        versions = ledger.versions("RUN-001")

        self.assertEqual(len(versions), 2)
        self.assertEqual(versions[1].fingerprint, ("GIT-002", "SS-001", "PAR-2"))
        self.assertIn(("JOB-0002", 2), versions[1].job_keys)
        self.assertIn(("JOB-0003", 3), versions[1].job_keys)


class RecoveryTests(unittest.TestCase):
    def _ledger(self):
        ledger = LakeCommitLedger()
        ledger.register_job(job(job_id="JOB-0001", attempt=1))
        ledger.register_job(job(job_id="JOB-0002", attempt=2, params="PAR-2"))
        for attempt, key in ((1, "result/01/00001"), (2, "result/02/00002")):
            ledger.register_output(
                output(job_id=f"JOB-000{attempt}", attempt=attempt, key=key)
            )
            ledger.register_receipt(
                receipt(
                    receipt_id=f"RC-{attempt}",
                    job_id=f"JOB-000{attempt}",
                    attempt=attempt,
                    key=f"IDEM-{attempt}",
                    at=BASE + timedelta(hours=attempt),
                )
            )
        ledger.confirm_version("RUN-001", job_id="JOB-0002", confirmed_by="负责人")
        return ledger

    def test_recovery_promotes_complete_staging_partition(self):
        ledger = self._ledger()
        recorded = [
            RecordedPartition(
                "P-0001", "RUN-001", 2, "staging", BASE + timedelta(days=1), "MAN-1", 0
            )
        ]

        report = ledger.recover(recorded)

        self.assertIn("P-0001", report.promoted)
        self.assertTrue(ledger.get_partition("P-0001").visible)
        self.assertEqual(ledger.get_partition("P-0001").certificate.attempt, 2)

    def test_recovery_quarantines_recorded_committed_but_incomplete(self):
        ledger = LakeCommitLedger()
        ledger.register_job(job(job_id="JOB-0001", attempt=1))
        ledger.register_output(output(state=WriteState.PARTIAL))
        ledger.register_receipt(receipt())
        recorded = [
            RecordedPartition(
                "P-0001", "RUN-001", 1, "committed", BASE + timedelta(days=1), "MAN-1", 3
            )
        ]

        report = ledger.recover(recorded)

        self.assertIn("P-0001", report.quarantined)
        partition = ledger.get_partition("P-0001")
        self.assertEqual(partition.state, PartitionState.QUARANTINED)
        self.assertFalse(partition.visible)
        self.assertIn("outputs_partial", partition.blockers)

    def test_recovery_supersedes_old_attempt_when_newer_same_version_visible(self):
        ledger = LakeCommitLedger()
        ledger.register_job(job(job_id="JOB-0001", attempt=1))
        ledger.register_job(job(job_id="JOB-0002", attempt=2))
        for attempt, key in ((1, "result/01/00001"), (2, "result/02/00002")):
            ledger.register_output(
                output(job_id=f"JOB-000{attempt}", attempt=attempt, key=key)
            )
            ledger.register_receipt(
                receipt(
                    receipt_id=f"RC-{attempt}",
                    job_id=f"JOB-000{attempt}",
                    attempt=attempt,
                    key=f"IDEM-{attempt}",
                    at=BASE + timedelta(hours=attempt),
                )
            )
        recorded = [
            RecordedPartition("P-0001", "RUN-001", 1, "committed", BASE, "MAN-1", 1),
            RecordedPartition(
                "P-0002", "RUN-001", 2, "committed", BASE + timedelta(days=1), "MAN-2", 1
            ),
        ]

        report = ledger.recover(recorded)

        self.assertIn("P-0002", report.verified)
        self.assertIn("P-0001", report.superseded)
        self.assertEqual(
            ledger.get_partition("P-0001").state, PartitionState.SUPERSEDED
        )
        self.assertTrue(ledger.get_partition("P-0002").visible)

    def test_recovery_verifies_both_versions_of_rerun(self):
        ledger = self._ledger()
        recorded = [
            RecordedPartition("P-0001", "RUN-001", 1, "committed", BASE, "MAN-1", 1),
            RecordedPartition(
                "P-0002", "RUN-001", 2, "committed", BASE + timedelta(days=1), "MAN-2", 1
            ),
        ]

        report = ledger.recover(recorded)

        self.assertEqual(set(report.verified), {"P-0001", "P-0002"})
        self.assertEqual(len(ledger.visible_partitions("RUN-001")), 2)

    def test_recovery_is_idempotent(self):
        ledger = self._ledger()
        recorded = [
            RecordedPartition(
                "P-0001", "RUN-001", 2, "staging", BASE + timedelta(days=1), "MAN-1", 0
            )
        ]

        first = ledger.recover(recorded)
        second = ledger.recover(recorded)

        self.assertIn("P-0001", first.promoted)
        self.assertEqual(len(ledger.partitions()), 2)  # P-0001 + 补建的基线合格分区
        self.assertTrue(ledger.get_partition("P-0001").visible)
        self.assertIn("P-0001", second.verified)
        self.assertEqual(len(ledger.visible_partitions()), 2)
        self.assertEqual(ledger.get_partition("P-0001").state, PartitionState.COMMITTED)

    def test_recovery_records_partition_without_job(self):
        ledger = self._ledger()
        recorded = [
            RecordedPartition(
                "P-9999", "RUN-404", 1, "committed", BASE, "MAN-9", 0
            )
        ]

        report = ledger.recover(recorded)

        self.assertIn("P-9999", report.skipped)
        codes = {anomaly.code for anomaly in ledger.anomalies}
        self.assertIn("partition_without_job", codes)

    def test_recovery_without_auto_commit_keeps_staging(self):
        ledger = self._ledger()
        recorded = [
            RecordedPartition(
                "P-0001", "RUN-001", 2, "staging", BASE + timedelta(days=1), "MAN-1", 0
            )
        ]

        report = ledger.recover(recorded, auto_commit=False)

        self.assertIn("P-0001", report.staged)
        self.assertFalse(ledger.get_partition("P-0001").visible)


class ReferenceAndCleanupTests(unittest.TestCase):
    def _ledger(self):
        ledger = LakeCommitLedger()
        ledger.register_job(
            job(job_id="JOB-0001", state=JobState.FAILED)
        )
        ledger.register_output(
            output(key="result/01/00001", state=WriteState.PARTIAL, checksum="SHA-001")
        )
        ledger.register_output(
            output(key="result/02/00002", state=WriteState.WRITTEN, checksum="SHA-002")
        )
        return ledger

    def test_valid_reference_protects_written_object(self):
        ledger = self._ledger()
        ledger.register_reference(
            ReferenceRecord(
                reference_id="RES-1",
                target_kind=ReferenceTarget.OBJECT_KEY,
                target="result/02/00002",
                reference_state=ReferenceState.VALID,
            )
        )

        plan = ledger.cleanup_plan()
        actions = {item.object_key: item.action for item in plan.items}

        self.assertEqual(actions["result/02/00002"], CleanupAction.PROTECT)
        self.assertEqual(actions["result/01/00001"], CleanupAction.DELETE)

    def test_incomplete_object_with_valid_reference_is_held(self):
        ledger = self._ledger()
        ledger.register_reference(
            ReferenceRecord(
                reference_id="RES-1",
                target_kind=ReferenceTarget.OBJECT_KEY,
                target="result/01/00001",
                reference_state=ReferenceState.VALID,
            )
        )

        plan = ledger.cleanup_plan()
        item = next(
            item for item in plan.items if item.object_key == "result/01/00001"
        )

        self.assertEqual(item.action, CleanupAction.HOLD)

    def test_review_reference_is_held_not_deleted(self):
        ledger = self._ledger()
        ledger.register_reference(
            ReferenceRecord(
                reference_id="RES-REVIEW",
                target_kind=ReferenceTarget.OBJECT_KEY,
                target="result/01/00001",
                reference_state=ReferenceState.REVIEW,
            )
        )

        plan = ledger.cleanup_plan()
        item = next(
            item for item in plan.items if item.object_key == "result/01/00001"
        )

        self.assertEqual(item.action, CleanupAction.HOLD)

    def test_committed_objects_are_protected(self):
        ledger = LakeCommitLedger()
        ledger.register_job(job(job_id="JOB-0001"))
        ledger.register_output(output())
        ledger.register_receipt(receipt())
        ledger.commit("RUN-001", 1)

        plan = ledger.cleanup_plan()

        self.assertEqual(plan.items[0].action, CleanupAction.PROTECT)
        self.assertEqual(plan.reclaimable_bytes, 0)

    def test_cleanup_dry_run_does_not_mutate(self):
        ledger = self._ledger()
        plan = ledger.cleanup_plan()

        outcome = ledger.apply_cleanup(plan, confirm=False)

        self.assertFalse(outcome["applied"])
        self.assertEqual(len(ledger.outputs_for("JOB-0001", 1)), 2)

    def test_cleanup_apply_removes_only_deletable(self):
        ledger = self._ledger()
        ledger.register_reference(
            ReferenceRecord(
                reference_id="RES-1",
                target_kind=ReferenceTarget.OBJECT_KEY,
                target="result/02/00002",
                reference_state=ReferenceState.VALID,
            )
        )
        plan = ledger.cleanup_plan()

        ledger.apply_cleanup(plan, confirm=True)

        self.assertEqual(
            [obj.object_key for obj in ledger.outputs_for("JOB-0001", 1)],
            ["result/02/00002"],
        )

    def test_reference_to_checksum_resolves_objects(self):
        ledger = self._ledger()
        ledger.register_reference(
            ReferenceRecord(
                reference_id="RES-CHK",
                target_kind=ReferenceTarget.CHECKSUM,
                target="SHA-002",
                reference_state=ReferenceState.VALID,
            )
        )

        objects = ledger.objects_for_reference("RES-CHK")
        plan = ledger.cleanup_plan()
        item = next(
            item for item in plan.items if item.object_key == "result/02/00002"
        )

        self.assertEqual([obj.object_key for obj in objects], ["result/02/00002"])
        self.assertEqual(item.action, CleanupAction.PROTECT)

    def test_quarantine_clears_certificate_and_blocks_commit(self):
        ledger = LakeCommitLedger()
        ledger.register_job(job(job_id="JOB-0001"))
        ledger.register_output(output())
        ledger.register_receipt(receipt())
        committed = ledger.commit("RUN-001", 1)
        self.assertTrue(committed.visible)

        partition = ledger.quarantine(
            committed.partition.partition_id, reasons=("manual_review",)
        )
        result = ledger.commit("RUN-001", 1)

        self.assertIsNone(partition.certificate)
        self.assertFalse(result.ok)
        self.assertIn("partition_quarantined", result.reasons)
        self.assertEqual(partition.state, PartitionState.QUARANTINED)

    def test_late_failure_after_commit_does_not_change_partition(self):
        ledger = LakeCommitLedger()
        ledger.register_job(job(job_id="JOB-0001"))
        ledger.register_output(output())
        ledger.register_receipt(receipt())
        committed = ledger.commit("RUN-001", 1)
        digest = committed.partition.certificate.output_digest

        ledger.register_receipt(
            receipt(
                receipt_id="RC-LATE",
                kind=ReceiptType.FAILURE,
                key="IDEM-LATE",
                at=BASE + timedelta(days=10),
            )
        )

        restored = LakeCommitLedger.from_state(ledger.to_state())
        snapshot = restored.current_snapshot("RUN-001")
        self.assertEqual(snapshot.certificate.output_digest, digest)
        self.assertEqual(restored.outcome("JOB-0001", 1).terminal, ReceiptType.SUCCESS)

    def test_superseded_partition_keeps_certificate_for_traceability(self):
        ledger = LakeCommitLedger()
        ledger.register_job(job(job_id="J1", attempt=1))
        ledger.register_job(job(job_id="J2", attempt=2))
        for attempt in (1, 2):
            ledger.register_output(
                output(job_id=f"J{attempt}", attempt=attempt, key=f"result/0{attempt}/1")
            )
            ledger.register_receipt(
                receipt(
                    receipt_id=f"RC-{attempt}",
                    job_id=f"J{attempt}",
                    attempt=attempt,
                    key=f"K-{attempt}",
                    at=BASE + timedelta(hours=attempt),
                )
            )
        ledger.commit("RUN-001", 1)
        ledger.commit("RUN-001", 2)

        older = ledger.partition_for("RUN-001", 1)

        self.assertEqual(older.state, PartitionState.SUPERSEDED)
        self.assertIsNotNone(older.certificate)
        self.assertEqual(older.certificate.code_version, "GIT-001")
        with self.assertRaises(PartitionNotVisible):
            ledger.snapshot_for_partition(older.partition_id)

    def test_cleanup_plan_protects_eligible_uncommitted_attempt(self):
        ledger = LakeCommitLedger()
        ledger.register_job(job(job_id="JOB-0001"))
        ledger.register_output(output())
        ledger.register_receipt(receipt())

        plan = ledger.cleanup_plan()

        self.assertEqual(plan.items[0].action, CleanupAction.PROTECT)
        self.assertEqual(plan.reclaimable_bytes, 0)

    def test_is_object_protected_by_any_reference(self):
        ledger = self._ledger()
        ledger.register_reference(
            ReferenceRecord(
                reference_id="RES-W",
                target_kind=ReferenceTarget.OBJECT_KEY,
                target="result/01/00001",
                reference_state=ReferenceState.WITHDRAWN,
            )
        )

        self.assertFalse(ledger.is_object_protected("result/01/00001"))
        self.assertFalse(ledger.is_object_protected("result/02/00002"))

        ledger.register_reference(
            ReferenceRecord(
                reference_id="RES-R",
                target_kind=ReferenceTarget.OBJECT_KEY,
                target="result/02/00002",
                reference_state=ReferenceState.REVIEW,
            )
        )
        self.assertTrue(ledger.is_object_protected("result/02/00002"))

    def test_quarantine_object_is_not_reclaimed_twice(self):
        ledger = LakeCommitLedger()
        ledger.register_job(job(job_id="JOB-0001", state=JobState.FAILED))
        ledger.register_output(output())
        plan = ledger.cleanup_plan()
        self.assertEqual(plan.items[0].action, CleanupAction.QUARANTINE)

        ledger.apply_cleanup(plan, confirm=True)
        self.assertTrue(ledger.outputs_for("JOB-0001", 1)[0].quarantined)

        second = ledger.apply_cleanup(plan, confirm=True)
        self.assertEqual(second["quarantined"], ["result/01/00001"])
        self.assertEqual(len(ledger.outputs_for("JOB-0001", 1)), 1)


class PersistenceTests(unittest.TestCase):
    def test_state_round_trip_preserves_visibility_and_certificate(self):
        ledger = LakeCommitLedger()
        ledger.register_job(job(job_id="JOB-0001"))
        ledger.register_output(output())
        ledger.register_receipt(receipt())
        ledger.commit("RUN-001", 1)
        original = ledger.select_snapshot("RUN-001")

        restored = LakeCommitLedger.from_state(ledger.to_state())
        restored_snapshot = restored.select_snapshot("RUN-001")

        self.assertEqual(
            restored_snapshot.certificate.output_digest,
            original.certificate.output_digest,
        )
        self.assertEqual(restored_snapshot.version.version_id, "RUN-001:V1")
        self.assertTrue(restored_snapshot.partition.visible)

    def test_duplicate_ingest_is_stable_across_round_trip(self):
        ledger = LakeCommitLedger()
        ledger.register_job(job(job_id="JOB-0001"))
        ledger.register_output(output())
        ledger.register_receipt(receipt())
        ledger.register_receipt(receipt(receipt_id="RC-0002", key="IDEM-002"))

        restored = LakeCommitLedger.from_state(ledger.to_state())

        self.assertEqual(restored.outcome("JOB-0001", 1).terminal, ReceiptType.SUCCESS)
        self.assertEqual(len(restored.partitions()), 0)


class DatasetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dataset = load_data_dir(ROOT / "data")
        cls.ledger = cls.dataset.ledger

    def test_all_outputs_and_receipts_are_reconciled(self):
        reconcile = self.dataset.reconcile
        self.assertEqual(reconcile.matched_outputs, 160)
        self.assertEqual(reconcile.matched_receipts, 90)
        self.assertEqual(reconcile.unmatched_outputs, [])
        self.assertEqual(reconcile.unmatched_receipts, [])
        self.assertEqual(reconcile.output_label_offsets.get("+1"), 159)
        self.assertEqual(reconcile.receipt_label_offsets.get("+1"), 90)

    def test_ledger_counts_match_source(self):
        counts = self.ledger.summary()["counts"]
        self.assertEqual(counts["jobs"], 100)
        self.assertEqual(counts["outputs"], 160)
        self.assertEqual(counts["receipts"], 90)
        self.assertEqual(counts["logical_runs"], 45)

    def test_no_rerun_is_eligible_before_owner_confirmation(self):
        self.assertEqual(self.ledger.eligible_attempts(), [])

    def test_confirmed_reruns_become_eligible(self):
        ledger = load_data_dir(ROOT / "data").ledger
        for run in ("RUN-000", "RUN-021", "RUN-033"):
            version = ledger.versions(run)[1]
            ledger.confirm_version(
                run, version_id=version.version_id, confirmed_by="测试负责人"
            )
        eligible = {
            (report.logical_run, report.attempt)
            for report in ledger.eligible_attempts()
        }
        self.assertEqual(
            eligible, {("RUN-000", 3), ("RUN-021", 3), ("RUN-033", 3)}
        )

    def test_recovery_yields_three_visible_partitions(self):
        ledger = load_data_dir(ROOT / "data").ledger
        for run in ("RUN-000", "RUN-021", "RUN-033"):
            version = ledger.versions(run)[1]
            ledger.confirm_version(
                run, version_id=version.version_id, confirmed_by="测试负责人"
            )
        report = ledger.recover(self.dataset.recorded_partitions)

        self.assertEqual(len(ledger.visible_partitions()), 3)
        self.assertEqual(len(report.promoted), 3)
        self.assertEqual(len(report.quarantined), 27)
        self.assertEqual(len(report.held), 24)
        snapshot = ledger.select_snapshot("RUN-021")
        self.assertEqual(snapshot.job.job_id, "JOB-0066")
        self.assertEqual(snapshot.version.ordinal, 2)

    def test_recorded_committed_partitions_are_quarantined_or_verified(self):
        ledger = load_data_dir(ROOT / "data").ledger
        for run in ("RUN-000", "RUN-021", "RUN-033"):
            version = ledger.versions(run)[1]
            ledger.confirm_version(
                run, version_id=version.version_id, confirmed_by="测试负责人"
            )
        ledger.recover(self.dataset.recorded_partitions)

        for partition in ledger.partitions():
            if partition.recorded_state == "committed":
                self.assertIn(
                    partition.state,
                    (PartitionState.QUARANTINED, PartitionState.COMMITTED),
                )
                if partition.state is PartitionState.COMMITTED:
                    self.assertTrue(partition.certificate is not None)

    def test_cleanup_never_deletes_validly_referenced_objects(self):
        ledger = load_data_dir(ROOT / "data").ledger
        plan = ledger.cleanup_plan()
        for item in plan.items:
            if item.action is CleanupAction.DELETE:
                references = ledger.references_to_object(item.object_key)
                self.assertFalse(
                    any(
                        ref.reference_state is ReferenceState.VALID
                        for ref in references
                    ),
                    item.object_key,
                )
        self.assertGreater(plan.reclaimable_bytes, 0)
        self.assertGreater(plan.protected_bytes, 0)


if __name__ == "__main__":
    unittest.main()
