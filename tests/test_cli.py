import contextlib
import io
import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from compute_output_lake_commit_ledger.cli import main  # noqa: E402


def run_cli(*argv):
    buffer = io.StringIO()
    with contextlib.redirect_stdout(buffer):
        try:
            code = main(list(argv))
        except SystemExit as exc:  # argparse 或显式退出
            code = exc.code if isinstance(exc.code, int) else 1
    text = buffer.getvalue()
    try:
        payload = json.loads(text) if text.strip() else None
    except json.JSONDecodeError:
        payload = None
    return code, payload, text


class CliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="lake-cli-"))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write_confirm(self):
        path = self.tmp / "confirm.json"
        path.write_text(
            json.dumps(
                [
                    {
                        "logical_run": run,
                        "version_id": f"{run}:V2",
                        "confirmed_by": "测试负责人",
                        "is_new_experiment": True,
                        "note": "新参数",
                    }
                    for run in ("RUN-000", "RUN-021", "RUN-033")
                ],
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return path

    def test_inspect_reports_reconciliation(self):
        code, payload, _ = run_cli("--data", str(ROOT / "data"), "inspect")

        self.assertEqual(code, 0)
        self.assertEqual(payload["reconcile"]["matched_outputs"], 160)
        self.assertEqual(payload["summary"]["counts"]["jobs"], 100)
        self.assertEqual(payload["summary"]["eligible"], [])

    def test_snapshot_without_confirmation_fails(self):
        code, payload, _ = run_cli(
            "--data", str(ROOT / "data"), "snapshot", "--run", "RUN-021"
        )

        self.assertEqual(code, 1)
        self.assertIn("error", payload)

    def test_full_flow_with_confirmation(self):
        confirm = self._write_confirm()

        code, payload, _ = run_cli(
            "--data", str(ROOT / "data"),
            "--confirm", str(confirm),
            "snapshot", "--run", "RUN-021",
        )
        self.assertEqual(code, 0)
        self.assertEqual(payload["job"]["job_id"], "JOB-0066")
        self.assertEqual(payload["partition"]["state"], "committed")
        self.assertIsNotNone(payload["certificate"]["output_digest"])

        code, payload, _ = run_cli(
            "--data", str(ROOT / "data"),
            "--confirm", str(confirm),
            "references", "--object", "result/30/00066",
        )
        self.assertEqual(code, 0)
        self.assertTrue(payload["references"])
        self.assertEqual(payload["references"][0]["reference_state"], "valid")

        code, payload, _ = run_cli(
            "--data", str(ROOT / "data"),
            "--confirm", str(confirm),
            "cleanup", "--run", "RUN-021",
        )
        self.assertEqual(code, 0)
        self.assertEqual(payload["counts"]["delete"], 0)

    def test_cleanup_apply_requires_yes(self):
        code, _, text = run_cli(
            "--data", str(ROOT / "data"),
            "cleanup", "--run", "RUN-021", "--apply",
        )

        self.assertEqual(code, 1)
        self.assertIn("--yes", text)

    def test_state_save_and_load(self):
        confirm = self._write_confirm()
        state_path = self.tmp / "state.json"

        code, payload, _ = run_cli(
            "--data", str(ROOT / "data"),
            "--confirm", str(confirm),
            "state", "--save", str(state_path),
        )
        self.assertEqual(code, 0)
        self.assertTrue(state_path.exists())

        code, payload, _ = run_cli("state", "--load", str(state_path))
        self.assertEqual(code, 0)
        self.assertEqual(payload["counts"]["visible_partitions"], 3)

    def test_commit_unknown_attempt_errors(self):
        code, _, text = run_cli(
            "--data", str(ROOT / "data"), "commit", "RUN-021", "99"
        )

        self.assertEqual(code, 1)
        self.assertIn("UnknownJobError", text)
