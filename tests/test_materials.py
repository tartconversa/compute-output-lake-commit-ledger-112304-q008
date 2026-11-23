import csv
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[1]
EXPECTED={'jobs.csv': (['job_id', 'logical_run', 'code_version', 'input_snapshot', 'parameter_hash', 'attempt', 'job_state', 'finished_at'], 100), 'output_objects.csv': (['job_id', 'attempt', 'object_key', 'bytes', 'checksum', 'write_state', 'written_at'], 160), 'completion_receipts.csv': (['receipt_id', 'job_id', 'attempt', 'receipt_type', 'received_at', 'idempotency_key'], 90), 'lake_partitions.csv': (['partition_id', 'logical_run', 'attempt', 'commit_state', 'committed_at', 'manifest_digest', 'consumer_refs'], 80)}

class MaterialTests(unittest.TestCase):
    def test_csv_contract(self):
        for filename,(columns,count) in EXPECTED.items():
            with self.subTest(filename=filename):
                with (ROOT/"data"/filename).open(encoding="utf-8-sig",newline="") as stream:
                    reader=csv.DictReader(stream); rows=list(reader)
                self.assertEqual(reader.fieldnames,columns)
                self.assertEqual(len(rows),count)
                self.assertTrue(all(None not in row for row in rows))

if __name__=="__main__": unittest.main()
