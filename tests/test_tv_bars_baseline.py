"""Invented metadata only; the operation's data/credential boundary is closed."""
import copy
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("tv_baseline", ROOT / "aws/ops/staged/ops_6477_tv_bars_baseline.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def fixture():
    return {"function_name": "justhodl-tv-bars", "source_files_checked": 1, "handler_bytes": 12,
            "timeout": 600, "memory_mb": 1024, "ephemeral_storage_mb": 512, "code_sha256": "invented",
            "runtime": "python3.12", "handler": "lambda_function.lambda_handler", "role": "invented",
            "architectures": ["x86_64"], "receipt": {"status": "matched", "commit": "invented"},
            "schedules": [{"kind": "EventBridge rule", "name": "invented", "state": "DISABLED", "native_targets": 1}]}


class BaselineTests(unittest.TestCase):
    def test_exact_named_receipt_only(self):
        client = Mock()
        wrapper = module.ReceiptOnly(client)
        wrapper.get_object(Bucket=module.BUCKET, Key="data/ops/releases/justhodl-tv-bars.json")
        self.assertEqual(client.get_object.call_count, 1)
        for key in ("data/warm/tv-bars/_index.json", "data/private/example.json", "data/ops/releases/other.json"):
            with self.assertRaises(ValueError):
                wrapper.get_object(Bucket=module.BUCKET, Key=key)
        self.assertEqual(client.get_object.call_count, 1)

    def test_receipt_read_error_propagates(self):
        client = Mock()
        client.get_object.side_effect = RuntimeError("Invented denied receipt")
        with self.assertRaisesRegex(RuntimeError, "denied"):
            module.ReceiptOnly(client).get_object(Bucket=module.BUCKET, Key="data/ops/releases/justhodl-tv-bars.json")

    def test_disabled_and_empty_census_are_observed_not_enabled(self):
        record = fixture()
        self.assertEqual(module.normalize(record)["schedules"][0]["state"], "DISABLED")
        record["schedules"] = []
        self.assertEqual(module.normalize(record)["schedules"], [])

    def test_unknown_control_counts_are_rejected(self):
        for key in ("source_files_checked", "handler_bytes", "timeout", "memory_mb", "ephemeral_storage_mb"):
            for value in (None, True, 0, -1, "1024"):
                record = fixture()
                record[key] = value
                with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                    module.normalize(record)

    def test_unknown_identity_receipt_and_schedule_are_rejected(self):
        for key, value in (("function_name", "other"), ("architectures", []), ("receipt", {"status": "unknown"}),
                           ("receipt", {"status": "matched"}), ("schedules", None), ("schedules", [{}])):
            record = fixture()
            record[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                module.normalize(record)

    def test_missing_predecessor_receipt_stays_explicit(self):
        record = fixture()
        record["receipt"] = {"status": "missing_predecessor_receipt"}
        self.assertEqual(module.normalize(record)["receipt"]["status"], "missing_predecessor_receipt")

    def test_only_order_changes_are_normalized(self):
        a = fixture()
        a["schedules"].append({**a["schedules"][0], "name": "second", "state": "ENABLED"})
        b = copy.deepcopy(a)
        b["schedules"].reverse()
        with patch.object(module, "runtime", side_effect=[a, b]) as read:
            self.assertEqual(len(module.capture((None,) * 4)["schedules"]), 2)
            self.assertEqual(read.call_count, 2)

    def test_changed_native_snapshot_is_not_accepted(self):
        a = fixture()
        b = copy.deepcopy(a)
        b["schedules"][0]["state"] = "ENABLED"
        with patch.object(module, "runtime", side_effect=[a, b]), self.assertRaisesRegex(ValueError, "changed"):
            module.capture((None,) * 4)


if __name__ == "__main__":
    unittest.main()
