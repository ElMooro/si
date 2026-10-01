"""Offline counterexamples; invented prices/clocks, never historical evidence."""
import ast
import copy
import hashlib
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from statistics import mean, median
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
PREDECESSOR = ROOT / "tests/fixtures/katlin-oos-before-availability.py.txt"
spec = importlib.util.spec_from_file_location("boundary", ROOT / "scripts/research/katlin_label_boundary.py")
boundary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(boundary)


def stamp(i):
    # Invented session calendar; not an exchange calendar or availability estimate.
    return (datetime(2020, 1, 1, tzinfo=timezone.utc) + timedelta(days=i)).isoformat()


def observation(name, entry, horizons=(63, 126, 252), prices=None):
    prices = prices if prices is not None else [100.0] * 1000
    return {"id": name, "entry_index": entry, "entry_at": stamp(entry),
            "features_available_at": stamp(entry),
            "labels": {str(h): {"endpoint_index": entry + h, "endpoint_at": stamp(entry + h),
                                "available_at": stamp(entry + h),
                                "source_record_id": "invented-fixture-" + name + "-" + str(h),
                                "excess_return_pct": 100 * (prices[entry + h] / prices[entry] - 1)}
                       for h in horizons}}


def retained_predecessor_fit():
    # The historical defect belongs to the explicitly retained predecessor, not
    # corrected production. Compile only its pure fit and selection expression.
    body = PREDECESSOR.read_bytes()
    assert hashlib.sha256(body).hexdigest() == "8d9f3fed0c664f24a2781719c92089b0f945f99df865525715c71fd9628c3e41"
    tree = ast.parse(body.decode("utf-8"))
    run = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run_backtest")
    fit = next(n for n in run.body if isinstance(n, ast.FunctionDef) and n.name == "fit")
    scope = {"mean": mean, "median": median, "FEATURES": ("fixture_bucket",), "rnd": round}
    exec(compile(ast.Module(body=[fit], type_ignores=[]), str(PREDECESSOR), "exec"), scope)
    # Retain the real legacy training-selection expression as the counterexample.
    split = next(n.value for n in ast.walk(run) if isinstance(n, ast.Assign)
                 and any(isinstance(t, ast.Name) and t.id == "train" for t in n.targets))
    return scope["fit"], compile(ast.Expression(body=split), str(PREDECESSOR), "eval")


def native_rows(rows, h):
    return [{"i": r["entry_index"], "b": {"fixture_bucket": "base"},
             "fwd": {h: (0, r["labels"][str(h)]["excess_return_pct"], 0, False)}} for r in rows]


class BoundaryTests(unittest.TestCase):
    def audit(self, rows, h=63, start=400):
        return boundary.training_audit(rows, horizon=h, test_start_index=start, test_start_at=stamp(start))

    def test_test_price_mutation_changes_legacy_fit_but_not_eligible_fit(self):
        fit, legacy = retained_predecessor_fit()
        for h in (63, 126, 252):
            with self.subTest(horizon=h):
                before = [100.0] * 1000
                after = before[:400] + [200.0 + i for i in range(600)]
                def sample(prices):
                    return [observation("mature", 10, (h,), prices),
                            observation("edge", 400 - h, (h,), prices),
                            observation("crossing", 399, (h,), prices),
                            observation("test", 400, (h,), prices)]
                a, b = sample(before), sample(after)
                def legacy_fit(rows):
                    selected = eval(legacy, {"obs": native_rows(rows, h), "split_i": 400})
                    return fit(selected, h)
                self.assertNotEqual(legacy_fit(a), legacy_fit(b))
                aa, bb = self.audit(a, h), self.audit(b, h)
                self.assertEqual(aa, bb)
                self.assertEqual(aa["accepted_ids"], ["mature"])
                self.assertEqual(aa["excluded_count"], 3)
                clean_a = [r for r in a if r["id"] in aa["accepted_ids"]]
                clean_b = [r for r in b if r["id"] in bb["accepted_ids"]]
                self.assertEqual(fit(native_rows(clean_a, h), h), fit(native_rows(clean_b, h), h))

    def test_current_production_still_withholds_invalid_oos(self):
        # Reuse PR24's actual-producer replay with invented prices and local I/O
        # stubs. This imports a test helper, never the cloud-initializing Lambda.
        path = ROOT / "aws/lambdas/justhodl-katlin/tests/test_oos_boundary.py"
        spec = importlib.util.spec_from_file_location("current_oos_replay", path)
        replay = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(replay)
        current, _, _ = replay.replay()
        legacy, _, _ = replay.replay(legacy=True)

        def assert_withheld(packet):
            self.assertEqual(packet["oos"], {})
            validation = packet.get("oos_validation") or {}
            self.assertEqual(validation.get("contract"), "katlin-oos-boundary.v1")
            self.assertEqual(validation.get("status"), "BLOCKED")
            self.assertIs(validation.get("historical_availability_verified"), False)
            self.assertIs(validation.get("validated_strategy"), False)
            self.assertEqual(set(validation.get("folds", {})), {"63s", "126s"})
            for fold in validation["folds"].values():
                self.assertEqual(fold["eligible_training_observations"], 0)
                self.assertIsNone(fold["split_decision_at"])
                self.assertGreater(fold["missing_verified_label_availability"], 0)

        assert_withheld(current)
        self.assertTrue(legacy["oos"])
        with self.assertRaises(AssertionError):
            assert_withheld(legacy)
        # The guard must reject reintroduced claims even alongside the new block.
        mutant = copy.deepcopy(current)
        mutant["oos"] = legacy["oos"]
        with self.assertRaises(AssertionError):
            assert_withheld(mutant)
        mutant = copy.deepcopy(current)
        mutant["oos_validation"]["validated_strategy"] = True
        with self.assertRaises(AssertionError):
            assert_withheld(mutant)

    def test_each_horizon_has_its_own_membership(self):
        row = observation("x", 200)
        self.assertEqual(self.audit([row], 63)["accepted_ids"], ["x"])
        self.assertEqual(self.audit([row], 126)["accepted_ids"], ["x"])
        self.assertEqual(self.audit([row], 252)["accepted_ids"], [])

    def test_endpoint_and_availability_must_be_strictly_before_boundary(self):
        for h in (63, 126, 252):
            for delta, count in ((-1, 1), (0, 0), (1, 0)):
                row = observation("endpoint", 400 - h + delta, (h,))
                self.assertEqual(self.audit([row], h)["accepted_count"], count)
                row = observation("delayed", 10, (h,))
                row["labels"][str(h)]["available_at"] = stamp(400 + delta)
                self.assertEqual(self.audit([row], h)["accepted_count"], count)

    def test_missing_malformed_or_future_evidence_never_guessed(self):
        cases = [("available_at", None), ("available_at", "2020-01-01"),
                 ("available_at", "2020-01-01T00:00:00"), ("available_at", stamp(1)),
                 ("endpoint_at", None), ("endpoint_index", True), ("endpoint_index", 72),
                 ("source_record_id", ""), ("excess_return_pct", float("nan")),
                 ("excess_return_pct", float("inf")), ("excess_return_pct", True)]
        for field, value in cases:
            with self.subTest(field=field, value=value):
                row = observation("x", 10)
                row["labels"]["63"][field] = value
                self.assertEqual(self.audit([row])["accepted_count"], 0)
        for field, value in (("features_available_at", stamp(11)), ("entry_at", stamp(400)),
                             ("features_available_at", None), ("entry_index", True)):
            row = observation("x", 10); row[field] = value
            self.assertEqual(self.audit([row])["accepted_count"], 0)
        row = observation("x", 10); del row["labels"]["63"]
        self.assertEqual(self.audit([row])["exclusion_reason_counts"], {"missing_horizon_label": 1})

    def test_timezone_normalization_and_equality(self):
        row = observation("x", 10)
        at = datetime.fromisoformat(stamp(400))
        row["labels"]["63"]["available_at"] = at.astimezone(timezone(timedelta(hours=2))).isoformat()
        self.assertEqual(self.audit([row])["accepted_count"], 0)
        row["labels"]["63"]["available_at"] = (at - timedelta(microseconds=1)).isoformat()
        self.assertEqual(self.audit([row])["accepted_count"], 1)

    def test_empty_duplicate_invalid_fold_and_input_immutability(self):
        self.assertEqual(self.audit([])["status"], "no_eligible_training_evidence")
        row = observation("x", 10); original = copy.deepcopy(row)
        self.audit([row]); self.assertEqual(row, original)
        with self.assertRaises(ValueError): self.audit([row, row])
        for h in (0, -1, True, 1.5):
            with self.assertRaises(ValueError): self.audit([row], h)
        with self.assertRaises(ValueError):
            boundary.training_audit([], horizon=63, test_start_index=400, test_start_at="2021-01-01")

    def test_research_authority_and_limitations_always_present(self):
        for rows in ([], [observation("x", 10)]):
            audit = self.audit(rows)
            self.assertFalse(audit["validated_strategy"])
            self.assertFalse(audit["decision_eligible"])
            self.assertTrue(audit["research_only"])
            self.assertEqual(audit["input_count"], audit["accepted_count"] + audit["excluded_count"])
            self.assertEqual(len(audit["limitations"]), 4)

    def test_local_cli_emits_same_evidence_and_no_output_file(self):
        doc = {"horizon": 63, "test_start_index": 400, "test_start_at": stamp(400),
               "observations": [observation("x", 10)]}
        with tempfile.TemporaryDirectory() as directory:
            evidence = Path(directory) / "evidence.json"
            evidence.write_text(json.dumps(doc), encoding="utf-8")
            completed = subprocess.run([sys.executable, str(ROOT / "scripts/research/katlin_label_boundary.py"),
                                        str(evidence)], cwd=directory, capture_output=True, text=True, check=True)
            self.assertEqual(json.loads(completed.stdout), self.audit(doc["observations"]))
            self.assertEqual(list(Path(directory).iterdir()), [evidence])


if __name__ == "__main__":
    unittest.main()
