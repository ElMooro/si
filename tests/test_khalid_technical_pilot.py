"""Invented offline fixtures; never contact providers or import a Lambda."""
import copy
from datetime import date, timedelta
import importlib.util
import json
import math
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts/research"))
import khalid_technical_pilot as pilot


def bars(n=600):
    result = []
    for i in range(n):
        c = 100 + 5 * math.sin(i / 10)
        result.append(dict(date=(date(2020, 1, 1)+timedelta(days=i)).isoformat(),
                           open=c, high=c+2, low=c-2, close=c, volume=1000))
    return result


class PilotTests(unittest.TestCase):
    def setUp(self):
        self.spec = json.loads(pilot.SPEC.read_text(encoding="utf-8"))

    def test_conflicting_dates_exclude_whole_asset_without_choosing_a_winner(self):
        doc = {"rows": [["2020-01-01", 100, 110, 90, 105, 1000],
                        ["2020-01-01", 100, 110, 90, 106, 1000]]}
        got, audit = pilot.normalize(doc, "AAPL", "2020-01-01", "2020-01-02")
        self.assertEqual(got, [])
        self.assertEqual(audit["conflicting_dates"], 1)
        self.assertIn("conflicting_same_date_bars", audit["excluded_reasons"])
        doc["rows"][1] = doc["rows"][0][:]
        got, audit = pilot.normalize(doc, "AAPL", "2020-01-01", "2020-01-02")
        self.assertEqual(len(got), 1)
        self.assertEqual(audit["duplicate_rows"], 1)

    def test_invalid_prices_volume_and_crypto_calendar_gaps_fail_closed(self):
        base = {"rows": [["2020-01-01", 100, 110, 90, 105, 1000]]}
        for index, value in ((1, 0), (2, 80), (3, 120), (4, float("nan")), (5, -1), (5, True)):
            doc = copy.deepcopy(base); doc["rows"][0][index] = value
            self.assertEqual(pilot.normalize(doc, "BTC", "2020-01-01", "2020-01-03")[0], [])
        base["rows"].append(["2020-01-03", 100, 110, 90, 105, 1000])
        self.assertEqual(pilot.normalize(base, "BTC", "2020-01-01", "2020-01-03")[0], [])

    def test_native_scorer_cannot_see_future_bar_mutations(self):
        before = bars(); after = copy.deepcopy(before)
        for row in after[570:]:
            for key in ("open", "high", "low", "close"): row[key] *= .25
        original = pilot.score_prefixes(before, self.spec)
        changed = pilot.score_prefixes(after, self.spec)
        self.assertEqual([r for r in original if r["i"] < 570], [r for r in changed if r["i"] < 570])
        self.assertNotEqual(original[-1]["checks"], changed[-1]["checks"])

    def test_scorer_hash_mismatch_blocks_execution(self):
        self.spec["scorer_sha256"] = "0"*64
        with self.assertRaisesRegex(ValueError, "frozen scorer"):
            pilot.score_prefixes(bars(), self.spec)

    def test_funnel_exact_outcomes_censoring_and_unavailable_training(self):
        sample = bars(530)
        sample[503]["close"] = 100
        sample[524]["close"] = 110
        checks = {key: True for key in self.spec["gates_in_order"]}
        missed = dict(checks, bb=False)
        with patch.object(pilot, "score_prefixes", return_value=[{"i":503,"checks":checks},
                           {"i":504,"checks":missed},{"i":529,"checks":checks}]):
            result = pilot.study_series("BTC", sample, self.spec)
        self.assertEqual(result["funnel"], {"after_warmup":3,"offhigh":3,"rsi":3,"bb":2,"spread":2,"exhaust":2})
        out = result["outcomes"]["21"]
        self.assertAlmostEqual(out["events"][0]["gross_price_return_pct"], 10)
        self.assertEqual(out["right_censored_events"], 1)
        self.assertEqual(out["training_availability_audit"]["accepted_count"], 0)
        self.assertEqual(out["training_availability_audit"]["excluded_count"], 1)
        self.assertEqual(result["outcomes"]["63"]["summary"]["events"], 0)
        self.assertIsNone(result["outcomes"]["63"]["summary"]["mean_gross_price_return_pct"])

    def test_overlap_clusters_include_touching_intervals_and_cross_asset_events(self):
        events = [{"symbol":sym,"date":start,"endpoint_date":end,"gross_price_return_pct":value}
                  for sym,start,end,value in [("BTC","2020-01-01","2020-01-22",10),
                     ("ETH","2020-01-22","2020-02-10",-10),("BTC","2020-03-01","2020-03-22",30)]]
        summary = pilot.summarize(events)
        self.assertEqual(summary["overlap_connected_clusters"], 2)
        self.assertEqual(summary["unique_assets"], 2)
        self.assertEqual(summary["mean_gross_price_return_pct"], 10)
        self.assertIsNone(summary["confidence_interval"])

    def test_manifest_hashes_and_exact_inventory_are_required(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "manifest.json").write_text("[]", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "inventory"): pilot.run(root)
            refs = [{"symbol":s,"file":s+".json","sha256":"0"*64} for s in self.spec["assets"]+["SPY"]]
            (root / "manifest.json").write_text(json.dumps(refs), encoding="utf-8")
            (root / "BTC.json").write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "hash mismatch"): pilot.run(root)


if __name__ == "__main__":
    unittest.main()
