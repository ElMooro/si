"""earnings-whisper v1.1.0 - offline: inputs are reduced to the scored fields, loaded one at a time, output unchanged."""
from pathlib import Path
import io
import json
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "source"))
import lambda_function as engine  # noqa: E402


class FakeS3:
    def __init__(self, docs):
        self.docs = docs
        self.gets = []
        self.puts = {}
        self.live_bodies = 0          # how many fetched bodies are alive at once (through fetch_json)

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        if Key not in self.docs:
            raise KeyError(Key)
        return {"Body": io.BytesIO(json.dumps(self.docs[Key]).encode())}

    def put_object(self, Bucket, Key, Body, **kw):
        self.puts[Key] = json.loads(Body)


def big_eps(n=3000):
    # the real file is 55 MB: thousands of rows, each with a history the whisper never reads
    return {"all_qualifying": [{"symbol": "S%04d" % i, "flag": "TIER_A_HIGH" if i % 3 else "TIER_S_HIGHEST", "score": 70 + (i % 30),
                                "history": [{"d": "2026-%02d-01" % (m + 1), "eps": i * 0.01} for m in range(12)] * 5,
                                "narrative": "x" * 400} for i in range(n)]}


DOCS = {
    "data/earnings-tracker.json": {"upcoming_14d": [
        {"ticker": "S0001", "name": "One", "earnings_date": "2026-11-02", "time": "amc", "eps_consensus": 1.2, "n_estimates": 9, "market_cap": 5e9},
        {"ticker": "S0003", "name": "Three", "earnings_date": "2026-11-04", "time": "bmo"},
        {"ticker": "ZZZZ", "name": "Nobody", "earnings_date": "2026-11-05"},
    ]},
    "data/eps-revision-velocity.json": big_eps(),
    "data/options-flow-scanner.json": {"all_qualifying": [{"symbol": "S0001", "score": 85, "tier": "TIER_A_BULLISH_FLOW", "flags": ["CPR_SURGING"], "chains": [1] * 5000}]},
    "data/insider-trades.json": {"transactions": [{"ticker": "S0001", "txn_date": "2026-10-20", "side": "BUY", "code": "P", "role": "CEO", "filing_url": "u", "shares": 1000},
                                                   {"ticker": "S0003", "txn_date": "2026-10-21", "side": "SELL", "code": "S", "role": "Director"}]},
    "data/8k-filings.json": {"filings": [{"ticker": "S0001", "filed_at": "2026-10-24", "items": ["2.02", "9.01"], "body": "x" * 1000}]},
    "data/revenue-acceleration.json": {"all_qualifying": [{"symbol": "S0003", "tier": "TIER_A_ACCELERATING", "quarters": [1] * 40}]},
    "data/smart-money-clusters.json": {"clusters": [{"ticker": "S0001", "score": 82, "legend_buyers": ["Buffett"], "holders": [1] * 300}]},
}


class Whisper(unittest.TestCase):
    def setUp(self):
        self.s3 = FakeS3(json.loads(json.dumps(DOCS)))
        engine.S3 = self.s3

    def test_indices_keep_only_the_scored_fields(self):
        eps = engine.build_eps_idx(DOCS["data/eps-revision-velocity.json"])
        self.assertEqual(len(eps), 3000)
        self.assertEqual(set(eps["S0001"]), {"flag", "score"})
        opt = engine.build_options_idx(DOCS["data/options-flow-scanner.json"])
        self.assertEqual(set(opt["S0001"]), {"score", "tier", "flags"})
        txn = engine.build_transactions_idx(DOCS["data/insider-trades.json"])
        self.assertEqual(set(txn["S0001"][0]), {"txn_date", "side", "code", "role"})
        fil = engine.build_filings_idx(DOCS["data/8k-filings.json"])
        self.assertEqual(set(fil["S0001"][0]), {"filed_at", "items"})
        rev = engine.build_revenue_idx(DOCS["data/revenue-acceleration.json"])
        self.assertEqual(set(rev["S0003"]), {"tier"})
        sm = engine.build_smart_idx(DOCS["data/smart-money-clusters.json"])
        self.assertEqual(set(sm["S0001"]), {"score", "legend_buyers"})
        self.assertEqual(engine.build_eps_idx(None), {})
        self.assertEqual(engine.build_eps_idx({"all_qualifying": ["junk", None]}), {})

    def test_handler_scores_as_before_and_reports_input_sizes(self):
        res = json.loads(engine.lambda_handler({}, None)["body"])
        self.assertTrue(res["ok"])
        self.assertEqual(res["n_upcoming"], 3)
        out = self.s3.puts["data/earnings-whisper.json"]
        self.assertEqual(out["schema_version"], "1.0")
        self.assertEqual(out["method"], "earnings_whisper_v1")
        self.assertEqual(out["engine_version"], engine.VERSION)
        by = {s["ticker"]: s for s in out["all_setups"]}
        one = by["S0001"]
        # TIER_A eps 18 + options 20 (tier A bullish, capped) + CEO buy 20 + 8-K 2.02 within 14 d 8 + revenue 0 + smart 10
        self.assertEqual(one["components"], {"eps_velocity": 18, "options_flow": 20, "insider": 20, "eight_k": 8, "revenue_accel": 0, "smart_money": 10})
        self.assertEqual(one["whisper_score"], 76)
        self.assertEqual(one["tier"], "S")
        self.assertIn("INSIDER_BUYING", one["flags"])
        three = by["S0003"]
        self.assertEqual(three["components"]["eps_velocity"], 25)       # i=3 -> TIER_S
        self.assertEqual(three["components"]["insider"], 0)             # only selling -> -5 floored at 0 (unchanged scorer)
        self.assertEqual(three["components"]["revenue_accel"], 7)
        self.assertEqual(by["ZZZZ"]["components"], {"eps_velocity": 0, "options_flow": 0, "insider": 5, "eight_k": 0, "revenue_accel": 0, "smart_money": 0})
        self.assertEqual(out["feed_health"], {k: True for k in out["feed_health"]})
        self.assertEqual(set(out["input_bytes"]), set(DOCS))
        self.assertTrue(all(v > 0 for v in out["input_bytes"].values()))
        self.assertEqual(out["top_setups"][0]["ticker"], "S0001")
        self.assertEqual(self.s3.gets, list(DOCS))                      # one GET per input, in order

    def test_missing_input_is_reported_not_fatal(self):
        del self.s3.docs["data/options-flow-scanner.json"]
        res = json.loads(engine.lambda_handler({}, None)["body"])
        self.assertTrue(res["ok"])
        out = self.s3.puts["data/earnings-whisper.json"]
        self.assertFalse(out["feed_health"]["options_flow"])
        self.assertIsNone(out["input_bytes"]["data/options-flow-scanner.json"])

    def test_config_gives_the_budget_the_inputs_need(self):
        cfg = json.loads((Path(__file__).resolve().parents[1] / "config.json").read_text())
        self.assertGreaterEqual(cfg["memory"], 1024)
        self.assertGreaterEqual(cfg["timeout"], 120)
        self.assertEqual(cfg["handler"], "lambda_function.lambda_handler")
        self.assertEqual(cfg["function_name"], "justhodl-earnings-whisper")
        self.assertNotIn("schedule", cfg)   # the live trigger (fan-out tick) is not redeclared here


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Whisper))
    if not result.wasSuccessful():
        raise SystemExit(1)
