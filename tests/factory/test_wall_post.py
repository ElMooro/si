"""The student's Monday wall post (justhodl-ai wall_post.py) -- two agents per symbol through the guests' door."""
import gzip
import json
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws/shared'))
sys.path.insert(0, str(ROOT / 'aws/lambdas/justhodl-ai/source'))
sys.path.insert(0, str(ROOT / 'tests/factory'))
from test_factory import MemoryS3  # noqa: E402
from factory_store import Store  # noqa: E402
import factory_inference as fi  # noqa: E402
import factory_core as fc  # noqa: E402
import wall_post as wp  # noqa: E402

try:
    import boto3  # noqa: F401  -- the engine harness fakes botocore, which breaks a fresh real import
except ModuleNotFoundError:
    sys.modules['boto3'] = types.SimpleNamespace(client=lambda *a, **k: None)
from factory_gateway import accept_prediction, holdout_manifest_hash  # noqa: E402

NY = ZoneInfo("America/New_York")
MONDAY = "2026-09-21"
SEASON = {"id": "season-2026-09-14", "starts_on": "2026-09-14", "weeks": 13, "policy_hash": "p", "calendar_review_required": False,
          "price_sources": {s: "polygon-full" for s in fc.SYMBOLS}, "flat_thresholds": {s: 0.003 for s in fc.SYMBOLS},
          "crisis_drawdown_thresholds": {s: 0.05 for s in fc.SYMBOLS}, "extra_closed": [], "early_closes": {},
          "weights": {"direction": 0.5, "regime": 0.3, "crisis": 0.2}}
READ_DOC = {"read_id": "20260921T054500Z", "board": {"regime": {"risk_gate_posture": "RISK_OFF_DEFENSIVE"}},
            "read": {"voice": "owned", "stocks": {"stance": "DEFENSIVE"}, "bonds": {"stance": "LONG_DURATION"}, "metals": {"stance": "HOLD"}, "crypto": {"stance": "AVOID"}}}


def grouped_day(day, price):
    results = [{"T": s, "o": price, "h": price * 1.01, "l": price * 0.99, "c": price * 1.002, "v": 1000 + i} for i, s in enumerate(("SPY", "QQQ", "IWM", "TLT", "GLD"))]
    results.append({"T": "X:BTCUSD", "o": price * 400, "h": price * 404, "l": price * 396, "c": price * 401, "v": 50})
    return gzip.compress(json.dumps({"results": results}).encode())


class WallPostTests(unittest.TestCase):
    def setUp(self):
        self.cloud = MemoryS3()
        if not hasattr(self.cloud, 'delete_object'):
            self.cloud.delete_object = lambda Bucket, Key: self.cloud.rows.pop((Bucket, Key), None)
        self.store = Store(self.cloud, 'private', 'public', lambda: self.now)
        self.now = datetime(2026, 9, 18, 15, 0, tzinfo=timezone.utc)                      # Friday, the rehearsal day
        self.control = {'enabled': True, 'endpoint_name': 'ep', 'model_id': 'qwen', 'revision': 'r1'}
        self.cloud.rows[('private', fi.CONTROL_KEY)] = json.dumps(self.control).encode()
        self.cloud.rows[('private', 'factory/control/season.json')] = json.dumps(SEASON).encode()
        self.cloud.rows[('private', 'factory/holdout/manifest.json')] = json.dumps({"schema_version": "factory-holdout.v1", "code": {}}).encode()
        for i, day in enumerate(wp.sessions_before(MONDAY, 20, SEASON)):
            self.cloud.rows[('public', wp.GROUPED + "%s/%s.json.gz" % (day[:4], day))] = grouped_day(day, 100 + i * 0.5)
        self.answers = {}
        def invoke(**kw):
            key = kw["InputLocation"].split("/", 3)[-1]
            payload = json.loads(self.cloud.rows[('private', key)])
            symbol = next(s for s in ("bonds", "metals", "crypto", "equity_index") if "Asset class: %s" % s in payload["inputs"])
            out = "factory/inference/out/%s.json" % kw["InferenceId"]
            self.answers[kw["InferenceId"]] = out
            self.cloud.rows[('private', out)] = json.dumps({"generated_text": '{"direction": "DOWN", "regime": "TREND", "crisis_probability": 0.4, "why": "efficient decline"}' if symbol == "equity_index" else 'no json'}).encode()
            return {"OutputLocation": "s3://private/" + out, "FailureLocation": "s3://private/fail/" + kw["InferenceId"]}
        self.rt = types.SimpleNamespace(invoke_endpoint_async=invoke)

    def test_week_selection_and_sessions(self):
        self.assertEqual(wp.week_for(datetime(2026, 9, 18, 15, 0, tzinfo=timezone.utc)), MONDAY)                        # Friday -> coming Monday
        self.assertEqual(wp.week_for(datetime(2026, 9, 21, 13, 5, tzinfo=timezone.utc)), MONDAY)                        # Monday 09:05 ET -> today
        self.assertEqual(wp.week_for(datetime(2026, 9, 21, 15, 0, tzinfo=timezone.utc)), "2026-09-28")                  # Monday after the lock -> next
        days = wp.sessions_before(MONDAY, 20, SEASON)
        self.assertEqual(len(days), 20); self.assertEqual(days[-1], "2026-09-18"); self.assertTrue(all(d < MONDAY for d in days))

    def test_desk_mapping_follows_the_read(self):
        p = wp.desk_prediction(READ_DOC["read"], "SPY", READ_DOC["board"])
        self.assertEqual((p["direction"], p["regime"], p["crisis_probability"]), ("DOWN", "TREND", 0.35))
        self.assertEqual(wp.desk_prediction(READ_DOC["read"], "TLT", READ_DOC["board"])["direction"], "UP")
        self.assertEqual(wp.desk_prediction(READ_DOC["read"], "GLD", READ_DOC["board"])["direction"], "FLAT")
        self.assertAlmostEqual(sum(p["direction_probabilities"].values()), 1.0)

    def test_prepare_then_post_accepts_real_entries_through_the_door(self):
        staged = wp.prepare(self.store, self.cloud, self.rt, self.control, SEASON, READ_DOC, self.now, 'public')
        self.assertEqual(staged["week"], MONDAY); self.assertEqual(len(staged["symbols"]), 6)
        self.assertEqual(len(staged["symbols"]["SPY"]["bars"]), 20); self.assertEqual(staged["symbols"]["SPY"]["bars"][0]["o"], 100.0)
        self.assertEqual(staged["symbols"]["BTC"]["owned"]["state"], "queued")
        self.assertIn(('private', wp.STAGING + MONDAY + ".json"), self.cloud.rows)
        # post inside the window: Monday 09:31 ET
        monday = datetime(2026, 9, 21, 9, 31, tzinfo=NY)
        store = Store(self.cloud, 'private', 'public', lambda: monday)
        receipt = wp.post(store, staged, SEASON, self.control, holdout_manifest_hash(store), accept_prediction)
        locked = [e for e in receipt["entries"] if e["status"] == "locked"]
        self.assertEqual(len([e for e in locked if e["agent"] == "student"]), 6)                     # the desk posts every symbol
        self.assertEqual(len([e for e in locked if e["agent"] == "student-owned"]), 3)               # the owned model answered the three equity indices only
        self.assertTrue(all(e["learnable"] for e in locked))                                        # evidence envelopes attached
        self.assertEqual({s["symbol"] for s in receipt["skipped"]}, {"TLT", "GLD", "BTC"})
        self.assertIn(('private', 'factory/salon/accepted/%s-student-SPY.json' % MONDAY), self.cloud.rows)
        self.assertIn(('private', 'factory/salon/accepted/%s-student-owned-SPY.json' % MONDAY), self.cloud.rows)
        entry = json.loads(self.cloud.rows[('private', 'factory/salon/accepted/%s-student-owned-SPY.json' % MONDAY)])
        self.assertEqual(entry["direction"], "DOWN"); self.assertEqual(entry["price_source"], "polygon-full"); self.assertTrue(entry["evidence_id"])
        self.assertIn(('public', 'data/ai/wall/student-latest.json'), self.cloud.rows)
        # posting twice is refused by immutability, not duplicated
        again = wp.post(store, staged, SEASON, self.control, holdout_manifest_hash(store), accept_prediction)
        self.assertEqual(len([e for e in again["entries"] if e["status"] == "locked"]), 0); self.assertTrue(again["skipped"])

    def test_rehearsal_validates_everything_and_writes_nothing(self):
        staged = wp.prepare(self.store, self.cloud, self.rt, self.control, SEASON, READ_DOC, self.now, 'public')
        before = {k for k in self.cloud.rows if k[1].startswith('factory/salon/accepted/') or k[1].startswith('data/ai/wall/')}
        monday = datetime(2026, 9, 21, 9, 31, tzinfo=NY)
        store = Store(self.cloud, 'private', 'public', lambda: monday)
        receipt = wp.post(store, staged, SEASON, self.control, holdout_manifest_hash(store), accept_prediction, rehearse=True)
        self.assertEqual(len([e for e in receipt["entries"] if e["status"] == "rehearsed"]), 9); self.assertTrue(receipt["rehearsal"])
        after = {k for k in self.cloud.rows if k[1].startswith('factory/salon/accepted/') or k[1].startswith('data/ai/wall/')}
        self.assertEqual(before, after)

    def test_outside_the_window_the_door_refuses(self):
        staged = wp.prepare(self.store, self.cloud, self.rt, self.control, SEASON, READ_DOC, self.now, 'public')
        store = Store(self.cloud, 'private', 'public', lambda: datetime(2026, 9, 21, 9, 50, tzinfo=NY))
        receipt = wp.post(store, staged, SEASON, self.control, holdout_manifest_hash(store), accept_prediction)
        self.assertEqual(receipt["entries"], []); self.assertTrue(all("outside_submission_window" in s["reason"] for s in receipt["skipped"] if s["agent"] != "student-owned" or "outside" in s["reason"]))


    def test_a_missing_newest_session_steps_the_window_back_never_inside(self):
        newest = wp.sessions_before(MONDAY, 20, SEASON)[-1]                       # Friday 2026-09-18, still trading during the rehearsal
        self.cloud.rows.pop(('public', wp.GROUPED + "%s/%s.json.gz" % (newest[:4], newest)))
        earlier = wp.sessions_before(newest, 20, SEASON)
        for i, day in enumerate(earlier):
            self.cloud.rows.setdefault(('public', wp.GROUPED + "%s/%s.json.gz" % (day[:4], day)), grouped_day(day, 90 + i * 0.5))
        staged = wp.prepare(self.store, self.cloud, self.rt, self.control, SEASON, READ_DOC, self.now, 'public')
        spy = staged["symbols"]["SPY"]
        self.assertEqual(len(spy["bars"]), 20); self.assertEqual(spy["sessions_used"][1], earlier[-1]); self.assertIn("not on the warehouse yet", spy["note"])
        self.assertTrue(spy["data_cutoff"].startswith(earlier[-1]))
        gap = wp.sessions_before(MONDAY, 20, SEASON)[10]                            # a hole inside the window is never bridged
        self.cloud.rows.pop(('public', wp.GROUPED + "%s/%s.json.gz" % (gap[:4], gap)))
        staged = wp.prepare(self.store, self.cloud, self.rt, self.control, SEASON, READ_DOC, self.now, 'public')
        self.assertIn("session_file_missing", staged["symbols"]["SPY"].get("error") or "")


    def test_evidence_keys_name_the_bucket_role_and_rehearsal_settles_on_the_real_clock(self):
        staged = wp.prepare(self.store, self.cloud, self.rt, self.control, SEASON, READ_DOC, self.now, 'public')
        self.assertTrue(all(k["bucket"] == "public" for k in staged["symbols"]["SPY"]["data_keys"]))
        # the rehearsal store says Monday; the owned answers were submitted "now" (Friday): settling on the Monday clock would call them expired
        monday_store = Store(self.cloud, 'private', 'public', lambda: datetime(2026, 9, 21, 9, 31, tzinfo=NY))
        receipt = wp.post(monday_store, staged, SEASON, self.control, holdout_manifest_hash(monday_store), accept_prediction, rehearse=True, settle_store=self.store)
        owned = [e for e in receipt["entries"] if e["agent"] == "student-owned"]
        self.assertEqual(len(owned), 3); self.assertTrue(all(e["status"] == "rehearsed" for e in receipt["entries"]))
        self.assertFalse(any("expired" in s["reason"] for s in receipt["skipped"]))

    def test_a_new_prepare_hour_submits_fresh_owned_tasks(self):
        wp.prepare(self.store, self.cloud, self.rt, self.control, SEASON, READ_DOC, self.now, 'public')
        first = {k for k in self.cloud.rows if k[1].startswith(fi.REQ_PREFIX)}
        wp.prepare(self.store, self.cloud, self.rt, self.control, SEASON, READ_DOC, self.now + timedelta(hours=2), 'public')
        second = {k for k in self.cloud.rows if k[1].startswith(fi.REQ_PREFIX)}
        self.assertEqual(len(second - first), 6)


if __name__ == '__main__':
    unittest.main()
