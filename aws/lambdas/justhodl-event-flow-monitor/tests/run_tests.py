"""event-flow-monitor v1.1.0 - offline: streamed day-logs, cached per-day counts, budget guard, unchanged analysis."""
from pathlib import Path
import io
import json
import os
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone

os.environ.setdefault("TELEGRAM_TOKEN", "test-token")        # managed_secret resolves from env: no SSM in tests
ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "source"))
import lambda_function as engine  # noqa: E402


class Body:
    def __init__(self, raw):
        self._b = io.BytesIO(raw)
        self.closed = False

    def iter_lines(self):
        for line in self._b:
            yield line.rstrip(b"\n")

    def read(self):
        return self._b.read()

    def close(self):
        self.closed = True


class NoSuchKey(Exception):
    pass


class FakeS3:
    exceptions = types.SimpleNamespace(NoSuchKey=NoSuchKey)

    def __init__(self, objects):
        self.objects = dict(objects)
        self.gets, self.puts, self.bodies = [], {}, []

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        if Key not in self.objects:
            raise NoSuchKey(Key)
        b = Body(self.objects[Key])
        self.bodies.append(b)
        return {"Body": b}

    def put_object(self, Bucket, Key, Body, **kw):
        self.puts[Key] = Body
        self.objects[Key] = Body


class FakeSSM:
    def __init__(self):
        self.params = {}

    def get_parameter(self, Name, **kw):
        if Name not in self.params:
            raise KeyError(Name)
        return {"Parameter": {"Value": self.params[Name]}}

    def put_parameter(self, Name, Value, **kw):
        self.params[Name] = Value


class FakeCW:
    def get_metric_statistics(self, **kw):
        return {"Datapoints": [{"Sum": 10.0}] if kw["MetricName"] == "Invocations" else [{"Sum": 0.0}]}


NOW = datetime.now(timezone.utc)
TODAY = engine._date_key(NOW)


def day(i):
    return engine._date_key(NOW - timedelta(days=i))


def jsonl(entries):
    return ("\n".join(json.dumps(e) for e in entries) + "\n").encode()


def entry(event, ts, **detail):
    return {"ts": ts, "event": event, "detail": dict(detail, big="x" * 500), "route": {"invoked": ["a", "b"]}}


class Monitor(unittest.TestCase):
    def setUp(self):
        self.ssm, self.cw = FakeSSM(), FakeCW()
        objs = {}
        t_recent = (NOW - timedelta(minutes=30)).isoformat()
        objs["system-events/audit/%s.jsonl" % TODAY] = jsonl(
            [entry("outcome.resolved", t_recent), entry("engine.error", t_recent, engine="justhodl-x", phase="run", error="boom"),
             entry("engine.error", t_recent, engine="justhodl-x", phase="run", error="boom"), entry("engine.error", t_recent, engine="justhodl-x", phase="run", error="boom")]
            + [{"not": "a dict"}] * 0) + b"garbage line\n" + jsonl([entry("signal.fired", t_recent)])
        # a storm day: many events, read once then cached
        objs["system-events/audit/%s.jsonl" % day(3)] = jsonl([entry("near_miss.extreme", "2026-10-13T10:00:00+00:00")] * 2000 + [entry("outcome.resolved", "2026-10-13T11:00:00+00:00")] * 4)
        objs["system-events/audit/%s.jsonl" % day(5)] = jsonl([entry("outcome.resolved", "2026-10-11T11:00:00+00:00")] * 3)
        self.s3 = FakeS3(objs)
        engine.s3, engine.ssm, engine.cw = self.s3, self.ssm, self.cw
        self.sent = []
        engine.send_telegram = lambda text: self.sent.append(text) or True

    def ctx(self, seconds=300):
        return types.SimpleNamespace(get_remaining_time_in_millis=lambda: int(seconds * 1000))

    def test_streams_day_logs_caches_finished_days_and_keeps_the_analysis(self):
        res = json.loads(engine.handler({}, self.ctx())["body"])
        out = json.loads(self.s3.puts["data/event-flow-health.json"])
        self.assertEqual(out["engine_version"], engine.VERSION)
        self.assertEqual(out["window_days_read"], 14)
        self.assertEqual(out["audit_days_present"], 3)
        self.assertEqual(out["days_not_read"], [])
        self.assertEqual(out["pulse"]["status"], "GREEN")
        self.assertEqual(out["totals_today"], {"n_events": 5, "n_event_types_seen": 3})
        self.assertEqual(out["by_event"]["near_miss.extreme"]["history"], {day(3): 2000})
        self.assertEqual(out["by_event"]["outcome.resolved"]["today"], 1)
        self.assertEqual(out["by_event"]["outcome.resolved"]["history"][day(5)], 3)
        self.assertEqual(out["engine_errors"]["n_total_today"], 3)
        self.assertEqual(out["engine_errors"]["by_engine"], {"x": 3})
        self.assertEqual(out["engine_errors"]["recent_errors"][0]["error"], "boom")
        self.assertTrue(any(a["type"] == "ENGINE_ERROR_SPIKE" for a in out["anomalies"]))
        self.assertEqual(out["coordinator_health"]["status"], "GREEN")
        self.assertTrue(all(b.closed for b in self.s3.bodies))
        # finished days were summarised and cached; today was not
        cached = sorted(k for k in self.s3.puts if k.startswith(engine.COUNTS_PREFIX))
        self.assertEqual(len(cached), 13)
        self.assertNotIn(engine.COUNTS_PREFIX + TODAY + ".json", cached)
        storm = json.loads(self.s3.puts[engine.COUNTS_PREFIX + day(3) + ".json"])
        self.assertEqual((storm["schema"], storm["present"], storm["n_events"], storm["counts"]["near_miss.extreme"]), (engine.COUNTS_SCHEMA, True, 2004, 2000))
        missing = json.loads(self.s3.puts[engine.COUNTS_PREFIX + day(1) + ".json"])
        self.assertEqual((missing["present"], missing["counts"]), (False, {}))
        self.assertEqual(res["alerts_sent"], len(self.sent))
        self.assertTrue(any("Engine error spike" in s for s in self.sent))
        self.assertIn(engine.SSM_PULSE_PATH, self.ssm.params)
        # second run: finished days come from the cache, the storm log is not streamed again
        self.s3.gets.clear()
        engine.handler({}, self.ctx())
        self.assertNotIn("system-events/audit/%s.jsonl" % day(3), self.s3.gets)
        self.assertIn("system-events/audit/%s.jsonl" % TODAY, self.s3.gets)
        self.assertEqual(sum(1 for k in self.s3.gets if k.startswith(engine.COUNTS_PREFIX)), 13)

    def test_budget_stops_reading_older_days_and_says_so(self):
        old = engine.READ_RESERVE_S
        engine.READ_RESERVE_S = 10 ** 6
        try:
            engine.handler({}, self.ctx())
        finally:
            engine.READ_RESERVE_S = old
        out = json.loads(self.s3.puts["data/event-flow-health.json"])
        self.assertEqual(out["window_days_read"], 1)
        self.assertEqual(len(out["days_not_read"]), 13)
        self.assertEqual(out["pulse"]["status"], "GREEN", "today is always read")
        self.assertEqual(out["by_event"]["outcome.resolved"]["rolling_mean"], None)

    def test_silent_bus_is_reported_red_not_hidden(self):
        del self.s3.objects["system-events/audit/%s.jsonl" % TODAY]
        engine.handler({}, self.ctx())
        out = json.loads(self.s3.puts["data/event-flow-health.json"])
        self.assertEqual(out["pulse"]["status"], "RED")
        self.assertEqual(out["audit_days_present"], 2)
        self.assertTrue(any("Event bus silent" in s for s in self.sent))

    def test_slim_entries_keep_only_what_the_analysis_reads(self):
        e = engine._slim(entry("engine.error", "2026-10-26T00:00:00+00:00", engine="justhodl-y", phase="p", error="e", extra=1))
        self.assertEqual(e, {"ts": "2026-10-26T00:00:00+00:00", "event": "engine.error", "detail": {"engine": "justhodl-y", "phase": "p", "error": "e"}})
        self.assertEqual(engine.aggregate_by_event({"d1": {"a": 2}, "d2": {"a": 1, "b": 1}, "d3": {}}), {"a": {"d1": 2, "d2": 1}, "b": {"d2": 1}})

    def test_config_budget(self):
        cfg = json.loads((Path(__file__).resolve().parents[1] / "config.json").read_text())
        self.assertGreaterEqual(cfg["memory"], 1024)
        self.assertGreaterEqual(cfg["timeout"], 300)
        self.assertEqual(cfg["schedule"]["cron"], "cron(0 5 * * ? *)")
        self.assertEqual(cfg["schedule"]["rule_name"], "event-flow-monitor-hourly")


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Monitor))
    if not result.wasSuccessful():
        raise SystemExit(1)
