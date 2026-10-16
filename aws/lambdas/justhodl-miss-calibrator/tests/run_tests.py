"""Dependency-free tests for justhodl-miss-calibrator (deploy gate). Never contacts AWS.

Regression for the ops 6520 loop (2026-10-16): this engine consumes near_miss.extreme; it must never
publish it, and event-triggered runs are debounced and publish nothing.
"""
import io
import json
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "source" / "lambda_function.py"
FAILS = []


def check(cond, msg):
    print(("ok   " if cond else "FAIL ") + msg)
    if not cond:
        FAILS.append(msg)


class FakeS3:
    def __init__(self, objects, last_modified=None):
        self.objects = dict(objects)
        self.last_modified = last_modified
        self.puts, self.gets, self.heads = [], [], []

    def get_object(self, Bucket, Key):
        self.gets.append(Key)
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
        return {"Body": io.BytesIO(json.dumps(self.objects[Key]).encode())}

    def head_object(self, Bucket, Key):
        self.heads.append(Key)
        if self.last_modified is None:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {"LastModified": self.last_modified}

    def put_object(self, **kw):
        self.puts.append(kw["Key"])


class FakeSSM:
    def __init__(self):
        self.puts = []

    def put_parameter(self, **kw):
        self.puts.append(kw["Name"])


class ClientError(Exception):
    def __init__(self, response, op):
        super().__init__(op)
        self.response = response


def load(published):
    """Exec the engine with stubbed AWS/shared modules; returns the module namespace."""
    stubs = {
        "boto3": types.SimpleNamespace(client=lambda *a, **k: None),
        "botocore": types.ModuleType("botocore"),
        "botocore.exceptions": types.SimpleNamespace(ClientError=ClientError),
        "_sentry_lite": types.SimpleNamespace(track_errors=lambda f: f),
        "managed_secret": types.SimpleNamespace(managed_secret=lambda *a, **k: ""),
        "system_events": types.SimpleNamespace(
            publish=lambda name, detail, **k: published.append(name) or True,
            publish_many=lambda evts, **k: [published.append(e[0]) for e in evts] and {"ok": True}),
    }
    # Stubs stay installed: the handler imports system_events lazily at call time, and this
    # file runs as its own process, so nothing else sees them.
    sys.modules.update(stubs)
    ns = {"__name__": "miss_calibrator_under_test"}
    exec(compile(SRC.read_text(encoding="utf-8"), str(SRC), "exec"), ns)
    return ns


INPUTS = {
    "data/miss-summary.json": {"near_misses_by_signal": {"momentum_breakout": 120, "rsi_reversal": 60, "tiny": 3},
                               "top_recurring_tickers": {"AAPL": 5}},
    "data/signal-scorecard.json": {"by_signal_type": [{"signal_type": "momentum_breakout", "wilson_lb": 0.6, "n_scored": 40}],
                                   "promoted_signals": ["momentum_breakout"]},
    "data/universe.json": {"stocks": [{"ticker": "AAPL"}]},
}


def test_static_no_reemission():
    src = SRC.read_text(encoding="utf-8")
    emits = [ln for ln in src.splitlines() if '"near_miss.extreme"' in ln and not ln.strip().startswith("#")]
    check(not emits, "source never emits near_miss.extreme (found %d emitting lines)" % len(emits))


def test_scheduled_run_publishes_no_extreme():
    published = []
    ns = load(published)
    ns["s3"] = FakeS3(INPUTS)
    ns["ssm"] = FakeSSM()
    res = ns["handler"]({"source": "aws.events", "detail-type": "Scheduled Event"}, None)
    body = json.loads(res["body"])
    check(body.get("ok") is True and body.get("proposals", 0) >= 1, "scheduled run produces proposals")
    check("near_miss.extreme" not in published, "scheduled run publishes no near_miss.extreme")
    check(published == ["calibrator.proposal_high_confidence"],
          "scheduled run still publishes its HIGH-confidence proposal (got %s)" % published)
    check("data/miss-calibrator-proposals.json" in ns["s3"].puts, "scheduled run writes proposals")


def test_event_triggered_debounced():
    published = []
    ns = load(published)
    ns["s3"] = FakeS3(INPUTS, last_modified=datetime.now(timezone.utc) - timedelta(minutes=5))
    ns["ssm"] = FakeSSM()
    ev = {"trigger_event": "near_miss.extreme", "triggered_by": "justhodl-event-coordinator", "trigger_detail": {}}
    res = ns["handler"](ev, None)
    body = json.loads(res["body"])
    check(body.get("debounced") is True, "event-triggered run within 6h of last write is debounced")
    check(not ns["s3"].puts and not ns["s3"].gets and not ns["ssm"].puts, "debounced run reads/writes nothing")
    check(not published, "debounced run publishes nothing")


def test_event_triggered_stale_runs_but_publishes_nothing():
    published = []
    ns = load(published)
    ns["s3"] = FakeS3(INPUTS, last_modified=datetime.now(timezone.utc) - timedelta(hours=7))
    ns["ssm"] = FakeSSM()
    ev = {"trigger_event": "near_miss.extreme", "triggered_by": "justhodl-event-coordinator", "trigger_detail": {}}
    res = ns["handler"](ev, None)
    body = json.loads(res["body"])
    check(body.get("ok") is True and not body.get("debounced"), "event-triggered run after 6h recomputes")
    check(not published, "event-triggered run publishes no events at all")


def test_event_triggered_truth_table():
    ns = load([])
    et = ns["event_triggered"]
    check(et({"triggered_by": "justhodl-event-coordinator"}), "coordinator payload is event-triggered")
    check(et({"trigger_event": "x"}), "trigger_event payload is event-triggered")
    check(not et({"source": "aws.events"}), "schedule payload is not event-triggered")
    check(not et(None), "None is not event-triggered")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
            except Exception as e:  # noqa: BLE001
                FAILS.append("%s raised %r" % (name, e))
                print("FAIL %s raised %r" % (name, e))
    print("\n%d failure(s)" % len(FAILS))
    sys.exit(1 if FAILS else 0)
