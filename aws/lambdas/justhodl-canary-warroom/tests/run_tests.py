#!/usr/bin/env python3
"""Tests of the war room 2.0 (justhodl-canary-warroom) on its real inputs of 2026-11-27.

    python3 aws/lambdas/justhodl-canary-warroom/tests/run_tests.py

No network, no AWS: the bucket is a dictionary. Fixtures: tests/fixtures/canary-warroom-world.json.gz (the twelve
feeds, the history, the weights, the record) and tests/fixtures/canary-warroom-1x-document.json.gz (what the engine
published from those same inputs before 2.0 - every figure of it must still be published).
"""
import gzip
import json
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
SRC = ROOT / "aws/lambdas/justhodl-canary-warroom/source"
WORLD = json.loads(gzip.decompress((ROOT / "tests/fixtures/canary-warroom-world.json.gz").read_bytes()))
BEFORE = json.loads(gzip.decompress((ROOT / "tests/fixtures/canary-warroom-1x-document.json.gz").read_bytes()))
NOW = datetime(2026, 11, 27, 20, 50, 0, tzinfo=timezone.utc)


class Bucket:
    def __init__(self, objects):
        self.objects = dict(objects)
        self.puts = []

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise KeyError(Key)
        body = self.objects[Key]
        raw = body if isinstance(body, bytes) else json.dumps(body).encode()

        class Body:
            def read(self_inner):
                return raw
        return {"Body": Body()}

    def put_object(self, **kw):
        self.puts.append(kw)
        self.objects[kw["Key"]] = kw["Body"]


def world(record=True, history=None):
    objects = dict(WORLD["feeds"])
    objects["data/warroom-history.json"] = history if history is not None else WORLD["data/warroom-history.json"]
    objects["data/warroom-weights.json"] = WORLD["data/warroom-weights.json"]
    if record:
        objects["data/canary-record.json"] = WORLD["data/canary-record.json"]
    return Bucket(objects)


def load(bucket, source=None):
    """The engine's module with the bucket and the clock handed in."""
    fake = types.ModuleType("boto3")
    fake.client = lambda *a, **k: bucket
    saved = sys.modules.get("boto3")
    sys.modules["boto3"] = fake
    for p in (str(SRC), str(ROOT / "aws/shared")):
        if p not in sys.path:
            sys.path.insert(0, p)
    try:
        mod = types.ModuleType("warroom_under_test")
        mod.__file__ = str(SRC / "lambda_function.py")
        code = source if source is not None else (SRC / "lambda_function.py").read_text(encoding="utf-8")
        exec(compile(code, mod.__file__, "exec"), mod.__dict__)
    finally:
        if saved is not None:
            sys.modules["boto3"] = saved
        else:
            sys.modules.pop("boto3", None)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW
    mod.datetime = Clock
    return mod


_DOC = {}


def document():
    if "d" not in _DOC:
        b = world()
        out = load(b).lambda_handler({"dry_run": True}, None)
        assert out["ok"] and out["dry_run"] and b.puts == []
        _DOC["d"] = out["document"]
    return _DOC["d"]


def test_everything_published_before_is_still_published_to_the_figure():
    doc = document()
    assert set(BEFORE) - set(doc) == set(), set(BEFORE) - set(doc)
    assert set(doc) - set(BEFORE) == {"calibrated", "version"}
    for key in ("barometer", "top_picks", "divergences", "mechanisms", "note", "engine", "brain_playbook"):
        assert doc[key] == BEFORE[key], key
    for key in ("early_warning_0_100", "band", "n_firing", "n_canaries", "n_divergences"):
        assert doc["master"][key] == BEFORE["master"][key], key
    assert len(doc["all_canaries"]) == len(BEFORE["all_canaries"]) == 263 and len(doc["firing"]) == len(BEFORE["firing"])
    added = {"record", "id", "key"}
    for new, old in zip(doc["all_canaries"], BEFORE["all_canaries"]):
        assert set(new) - set(old) <= added and {k: v for k, v in new.items() if k not in added} == old
    for new, old in zip(doc["firing"], BEFORE["firing"]):
        assert {k: v for k, v in new.items() if k not in added} == old


def test_the_headline_counts_the_mechanisms_that_vote():
    doc = document()
    voting = len(doc["barometer"]["views"]["per_mechanism"]["by_mechanism"])
    assert doc["master"]["n_mechanisms_voting"] == voting == 9 and doc["master"]["n_mechanisms_watched"] == 12
    assert "across 9 voting mechanisms (12 watched)" in doc["master"]["headline"] and "across 12 mechanisms" in BEFORE["master"]["headline"]
    assert doc["version"] == "2.0.0"


def test_every_canary_is_marked_with_its_record():
    doc = document()
    rec = {c["id"]: c for c in WORLD["data/canary-record.json"]["canaries"]}
    tested = [c for c in doc["all_canaries"] if c["record"]["verdict"] != "no record"]
    assert len(tested) == doc["calibrated"]["canaries"] == 110
    for c in doc["all_canaries"]:
        r = c["record"]
        if c["mechanism"] in ("macro_grid", "plumbing", "ciss"):
            assert c["id"].startswith(c["mechanism"] + ":") and c["key"]
        if r["verdict"] == "no record":
            assert r["why"], c["name"]
        else:
            src = rec[c["id"]]
            assert (r["verdict"], r["months"], r["score_early"], r["weight"]) == (src["verdict"], src["months"], src["auc_early"], src["weight"])
    by = {c.get("id"): c for c in doc["all_canaries"]}
    assert by["macro_grid:em_hy_ytw"]["record"]["verdict"] == "leads" and by["plumbing:SBCACBW027SBOG"]["record"]["verdict"] == "stuck"
    assert by["macro_grid:hyg_lqd"]["record"] == {"verdict": "no record", "why": "its series is not in the warehouse, or its kept history is too short to test"}
    funding = next(c for c in doc["all_canaries"] if c["mechanism"] == "funding")
    assert funding["record"] == {"verdict": "no record", "why": "its mechanism has not been rebuilt through history yet"}


def test_the_calibrated_reading_is_the_records_own_arithmetic():
    doc = document()
    sys.path.insert(0, str(SRC))
    import canary_record as CR
    live = {c["id"]: c["stress"] for c in doc["all_canaries"] if c["record"]["verdict"] != "no record"}
    want = CR.live_reading(WORLD["data/canary-record.json"], live, today=20261127)
    cal = doc["calibrated"]
    assert cal["level"] == want["level"] and cal["turn"] == want["turn"] and cal["cell"] == want["cell"] and cal["day"] == 20261127
    assert cal["turn"]["against_from"] == "the record's own month-end rebuild" and cal["turn"]["against_day"] == 20260831
    assert cal["level"]["band"] in ("CALM", "GUARDED", "ELEVATED", "HIGH RISK", "CRISIS") and cal["turn"]["band"] in ("EASING", "STEADY", "RISING", "RISING FAST")
    assert cal["level"]["after"]["months"] > 0 and cal["turn"]["after"]["months"] > 0
    r = cal["record"]
    assert (r["tested"], r["defined"], r["turn_score_early"], r["level_score_early"]) == (164, 192, 0.6478, 0.5081)
    assert r["turn_weights"] == {"ciss": 0.0, "macro_grid": 0.1311, "plumbing": 0.0733} and cal["what_is_counted"]["early_fall_share"] == 0.1461
    assert "history_entry" not in cal


def test_without_a_record_the_board_is_whole_and_says_so():
    b = world(record=False)
    doc = load(b).lambda_handler({"dry_run": True}, None)["document"]
    assert doc["calibrated"] is None and all(c["record"] == {"verdict": "no record"} for c in doc["all_canaries"])
    assert doc["barometer"] == BEFORE["barometer"] and doc["master"]["early_warning_0_100"] == BEFORE["master"]["early_warning_0_100"]


def test_the_turn_uses_this_engines_own_reading_once_it_is_three_months_old():
    hist = [dict(h) for h in WORLD["data/warroom-history.json"]]
    for h in hist:
        if h["d"] == "2026-08-28":
            h["cal"] = {"l": 44.0, "lp": 30.0, "t": None, "tp": None, "mech": {"macro_grid": 40.0, "plumbing": 60.0, "ciss": 30.0}}
    doc = load(world(history=hist)).lambda_handler({"dry_run": True}, None)["document"]
    t = doc["calibrated"]["turn"]
    assert t["against_day"] == 20260828 and t["against_from"] == "this engine's own reading that day" and t["days_back"] == 91
    lvl = doc["calibrated"]["level"]["by_mechanism"]
    assert t["by_mechanism"] == {"ciss": round(lvl["ciss"] - 30.0, 1), "macro_grid": round(lvl["macro_grid"] - 40.0, 1), "plumbing": round(lvl["plumbing"] - 60.0, 1)}


def test_a_real_run_writes_the_history_then_the_document_and_a_dry_run_writes_nothing():
    b = world()
    out = load(b).lambda_handler({}, None)
    assert out["ok"] and out["version"] == "2.0.0" and out["calibrated"] is True
    keys = [p["Key"] for p in b.puts]
    assert keys[:2] == ["data/warroom-history.json", "data/canary-warroom.json"] and set(keys) == {"data/warroom-history.json", "data/canary-warroom.json"}
    hist = json.loads(b.puts[0]["Body"])
    last = hist[-1]
    assert last["d"] == "2026-11-27" and set(last["cal"]) == {"l", "lp", "t", "tp", "mech"} and set(last["cal"]["mech"]) == {"ciss", "macro_grid", "plumbing"}
    assert all("cal" not in h or h["d"] == "2026-11-27" for h in hist) and len(hist) == len(WORLD["data/warroom-history.json"]) + 1
    doc = json.loads(b.puts[1]["Body"])
    assert doc["calibrated"]["level"]["score"] == last["cal"]["l"] and doc["version"] == "2.0.0"
    assert document()["calibrated"]["level"] == doc["calibrated"]["level"]


def test_the_record_module_is_one_file_in_two_engines():
    a = (SRC / "canary_record.py").read_bytes()
    b = (ROOT / "aws/lambdas/justhodl-warroom-weights/source/canary_record.py").read_bytes()
    assert a == b


def main():
    tests = [(k, v) for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    failed = 0
    for name, fn in tests:
        try:
            fn()
            print("ok   %s" % name)
        except Exception as exc:  # noqa: BLE001
            failed += 1
            import traceback
            print("FAIL %s: %s" % (name, str(exc)[:300]))
            traceback.print_exc(limit=2)
    print("%d tests, %d failed" % (len(tests), failed))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
