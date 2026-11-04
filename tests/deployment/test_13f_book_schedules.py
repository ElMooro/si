"""The 13F book's observability block (justhodl-13f-book ENGINES) states each 13F-family engine's cadence in words. Those
words must match config/schedule-manifest.json and the book engine's own config, or the page would show a stale cadence
- the exact failure Khalid banked on the old 13F desk ("runs every 6h" while provenance said 19h).

Also pins: the book's Scheduler entry exists in the manifest with the config's cron and target, and the engine invokes
nothing (it is a reader, so the trigger cap does not need to know it).
"""
import importlib.util
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = json.loads((ROOT / "config/schedule-manifest.json").read_text(encoding="utf-8"))
CONFIG = json.loads((ROOT / "aws/lambdas/justhodl-13f-book/config.json").read_text(encoding="utf-8"))
SOURCE = ROOT / "aws/lambdas/justhodl-13f-book/source/lambda_function.py"

# engine -> (manifest kind, trigger name) whose expression must be worded in the engine's ENGINES schedule text
PINS = {
    "justhodl-13f-book": [("scheduler", "justhodl-13f-book-daily")],
    "justhodl-13f-positions": [("scheduler", "justhodl-holdings-originals-research"), ("events", "justhodl-13f-positions-sched")],
    "justhodl-sec-13f": [("events", "justhodl-sec-13f-daily")],
    "justhodl-13f-clone-alpha": [("scheduler", "justhodl-13f-clone-alpha-weekly")],
    "justhodl-13f-price-divergence": [("events", "justhodl-13f-price-divergence-sched")],
    "justhodl-page-ai-commentary": [("events", "justhodl-page-ai-commentary-daily")],
    "justhodl-smart-money-13f": [("events", "smart-money-13f-weekly")],
    "justhodl-smart-money-cluster": [("scheduler", "justhodl-smart-money-cluster-hourly")],
    "justhodl-institutional-footprint": [("events", "justhodl-institutional-footprint-daily")],
}
DAYS = {"MON": "Monday", "TUE": "Tuesday", "WED": "Wednesday", "THU": "Thursday", "FRI": "Friday", "SAT": "Saturday", "SUN": "Sunday"}


def entries():
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in MANIFEST.get(sec, [])}


def words(expr):
    """cron(M H ...) -> the HH:MM UTC and day words the engine text must contain."""
    m = re.match(r"cron\((\d+) (\d+) (\S+) (\S+) (\S+) (\S+)\)", expr)
    assert m, expr
    minute, hour, dom, _mon, dow, _year = m.groups()
    need = ["%02d:%02d UTC" % (int(hour), int(minute))]
    if dow in DAYS:
        need.append(DAYS[dow])
    elif dow == "MON-FRI":
        need.append("weekdays")
    return need


def load_engine():
    shared = str(ROOT / "aws/shared")          # the book imports chain_guard (trigger cap) at module level
    if shared not in sys.path:
        sys.path.insert(0, shared)
    spec = importlib.util.spec_from_file_location("book13f", SOURCE)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["book13f"] = mod
    spec.loader.exec_module(mod)
    return mod


def test_observability_schedule_words_match_the_manifest():
    mod = load_engine()
    by_engine = {e["engine"]: e for e in mod.ENGINES}
    ent = entries()
    bad = []
    for engine, pins in PINS.items():
        text = by_engine[engine]["schedule"]
        for kind, name in pins:
            e = ent.get((kind, name))
            if not e:
                bad.append("%s: %s %s missing from the manifest" % (engine, kind, name))
                continue
            for w in words(e["expr"]):
                if w not in text:
                    bad.append("%s: text %r lacks %r (%s %s = %s)" % (engine, text, w, kind, name, e["expr"]))
            if e["state"] == "DISABLED" and "DISABLED" not in text:
                bad.append("%s: %s is DISABLED but the text does not say so" % (engine, name))
    assert not bad, "\n".join(bad)


def test_disabled_family_members_are_named_as_disabled():
    mod = load_engine()
    ent = entries()
    for engine, name in (("justhodl-smart-money-holdings", "smart-money-holdings-sched"), ("justhodl-smart-money-tracker", "smart-money-tracker-sched")):
        text = {e["engine"]: e for e in mod.ENGINES}[engine]["schedule"]
        e = ent[("scheduler", name)]
        assert e["state"] == "DISABLED" and "DISABLED" in text, (engine, e["state"], text)


def test_book_schedule_is_declared_once_daily_and_targets_the_book():
    e = entries()[("scheduler", CONFIG["eventbridge_scheduler"]["schedule_name"])]
    assert e["expr"] == CONFIG["eventbridge_scheduler"]["cron"] == "cron(37 16 * * ? *)"
    assert e["state"] == "ENABLED" and e["targets"][0]["arn"].endswith(":function:justhodl-13f-book")
    assert CONFIG["eventbridge_scheduler"]["role_arn"].endswith(":role/justhodl-scheduler-role")


def test_book_reads_only_and_declares_its_outputs():
    text = SOURCE.read_text(encoding="utf-8")
    assert ".invoke(" not in text and "boto3.client(\"lambda\"" not in text, "the book is a reader: it must not start engines"
    assert "chain_guard.begin(event, context)" in text, "the book begins its lineage (sec-13f -> 13f-positions -> 13f-book)"
    assert 'Key=OUT_KEY' in text and 'OUT_KEY = os.environ.get("OUT_KEY", "data/13f-book.json")' in text
    mod = load_engine()
    assert mod.CONTRACT == "13f-book.v1" and "data/13f-book.json" in {o for e in mod.ENGINES for o in e["outputs"]}


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print("ok", name)
