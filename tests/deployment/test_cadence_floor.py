"""Repo-wide cadence floor: nothing in this repository may schedule an engine more than once per 24 hours.

Khalid, 2026-10-20, after the $2,800 October bill: "every single [engine] runs once every 24 hours".  This test runs on
every Lambda deploy (tests/deployment/run_tests.py), so a new or edited engine config, a manifest edit, a fan-out tick
membership or a GitHub workflow cron that is faster than daily fails the deploy gate before it can reach the account.
The live account is checked separately (ops cadence snapshot + the governor's daily sentinel).

Exemptions live in config/cadence-floor.json, each with a reason; the only one today is the spend governor's own hourly
tick (the control plane that enforces the cap).
"""
from pathlib import Path
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "aws/lambdas/justhodl-spend-governor/source"))
import cadence_core as cc  # noqa: E402

FLOOR = json.loads((ROOT / "config/cadence-floor.json").read_text(encoding="utf-8"))
EXEMPT = {(e["kind"], e["name"]) for e in FLOOR.get("exempt_triggers", [])}
EXEMPT_NAMES = {n for _, n in EXEMPT}
EXEMPT_WORKFLOWS = {e["file"] for e in FLOOR.get("exempt_workflows", [])}
EXPR = re.compile(r"^\s*(cron|rate|at)\(.*\)\s*$")
# Records of a binding that used to exist, kept for history only: no deploy step reads them (scripts/ greps clean).
REFERENCE_ONLY = ("preserved_schedule_reference", "preserved_eventbridge_scheduler_reference", "retained_legacy_schedule",
                  "retained_unbound_schedule_declaration", "release_schedule_note")


def _walk(o, path=()):
    if isinstance(o, dict):
        for k, v in o.items():
            yield from _walk(v, path + (k,))
    elif isinstance(o, list):
        for i, v in enumerate(o):
            yield from _walk(v, path + (i,))
    elif isinstance(o, str) and EXPR.match(o):
        yield path, o


def _block_name(cfg, path):
    """The trigger name the expression at `path` belongs to (so an exemption can be matched), or None."""
    cur = cfg
    for k in path[:-1]:
        cur = cur[k]
    if isinstance(cur, dict):
        for key in ("scheduler_name", "schedule_name", "rule_name", "name"):
            if isinstance(cur.get(key), str):
                return cur[key]
    return None


def _configs():
    for p in sorted((ROOT / "aws/lambdas").glob("*/config.json")):
        try:
            yield p, json.loads(p.read_text(encoding="utf-8"))
        except ValueError:
            continue


def test_every_engine_config_declares_at_most_one_run_per_day():
    bad = []
    for p, cfg in _configs():
        for path, expr in _walk(cfg):
            if path[0] in REFERENCE_ONLY:
                continue
            if _block_name(cfg, path) in EXEMPT_NAMES:
                continue
            try:
                if cc.violates(expr):
                    bad.append("%s %s = %s" % (p.parent.name, ".".join(map(str, path)), expr))
            except cc.ExpressionError as e:
                bad.append("%s %s = %s (unparseable: %s)" % (p.parent.name, ".".join(map(str, path)), expr, e))
    assert not bad, "faster than once a day (config/cadence-floor.json):\n" + "\n".join(bad)


def test_schedule_manifest_entries_are_at_most_daily_whatever_their_state():
    """A DISABLED entry with a fast expression is one State flip from a storm: the manifest is the declared target."""
    m = json.loads((ROOT / "config/schedule-manifest.json").read_text(encoding="utf-8"))
    bad = []
    for section, kind in (("rules", "rule"), ("schedules", "sched")):
        for e in m.get(section, []):
            if (kind, e.get("name")) in EXEMPT or not e.get("expr"):
                continue
            try:
                if cc.violates(e["expr"]):
                    bad.append("%s %s %s = %s" % (section, e.get("name"), e.get("state"), e["expr"]))
            except cc.ExpressionError:
                bad.append("%s %s unparseable %r" % (section, e.get("name"), e["expr"]))
    assert not bad, "\n".join(bad)


def test_fan_out_membership_is_one_tick_per_function_and_every_tick_is_daily():
    fan = json.loads((ROOT / "config/fanout-manifest.json").read_text(encoding="utf-8"))
    seen = {}
    for tick, fns in fan.get("ticks", {}).items():
        for f in fns:
            assert f not in seen, "%s is in ticks %s and %s: it would run twice a day" % (f, seen[f], tick)
            seen[f] = tick
    for f, rec in fan.get("assignment", {}).items():          # per_day there is observed history, not a target
        assert len(rec.get("ticks", [])) <= 1, (f, rec)
        assert sorted(rec.get("ticks", [])) == sorted(t for t, fs in fan["ticks"].items() if f in fs), f
    # the jhk-tick-* rules that drive those ticks are classic rules in the manifest: covered by the manifest test
    m = json.loads((ROOT / "config/schedule-manifest.json").read_text(encoding="utf-8"))
    ticks = {e["name"]: e for e in m["rules"] if e["name"].startswith("jhk-tick-")}
    assert ticks, "fan-out tick rules missing from the manifest"
    for name, e in ticks.items():
        assert not cc.violates(e["expr"]), (name, e["expr"])


def test_cadence_plan_only_lists_changes_and_every_change_lands_at_most_daily():
    plan = json.loads((ROOT / "config/cadence-plan.json").read_text(encoding="utf-8"))
    for a in plan["actions"]:
        assert a["op"] in ("retime", "disable"), a["name"]
        assert not cc.violates(a["after"]["expr"]), (a["name"], a["after"])
        assert a["after"]["state"] in ("ENABLED", "DISABLED")
        assert "input" not in a and "Input" not in json.dumps(a), "a plan must never carry a payload"


def test_every_exemption_says_why():
    for e in FLOOR.get("exempt_triggers", []) + FLOOR.get("exempt_workflows", []):
        assert len(e.get("why", "")) > 40, e
    assert FLOOR.get("exempt_functions", []) == [], "exempt a trigger, not a whole function"


# ------------------------------------------------------------------------------- GitHub + Cloudflare schedules --
def _field_count(field, lo, hi):
    """How many distinct values a 5-field cron field selects."""
    vals = set()
    for part in field.split(","):
        step = 1
        if "/" in part:
            part, s = part.split("/", 1)
            step = int(s)
        if part == "*":
            a, b = lo, hi
        elif "-" in part:
            a, b = (int(x) for x in part.split("-", 1))
        else:
            a = b = int(part)
        vals.update(range(a, b + 1, step))
    return len(vals)


def github_cron_fires_per_active_day(expr):
    f = expr.split()
    assert len(f) == 5, expr
    return _field_count(f[0], 0, 59) * _field_count(f[1], 0, 23)


def _days_of(expr):
    """Calendar days (of a leap year) on which a 5-field cron line fires.  GitHub semantics: when both day-of-month and
    day-of-week are restricted, a day matching either one fires."""
    import datetime
    f = expr.split()
    dom = {int(x) for x in _values(f[2], 1, 31)}
    mon = {int(x) for x in _values(f[3], 1, 12)}
    dow = {int(x) % 7 for x in _values(f[4], 0, 7)}
    out = set()
    d = datetime.date(2024, 1, 1)
    while d.year == 2024:
        in_dom, in_dow = d.day in dom, (d.weekday() + 1) % 7 in dow
        hit = (in_dom or in_dow) if (f[2] != "*" and f[4] != "*") else (in_dom and in_dow)
        if d.month in mon and hit:
            out.add(d)
        d += datetime.timedelta(days=1)
    return out


def _values(field, lo, hi):
    vals = set()
    for part in field.split(","):
        step = 1
        if "/" in part:
            part, s = part.split("/", 1)
            step = int(s)
        if part == "*":
            a, b = lo, hi
        elif "-" in part:
            a, b = (int(x) for x in part.split("-", 1))
        else:
            a = b = int(part)
        vals.update(range(a, b + 1, step))
    return vals


def workflow_fires_on_busiest_day(exprs):
    """Most firings a workflow with these cron lines can have on one calendar day."""
    per_day = {}
    for e in exprs:
        n = github_cron_fires_per_active_day(e)
        for d in _days_of(e):
            per_day[d] = per_day.get(d, 0) + n
    return max(per_day.values(), default=0)


def _workflow_crons():
    for p in sorted((ROOT / ".github/workflows").glob("*.yml")):
        text = p.read_text(encoding="utf-8")
        for m in re.finditer(r"^\s*-\s*cron:\s*['\"]([^'\"]+)['\"]", text, re.M):
            yield p.name, m.group(1).strip()


def test_github_workflow_schedules_fire_at_most_once_a_day():
    """One workflow with several cron lines fires once per line: the lines of one file are added up per day.
    pages.yml ran every 15 minutes and purged the whole Cloudflare cache 96 times a day (ops 6553)."""
    lines = {}
    for name, expr in _workflow_crons():
        lines.setdefault(name, []).append(expr)
    per_file = {n: workflow_fires_on_busiest_day(ex) for n, ex in lines.items()}
    bad = {n: c for n, c in per_file.items() if c > 1 and n not in EXEMPT_WORKFLOWS}
    assert not bad, "workflows scheduled more than once a day: %s" % bad
    assert per_file, "no scheduled workflows found"


def test_github_cron_counter_is_exact():
    assert github_cron_fires_per_active_day("*/15 * * * *") == 96
    assert github_cron_fires_per_active_day("9,39 * * * *") == 48
    assert github_cron_fires_per_active_day("45 21 * * 1-5") == 1
    assert github_cron_fires_per_active_day("23 0,3,6 1 11 *") == 3
    assert github_cron_fires_per_active_day("0 */6 * * *") == 4
    assert workflow_fires_on_busiest_day(["45 4 * * 6", "0 12 * * 0"]) == 1, "Saturday + Sunday lines never share a day"
    assert workflow_fires_on_busiest_day(["45 20 * * 1-5", "45 21 * * 1-5", "15 3 * * 2-6"]) == 3
    assert workflow_fires_on_busiest_day(["23 0,3,6 1 11 *"]) == 3


def test_cloudflare_worker_crons_are_at_most_daily():
    for toml in (ROOT / "cloudflare/workers").glob("*/wrangler.toml"):
        text = toml.read_text(encoding="utf-8")
        for m in re.finditer(r'crons\s*=\s*\[([^\]]*)\]', text):
            for expr in re.findall(r'"([^"]+)"', m.group(1)):
                assert github_cron_fires_per_active_day(expr) <= 1, (toml.parent.name, expr)


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
    print("cadence_floor tests passed:", len(tests))
