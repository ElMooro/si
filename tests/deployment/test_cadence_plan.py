"""cadence_plan: which trigger survives for a job, what changes, and how the repo files are edited.

A job is one function with one payload.  Several triggers for one job run it several times for nothing: one survives.
Khalid, 2026-10-20: every engine runs at most once every 24 hours.
"""
from pathlib import Path
import copy
import json
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import cadence_plan as cp  # noqa: E402
import cadence_core as cc  # noqa: E402

EXEMPT = {"exempt_triggers": [{"kind": "sched", "name": "gov-tick"}]}


def trig(kind, name, expr, fn, state="ENABLED", ikey="{}", **kw):
    return dict({"kind": kind, "name": name, "expr": expr, "fn": fn, "state": state, "ikey": ikey, "input_sha": "h"}, **kw)


def repo_with(tmp, ticks=None, configs=None):
    root = Path(tmp)
    (root / "config").mkdir(parents=True)
    members = {t: list(v) for t, v in (ticks or {}).items()}
    assign = {}
    for t, fns in members.items():
        for f in fns:
            assign.setdefault(f, {"ticks": [], "per_day": 0.0})
            assign[f]["ticks"].append(t)
            assign[f]["per_day"] += 1.0
    fan = {"version": "1.0", "ticks": members, "assignment": assign, "disabled": []}
    (root / "config/fanout-manifest.json").write_text(json.dumps(fan, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    for fn, cfg in (configs or {}).items():
        d = root / "aws/lambdas" / fn
        d.mkdir(parents=True)
        (d / "config.json").write_text(json.dumps(dict({"function_name": fn}, **cfg), indent=2) + "\n", encoding="utf-8")
    return root


def plan_for(snapshot, ticks=None, configs=None):
    with tempfile.TemporaryDirectory() as tmp:
        root = repo_with(tmp, ticks, configs)
        p = cp.build_plan(snapshot, root, EXEMPT)
    return p, {(a["kind"], a["name"]): a for a in p["actions"]}


def test_a_fast_single_trigger_is_retimed_to_once_a_day():
    p, a = plan_for([trig("rule", "fast", "rate(5 minutes)", "fn-a")])
    act = a[("rule", "fast")]
    assert act["op"] == "retime" and act["after"]["state"] == "ENABLED"
    assert not cc.violates(act["after"]["expr"])


def test_twin_triggers_for_one_job_collapse_to_the_daily_one():
    p, a = plan_for([trig("rule", "x-hourly", "rate(1 hour)", "fn-a"),
                     trig("rule", "x-daily", "cron(0 12 * * ? *)", "fn-a")])
    assert a[("rule", "x-daily")]["op"] == "keep"
    dis = a[("rule", "x-hourly")]
    assert dis["op"] == "disable" and dis["after"]["state"] == "DISABLED" and not cc.violates(dis["after"]["expr"])


def test_different_payloads_are_different_jobs_and_each_runs_once_a_day():
    p, a = plan_for([trig("rule", "brief-am", "rate(6 hours)", "router", ikey="sha:aaa"),
                     trig("rule", "brief-pm", "rate(6 hours)", "router", ikey="sha:bbb")])
    assert a[("rule", "brief-am")]["op"] == "retime" and a[("rule", "brief-pm")]["op"] == "retime"


def test_exempt_control_plane_trigger_is_kept_as_is():
    p, a = plan_for([trig("sched", "gov-tick", "cron(7 * * * ? *)", "gov")])
    assert a[("sched", "gov-tick")]["op"] == "keep" and a[("sched", "gov-tick")]["after"]["expr"] == "cron(7 * * * ? *)"


def test_shared_rule_whose_second_function_runs_elsewhere_drops_that_target():
    p, a = plan_for([trig("rule", "shared", "rate(5 minutes)", "fn-a"), trig("rule", "shared", "rate(5 minutes)", "fn-b"),
                     trig("sched", "b-own", "cron(5 6 * * ? *)", "fn-b")])
    act = a[("rule", "shared")]
    assert act["op"] == "retime" and act["remove_functions"] == ["fn-b"]
    assert a[("sched", "b-own")]["op"] == "keep"


def test_fan_out_membership_in_several_ticks_is_cut_to_one():
    ticks = {"hourly": ["fn-a", "fn-b"], "daily-eve": ["fn-a"], "15min": ["fn-b"]}
    p, a = plan_for([], ticks=ticks)
    fan = {"ticks": copy.deepcopy(ticks),
           "assignment": {"fn-a": {"ticks": ["hourly", "daily-eve"], "per_day": 2.0},
                          "fn-b": {"ticks": ["hourly", "15min"], "per_day": 2.0}}}
    cp.apply_fanout(fan, p["tick_removals"])
    members = [f for fs in fan["ticks"].values() for f in fs]
    assert sorted(members) == ["fn-a", "fn-b"], "each function stays in exactly one tick"
    for f in ("fn-a", "fn-b"):
        (tick,) = [t for t, fs in fan["ticks"].items() if f in fs]
        assert fan["assignment"][f] == {"ticks": [tick], "per_day": 1.0}
    assert "cadence_floor_2026_10_20" in fan and set(fan["cadence_floor_2026_10_20"]["removed"]) == {"fn-a", "fn-b"}


def test_a_function_with_its_own_daily_rule_leaves_the_fan_out_tick():
    p, a = plan_for([trig("rule", "own", "cron(0 9 * * ? *)", "fn-a")], ticks={"hourly": ["fn-a"]})
    assert a[("rule", "own")]["op"] == "keep"
    assert p["tick_removals"].get("hourly") == ["fn-a"]


def test_disabled_triggers_are_not_jobs_but_never_keep_a_fast_expression():
    """A switched-off 1-minute rule must not compete with an enabled twin, and must not be one State flip away from
    running every minute: it gets the daily expression and stays DISABLED.  Already-compliant ones are not touched."""
    p, a = plan_for([trig("rule", "off", "rate(1 minute)", "fn-a", state="DISABLED"),
                     trig("rule", "on", "cron(0 9 * * ? *)", "fn-a"),
                     trig("rule", "off-fine", "cron(0 9 * * ? *)", "fn-b", state="DISABLED")])
    assert a[("rule", "on")]["op"] == "keep"
    off = a[("rule", "off")]
    assert off["op"] == "retime" and off["after"]["state"] == "DISABLED" and not cc.violates(off["after"]["expr"])
    assert ("rule", "off-fine") not in a
    assert p["final"][("rule", "off")] == (off["after"]["expr"], "DISABLED")


def test_schedule_manifest_edit_changes_only_the_planned_strings():
    manifest = {"generated_at": "now", "rules": [
        {"expr": "rate(5 minutes)", "kind": "events", "name": "fast", "state": "ENABLED",
         "targets": [{"arn": "arn:aws:lambda:us-east-1:1:function:fn-a", "id": "1"}, {"arn": "arn:aws:lambda:us-east-1:1:function:fn-b", "id": "2"}]},
        {"expr": "cron(0 1 * * ? *)", "kind": "events", "name": "untouched", "state": "ENABLED", "targets": []}],
        "schedules": [{"expr": "rate(1 hour)", "kind": "scheduler", "name": "twin", "state": "ENABLED", "targets": []}]}
    text = json.dumps(manifest, indent=1)
    acts = [{"kind": "rule", "name": "fast", "op": "retime", "after": {"expr": "cron(7 3 * * ? *)", "state": "ENABLED"},
             "remove_functions": ["fn-b"]},
            {"kind": "sched", "name": "twin", "op": "disable", "after": {"expr": "cron(9 4 * * ? *)", "state": "DISABLED"}}]
    new, n = cp.apply_schedule_manifest(text, acts)
    out = json.loads(new)
    assert out["rules"][0]["expr"] == "cron(7 3 * * ? *)" and [t["id"] for t in out["rules"][0]["targets"]] == ["1"]
    assert out["schedules"][0]["state"] == "DISABLED" and out["schedules"][0]["expr"] == "cron(9 4 * * ? *)"
    assert out["rules"][1] == manifest["rules"][1] and out["generated_at"] == "now"
    assert n == 3


def test_config_json_edit_keeps_every_other_byte():
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "config.json"
        text = '{\n  "function_name": "fn-a",\n  "list": [1,2,\n    3],\n  "schedule": {"rule_name": "r", "cron": "cron(0/5 * * * ? *)"}\n}\n'
        path.write_text(text, encoding="utf-8")
        f = cp.JsonFile(path)
        assert f.set_string(("schedule", "cron"), "cron(7 3 * * ? *)")
        assert f.save() == 1
        assert path.read_text(encoding="utf-8") == text.replace("cron(0/5 * * * ? *)", "cron(7 3 * * ? *)")


def test_extra_scheduler_blocks_recorded_by_launch_ops_follow_the_live_cadence():
    """Katlin, options-confluence and chain-resumer record schedules the deploy workflow does not manage in
    `eventbridge_schedulers_extra`, `eventbridge_scheduler_extra` and `scheduler`; the repo must not keep the old cadence."""
    cfgs = {"fn-a": {"eventbridge_schedulers_extra": [
                         {"schedule_name": "a-extra", "expression": "rate(15 minutes)", "state": "ENABLED"},
                         {"schedule_name": "a-ok", "expression": "cron(0 9 ? * SUN *)"}],
                     "eventbridge_scheduler_extra": {"schedule_name": "a-weekly", "cron": "cron(0 9 ? * SUN *)"},
                     "scheduler": {"name": "a-5min", "expr": "rate(5 minutes)"}}}
    snapshot = [trig("sched", "a-extra", "rate(15 minutes)", "fn-a", ikey="sha:1"),
                trig("sched", "a-ok", "cron(0 9 ? * SUN *)", "fn-a", ikey="sha:2"),
                trig("sched", "a-weekly", "cron(0 9 ? * SUN *)", "fn-a", ikey="sha:3"),
                trig("sched", "a-5min", "rate(5 minutes)", "fn-a", ikey="sha:4")]
    with tempfile.TemporaryDirectory() as tmp:
        root = repo_with(tmp, configs=cfgs)
        (root / "config/schedule-manifest.json").write_text(json.dumps({"rules": [], "schedules": []}, indent=1), encoding="utf-8")
        plan = cp.build_plan(snapshot, root, EXEMPT)
        stats, _ = cp.apply_repo(plan, root)
        cfg = json.loads((root / "aws/lambdas/fn-a/config.json").read_text(encoding="utf-8"))
    extras = cfg["eventbridge_schedulers_extra"]
    assert not cc.violates(extras[0]["expression"]) and extras[0]["state"] == "ENABLED"
    assert extras[1]["expression"] == "cron(0 9 ? * SUN *)", "a compliant record is left alone"
    assert cfg["eventbridge_scheduler_extra"]["cron"] == "cron(0 9 ? * SUN *)"
    assert not cc.violates(cfg["scheduler"]["expr"])
    assert stats["config_expressions"] == 2


def _apply_repo_for(configs, snapshot):
    with tempfile.TemporaryDirectory() as tmp:
        root = repo_with(tmp, configs=configs)
        (root / "config/schedule-manifest.json").write_text(json.dumps({"rules": [], "schedules": []}, indent=1), encoding="utf-8")
        plan = cp.build_plan(snapshot, root, EXEMPT)
        stats, notes = cp.apply_repo(plan, root)
        cfgs = {fn: json.loads((root / "aws/lambdas" / fn / "config.json").read_text(encoding="utf-8")) for fn in configs}
    return cfgs, stats, notes


def test_exempt_control_plane_declaration_is_never_rewritten():
    """The governor's own hourly tick is the one thing that must stay fast: its config.json is not touched."""
    cfgs, stats, _ = _apply_repo_for(
        {"gov": {"eventbridge_scheduler": {"schedule_name": "gov-tick", "cron": "cron(7 * * * ? *)", "state": "ENABLED"}}},
        [trig("sched", "gov-tick", "cron(7 * * * ? *)", "gov")])
    assert cfgs["gov"]["eventbridge_scheduler"]["cron"] == "cron(7 * * * ? *)"
    assert not stats.get("config_expressions")


def test_declared_violation_follows_the_live_expression_when_the_account_already_runs_it_daily():
    """check_existing_schedule.py refuses a deploy whose config differs from live: the declaration is set to what the
    account says, not to a fresh invention."""
    cfgs, stats, _ = _apply_repo_for({"fn-a": {"schedule": {"rule_name": "r", "cron": "cron(0 * * * ? *)"}}},
                                     [trig("rule", "r", "cron(46 19 * * ? *)", "fn-a")])
    assert cfgs["fn-a"]["schedule"]["cron"] == "cron(46 19 * * ? *)"


def test_declaration_of_a_trigger_that_is_not_in_the_account_is_still_made_compliant():
    cfgs, stats, _ = _apply_repo_for({"fn-a": {"schedule": {"rule_name": "ghost", "cron": "cron(0 * * * ? *)"}}}, [])
    assert not cc.violates(cfgs["fn-a"]["schedule"]["cron"])


def test_declaration_of_a_disabled_fast_trigger_equals_what_the_account_will_say():
    cfgs, stats, _ = _apply_repo_for({"fn-a": {"schedule": {"rule_name": "off", "cron": "rate(15 minutes)"}}},
                                     [trig("rule", "off", "rate(15 minutes)", "fn-a", state="DISABLED")])
    p, a = plan_for([trig("rule", "off", "rate(15 minutes)", "fn-a", state="DISABLED")])
    assert cfgs["fn-a"]["schedule"]["cron"] == a[("rule", "off")]["after"]["expr"]


def test_manifest_entries_the_plan_does_not_touch_are_brought_in_line_with_the_account():
    manifest = {"rules": [{"expr": "rate(30 minutes)", "kind": "events", "name": "stale", "state": "ENABLED", "targets": []},
                          {"expr": "cron(0 1 * * ? *)", "kind": "events", "name": "same", "state": "ENABLED", "targets": []},
                          {"expr": "rate(5 minutes)", "kind": "events", "name": "unknown-to-account", "state": "ENABLED", "targets": []}],
                "schedules": []}
    text = json.dumps(manifest, indent=1)
    final = {("rule", "stale"): ("cron(9 9 * * ? *)", "DISABLED"), ("rule", "same"): ("cron(0 1 * * ? *)", "ENABLED")}
    new, n = cp.apply_schedule_manifest(text, [], final)
    out = json.loads(new)
    assert out["rules"][0]["expr"] == "cron(9 9 * * ? *)" and out["rules"][0]["state"] == "DISABLED"
    assert out["rules"][1] == manifest["rules"][1] and out["rules"][2] == manifest["rules"][2] and n == 1


def test_hashed_input_key_never_contains_the_payload():
    k = cp.hashed_input_key('{"token": "SYNTHETIC-SECRET", "mode": "x", "source": "aws.events"}')
    assert k.startswith("sha:") and "SYNTHETIC" not in k
    assert cp.hashed_input_key("") == cp.hashed_input_key("{}") == "{}"
    assert cp.hashed_input_key('{"source": "aws.events"}') == "{}"


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
    print("cadence_plan tests passed:", len(tests))
