"""pipeline_order: readers fire after their writers (Khalid, 2026-10-20).

Synthetic repos exercise the planner's rules one at a time; the last tests gate the committed plan itself.
"""
from pathlib import Path
import json
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "aws/lambdas/justhodl-spend-governor/source"))
import cadence_core as cc  # noqa: E402
import pipeline_order as po  # noqa: E402

ARN = "arn:aws:lambda:us-east-1:857687956942:function:%s"


def rule(name, expr, fn, state="ENABLED"):
    return {"expr": expr, "kind": "events", "name": name, "state": state, "targets": [{"arn": ARN % fn, "id": "target1", "input": None, "path": None}]}


def tick_rule(tick, expr):
    return {"expr": expr, "kind": "events", "name": "jhk-tick-" + tick, "state": "ENABLED",
            "targets": [{"arn": ARN % "justhodl-scheduler", "id": "target1", "input": json.dumps({"tick": tick}), "path": None}]}


def engine(name, keys=(), reads=()):
    return {"engine": name, "keys": list(keys), "reads": list(reads)}


def repo_with(tmp, rules, engines, ticks=None, exempt=()):
    root = Path(tmp)
    (root / "config").mkdir(parents=True)
    (root / "aws/lambdas").mkdir(parents=True)
    members = {t: list(v) for t, v in (ticks or {}).items()}
    assign = {f: {"ticks": [t], "per_day": 1.0} for t, fns in members.items() for f in fns}
    fan = {"version": 2, "ticks": members, "assignment": assign, "disabled": []}
    (root / "config/fanout-manifest.json").write_text(json.dumps(fan, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    (root / "config/schedule-manifest.json").write_text(json.dumps({"rules": rules, "schedules": []}, indent=1) + "\n", encoding="utf-8")
    (root / "config/cadence-floor.json").write_text(json.dumps({"exempt_triggers": [{"kind": "rule", "name": n} for n in exempt]}) + "\n", encoding="utf-8")
    (root / "engine-manifest.json").write_text(json.dumps({"engines": engines}) + "\n", encoding="utf-8")
    return root


def plan_for(rules, engines, ticks=None, exempt=(), **kw):
    with tempfile.TemporaryDirectory() as tmp:
        root = repo_with(tmp, rules, engines, ticks, exempt)
        plan, _ = po.build(root, **kw)
    return plan, {a["name"]: a for a in plan["actions"]}


def test_a_reader_firing_inside_its_writers_margin_moves_past_it():
    plan, by = plan_for([rule("w", "cron(0 10 * * ? *)", "w"), rule("r", "cron(2 10 * * ? *)", "r")],
                        [engine("w", keys=["data/a.json"]), engine("r", reads=["data/a.json"])])
    assert by["r"]["after"]["expr"] == "cron(5 10 * * ? *)", by["r"]              # margin 5 min (no runtime known -> 60 s -> 5)
    assert "w" not in by, "the writer never moves"
    assert plan["summary"]["before"]["buckets"]["race"] == 1 and "race" not in plan["summary"]["after"]["buckets"]


def test_a_reader_just_before_its_writer_moves_to_just_after_it():
    plan, by = plan_for([rule("w", "cron(30 10 * * ? *)", "w"), rule("r", "cron(0 10 * * ? *)", "r")],
                        [engine("w", keys=["data/a.json"]), engine("r", reads=["data/a.json"])])
    assert by["r"]["after"]["expr"] == "cron(35 10 * * ? *)"
    assert by["r"]["move_min"] == 35 and by["r"]["window"] == "pre-market" and not by["r"]["crosses_window_end"]
    assert plan["summary"]["after"]["buckets"].get("stale", 0) == 0


def test_a_reader_already_after_its_writer_is_left_alone():
    plan, by = plan_for([rule("w", "cron(0 10 * * ? *)", "w"), rule("r", "cron(0 11 * * ? *)", "r")],
                        [engine("w", keys=["data/a.json"]), engine("r", reads=["data/a.json"])])
    assert not plan["actions"]


def test_a_move_that_would_need_more_than_the_cap_is_left_alone_and_listed():
    plan, by = plan_for([rule("w", "cron(0 12 * * ? *)", "w"), rule("r", "cron(50 10 * * ? *)", "r")],
                        [engine("w", keys=["data/a.json"]), engine("r", reads=["data/a.json"])], stale_min=120, max_move=60)
    assert not plan["actions"]
    assert plan["left_alone"]["r"]["left_alone"][0]["writer"] == "w" and plan["left_alone"]["r"]["left_alone"][0]["would_need"] == "12:05"


def test_a_move_may_cross_the_window_end_only_within_the_grace():
    # pre-market ends 14:30; writer at 14:40 -> reader would need 14:45 (15 min past the end): inside the 30 min grace
    plan, by = plan_for([rule("w", "cron(40 14 * * ? *)", "w"), rule("r", "cron(20 14 * * ? *)", "r")],
                        [engine("w", keys=["data/a.json"]), engine("r", reads=["data/a.json"])])
    assert by["r"]["after"]["expr"] == "cron(45 14 * * ? *)" and by["r"]["crosses_window_end"]
    plan, by = plan_for([rule("w", "cron(40 14 * * ? *)", "w"), rule("r", "cron(20 14 * * ? *)", "r")],
                        [engine("w", keys=["data/a.json"]), engine("r", reads=["data/a.json"])], grace_min=10)
    assert not plan["actions"] and "r" in plan["left_alone"]


def test_post_close_window_wraps_midnight():
    plan, by = plan_for([rule("w", "cron(50 23 * * ? *)", "w"), rule("r", "cron(30 23 * * ? *)", "r")],
                        [engine("w", keys=["data/a.json"]), engine("r", reads=["data/a.json"])])
    assert by["r"]["after"]["expr"] == "cron(55 23 * * ? *)" and by["r"]["window"] == "post-close" and not by["r"]["crosses_window_end"]
    plan, by = plan_for([rule("w", "cron(10 0 * * ? *)", "w"), rule("r", "cron(50 23 * * ? *)", "r")],
                        [engine("w", keys=["data/a.json"]), engine("r", reads=["data/a.json"])])
    assert by["r"]["after"]["expr"] == "cron(15 0 * * ? *)" and by["r"]["move_min"] == 25 and not by["r"]["crosses_window_end"]


def test_a_moved_reader_is_somebody_elses_writer_and_the_chain_follows():
    rules = [rule("a", "cron(30 10 * * ? *)", "a"), rule("b", "cron(0 10 * * ? *)", "b"), rule("c", "cron(10 10 * * ? *)", "c")]
    engines = [engine("a", keys=["x"]), engine("b", keys=["y"], reads=["x"]), engine("c", reads=["y"])]
    plan, by = plan_for(rules, engines)
    assert by["b"]["after"]["expr"] == "cron(35 10 * * ? *)"           # after a
    assert by["c"]["after"]["expr"] == "cron(40 10 * * ? *)"           # c read b 10 min after it; b moved, so c follows
    assert plan["summary"]["unresolved_regressions"] == 0


def test_a_writer_whose_reader_cannot_follow_stays_in_place():
    # b reads a (stale: 10:00 vs 10:30) and c (a pinned tick member at 10:05) reads b freshly; moving b past 10:30 would
    # make c read yesterday's b, and c cannot move -> b stays where it is
    rules = [rule("a", "cron(30 10 * * ? *)", "a"), rule("b", "cron(0 10 * * ? *)", "b"), tick_rule("t", "cron(5 10 * * ? *)")]
    engines = [engine("a", keys=["x"]), engine("b", keys=["y"], reads=["x"]), engine("c", reads=["y"])]
    plan, by = plan_for(rules, engines, ticks={"t": ["c"]})
    assert "b" not in by and "b" in plan["kept_in_place"], plan["kept_in_place"]
    assert plan["summary"]["unresolved_regressions"] == 0


def test_feedback_pairs_are_not_ordering_constraints():
    rules = [rule("a", "cron(0 10 * * ? *)", "a"), rule("b", "cron(2 10 * * ? *)", "b")]
    engines = [engine("a", keys=["x"], reads=["y"]), engine("b", keys=["y"], reads=["x"])]
    plan, by = plan_for(rules, engines)
    assert not plan["actions"] and plan["summary"]["feedback_edges_ignored"] == 2


def test_tick_members_reading_other_members_of_their_tick_get_a_late_tick():
    rules = [tick_rule("daily-morn", "cron(0 11 * * ? *)"), rule("r2", "cron(5 11 * * ? *)", "r2")]
    engines = [engine("w", keys=["x"]), engine("r", keys=["r-out"], reads=["x"]), engine("r2", reads=["r-out"]), engine("o", reads=["nothing"])]
    plan, by = plan_for(rules, engines, ticks={"daily-morn": ["w", "r", "o"]})
    lt = plan["late_ticks"]["daily-morn-late"]
    assert lt["expr"] == "cron(15 11 * * ? *)" and lt["members"] == ["r"] and lt["name"] == "jhk-tick-daily-morn-late" and lt["kind"] == "sched"
    assert plan["tick_moves"]["r"]["reads_in_tick"] == ["w"]
    assert by["r2"]["after"]["expr"] == "cron(20 11 * * ? *)"          # r2 read r five minutes after the tick: it follows the late tick
    fan = {"ticks": {"daily-morn": ["w", "r", "o"]}, "assignment": {"r": {"ticks": ["daily-morn"], "per_day": 1.0}}}
    assert po.apply_fanout(fan, plan) == 1
    assert fan["ticks"] == {"daily-morn": ["w", "o"], "daily-morn-late": ["r"]} and fan["assignment"]["r"]["ticks"] == ["daily-morn-late"]
    assert po.apply_fanout(fan, plan) == 0                              # idempotent


def test_exempt_and_non_daily_triggers_are_pinned():
    rules = [rule("w", "cron(30 10 * * ? *)", "w"), rule("r", "cron(0 10 * * ? *)", "r"), rule("m", "cron(0 10 1 * ? *)", "m")]
    engines = [engine("w", keys=["x"]), engine("r", reads=["x"]), engine("m", reads=["x"])]
    plan, by = plan_for(rules, engines, exempt=("r",))
    assert not plan["actions"] and plan["summary"]["pinned"] == 2


def test_weekday_crons_keep_their_days():
    plan, by = plan_for([rule("w", "cron(30 10 ? * MON-FRI *)", "w"), rule("r", "cron(0 10 ? * MON-FRI *)", "r")],
                        [engine("w", keys=["x"]), engine("r", reads=["x"])])
    assert by["r"]["after"]["expr"] == "cron(35 10 ? * MON-FRI *)"


def test_at_most_four_triggers_land_on_one_minute():
    rules = [rule("w", "cron(0 10 * * ? *)", "w")] + [rule("r%d" % i, "cron(10 9 * * ? *)", "r%d" % i) for i in range(6)]
    engines = [engine("w", keys=["x"])] + [engine("r%d" % i, reads=["x"]) for i in range(6)]
    plan, by = plan_for(rules, engines)
    minutes = sorted(a["after"]["expr"] for a in plan["actions"])
    assert minutes.count("cron(5 10 * * ? *)") == 4 and minutes.count("cron(6 10 * * ? *)") == 2


def test_committed_plan_matches_the_configs_and_lands_at_most_daily():
    plan = json.loads((ROOT / "config/pipeline-order-plan.json").read_text(encoding="utf-8"))
    assert plan["version"] == "pipeline-order.v1"
    for a in plan["actions"]:
        assert a["op"] == "retime" and a["after"]["state"] == a["before"]["state"] == "ENABLED", a["name"]
        assert not cc.violates(a["after"]["expr"]), (a["name"], a["after"])
        assert a["move_min"] <= plan["summary"]["rules"]["max_move_min"] + 10, a["name"]
        assert "Input" not in json.dumps(a), "a plan must never carry a payload"
    for name, lt in plan["late_ticks"].items():
        assert lt["name"] == "jhk-tick-" + name and lt["kind"] == "sched" and not cc.violates(lt["expr"])
    assert plan["summary"]["after"]["buckets"].get("race", 0) == 0, "the plan leaves a race"
    assert plan["summary"]["unresolved_regressions"] == 0
    assert po.main(["--check"]) == 0, "config/pipeline-order-plan.json is stale: python3 scripts/pipeline_order.py --write"
