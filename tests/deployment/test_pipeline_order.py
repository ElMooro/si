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


PUB = {"margin_min": 30, "max_move_min": 300, "tick_step_min": 15,
       "events": {"FRED": [{"utc": "21:15"}], "FRED macro": [{"utc": "13:30"}], "SEC EDGAR": [{"utc": "03:00"}], "NY Fed": [{"utc": "13:00"}, {"utc": "18:15"}]},
       "slot_max": {"FRED": 2, "FRED macro": 2, "SEC EDGAR": 2, "NY Fed": 2, "FMP": 4}}
HOSTS = {"FRED": "https://api.stlouisfed.org/fred/series/observations?series_id=CPIAUCSL",
         "FRED daily": "https://api.stlouisfed.org/fred/series/observations?series_id=DGS10",
         "SEC EDGAR": "https://efts.sec.gov/LATEST/search-index", "NY Fed": "https://markets.newyorkfed.org/api/rates/sofr",
         "FMP": "https://financialmodelingprep.com/stable/profile", "Polygon": "https://api.polygon.io/v2/snapshot"}


def repo_with(tmp, rules, engines, ticks=None, exempt=(), pulls=None, pub=False, lane=None):
    root = Path(tmp)
    (root / "config").mkdir(parents=True)
    (root / "aws/lambdas").mkdir(parents=True)
    (root / "aws/shared").mkdir(parents=True)
    for fn, hosts in (pulls or {}).items():
        d = root / "aws/lambdas" / fn / "source"
        d.mkdir(parents=True)
        (d / "lambda_function.py").write_text("URLS = %r\n" % [HOSTS[h] for h in hosts], encoding="utf-8")
        (root / "aws/lambdas" / fn / "config.json").write_text(json.dumps({"function_name": fn}), encoding="utf-8")
    if pub:
        (root / "config/provider-publication.json").write_text(json.dumps(PUB), encoding="utf-8")
    if lane:
        (root / "config/intraday-lane.json").write_text(json.dumps(lane), encoding="utf-8")
    members = {t: list(v) for t, v in (ticks or {}).items()}
    assign = {f: {"ticks": [t], "per_day": 1.0} for t, fns in members.items() for f in fns}
    fan = {"version": 2, "ticks": members, "assignment": assign, "disabled": []}
    (root / "config/fanout-manifest.json").write_text(json.dumps(fan, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    (root / "config/schedule-manifest.json").write_text(json.dumps({"rules": rules, "schedules": []}, indent=1) + "\n", encoding="utf-8")
    (root / "config/cadence-floor.json").write_text(json.dumps({"exempt_triggers": [{"kind": "rule", "name": n} for n in exempt]}) + "\n", encoding="utf-8")
    (root / "engine-manifest.json").write_text(json.dumps({"engines": engines}) + "\n", encoding="utf-8")
    return root


def plan_for(rules, engines, ticks=None, exempt=(), pulls=None, pub=False, lane=None, **kw):
    with tempfile.TemporaryDirectory() as tmp:
        root = repo_with(tmp, rules, engines, ticks, exempt, pulls, pub, lane)
        plan, _ = po.build(root, **kw)
    return plan, {a["name"]: a for a in plan["actions"]}


LANE = {"why": "test", "tick": "session-5min", "cadence_floor_reason": "test",
        "schedule": {"name": "jhk-tick-session-5min", "kind": "sched", "group": "default", "expr": "cron(0/5 9-16 ? * MON-FRI *)", "timezone": "America/New_York"},
        "members": ["p1", "p2"]}


def test_a_macro_puller_before_the_print_moves_after_it_and_a_daily_series_puller_does_not():
    rules = [rule("m", "cron(0 11 * * ? *)", "m"), rule("d", "cron(0 11 * * ? *)", "d"), rule("e", "cron(5 21 * * ? *)", "e")]
    engines = [engine("m", keys=["a"]), engine("d", keys=["b"]), engine("e", keys=["c"])]
    plan, by = plan_for(rules, engines, pulls={"m": ["FRED"], "d": ["FRED daily"], "e": ["FRED daily"]}, pub=True, pass_no=2)
    assert by["m"]["after"]["expr"] == "cron(0 14 * * ? *)" and by["m"]["why"] == ["print"]       # 13:30 release + 30 min
    assert "d" not in by, "a daily-series puller in the morning already has yesterday's close"
    assert by["e"]["after"]["expr"] == "cron(45 21 * * ? *)"                                       # H.15 at 21:15 + 30 min
    assert plan["summary"]["providers_before"]["FRED macro"] == {"after_the_print": 0, "before_it": 1}
    assert plan["summary"]["providers_after"]["FRED macro"] == {"after_the_print": 1, "before_it": 0}


def test_a_print_outside_the_pullers_window_leaves_it_alone():
    rules = [rule("o", "cron(0 5 * * ? *)", "o")]                # overnight: no FRED print inside 01:00-09:00
    plan, by = plan_for(rules, [engine("o", keys=["a"])], pulls={"o": ["FRED"]}, pub=True, pass_no=2)
    assert not plan["actions"]


def test_pullers_of_one_provider_never_start_together_beyond_the_slot_cap():
    rules = [rule("f%d" % i, "cron(0 16 * * ? *)", "f%d" % i) for i in range(5)]
    engines = [engine("f%d" % i, keys=["k%d" % i]) for i in range(5)]
    plan, by = plan_for(rules, engines, pulls={"f%d" % i: ["FRED daily"] for i in range(5)}, pub=True, pass_no=2)
    minutes = sorted(a["after"]["expr"] for a in plan["actions"])
    assert len(plan["actions"]) == 3 and minutes == ["cron(1 16 * * ? *)", "cron(1 16 * * ? *)", "cron(2 16 * * ? *)"], minutes
    assert all(a["why"] == ["burst"] for a in plan["actions"])
    assert plan["summary"]["bursts_before"]["FRED"] == {"max_per_minute": 5, "minutes_over_cap": 1}
    assert plan["summary"]["bursts_after"]["FRED"] == {"max_per_minute": 2, "minutes_over_cap": 0}


def test_a_tick_is_packed_into_late_ticks_by_provider_cap_with_writers_of_readers_first():
    rules = [tick_rule("daily-morn", "cron(0 11 * * ? *)")]
    members = ["w1", "w2", "w3", "w4", "w5", "w6", "r1"]
    engines = [engine("w%d" % i, keys=["k%d" % i]) for i in range(1, 7)] + [engine("r1", reads=["k6"])]
    plan, by = plan_for(rules, engines, ticks={"daily-morn": members}, pulls={"w%d" % i: ["FMP"] for i in range(1, 7)}, pub=True, pass_no=2)
    lt = plan["late_ticks"]
    assert sorted(lt) == ["daily-morn-late"], lt
    assert "w6" not in lt["daily-morn-late"]["members"], "the writer of an in-tick reader stays in the first tick"
    assert len(lt["daily-morn-late"]["members"]) == 3 and lt["daily-morn-late"]["expr"] == "cron(15 11 * * ? *)"
    assert plan["tick_moves"]["r1"]["to"] == "daily-morn-late" and plan["tick_moves"]["r1"]["reads_in_tick"] == ["w6"]


def test_the_intraday_lane_is_planned_and_refuses_a_member_of_another_tick():
    rules = [rule("p1", "cron(0 22 * * ? *)", "p1"), rule("p2", "cron(10 22 * * ? *)", "p2")]
    engines = [engine("p1", keys=["a"]), engine("p2", keys=["b"])]
    plan, by = plan_for(rules, engines, pulls={"p1": ["Polygon"], "p2": ["Polygon"]}, pub=True, lane=LANE, pass_no=2)
    lane = plan["intraday_lane"]
    assert lane["members"] == ["p1", "p2"] and not lane["already_in_manifest"] and not lane["exempt_in_floor"]
    fan = {"ticks": {"daily-eve": ["x"]}, "assignment": {}}
    assert po.apply_fanout(fan, plan) == 1 and fan["ticks"]["session-5min"] == ["p1", "p2"] and fan["assignment"]["p1"]["ticks"] == ["session-5min"]
    floor = {"exempt_triggers": []}
    assert po.apply_floor(floor, plan) == 1 and floor["exempt_triggers"][0]["name"] == "jhk-tick-session-5min"
    entries = po.late_tick_entries({"rules": [tick_rule("daily-morn", "cron(0 11 * * ? *)")], "schedules": []}, plan)
    assert entries[-1]["name"] == "jhk-tick-session-5min" and entries[-1]["timezone"] == "America/New_York" and '"session-5min"' in entries[-1]["targets"][0]["input"]
    try:
        plan_for(rules + [tick_rule("daily-eve", "cron(0 22 * * ? *)")], engines, ticks={"daily-eve": ["p1"]}, pulls={"p1": ["Polygon"], "p2": ["Polygon"]}, pub=True, lane=LANE, pass_no=2)
        assert False, "a lane member of another tick must be refused"
    except ValueError:
        pass


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


def test_committed_plans_match_the_configs_and_land_at_most_daily():
    floor = json.loads((ROOT / "config/cadence-floor.json").read_text(encoding="utf-8"))
    exempt = {e["name"] for e in floor.get("exempt_triggers", [])}
    for path in po.plan_paths(ROOT):
        plan = json.loads(path.read_text(encoding="utf-8"))
        assert plan["version"] in ("pipeline-order.v1", "pipeline-order.v2"), path.name
        cap = plan["summary"]["rules"]["max_move_min"] if plan.get("pass", 1) == 1 else plan["summary"]["rules"]["provider_max_move_min"] or 300
        for a in plan["actions"]:
            assert a["op"] == "retime" and a["after"]["state"] == a["before"]["state"] == "ENABLED", a["name"]
            assert not cc.violates(a["after"]["expr"]), (a["name"], a["after"])
            assert a["move_min"] <= cap + 45, (path.name, a["name"], a["move_min"])
            assert "Input" not in json.dumps(a), "a plan must never carry a payload"
        for name, lt in plan["late_ticks"].items():
            assert lt["name"] == "jhk-tick-" + name and lt["kind"] == "sched" and not cc.violates(lt["expr"])
        lane = plan.get("intraday_lane")
        if lane:
            assert lane["schedule"]["name"] in exempt or not plan.get("applied"), "an applied lane must be exempt by name in config/cadence-floor.json"
            assert lane["schedule"]["timezone"] == "America/New_York"
        assert plan["summary"]["after"]["buckets"].get("race", 0) == 0, "the plan leaves a race"
        assert plan["summary"]["unresolved_regressions"] == 0
    assert po.main(["--check"]) == 0, "a pipeline-order plan is stale: python3 scripts/pipeline_order.py --pass N --write"
