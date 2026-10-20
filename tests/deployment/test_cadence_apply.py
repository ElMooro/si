"""cadence_apply against fake EventBridge / Scheduler / S3 clients.  Never touches AWS.

What must hold when the once-a-day plan is applied to the live account (Khalid, 2026-10-20):
  * an item is only changed when it still looks exactly like the snapshot the plan was made from
  * the full definition is backed up first, put_rule / update_schedule lose no field, the result is read back
  * retimes happen before disables; a dry run writes nothing; one failure never stops the others
"""
from pathlib import Path
import copy
import json
import sys

from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))
import cadence_apply as ca  # noqa: E402

ACCT = "123456789012"


def lam(name):
    return "arn:aws:lambda:us-east-1:%s:function:%s" % (ACCT, name)


def err(code):
    return ClientError({"Error": {"Code": code, "Message": "synthetic"}}, "op")


class Pager:
    def __init__(self, pages):
        self.pages = pages

    def paginate(self, **kw):
        return iter(self.pages)


class FakeEvents:
    def __init__(self):
        self.rules, self.targets, self.calls, self.fail_put = {}, {}, [], set()
        self.ignore_put = set()

    def add(self, name, expr, state="ENABLED", targets=(), bus="default", **extra):
        self.rules[(bus, name)] = dict({"Name": name, "ScheduleExpression": expr, "State": state, "EventBusName": bus}, **extra)
        self.targets[(bus, name)] = [dict(t) for t in targets]

    def list_event_buses(self, Limit=100, NextToken=None):
        names = sorted({b for b, _ in self.rules} | {"default"})
        if NextToken is None and len(names) > 1:
            return {"EventBuses": [{"Name": names[0]}], "NextToken": "t1"}
        return {"EventBuses": [{"Name": n} for n in (names[1:] if NextToken else names)]}

    def get_paginator(self, op):
        if op == "list_rules":
            class P:
                def paginate(s, EventBusName="default"):
                    rows = [dict(v) for (b, _), v in sorted(self.rules.items()) if b == EventBusName]
                    return iter([{"Rules": rows[:2]}, {"Rules": rows[2:]}])
            return P()
        if op == "list_targets_by_rule":
            class T:
                def paginate(s, Rule, EventBusName="default"):
                    return iter([{"Targets": copy.deepcopy(self.targets.get((EventBusName, Rule), []))}])
            return T()
        raise AssertionError(op)

    def describe_rule(self, Name, EventBusName="default"):
        if (EventBusName, Name) not in self.rules:
            raise err("ResourceNotFoundException")
        return dict(self.rules[(EventBusName, Name)], ResponseMetadata={})

    def put_rule(self, **kw):
        self.calls.append(("put_rule", kw["Name"], kw.get("EventBusName", "default")))
        key = (kw.get("EventBusName", "default"), kw["Name"])
        if kw["Name"] in self.fail_put:
            raise err("InternalException")
        if kw["Name"] in self.ignore_put:
            return {}
        # full replacement: a field that is not passed is lost
        self.rules[key] = {k: v for k, v in kw.items()}
        self.rules[key]["EventBusName"] = key[0]
        return {}

    def disable_rule(self, **kw):
        raise AssertionError("retime and disable are one put_rule write")

    def remove_targets(self, Rule, EventBusName="default", Ids=()):
        self.calls.append(("remove_targets", Rule, tuple(Ids)))
        self.targets[(EventBusName, Rule)] = [t for t in self.targets[(EventBusName, Rule)] if t["Id"] not in set(Ids)]


class FakeScheduler:
    def __init__(self):
        self.items, self.calls, self.ignore = {}, [], set()

    def add(self, name, expr, state="ENABLED", group="default", fn="x", **extra):
        self.items[(group, name)] = dict({
            "Name": name, "GroupName": group, "ScheduleExpression": expr, "State": state,
            "FlexibleTimeWindow": {"Mode": "OFF"}, "Description": "synthetic description",
            "ScheduleExpressionTimezone": "UTC",
            "Target": {"Arn": lam(fn), "RoleArn": "arn:aws:iam::%s:role/r" % ACCT, "Input": json.dumps({"secret": "SYNTHETIC-SECRET"}),
                       "RetryPolicy": {"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 3600}}}, **extra)

    def get_paginator(self, op):
        if op == "list_schedule_groups":
            class G:
                def paginate(s):
                    return iter([{"ScheduleGroups": [{"Name": g} for g in sorted({g for g, _ in self.items} | {"default"})]}])
            return G()
        if op == "list_schedules":
            class L:
                def paginate(s, GroupName="default"):
                    return iter([{"Schedules": [{"Name": n, "GroupName": g} for (g, n) in sorted(self.items) if g == GroupName]}])
            return L()
        raise AssertionError(op)

    def get_schedule(self, Name, GroupName="default"):
        if (GroupName, Name) not in self.items:
            raise err("ResourceNotFoundException")
        return dict(copy.deepcopy(self.items[(GroupName, Name)]), ResponseMetadata={}, Arn="arn:x", CreationDate="then")

    def update_schedule(self, **kw):
        self.calls.append(("update_schedule", kw["Name"]))
        key = (kw["GroupName"], kw["Name"])
        if kw["Name"] in self.ignore:
            return {}
        self.items[key] = copy.deepcopy(kw)       # full replacement
        return {}


class FakeS3:
    def __init__(self):
        self.objects, self.order = {}, []

    def put_object(self, Bucket, Key, Body, **kw):
        self.objects[Key] = json.loads(Body)
        self.order.append(Key)


def store():
    s3 = FakeS3()
    return ca.Store(s3, "private-bucket", "backups/cadence-test/"), s3


def action(kind, name, op, before, after, **extra):
    a = {"kind": kind, "name": name, "op": op, "before": {"expr": before[0], "state": before[1]},
         "after": {"expr": after[0], "state": after[1]}, "functions": ["fn"]}
    if kind == "rule":
        a["bus"] = "default"
    else:
        a["group"] = "default"
    a.update(extra)
    return a


# --------------------------------------------------------------------------------------------------- tests --
def test_snapshot_lists_scheduled_rules_and_schedules_and_hides_payloads():
    ev, sc = FakeEvents(), FakeScheduler()
    ev.add("fast", "rate(5 minutes)", targets=[{"Id": "1", "Arn": lam("fn-a"), "Input": '{"token":"SYNTHETIC-SECRET","x":1}'}])
    ev.add("two", "cron(0 12 * * ? *)", targets=[{"Id": "1", "Arn": lam("fn-a")}, {"Id": "2", "Arn": "arn:aws:sqs:us-east-1:%s:q" % ACCT}])
    ev.add("pattern-only", None, targets=[])               # an event-pattern rule is not a clock
    ev.add("empty", "rate(1 hour)", targets=[])
    ev.add("custom", "rate(10 minutes)", bus="justhodl-system-events", targets=[{"Id": "1", "Arn": lam("fn-b")}])
    sc.add("s1", "rate(1 hour)", fn="fn-c")
    sc.add("s2", "cron(5 5 * * ? *)", group="other", fn="fn-d", State="DISABLED")
    snap = ca.snapshot_triggers(ev, sc, log=lambda *_: None)
    names = {(r["kind"], r["name"], r.get("fn")) for r in snap}
    assert ("rule", "fast", "fn-a") in names and ("rule", "two", "fn-a") in names and ("rule", "two", None) in names
    assert ("rule", "empty", None) in names and ("rule", "custom", "fn-b") in names
    assert ("sched", "s1", "fn-c") in names and ("sched", "s2", "fn-d") in names
    assert not any(r["name"] == "pattern-only" for r in snap)
    assert "SYNTHETIC-SECRET" not in json.dumps(snap), "payloads must never reach the snapshot"
    fast = next(r for r in snap if r["name"] == "fast")
    assert fast["ikey"].startswith("sha:") and fast["input_sha"]
    assert next(r for r in snap if r["name"] == "two" and r["fn"] == "fn-a")["ikey"] == "{}"
    assert {r["bus"] for r in snap if r["kind"] == "rule"} == {"default", "justhodl-system-events"}


def test_retime_rule_backs_up_first_preserves_fields_and_reads_back():
    ev, sc = FakeEvents(), FakeScheduler()
    ev.add("fast", "rate(5 minutes)", Description="keep me", RoleArn="arn:aws:iam::%s:role/rule-role" % ACCT,
           targets=[{"Id": "1", "Arn": lam("fn-a"), "Input": "{}"}])
    st, s3 = store()
    a = action("rule", "fast", "retime", ("rate(5 minutes)", "ENABLED"), ("cron(7 3 * * ? *)", "ENABLED"))
    res = ca.apply_actions([a], ev, sc, st, dry_run=False, log=lambda *_: None)
    assert res[0]["status"] == "applied", res
    live = ev.rules[("default", "fast")]
    assert live["ScheduleExpression"] == "cron(7 3 * * ? *)" and live["State"] == "ENABLED"
    assert live["Description"] == "keep me" and live["RoleArn"].endswith("rule-role"), "put_rule is a full replacement"
    saved = s3.objects["backups/cadence-test/rule/default/fast.json"]
    assert saved["rule"]["ScheduleExpression"] == "rate(5 minutes)" and saved["targets"][0]["Id"] == "1"
    assert ev.targets[("default", "fast")][0]["Arn"] == lam("fn-a"), "targets untouched"


def test_disable_gives_the_twin_the_compliant_expression_and_disables_it():
    ev, sc = FakeEvents(), FakeScheduler()
    ev.add("twin", "rate(1 hour)", Description="d", targets=[{"Id": "1", "Arn": lam("fn-a")}])
    st, _ = store()
    a = action("rule", "twin", "disable", ("rate(1 hour)", "ENABLED"), ("cron(9 4 * * ? *)", "DISABLED"))
    res = ca.apply_actions([a], ev, sc, st, dry_run=False, log=lambda *_: None)
    assert res[0]["status"] == "applied"
    live = ev.rules[("default", "twin")]
    assert (live["ScheduleExpression"], live["State"], live["Description"]) == ("cron(9 4 * * ? *)", "DISABLED", "d")


def test_drift_missing_already_and_managed_are_left_alone():
    ev, sc = FakeEvents(), FakeScheduler()
    ev.add("changed", "rate(30 minutes)", targets=[])               # someone changed it since the snapshot
    ev.add("done", "cron(7 3 * * ? *)", targets=[])                  # already at the planned value
    ev.add("managed", "rate(5 minutes)", targets=[], ManagedBy="events.amazonaws.com")
    st, s3 = store()
    acts = [action("rule", "changed", "retime", ("rate(5 minutes)", "ENABLED"), ("cron(7 3 * * ? *)", "ENABLED")),
            action("rule", "done", "retime", ("rate(5 minutes)", "ENABLED"), ("cron(7 3 * * ? *)", "ENABLED")),
            action("rule", "gone", "retime", ("rate(5 minutes)", "ENABLED"), ("cron(7 3 * * ? *)", "ENABLED")),
            action("rule", "managed", "retime", ("rate(5 minutes)", "ENABLED"), ("cron(7 3 * * ? *)", "ENABLED"))]
    res = {r["name"]: r["status"] for r in ca.apply_actions(acts, ev, sc, st, dry_run=False, log=lambda *_: None)}
    assert res == {"changed": "drift", "done": "already", "gone": "missing", "managed": "skipped_managed"}
    assert ev.calls == [] and s3.objects == {}, "nothing was written"
    assert ev.rules[("default", "changed")]["ScheduleExpression"] == "rate(30 minutes)"


def test_dry_run_writes_nothing_and_says_what_it_would_do():
    ev, sc = FakeEvents(), FakeScheduler()
    ev.add("fast", "rate(5 minutes)", targets=[{"Id": "1", "Arn": lam("fn-a")}])
    sc.add("sfast", "rate(1 hour)")
    st, s3 = store()
    acts = [action("rule", "fast", "retime", ("rate(5 minutes)", "ENABLED"), ("cron(7 3 * * ? *)", "ENABLED")),
            action("sched", "sfast", "retime", ("rate(1 hour)", "ENABLED"), ("cron(1 2 * * ? *)", "ENABLED"))]
    res = ca.apply_actions(acts, ev, sc, st, dry_run=True, log=lambda *_: None)
    assert [r["status"] for r in res] == ["would_apply", "would_apply"]
    assert ev.calls == [] and sc.calls == [] and s3.objects == {}


def test_retimes_run_before_disables():
    ev, sc = FakeEvents(), FakeScheduler()
    ev.add("twin", "rate(1 hour)", targets=[{"Id": "1", "Arn": lam("fn-a")}])
    ev.add("keeper", "rate(5 minutes)", targets=[{"Id": "1", "Arn": lam("fn-a")}])
    st, _ = store()
    acts = [action("rule", "twin", "disable", ("rate(1 hour)", "ENABLED"), ("cron(9 4 * * ? *)", "DISABLED")),
            action("rule", "keeper", "retime", ("rate(5 minutes)", "ENABLED"), ("cron(7 3 * * ? *)", "ENABLED"))]
    ca.apply_actions(acts, ev, sc, st, dry_run=False, workers=1, log=lambda *_: None)
    assert [c[1] for c in ev.calls if c[0] == "put_rule"] == ["keeper", "twin"]


def test_schedule_update_is_a_full_payload_and_never_prints_or_loses_the_target():
    ev, sc = FakeEvents(), FakeScheduler()
    sc.add("s", "rate(15 minutes)", fn="fn-a", EndDate="2030-01-01", ActionAfterCompletion="NONE")
    before = copy.deepcopy(sc.items[("default", "s")])
    st, s3 = store()
    a = action("sched", "s", "retime", ("rate(15 minutes)", "ENABLED"), ("cron(3 5 * * ? *)", "ENABLED"))
    res = ca.apply_actions([a], ev, sc, st, dry_run=False, log=lambda *_: None)
    assert res[0]["status"] == "applied", res
    live = sc.items[("default", "s")]
    assert live["ScheduleExpression"] == "cron(3 5 * * ? *)"
    for k in ("Target", "FlexibleTimeWindow", "Description", "ScheduleExpressionTimezone", "EndDate", "ActionAfterCompletion"):
        assert live[k] == before[k], k
    assert "backups/cadence-test/schedule/default/s.json" in s3.objects
    assert "SYNTHETIC-SECRET" not in json.dumps(res), "the result never carries a payload"


def test_schedule_disable_and_drift():
    ev, sc = FakeEvents(), FakeScheduler()
    sc.add("twin", "rate(1 hour)")
    sc.add("moved", "rate(2 hours)")
    st, _ = store()
    acts = [action("sched", "twin", "disable", ("rate(1 hour)", "ENABLED"), ("cron(4 4 * * ? *)", "DISABLED")),
            action("sched", "moved", "retime", ("rate(1 hour)", "ENABLED"), ("cron(4 4 * * ? *)", "ENABLED")),
            action("sched", "nothere", "retime", ("rate(1 hour)", "ENABLED"), ("cron(4 4 * * ? *)", "ENABLED"))]
    res = {r["name"]: r["status"] for r in ca.apply_actions(acts, ev, sc, st, dry_run=False, log=lambda *_: None)}
    assert res == {"twin": "applied", "moved": "drift", "nothere": "missing"}
    assert sc.items[("default", "twin")]["State"] == "DISABLED"
    assert sc.items[("default", "twin")]["ScheduleExpression"] == "cron(4 4 * * ? *)"


def test_one_failure_does_not_stop_the_rest_and_a_silent_noop_is_caught_by_the_read_back():
    ev, sc = FakeEvents(), FakeScheduler()
    for n in ("a", "b", "c"):
        ev.add(n, "rate(5 minutes)", targets=[])
    ev.fail_put.add("a")
    ev.ignore_put.add("b")                                            # the API "succeeds" but nothing changed
    st, _ = store()
    acts = [action("rule", n, "retime", ("rate(5 minutes)", "ENABLED"), ("cron(7 3 * * ? *)", "ENABLED")) for n in "abc"]
    res = {r["name"]: r["status"] for r in ca.apply_actions(acts, ev, sc, st, dry_run=False, workers=1, log=lambda *_: None)}
    assert res == {"a": "failed", "b": "failed", "c": "applied"}


def test_shared_rule_drops_only_the_listed_function_and_never_the_last_target():
    ev, sc = FakeEvents(), FakeScheduler()
    ev.add("shared", "rate(1 hour)", targets=[{"Id": "1", "Arn": lam("keep-me")}, {"Id": "2", "Arn": lam("drop-me")}])
    ev.add("sole", "rate(1 hour)", targets=[{"Id": "1", "Arn": lam("drop-me")}])
    st, s3 = store()
    acts = [action("rule", "shared", "retime", ("rate(1 hour)", "ENABLED"), ("cron(7 3 * * ? *)", "ENABLED"), remove_functions=["drop-me"]),
            action("rule", "sole", "retime", ("rate(1 hour)", "ENABLED"), ("cron(7 3 * * ? *)", "ENABLED"), remove_functions=["drop-me"])]
    res = {r["name"]: r for r in ca.apply_actions(acts, ev, sc, st, dry_run=False, log=lambda *_: None)}
    assert res["shared"]["targets_removed"] == 1
    assert res["sole"]["targets_removed"] == 0, "a rule is never stripped of its last target"
    assert [t["Id"] for t in ev.targets[("default", "shared")]] == ["1"]
    assert [t["Id"] for t in ev.targets[("default", "sole")]] == ["1"]
    assert "backups/cadence-test/rule-targets/default/shared.json" in s3.objects


def test_canary_failure_aborts_before_anything_else_is_touched():
    ev, sc = FakeEvents(), FakeScheduler()
    for n in ("a", "b", "c"):
        ev.add(n, "rate(5 minutes)", targets=[])
    ev.fail_put.add("a")
    st, _ = store()
    acts = [action("rule", n, "retime", ("rate(5 minutes)", "ENABLED"), ("cron(7 3 * * ? *)", "ENABLED")) for n in "abc"]
    res = ca.apply_actions(acts, ev, sc, st, dry_run=False, canary=True, log=lambda *_: None)
    assert [r["status"] for r in res] == ["failed"] and res[0].get("aborted") is True
    assert [c[1] for c in ev.calls] == ["a"], "only the canary was attempted"


def test_violations_reports_enabled_noncompliant_triggers_only():
    snap = [
        {"kind": "rule", "name": "fast", "state": "ENABLED", "expr": "rate(5 minutes)", "fn": "a"},
        {"kind": "rule", "name": "fast", "state": "ENABLED", "expr": "rate(5 minutes)", "fn": "b"},     # same trigger, 2nd target
        {"kind": "rule", "name": "off", "state": "DISABLED", "expr": "rate(5 minutes)", "fn": "a"},
        {"kind": "rule", "name": "daily", "state": "ENABLED", "expr": "cron(0 1 * * ? *)", "fn": "a"},
        {"kind": "sched", "name": "justhodl-spend-governor-tick", "state": "ENABLED", "expr": "cron(7 * * * ? *)", "fn": "g"},
        {"kind": "sched", "name": "weird", "state": "ENABLED", "expr": "cron(nonsense)", "fn": None},
        {"kind": "rule", "name": "sqs-target", "state": "ENABLED", "expr": "rate(1 minute)", "fn": None},
    ]
    exempt = [{"kind": "sched", "name": "justhodl-spend-governor-tick"}]
    out = ca.violations(snap, exempt)
    assert sorted(v["name"] for v in out) == ["fast", "sqs-target", "weird"]
    assert next(v for v in out if v["name"] == "weird")["why"] == "unparseable expression"


if __name__ == "__main__":
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    for t in tests:
        t()
    print("cadence_apply tests passed:", len(tests))
