"""Dependency-free tests for justhodl-spend-governor (deploy gate). Never contacts AWS.

Pure decision logic (governor_core) plus full handler flows against in-memory fake AWS clients:
freeze -> exact restore, a human change made during a freeze wins, month rollover, the runaway fuse,
monitor mode, release on a cap raise, the canary selftest, and the Budgets SNS trigger.
"""
import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "source"))
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ["AWS_LAMBDA_FUNCTION_NAME"] = "justhodl-spend-governor"

try:  # real botocore on the deploy runner; stubs anywhere else
    from botocore.exceptions import ClientError  # noqa: F401
except Exception:  # noqa: BLE001
    class ClientError(Exception):
        def __init__(self, response, op):
            super().__init__(op)
            self.response = response
    exc = types.ModuleType("botocore.exceptions")
    exc.ClientError = ClientError
    cfgm = types.ModuleType("botocore.config")
    cfgm.Config = lambda **k: None
    sys.modules.update({"botocore": types.ModuleType("botocore"), "botocore.exceptions": exc,
                        "botocore.config": cfgm, "boto3": types.SimpleNamespace(client=lambda *a, **k: None)})
from botocore.exceptions import ClientError  # noqa: E402

import governor_core as core  # noqa: E402
import governor_levers as lv  # noqa: E402
import lambda_function as gov  # noqa: E402

FAILS = []


def check(cond, msg):
    print(("ok   " if cond else "FAIL ") + msg)
    if not cond:
        FAILS.append(msg)


def cerr(code):
    return ClientError({"Error": {"Code": code, "Message": code}}, "op")


# ------------------------------------------------------------------ fakes --
class Pager:
    def __init__(self, fn):
        self.fn = fn

    def paginate(self, **kw):
        yield self.fn(**kw)


class FakeLambda:
    def __init__(self, fns):
        self.fns = {n: dict(v) for n, v in fns.items()}
        self.puts, self.deletes = [], []

    def get_paginator(self, name):
        if name == "list_functions":
            return Pager(lambda **k: {"Functions": [{"FunctionName": n, "MemorySize": v.get("mem", 512),
                                                     "Architectures": ["x86_64"]} for n, v in self.fns.items()]})
        if name == "list_event_source_mappings":
            return Pager(lambda **k: {"EventSourceMappings": []})
        raise AssertionError(name)

    def get_function_concurrency(self, FunctionName):
        if FunctionName not in self.fns:
            raise cerr("ResourceNotFoundException")
        r = self.fns[FunctionName].get("reserved")
        return {} if r is None else {"ReservedConcurrentExecutions": r}

    def put_function_concurrency(self, FunctionName, ReservedConcurrentExecutions):
        self.puts.append((FunctionName, ReservedConcurrentExecutions))
        self.fns[FunctionName]["reserved"] = ReservedConcurrentExecutions

    def delete_function_concurrency(self, FunctionName):
        self.deletes.append(FunctionName)
        self.fns[FunctionName]["reserved"] = None

    def invoke(self, FunctionName, **kw):
        if self.fns[FunctionName].get("reserved") == 0:
            raise cerr("TooManyRequestsException")
        return {"StatusCode": 200}


class FakeBudgets:
    def __init__(self, limit, actual):
        self.limit, self.actual = limit, actual

    def describe_budget(self, AccountId, BudgetName):
        return {"Budget": {"BudgetLimit": {"Amount": str(self.limit)},
                           "CalculatedSpend": {"ActualSpend": {"Amount": str(self.actual)},
                                               "ForecastedSpend": {"Amount": str(self.actual * 1.5)}}}}


class FakeCE:
    def __init__(self, daily_total=20.0):
        self.calls = 0
        self.daily_total = daily_total
        self.usage = {}   # {day: {usage_type: usd}} extra grouped rows (v1.2.0 egress test)

    def get_cost_and_usage(self, TimePeriod, Granularity, Metrics, GroupBy=None, **kw):
        self.calls += 1
        start = datetime.fromisoformat(TimePeriod["Start"]).date()
        end = datetime.fromisoformat(TimePeriod["End"]).date()
        res, d = [], start
        while d < end:
            item = {"TimePeriod": {"Start": d.isoformat(), "End": (d + timedelta(days=1)).isoformat()},
                    "Total": {"UnblendedCost": {"Amount": str(self.daily_total)}}, "Groups": []}
            if GroupBy:
                item["Groups"] = [{"Keys": ["TimedStorage-ByteHrs"], "Metrics": {"UnblendedCost": {"Amount": "1.5"}}},
                                  {"Keys": ["Lambda-GB-Second"], "Metrics": {"UnblendedCost": {"Amount": "9"}}}]
                item["Groups"] += [{"Keys": [k], "Metrics": {"UnblendedCost": {"Amount": str(v)}}}
                                   for k, v in self.usage.get(d.isoformat(), {}).items()]
            res.append(item)
            d += timedelta(days=1)
        return {"ResultsByTime": res}


class FakeCW:
    def __init__(self, alarm_state="OK", durations=None, ended=(), burst=()):
        self.alarm_state = alarm_state
        self.durations = durations or {}
        self.ended = set(ended)  # burned in the past hour but no invocations in the last minutes
        self.burst = set(burst)  # the whole window's burn landed in its first bucket (a burst that ended)
        self.deleted = set()     # alarms someone removed
        self.metrics = []

    def describe_alarms(self, AlarmNames):
        return {"MetricAlarms": [{"AlarmName": n, "StateValue": self.alarm_state, "ActionsEnabled": True,
                                  "AlarmActions": ["arn:aws:sns:us-east-1:1:justhodl-spend-governor"]}
                                 for n in AlarmNames if n not in self.deleted]}

    def get_metric_data(self, MetricDataQueries, StartTime, EndTime, **kw):
        out = []
        for q in MetricDataQueries:
            m = q["MetricStat"]["Metric"]
            fn = m["Dimensions"][0]["Value"]
            per_h = self.durations.get(fn, 0.0)
            hours = (EndTime - StartTime).total_seconds() / 3600.0
            v = per_h * hours if m["MetricName"] == "Duration" else (10.0 * hours if per_h else 0.0)
            if m["MetricName"] == "Invocations" and hours < 0.5 and fn in self.ended:
                v = 0.0
            period = q["MetricStat"].get("Period") or int(hours * 3600)
            n = max(1, int(round(hours * 3600 / period)))
            vals = [v / n] * n
            if fn in self.burst and n > 1:
                vals = [v] + [0.0] * (n - 1)
            out.append({"Id": q["Id"], "Values": vals,
                        "Timestamps": [StartTime + timedelta(seconds=period * k) for k in range(n)]})
        return {"MetricDataResults": out}

    def put_metric_data(self, **kw):
        self.metrics.append(kw)


class FakeS3:
    def __init__(self):
        self.objects = {}

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise cerr("NoSuchKey")
        return {"Body": types.SimpleNamespace(read=lambda: self.objects[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.objects[Key] = Body


class FakeSNS:
    def __init__(self):
        self.sent = []

    def publish(self, **kw):
        self.sent.append(kw)


class Empty:
    """Every other service: nothing running."""
    def get_paginator(self, name):
        return Pager(lambda **k: {"Endpoints": [], "Reservations": []})

    def __getattr__(self, name):
        if name.startswith(("list_training", "list_processing", "list_transform", "list_hyper")):
            return lambda **k: {"TrainingJobSummaries": [], "ProcessingJobSummaries": [], "TransformJobSummaries": [],
                                "HyperParameterTuningJobSummaries": []}
        return lambda **k: {"NotebookInstances": [], "Apps": [], "DBInstances": [], "DBClusters": [],
                            "ServiceSummaryList": [], "clusterArns": [], "ScalableTargets": []}


class Context:
    def get_remaining_time_in_millis(self):
        return 600000


def world(fns, limit=500, actual=100.0, alarm="OK", durations=None, daily=20.0, ended=(), burst=()):
    w = {"lambda": FakeLambda(fns), "budgets": FakeBudgets(limit, actual), "ce": FakeCE(daily),
         "cloudwatch": FakeCW(alarm, durations, ended, burst), "s3": FakeS3(), "sns": FakeSNS()}
    w["lambda-probe"] = w["lambda"]
    gov._clients.clear()
    gov.client = lambda name: w.get(name) or w.setdefault(name, Empty())
    return w


def set_control(w, **kw):
    c = {"mode": "enforce"}
    c.update(kw)
    w["s3"].put_object(Bucket="b", Key=gov.PREFIX + "control.json", Body=json.dumps(c).encode())


def state_of(w):
    return json.loads(w["s3"].objects[gov.PREFIX + "state.json"])


FLEET = {"justhodl-spend-governor": {"reserved": 1}, "justhodl-guardrail-notify": {},
         "justhodl-alpha": {"reserved": None}, "justhodl-beta": {"reserved": 1}, "justhodl-gamma": {"reserved": 5},
         "justhodl-quarantined": {"reserved": 0}, "justhodl-spend-governor-canary": {}}


# -------------------------------------------------------------- core tests --
def test_thresholds_mid_month():
    now = datetime(2026, 11, 16, 12, 0, tzinfo=timezone.utc)
    th = core.thresholds(500, 300, now, 20, 5, core.merged_cfg())
    check(abs(th["days_left"] - 14.5) < 0.01, "days_left 14.5 on Nov 16 noon")
    check(abs(th["freeze_at"] - 397.5) < 0.01, "freeze line = 500 - (5*14.5 + 20 + 10) = 397.5")
    check(abs(th["brake_at"] - 357.5) < 0.01, "brake line 40 below freeze")
    check(not th["cap_at_risk_even_frozen"], "freezing now keeps the month under the cap")


def test_thresholds_floor_and_risk():
    now = datetime(2026, 11, 2, 0, 0, tzinfo=timezone.utc)
    th = core.thresholds(500, 50, now, 20, 30, core.merged_cfg())
    check(th["freeze_at"] == 250 and th["freeze_line_floored"], "freeze line floored at 50% of cap")
    check(th["cap_at_risk_even_frozen"], "residual alone overruns the cap -> flagged")


def test_decide():
    cfg = core.merged_cfg()
    th = {"cap": 500, "freeze_at": 400, "brake_at": 360}
    check(core.decide("NORMAL", 365, th, None, cfg)[0] == "BRAKE", "NORMAL -> BRAKE at the brake line")
    check(core.decide("NORMAL", 401, th, None, cfg)[0] == "FREEZE", "NORMAL -> FREEZE straight past both lines")
    check(core.decide("FREEZE", 300, th, 500, cfg)[0] == "FREEZE", "lines drifting up never release a freeze")
    th2 = {"cap": 800, "freeze_at": 700, "brake_at": 650}
    check(core.decide("FREEZE", 450, th2, 500, cfg)[0] == "NORMAL", "cap raise releases when MTD clears the lines")
    check(core.decide("FREEZE", 695, th2, 500, cfg)[0] == "FREEZE", "cap raise inside hysteresis holds")
    check(core.decide("FREEZE", 450, th, 500, cfg, suppressed=True)[0] == "NORMAL", "operator suppression wins")


def test_rank_and_runaway():
    cfg = core.merged_cfg()
    picks = core.rank_brake_targets({"a": 10, "b": 5, "c": 3, "d": 1, "e": 0.01}, {"b"}, cfg, always=("x", "b"))
    check(picks == ["a", "c", "x"], "brake picks top spenders to 80%%, skips exempt, adds always-list (%s)" % picks)
    run = core.runaway_functions({"loop": 300, "coord": 80, "ok": 1}, 20, {"coord"}, cfg)
    check(run == ["loop"], "fuse picks the runaway, never an exempt function")
    check(core.runaway_functions({"a": 25, "b": 1}, 20, set(), cfg) == [], "no fuse while the fleet is near normal")


def test_baseline_and_residual():
    today = datetime(2026, 10, 17).date()
    daily = {(today - timedelta(days=i)).isoformat(): (550.0 if i <= 4 else 20.0) for i in range(1, 20)}
    b, src = core.baseline_from_daily(daily, today, core.merged_cfg())
    check(b == 20.0 and src == "median14", "a 4-day spike cannot drag the baseline (median of 14)")
    usage = {(today - timedelta(days=i)).isoformat(): {"TimedStorage-ByteHrs": 80.0 if i <= 3 else 0.8,
                                                       "Lambda-GB-Second": 400.0} for i in range(1, 8)}
    r, _ = core.residual_from_usage(usage, today, core.merged_cfg())
    check(abs(r - 0.8) < 1e-9, "residual = min of persistent lines over 7 days, compute lines excluded")
    check(core.effective_cap(500, {"cap_overrides": {"2026-10": {"cap_usd": 3300}}},
                             datetime(2026, 10, 20, tzinfo=timezone.utc)) == (3300.0, "override:2026-10"), "month override")
    check(core.effective_cap(500, {"cap_overrides": {"2026-10": {"cap_usd": 3300}}},
                             datetime(2026, 11, 1, tzinfo=timezone.utc)) == (500.0, "budget"), "override ends with its month")
    check(abs(core.lambda_cost_per_day(3600000, 0, 1024, "x86_64", 1) - 1.440001) < 1e-3, "GB-s pricing math")


# ----------------------------------------------------------- handler flows --
def test_monitor_mode_never_mutates():
    w = world(FLEET, actual=480)
    set_control(w, mode="monitor")
    out = gov.lambda_handler({"action": "tick"}, Context())
    check(out["decision"] == "FREEZE" and out["stage"] == "NORMAL", "monitor mode decides FREEZE but stays NORMAL")
    check(not w["lambda"].puts, "monitor mode makes no Lambda changes")
    check(any("[monitor]" in m["Subject"] for m in w["sns"].sent), "monitor notice is tagged")
    n = len(w["sns"].sent)
    gov.lambda_handler({"action": "tick"}, Context())
    check(len(w["sns"].sent) == n, "monitor escalation notice is sent once a day, not every tick")


def test_freeze_and_exact_restore():
    w = world(FLEET, actual=480)
    set_control(w)
    out = gov.lambda_handler({"action": "tick"}, Context())
    st = state_of(w)
    fz = w["lambda"].fns
    check(out["stage"] == "FREEZE" and st["stage"] == "FREEZE", "enforce mode freezes past the freeze line")
    check(fz["justhodl-alpha"]["reserved"] == 0 and fz["justhodl-beta"]["reserved"] == 0 and fz["justhodl-gamma"]["reserved"] == 0,
          "every non-exempt function throttled to 0")
    check(fz["justhodl-spend-governor"]["reserved"] == 1 and fz["justhodl-guardrail-notify"].get("reserved") is None,
          "governor and exempt functions untouched")
    check("lambda:justhodl-quarantined" not in st["ledger"], "an already-throttled function is not claimed by the ledger")
    check(st["ledger"]["lambda:justhodl-gamma"]["before"] == {"reserved": 5}, "ledger keeps the BEFORE value")
    check(any("FREEZE" in m["Subject"] for m in w["sns"].sent), "freeze notification sent")
    fz["justhodl-beta"]["reserved"] = 7  # a human changed this one during the freeze
    res = gov.lambda_handler({"action": "restore", "suppress_hours": 24}, Context())
    check(fz["justhodl-alpha"]["reserved"] is None, "unset concurrency restored by delete")
    check(fz["justhodl-gamma"]["reserved"] == 5, "explicit concurrency restored to its old value")
    check(fz["justhodl-beta"]["reserved"] == 7, "a change made during the freeze wins over the restore")
    check(fz["justhodl-quarantined"]["reserved"] == 0, "the pre-existing quarantine is left alone")
    check(res["restored"].get("restored") == 3 and res["restored"].get("skipped_changed") == 1, "restore counts")
    out2 = gov.lambda_handler({"action": "tick"}, Context())
    check(out2["stage"] == "NORMAL" and "suppressed" in out2["reason"], "suppression holds after an operator release")


def test_month_rollover_restores_cap_but_not_fuse():
    w = world(FLEET, actual=480)
    set_control(w)
    gov.lambda_handler({"action": "tick"}, Context())
    st = state_of(w)
    st["month"] = "2026-01"
    st["ledger"]["lambda:justhodl-alpha"]["stage"] = "FUSE"
    w["s3"].put_object(Bucket="b", Key=gov.PREFIX + "state.json", Body=json.dumps(st).encode())
    w["budgets"].actual = 5.0
    out = gov.lambda_handler({"action": "tick"}, Context())
    fz = w["lambda"].fns
    check(out["rollover"].get("restored", 0) >= 2 and out["stage"] == "NORMAL", "new month lifts the freeze")
    check(fz["justhodl-gamma"]["reserved"] == 5, "frozen function restored on the 1st")
    check(fz["justhodl-alpha"]["reserved"] == 0, "runaway-fuse hold survives the month boundary")


def age_fuse(w, hours=1.0):
    """Pretend the previous fuse check (and every suspect) happened `hours` ago."""
    st = state_of(w)
    then = (gov.now_utc() - timedelta(hours=hours)).isoformat()
    st["fuse"]["checked_at"] = then
    st["fuse"]["suspects"] = {k: then for k in (st["fuse"].get("suspects") or {})}
    w["s3"].put_object(Bucket="b", Key=gov.PREFIX + "state.json", Body=json.dumps(st).encode())


def test_runaway_fuse_two_strikes():
    durations = {"justhodl-alpha": 900000000.0, "justhodl-gamma": 10000.0}  # ms per hour
    w = world(FLEET, actual=50, alarm="ALARM", durations=durations)
    set_control(w)
    out = gov.lambda_handler({"action": "tick"}, Context())
    check(out["fuse"].get("new") == [] and out["fuse"].get("suspects") == ["justhodl-alpha"],
          "strike one only marks the runaway as a suspect")
    check(w["lambda"].fns["justhodl-alpha"].get("reserved") is None, "nothing throttled on strike one")
    age_fuse(w)
    out = gov.lambda_handler({"action": "tick"}, Context())
    st = state_of(w)
    check(out["fuse"].get("new") == ["justhodl-alpha"], "strike two (0.8-3 h later) throttles the runaway")
    check(w["lambda"].fns["justhodl-alpha"]["reserved"] == 0 and st["ledger"]["lambda:justhodl-alpha"]["stage"] == "FUSE",
          "fuse entry recorded with stage FUSE")
    check(w["lambda"].fns["justhodl-gamma"]["reserved"] == 5 and out["stage"] == "NORMAL", "nothing else touched")
    gov.lambda_handler({"action": "restore"}, Context())
    check(w["lambda"].fns["justhodl-alpha"]["reserved"] == 0, "plain restore keeps fuse holds")
    r = gov.lambda_handler({"action": "restore", "include_fuse": True, "only_ids": ["justhodl-alpha"]}, Context())
    check(w["lambda"].fns["justhodl-alpha"]["reserved"] is None and r["stage"] == "NORMAL", "include_fuse + only_ids releases it")


def test_fuse_ignores_a_storm_that_already_ended():
    """ops 6523 regression: a 2 h window still held the pre-quarantine storm and re-throttled fixed engines."""
    durations = {"justhodl-alpha": 900000000.0}
    w = world(FLEET, actual=50, alarm="ALARM", durations=durations, ended=("justhodl-alpha",))
    set_control(w)
    gov.lambda_handler({"action": "fuse_check"}, Context())
    age_fuse(w)
    out = gov.lambda_handler({"action": "fuse_check"}, Context())
    check(out.get("new") == [] and not out.get("suspects"), "no suspect, no throttle once invocations stopped")
    check(w["lambda"].fns["justhodl-alpha"].get("reserved") is None, "fixed engine left running")


def test_partial_restore_keeps_the_freeze():
    w = world(FLEET, actual=480)
    set_control(w)
    gov.lambda_handler({"action": "tick"}, Context())
    r = gov.lambda_handler({"action": "restore", "only_ids": ["justhodl-gamma"]}, Context())
    check(w["lambda"].fns["justhodl-gamma"]["reserved"] == 5 and w["lambda"].fns["justhodl-alpha"]["reserved"] == 0,
          "only the named function is released")
    check(r["stage"] == "FREEZE", "a partial release does not pretend the freeze is over")


def test_drift_during_freeze_rethrottles_and_keeps_the_new_value():
    w = world(FLEET, actual=480)
    set_control(w)
    gov.lambda_handler({"action": "tick"}, Context())
    w["lambda"].fns["justhodl-gamma"]["reserved"] = 2  # an ops script changed it mid-freeze
    out = gov.lambda_handler({"action": "tick"}, Context())
    st = state_of(w)
    check(w["lambda"].fns["justhodl-gamma"]["reserved"] == 0, "drift is re-throttled on the next tick")
    check(st["ledger"]["lambda:justhodl-gamma"]["before"] == {"reserved": 2}, "the mid-freeze value is what gets restored")
    check(out["fuse"].get("reason", "").startswith("frozen"), "no fuse metric reads while frozen")
    gov.lambda_handler({"action": "restore"}, Context())
    check(w["lambda"].fns["justhodl-gamma"]["reserved"] == 2, "restore puts back the value set during the freeze")


def test_cap_raise_releases():
    w = world(FLEET, actual=480)
    set_control(w)
    gov.lambda_handler({"action": "tick"}, Context())
    w["budgets"].limit = 900
    out = gov.lambda_handler({"action": "tick"}, Context())
    check(out["stage"] == "NORMAL" and w["lambda"].fns["justhodl-gamma"]["reserved"] == 5,
          "raising the AWS Budget amount releases the freeze on the next tick")


def test_october_override_cap():
    w = world(FLEET, actual=2400)
    set_control(w, cap_overrides={gov.core.month_key(gov.now_utc()): {"cap_usd": 99999}})
    out = gov.lambda_handler({"action": "tick"}, Context())
    check(out["stage"] == "NORMAL" and out["thresholds"]["cap_source"].startswith("override"),
          "a per-month override cap is honoured")


def test_sns_trigger_and_selftest():
    w = world(FLEET, actual=10)
    set_control(w)
    out = gov.lambda_handler({"Records": [{"EventSource": "aws:sns", "Sns": {"Message": "AWS Budgets"}}]}, Context())
    check(out["trigger"] == "budget-sns", "Budgets SNS message runs a tick")
    st = gov.lambda_handler({"action": "selftest"}, Context())
    check(st["ok"] is True and w["lambda"].fns["justhodl-spend-governor-canary"].get("reserved") is None,
          "canary selftest round trip leaves the canary as it was")
    bad = gov.lambda_handler({"action": "selftest", "function": "justhodl-alpha"}, Context())
    check("error" in bad and w["lambda"].fns["justhodl-alpha"].get("reserved") is None, "selftest refuses real engines")


class FakeAAS:
    def __init__(self, refuse=None):
        self.registered, self.refuse = [], refuse

    def register_scalable_target(self, **kw):
        if self.refuse:
            raise ClientError({"Error": {"Code": "ValidationException", "Message": self.refuse}}, "RegisterScalableTarget")
        self.registered.append(kw)


class FakeSM:
    def __init__(self, running, status="InService", refuse=None):
        self.running, self.status, self.refuse, self.updates = running, status, refuse, []

    def describe_endpoint(self, EndpointName):
        return {"EndpointStatus": self.status, "ProductionVariants": [
            {"VariantName": "owned", "CurrentInstanceCount": self.running, "DesiredInstanceCount": self.running}]}

    def update_endpoint_weights_and_capacities(self, **kw):
        if self.refuse:
            raise ClientError({"Error": {"Code": "ValidationException", "Message": self.refuse}}, "UpdateEndpoint")
        self.updates.append(kw)


def smasync_entry(desired=1):
    return lv.entry("smasync", "endpoint/ep/variant/owned", "FREEZE",
                    {"endpoint": "ep", "variant": "owned", "min": 0, "max": 1, "suspended": lv.NO_SUSPEND,
                     "desired": desired})


def test_smasync_lever():
    def ctx_of(aas, sm):
        return types.SimpleNamespace(c=lambda name: {"application-autoscaling": aas, "sagemaker": sm}[name])
    aas, sm = FakeAAS(), FakeSM(running=0)
    e = smasync_entry(desired=1)  # snapshot said 1, the endpoint has since drained to 0
    lv.smasync_apply(ctx_of(aas, sm), e)
    check(aas.registered and aas.registered[0]["SuspendedState"]["DynamicScalingOutSuspended"] is True
          and aas.registered[0]["SuspendedState"]["DynamicScalingInSuspended"] is False,
          "smasync suspends scale-out and leaves scale-in live")
    check(not sm.updates, "no capacity update for an endpoint already at 0 (decided on current state, not snapshot)")
    aas, sm = FakeAAS(), FakeSM(running=1, refuse="Cannot update in-progress endpoint")
    e = smasync_entry()
    lv.smasync_apply(ctx_of(aas, sm), e)
    check(aas.registered and "in-progress" in e.get("scale_down_note", ""),
          "a refused scale-down is a note, the suspension still counts (%s)" % e.get("scale_down_note"))
    aas, sm = FakeAAS(), FakeSM(running=2)
    e = smasync_entry()
    lv.smasync_apply(ctx_of(aas, sm), e)
    check(sm.updates and sm.updates[0]["DesiredWeightsAndCapacities"][0]["DesiredInstanceCount"] == 0
          and e.get("scaled_down_from") == 2, "a running InService variant is dropped to 0 now")
    aas, sm = FakeAAS(), FakeSM(running=1, status="Updating")
    e = smasync_entry()
    lv.smasync_apply(ctx_of(aas, sm), e)
    check(not sm.updates and "Updating" in e.get("scale_down_note", ""), "no capacity update while Updating")
    # full apply path: a refused registration is recorded with the service's own message
    ctx = types.SimpleNamespace(c=lambda name: {"application-autoscaling": FakeAAS(refuse="why it failed"),
                                                "sagemaker": FakeSM(0)}[name],
                                time_left=lambda: 600, ledger={})
    e = smasync_entry()
    gov.apply_entries(ctx, [e], True, datetime(2026, 10, 17, tzinfo=timezone.utc))
    check(e["status"] == "failed" and e["error"] == "ValidationException" and e["error_msg"] == "why it failed",
          "failed apply keeps the error message in the ledger (%s)" % e.get("error_msg"))
    ctx.c = lambda name: {"application-autoscaling": FakeAAS(), "sagemaker": FakeSM(0)}[name]
    gov.apply_entries(ctx, [e], True, datetime(2026, 10, 17, 1, tzinfo=timezone.utc))
    check(e["status"] == "applied" and "error" not in e and "error_msg" not in e,
          "a later success clears the old error")


def alarm_record(name="justhodl-runaway-concurrency", state="ALARM"):
    msg = {"AlarmName": name, "NewStateValue": state, "NewStateReason": "Threshold Crossed: 3 datapoints"}
    return {"Records": [{"EventSource": "aws:sns", "Sns": {"Message": json.dumps(msg)}}]}


def test_fast_fuse_selection():
    cfg = core.merged_cfg()
    costs = {"loop": 400.0, "other": 30.0, "small": 1.0}
    b = {"loop": [390.0, 410.0, 400.0], "other": [90.0, 0.0, 0.0]}
    check(core.fast_fuse_targets(costs, b, set(), cfg) == ["loop"],
          "fast fuse picks the sustained burner, not a 5% sharer whose burn already stopped")
    check(core.fast_fuse_targets(costs, b, {"loop"}, cfg) == [], "an exempt function is never fast-fused")
    check(core.fast_fuse_targets({"burst": 300.0}, {"burst": [900.0, 0.0, 0.0]}, set(), cfg) == [],
          "a burst that ended inside the window (the 2026-10-10 654-copy fan-out) is ignored")
    check(core.fast_fuse_targets({"a": 15.0}, {"a": [15.0, 15.0, 15.0]}, set(), cfg) == [],
          "below the $20/day floor nothing is fused")
    many = {"f%d" % i: 100.0 for i in range(9)}
    check(len(core.fast_fuse_targets(many, {k: [100.0] * 3 for k in many}, set(), cfg)) == 5,
          "at most 5 functions per alarm")


def test_alarm_runs_the_fast_fuse():
    durations = {"justhodl-alpha": 900000000.0, "justhodl-gamma": 10000.0}  # ms per hour: ~$180/day vs pennies
    w = world(FLEET, actual=50, durations=durations)
    set_control(w)
    out = gov.lambda_handler(alarm_record(), Context())
    st = state_of(w)
    check(out.get("new") == ["justhodl-alpha"] and w["lambda"].fns["justhodl-alpha"]["reserved"] == 0,
          "a sustained-runaway alarm throttles the runaway in the same invocation (%s)" % out.get("new"))
    check(st["ledger"]["lambda:justhodl-alpha"]["stage"] == "FUSE", "fast-fuse hold is a FUSE entry (survives the 1st)")
    check(w["lambda"].fns["justhodl-gamma"]["reserved"] == 5, "fast fuse leaves every other function alone")
    check(any("STOPPED" in m["Subject"] for m in w["sns"].sent), "fast fuse sends a notification")
    again = gov.lambda_handler(alarm_record("justhodl-runaway-duration"), Context())
    check(again.get("checked") is False, "a second alarm within 5 minutes does not re-run the fast fuse")
    ok = gov.lambda_handler(alarm_record(state="OK"), Context())
    check(ok.get("action") == "none", "an OK transition does nothing")
    guard = gov.lambda_handler(alarm_record("justhodl-guard-lambda-concurrency"), Context())
    check(guard.get("action") == "none", "short-window guard alarms never trigger the fast fuse")
    r = gov.lambda_handler({"action": "restore", "include_fuse": True, "only_ids": ["justhodl-alpha"]}, Context())
    check(w["lambda"].fns["justhodl-alpha"]["reserved"] is None, "operator restore releases a fast-fuse hold")


def test_fast_fuse_ignores_a_burst_that_ended():
    w = world(FLEET, actual=50, durations={"justhodl-alpha": 900000000.0}, burst=("justhodl-alpha",))
    set_control(w)
    out = gov.lambda_handler(alarm_record(), Context())
    check(out.get("new") == [] and w["lambda"].fns["justhodl-alpha"].get("reserved") is None,
          "a function whose burn stopped inside the alarm window is not throttled")


def test_fast_fuse_monitor_mode_changes_nothing():
    w = world(FLEET, actual=50, durations={"justhodl-alpha": 900000000.0})
    set_control(w, mode="monitor")
    gov.lambda_handler(alarm_record(), Context())
    check(w["lambda"].fns["justhodl-alpha"].get("reserved") is None, "monitor mode reports, never throttles")


def test_budgets_sns_still_ticks():
    w = world(FLEET, actual=10)
    set_control(w)
    out = gov.lambda_handler({"Records": [{"EventSource": "aws:sns", "Sns": {"Message": "AWS Budget Notification"}}]},
                             Context())
    check(out.get("trigger") == "budget-sns", "a plain-text Budgets notice still runs a tick")


def test_guardrails_watch_themselves():
    w = world(FLEET, actual=10)
    set_control(w)
    out = gov.lambda_handler({"action": "tick"}, Context())
    check(out.get("guardrails", {}).get("problems") == [], "all runaway alarms present -> no problem reported")
    w["cloudwatch"].deleted.add("justhodl-runaway-concurrency")
    n = len(w["sns"].sent)
    out = gov.lambda_handler({"action": "tick"}, Context())
    check(out["guardrails"]["problems"] == ["justhodl-runaway-concurrency is missing"], "a deleted alarm is reported")
    check(any("guardrails degraded" in m["Subject"] for m in w["sns"].sent[n:]), "and e-mailed")
    m = len(w["sns"].sent)
    gov.lambda_handler({"action": "tick"}, Context())
    check(not any("guardrails degraded" in x["Subject"] for x in w["sns"].sent[m:]), "at most once a day")


def test_plan_is_read_only():
    w = world(FLEET, actual=10)
    set_control(w)
    p = gov.lambda_handler({"action": "plan", "stage": "FREEZE"}, Context())
    check(p["would_act_on"].get("lambda") == 4 and not w["lambda"].puts, "plan lists 4 Lambdas and changes nothing")
    check("justhodl-guardrail-notify" in p["exempt"], "plan reports the exempt set")


# ------------------------------------------------------------- v1.2.0 tests --
class FakeEvents:
    def __init__(self, rules):
        self.rules = {r["Name"]: dict(r) for r in rules}
        self.puts = []

    def list_event_buses(self, **kw):
        return {"EventBuses": [{"Name": "default"}]}

    def get_paginator(self, op):
        assert op == "list_rules", op
        return Pager(lambda **k: {"Rules": [dict(r) for r in self.rules.values()]})

    def describe_rule(self, Name, EventBusName="default"):
        return dict(self.rules[Name])

    def put_rule(self, **kw):
        self.puts.append(kw["Name"])
        self.rules[kw["Name"]] = dict(kw)
        return {}


class FakeScheduler:
    def __init__(self, scheds):
        self.items = {s["Name"]: dict(s, FlexibleTimeWindow={"Mode": "OFF"},
                                      Target={"Arn": "arn:aws:lambda:us-east-1:1:function:" + s.pop("fn"),
                                              "RoleArn": "r", "Input": "{\"token\":\"SYNTHETIC\"}"}) for s in scheds}
        self.updates = []

    def get_paginator(self, op):
        if op == "list_schedule_groups":
            return Pager(lambda **k: {"ScheduleGroups": [{"Name": "default"}]})
        return Pager(lambda **k: {"Schedules": [{"Name": n, "GroupName": "default", "State": v["State"]}
                                               for n, v in self.items.items()]})

    def get_schedule(self, Name, GroupName="default"):
        return dict(self.items[Name], GroupName=GroupName)

    def update_schedule(self, **kw):
        self.updates.append(kw["Name"])
        assert kw["Target"].get("Input") == "{\"token\":\"SYNTHETIC\"}", "payload must be passed back whole"
        self.items[kw["Name"]] = dict(kw)
        return {}


def cadence_world(**kw):
    w = world(FLEET, actual=10, **kw)
    w["events"] = FakeEvents([
        {"Name": "fast-5min", "ScheduleExpression": "rate(5 minutes)", "State": "ENABLED", "Description": "d"},
        {"Name": "off-fast", "ScheduleExpression": "rate(1 minute)", "State": "DISABLED"},
        {"Name": "daily", "ScheduleExpression": "cron(0 12 * * ? *)", "State": "ENABLED"},
        {"Name": "managed", "ScheduleExpression": "rate(1 minute)", "State": "ENABLED", "ManagedBy": "aws"}])
    w["scheduler"] = FakeScheduler([
        {"Name": "justhodl-spend-governor-tick", "ScheduleExpression": "cron(7 * * * ? *)", "State": "ENABLED", "fn": "justhodl-spend-governor"},
        {"Name": "hourly-sched", "ScheduleExpression": "cron(20 * * * ? *)", "State": "ENABLED", "fn": "justhodl-alpha"},
        {"Name": "allowed-fast", "ScheduleExpression": "rate(10 minutes)", "State": "ENABLED", "fn": "justhodl-beta"}])
    return w


def test_cadence_sentinel_retimes_fast_triggers_once_a_day_and_restores_on_request():
    w = cadence_world()
    set_control(w, cadence_exempt=["sched:allowed-fast"])
    out = gov.lambda_handler({"action": "tick"}, Context())
    c = out["cadence"]
    check(c["violators"] == 2 and c["fixed"] == 2 and not c["errors"], "sentinel: 2 violators (5-min rule, hourly schedule), both retimed")
    check(w["events"].puts == ["fast-5min"] and w["scheduler"].updates == ["hourly-sched"], "exactly those two were written")
    check(not core_violates(w["events"].rules["fast-5min"]["ScheduleExpression"]) and
          w["events"].rules["fast-5min"]["State"] == "ENABLED" and w["events"].rules["fast-5min"]["Description"] == "d",
          "rule retimed to a daily expression, still enabled, description kept")
    check(w["events"].rules["off-fast"]["ScheduleExpression"] == "rate(1 minute)", "a disabled rule is not a running engine: untouched")
    check(w["scheduler"].items["justhodl-spend-governor-tick"]["ScheduleExpression"] == "cron(7 * * * ? *)", "own tick exempt")
    check(w["scheduler"].items["allowed-fast"]["ScheduleExpression"] == "rate(10 minutes)", "control.json cadence_exempt honoured")
    check(any("cadence" in m["Subject"] for m in w["sns"].sent), "operator told")
    out2 = gov.lambda_handler({"action": "tick"}, Context())
    check("cadence" not in out2, "runs once a day")
    st = gov.lambda_handler({"action": "status"}, Context())
    check(st["cadence_fixed"] == 2 and st["cadence"]["violators"][0]["name"] in ("fast-5min", "hourly-sched"), "status shows it")
    r = gov.lambda_handler({"action": "cadence_restore", "names": ["hourly-sched"]}, Context())
    check(w["scheduler"].items["hourly-sched"]["ScheduleExpression"] == "cron(20 * * * ? *)" and r["remaining"] == 1,
          "cadence_restore puts the recorded expression back, the other fix stays recorded")


def test_cadence_sentinel_monitor_mode_reports_only():
    w = cadence_world()
    set_control(w, mode="monitor")
    out = gov.lambda_handler({"action": "tick"}, Context())
    check(out["cadence"]["violators"] == 3 and out["cadence"]["fixed"] == 0 and not w["events"].puts and not w["scheduler"].updates,
          "monitor mode: 3 reported (allowed-fast not exempt here), nothing written")
    set_control(w, cadence_enforce=False)
    w["s3"].objects.pop(gov.PREFIX + "state.json")
    out = gov.lambda_handler({"action": "tick"}, Context())
    check(out["cadence"]["fixed"] == 0 and not w["events"].puts, "cadence_enforce=false: enforce mode still only reports")


def core_violates(expr):
    import cadence_core
    return cadence_core.violates(expr)


def test_diffuse_storm_fuse():
    cfg = core.merged_cfg()
    exempt = {"justhodl-spend-governor"}
    calm = {"f%d" % i: 0.3 for i in range(30)}
    check(core.diffuse_targets(calm, 4.0, exempt, cfg) == [], "normal fleet: nothing")
    storm = {"f%d" % i: 4.0 for i in range(30)}          # $120/day spread over 30 functions, none >= $20
    storm["justhodl-spend-governor"] = 50.0
    picks = core.diffuse_targets(storm, 4.0, exempt, cfg)
    check(core.runaway_functions({k: v for k, v in storm.items() if k != "justhodl-spend-governor"}, 4.0, exempt, cfg) == [],
          "the single-function fuse is blind to it")
    check(len(picks) == 26 and "justhodl-spend-governor" not in picks, "diffuse fuse picks the hottest covering 60%% of the burn (%d)" % len(picks))
    picks = core.diffuse_targets({"a": 100.0, "b": 1.0}, 4.0, exempt, cfg)
    check(picks == ["a"], "a function under diffuse_min_fn_usd_day is never picked")
    check(core.diffuse_targets(storm, 60.0, exempt, cfg) == [], "a fleet that is normally this busy is not a storm")


def test_hourly_fuse_trips_on_a_diffuse_storm():
    fns = dict(FLEET)
    fns.update({"justhodl-d%d" % i: {"reserved": None} for i in range(12)})
    durations = {"justhodl-d%d" % i: 40000000.0 for i in range(12)}     # each ~$8/day at 512 MB: no single runaway, fleet ~$96/day
    w = world(fns, actual=50, alarm="ALARM", durations=durations)
    set_control(w)
    out = gov.lambda_handler({"action": "tick"}, Context())
    check(out["fuse"].get("new") == [] and len(out["fuse"].get("suspects") or []) >= 8, "strike one: the storm's functions become suspects")
    age_fuse(w)
    out = gov.lambda_handler({"action": "tick"}, Context())
    check(len(out["fuse"].get("new") or []) >= 8 and all(n.startswith("justhodl-d") for n in out["fuse"]["new"]),
          "strike two: the diffuse storm is throttled (%d functions)" % len(out["fuse"].get("new") or []))
    check(w["lambda"].fns["justhodl-gamma"]["reserved"] == 5, "a quiet function is untouched")
    check(any("diffuse" in m["Message"] for m in w["sns"].sent), "the alert says it was a diffuse trip")


def test_fast_fuse_trips_on_a_sustained_diffuse_storm():
    fns = dict(FLEET)
    fns.update({"justhodl-d%d" % i: {"reserved": None} for i in range(12)})
    durations = {"justhodl-d%d" % i: 40000000.0 for i in range(12)}
    w = world(fns, actual=50, alarm="ALARM", durations=durations)
    set_control(w)
    out = gov.lambda_handler(alarm_record(), Context())
    check(out.get("diffuse") is None and len(out.get("new") or []) >= 8, "alarm + no dominant function -> diffuse picks throttled")
    w2 = world(fns, actual=50, alarm="ALARM", durations=durations, burst=["justhodl-d%d" % i for i in range(12)])
    set_control(w2)
    out = gov.lambda_handler(alarm_record(), Context())
    check(out.get("new") == [], "a diffuse burst that already ended is not throttled")


def test_egress_alert_once_a_day():
    w = world(FLEET, actual=10)
    set_control(w)
    yday = (datetime.now(timezone.utc) - timedelta(days=1)).date().isoformat()
    w["ce"].usage = {yday: {"USE1-DataTransfer-Out-Bytes": 9.5, "TimedStorage-ByteHrs": 2.0}}
    out = gov.lambda_handler({"action": "tick"}, Context())
    check(out["egress"]["alert"] and out["egress"]["usd"] == 9.5, "yesterday's data-transfer-out above the line is flagged")
    n = sum(1 for m in w["sns"].sent if "egress" in m["Subject"])
    gov.lambda_handler({"action": "tick"}, Context())
    check(n == 1 and sum(1 for m in w["sns"].sent if "egress" in m["Subject"]) == 1, "one alert a day")
    check(core.egress_usd_by_day({"d": {"TimedStorage": 1.0}}) == {}, "storage is not egress")


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
            except Exception as e:  # noqa: BLE001
                import traceback
                traceback.print_exc()
                FAILS.append("%s raised %r" % (name, e))
    print("\n%d failure(s)" % len(FAILS))
    sys.exit(1 if FAILS else 0)
