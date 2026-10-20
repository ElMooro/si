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
# The suite runs at a pinned clock just after a month start so the v1.3.0 meter's first read (from the 1st) sees
# 30 minutes of the synthetic fleet, not three weeks of it: the fuse tests need their runaways under the $50 cap.
PINNED = datetime(2026, 10, 1, 0, 30, tzinfo=timezone.utc)
gov.now_utc = lambda: PINNED


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
        self.s3_usd = {}  # {day: usd} of the S3 service (v1.5.0 storage fuse)

    def get_cost_and_usage(self, TimePeriod, Granularity, Metrics, GroupBy=None, **kw):
        self.calls += 1
        start = datetime.fromisoformat(TimePeriod["Start"]).date()
        end = datetime.fromisoformat(TimePeriod["End"]).date()
        res, d = [], start
        while d < end:
            item = {"TimePeriod": {"Start": d.isoformat(), "End": (d + timedelta(days=1)).isoformat()},
                    "Total": {"UnblendedCost": {"Amount": str(self.daily_total)}}, "Groups": []}
            if GroupBy and GroupBy[0].get("Key") == "SERVICE":
                item["Groups"] = [{"Keys": ["Amazon Simple Storage Service"], "Metrics": {"UnblendedCost": {"Amount": str(self.s3_usd.get(d.isoformat(), 1.0))}}},
                                  {"Keys": ["AWS Lambda"], "Metrics": {"UnblendedCost": {"Amount": "4"}}}]
            elif GroupBy:
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
        self.timeline = []       # [(datetime, durations)] rate changes over time (v1.4.0 tests); earlier = self.durations
        self.errors = {}         # {fn: errors per hour}
        self.ended = set(ended)  # burned in the past hour but no invocations in the last minutes
        self.burst = set(burst)  # the whole window's burn landed in its first bucket (a burst that ended)
        self.deleted = set()     # alarms someone removed
        self.metrics = []
        self.storage = {}        # {day: {"StandardStorage": bytes, ..., "objects": n}} daily S3 metrics (v1.5.0)

    def get_paginator(self, name):
        assert name == "list_metrics", name
        types = sorted({st for d in self.storage.values() for st in d if st != "objects"})
        return Pager(lambda **k: {"Metrics": [{"MetricName": "BucketSizeBytes", "Dimensions": [
            {"Name": "BucketName", "Value": k["Dimensions"][0]["Value"]}, {"Name": "StorageType", "Value": st}]} for st in types]})

    def rate_at(self, t):
        d = self.durations
        for when, durations in self.timeline:
            if t >= when:
                d = durations
        return d

    def describe_alarms(self, AlarmNames):
        return {"MetricAlarms": [{"AlarmName": n, "StateValue": self.alarm_state, "ActionsEnabled": True,
                                  "AlarmActions": ["arn:aws:sns:us-east-1:1:justhodl-spend-governor"]}
                                 for n in AlarmNames if n not in self.deleted]}

    def get_metric_data(self, MetricDataQueries, StartTime, EndTime, **kw):
        out = []
        if MetricDataQueries and MetricDataQueries[0]["MetricStat"]["Metric"]["Namespace"] == "AWS/S3":
            for q in MetricDataQueries:
                m = q["MetricStat"]["Metric"]
                st = m["Dimensions"][1]["Value"]
                key = "objects" if m["MetricName"] == "NumberOfObjects" else st
                ts, vals = [], []
                for day in sorted(self.storage):
                    d = datetime.fromisoformat(day).replace(tzinfo=timezone.utc)
                    if StartTime <= d <= EndTime and key in self.storage[day]:
                        ts.append(d)
                        vals.append(float(self.storage[day][key]))
                out.append({"Id": q["Id"], "Timestamps": ts, "Values": vals})
            return {"MetricDataResults": out}
        for q in MetricDataQueries:
            m = q["MetricStat"]["Metric"]
            fn = m["Dimensions"][0]["Value"]
            hours = (EndTime - StartTime).total_seconds() / 3600.0
            period = q["MetricStat"].get("Period") or int(hours * 3600)
            n = max(1, int(round(hours * 3600 / period)))
            vals = []
            for k in range(n):
                t = StartTime + timedelta(seconds=period * k)
                per_h = self.rate_at(t).get(fn, 0.0)
                bh = period / 3600.0
                if m["MetricName"] == "Duration":
                    v = per_h * bh
                elif m["MetricName"] == "Errors":
                    v = self.errors.get(fn, 0.0) * bh if per_h else 0.0
                else:
                    v = 10.0 * bh if per_h else 0.0
                vals.append(v)
            if m["MetricName"] == "Invocations" and hours < 0.5 and fn in self.ended:
                vals = [0.0] * n
            if fn in self.burst and n > 1:
                vals = [sum(vals)] + [0.0] * (n - 1)
            out.append({"Id": q["Id"], "Values": vals,
                        "Timestamps": [StartTime + timedelta(seconds=period * k) for k in range(n)]})
        return {"MetricDataResults": out}

    def put_metric_data(self, **kw):
        self.metrics.append(kw)


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.versioning = "Enabled"
        self.versioning_puts = []

    def get_bucket_versioning(self, Bucket):
        return {"Status": self.versioning}

    def put_bucket_versioning(self, Bucket, VersioningConfiguration):
        self.versioning = VersioningConfiguration["Status"]
        self.versioning_puts.append((Bucket, self.versioning))

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
    yday = (gov.now_utc() - timedelta(days=1)).date().isoformat()
    w["ce"].usage = {yday: {"USE1-DataTransfer-Out-Bytes": 9.5, "TimedStorage-ByteHrs": 2.0}}
    out = gov.lambda_handler({"action": "tick"}, Context())
    check(out["egress"]["alert"] and out["egress"]["usd"] == 9.5, "yesterday's data-transfer-out above the line is flagged")
    n = sum(1 for m in w["sns"].sent if "egress" in m["Subject"])
    gov.lambda_handler({"action": "tick"}, Context())
    check(n == 1 and sum(1 for m in w["sns"].sent if "egress" in m["Subject"]) == 1, "one alert a day")
    check(core.egress_usd_by_day({"d": {"TimedStorage": 1.0}}) == {}, "storage is not egress")


# ------------------------------------------------------------- v1.3.0 tests --
def at(iso):
    """Pin the governor's clock."""
    t = datetime.fromisoformat(iso).replace(tzinfo=timezone.utc)
    gov.now_utc = lambda: t
    return t


def test_meter_accumulates_month_to_date_lambda_cost_and_reconciles_daily():
    durations = {"justhodl-alpha": 20000000.0, "justhodl-gamma": 1000.0}        # ms per hour at 512 MB
    w = world(FLEET, actual=10, durations=durations)
    set_control(w)
    at("2026-10-20T12:07:00")
    out = gov.lambda_handler({"action": "tick"}, Context())
    st = state_of(w)
    m = st["meter"]
    check(out["meter"]["ran"] and out["meter"]["reconciled"] and m["reads"] == 1, "first read of the month reconciles from the 1st")
    expected = core.lambda_cost_usd(20000000.0 * m["last_window_h"], 10.0 * m["last_window_h"], 512, "x86_64")
    check(abs(m["mtd"]["justhodl-alpha"] - expected) < 0.05 * expected, "month-to-date equals duration x memory x price + requests (%.2f vs %.2f)" % (m["mtd"]["justhodl-alpha"], expected))
    check(m["mtd"].get("justhodl-beta") is None, "a function that never ran has no row (Duration is only read for active functions)")
    at("2026-10-20T13:07:00")
    gov.lambda_handler({"action": "tick"}, Context())
    m2 = state_of(w)["meter"]
    check(m2["reads"] == 2 and not m2["last_reconcile"] and 0.9 <= m2["last_window_h"] <= 1.1, "an hour later: a one-hour incremental window")
    check(m2["mtd"]["justhodl-alpha"] > m["mtd"]["justhodl-alpha"], "the increment is added")
    at("2026-10-20T13:30:00")
    out = gov.lambda_handler({"action": "tick"}, Context())
    check(not out["meter"]["ran"] and state_of(w)["meter"]["reads"] == 2, "23 minutes later: no read (meter_interval_h)")
    check("fuse" in out and out["fuse"].get("checked") in (True, False), "the fuse path still runs")
    gov.now_utc = lambda: PINNED


def test_engine_cap_shuts_an_engine_down_and_only_release_cap_brings_it_back():
    durations = {"justhodl-alpha": 20000000.0}                                   # ~$78 month-to-date by Oct 20, ~$0.17 an hour
    w = world(FLEET, actual=10, durations=durations)
    set_control(w)
    at("2026-10-20T12:07:00")
    out = gov.lambda_handler({"action": "tick"}, Context())
    st = state_of(w)
    check(out["caps"]["new"] == ["justhodl-alpha"], "alpha reached the $50 cap: %s" % json.dumps(out["caps"]["breaches"]))
    check(w["lambda"].fns["justhodl-alpha"]["reserved"] == 0 and st["ledger"]["lambda:justhodl-alpha"]["stage"] == "CAP", "held at 0 under stage CAP")
    check(w["lambda"].fns["justhodl-gamma"]["reserved"] == 5, "nothing else touched")
    msg = [m for m in w["sns"].sent if "ENGINE SHUT DOWN" in m["Subject"]]
    check(len(msg) == 1 and "release_cap" in msg[0]["Message"] and "justhodl-alpha" in msg[0]["Message"], "Khalid is told, with the exact re-enable command")
    gov.lambda_handler({"action": "restore", "include_fuse": True}, Context())
    check(w["lambda"].fns["justhodl-alpha"]["reserved"] == 0, "a plain restore (even with include_fuse) does not lift the cap")
    w["budgets"].limit = 5000
    at("2026-10-20T13:07:00")
    out = gov.lambda_handler({"action": "tick"}, Context())
    check(w["lambda"].fns["justhodl-alpha"]["reserved"] == 0 and out["caps"]["new"] == [], "a budget raise does not lift it, and it is not re-reported")
    at("2026-11-01T00:07:00")
    out = gov.lambda_handler({"action": "tick"}, Context())
    st = state_of(w)
    check("rollover" in out and w["lambda"].fns["justhodl-alpha"]["reserved"] == 0 and st["ledger"]["lambda:justhodl-alpha"]["stage"] == "CAP",
          "the 1st of the month does not lift it")
    check(st["meter"]["month"] == "2026-11", "the meter starts a fresh month")
    w["lambda"].fns["justhodl-alpha"]["reserved"] = None                        # someone un-throttles it by hand
    at("2026-11-01T01:07:00")
    gov.lambda_handler({"action": "tick"}, Context())
    check(w["lambda"].fns["justhodl-alpha"]["reserved"] == 0, "a hand change is re-held on the next tick (NORMAL stage drift)")
    r = gov.lambda_handler({"action": "release_cap", "function": "justhodl-alpha", "confirm": "nope"}, Context())
    check("error" in r and w["lambda"].fns["justhodl-alpha"]["reserved"] == 0, "release needs confirm = function")
    r = gov.lambda_handler({"action": "release_cap", "function": "justhodl-alpha", "confirm": "justhodl-alpha", "reason": "fixed the loop"}, Context())
    st = state_of(w)
    check(r["status"] == "restored" and w["lambda"].fns["justhodl-alpha"]["reserved"] is None and "lambda:justhodl-alpha" not in st["ledger"],
          "release_cap restores the exact previous value and clears the ledger entry")
    check(st["caps"]["released"]["justhodl-alpha"]["reason"] == "fixed the loop", "the release is recorded with its reason")
    at("2026-11-01T02:07:00")
    out = gov.lambda_handler({"action": "tick"}, Context())
    check(out["caps"]["new"] == [] and w["lambda"].fns["justhodl-alpha"]["reserved"] is None, "after a release the cap counts from zero: a normal hour does not re-trip it")
    c = gov.lambda_handler({"action": "caps"}, Context())
    check(c["cap_usd_month"] == 50.0 and c["held"] == [] and "justhodl-alpha" in c["released"], "caps action reports")
    gov.now_utc = lambda: PINNED


def test_engine_cap_outranks_a_fuse_hold_and_per_function_overrides_apply():
    durations = {"justhodl-alpha": 20000000.0, "justhodl-gamma": 20000000.0}
    w = world(FLEET, actual=10, durations=durations)
    set_control(w, engine_caps={"justhodl-gamma": 500})
    at("2026-10-20T12:07:00")
    # alpha is already held by the fuse
    w["s3"].put_object(Bucket="b", Key=gov.PREFIX + "state.json", Body=json.dumps({
        "version": core.VERSION, "month": "2026-10", "stage": "NORMAL", "stage_since": "2026-10-01T00:00:00+00:00",
        "ledger": {"lambda:justhodl-alpha": {"key": "lambda:justhodl-alpha", "lever": "lambda", "id": "justhodl-alpha",
                                             "stage": "FUSE", "before": {"reserved": None}, "status": "applied"}},
        "released": [], "ce": {}, "alerts": {}, "observations": [], "fuse": {}}).encode())
    w["lambda"].fns["justhodl-alpha"]["reserved"] = 0
    out = gov.lambda_handler({"action": "tick"}, Context())
    st = state_of(w)
    check(out["caps"]["new"] == ["justhodl-alpha"] and st["ledger"]["lambda:justhodl-alpha"]["stage"] == "CAP"
          and st["ledger"]["lambda:justhodl-alpha"]["capped_from"] == "FUSE", "a fuse hold is upgraded to CAP")
    check(w["lambda"].fns["justhodl-gamma"]["reserved"] == 5, "gamma's own $500 cap is not reached")
    gov.lambda_handler({"action": "restore", "include_fuse": True}, Context())
    check(w["lambda"].fns["justhodl-alpha"]["reserved"] == 0, "include_fuse no longer releases it")
    gov.now_utc = lambda: PINNED


def test_engine_cap_monitor_mode_reports_once_and_changes_nothing():
    w = world(FLEET, actual=10, durations={"justhodl-alpha": 20000000.0})
    set_control(w, mode="monitor")
    at("2026-10-20T12:07:00")
    out = gov.lambda_handler({"action": "tick"}, Context())
    check(out["caps"]["new"] == ["justhodl-alpha"] and w["lambda"].fns["justhodl-alpha"].get("reserved") is None and not w["lambda"].puts,
          "monitor mode: reported, not held")
    n = len([m for m in w["sns"].sent if "engine cap" in m["Subject"] or "ENGINE" in m["Subject"]])
    at("2026-10-20T13:07:00")
    gov.lambda_handler({"action": "tick"}, Context())
    check(len([m for m in w["sns"].sent if "engine cap" in m["Subject"] or "ENGINE" in m["Subject"]]) == n == 1, "one notice a day")
    gov.now_utc = lambda: PINNED


def test_operator_hold_is_a_cap_until_release_cap():
    w = world(FLEET, actual=10)
    set_control(w)
    r = gov.lambda_handler({"action": "hold", "function": "justhodl-gamma", "confirm": "justhodl-gamma", "reason": "retired 2026-10-20"}, Context())
    st = state_of(w)
    check(r["reserved"] == 0 and st["ledger"]["lambda:justhodl-gamma"]["stage"] == "CAP" and st["ledger"]["lambda:justhodl-gamma"]["before"]["reserved"] == 5, "hold: held at 0 under CAP, previous value kept")
    gov.lambda_handler({"action": "tick"}, Context())
    gov.lambda_handler({"action": "restore", "include_fuse": True}, Context())
    check(w["lambda"].fns["justhodl-gamma"]["reserved"] == 0, "nothing automatic lifts an operator hold")
    r = gov.lambda_handler({"action": "release_cap", "function": "justhodl-gamma", "confirm": "justhodl-gamma"}, Context())
    check(r["status"] == "restored" and w["lambda"].fns["justhodl-gamma"]["reserved"] == 5, "release_cap restores the previous value")
    r = gov.lambda_handler({"action": "hold", "function": "justhodl-spend-governor", "confirm": "justhodl-spend-governor"}, Context())
    check("error" in r, "the governor cannot hold itself")


def test_cap_selftest_round_trips_the_canary():
    w = world(FLEET, actual=10)
    set_control(w)
    r = gov.lambda_handler({"action": "cap_selftest"}, Context())
    check(r["ok"] and r["held"] == 0 and r["ledger_stage"] == "CAP" and r["after"] == r["before"], "cap_selftest: held, released, restored (%s)" % json.dumps(r)[:200])
    st = state_of(w)
    check("justhodl-spend-governor-canary" not in (st.get("meter") or {}).get("mtd", {}) and "lambda:justhodl-spend-governor-canary" not in st["ledger"],
          "the self-test leaves no trace")
    r = gov.lambda_handler({"action": "cap_selftest", "function": "justhodl-alpha"}, Context())
    check("error" in r, "only the canary may be self-tested")


# ------------------------------------------------------------- v1.4.0 tests --
def ticks(w, start_iso, hours, step_h=1.0):
    """Run hourly ticks from start for `hours` hours; returns the last tick's output."""
    t0 = datetime.fromisoformat(start_iso).replace(tzinfo=timezone.utc)
    out = None
    for i in range(int(hours / step_h)):
        t = t0 + timedelta(hours=step_h * i)
        gov.now_utc = lambda t=t: t
        out = gov.lambda_handler({"action": "tick"}, Context())
    return out


def test_anomaly_sentinel_holds_a_storm_for_a_cooldown_then_releases_and_the_third_time_is_permanent():
    w = world(FLEET, actual=10, durations={"justhodl-alpha": 100000.0, "justhodl-gamma": 50000.0})   # ~$0.02/day each
    set_control(w)
    ticks(w, "2026-10-02T00:30:00", 72)                                   # three quiet days of baseline
    st = state_of(w)
    base = core.anomaly_baselines(st["meter"]["daily"], "2026-10-05", 14)
    check("justhodl-alpha" in base and base["justhodl-alpha"][2] >= 2 and base["justhodl-alpha"][1] < 0.05, "baseline built from the daily record: %s" % str(base.get("justhodl-alpha")))
    check(not st["ledger"] and not (st.get("anomalies") or {}).get("suspects"), "quiet days: nothing suspected")
    storm = lambda when, per_h: w["cloudwatch"].timeline.append((datetime.fromisoformat(when).replace(tzinfo=timezone.utc), {"justhodl-alpha": per_h, "justhodl-gamma": 50000.0}))
    storm("2026-10-04T23:30:00", 200000000.0)                                   # ~$1.67 an hour, 80x its normal day
    out = ticks(w, "2026-10-05T00:30:00", 1)
    check(out["anomalies"]["suspects"] == ["justhodl-alpha"] and w["lambda"].fns["justhodl-alpha"].get("reserved") is None, "first anomalous window: suspect only")
    out = ticks(w, "2026-10-05T01:30:00", 1)
    st = state_of(w)
    e = st["ledger"].get("lambda:justhodl-alpha") or {}
    check(out["anomalies"]["held"] == ["justhodl-alpha"] and w["lambda"].fns["justhodl-alpha"]["reserved"] == 0 and e.get("stage") == "ANOM",
          "second window in a row: held at 0 under ANOM")
    check(e.get("release_after") and e.get("hold_no") == 1, "cooldown recorded")
    check(any("anomaly" in m["Subject"] for m in w["sns"].sent) and "release_anomaly" in [m for m in w["sns"].sent if "anomaly" in m["Subject"]][0]["Message"], "Khalid told, with the release command")
    check(w["lambda"].fns["justhodl-gamma"]["reserved"] == 5, "a normal engine is untouched")
    gov.lambda_handler({"action": "restore", "include_fuse": True}, Context())
    check(w["lambda"].fns["justhodl-alpha"]["reserved"] == 0, "a plain restore does not lift an anomaly hold")
    storm("2026-10-05T01:30:00", 100000.0)                                      # the storm stops (it is held anyway)
    out = ticks(w, "2026-10-05T03:30:00", 1)
    check(w["lambda"].fns["justhodl-alpha"]["reserved"] == 0, "still held inside the cooldown")
    out = ticks(w, "2026-10-05T08:30:00", 1)
    st = state_of(w)
    check(out["anomaly_releases"] and w["lambda"].fns["justhodl-alpha"]["reserved"] is None and "lambda:justhodl-alpha" not in st["ledger"],
          "released by itself after the cooldown, to the previous concurrency")
    check(any("released" in m["Subject"] for m in w["sns"].sent), "the release is reported")
    ticks(w, "2026-10-05T09:30:00", 15)                                        # a quiet rest of the day
    # second storm: a hard window (>= $5) trips at once
    storm("2026-10-05T23:30:00", 1000000000.0)                                  # ~$8 an hour
    out = ticks(w, "2026-10-06T00:30:00", 1)
    st = state_of(w)
    check(out["anomalies"]["held"] == ["justhodl-alpha"] and st["ledger"]["lambda:justhodl-alpha"]["hold_no"] == 2, "a >= $5 window is held immediately (hold 2)")
    r = gov.lambda_handler({"action": "release_anomaly", "function": "justhodl-alpha"}, Context())
    check(r.get("released") and w["lambda"].fns["justhodl-alpha"]["reserved"] is None, "release_anomaly lifts it early")
    out = ticks(w, "2026-10-06T01:30:00", 1)
    st = state_of(w)
    e = st["ledger"]["lambda:justhodl-alpha"]
    check(out["anomalies"]["capped"] == ["justhodl-alpha"] and e["stage"] == "CAP" and e.get("capped_from") == "ANOM" and w["lambda"].fns["justhodl-alpha"]["reserved"] == 0,
          "third hold in a month is permanent (CAP)")
    out = ticks(w, "2026-10-06T02:30:00", 10)
    check(w["lambda"].fns["justhodl-alpha"]["reserved"] == 0 and not out.get("anomaly_releases"), "no cooldown release for a CAP")
    r = gov.lambda_handler({"action": "release_cap", "function": "justhodl-alpha", "confirm": "justhodl-alpha", "reason": "fixed"}, Context())
    check(r.get("status") == "restored" and w["lambda"].fns["justhodl-alpha"]["reserved"] is None, "only release_cap lifts it")
    gov.now_utc = lambda: PINNED


def test_anomaly_sentinel_respects_a_busy_engines_own_baseline_and_reports_error_storms():
    w = world(FLEET, actual=10, durations={"justhodl-alpha": 20000000.0})          # a busy engine, ~$4/day every day
    set_control(w)
    ticks(w, "2026-10-02T00:30:00", 72)
    st = state_of(w)
    check(not st["ledger"] and not (st.get("anomalies") or {}).get("suspects"), "a consistently busy engine is its own normal: no anomaly")
    w["cloudwatch"].timeline.append((datetime(2026, 10, 4, 23, 30, tzinfo=timezone.utc), {"justhodl-alpha": 40000000.0}))   # 2x: inside 3x its day
    out = ticks(w, "2026-10-05T00:30:00", 2)
    check(not out["anomalies"]["held"] and not out["anomalies"]["suspects"], "doubling is not an anomaly (limit is 3x a normal day)")
    gov.now_utc = lambda: PINNED


def test_anomaly_selftest_round_trips_the_canary():
    w = world(FLEET, actual=10)
    set_control(w)
    r = gov.lambda_handler({"action": "anomaly_selftest"}, Context())
    check(r["ok"] and r["held"] == 0 and r["ledger_stage"] == "ANOM" and r["after"] == r["before"], "anomaly_selftest: detected, held, released, restored (%s)" % json.dumps(r)[:200])
    st = state_of(w)
    check("lambda:justhodl-spend-governor-canary" not in st["ledger"] and not (st.get("anomalies") or {}).get("holds"), "no trace left")


def test_anomaly_monitor_mode_changes_nothing():
    w = world(FLEET, actual=10, durations={"justhodl-alpha": 100000.0})
    set_control(w, mode="monitor")
    ticks(w, "2026-10-02T00:30:00", 48)
    w["cloudwatch"].timeline.append((datetime(2026, 10, 3, 23, 30, tzinfo=timezone.utc), {"justhodl-alpha": 1000000000.0}))
    out = ticks(w, "2026-10-04T00:30:00", 1)
    check(out["anomalies"]["held"] == ["justhodl-alpha"] and w["lambda"].fns["justhodl-alpha"].get("reserved") is None and not w["lambda"].puts,
          "monitor mode reports the hold it would make and writes nothing")
    gov.now_utc = lambda: PINNED



# ------------------------------------------------------------- v1.5.0 tests --
import storage_fuse as sf  # noqa: E402

GB = 1024.0 ** 3


def storage_world(points, meter_daily=None, mode="enforce"):
    """A fleet with the live bucket's daily metrics {day: gb or (gb, objects)} and a meter daily record injected."""
    w = world(FLEET, actual=10)
    set_control(w, mode=mode)
    for day, v in points.items():
        g, n = (v if isinstance(v, tuple) else (v, 4000000))
        w["cloudwatch"].storage[day] = {"StandardStorage": g * GB * 0.8, "StandardIAStorage": g * GB * 0.2, "objects": n}
    st = gov.new_state(gov.now_utc())
    st["month"] = "2026-10"
    st["meter"] = {"month": "2026-10", "daily": meter_daily or {}, "mtd": {fn: 0.5 for fn in (meter_daily or {}).get(max(meter_daily or ["x"]), {})}}
    w["s3"].put_object(Bucket="b", Key=gov.PREFIX + "state.json", Body=json.dumps(st).encode())
    return w


def test_storage_fuse_levels_are_pure_logic():
    cfg = core.merged_cfg(None)
    base = {"2026-10-18": {"gb": 1000.0, "objects": 4000000}}
    quiet = sf.assess(dict(base, **{"2026-10-19": {"gb": 1040.0, "objects": 4050000}}), cfg, {})
    warn = sf.assess(dict(base, **{"2026-10-19": {"gb": 1160.0, "objects": 4000000}}), cfg, {})
    trip = sf.assess(dict(base, **{"2026-10-19": {"gb": 1600.0, "objects": 4000000}}), cfg, {})
    objs = sf.assess(dict(base, **{"2026-10-19": {"gb": 1000.0, "objects": 6500000}}), cfg, {})
    two = sf.assess(dict(base, **{"2026-10-19": {"gb": 1160.0, "objects": 4000000}}), cfg, {"warn_days": ["2026-10-18"]})
    second = sf.assess(dict(base, **{"2026-10-19": {"gb": 1600.0, "objects": 4000000}}), cfg, {"trip_days": ["2026-10-18"]})
    check([quiet["level"], warn["level"], trip["level"], objs["level"], two["level"], second["level"]] == [None, "warn", "trip", "trip", "trip", "second"],
          "assess: quiet / warn / trip / objects trip / two warns = trip / trip after trip = second")
    check(warn["delta_gb"] == 160.0 and warn["delta_pct"] == 16.0 and trip["delta_pct"] == 60.0, "deltas computed in GB and percent")
    check(sf.assess({"2026-10-19": {"gb": 1.0}}, cfg, {})["level"] is None, "one point: no verdict")
    daily = {"2026-10-19": {"justhodl-beta": [500.0, 3.0], "justhodl-alpha": [10.0, 0.1], "justhodl-spend-governor": [24.0, 9.0], "justhodl-held": [900.0, 5.0]}}
    check(sf.pick_writers(daily, "2026-10-19", {"justhodl-spend-governor"}, {"justhodl-held"}, 2) == ["justhodl-beta", "justhodl-alpha"],
          "pick_writers: hottest by cost, never exempt, never already held")
    check(sf.pick_writers(daily, "2026-10-20", {"justhodl-spend-governor"}, set(), 1) == ["justhodl-held"], "a day without a record falls back to the latest earlier one")
    check(sf.s3_usd_by_day({"d": [("Amazon Simple Storage Service", "3.5"), ("AWS Lambda", "9")]}) == {"d": 3.5}, "S3 service line extracted")
    check(sf.storage_days([("d", "StandardStorage", GB), ("d", "StandardIAStorage", GB), ("d", "NumberOfObjects", 7)]) == {"d": {"gb": 2.0, "objects": 7}},
          "storage types summed, objects kept")


def test_storage_fuse_holds_the_hottest_writer_then_suspends_versioning_on_a_second_strike():
    daily = {"2026-10-19": {"justhodl-beta": [500.0, 3.0], "justhodl-alpha": [10.0, 0.1], "justhodl-gamma": [50.0, 0.4]},
             "2026-10-20": {"justhodl-beta": [600.0, 3.5], "justhodl-alpha": [10.0, 0.1], "justhodl-gamma": [50.0, 0.4]}}
    w = storage_world({"2026-10-18": 1000.0, "2026-10-19": 1600.0}, daily)
    at("2026-10-20T06:30:00")
    out = gov.lambda_handler({"action": "tick"}, Context())
    st = state_of(w)
    s = out["storage"]
    check(s["ran"] and s["assessment"]["level"] == "trip" and s["held"] == ["justhodl-beta"], "600 GB in a day: trip, the hottest writer is held (%s)" % json.dumps(s)[:160])
    check(st["ledger"]["lambda:justhodl-beta"]["stage"] == "CAP" and w["lambda"].fns["justhodl-beta"]["reserved"] == 0 and st["ledger"]["lambda:justhodl-beta"].get("storage_fuse"),
          "held under CAP at concurrency 0, tagged storage_fuse")
    check(w["s3"].versioning == "Enabled" and any("STORAGE FUSE" in m["Subject"] for m in w["sns"].sent), "first strike: versioning untouched, notice sent")
    ev = [k for k in w["s3"].objects if "-storage-fuse.json" in k]
    check(len(ev) == 1, "storage-fuse event recorded")
    out2 = gov.lambda_handler({"action": "tick"}, Context())
    check(out2["storage"] == {"ran": False, "reason": "not due"}, "once a day")
    # next day: still growing -> second strike
    w["cloudwatch"].storage["2026-10-20"] = {"StandardStorage": 2200.0 * GB, "StandardIAStorage": 0.0, "objects": 4000000}
    at("2026-10-21T06:30:00")
    out3 = gov.lambda_handler({"action": "tick"}, Context())
    s3 = out3["storage"]
    check(s3["assessment"]["level"] == "second" and s3["versioning"]["suspended"] and w["s3"].versioning == "Suspended",
          "second strike: versioning suspended (%s)" % json.dumps(s3.get("versioning")))
    check(set(s3["held"]) == {"justhodl-alpha", "justhodl-gamma"}, "the next hottest writers are held (beta already held)")
    r = gov.lambda_handler({"action": "storage_restore"}, Context())
    check("error" in r and w["s3"].versioning == "Suspended", "storage_restore needs the confirm word")
    r = gov.lambda_handler({"action": "storage_restore", "confirm": "versioning", "reason": "fixed"}, Context())
    check(r["after"] == "Enabled" and w["s3"].versioning == "Enabled", "storage_restore re-enables versioning")
    gov.lambda_handler({"action": "restore", "include_fuse": True}, Context())
    check(w["lambda"].fns["justhodl-beta"]["reserved"] == 0, "a storage-fuse hold is a CAP: only release_cap lifts it")
    r = gov.lambda_handler({"action": "release_cap", "function": "justhodl-beta", "confirm": "justhodl-beta"}, Context())
    check(r["status"] == "restored" and w["lambda"].fns["justhodl-beta"]["reserved"] == 1, "release_cap restores the writer")
    gov.now_utc = lambda: PINNED


def test_storage_warn_reports_only_and_two_warn_days_trip():
    daily = {"2026-10-19": {"justhodl-beta": [500.0, 3.0]}, "2026-10-20": {"justhodl-beta": [500.0, 3.0]}}
    w = storage_world({"2026-10-18": 1000.0, "2026-10-19": 1160.0}, daily)
    at("2026-10-20T06:30:00")
    out = gov.lambda_handler({"action": "tick"}, Context())
    st = state_of(w)
    check(out["storage"]["assessment"]["level"] == "warn" and "lambda:justhodl-beta" not in st["ledger"] and w["s3"].versioning == "Enabled",
          "160 GB in a day: reported, nothing held")
    check(any("storage:" in m["Subject"] for m in w["sns"].sent) and any("-storage-growth.json" in k for k in w["s3"].objects), "warn notice + event")
    w["cloudwatch"].storage["2026-10-20"] = {"StandardStorage": 1330.0 * GB, "StandardIAStorage": 0.0, "objects": 4000000}
    at("2026-10-21T06:30:00")
    out2 = gov.lambda_handler({"action": "tick"}, Context())
    check(out2["storage"]["assessment"]["level"] == "trip" and out2["storage"]["held"] == ["justhodl-beta"], "a second warn day in a row trips")
    gov.now_utc = lambda: PINNED


def test_storage_check_waits_for_the_metric_and_runs_once_a_day():
    w = storage_world({"2026-10-18": 1000.0}, {})
    at("2026-10-20T03:30:00")
    check(gov.lambda_handler({"action": "tick"}, Context())["storage"] == {"ran": False, "reason": "not due"}, "before 05:00 UTC: not due")
    at("2026-10-20T05:30:00")
    s = gov.lambda_handler({"action": "tick"}, Context())["storage"]
    check(not s["ran"] and s["reason"].startswith("no point for 2026-10-19"), "yesterday's metric not in yet: retry next tick")
    w["cloudwatch"].storage["2026-10-19"] = {"StandardStorage": 1005.0 * GB, "StandardIAStorage": 0.0, "objects": 4010000}
    at("2026-10-20T06:30:00")
    s = gov.lambda_handler({"action": "tick"}, Context())["storage"]
    check(s["ran"] and s["assessment"]["level"] is None and s["s3_usd_yesterday"] == 1.0, "quiet day: ran, no level, S3 line read")
    st = state_of(w)
    check(st["storage"]["day"] == "2026-10-20" and "2026-10-19" in st["storage"]["days"], "day recorded")
    r = gov.lambda_handler({"action": "storage_check"}, Context())
    check(r["ran"] and r["assessment"]["day"] == "2026-10-19" and state_of(w)["storage"]["day"] == "2026-10-20", "storage_check (dry) evaluates without recording")
    r = gov.lambda_handler({"action": "storage_selftest"}, Context())
    check(r["ok"] and r["scenarios"]["second"] == "second" and r["live"]["versioning"] == "Enabled", "storage_selftest: scenarios + live read (%s)" % json.dumps(r)[:120])
    gov.now_utc = lambda: PINNED


def test_storage_s3_spend_alert_once_a_day():
    w = storage_world({"2026-10-18": 1000.0, "2026-10-19": 1001.0}, {})
    w["ce"].s3_usd = {"2026-10-19": 12.25}
    at("2026-10-20T06:30:00")
    s = gov.lambda_handler({"action": "tick"}, Context())["storage"]
    check(s["s3_usd_yesterday"] == 12.25 and any("S3 spend" in m["Subject"] for m in w["sns"].sent) and any("-s3-spend.json" in k for k in w["s3"].objects),
          "S3 service cost above $10 a day is reported with an event")
    gov.now_utc = lambda: PINNED


def test_storage_fuse_monitor_mode_changes_nothing():
    daily = {"2026-10-19": {"justhodl-beta": [500.0, 3.0]}}
    w = storage_world({"2026-10-18": 1000.0, "2026-10-19": 1700.0}, daily, mode="monitor")
    at("2026-10-20T06:30:00")
    s = gov.lambda_handler({"action": "tick"}, Context())["storage"]
    check(s["assessment"]["level"] == "trip" and not w["lambda"].puts and w["s3"].versioning == "Enabled" and any("[monitor]" in m["Subject"] for m in w["sns"].sent),
          "monitor mode: reports the trip, holds nothing")
    gov.now_utc = lambda: PINNED

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
