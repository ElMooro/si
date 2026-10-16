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
            res.append(item)
            d += timedelta(days=1)
        return {"ResultsByTime": res}


class FakeCW:
    def __init__(self, alarm_state="OK", durations=None):
        self.alarm_state = alarm_state
        self.durations = durations or {}
        self.metrics = []

    def describe_alarms(self, AlarmNames):
        return {"MetricAlarms": [{"AlarmName": n, "StateValue": self.alarm_state} for n in AlarmNames]}

    def get_metric_data(self, MetricDataQueries, StartTime, EndTime, **kw):
        out = []
        for q in MetricDataQueries:
            m = q["MetricStat"]["Metric"]
            fn = m["Dimensions"][0]["Value"]
            per_h = self.durations.get(fn, 0.0)
            hours = (EndTime - StartTime).total_seconds() / 3600.0
            v = per_h * hours if m["MetricName"] == "Duration" else (10.0 * hours if per_h else 0.0)
            out.append({"Id": q["Id"], "Values": [v]})
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


def world(fns, limit=500, actual=100.0, alarm="OK", durations=None, daily=20.0):
    w = {"lambda": FakeLambda(fns), "budgets": FakeBudgets(limit, actual), "ce": FakeCE(daily),
         "cloudwatch": FakeCW(alarm, durations), "s3": FakeS3(), "sns": FakeSNS()}
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


def test_runaway_fuse():
    durations = {"justhodl-alpha": 900000000.0, "justhodl-gamma": 10000.0}  # ms per hour
    w = world(FLEET, actual=50, alarm="ALARM", durations=durations)
    set_control(w)
    out = gov.lambda_handler({"action": "tick"}, Context())
    st = state_of(w)
    check(out["fuse"].get("new") == ["justhodl-alpha"], "fuse throttles the runaway during a guard alarm")
    check(w["lambda"].fns["justhodl-alpha"]["reserved"] == 0 and st["ledger"]["lambda:justhodl-alpha"]["stage"] == "FUSE",
          "fuse entry recorded with stage FUSE")
    check(w["lambda"].fns["justhodl-gamma"]["reserved"] == 5 and out["stage"] == "NORMAL", "nothing else touched")
    gov.lambda_handler({"action": "restore"}, Context())
    check(w["lambda"].fns["justhodl-alpha"]["reserved"] == 0, "plain restore keeps fuse holds")
    gov.lambda_handler({"action": "restore", "include_fuse": True}, Context())
    check(w["lambda"].fns["justhodl-alpha"]["reserved"] is None, "include_fuse releases it")


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


def test_plan_is_read_only():
    w = world(FLEET, actual=10)
    set_control(w)
    p = gov.lambda_handler({"action": "plan", "stage": "FREEZE"}, Context())
    check(p["would_act_on"].get("lambda") == 4 and not w["lambda"].puts, "plan lists 4 Lambdas and changes nothing")
    check("justhodl-guardrail-notify" in p["exempt"], "plan reports the exempt set")


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
