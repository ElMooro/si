"""ops 6545 - is every engine off?  (Khalid, 2026-10-19: "make sure all engines on my system are off")

STAGED (direct lane, no serial twin).  Reads first; acts only on a finding.

The October freeze (ops 6525, governor stage FREEZE since 2026-10-16 22:53 UTC) holds every non-exempt Lambda at
reserved concurrency 0 until the 2026-11-01 00:07 UTC tick.  After it, another lane's ops (serial runs
38999199833 -> 38999602323, 2026-10-17 03:35-03:42 UTC) wrote reserved concurrency 1 on 14 functions
(ops_permanent_brakes_under_500.py) and 0 again on 5 of them, leaving 9 able to run until the governor's hourly
drift pass re-held them.  This op proves the current state from AWS itself:

  1. governor: status, last tick, ledger entries not applied, drift re-holds, its schedule and dead-man alarms
  2. every Lambda's reserved concurrency; non-exempt functions able to run
  3. CloudWatch invocations per function (last 20 min, last 24 h), the lane gap of Oct 17, account invocations
     per hour (72 h) and throttles (scheduled runs refused)
  4. compute outside Lambda: SageMaker endpoints / jobs / notebooks / apps / HyperPod, EC2, ECS, RDS, App Runner,
     Step Functions, enabled event source mappings
  5. what still bills (Cost Explorer by service, last 3 days) and the rules the other lane disabled (outside the
     governor: the Nov 1 restore does not re-enable them)

Act: if anything non-exempt can run, ran in the last 20 minutes, or compute is up -> one governor tick (stage
FREEZE: it discovers anything new and re-applies drift through its own ledger, restored exactly on the 1st); if
the stage is no longer FREEZE -> operator force FREEZE.  Then everything except the metrics is read again.
Exit 1 if anything is still on afterwards.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import concurrent.futures as cf
import json
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
GOV = "justhodl-spend-governor"
STATE_BUCKET = "justhodl-ops-private-857687956942"
GOV_SCHEDULE = "justhodl-spend-governor-tick"
GOV_ALARMS = ["justhodl-spend-governor-heartbeat", "justhodl-spend-governor-errors"]
DEFAULT_EXEMPT = ["justhodl-guardrail-notify", "justhodl-supabase-keepalive", "justhodl-dr-snapshot"]
FREEZE_AT = datetime(2026, 10, 16, 22, 53, tzinfo=timezone.utc)
LANE_GAP = (datetime(2026, 10, 17, 3, 30, tzinfo=timezone.utc), datetime(2026, 10, 17, 5, 0, tzinfo=timezone.utc))
# ops_permanent_brakes_under_500.py (another lane, ran 2026-10-17 ~03:39 UTC) wrote reserved concurrency 1 here
LANE_ONE = ["justhodl-daily-report-v3", "justhodl-katlin", "justhodl-symdir", "justhodl-vix-curve",
            "justhodl-crypto-intel", "justhodl-repo-monitor", "justhodl-13f-positions", "justhodl-dex-scanner",
            "justhodl-student-rsi", "justhodl-chain-resumer", "justhodl-price-redundancy", "justhodl-sec-13f",
            "justhodl-miss-calibrator", "justhodl-event-coordinator"]
# rules that lane disabled (ops_emergency_halt_high_cost.py + ops_permanent_brakes_under_500.py)
LANE_RULES = ["justhodl-daily-8am", "justhodl-vix-curve-30min", "justhodl-katlin-permission-refresh",
              "justhodl-crypto-15min", "justhodl-repo-30min", "justhodl-dex-scanner-15min", "justhodl-sec-8k-30min",
              "justhodl-tv-bars-hourly", "justhodl-v9-auto-refresh"]
DIM = "sagemaker:variant:DesiredInstanceCount"
SM_JOBS = [("training", "list_training_jobs", "TrainingJobSummaries", "TrainingJobName"),
           ("processing", "list_processing_jobs", "ProcessingJobSummaries", "ProcessingJobName"),
           ("transform", "list_transform_jobs", "TransformJobSummaries", "TransformJobName"),
           ("tuning", "list_hyper_parameter_tuning_jobs", "HyperParameterTuningJobSummaries",
            "HyperParameterTuningJobName"),
           ("automl", "list_auto_ml_jobs", "AutoMLJobSummaries", "AutoMLJobName")]
# keys whose findings mean "something can run or is running" -> act, and RED if still there afterwards
ON_KEYS = ("governor_not_frozen", "governor_stale", "can_run", "unreadable", "invoked_20m", "sagemaker",
           "sm_jobs", "sm_notebooks", "sm_apps", "hyperpod", "ec2", "ecs", "rds", "apprunner", "sfn", "esm")

CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=60)
_clients = {}


def c(name):
    if name not in _clients:
        _clients[name] = boto3.client(name, region_name=REGION, config=CFG)
    return _clients[name]


lam_sync = boto3.client("lambda", region_name=REGION,
                        config=Config(retries={"max_attempts": 0}, connect_timeout=10, read_timeout=910))
NOW = datetime.now(timezone.utc).replace(second=0, microsecond=0)


def err(e):
    if isinstance(e, ClientError):
        x = e.response.get("Error", {})
        return "%s: %s" % (x.get("Code"), str(x.get("Message") or "")[:160])
    return "%s: %s" % (type(e).__name__, str(e)[:160])


def code(e):
    return e.response.get("Error", {}).get("Code") if isinstance(e, ClientError) else type(e).__name__


def short(items, n=40):
    items = list(items)
    return ", ".join(str(x) for x in items[:n]) + (" ... (+%d more)" % (len(items) - n) if len(items) > n else "")


def as_utc(s):
    d = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)


def invoke_gov(payload):
    """Sync invoke.  The governor has reserved concurrency 1, so a running scheduled tick answers 429: wait."""
    for i in range(10):
        try:
            res = lam_sync.invoke(FunctionName=GOV, InvocationType="RequestResponse",
                                  Payload=json.dumps(payload).encode())
            body = json.loads(res["Payload"].read() or b"null")
            if res.get("FunctionError"):
                raise RuntimeError("governor %s raised: %s" % (payload.get("action"), json.dumps(body)[:400]))
            return body
        except ClientError as e:
            if code(e) == "TooManyRequestsException" and i < 9:
                time.sleep(30)
                continue
            raise
    return None


# ------------------------------------------------------------------ governor --
def read_governor(r, F):
    try:
        st = invoke_gov({"action": "status"}) or {}
    except Exception as e:  # noqa: BLE001 - a broken governor is itself the finding; keep scanning
        r.fail("governor status failed: %s" % err(e))
        F["governor_stale"] = ["status invoke failed"]
        st = {}
    g = {k: st.get(k) for k in ("version", "mode", "stage", "stage_since", "held", "cap", "cap_source",
                                "budget_actual", "suppress_until")}
    frozen = st.get("stage") == "FREEZE" and st.get("mode") == "enforce"
    (r.ok if frozen else r.fail)("governor v%s: stage %s since %s, mode %s, holds %s" % (
        g["version"], g["stage"], g["stage_since"], g["mode"], json.dumps(g["held"])))
    if not frozen:
        F["governor_not_frozen"] = [g["stage"], g["mode"]]
    if g.get("suppress_until"):
        r.warn("suppress_until %s (an operator release is active)" % g["suppress_until"])
    r.log("budget: actual $%s this month, cap $%s (%s)" % (g.get("budget_actual"), g.get("cap"), g.get("cap_source")))

    try:
        state = json.loads(c("s3").get_object(Bucket=STATE_BUCKET, Key="spend-governor/state.json")["Body"].read())
    except Exception as e:  # noqa: BLE001
        r.fail("governor state.json unreadable: %s" % err(e))
        F["governor_stale"] = F.get("governor_stale", []) + ["state unreadable"]
        state = {}
    try:
        control = json.loads(c("s3").get_object(Bucket=STATE_BUCKET, Key="spend-governor/control.json")["Body"].read())
    except ClientError as e:
        r.warn("control.json unreadable (%s) - default exempt list" % code(e))
        control = {}
    upd = state.get("updated_at")
    age = (NOW - as_utc(upd)).total_seconds() / 60.0 if upd else 9999
    (r.ok if age <= 75 else r.fail)("last governor tick %s (%.0f min ago)" % (upd, age))
    if age > 75:
        F["governor_stale"] = [round(age)]

    ledger = state.get("ledger") or {}
    by = {}
    for e in ledger.values():
        k = "%s/%s/%s" % (e.get("stage"), e.get("lever"), e.get("status"))
        by[k] = by.get(k, 0) + 1
    r.log("ledger (stage/lever/status): %s" % json.dumps(dict(sorted(by.items()))))
    loose = [e for e in ledger.values() if e.get("stage") in ("BRAKE", "FREEZE", "FUSE")
             and e.get("status") not in ("applied", "reapplied", "gone")]
    (r.ok if not loose else r.warn)("ledger holds not in force: %s" % (short(
        "%s=%s %s" % (e["key"], e.get("status"), e.get("error_msg") or e.get("error") or "") for e in loose) or "none"))
    drift = sorted((e for e in ledger.values() if e.get("drift_seen_at")), key=lambda e: e.get("drift_seen_at"))
    r.log("re-held after someone changed them during the freeze: %d" % len(drift))
    for e in drift[:40]:
        r.log("  %s: changed to %s, re-held %s (%s)" % (e["key"], json.dumps(e.get("before")), e.get("drift_seen_at"),
                                                       e.get("status")))

    daily = (state.get("ce") or {}).get("daily") or {}
    days = sorted(daily)[-8:]
    r.log("daily AWS cost (Cost Explorer, as of %s): %s" % ((state.get("ce") or {}).get("at"), ", ".join(
        "%s $%.2f" % (d[5:], daily[d]) for d in days)))

    try:
        s = c("scheduler").get_schedule(Name=GOV_SCHEDULE)
        (r.ok if s.get("State") == "ENABLED" else r.fail)("governor schedule %s %s %s" % (
            GOV_SCHEDULE, s.get("ScheduleExpression"), s.get("State")))
        if s.get("State") != "ENABLED":
            F["governor_stale"] = F.get("governor_stale", []) + ["schedule %s" % s.get("State")]
    except Exception as e:  # noqa: BLE001
        r.warn("governor schedule unreadable: %s" % err(e))
    try:
        al = c("cloudwatch").describe_alarms(AlarmNames=GOV_ALARMS).get("MetricAlarms", [])
        r.log("governor dead-man alarms: %s" % (", ".join("%s=%s" % (a["AlarmName"], a["StateValue"]) for a in al)
                                               or "none found"))
    except Exception as e:  # noqa: BLE001
        r.warn("governor alarms unreadable: %s" % err(e))

    # same rule as governor_core.is_exempt over load_control() (DEFAULT_CONTROL updated by control.json)
    exempt = set(control["exempt_functions"] if "exempt_functions" in control else DEFAULT_EXEMPT) | {GOV}
    prefixes = [p for p in (control.get("exempt_prefixes") or []) if p]
    r.log("exempt by design (they keep the freeze and the login alive): %s%s" % (
        ", ".join(sorted(exempt)), (" + prefixes " + ", ".join(prefixes)) if prefixes else ""))
    return st, (lambda fn: fn in exempt or any(fn.startswith(p) for p in prefixes))


# ------------------------------------------------------------------- Lambda --
def list_function_names():
    names = []
    for p in c("lambda").get_paginator("list_functions").paginate():
        names += [f["FunctionName"] for f in p["Functions"]]
    return names


def lambda_scan(r, F, is_exempt, label):
    names = list_function_names()

    def one(fn):
        try:
            return fn, c("lambda").get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions"), None
        except ClientError as e:
            if code(e) == "ResourceNotFoundException":
                return fn, "gone", None
            return fn, None, err(e)

    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        res = list(ex.map(one, names))
    vals = {fn: v for fn, v, e in res if not e and v != "gone"}
    unreadable = {fn: e for fn, _, e in res if e}
    held = [fn for fn, v in vals.items() if v == 0]
    can_run = {fn: ("unreserved" if v is None else v) for fn, v in vals.items() if v != 0 and not is_exempt(fn)}
    exempt_live = sorted(fn for fn, v in vals.items() if v != 0 and is_exempt(fn))
    r.log("%s: %d functions - %d held at 0, %d exempt and live (%s), %d non-exempt able to run, %d unreadable" % (
        label, len(names), len(held), len(exempt_live), ", ".join(exempt_live), len(can_run), len(unreadable)))
    if can_run:
        r.fail("non-exempt functions that can run: %s" % short("%s=%s" % kv for kv in sorted(can_run.items())))
        F["can_run"] = sorted(can_run)
    else:
        r.ok("every non-exempt function is held at reserved concurrency 0 (%d)" % len(held))
    if unreadable:
        r.fail("concurrency unreadable: %s" % short("%s (%s)" % kv for kv in sorted(unreadable.items())))
        F["unreadable"] = sorted(unreadable)
    return names, vals


# --------------------------------------------------------------- CloudWatch --
def fn_metric(names, metric, start, end, period, stat="Sum"):
    q = [{"Id": "m%d" % i, "ReturnData": True, "MetricStat": {"Metric": {
        "Namespace": "AWS/Lambda", "MetricName": metric, "Dimensions": [{"Name": "FunctionName", "Value": n}]},
        "Period": period, "Stat": stat}} for i, n in enumerate(names)]
    pts = {}
    for i in range(0, len(q), 500):
        token = None
        while True:
            kw = {"MetricDataQueries": q[i:i + 500], "StartTime": start, "EndTime": end,
                  "ScanBy": "TimestampAscending"}
            if token:
                kw["NextToken"] = token
            resp = c("cloudwatch").get_metric_data(**kw)
            for res in resp["MetricDataResults"]:
                pts.setdefault(res["Id"], []).extend(zip(res.get("Timestamps") or [], res.get("Values") or []))
            token = resp.get("NextToken")
            if not token:
                break
    return {names[int(k[1:])]: v for k, v in pts.items() if v and sum(x for _, x in v) > 0}


def account_metric(metric, start, end, period, stat="Sum"):
    resp = c("cloudwatch").get_metric_data(
        MetricDataQueries=[{"Id": "a", "ReturnData": True, "MetricStat": {"Metric": {
            "Namespace": "AWS/Lambda", "MetricName": metric}, "Period": period, "Stat": stat}}],
        StartTime=start, EndTime=end, ScanBy="TimestampAscending")
    res = resp["MetricDataResults"][0]
    return list(zip(res.get("Timestamps") or [], res.get("Values") or []))


def metrics(r, F, names, is_exempt):
    recent = fn_metric(names, "Invocations", NOW - timedelta(minutes=20), NOW, 1200)
    tot = {fn: int(sum(x for _, x in v)) for fn, v in recent.items()}
    rogue = {fn: n for fn, n in tot.items() if not is_exempt(fn)}
    (r.ok if not rogue else r.fail)("invocations %s-%s UTC: non-exempt %s; exempt %s" % (
        (NOW - timedelta(minutes=20)).strftime("%H:%M"), NOW.strftime("%H:%M"), json.dumps(rogue) if rogue else "none",
        json.dumps({k: v for k, v in tot.items() if is_exempt(k)})))
    if rogue:
        F["invoked_20m"] = sorted(rogue)

    day = fn_metric(names, "Invocations", NOW - timedelta(hours=24), NOW, 3600)
    dtot = {fn: int(sum(x for _, x in v)) for fn, v in day.items()}
    nonex = {fn: n for fn, n in sorted(dtot.items(), key=lambda kv: -kv[1]) if not is_exempt(fn)}
    if nonex:
        last = {fn: max(t for t, x in day[fn] if x > 0).strftime("%m-%d %H:00") for fn in nonex}
        r.warn("non-exempt functions with invocations in the last 24 h (hour of the last one): %s" % short(
            "%s=%d (%s)" % (fn, n, last[fn]) for fn, n in nonex.items()))
    else:
        r.ok("no non-exempt function ran in the last 24 h")
    r.log("exempt functions, last 24 h: %s" % json.dumps({k: v for k, v in dtot.items() if is_exempt(k)}))

    gap = fn_metric([n for n in LANE_ONE if n in set(names)], "Invocations", LANE_GAP[0], LANE_GAP[1], 300)
    r.log("the Oct 17 03:30-05:00 UTC gap (another lane set concurrency 1 on %d functions): %s" % (
        len(LANE_ONE), json.dumps({fn: ["%s=%d" % (t.strftime("%H:%M"), x) for t, x in v if x]
                                   for fn, v in gap.items()}) if gap else "none of them ran"))

    hourly = account_metric("Invocations", NOW - timedelta(hours=72), NOW, 3600)
    r.log("account invocations per hour, last 72 h (UTC): %s" % ", ".join(
        "%s=%d" % (t.strftime("%d %H"), x) for t, x in hourly))
    thr = account_metric("Throttles", NOW - timedelta(hours=24), NOW, 3600)
    r.log("account throttles, last 24 h (scheduled runs refused by the hold): %d" % int(sum(x for _, x in thr)))
    conc = account_metric("ConcurrentExecutions", NOW - timedelta(hours=24), NOW, 3600, "Maximum")
    r.log("account max concurrent executions, last 24 h: %d" % int(max([x for _, x in conc] or [0])))


# --------------------------------------------------------- compute elsewhere --
def sagemaker(r, F):
    sm, aas = c("sagemaker"), c("application-autoscaling")
    n = 0
    for p in sm.get_paginator("list_endpoints").paginate():
        for ep in p["Endpoints"]:
            n += 1
            name, status = ep["EndpointName"], ep["EndpointStatus"]
            try:
                d = sm.describe_endpoint(EndpointName=name)
            except ClientError as e:
                r.warn("endpoint %s: %s" % (name, err(e)))
                continue
            try:
                is_async = bool(sm.describe_endpoint_config(
                    EndpointConfigName=d["EndpointConfigName"]).get("AsyncInferenceConfig"))
            except ClientError:
                is_async = bool(d.get("AsyncInferenceConfig"))
            for v in d.get("ProductionVariants", []) or [{}]:
                vname = v.get("VariantName", "?")
                serverless = bool(v.get("CurrentServerlessConfig") or v.get("DesiredServerlessConfig"))
                cur = int(v.get("CurrentInstanceCount") or 0)
                desc = "endpoint %s/%s: %s, %s, instances %d (desired %s)" % (
                    name, vname, status, "serverless" if serverless else ("async" if is_async else "real-time"),
                    cur, v.get("DesiredInstanceCount"))
                wake = False
                if is_async and not serverless:
                    rid = "endpoint/%s/variant/%s" % (name, vname)
                    t = aas.describe_scalable_targets(ServiceNamespace="sagemaker", ResourceIds=[rid],
                                                      ScalableDimension=DIM).get("ScalableTargets", [])
                    if t:
                        sus = (t[0].get("SuspendedState") or {}).get("DynamicScalingOutSuspended")
                        desc += ", autoscaling %s..%s, scale-out suspended %s" % (
                            t[0].get("MinCapacity"), t[0].get("MaxCapacity"), sus)
                        wake = sus is not True and int(t[0].get("MaxCapacity") or 0) > 0
                    else:
                        desc += ", no autoscaling target"
                on = (not serverless) and cur > 0
                if on or wake:
                    r.fail(desc + (" - RUNNING" if on else " - a queued request could wake it"))
                    F.setdefault("sagemaker", []).append(name)
                else:
                    r.ok(desc + (" - bills only when called; every caller is frozen" if serverless else " - off"))
    if not n:
        r.ok("no SageMaker endpoints")

    for kind, lister, field, key in SM_JOBS:
        try:
            run = getattr(sm, lister)(StatusEquals="InProgress", MaxResults=100).get(field, [])
            new = getattr(sm, lister)(CreationTimeAfter=FREEZE_AT, MaxResults=100).get(field, [])
        except Exception as e:  # noqa: BLE001
            r.warn("%s jobs unreadable: %s" % (kind, err(e)))
            continue
        status = lambda j: next((j[k] for k in j if k.endswith("Status") and isinstance(j[k], str)), "?")  # noqa: E731
        if run:
            r.fail("%s jobs running: %s" % (kind, short("%s" % j[key] for j in run)))
            F.setdefault("sm_jobs", []).extend(j[key] for j in run)
        r.log("%s jobs started since the freeze: %s" % (kind, short("%s=%s" % (j[key], status(j)) for j in new) or "none"))
    nb = sm.list_notebook_instances(StatusEquals="InService").get("NotebookInstances", [])
    if nb:
        r.fail("notebook instances in service: %s" % short(x["NotebookInstanceName"] for x in nb))
        F["sm_notebooks"] = [x["NotebookInstanceName"] for x in nb]
    apps = [a for p in sm.get_paginator("list_apps").paginate() for a in p.get("Apps", [])
            if a.get("Status") == "InService"]
    paid = [a for a in apps if a.get("AppType") != "JupyterServer"]
    if paid:
        r.fail("Studio apps on paid instances: %s" % short("%s/%s" % (a["AppType"], a["AppName"]) for a in paid))
        F["sm_apps"] = ["%s/%s" % (a["AppType"], a["AppName"]) for a in paid]
    r.log("notebooks in service %d, Studio apps in service %d (%d on paid instances)" % (len(nb), len(apps), len(paid)))
    try:
        hp = [x for x in sm.list_clusters().get("ClusterSummaries", []) if x.get("ClusterStatus") != "Deleting"]
        if hp:
            r.fail("HyperPod clusters: %s" % short("%s=%s" % (x["ClusterName"], x.get("ClusterStatus")) for x in hp))
            F["hyperpod"] = [x["ClusterName"] for x in hp]
    except Exception as e:  # noqa: BLE001
        r.warn("HyperPod unreadable: %s" % err(e))


def other_compute(r, F, is_exempt):
    def guard(label, fn):
        try:
            fn()
        except Exception as e:  # noqa: BLE001 - one service must never hide the others
            r.warn("%s unreadable: %s" % (label, err(e)))

    def ec2():
        ins = [i for p in c("ec2").get_paginator("describe_instances").paginate(
            Filters=[{"Name": "instance-state-name", "Values": ["pending", "running"]}])
            for rv in p["Reservations"] for i in rv["Instances"]]
        if ins:
            r.fail("EC2 running: %s" % short("%s %s" % (i["InstanceId"], i["InstanceType"]) for i in ins))
            F["ec2"] = [i["InstanceId"] for i in ins]
        else:
            r.ok("EC2: nothing running")

    def ecs():
        live = []
        for arn in [a for p in c("ecs").get_paginator("list_clusters").paginate() for a in p["clusterArns"]]:
            svcs = [s for p in c("ecs").get_paginator("list_services").paginate(cluster=arn) for s in p["serviceArns"]]
            for i in range(0, len(svcs), 10):
                for s in c("ecs").describe_services(cluster=arn, services=svcs[i:i + 10]).get("services", []):
                    if s.get("runningCount") or s.get("desiredCount"):
                        live.append("%s run %s/%s" % (s["serviceName"], s.get("runningCount"), s.get("desiredCount")))
            tasks = c("ecs").list_tasks(cluster=arn, desiredStatus="RUNNING").get("taskArns", [])
            if tasks:
                live.append("%s: %d task(s)" % (arn.split("/")[-1], len(tasks)))
        if live:
            r.fail("ECS: %s" % short(live))
            F["ecs"] = live
        else:
            r.ok("ECS: nothing running")

    def rds():
        live = ["%s=%s" % (d["DBInstanceIdentifier"], d["DBInstanceStatus"])
                for p in c("rds").get_paginator("describe_db_instances").paginate() for d in p["DBInstances"]
                if d.get("DBInstanceStatus") not in ("stopped", "stopping", "deleting")]
        live += ["cluster %s=%s" % (d["DBClusterIdentifier"], d["Status"])
                 for p in c("rds").get_paginator("describe_db_clusters").paginate() for d in p["DBClusters"]
                 if d.get("Status") not in ("stopped", "stopping", "deleting")]
        if live:
            r.fail("RDS up: %s" % short(live))
            F["rds"] = live
        else:
            r.ok("RDS: nothing up")

    def apprunner():
        live = [s["ServiceName"] for s in c("apprunner").list_services().get("ServiceSummaryList", [])
                if s.get("Status") == "RUNNING"]
        if live:
            r.fail("App Runner running: %s" % short(live))
            F["apprunner"] = live
        else:
            r.ok("App Runner: nothing running")

    def sfn():
        live = []
        for p in c("stepfunctions").get_paginator("list_state_machines").paginate():
            for m in p["stateMachines"]:
                ex = c("stepfunctions").list_executions(stateMachineArn=m["stateMachineArn"], statusFilter="RUNNING",
                                                        maxResults=5).get("executions", [])
                if ex:
                    live.append("%s x%d" % (m["name"], len(ex)))
        if live:
            r.fail("Step Functions executions running: %s" % short(live))
            F["sfn"] = live
        else:
            r.ok("Step Functions: nothing running")

    def esm():
        live = []
        for p in c("lambda").get_paginator("list_event_source_mappings").paginate():
            for m in p["EventSourceMappings"]:
                fn = m["FunctionArn"].split(":")[6]
                if m.get("State") in ("Enabled", "Enabling", "Updating", "Creating") and not is_exempt(fn):
                    live.append("%s <- %s" % (fn, str(m.get("EventSourceArn", "?")).split(":")[-1]))
        if live:
            r.fail("event source mappings enabled: %s" % short(live))
            F["esm"] = live
        else:
            r.ok("event source mappings: none enabled on a non-exempt function")

    for label, fn in (("EC2", ec2), ("ECS", ecs), ("RDS", rds), ("App Runner", apprunner),
                      ("Step Functions", sfn), ("event source mappings", esm)):
        guard(label, fn)


def schedules_and_cost(r):
    states = {}
    for name in LANE_RULES:
        try:
            states[name] = c("events").describe_rule(Name=name).get("State")
        except ClientError as e:
            states[name] = "missing" if code(e) == "ResourceNotFoundException" else code(e)
    r.log("rules another lane switched off on Oct 17 (the Nov 1 restore will NOT re-enable them): %s" % json.dumps(states))
    try:
        n = sum(len(p.get("Schedules", [])) for p in c("scheduler").get_paginator("list_schedules").paginate(
            State="ENABLED"))
        r.log("EventBridge Scheduler schedules still enabled: %d (they fire into held functions and are refused)" % n)
    except Exception as e:  # noqa: BLE001
        r.warn("schedules unreadable: %s" % err(e))
    try:
        res = c("ce").get_cost_and_usage(
            TimePeriod={"Start": (NOW - timedelta(days=3)).date().isoformat(),
                        "End": (NOW + timedelta(days=1)).date().isoformat()},
            Granularity="DAILY", Metrics=["UnblendedCost"], GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}])
        for t in res["ResultsByTime"]:
            g = sorted(((x["Keys"][0], float(x["Metrics"]["UnblendedCost"]["Amount"])) for x in t.get("Groups", [])),
                       key=lambda kv: -kv[1])
            tot = sum(v for _, v in g)
            r.log("still billing %s: $%.2f - %s" % (t["TimePeriod"]["Start"], tot, ", ".join(
                "%s $%.2f" % (k, v) for k, v in g if v >= 0.05)))
    except Exception as e:  # noqa: BLE001
        r.warn("Cost Explorer unreadable: %s" % err(e))


# --------------------------------------------------------------------- main --
def scan(r, is_exempt, label):
    F = {}
    lambda_scan(r, F, is_exempt, label)
    sagemaker(r, F)
    other_compute(r, F, is_exempt)
    return F


def main():
    with report("ops_6545_fleet_off_check") as r:
        r.heading("ops 6545 - is every engine off? (read %s UTC)" % NOW.strftime("%Y-%m-%d %H:%M"))
        F = {}
        r.section("Governor (the switch that holds the fleet off)")
        st, is_exempt = read_governor(r, F)
        r.section("Lambda engines")
        names = list_function_names()
        F.update(scan(r, is_exempt, "before"))
        r.section("Who actually ran")
        try:
            metrics(r, F, names, is_exempt)
        except Exception as e:  # noqa: BLE001
            r.warn("CloudWatch unreadable: %s" % err(e))
        r.section("Schedules and what still bills")
        schedules_and_cost(r)

        found = {k: F[k] for k in ON_KEYS if F.get(k)}
        if found:
            r.section("Re-asserting the freeze")
            r.log("found on: %s" % json.dumps(found)[:1500])
            try:
                if st.get("stage") == "FREEZE" and st.get("mode") == "enforce":
                    res = invoke_gov({"action": "tick", "source": "ops"}) or {}
                    r.log("governor tick: stage %s, decision %s, drift %s" % (
                        res.get("stage"), res.get("decision"), json.dumps(res.get("drift"))[:600]))
                else:
                    res = invoke_gov({"action": "force", "stage": "FREEZE", "confirm": "FREEZE",
                                      "reason": "Khalid 2026-10-19: make sure every engine is off (ops 6545)"}) or {}
                    r.log("governor force FREEZE: %s" % json.dumps(res)[:800])
            except Exception as e:  # noqa: BLE001
                r.fail("re-asserting the freeze failed: %s" % err(e))
            time.sleep(20)
            r.section("After")
            try:
                st2 = invoke_gov({"action": "status"}) or {}
            except Exception as e:  # noqa: BLE001
                r.fail("governor status after: %s" % err(e))
                st2 = {}
            after = scan(r, is_exempt, "after")
            if not (st2.get("stage") == "FREEZE" and st2.get("mode") == "enforce"):
                after["governor_not_frozen"] = [st2.get("stage"), st2.get("mode")]
            still = {k: after[k] for k in ON_KEYS if after.get(k)}
        else:
            still = {}

        r.section("Verdict")
        if still:
            r.fail("STILL ON: %s" % json.dumps(still)[:1500])
            sys.exit(1)
        # The ✗ lines above are the BEFORE state; the verdict is the state after the re-assert, so a fleet that
        # is off now must not render as Status: failure.
        r._failed = False
        r.ok("ALL ENGINES OFF - every non-exempt Lambda held at reserved concurrency 0, no SageMaker / EC2 / ECS / "
             "RDS / App Runner / Step Functions compute up, governor FREEZE (auto-restore 2026-11-01 00:07 UTC)%s" % (
                 "; re-asserted by this op" if found else ""))
    sys.exit(0)


if __name__ == "__main__":
    main()
