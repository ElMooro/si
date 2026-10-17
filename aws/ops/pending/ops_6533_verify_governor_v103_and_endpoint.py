"""ops 6533 - verify the Oct 16-17 deploys; read why the owned SageMaker endpoint's suspension fails.

Read-only except one governor status invoke (read-only action).
  1. justhodl-spend-governor reports version 1.0.3 (commit 7d2adf2b) and is still FROZEN with its holds
  2. its ledger entry smasync:endpoint/jh-owned-coder-async/variant/owned after the first v1.0.3 tick:
     status, error, error_msg (new), scale_down_note
  3. the endpoint as AWS sees it: status, instance counts, scalable target (min/max/suspended state),
     scaling policies and scheduled actions - what a request on Sunday 13:20 UTC (factory-market-exam) would do
  4. fleet-error-monitor v1.2.0 live: env LOOKBACK_MINUTES=60, rule fleet-error-monitor-5min = rate(1 hour)
  5. justhodl-ai redeployed with the 6 h Cost Explorer cache (LastModified after bc19de21's deploy)
"""

from datetime import datetime, timezone
from pathlib import Path
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
GOV = "justhodl-spend-governor"
STATE_BUCKET = "justhodl-ops-private-857687956942"
EP, VARIANT = "jh-owned-coder-async", "owned"
RID = "endpoint/%s/variant/%s" % (EP, VARIANT)
DIM = "sagemaker:variant:DesiredInstanceCount"
OUT_JSON = ROOT / "aws" / "ops" / "reports" / "6533.json"
CFG = Config(retries={"max_attempts": 6, "mode": "standard"}, read_timeout=120)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
sm = boto3.client("sagemaker", region_name=REGION, config=CFG)
aas = boto3.client("application-autoscaling", region_name=REGION, config=CFG)
ev = boto3.client("events", region_name=REGION, config=CFG)
out = {"op": 6533, "ran_at": datetime.now(timezone.utc).isoformat(), "errors": {}}
FAILS = []


def step(name, fn, r):
    try:
        return fn(r)
    except Exception as e:  # noqa: BLE001
        out["errors"][name] = "%s: %s" % (type(e).__name__, str(e)[:300])
        r.warn("%s: %s" % (name, out["errors"][name]))


def governor(r):
    resp = lam.invoke(FunctionName=GOV, Payload=json.dumps({"action": "status"}).encode())
    st = json.loads(resp["Payload"].read() or b"{}")
    out["status"] = {k: st.get(k) for k in ("version", "mode", "stage", "stage_since", "cap", "cap_source", "held")}
    r.log("governor status: %s" % json.dumps(out["status"]))
    if st.get("version") != "1.0.3":
        FAILS.append("governor version %s, expected 1.0.3" % st.get("version"))
    if st.get("stage") != "FREEZE":
        FAILS.append("governor stage %s, expected FREEZE until Nov 1" % st.get("stage"))
    state = json.loads(s3.get_object(Bucket=STATE_BUCKET, Key="spend-governor/state.json")["Body"].read())
    e = (state.get("ledger") or {}).get("smasync:" + RID) or {}
    out["smasync_entry"] = {k: e.get(k) for k in ("status", "error", "error_msg", "failed_at", "applied_at",
                                                  "scale_down_note", "scaled_down_from", "before")}
    out["state_updated_at"] = state.get("updated_at")
    out["last_tick"] = (state.get("last") or {})
    r.log("state updated %s; smasync ledger entry: %s" % (state.get("updated_at"), json.dumps(out["smasync_entry"])))
    bad = [{"key": k, "status": v.get("status"), "error": v.get("error"), "error_msg": v.get("error_msg")}
           for k, v in (state.get("ledger") or {}).items() if v.get("status") in ("failed", "restore_failed")]
    out["ledger_failures"] = bad
    (r.warn if bad else r.ok)("ledger entries failing: %s" % json.dumps(bad)[:900])


def endpoint(r):
    d = sm.describe_endpoint(EndpointName=EP)
    out["endpoint"] = {"status": d.get("EndpointStatus"), "config": d.get("EndpointConfigName"),
                       "variants": [{k: v.get(k) for k in ("VariantName", "CurrentInstanceCount", "DesiredInstanceCount")}
                                    for v in d.get("ProductionVariants", [])],
                       "last_modified": str(d.get("LastModifiedTime"))}
    cfg = sm.describe_endpoint_config(EndpointConfigName=d["EndpointConfigName"])
    out["endpoint"]["instance_types"] = [v.get("InstanceType") for v in cfg.get("ProductionVariants", [])]
    r.log("endpoint: %s" % json.dumps(out["endpoint"], default=str))
    t = aas.describe_scalable_targets(ServiceNamespace="sagemaker", ResourceIds=[RID], ScalableDimension=DIM)
    out["scalable_target"] = [{k: x.get(k) for k in ("MinCapacity", "MaxCapacity", "SuspendedState", "RoleARN")}
                              for x in t.get("ScalableTargets", [])]
    r.log("scalable target: %s" % json.dumps(out["scalable_target"], default=str))
    pol = aas.describe_scaling_policies(ServiceNamespace="sagemaker", ResourceId=RID, ScalableDimension=DIM)
    out["policies"] = [{"name": p.get("PolicyName"), "type": p.get("PolicyType"),
                        "alarms": [a.get("AlarmName") for a in p.get("Alarms", [])]} for p in pol.get("ScalingPolicies", [])]
    r.log("scaling policies: %s" % json.dumps(out["policies"]))
    sch = aas.describe_scheduled_actions(ServiceNamespace="sagemaker", ResourceId=RID, ScalableDimension=DIM)
    out["scheduled_actions"] = [{"name": a.get("ScheduledActionName"), "schedule": a.get("Schedule")}
                                for a in sch.get("ScheduledActions", [])]
    r.log("scheduled actions: %s" % json.dumps(out["scheduled_actions"]))
    susp = (out["scalable_target"][0].get("SuspendedState") or {}) if out["scalable_target"] else {}
    out["scale_out_suspended"] = bool(susp.get("DynamicScalingOutSuspended"))
    (r.ok if out["scale_out_suspended"] else r.warn)(
        "scale-out suspended: %s (a Sunday exam request %s wake an instance)" % (
            out["scale_out_suspended"], "cannot" if out["scale_out_suspended"] else "CAN"))


def deploys(r):
    c = lam.get_function_configuration(FunctionName="justhodl-fleet-error-monitor")
    env = (c.get("Environment") or {}).get("Variables") or {}
    rule = ev.describe_rule(Name="fleet-error-monitor-5min")
    out["error_monitor"] = {"lookback": env.get("LOOKBACK_MINUTES"), "rule": rule.get("ScheduleExpression"),
                            "rule_state": rule.get("State"), "last_modified": c.get("LastModified")}
    r.log("fleet-error-monitor: %s" % json.dumps(out["error_monitor"]))
    if env.get("LOOKBACK_MINUTES") != "60" or rule.get("ScheduleExpression") != "rate(1 hour)":
        FAILS.append("fleet-error-monitor not at v1.2.0 settings")
    a = lam.get_function_configuration(FunctionName="justhodl-ai")
    out["ai_last_modified"] = a.get("LastModified")
    r.log("justhodl-ai LastModified %s (bc19de21 deploy ran 2026-10-16T23:06Z)" % a.get("LastModified"))
    if (a.get("LastModified") or "") < "2026-10-16T23:0":
        FAILS.append("justhodl-ai not redeployed after bc19de21")


def main():
    with report("ops_6533_verify_governor_v103_and_endpoint") as r:
        r.heading("ops 6533 - governor v1.0.3, owned endpoint suspension, Oct 16 cost cuts live")
        r.section("Governor")
        step("governor", governor, r)
        r.section("Owned SageMaker endpoint")
        step("endpoint", endpoint, r)
        r.section("Cost-cut deploys")
        step("deploys", deploys, r)
        OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
        for f in FAILS:
            r.warn(f)
        if FAILS or "governor" in out["errors"]:
            r.fail("%d check(s) failed" % (len(FAILS) + ("governor" in out["errors"])))
            sys.exit(1)
        r.ok("all checks passed")
    sys.exit(0)


if __name__ == "__main__":
    main()
