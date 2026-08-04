"""
ops_4356 — restore the crypto-intel schedule, prove auto-update, triage the class.

Ops 4355 evidence: both declared rules for justhodl-crypto-intel are RULE_MISSING,
last invocation bin 2026-07-31T14:52Z, zero Errors — the engine was never called
for 85h. Force invoke worked and the feed moved (03:53Z), so the ONLY break is
the schedule. Manifest check: no scheduler-schedule under any other name either
(true orphan, not a migration). config.json now declares the single real cadence
`justhodl-crypto-15min` (legacy duplicate `justhodl-crypto-intel-schedule`
removed from declaration — the handler ignores event payloads, so two rules only
meant double CMC/API burn), and the rule is appended to
config/schedule-manifest.json so desired state includes it.

This op:
  1. CREATE  put_rule justhodl-crypto-15min = rate(15 minutes) ENABLED
             (name-encoded cadence, corroborated by the object's own
             CacheControl max-age=900 and the engine's design history),
             put_targets -> fn, add_permission (all idempotent).
  2. TRIAGE  the fleet-wide finding from 4355's follow-up scan: 50 rules are
             declared in configs but missing live. For each engine, check the
             live manifest for ANY rule/schedule targeting its ARN:
               has one  -> STALE_DECLARATION (config cleanup queue, no action)
               has none -> ORPHANED (real outage candidates — REPORT ONLY;
                           no mass rule-creation on inferred cadences).
  3. PROVE   the natural fire: record feed LastModified, sleep past the next
             quarter-hour boundary + 240s grace, re-head. The feed must advance
             WITHOUT any manual invoke, and CW Invocations must show the tick.
             A fix that is not measured is a hope.

Idempotent; stays in pending/.
"""
import json, os, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-crypto-intel"
KEY = "crypto-intel.json"
RULE = "justhodl-crypto-15min"
EXPR = "rate(15 minutes)"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=120)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
ev = boto3.client("events", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
NOW = datetime.now(timezone.utc)
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
OUT = {"ops": 4356, "ts": NOW.isoformat(), "engine": FN}


def head_lm():
    try:
        return s3.head_object(Bucket=BUCKET, Key=KEY)["LastModified"]
    except Exception:
        return None


with report("4356_crypto_schedule_restore") as rep:
    rep.heading("ops 4356 — restore crypto-intel schedule + prove natural fire")

    # 1 ── create the binding (idempotent)
    rep.section("1. rule -> target -> permission")
    fn_arn = lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
    rule_arn = ev.put_rule(Name=RULE, ScheduleExpression=EXPR, State="ENABLED",
                           Description="crypto-intel 15-min publish (ops 4356 restore; "
                                       "orphaned ~2026-07-31T15:00Z, feed frozen 85h)")["RuleArn"]
    ev.put_targets(Rule=RULE, Targets=[{"Id": "1", "Arn": fn_arn}])
    try:
        lam.add_permission(FunctionName=FN, StatementId=f"evt-{RULE}",
                           Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com", SourceArn=rule_arn)
        rep.ok("permission added")
    except lam.exceptions.ResourceConflictException:
        rep.ok("permission already present")
    d = ev.describe_rule(Name=RULE)
    tg = ev.list_targets_by_rule(Rule=RULE)["Targets"]
    rep.kv(rule=RULE, state=d["State"], expr=d["ScheduleExpression"],
           targets=len(tg), target_ok=any(t["Arn"] == fn_arn for t in tg))
    OUT["binding"] = {"state": d["State"], "expr": d["ScheduleExpression"],
                      "target_ok": any(t["Arn"] == fn_arn for t in tg)}

    # 2 ── triage the 50-rule class (report only)
    rep.section("2. fleet triage: declared-but-missing rules (no mutations)")
    man = json.loads((ROOT / "config/schedule-manifest.json").read_text())
    live_names = {r.get("name") for r in man["rules"]} | \
                 {s.get("name") for s in man.get("schedules", [])}
    arn_targets = set()
    for r in man["rules"]:
        for t in r.get("targets", []) or []:
            arn_targets.add(t.get("arn", ""))
    for sch in man.get("schedules", []):
        if isinstance(sch, dict):
            t = sch.get("target")
            if isinstance(t, dict):
                arn_targets.add(t.get("arn") or "")
            elif isinstance(t, str):
                arn_targets.add(t)
    sched_blob = json.dumps(man.get("schedules", []))
    orphaned, stale = [], []
    import glob
    for cf in sorted(glob.glob(str(ROOT / "aws/lambdas/*/config.json"))):
        try:
            c = json.loads(open(cf).read())
        except Exception:
            continue
        fn = c.get("function_name", "")
        declared = []
        for r in c.get("eventbridge_rules", []) or []:
            declared.append(r if isinstance(r, str) else (r.get("name") or r.get("rule") or ""))
        missing = [n for n in declared if n and n not in live_names]
        if not missing:
            continue
        this_arn = f"arn:aws:lambda:{REGION}:857687956942:function:{fn}"
        alive_elsewhere = any(this_arn in a for a in arn_targets) or (fn in sched_blob)
        (stale if alive_elsewhere else orphaned).append(
            {"fn": fn, "missing_declared": missing})
    OUT["triage"] = {"orphaned_no_live_schedule": orphaned,
                     "stale_declaration_live_elsewhere": stale}
    rep.kv(orphaned=len(orphaned), stale_declarations=len(stale))
    for o in orphaned:
        rep.warn(f"ORPHANED  {o['fn']}: {o['missing_declared']}")
    for s_ in stale[:12]:
        rep.log(f"stale-cfg {s_['fn']}: {s_['missing_declared']}")
    if len(stale) > 12:
        rep.log(f"... +{len(stale)-12} more stale declarations (full list in JSON)")

    # 3 ── prove the natural fire
    rep.section("3. natural-fire proof (no manual invoke)")
    lm0 = head_lm()
    rep.log(f"feed before wait: {lm0.isoformat() if lm0 else 'HEAD failed'}")
    # rate(15 minutes) first fire is within one period of rule creation; wait
    # one full period + grace, bounded.
    wait_s = 15 * 60 + 240
    rep.log(f"sleeping {wait_s}s for one full period + grace ...")
    t0 = datetime.now(timezone.utc)
    time.sleep(wait_s)
    lm1 = head_lm()
    inv = cw.get_metric_statistics(
        Namespace="AWS/Lambda", MetricName="Invocations",
        Dimensions=[{"Name": "FunctionName", "Value": FN}],
        StartTime=t0 - timedelta(minutes=1), EndTime=datetime.now(timezone.utc),
        Period=60, Statistics=["Sum"])
    ticks = int(sum(d["Sum"] for d in inv["Datapoints"]))
    moved = bool(lm1 and lm0 and lm1 > lm0)
    OUT["natural_fire"] = {"feed_before": lm0.isoformat() if lm0 else None,
                           "feed_after": lm1.isoformat() if lm1 else None,
                           "invocations_during_wait": ticks, "moved": moved}
    if moved and ticks >= 1:
        rep.ok(f"PROVEN — feed advanced to {lm1.isoformat()} on its own "
               f"({ticks} scheduled invocation[s] during the window)")
        OUT["verdict"] = "SCHEDULE_RESTORED_AND_PROVEN"
    else:
        rep.fail(f"not proven: moved={moved} ticks={ticks} "
                 f"(after={lm1.isoformat() if lm1 else None}) — investigate")
        OUT["verdict"] = "NOT_PROVEN"
    rep.section("verdict")
    rep.log(OUT["verdict"])

(ROOT / "aws/ops/reports").mkdir(parents=True, exist_ok=True)
(ROOT / "aws/ops/reports/4356_crypto_schedule_restore.json").write_text(
    json.dumps(OUT, indent=1, default=str))
print("verdict:", OUT["verdict"])
