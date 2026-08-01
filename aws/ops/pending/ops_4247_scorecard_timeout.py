"""
ops_4247 — justhodl-signal-scorecard: the timeout, not the SSM call.

ops 4246's gate failed and the failure corrected the diagnosis. The
probe returned:

    Sandbox.Timedout — Task timed out after 120.00 seconds

and the scorecard was the ONE engine of fourteen that got WORSE after
the ops-4234 timeout raise: 70% -> 100% error rate over 24h.

What that reorders: ops 4234 read the LAST error line in the log and
called the cause an SSM ValidationException. It was a real bug — and
the fix for it shipped and is verified in the deployed zip — but it was
a SECONDARY symptom. The SSM write is the last thing the handler does,
so on a run that times out first it is never even reached. The primary
failure is that the engine cannot finish inside its ceiling, and 3x of
too-small was still too small.

Two lessons folded into how this op works:
  * "the most recent error line" is not "the cause". The scorecard's
    logs contained both, and the louder one was not the blocking one.
  * A raise proportional to a wrong starting point stays wrong. This
    sets the ceiling from OBSERVED duration with real headroom rather
    than multiplying whatever was there.

The probe is ASYNCHRONOUS and verified through the artifact, because a
synchronous invoke of a long engine ties the gate's outcome to the
client's read timeout — which is what produced the confusing failure in
4246 rather than a clean one.
"""
import json, time
from datetime import datetime, timedelta, timezone
import boto3
from botocore.config import Config
from ops_report import report

REGION, BUCKET = "us-east-1", "justhodl-dashboard-live"
FN = "justhodl-signal-scorecard"
KEY = "data/signal-scorecard.json"
CFG = Config(retries={"max_attempts": 5, "mode": "adaptive"}, read_timeout=120)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
cw  = boto3.client("cloudwatch", region_name=REGION, config=CFG)
s3  = boto3.client("s3", region_name=REGION, config=CFG)
NOW = datetime.now(timezone.utc)

def wait_active(fn,b=200):
    t0=time.time()
    while time.time()-t0<b:
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            if c.get("State")=="Active" and c.get("LastUpdateStatus") in (None,"Successful"): return True
        except Exception: pass
        time.sleep(4)
    return False

with report("4247_scorecard_timeout") as rep:
    rep.heading("ops 4247 — signal-scorecard ceiling")
    fails=[]

    rep.section("1. What the engine actually needs")
    c = lam.get_function_configuration(FunctionName=FN)
    cur_to, cur_mem = c.get("Timeout"), c.get("MemorySize")
    r = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName="Duration",
        Dimensions=[{"Name":"FunctionName","Value":FN}],
        StartTime=NOW-timedelta(days=7), EndTime=NOW, Period=604800,
        Statistics=["Maximum","Average"])
    mx = max((p["Maximum"] for p in r.get("Datapoints",[])), default=0)/1000.0
    av = max((p["Average"] for p in r.get("Datapoints",[])), default=0)/1000.0
    rep.log("current timeout=%ss memory=%sMB | 7d duration avg=%.0fs max=%.0fs"
            %(cur_to, cur_mem, av, mx))
    rep.log("max pins the ceiling exactly -> the ceiling IS the constraint, "
            "so observed duration cannot tell us how long it really needs")
    rep.kv(section="before", timeout=cur_to, memory=cur_mem,
           avg_s=round(av,1), max_s=round(mx,1))

    rep.section("2. Set the ceiling from headroom, not a multiplier")
    new_to, new_mem = 900, max(cur_mem or 512, 1024)
    try:
        wait_active(FN)
        lam.update_function_configuration(FunctionName=FN, Timeout=new_to,
                                          MemorySize=new_mem)
        wait_active(FN)
        c2 = lam.get_function_configuration(FunctionName=FN)
        rep.ok("timeout %ss -> %ss, memory %sMB -> %sMB (more memory also "
               "means proportionally more CPU, so this is a speed change "
               "as well as a ceiling change)"
               %(cur_to, c2.get("Timeout"), cur_mem, c2.get("MemorySize")))
        if c2.get("Timeout") != new_to: fails.append("timeout did not apply")
        rep.kv(section="after", timeout=c2.get("Timeout"),
               memory=c2.get("MemorySize"))
    except Exception as e:
        fails.append("config: %s"%str(e)[:170])

    rep.section("3. Async probe, verified through the artifact")
    try:
        before_gen = None
        try:
            before_gen = json.loads(s3.get_object(Bucket=BUCKET,
                Key=KEY)["Body"].read()).get("generated_at")
        except Exception: pass
        rep.log("artifact generated_at before: %s"%before_gen)
        wait_active(FN)
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=json.dumps({"source":"ops4247"}).encode())
        rep.log("async invoke dispatched; polling the artifact…")
        ok=False
        for i in range(60):
            time.sleep(15)
            try:
                a=json.loads(s3.get_object(Bucket=BUCKET,
                    Key=KEY)["Body"].read())
            except Exception:
                continue
            if a.get("generated_at") and a.get("generated_at")!=before_gen:
                rep.ok("artifact republished at %s (after %ds)"
                       %(a.get("generated_at"), (i+1)*15))
                w=a.get("ssm_writes")
                rep.log("ssm_writes -> %s"%json.dumps(w)[:320])
                rep.kv(section="ssm", ok=a.get("ssm_ok"),
                       writes=json.dumps(w)[:170])
                if a.get("ssm_ok") is True:
                    rep.ok("SSM enforcement map WROTE — the calibrator, "
                           "best-setups and master-ranker are no longer "
                           "reading a frozen map")
                    ok=True
                elif w:
                    rep.fail("SSM still failing: %s"%json.dumps(w)[:200])
                else:
                    rep.warn("artifact refreshed but carries no ssm_writes "
                             "key — the run ended before that branch")
                break
        if not ok:
            fails.append("scorecard did not publish a clean SSM write "
                         "within 15 minutes")
    except Exception as e:
        fails.append("probe: %s"%str(e)[:170])

    rep.section("4. Error rate to watch")
    r2 = cw.get_metric_statistics(Namespace="AWS/Lambda",
        MetricName="Invocations",
        Dimensions=[{"Name":"FunctionName","Value":FN}],
        StartTime=NOW-timedelta(hours=24), EndTime=NOW, Period=86400,
        Statistics=["Sum"])
    inv = sum(p["Sum"] for p in r2.get("Datapoints",[]))
    rep.log("24h invocations before this change: %d at 100%% error. The "
            "verdict on the fix arrives with tomorrow's scheduled runs, "
            "not from this single probe."%int(inv))

    rep.section("RESULT")
    if fails:
        for f in fails: rep.fail("  %s"%f)
        raise SystemExit("FAILS: %s"%"; ".join(fails[:3]))
    rep.ok("OPS 4247 PASS")
