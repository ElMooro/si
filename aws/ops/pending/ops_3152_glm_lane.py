"""ops 3152 — final layer: policy vs revert vs GLM reasoning-trap.

Facts so far: 2891 set mode=on_demand as deliberate FinOps policy;
3149 flipped it to normal; 3150/3151 still produced empties, but the CW
window may have captured the pre-flip invoke. Anthropic leg: credits
out (verbatim 400). GLM leg: alive, but the probe showed the
reasoning-token trap (finish_reason=length, content empty).

THIS OP settles it:
  1. Read mode/budget/caps RIGHT NOW — did anything revert 3149's flip?
  2. CW for the LAST invoke only (tight window), unfiltered tail —
     per-call router lines verbatim.
  3. One more cold invoke with a generous poll (GLM latency ≈ minutes
     for 15 ideas). Gate ≥5 rich theses.
  4. Whatever the outcome: restore mode to the 2891 POLICY value
     (on_demand) at the end — background-LLM spend is Khalid's call,
     not mine; the report presents the switch + cost math.
"""

import json
import sys
import time
from datetime import datetime, timezone

import boto3

from ops_report import report
from _lambda_deploy_helpers import _retry_on_conflict

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-premortem-engine"

S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
LOGS = boto3.client("logs", region_name=REGION)
SSM = boto3.client("ssm", region_name=REGION)


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


def ssm_get(name):
    try:
        return SSM.get_parameter(Name=name)["Parameter"]["Value"]
    except Exception as e:
        return f"ERR:{str(e)[:60]}"


with report("3152_glm_lane") as rep:
    fails, warns = [], []
    rep.heading("ops 3152 — policy vs revert vs GLM trap")

    rep.section("1. Governance NOW")
    mode_now = ssm_get("/justhodl/llm/mode")
    rep.kv(mode_now=mode_now,
           budget=ssm_get("/justhodl/llm/daily-budget-usd"))
    if str(mode_now).strip().lower() != "normal":
        rep.log("mode reverted since 3149 — a re-setter exists somewhere; "
                "flipping to normal for THIS test window")
        SSM.put_parameter(Name="/justhodl/llm/mode", Value="normal",
                          Type="String", Overwrite=True)

    rep.section("2. CW tail of the LAST invoke (unfiltered)")
    try:
        ev = LOGS.filter_log_events(
            logGroupName=f"/aws/lambda/{FN}",
            startTime=int(time.time() * 1000) - 9 * 60 * 1000, limit=50)
        msgs = [e["message"].strip() for e in ev.get("events") or []]
        keep = [m for m in msgs if "llm_router" in m or "premortem" in m
                or "GLM" in m or "empty" in m][-10:]
        for m in keep:
            rep.log(f"CW: {m[:230]}")
    except Exception as e:
        warns.append(f"CW: {str(e)[:80]}")

    rep.section("3. Cold invoke, patient poll")
    cfg = LAM.get_function_configuration(FunctionName=FN)
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    env["LLM_CFG_NONCE"] = str(int(time.time()))
    _retry_on_conflict(LAM.update_function_configuration,
                       FunctionName=FN, Environment={"Variables": env})
    LAM.get_waiter("function_updated").wait(
        FunctionName=FN, WaiterConfig={"Delay": 3, "MaxAttempts": 40})
    time.sleep(5)
    t0 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    doc, first_ts = None, None
    deadline = time.time() + 660
    while time.time() < deadline:
        try:
            d = s3_json("data/kill-theses.json")
            ts = d.get("generated_at") or d.get("as_of")
            if ts and datetime.fromisoformat(ts) >= t0:
                doc, first_ts = d, ts
                break
        except Exception:
            pass
        time.sleep(15)
    if doc is None:
        fails.append("kill-theses never freshened")
    else:
        wall = (datetime.fromisoformat(first_ts) - t0).total_seconds()
        th = [t for t in (doc.get("theses") or []) if isinstance(t, dict)]
        rich = [t for t in th if t.get("kill_conditions")]
        errs = [t for t in th if t.get("error")]
        rep.kv(theses=len(th), rich=len(rich), row_errors=len(errs),
               engine_wall_s=round(wall, 1))
        if errs:
            rep.log(f"error sample: {json.dumps(errs[0])[:220]}")
        if len(rich) >= 5:
            rep.ok(f"KILL PIPELINE LIVE: {len(rich)}/{len(th)} "
                   f"(engine {wall:.0f}s — real model calls)")
            for t in rich[:4]:
                kc = (t.get("kill_conditions") or [{}])[0]
                rep.log(f"  · {t.get('symbol')}: "
                        f"{str(kc.get('risk') or kc.get('condition') or kc)[:140]}")
        else:
            fails.append(f"{len(rich)} rich · wall {wall:.0f}s — CW above "
                         "names the layer")

    rep.section("4. Restore FinOps policy")
    SSM.put_parameter(Name="/justhodl/llm/mode", Value="on_demand",
                      Type="String", Overwrite=True)
    rep.log("mode restored to on_demand (ops-2891 policy). Re-enabling "
            "background LLM fleet-wide is Khalid's switch: "
            "aws ssm put-parameter --name /justhodl/llm/mode --value "
            "normal --overwrite  (premortem ≈15 GLM calls/weekday ≈ "
            "$0.05/day at current pricing; daily budget cap $8 stays)")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
