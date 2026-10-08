"""ops 3150 — premortem cold-start past the config cache.

3149 restored SSM /justhodl/llm/mode on_demand→normal ($0.00 of $8
spent — stuck switch). The immediate re-invoke still emptied: llm_cost
warm-caches governance for 5 minutes, and the container predated the
flip. Recycle via env nonce → fresh containers read mode=normal →
invoke → the gate that has been waiting since 3146:

  ≥5 theses WITH kill_conditions, samples printed.
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


def s3_json(key):
    o = S3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read().decode("utf-8"))


with report("3150_premortem_coldstart") as rep:
    fails, warns = [], []
    rep.heading("ops 3150 — cold-start premortem, final gate")

    rep.section("1. Container recycle (env nonce)")
    cfg = LAM.get_function_configuration(FunctionName=FN)
    env = (cfg.get("Environment") or {}).get("Variables") or {}
    env["LLM_CFG_NONCE"] = str(int(time.time()))
    _retry_on_conflict(LAM.update_function_configuration,
                       FunctionName=FN, Environment={"Variables": env})
    LAM.get_waiter("function_updated").wait(
        FunctionName=FN, WaiterConfig={"Delay": 3, "MaxAttempts": 40})
    rep.ok("env nonce applied — next invoke is a cold container")

    rep.section("2. Invoke + gate")
    t0 = datetime.now(timezone.utc)
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    doc = None
    deadline = time.time() + 660
    while time.time() < deadline:
        try:
            d = s3_json("data/kill-theses.json")
            ts = d.get("generated_at") or d.get("as_of")
            if ts and datetime.fromisoformat(ts) >= t0:
                doc = d
                break
        except Exception:
            pass
        time.sleep(15)
    if doc is None:
        fails.append("kill-theses never freshened")
    else:
        th = [t for t in (doc.get("theses") or []) if isinstance(t, dict)]
        rich = [t for t in th if t.get("kill_conditions")]
        errs = [t for t in th if t.get("error")]
        rep.kv(theses=len(th), rich=len(rich), row_errors=len(errs),
               elapsed_note=f"doc at {doc.get('generated_at')}")
        if errs:
            rep.log(f"error sample: {json.dumps(errs[0])[:220]}")
        if len(rich) >= 5:
            rep.ok(f"KILL PIPELINE LIVE: {len(rich)}/{len(th)} rich theses")
            for t in rich[:4]:
                kc = (t.get("kill_conditions") or [{}])[0]
                rep.log(f"  · {t.get('symbol')}: "
                        f"{str(kc.get('risk') or kc.get('condition') or kc)[:140]}")
        else:
            fails.append(f"still {len(rich)} rich after cold start — "
                         "error sample above names the next layer")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
