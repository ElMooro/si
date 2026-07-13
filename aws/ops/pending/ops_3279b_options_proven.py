"""ops 3279b — options actually captured. The cusip-collapse merged
PUT/CALL rows into equity rows and dropped put_call. Now: collapse
key (cusip, put_call), option rows tallied to put/call fund lists but
EXCLUDED from equity $ aggregates and change comparison. v4 re-parse.
Prove: with_puts > 0 with named rows (Citadel-class filers)."""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
FN = "justhodl-13f-positions"
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3279b_options_proven") as rep:
    fails, warns = [], []
    live_cfg = LAM.get_function_configuration(FunctionName=FN)
    env = (live_cfg.get("Environment") or {}).get("Variables") or {}
    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=env, eb_rule_name=None, eb_schedule=None,
                      timeout=max(int(live_cfg.get("Timeout") or 0),
                                  900),
                      memory=int(live_cfg.get("MemorySize") or 1536),
                      description=str(live_cfg.get("Description")
                                      or "")[:250], smoke=False)
        LAM.get_waiter("function_updated_v2").wait(
            FunctionName=FN, WaiterConfig={"Delay": 2,
                                           "MaxAttempts": 40})
    except Exception as e:
        fails.append(f"deploy: {str(e)[:80]}")
    if not fails:
        mark = datetime.now(timezone.utc).isoformat()
        LAM.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        d = None
        for _ in range(80):
            time.sleep(10)
            x = s3_json("data/13f-positions.json") or {}
            if str(x.get("generated_at", "")) > mark:
                d = x
                break
        if not d:
            fails.append("feed not fresh")
        else:
            agg = d.get("aggregate_by_ticker") or {}
            puts = sorted(((k, a) for k, a in agg.items()
                           if a.get("put_funds")),
                          key=lambda kv: -len(kv[1]["put_funds"]))
            calls = sorted(((k, a) for k, a in agg.items()
                            if a.get("call_funds")),
                           key=lambda kv: -len(kv[1]["call_funds"]))
            rep.kv(with_puts=len(puts), with_calls=len(calls))
            for k, a in puts[:4]:
                rep.log(f"  PUT  {str(k)[:8]:<8} "
                        f"{str(a.get('name'))[:26]:<26} by "
                        + ", ".join(a["put_funds"][:3]))
            for k, a in calls[:3]:
                rep.log(f"  CALL {str(k)[:8]:<8} "
                        f"{str(a.get('name'))[:26]:<26} by "
                        + ", ".join(a["call_funds"][:3]))
            if not puts and not calls:
                fails.append("still zero option rows post-v4 — "
                             "inspect raw XML for a sample filing")
    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
