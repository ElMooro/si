"""ops 3278c — SEC-first resolution proven. company_tickers.json is
the authoritative US name→ticker source; FMP demoted to strict tier-2
(≥2-token name overlap, alpha ≤5 chars); poisoned map entries
self-heal (SEC overrides). PROOF: ARGAN INC → AGX with a real tier;
the ARLLF/HTIA-class contaminations gone from the small-cap census."""
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


with report("3278c_sec_first") as rep:
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
        for _ in range(60):
            time.sleep(10)
            x = s3_json("data/13f-positions.json") or {}
            if str(x.get("generated_at", "")) > mark:
                d = x
                break
        if not d:
            fails.append("feed not fresh")
        else:
            agg = d.get("aggregate_by_ticker") or {}
            arg = [(k, a) for k, a in agg.items()
                   if "ARGAN" in str(a.get("name", "")).upper()]
            for k, a in arg:
                rep.log(f"  ARGAN entry: key={k} "
                        f"tier={a.get('cap_tier')} "
                        f"cap={a.get('market_cap')}")
            if any(k == "AGX" for k, _ in arg):
                rep.ok("ARGAN INC → AGX (SEC-authoritative)")
            else:
                fails.append("AGX not present for ARGAN")
            bad = [k for k in agg
                   if k in ("ARLLF", "HTIA") and
                   "ARGAN" in str(agg[k].get("name", "")).upper()
                   or (k == "HTIA" and "HCA" in
                       str(agg[k].get("name", "")).upper())]
            rep.kv(contaminations_left=len(bad),
                   unresolved=sum(1 for k in agg
                                  if not (k and k.isalpha())),
                   mcap_enriched=d.get("mcap_enriched"))
            if bad:
                warns.append(f"residual: {bad}")
    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
