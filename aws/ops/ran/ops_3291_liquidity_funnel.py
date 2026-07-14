"""ops 3291 — comeback liquidity-funnel refinement. Funnel showed
tradeable 593/11,436: the Average-Volume column drops most genuinely
liquid names (unit quirk — thousands-denominated on some rows). Now
uses max(avg-vol raw, avg-vol x1000 when suspiciously small, today's
Volume) so no liquid comeback is lost, and logs raw liquidity samples
into warns for ground truth. Truth bands: tradeable >= 1500,
candidates >= 7 (never below prior run), boards populated, sample
still satisfies the comeback definition."""
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
FN = "justhodl-comeback-screener"
S3 = boto3.client("s3", region_name=REGION)
LAM = boto3.client("lambda", region_name=REGION)
AWS_DIR = Path(__file__).resolve().parents[2]


def s3_json(key):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return None


with report("3291_liquidity_funnel") as rep:
    fails = []
    env = (LAM.get_function_configuration(FunctionName=FN)
           .get("Environment") or {}).get("Variables") or {}
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=840, memory=1024,
                  description="comeback screener (liquidity resolver, "
                  "ops 3291)", smoke=False)

    mark = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    d = None
    for i in range(60):
        time.sleep(14)
        d = s3_json("data/comeback-screener.json")
        if d and d.get("as_of", "") >= mark:
            break
    if not d or d.get("as_of", "") < mark:
        fails.append("output never freshened")
    else:
        warns = d.get("warns") or []
        funnel = next((w for w in warns
                       if str(w).startswith("funnel=")), "")
        rep.kv(universe=d.get("universe_n"),
               candidates=d.get("candidates_n"),
               funnel=funnel, warns=warns[:6])
        tr = 0
        try:
            tr = int(str(funnel).split("'tradeable': ")[1]
                     .split(",")[0])
        except Exception:
            pass
        if tr < 1500:
            fails.append("tradeable still thin: %s" % tr)
        if (d.get("candidates_n") or 0) < 7:
            fails.append("candidates regressed: %s"
                         % d.get("candidates_n"))
        b = d.get("boards") or {}
        rep.kv(boards={k: len(v or []) for k, v in b.items()})
        pool = (b.get("confirmed") or []) + (b.get("early_turn") or [])
        if not pool:
            fails.append("recovery boards empty")
        else:
            t0 = pool[0]
            rep.kv(sample=dict(t=t0.get("ticker"),
                               off_low=t0.get("off_low_pct"),
                               below_high=t0.get("below_high_pct"),
                               score=t0.get("comeback_score")))
            if not (t0.get("off_low_pct", 0) >= 75
                    and t0.get("below_high_pct", 0) <= -50):
                fails.append("sample violates definition")

    rep.kv(fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3291 PASS — full liquid market now flows the funnel.")
sys.exit(0)
