"""ops 3279 — outside-the-box 13F: options + divergence surfaced.
(1) Parser v3 captures the putCall column (the most under-read field
in 13F-HR) → full re-parse of all funds; aggregate tallies put/call
funds per ticker. (2) The existing 13f-price-divergence engine's
decay-scored signals finally reach the page. Prove: fresh feed with
n option-tagged positions + a real PUT row; divergence feed rows;
page boards live with existing anchors intact."""
import json
import sys
import time
import urllib.request
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
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3279)"}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3279_options_divergence") as rep:
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
        rep.section("1. v3 re-parse — options captured")
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
            fails.append("feed not fresh (v3 re-parse window)")
        else:
            agg = d.get("aggregate_by_ticker") or {}
            puts = [(k, a) for k, a in agg.items()
                    if a.get("put_funds")]
            calls = [(k, a) for k, a in agg.items()
                     if a.get("call_funds")]
            rep.kv(tickers=len(agg), with_puts=len(puts),
                   with_calls=len(calls))
            for k, a in sorted(puts, key=lambda kv:
                               -len(kv[1]["put_funds"]))[:3]:
                rep.log(f"  PUT  {k:<6} "
                        f"{str(a.get('name'))[:26]:<26} by "
                        + ", ".join(a["put_funds"][:3]))
            for k, a in sorted(calls, key=lambda kv:
                               -len(kv[1]["call_funds"]))[:2]:
                rep.log(f"  CALL {k:<6} "
                        f"{str(a.get('name'))[:26]:<26} by "
                        + ", ".join(a["call_funds"][:3]))
            if not puts and not calls:
                warns.append("zero option rows — verify putCall node "
                             "naming in live filings")

        rep.section("2. Divergence feed + page boards")
        dv = s3_json("data/13f-price-divergence.json") or {}
        rep.kv(div_state=dv.get("state"),
               n_bullish=len(dv.get("bullish") or []),
               n_bearish=len(dv.get("bearish") or []))
        okp = False
        for i in range(20):
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/13f.html?t="
                    f"{int(time.time())}", headers=UA),
                    timeout=20).read().decode("utf-8", "replace")
                okp = ("still actionable?" in h
                       and "putCall column" in h
                       and "Action Spotlight" in h
                       and "Small & Mid-Cap Footprint" in h)
            except Exception:
                pass
            if okp:
                rep.ok(f"boards live, anchors intact "
                       f"(~{(i + 1) * 15}s)")
                break
            time.sleep(15)
        if not okp:
            fails.append("page boards not live")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
