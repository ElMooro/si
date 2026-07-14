"""ops 3285 — flow/risk v2 proven. Gross-flow normalization must kill
the ±1 saturation (assert |cap_flow|<0.95 and |sector_flow|<0.95),
dual-scope numbers print (ALL vs DIRECTIONAL ex Citadel/Millennium),
per-fund scores now spread on fund-gross normalization, banner
directional lines live."""
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
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3285)"}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3285_flow_v2") as rep:
    fails, warns = [], []
    live_cfg = LAM.get_function_configuration(FunctionName=FN)
    env = (live_cfg.get("Environment") or {}).get("Variables") or {}
    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=env, eb_rule_name=None, eb_schedule=None,
                      timeout=900,
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
        for _ in range(70):
            time.sleep(10)
            x = s3_json("data/13f-positions.json") or {}
            if str(x.get("generated_at", "")) > mark:
                d = x
                break
        if not d:
            fails.append("feed not fresh")
        else:
            fa = d.get("flow_summary") or {}
            ra = d.get("risk_appetite") or {}
            fd_ = d.get("flow_summary_directional") or {}
            rd_ = d.get("risk_appetite_directional") or {}
            ca, cd = ra.get("components") or {}, \
                rd_.get("components") or {}
            rep.kv(all_net=fa.get("net_usd"),
                   all_verdict=ra.get("verdict"),
                   all_score=ra.get("score"),
                   dir_net=fd_.get("net_usd"),
                   dir_verdict=rd_.get("verdict"),
                   dir_score=rd_.get("score"))
            rep.log(f"  ALL comps: {json.dumps(ca)}")
            rep.log(f"  DIR comps: {json.dumps(cd)}")
            if abs(ca.get("cap_flow", 0)) >= 0.95 or \
                    abs(ca.get("sector_flow", 0)) >= 0.95:
                fails.append("saturation persists — normalization "
                             "wrong")
            bf = d.get("by_fund") or {}
            scs = [(v.get("risk") or {}).get("score")
                   for v in bf.values()
                   if (v.get("risk") or {}).get("score") is not None]
            vds = [(v.get("risk") or {}).get("verdict")
                   for v in bf.values()]
            rep.kv(fund_score_min=min(scs) if scs else None,
                   fund_score_max=max(scs) if scs else None,
                   non_neutral=sum(1 for v in vds
                                   if v in ("RISK-ON", "RISK-OFF")))
            for k, v in sorted(bf.items(), key=lambda kv:
                               -((kv[1].get("flow") or {})
                                 .get("net_usd") or 0))[:5]:
                fl, rk = v.get("flow") or {}, v.get("risk") or {}
                rep.log(f"  {str(v.get('fund_name'))[:24]:<24} "
                        f"${(fl.get('net_usd') or 0)/1e9:+6.2f}B "
                        f"{rk.get('verdict')} "
                        f"({rk.get('score'):+.0f})"
                        + ("  [hedge-book]" if fl.get("hedge_book")
                           else ""))

        okp = False
        for i in range(20):
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/13f.html?t="
                    f"{int(time.time())}", headers=UA),
                    timeout=20).read().decode("utf-8", "replace")
                okp = ("Directional (ex" in h
                       and "NET FLOW THIS QUARTER" in h)
            except Exception:
                pass
            if okp:
                rep.ok(f"banner directional lines live "
                       f"(~{(i + 1) * 15}s)")
                break
            time.sleep(15)
        if not okp:
            fails.append("banner lines not live")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
