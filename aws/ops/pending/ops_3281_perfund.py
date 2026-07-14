"""ops 3281 — per-fund NET + per-fund RISK, and the 3280 readback.
Section 0 prints the freshly-landed global numbers (net$, risk score/
verdict, PUT rows) from the current feed. Then deploy the per-fund
computation, invoke, print each fund's net + verdict, verify chips on
both fund-card sets + rotation '?' gone + conviction explainer live."""
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
UA = {"User-Agent": "Mozilla/5.0 (jh-ops-3281)"}


def s3_json(key, default=None):
    try:
        return json.loads(S3.get_object(Bucket=BUCKET, Key=key)
                          ["Body"].read())
    except Exception:
        return default


with report("3281_perfund") as rep:
    fails, warns = [], []
    rep.section("0. 3280 readback — the quarter's numbers")
    cur = s3_json("data/13f-positions.json") or {}
    f0 = cur.get("flow_summary") or {}
    r0 = cur.get("risk_appetite") or {}
    rep.kv(feed_at=str(cur.get("generated_at"))[:19],
           net_usd=f0.get("net_usd"),
           risk_score=r0.get("score"),
           risk_verdict=r0.get("verdict"),
           put_rows=r0.get("n_put_rows"),
           call_rows=r0.get("n_call_rows"))
    if f0:
        rep.log(f"  buys ${(f0.get('total_buy_usd') or 0)/1e9:.1f}B "
                f"sells ${(f0.get('total_sell_usd') or 0)/1e9:.1f}B "
                f"· {f0.get('n_funds_net_buying')} buying vs "
                f"{f0.get('n_funds_net_selling')} selling")
        rep.log(f"  components: "
                f"{json.dumps(r0.get('components'))}")
    else:
        warns.append("3280 fields not on feed yet — this run "
                     "produces them")

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
        rep.section("1. Per-fund flow + risk")
        mark = datetime.now(timezone.utc).isoformat()
        LAM.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        d = None
        for _ in range(90):
            time.sleep(10)
            x = s3_json("data/13f-positions.json") or {}
            if str(x.get("generated_at", "")) > mark:
                d = x
                break
        if not d:
            fails.append("feed not fresh in window")
        else:
            bf = d.get("by_fund") or {}
            rows = [(v.get("fund_name") or k,
                     (v.get("flow") or {}).get("net_usd"),
                     (v.get("risk") or {}).get("verdict"),
                     (v.get("risk") or {}).get("score"))
                    for k, v in bf.items() if v.get("flow")]
            rep.kv(funds_with_flow=len(rows), funds_total=len(bf))
            for nm, net, vd, sc in sorted(
                    rows, key=lambda r: -(r[1] or 0))[:6]:
                rep.log(f"  {str(nm)[:26]:<26} net "
                        f"${(net or 0)/1e9:+.2f}B · {vd} "
                        f"({sc:+.0f})" if sc is not None else
                        f"  {nm}")
            if len(rows) < max(1, len(bf) - 2):
                fails.append("per-fund fields missing on most funds")

        rep.section("2. Page chips + fixes live")
        okp = False
        for i in range(20):
            try:
                h = urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/13f.html?t="
                    f"{int(time.time())}", headers=UA),
                    timeout=20).read().decode("utf-8", "replace")
                okp = ("fundFlowChips" in h
                       and "rotation.ticker || '?'" not in h
                       and "AI confidence 0" in h
                       and "Action Spotlight" in h)
            except Exception:
                pass
            if okp:
                rep.ok(f"chips + rotation fix + conviction explainer "
                       f"live (~{(i + 1) * 15}s)")
                break
            time.sleep(15)
        if not okp:
            fails.append("page literals not live")

    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f in fails:
            rep.fail(f)
        sys.exit(1)
