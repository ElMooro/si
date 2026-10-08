"""ops 3282b — the KeyError killed, THE NUMBERS delivered. Root cause
(from 3282a logs): the 3278 init patch matched the schema DOCSTRING
before the real code init, so aggregate entries lacked put_funds →
KeyError on the first option row → every run since 3279c died
pre-write. Fixed the real init + setdefault armor + 7-day enrichment
cache. Deploy, invoke, and print what Khalid asked for: quarter net$,
risk score/verdict/components, per-fund net + verdicts, first PUT
rows."""
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


with report("3282b_engine_fixed") as rep:
    fails, warns = [], []
    live_cfg = LAM.get_function_configuration(FunctionName=FN)
    env = (live_cfg.get("Environment") or {}).get("Variables") or {}
    try:
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=AWS_DIR / "lambdas" / FN / "source",
                      env_vars=env, eb_rule_name=None, eb_schedule=None,
                      timeout=900,
                      memory=int(live_cfg.get("MemorySize") or 1024),
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
        for _ in range(90):
            time.sleep(10)
            x = s3_json("data/13f-positions.json") or {}
            if str(x.get("generated_at", "")) > mark:
                d = x
                break
        if not d:
            fails.append("feed STILL not fresh — read logs again")
        else:
            f = d.get("flow_summary") or {}
            ra = d.get("risk_appetite") or {}
            agg = d.get("aggregate_by_ticker") or {}
            bf = d.get("by_fund") or {}
            rep.section("THE QUARTER'S NUMBERS")
            rep.kv(as_of=d.get("as_of_quarter"),
                   net_usd=f.get("net_usd"),
                   buys=f.get("total_buy_usd"),
                   sells=f.get("total_sell_usd"),
                   funds_buying=f.get("n_funds_net_buying"),
                   funds_selling=f.get("n_funds_net_selling"),
                   risk_score=ra.get("score"),
                   risk_verdict=ra.get("verdict"),
                   puts=ra.get("n_put_rows"),
                   calls=ra.get("n_call_rows"))
            rep.log(f"  components: "
                    f"{json.dumps(ra.get('components'))}")
            rep.log(f"  smid ${(ra.get('net_smallmid_usd') or 0)/1e9:+.2f}B"
                    f" · large ${(ra.get('net_large_usd') or 0)/1e9:+.2f}B"
                    f" · cyc ${(ra.get('net_cyclical_usd') or 0)/1e9:+.2f}B"
                    f" · def ${(ra.get('net_defensive_usd') or 0)/1e9:+.2f}B")
            rep.section("PER FUND")
            rows = [(v.get("fund_name") or k,
                     (v.get("flow") or {}).get("net_usd") or 0,
                     (v.get("risk") or {}).get("verdict"),
                     (v.get("risk") or {}).get("score"))
                    for k, v in bf.items() if v.get("flow")]
            for nm, net, vd, sc in sorted(rows,
                                          key=lambda r: -r[1]):
                rep.log(f"  {str(nm)[:26]:<26} net "
                        f"${net/1e9:+6.2f}B · {vd} ({sc:+.0f})")
            if len(rows) < max(1, len(bf) - 2):
                fails.append("per-fund fields missing")
            rep.section("FIRST PUT ROWS")
            puts = sorted(((k, a) for k, a in agg.items()
                           if a.get("put_funds")),
                          key=lambda kv: -len(kv[1]["put_funds"]))
            for k, a in puts[:5]:
                rep.log(f"  PUT {str(k)[:8]:<8} "
                        f"{str(a.get('name'))[:24]:<24} by "
                        + ", ".join(a["put_funds"][:3]))
            if not puts:
                warns.append("options zero even post-fix — the "
                             "putCall theory dies; inspect one raw "
                             "Citadel XML next")
            if not f or not ra:
                fails.append("flow/risk missing")
    for w in warns:
        rep.warn(w)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        for f2 in fails:
            rep.fail(f2)
        sys.exit(1)
