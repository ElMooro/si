"""
ops 903 -- verify Edge #7: Stablecoin Mint / Supply-Growth Tracker
==================================================================

Lambda: justhodl-stablecoin-flow
Output: s3://justhodl-dashboard-live/data/stablecoin-flow.json
Page:   https://justhodl.ai/stablecoin-flow.html

Checks:
- Lambda deployed (python3.12, mem>=256)
- Schedule attached (hourly cadence)
- Invoke completes
- S3 output present + parseable
- Canonical schema (engine, state, signal_strength, aggregate,
  trigger_conditions, forward_expectations, forward_expectations_by_asset_60d,
  top_stablecoins_by_mcap, top_5_minters_30d, recommended_trade,
  historical_episodes, why_now_explainer, methodology)
- 3 horizons present (1m/3m/12m) with return_pct
- BTC/ETH/ALT/SPX forward priors present
- top_stablecoins_by_mcap is a non-empty list with mcap field
- recommended_trade.primary present (or trade ticket per state)
- State in valid enum {CONTRACTING, FLAT, EXPANDING, EXPLOSIVE_MINT, PARABOLIC_MINT}
- Page live (HTTP 200)
"""

import json
import os
import sys
import time
import urllib.request
import urllib.error
import datetime as dt

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
PAGES_BASE = "https://justhodl.ai"

FN = "justhodl-stablecoin-flow"
S3_KEY = "data/stablecoin-flow.json"
PAGE = "stablecoin-flow.html"

VALID_STATES = {"CONTRACTING", "FLAT", "EXPANDING", "EXPLOSIVE_MINT", "PARABOLIC_MINT"}

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
events = boto3.client("events", region_name=REGION)

CHECKS = []


def add(name, ok, detail=""):
    CHECKS.append({"name": name, "passed": bool(ok), "detail": str(detail)[:400]})
    print(("PASS" if ok else "FAIL"), name, "-", str(detail)[:200])


def main():
    # 1. Lambda deployed
    try:
        f = lam.get_function(FunctionName=FN)
        cfg = f["Configuration"]
        ok = (cfg["Runtime"] == "python3.12"
              and cfg["MemorySize"] >= 256
              and cfg["State"] == "Active")
        add("lambda_deployed", ok,
            f"runtime={cfg['Runtime']} mem={cfg['MemorySize']} state={cfg['State']}")
    except ClientError as e:
        add("lambda_deployed", False, str(e))
        write_report()
        return

    # 2. Schedule attached
    schedule_ok = False
    sched_name = None
    try:
        for s in sch.list_schedules(MaxResults=100).get("Schedules", []):
            if FN in s["Name"]:
                schedule_ok = True
                sched_name = s["Name"]
                break
    except ClientError:
        pass
    if not schedule_ok:
        # fallback: EventBridge rules
        try:
            for r in events.list_rules(NamePrefix=FN).get("Rules", []):
                schedule_ok = True
                sched_name = r["Name"]
                break
        except ClientError:
            pass
    add("schedule_attached", schedule_ok, f"name={sched_name or 'NONE'}")

    # 3. Invoke
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode()
        ok = (r["StatusCode"] == 200 and not r.get("FunctionError"))
        add("invoke_success", ok, f"status={r['StatusCode']} fnErr={r.get('FunctionError')} body={body[:200]}")
    except ClientError as e:
        add("invoke_success", False, str(e))

    # 4. S3 output
    try:
        r = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        data = json.loads(r["Body"].read())
        add("s3_output_present", True, f"size={r['ContentLength']}")
    except Exception as e:
        add("s3_output_present", False, str(e))
        data = {}

    # 5. Schema complete
    required = ["engine", "state", "signal_strength", "aggregate",
                "trigger_conditions", "forward_expectations",
                "forward_expectations_by_asset_60d",
                "top_stablecoins_by_mcap", "top_5_minters_30d",
                "recommended_trade", "historical_episodes",
                "why_now_explainer", "methodology"]
    missing = [k for k in required if k not in data]
    add("schema_complete", not missing, f"missing={missing}")

    # 6. State valid
    state = data.get("state")
    add("state_valid", state in VALID_STATES, f"state={state}")

    # 7. 3 horizons present
    fe = data.get("forward_expectations", {})
    horizons_present = sum(1 for h in ("1m", "3m", "12m") if fe.get(h, {}).get("return_pct") is not None)
    add("forward_expectations_3_horizons", horizons_present == 3,
        f"horizons with return_pct: {horizons_present}/3, "
        f"1m={fe.get('1m',{}).get('return_pct')} "
        f"3m={fe.get('3m',{}).get('return_pct')} "
        f"12m={fe.get('12m',{}).get('return_pct')}")

    # 8. By-asset priors
    ba = data.get("forward_expectations_by_asset_60d", {})
    asset_count = sum(1 for k in ("BTC_pct", "ETH_pct", "ALT_basket_pct", "SPX_pct") if k in ba)
    add("forward_by_asset_60d", asset_count == 4,
        f"assets={asset_count}/4, BTC={ba.get('BTC_pct')} ETH={ba.get('ETH_pct')}")

    # 9. Top stablecoins list
    top = data.get("top_stablecoins_by_mcap", [])
    has_mcap = top and all("mcap" in c and "symbol" in c for c in top[:5])
    add("top_stablecoins_present", len(top) >= 5 and has_mcap,
        f"n={len(top)}, sample_symbols={[c.get('symbol') for c in top[:5]]}")

    # 10. Recommended trade
    rt = data.get("recommended_trade", {})
    has_primary = isinstance(rt, dict) and rt.get("primary") and rt["primary"].get("instrument")
    add("recommended_trade_present", has_primary,
        f"primary={'OK' if has_primary else 'MISSING'} state={state}")

    # 11. Why-now explainer
    wn = data.get("why_now_explainer", "")
    add("why_now_explainer_present", len(wn) > 200,
        f"len={len(wn)}")

    # 12. Aggregate fields
    agg = data.get("aggregate", {})
    agg_keys = ["total_usd", "delta_24h_usd", "delta_7d_usd", "delta_30d_usd", "n_stablecoins"]
    missing_agg = [k for k in agg_keys if k not in agg]
    add("aggregate_complete", not missing_agg,
        f"missing={missing_agg} total=${agg.get('total_usd',0)/1e9:.1f}B n={agg.get('n_stablecoins')}")

    # 13. Page live
    try:
        req = urllib.request.Request(f"{PAGES_BASE}/{PAGE}",
                                     headers={"User-Agent": "justhodl-verify/1.0"})
        resp = urllib.request.urlopen(req, timeout=20)
        body = resp.read().decode()
        ok = (resp.status == 200 and "stablecoin-flow.json" in body)
        add("page_live", ok, f"status={resp.status} len={len(body)} url_present={'stablecoin-flow.json' in body}")
    except Exception as e:
        add("page_live", False, str(e))

    write_report()


def write_report():
    report = {
        "ops": 903,
        "edge": 7,
        "engine": "stablecoin-flow",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {
            "total": len(CHECKS),
            "passed": sum(1 for c in CHECKS if c["passed"]),
            "failed": sum(1 for c in CHECKS if not c["passed"]),
        },
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/903_stablecoin_flow.json", "w") as f:
        json.dump(report, f, indent=2)
    print("\n=== SUMMARY ===")
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
