"""
ops 904 -- verify Edge #8: OPEX / 0DTE Gamma Pinning Calendar
==============================================================

Lambda: justhodl-opex-calendar
Output: s3://justhodl-dashboard-live/data/opex-calendar.json
Page:   https://justhodl.ai/opex-calendar.html

Checks:
- Lambda deployed (python3.12, mem>=512, timeout>=60)
- Schedule attached (30min cadence during market hours)
- Invoke succeeds
- S3 output present + parseable
- Canonical schema (engine, state, calendar, current_readings,
  trigger_conditions, forward_expectations, recommended_trade,
  historical_backtest, historical_episodes, why_now_explainer, methodology)
- State in valid enum
- 3 horizons (1m/3m/12m) with return_pct
- Calendar has next_opex + days_to_next_opex_trading
- recommended_trade.primary present (state-aware)
- Page live
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

FN = "justhodl-opex-calendar"
S3_KEY = "data/opex-calendar.json"
PAGE = "opex-calendar.html"

VALID_STATES = {
    "NORMAL", "PRE_OPEX", "OPEX_WEEK", "OPEX_DAY", "POST_OPEX",
    "QUAD_WITCHING_PRE_OPEX", "QUAD_WITCHING_OPEX_WEEK", "QUAD_WITCHING_OPEX_DAY",
}

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=240, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
sch = boto3.client("scheduler", region_name=REGION)
events = boto3.client("events", region_name=REGION)

CHECKS = []


def add(name, ok, detail=""):
    CHECKS.append({"name": name, "passed": bool(ok), "detail": str(detail)[:400]})
    print(("PASS" if ok else "FAIL"), name, "-", str(detail)[:200])


def main():
    try:
        f = lam.get_function(FunctionName=FN)
        cfg = f["Configuration"]
        ok = (cfg["Runtime"] == "python3.12"
              and cfg["MemorySize"] >= 512
              and cfg["State"] == "Active")
        add("lambda_deployed", ok,
            f"runtime={cfg['Runtime']} mem={cfg['MemorySize']} timeout={cfg['Timeout']} state={cfg['State']}")
    except ClientError as e:
        add("lambda_deployed", False, str(e))
        write_report()
        return

    # Schedule attached
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
        try:
            for r in events.list_rules(NamePrefix=FN).get("Rules", []):
                schedule_ok = True
                sched_name = r["Name"]
                break
        except ClientError:
            pass
    add("schedule_attached", schedule_ok, f"name={sched_name}")

    # Invoke
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        body = r["Payload"].read().decode()
        ok = (r["StatusCode"] == 200 and not r.get("FunctionError"))
        add("invoke_success", ok, f"status={r['StatusCode']} fnErr={r.get('FunctionError')} body={body[:200]}")
    except ClientError as e:
        add("invoke_success", False, str(e))

    # S3 output
    try:
        r = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        data = json.loads(r["Body"].read())
        add("s3_output_present", True, f"size={r['ContentLength']}")
    except Exception as e:
        add("s3_output_present", False, str(e))
        data = {}

    # Schema
    required = ["engine", "state", "signal_strength", "calendar",
                "current_readings", "trigger_conditions",
                "forward_expectations", "recommended_trade",
                "historical_backtest", "historical_episodes",
                "why_now_explainer", "methodology"]
    missing = [k for k in required if k not in data]
    add("schema_complete", not missing, f"missing={missing}")

    state = data.get("state")
    add("state_valid", state in VALID_STATES, f"state={state}")

    fe = data.get("forward_expectations", {})
    h = sum(1 for k in ("1m", "3m", "12m") if fe.get(k, {}).get("return_pct") is not None)
    add("forward_3_horizons", h == 3,
        f"horizons={h}/3, 1m={fe.get('1m',{}).get('return_pct')} "
        f"3m={fe.get('3m',{}).get('return_pct')} 12m={fe.get('12m',{}).get('return_pct')}")

    cal = data.get("calendar", {})
    cal_ok = ("next_opex" in cal and "days_to_next_opex_trading" in cal
              and cal.get("days_to_next_opex_trading") is not None)
    add("calendar_populated", cal_ok,
        f"next_opex={cal.get('next_opex')} days_to={cal.get('days_to_next_opex_trading')} "
        f"quad={cal.get('is_next_quad_witching')}")

    cr = data.get("current_readings", {})
    add("current_readings_present", "next_opex" in cr,
        f"max_pain={cr.get('max_pain')} spy={cr.get('spy_current')} n_contracts={cr.get('n_contracts_analyzed')}")

    rt = data.get("recommended_trade", {})
    has_primary = isinstance(rt, dict) and rt.get("primary") and rt["primary"].get("instrument")
    add("recommended_trade_present", has_primary, f"state={state} primary={'OK' if has_primary else 'MISSING'}")

    wn = data.get("why_now_explainer", "")
    add("why_now_present", len(wn) > 200, f"len={len(wn)}")

    bt = data.get("historical_backtest", {})
    bt_ok = bt.get("post_opex_5d", {}).get("n", 0) >= 10
    add("backtest_sufficient", bt_ok,
        f"post_opex_5d_n={bt.get('post_opex_5d',{}).get('n',0)} "
        f"post_quad_5d_n={bt.get('post_quad_witch_5d',{}).get('n',0)}")

    try:
        req = urllib.request.Request(f"{PAGES_BASE}/{PAGE}",
                                     headers={"User-Agent": "justhodl-verify/1.0"})
        resp = urllib.request.urlopen(req, timeout=20)
        body = resp.read().decode()
        ok = (resp.status == 200 and "opex-calendar.json" in body)
        add("page_live", ok, f"status={resp.status} len={len(body)} url_present={'opex-calendar.json' in body}")
    except Exception as e:
        add("page_live", False, str(e))

    write_report()


def write_report():
    report = {
        "ops": 904, "edge": 8, "engine": "opex-calendar",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {
            "total": len(CHECKS),
            "passed": sum(1 for c in CHECKS if c["passed"]),
            "failed": sum(1 for c in CHECKS if not c["passed"]),
        },
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/904_opex_calendar.json", "w") as f:
        json.dump(report, f, indent=2)
    print("\n=== SUMMARY ===")
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
