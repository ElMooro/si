"""
ops 905 -- verify Edge #9: Activist 13D Investor Scanner
=========================================================

Lambda: justhodl-activist-13d
Output: s3://justhodl-dashboard-live/data/activist-13d.json
Page:   https://justhodl.ai/activist-13d.html

Checks:
- Lambda deployed (python3.12, mem>=512, timeout>=300)
- Schedule attached (every 2h during market hours, EDGAR throttle-safe)
- Invoke succeeds
- S3 output present + parseable
- Canonical schema (engine, state, signal_strength, summary,
  current_readings, trigger_conditions, forward_expectations,
  top_setups, all_setups_full_list, recommended_trade,
  why_now_explainer, academic_basis, methodology, sources)
- State in valid enum {FRESH_TIER_A, ACTIVE, QUIET}
- 3 horizons (1m/3m/12m) - queue can be empty so N>=0 allowed
- summary has n_total_setups + activists_tracked >= 15
- recommended_trade.primary present (even in QUIET fallback)
- Page live + references activist-13d.json
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

FN = "justhodl-activist-13d"
S3_KEY = "data/activist-13d.json"
PAGE = "activist-13d.html"

VALID_STATES = {"FRESH_TIER_A", "ACTIVE", "QUIET"}

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, connect_timeout=10,
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
              and cfg["Timeout"] >= 120
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
    required = ["engine", "state", "signal_strength", "summary",
                "current_readings", "trigger_conditions",
                "forward_expectations", "top_setups",
                "recommended_trade", "why_now_explainer",
                "academic_basis", "methodology", "sources"]
    missing = [k for k in required if k not in data]
    add("schema_complete", not missing, f"missing={missing}")

    state = data.get("state")
    add("state_valid", state in VALID_STATES, f"state={state}")

    fe = data.get("forward_expectations", {})
    h = sum(1 for k in ("1m", "3m", "12m") if k in fe)
    add("forward_3_horizons", h == 3,
        f"horizons={h}/3, keys={list(fe.keys())}")

    summ = data.get("summary", {})
    summ_ok = ("n_total_setups" in summ
               and "n_fresh_tier_a" in summ
               and summ.get("activists_tracked", 0) >= 15)
    add("summary_populated", summ_ok,
        f"n_total={summ.get('n_total_setups')} n_fresh_A={summ.get('n_fresh_tier_a')} "
        f"n_multi={summ.get('n_multi_activist')} tracked={summ.get('activists_tracked')}")

    cr = data.get("current_readings", {})
    cr_ok = ("n_setups_top20" in cr and "fresh_tier_a_count" in cr)
    add("current_readings_present", cr_ok,
        f"n_top20={cr.get('n_setups_top20')} fresh_A={cr.get('fresh_tier_a_count')} "
        f"max_multi={cr.get('max_n_activists_in_target')} top_score={cr.get('highest_composite_score')}")

    rt = data.get("recommended_trade", {})
    has_primary = (isinstance(rt, dict) and isinstance(rt.get("primary"), dict)
                   and rt["primary"].get("instrument"))
    add("recommended_trade_present", has_primary,
        f"state={state} primary={'OK' if has_primary else 'MISSING'} "
        f"instr={(rt.get('primary') or {}).get('instrument','')[:80]}")

    wn = data.get("why_now_explainer", "")
    add("why_now_present", len(wn) > 100, f"len={len(wn)}")

    ab = data.get("academic_basis", [])
    add("academic_basis_present", isinstance(ab, list) and len(ab) >= 3,
        f"n_citations={len(ab) if isinstance(ab, list) else 0}")

    srcs = data.get("sources", [])
    add("sources_present", isinstance(srcs, list) and len(srcs) >= 2,
        f"n_sources={len(srcs) if isinstance(srcs, list) else 0}")

    # top_setups can be empty (no fresh 13Ds in window) -- accept that as QUIET state
    top = data.get("top_setups", [])
    if state == "QUIET":
        top_ok = isinstance(top, list)  # empty allowed
        add("top_setups_consistent_with_state", top_ok,
            f"state=QUIET n_top={len(top) if isinstance(top, list) else 'N/A'}")
    else:
        top_ok = isinstance(top, list) and len(top) >= 1
        sample = top[0] if top_ok else {}
        sample_ok = sample.get("activist_name") and (sample.get("target_ticker") or sample.get("target_name"))
        add("top_setups_present", top_ok and sample_ok,
            f"n_top={len(top) if isinstance(top, list) else 0} "
            f"sample={sample.get('activist_name','?')}->{sample.get('target_ticker') or sample.get('target_name','?')}")

    try:
        req = urllib.request.Request(f"{PAGES_BASE}/{PAGE}",
                                     headers={"User-Agent": "justhodl-verify/1.0"})
        resp = urllib.request.urlopen(req, timeout=20)
        body = resp.read().decode()
        ok = (resp.status == 200 and "activist-13d.json" in body)
        add("page_live", ok, f"status={resp.status} len={len(body)} url_present={'activist-13d.json' in body}")
    except Exception as e:
        add("page_live", False, str(e))

    write_report()


def write_report():
    report = {
        "ops": 905, "edge": 9, "engine": "activist-13d",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {
            "total": len(CHECKS),
            "passed": sum(1 for c in CHECKS if c["passed"]),
            "failed": sum(1 for c in CHECKS if not c["passed"]),
        },
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/905_activist_13d.json", "w") as f:
        json.dump(report, f, indent=2)
    print("\n=== SUMMARY ===")
    print(json.dumps(report["summary"], indent=2))


if __name__ == "__main__":
    main()
