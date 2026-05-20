"""
ops 906 -- verify Edge #10: RV-IV Single-Stock Scanner
=======================================================

Lambda: justhodl-rv-iv-scanner
Output: s3://justhodl-dashboard-live/data/rv-iv-scanner.json
Page:   https://justhodl.ai/rv-iv-scanner.html
SSM:    /justhodl/rv-iv-scanner/state
"""

import datetime as dt
import json
import os
import sys
import time
import urllib.request
import urllib.error

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
PAGES_BASE = "https://justhodl.ai"

FN = "justhodl-rv-iv-scanner"
S3_KEY = "data/rv-iv-scanner.json"
PAGE = "rv-iv-scanner.html"
SSM_KEY = "/justhodl/rv-iv-scanner/state"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=620, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

CHECKS = []


def add(name, passed, detail=""):
    CHECKS.append({"name": name, "passed": bool(passed), "detail": str(detail)[:300]})


def main():
    print(f"ops 906 -- verify Edge #10 RV-IV scanner at {dt.datetime.utcnow().isoformat()}Z")

    # 1. Lambda existence + config
    try:
        info = lam.get_function(FunctionName=FN)
        cfg = info.get("Configuration", {})
        add("lambda_deployed", True, f"arn={cfg.get('FunctionArn', '')[-50:]}")
        add("runtime_python312", cfg.get("Runtime") == "python3.12", cfg.get("Runtime"))
        add("memory_sufficient", cfg.get("MemorySize", 0) >= 512,
            f"mem={cfg.get('MemorySize')}MB")
        add("timeout_sufficient", cfg.get("Timeout", 0) >= 300,
            f"timeout={cfg.get('Timeout')}s")
    except ClientError as e:
        add("lambda_deployed", False, str(e))
        write_report()
        return

    # 2. Invoke (long run -- 60 names FMP)
    print("invoking rv-iv-scanner (60-name FMP scan, ~2-7 min)...")
    t0 = time.time()
    try:
        r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        payload = r["Payload"].read().decode()
        dur = round(time.time() - t0, 1)
        ok = r["StatusCode"] == 200 and not r.get("FunctionError")
        add("invoke_success", ok,
            f"dur={dur}s status={r['StatusCode']} err={r.get('FunctionError')} "
            f"body={payload[:200]}")
    except ClientError as e:
        add("invoke_success", False, str(e))
        write_report()
        return

    # 3. S3 output
    try:
        head = s3.head_object(Bucket=S3_BUCKET, Key=S3_KEY)
        add("s3_output_present", True, f"size={head['ContentLength']}B")
    except ClientError as e:
        add("s3_output_present", False, str(e))
        write_report()
        return

    d = None
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        d = json.loads(obj["Body"].read())
        add("s3_output_parseable", True, f"top_keys={list(d.keys())[:5]}")
    except Exception as e:
        add("s3_output_parseable", False, str(e))
        write_report()
        return

    # 4. Full canonical schema
    required = [
        "engine", "version", "as_of", "state", "previous_state",
        "state_transition", "state_description", "signal_strength",
        "universe_size", "n_scanned", "n_with_vrp",
        "vrp_mean", "vrp_spread", "n_earnings_within_10d",
        "top_20_iv_rich", "top_20_iv_cheap", "earnings_within_10d",
        "trigger_conditions", "forward_expectations",
        "recommended_trade", "historical_episodes",
        "why_now_explainer", "methodology", "sources", "schedule",
    ]
    missing = [k for k in required if k not in d]
    add("schema_complete", len(missing) == 0,
        "all_present" if not missing else f"missing={missing}")
    add("engine_id", d.get("engine") == "rv-iv-scanner", d.get("engine"))

    valid_states = ("LOW_DISPERSION", "NORMAL", "HIGH_DISPERSION",
                    "EARNINGS_SEASON", "INSUFFICIENT_DATA")
    add("state_valid", d.get("state") in valid_states, d.get("state"))

    add("n_scanned_sane", d.get("n_scanned", 0) >= 20,
        f"n_scanned={d.get('n_scanned')}")
    add("n_with_vrp_sane", d.get("n_with_vrp", 0) >= 5,
        f"n_with_vrp={d.get('n_with_vrp')}")

    rich = d.get("top_20_iv_rich", [])
    add("top_iv_rich_list", isinstance(rich, list),
        f"n={len(rich) if isinstance(rich, list) else 'NA'}")
    cheap = d.get("top_20_iv_cheap", [])
    add("top_iv_cheap_list", isinstance(cheap, list),
        f"n={len(cheap) if isinstance(cheap, list) else 'NA'}")

    if isinstance(rich, list) and rich:
        sample = rich[0]
        ok = (isinstance(sample, dict) and "ticker" in sample
              and "vrp" in sample and "iv_30d" in sample)
        add("iv_rich_row_structure", ok,
            f"keys={list(sample.keys())[:7]}" if isinstance(sample, dict) else "not-dict")

    fe = d.get("forward_expectations", {})
    add("forward_horizons", all(h in fe for h in ("1m", "3m", "12m")),
        list(fe.keys()))

    tc = d.get("trigger_conditions", [])
    add("trigger_conditions_valid", isinstance(tc, list) and len(tc) >= 3,
        f"n={len(tc) if isinstance(tc, list) else 'NA'}")

    trade = d.get("recommended_trade", {})
    add("trade_ticket_present",
        isinstance(trade, dict) and "primary" in trade,
        f"keys={list(trade.keys())[:5]}" if isinstance(trade, dict) else "not-dict")

    why = d.get("why_now_explainer", "")
    add("why_now_explainer_long", isinstance(why, str) and len(why) > 200,
        f"len={len(why) if isinstance(why, str) else 0}")

    # 5. SSM
    try:
        p = ssm.get_parameter(Name=SSM_KEY)
        val = json.loads(p["Parameter"]["Value"])
        add("ssm_state_written", True, f"state={val.get('state', '?')}")
    except ClientError as e:
        add("ssm_state_written", False, str(e))

    # 6. Page live
    try:
        req = urllib.request.Request(f"{PAGES_BASE}/{PAGE}",
                                     headers={"User-Agent": "ops/906"})
        resp = urllib.request.urlopen(req, timeout=15)
        body = resp.read().decode("utf-8", errors="ignore")
        page_ok = resp.status == 200 and len(body) > 1000 and "rv-iv-scanner.json" in body
        add("page_live", page_ok,
            f"status={resp.status} len={len(body)} url_present={'rv-iv-scanner.json' in body}")
    except Exception as e:
        add("page_live", False, str(e))

    write_report()


def write_report():
    report = {
        "ops": 906, "edge": 10, "engine": "rv-iv-scanner",
        "run_at": dt.datetime.utcnow().isoformat() + "Z",
        "checks": CHECKS,
        "summary": {
            "total": len(CHECKS),
            "passed": sum(1 for c in CHECKS if c["passed"]),
            "failed": sum(1 for c in CHECKS if not c["passed"]),
        },
    }
    os.makedirs("aws/ops/reports", exist_ok=True)
    with open("aws/ops/reports/906_rv_iv_scanner.json", "w") as f:
        json.dump(report, f, indent=2)
    print("\n=== SUMMARY ===")
    print(json.dumps(report["summary"], indent=2))
    for c in CHECKS:
        print(f"  [{'OK ' if c['passed'] else 'FAIL'}] {c['name']:32} {c['detail'][:80]}")


if __name__ == "__main__":
    main()
