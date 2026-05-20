"""
ops 981 - Tier-3 Retail-Edges FINAL VERIFIER (post credit-equity fix)
=====================================================================

Fixes the false-negative invoke failures from ops 978:
  - Uses botocore Config(read_timeout=900) so long-runtime engines
    (sympathetic-momentum 540s, gap-fill-confirm 600s) don't trip
    the boto3 default 60s read timeout.

Steps (per engine):
  1. lambda.get_function -> wait for LastUpdateStatus=Successful
  2. lambda.invoke (sync, RequestResponse, long timeout)
  3. s3.head_object on data/<engine>.json -> size, last_modified
  4. parse the S3 JSON -> state, signal_strength, error?, picks_n

Plus:
  5. Invoke signal-board to refresh; verify n_engines>=42, n_live>=42.
  6. Fetch retail-edges.html via S3; count 21 engine markers.

Pass criteria:
  - 6/6 engines: invoke ok=true, status_code=200, S3 size>500, no 'error' key, state in valid set
  - signal-board: n_engines>=42, n_live>=42, n_stale==0
  - page: ok=true, markers>=21
"""
import json
import os
import sys
import time
import traceback
import urllib.request
import urllib.error
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"

ENGINES = [
    {"name": "justhodl-vvix-vov-regime",            "s3_key": "data/vvix-vov-regime.json",
     "valid_states": {"VEGA_RICH", "VEGA_CHEAP", "NEUTRAL", "QUIET"}},
    {"name": "justhodl-sympathetic-momentum",        "s3_key": "data/sympathetic-momentum.json",
     "valid_states": {"CATCHUP_RICH", "ACTIVE", "NORMAL", "QUIET"}},
    {"name": "justhodl-insider-buyback-confluence",  "s3_key": "data/insider-buyback-confluence.json",
     "valid_states": {"CONFLUENCE_RICH", "ACTIVE", "NORMAL", "QUIET"}},
    {"name": "justhodl-gap-fill-confirm",            "s3_key": "data/gap-fill-confirm.json",
     "valid_states": {"CONTINUATION_RICH", "ACTIVE", "NORMAL", "QUIET"}},
    {"name": "justhodl-13f-price-divergence",        "s3_key": "data/13f-price-divergence.json",
     "valid_states": {"BULLISH", "BEARISH", "MIXED", "NORMAL", "QUIET"}},
    {"name": "justhodl-credit-equity-divergence",    "s3_key": "data/credit-equity-divergence.json",
     "valid_states": {"CREDIT_BULL_RICH", "CREDIT_BEAR_RICH", "NEUTRAL", "QUIET"}},
]

SIGNAL_BOARD_FN = "justhodl-signal-board"
RETAIL_EDGES_KEY = "retail-edges.html"

EXPECTED_MARKERS = [
    "earnings-iv-crush", "stealth-accumulation", "failed-pattern-reversal",
    "squeeze-pretrigger", "catalyst-skew-premove", "crypto-etf-arb", "lockup-expiration",
    "precatalyst-vol-expansion", "cef-discount", "reit-nav-discount", "divcut-warning",
    "rating-change-cluster", "multi-tf-convergence", "52wk-quality-breakout", "spac-floor-warrant",
    "vvix-vov-regime", "sympathetic-momentum", "insider-buyback-confluence",
    "gap-fill-confirm", "13f-price-divergence", "credit-equity-divergence",
]

REPO_ROOT = Path(__file__).resolve().parents[3]
REPORTS_DIR = REPO_ROOT / "aws" / "ops" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
REPORT_PATH = REPORTS_DIR / "981.json"


def wait_for_settled(lambda_c, name, max_wait=180):
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            cfg = lambda_c.get_function_configuration(FunctionName=name)
            status = cfg.get("LastUpdateStatus")
            state = cfg.get("State")
            if status in ("Successful", None) and state == "Active":
                return {"ok": True, "status": status, "state": state}
            if status == "Failed":
                return {"ok": False, "status": status, "reason": cfg.get("LastUpdateStatusReason")}
        except ClientError as e:
            return {"ok": False, "error": str(e)}
        time.sleep(3)
    return {"ok": False, "error": "wait_timeout"}


def invoke_engine(lambda_c, name):
    try:
        r = lambda_c.invoke(FunctionName=name, InvocationType="RequestResponse", Payload=b"{}")
        sc = r["StatusCode"]
        body = r["Payload"].read().decode("utf-8", errors="ignore")
        fe = r.get("FunctionError")
        return {
            "ok": (sc == 200 and not fe),
            "status_code": sc,
            "function_error": fe,
            "body_preview": body[:400],
        }
    except Exception as e:
        return {"ok": False, "status_code": None, "error": str(e)[:300]}


def verify_s3(s3, key, valid_states):
    try:
        head = s3.head_object(Bucket=S3_BUCKET, Key=key)
        size = head["ContentLength"]
        body = s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
        obj = json.loads(body)
        state = obj.get("state")
        err = obj.get("error")
        picks_n = len(obj.get("picks") or obj.get("setups") or obj.get("divergences")
                      or obj.get("warnings") or obj.get("regimes") or [])
        ok = (size > 500 and not err and state in valid_states)
        return {
            "ok": ok,
            "size_bytes": size,
            "last_modified": head["LastModified"].isoformat(),
            "state": state,
            "signal_strength": obj.get("signal_strength"),
            "picks_n": picks_n,
            "error_in_payload": err,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def verify_signal_board(lambda_c, s3):
    out = {}
    out["wait"] = wait_for_settled(lambda_c, SIGNAL_BOARD_FN)
    inv = invoke_engine(lambda_c, SIGNAL_BOARD_FN)
    out["invoke"] = inv
    time.sleep(3)
    try:
        body = s3.get_object(Bucket=S3_BUCKET, Key="data/signal-board.json")["Body"].read()
        obj = json.loads(body)
        n_engines = obj.get("n_engines")
        n_live = obj.get("n_live")
        n_stale = obj.get("n_stale")
        out["verify"] = {
            "ok": (n_engines is not None and n_engines >= 42 and n_live >= 42 and n_stale == 0),
            "n_engines": n_engines,
            "n_live": n_live,
            "n_stale": n_stale,
            "composite_posture": obj.get("composite_posture"),
            "composite_signal": obj.get("composite_signal"),
            "expects_42": True,
        }
    except Exception as e:
        out["verify"] = {"ok": False, "error": str(e)[:300]}
    return out


def verify_page(s3):
    try:
        body = s3.get_object(Bucket=S3_BUCKET, Key=RETAIL_EDGES_KEY)["Body"].read().decode("utf-8", errors="ignore")
        found = sum(1 for m in EXPECTED_MARKERS if m in body)
        return {
            "ok": (found >= len(EXPECTED_MARKERS) and len(body) > 30000),
            "size": len(body),
            "markers_found": found,
            "expected_markers": len(EXPECTED_MARKERS),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)[:300]}


def main():
    report = {"started_at": int(time.time()), "engines": {}}
    try:
        # Long-timeout config so we don't false-fail on 540s/600s engines
        long_cfg = Config(read_timeout=900, connect_timeout=15, retries={"max_attempts": 0})
        lambda_c = boto3.client("lambda", region_name=REGION, config=long_cfg)
        s3 = boto3.client("s3", region_name=REGION)

        for eng in ENGINES:
            name = eng["name"]
            print(f"\n=== {name} ===")
            engr = {"name": name}

            print("  waiting for settled...")
            engr["wait"] = wait_for_settled(lambda_c, name)
            print(f"  wait: {engr['wait']}")

            print("  invoking (long timeout)...")
            engr["invoke"] = invoke_engine(lambda_c, name)
            print(f"  invoke: ok={engr['invoke'].get('ok')} status={engr['invoke'].get('status_code')} "
                  f"err={engr['invoke'].get('function_error') or engr['invoke'].get('error')}")
            if engr["invoke"].get("body_preview"):
                print(f"  body: {engr['invoke']['body_preview'][:280]}")
            time.sleep(2)

            engr["s3"] = verify_s3(s3, eng["s3_key"], eng["valid_states"])
            print(f"  s3: {engr['s3']}")

            report["engines"][name] = engr

        # Signal-board re-verify (already at 42 from prior runs, just confirm health)
        print("\n=== signal-board verify ===")
        report["signal_board"] = verify_signal_board(lambda_c, s3)
        print(f"  sb: {report['signal_board']}")

        # Page verify
        print("\n=== retail-edges.html verify ===")
        report["page"] = verify_page(s3)
        print(f"  page: {report['page']}")

        # Scorecard
        n_inv = sum(1 for e in report["engines"].values() if e["invoke"].get("ok"))
        n_s3 = sum(1 for e in report["engines"].values() if e["s3"].get("ok"))
        sb_ok = report["signal_board"]["verify"].get("ok", False)
        page_ok = report["page"].get("ok", False)
        report["scorecard"] = {
            "n_engines": len(ENGINES),
            "n_invoke_ok": n_inv,
            "n_s3_ok": n_s3,
            "signal_board_42_ok": sb_ok,
            "page_ok": page_ok,
            "all_pass": (n_inv == 6 and n_s3 == 6 and sb_ok and page_ok),
        }
    except Exception as e:
        report["fatal"] = str(e)
        report["traceback"] = traceback.format_exc()
    finally:
        REPORT_PATH.write_text(json.dumps(report, indent=2, default=str))
        print("\n=== FINAL REPORT ===")
        print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
