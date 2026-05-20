"""
ops 979 - Tier-3 Retail-Edges PURE VERIFIER
============================================

No Lambda updates. No race conditions.
Just probes the current live state:

  1. For each of the 6 Tier-3 engines:
       a. lambda.get_function -> exists? state=Active? last update=Successful?
       b. lambda.invoke (sync) -> 200 + body
       c. scheduler.get_schedule -> scheduled?
       d. s3.head_object on data/<engine>.json -> exists + size?
       e. parse the S3 JSON -> extract state + signal_strength

  2. Redeploy signal-board ONCE (cleanly, with wait_for_settled), then invoke
     to confirm it ingests 42 engines (was 36 after Tier-2).

  3. Fetch retail-edges.html via S3 -> count engine markers (expect >= 21).

Writes report to aws/ops/reports/979.json. Stdout-dumps full report at end
for log visibility. Try/finally guarantees the file write even on partial
failure.
"""
import json
import os
import sys
import time
import traceback
import urllib.request
import zipfile
from io import BytesIO
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT = "857687956942"
S3_BUCKET = "justhodl-dashboard-live"

# 6 Tier-3 engines pushed in commit facf28e3
ENGINES = [
    {"name": "justhodl-vvix-vov-regime",            "s3_key": "data/vvix-vov-regime.json"},
    {"name": "justhodl-sympathetic-momentum",        "s3_key": "data/sympathetic-momentum.json"},
    {"name": "justhodl-insider-buyback-confluence",  "s3_key": "data/insider-buyback-confluence.json"},
    {"name": "justhodl-gap-fill-confirm",            "s3_key": "data/gap-fill-confirm.json"},
    {"name": "justhodl-13f-price-divergence",        "s3_key": "data/13f-price-divergence.json"},
    {"name": "justhodl-credit-equity-divergence",    "s3_key": "data/credit-equity-divergence.json"},
]

SIGNAL_BOARD_FN = "justhodl-signal-board"
RETAIL_EDGES_KEY = "retail-edges.html"

# 21 engine-class CSS markers expected in the upgraded hub (7 Tier-1 + 8 Tier-2 + 6 Tier-3)
EXPECTED_MARKERS = [
    # Tier-1
    "earnings-iv-crush", "stealth-accumulation", "failed-pattern-reversal",
    "squeeze-pretrigger", "catalyst-skew-premove", "crypto-etf-arb", "lockup-expiration",
    # Tier-2
    "precatalyst-vol-expansion", "cef-discount", "reit-nav-discount", "divcut-warning",
    "rating-change-cluster", "multi-tf-convergence", "52wk-quality-breakout", "spac-floor-warrant",
    # Tier-3
    "vvix-vov-regime", "sympathetic-momentum", "insider-buyback-confluence",
    "gap-fill-confirm", "13f-price-divergence", "credit-equity-divergence",
]

REPO_ROOT = Path(__file__).resolve().parents[3]
REPORTS_DIR = REPO_ROOT / "aws" / "ops" / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
REPORT_PATH = REPORTS_DIR / "979.json"


def wait_for_settled(lambda_c, name, max_wait=120):
    """Poll until Lambda LastUpdateStatus is Successful (or None == already settled)."""
    deadline = time.time() + max_wait
    while time.time() < deadline:
        try:
            cfg = lambda_c.get_function_configuration(FunctionName=name)
            status = cfg.get("LastUpdateStatus")
            if status in ("Successful", None):
                return True
            if status == "Failed":
                return False
        except ClientError:
            pass
        time.sleep(2)
    return False


def probe_engine(eng, lambda_c, scheduler_c, s3_c):
    name = eng["name"]
    s3_key = eng["s3_key"]
    out = {"name": name}

    # 1. Lambda exists?
    try:
        fn = lambda_c.get_function(FunctionName=name)
        cfg = fn["Configuration"]
        out["lambda"] = {
            "exists": True,
            "state": cfg.get("State"),
            "last_update_status": cfg.get("LastUpdateStatus"),
            "runtime": cfg.get("Runtime"),
            "memory": cfg.get("MemorySize"),
            "timeout": cfg.get("Timeout"),
            "last_modified": cfg.get("LastModified"),
        }
        out["lambda"]["ok"] = (
            cfg.get("State") == "Active" and
            cfg.get("LastUpdateStatus") in ("Successful", None)
        )
    except ClientError as e:
        out["lambda"] = {"exists": False, "error": str(e), "ok": False}
        return out

    # 2. Invoke (sync)
    try:
        r = lambda_c.invoke(FunctionName=name, InvocationType="RequestResponse",
                            Payload=b"{}")
        payload = r["Payload"].read().decode("utf-8", "replace")
        fn_err = r.get("FunctionError")
        body_preview = payload[:300]
        out["invoke"] = {
            "ok": (r["StatusCode"] == 200 and not fn_err),
            "status_code": r["StatusCode"],
            "function_error": fn_err,
            "body_preview": body_preview,
        }
    except ClientError as e:
        out["invoke"] = {"ok": False, "error": str(e)}

    # 3. Scheduler entry?
    sched_name = name.replace("justhodl-", "schedule-")
    try:
        sched = scheduler_c.get_schedule(Name=sched_name)
        out["schedule"] = {
            "ok": sched.get("State") == "ENABLED",
            "state": sched.get("State"),
            "expression": sched.get("ScheduleExpression"),
        }
    except ClientError as e:
        # Try alternate scheduler name pattern
        try:
            sched = scheduler_c.get_schedule(Name=name)
            out["schedule"] = {
                "ok": sched.get("State") == "ENABLED",
                "state": sched.get("State"),
                "expression": sched.get("ScheduleExpression"),
            }
        except ClientError:
            out["schedule"] = {"ok": False, "error": str(e)}

    # 4. S3 head + parse
    try:
        head = s3_c.head_object(Bucket=S3_BUCKET, Key=s3_key)
        size = head["ContentLength"]
        out["s3"] = {"exists": True, "size_bytes": size,
                     "last_modified": str(head["LastModified"])}

        obj = s3_c.get_object(Bucket=S3_BUCKET, Key=s3_key)
        body = obj["Body"].read().decode("utf-8", "replace")
        try:
            data = json.loads(body)
            out["s3"]["state"] = data.get("state")
            out["s3"]["signal_strength"] = data.get("signal_strength")
            out["s3"]["as_of"] = data.get("as_of")
            n_picks = (len(data.get("picks", [])) +
                       len(data.get("setups", [])) +
                       len(data.get("divergences", [])) +
                       len(data.get("warnings", [])))
            out["s3"]["n_picks"] = n_picks
            out["s3"]["ok"] = bool(data.get("state"))
        except json.JSONDecodeError as e:
            out["s3"]["parse_error"] = str(e)
            out["s3"]["ok"] = False
    except ClientError as e:
        out["s3"] = {"exists": False, "error": str(e), "ok": False}

    return out


def redeploy_signal_board(lambda_c):
    """Build zip from local source and update the signal-board Lambda."""
    src_dir = REPO_ROOT / "aws" / "lambdas" / SIGNAL_BOARD_FN / "source"
    if not src_dir.exists():
        return {"ok": False, "error": f"source dir missing: {src_dir}"}

    # Build zip in memory
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(src_dir):
            for f in files:
                full = Path(root) / f
                arc = full.relative_to(src_dir)
                zf.write(full, str(arc))
    buf.seek(0)
    zip_bytes = buf.read()

    try:
        # Wait if previously in-flight
        wait_for_settled(lambda_c, SIGNAL_BOARD_FN)
        lambda_c.update_function_code(FunctionName=SIGNAL_BOARD_FN, ZipFile=zip_bytes)
        if not wait_for_settled(lambda_c, SIGNAL_BOARD_FN):
            return {"ok": False, "error": "did not settle after code update"}
        return {"ok": True, "zip_size": len(zip_bytes)}
    except ClientError as e:
        return {"ok": False, "error": str(e)}


def main():
    report = {
        "ops_id": "979",
        "purpose": "Tier-3 retail-edges pure verification (no race conditions)",
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "engines": {},
        "signal_board": {},
        "page": {},
        "scorecard": {},
    }

    lambda_c = boto3.client("lambda", region_name=REGION)
    scheduler_c = boto3.client("scheduler", region_name=REGION)
    s3_c = boto3.client("s3", region_name=REGION)

    try:
        # ---- 1. Probe each engine ----
        for eng in ENGINES:
            print(f"=== Probing {eng['name']} ===", flush=True)
            try:
                report["engines"][eng["name"]] = probe_engine(
                    eng, lambda_c, scheduler_c, s3_c)
            except Exception as e:
                report["engines"][eng["name"]] = {
                    "name": eng["name"], "error": str(e),
                    "traceback": traceback.format_exc()[:1500]}

        # ---- 2. Signal-board redeploy + invoke ----
        print(f"=== Redeploying {SIGNAL_BOARD_FN} ===", flush=True)
        deploy = redeploy_signal_board(lambda_c)
        report["signal_board"]["deploy"] = deploy

        print(f"=== Invoking {SIGNAL_BOARD_FN} ===", flush=True)
        try:
            r = lambda_c.invoke(FunctionName=SIGNAL_BOARD_FN,
                                InvocationType="RequestResponse", Payload=b"{}")
            payload = r["Payload"].read().decode("utf-8", "replace")
            fn_err = r.get("FunctionError")
            report["signal_board"]["invoke"] = {
                "ok": (r["StatusCode"] == 200 and not fn_err),
                "status_code": r["StatusCode"],
                "function_error": fn_err,
                "body_preview": payload[:500],
            }
            try:
                outer = json.loads(payload)
                body = json.loads(outer.get("body", "{}")) if isinstance(outer.get("body"), str) else outer.get("body", {})
                report["signal_board"]["verify"] = {
                    "n_engines": body.get("n_engines"),
                    "n_live": body.get("n_live"),
                    "n_stale": body.get("n_stale"),
                    "composite_posture": body.get("composite_posture"),
                    "composite_signal": body.get("composite_signal"),
                    "expects_42": (body.get("n_live", 0) == 42 or body.get("n_engines", 0) == 42),
                    "ok": body.get("n_live", 0) >= 42 or body.get("n_engines", 0) >= 42,
                }
            except (json.JSONDecodeError, AttributeError) as e:
                report["signal_board"]["verify"] = {"parse_error": str(e), "ok": False}
        except ClientError as e:
            report["signal_board"]["invoke"] = {"ok": False, "error": str(e)}

        # ---- 3. Page verify ----
        print(f"=== Fetching {RETAIL_EDGES_KEY} ===", flush=True)
        try:
            obj = s3_c.get_object(Bucket=S3_BUCKET, Key=RETAIL_EDGES_KEY)
            html = obj["Body"].read().decode("utf-8", "replace")
            markers = sum(1 for m in EXPECTED_MARKERS if m in html)
            report["page"] = {
                "ok": markers >= 21,
                "size": len(html),
                "markers_found": markers,
                "expected_markers": len(EXPECTED_MARKERS),
                "missing": [m for m in EXPECTED_MARKERS if m not in html],
            }
        except ClientError as e:
            report["page"] = {"ok": False, "error": str(e)}

        # ---- Scorecard ----
        eng_results = report["engines"]
        n_eng_ok = sum(1 for e in eng_results.values()
                       if e.get("lambda", {}).get("ok") and
                          e.get("invoke", {}).get("ok") and
                          e.get("s3", {}).get("ok"))
        n_sched_ok = sum(1 for e in eng_results.values() if e.get("schedule", {}).get("ok"))

        report["scorecard"] = {
            "n_engines": len(ENGINES),
            "n_engine_full_ok": n_eng_ok,
            "n_schedule_ok": n_sched_ok,
            "signal_board_deploy_ok": deploy.get("ok", False),
            "signal_board_verify_ok": report["signal_board"].get("verify", {}).get("ok", False),
            "n_signal_board_engines": report["signal_board"].get("verify", {}).get("n_engines"),
            "page_ok": report["page"].get("ok", False),
            "page_markers": report["page"].get("markers_found"),
            "all_pass": (
                n_eng_ok == len(ENGINES) and
                report["signal_board"].get("verify", {}).get("ok", False) and
                report["page"].get("ok", False)
            ),
        }
    except Exception as e:
        report["fatal_error"] = str(e)
        report["fatal_traceback"] = traceback.format_exc()
    finally:
        report["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(REPORT_PATH, "w") as f:
            json.dump(report, f, indent=2, default=str)
        # Stdout dump for log visibility
        print("\n" + "=" * 60)
        print(f"OPS 979 REPORT (full JSON below)")
        print("=" * 60)
        print(json.dumps(report, indent=2, default=str))

    sys.exit(0 if report.get("scorecard", {}).get("all_pass") else 1)


if __name__ == "__main__":
    main()
