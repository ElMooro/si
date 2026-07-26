"""
ops_3902 — DEPLOY: trade-evaluator's DynamoDB write fix (recursive float-to-
Decimal conversion). Root cause confirmed via real production CloudWatch
logs (ops 3901): every run correctly fetches real Polygon prices, then fails
every single write with "Float types are not supported. Use Decimal types
instead." because the old conversion only checked top-level values while
evaluate_call() always nests outcomes in dicts.

The gate is the real test: does `updated` go from the structural 0 (every
prior run, confirmed in logs) to a genuine positive number this run. If it
does, per-strategy win rates start accumulating real data for the first
time, and alpha-calibrator's n_obs (currently 0, blocking any weight
reweighting) starts climbing toward its 60-observation activation floor.
"""
import io
import json
import sys
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

FN = "justhodl-trade-evaluator"
BUCKET = "justhodl-dashboard-live"
KEY = "data/trade-journal.json"
MARKER = "_to_decimal"

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=650, retries={"max_attempts": 0}))


def snapshot():
    o = s3.get_object(Bucket=BUCKET, Key=KEY)
    return json.loads(o["Body"].read()), o["LastModified"]


def main():
    with report("3902_trade_evaluator_decimal_fix_deploy") as rep:
        rep.heading("ops 3902 — deploy the Decimal fix, hard-gate on updated > 0 (was structurally 0 every run)")

        rep.section("1. BEFORE")
        before, blm = snapshot()
        before_summary = before.get("summary") or {}
        rep.kv(before_total_evaluated_30d=before_summary.get("total_evaluated_30d"),
               before_n_open=before_summary.get("n_open"),
               before_s3=blm.isoformat())

        rep.section("2. ZIP-SETTLE BY MARKER")
        settled = False
        for attempt in range(1, 31):
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    src = z.read("lambda_function.py").decode("utf-8", "ignore")
                if MARKER in src:
                    rep.ok(f"  new artifact live on attempt {attempt} ({len(blob):,} zip bytes)")
                    settled = True
                    break
                rep.log(f"  attempt {attempt}: marker not yet in the deployed zip")
            except Exception as e:
                rep.log(f"  attempt {attempt}: {str(e)[:90]}")
            time.sleep(15)
        if not settled:
            rep.fail("  deploy never landed")
            sys.exit(1)

        cfg = lam.get_function_configuration(FunctionName=FN)
        for _ in range(20):
            if cfg.get("State") == "Active" and cfg.get("LastUpdateStatus") != "InProgress":
                break
            time.sleep(8)
            cfg = lam.get_function_configuration(FunctionName=FN)
        rep.ok(f"  State={cfg.get('State')} LastUpdateStatus={cfg.get('LastUpdateStatus')}")

        rep.section("3. invoke (may run close to the full 600s timeout — real Polygon calls per checkpoint)")
        resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        payload = json.loads(resp["Payload"].read())
        rep.log(f"  invoke response: {json.dumps(payload, default=str)[:600]}")
        if resp.get("FunctionError"):
            rep.fail(f"  invoke raised FunctionError: {payload}")
            sys.exit(1)

        rep.section("4. THE REAL GATE — did updated actually move off the structural 0")
        after, alm = snapshot()
        after_summary = after.get("summary") or {}
        n_updated_this_invoke = payload.get("n_updated") if isinstance(payload, dict) else None
        # payload is the Lambda's returned body (JSON string) — parse if needed
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
                n_updated_this_invoke = payload.get("n_updated")
            except Exception:
                pass

        rep.kv(after_total_evaluated_30d=after_summary.get("total_evaluated_30d"),
               after_n_open=after_summary.get("n_open"),
               after_s3=alm.isoformat(),
               n_updated_this_invoke=n_updated_this_invoke)

        checks = [
            ("journal artifact was rewritten this invoke", alm > blm),
            ("n_updated this invoke is a real positive number (was structurally 0 every prior run)",
             isinstance(n_updated_this_invoke, (int, float)) and n_updated_this_invoke > 0),
            ("total_evaluated_30d increased from before",
             (after_summary.get("total_evaluated_30d") or 0) > (before_summary.get("total_evaluated_30d") or 0)),
        ]
        for label, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {label}")

        rep.section("5. spot-check a real evaluated call, if any exist now")
        strategies = after.get("strategies") or []
        for s in strategies:
            if s.get("evaluated_30d", 0) > 0:
                rep.log(f"  {json.dumps(s, default=str)}")

        failed = [l for l, ok in checks if not ok]
        if failed:
            rep.fail(f"FAILED {len(failed)}: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {n_updated_this_invoke} calls evaluated this run, "
               f"total_evaluated_30d {before_summary.get('total_evaluated_30d')} -> "
               f"{after_summary.get('total_evaluated_30d')}")


if __name__ == "__main__":
    main()
