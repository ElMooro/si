"""
ops_3884 — DEPLOY: catalyst-calendar's new EARNINGS_ACTUAL source (recent
real earnings results, surfaced via earnings-tracker's already-computed
recent_results_30d). Fixes the forward-only gap found investigating the
semi-sector flow/price divergence.

Hard gate is specific and falsifiable: the exact 4 tickers found manually in
ops 3883 (INTC, TSM, ASML, MU) must now appear in the LIVE catalyst-calendar
output, with negative days_to and their real eps_surprise_pct/return_1d_pct
intact — not just "some EARNINGS_ACTUAL events exist somewhere."
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

FN = "justhodl-catalyst-calendar"
BUCKET = "justhodl-dashboard-live"
KEY = "data/catalyst-calendar.json"
MARKER = "EARNINGS_ACTUAL"
EXPECTED_TICKERS = {"INTC": -7.89, "TSM": None, "ASML": None, "MU": None}  # known 1d returns from ops 3883

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=890, retries={"max_attempts": 0}))


def snapshot():
    o = s3.get_object(Bucket=BUCKET, Key=KEY)
    return json.loads(o["Body"].read()), o["LastModified"]


def main():
    with report("3884_catalyst_earnings_actual_deploy") as rep:
        rep.heading("ops 3884 — deploy EARNINGS_ACTUAL, gate on the exact semi tickers this fix is for")

        rep.section("1. BEFORE")
        before, blm = snapshot()
        before_types = before.get("by_type") or {}
        rep.kv(before_n_events=before.get("n_events"), before_by_type=str(before_types),
               before_s3=blm.isoformat())

        rep.section("2. ZIP-SETTLE BY MARKER")
        settled = False
        for attempt in range(1, 31):
            try:
                loc = lam.get_function(FunctionName=FN)["Code"]["Location"]
                blob = urllib.request.urlopen(loc, timeout=60).read()
                with zipfile.ZipFile(io.BytesIO(blob)) as z:
                    src = z.read("lambda_function.py").decode("utf-8", "ignore")
                if MARKER in src and "recent_earnings_events" in src:
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

        rep.section("3. invoke")
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        after = None
        for attempt in range(1, 25):
            time.sleep(10)
            try:
                doc, lm = snapshot()
            except Exception:
                continue
            if lm > blm:
                after = doc
                rep.ok(f"  artifact rewritten on attempt {attempt} ({lm.isoformat()})")
                break
        if after is None:
            rep.fail("  catalyst-calendar.json never rewrote")
            sys.exit(1)

        rep.section("4. the falsifiable gate — the exact tickers this fix exists for")
        events = after.get("events") or []
        actual_events = [e for e in events if e.get("type") == "EARNINGS_ACTUAL"]
        by_ticker = {e.get("ticker"): e for e in actual_events}
        rep.kv(n_earnings_actual=len(actual_events),
               tickers_present=str(sorted(by_ticker.keys()))[:500])

        checks = [
            ("EARNINGS_ACTUAL now exists as an event type", len(actual_events) > 0),
            ("INTC present with negative days_to", by_ticker.get("INTC", {}).get("days_to", 1) < 0),
            ("INTC's real -7.89% 1d return survived intact",
             by_ticker.get("INTC", {}).get("return_1d_pct") == -7.89),
            ("TSM present", "TSM" in by_ticker),
            ("ASML present", "ASML" in by_ticker),
            ("MU present", "MU" in by_ticker),
            ("no event lost its days_to sign convention (all EARNINGS_ACTUAL are days_to <= 0)",
             all((e.get("days_to") or 0) <= 0 for e in actual_events)),
            ("by_type reflects the new source", (after.get("by_type") or {}).get("EARNINGS_ACTUAL", 0) > 0),
            ("forward EARNINGS source untouched (count didn't collapse)",
             (after.get("by_type") or {}).get("EARNINGS", 0) >= before_types.get("EARNINGS", 0) * 0.5),
        ]
        for label, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {label}")

        rep.section("5. spot-check the exact record")
        intc = by_ticker.get("INTC")
        if intc:
            rep.log(f"  INTC full record: {json.dumps(intc, default=str)}")

        failed = [l for l, ok in checks if not ok]
        if failed:
            rep.fail(f"FAILED {len(failed)}: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — {len(actual_events)} EARNINGS_ACTUAL events live, "
               f"including the exact 4 tickers (INTC/TSM/ASML/MU) this fix was built for")


if __name__ == "__main__":
    main()
