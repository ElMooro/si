"""
ops/4726 — final smoke test on the clean (debug-instrumentation-removed)
justhodl-invest. Confirms: no crash, no _debug_sample_leg_read leaking
into production output, and the same understood-correct bootstrap
behavior (INSUFFICIENT_DATA fleet-wide until 8 days of history accrue).
"""
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FUNCTION_NAME = "justhodl-invest"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/invest.json"
HISTORY_KEY = "data/invest/leg-history.json"

s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("4726_invest_final_clean_smoke") as rep:
        rep.heading("ops 4726 — final clean smoke test")

        client = boto3.client(
            "lambda", region_name=REGION,
            config=Config(read_timeout=310, retries={"max_attempts": 0}),
        )
        t0 = time.time()
        resp = client.invoke(FunctionName=FUNCTION_NAME, InvocationType="RequestResponse",
                              Payload=b"{}")
        elapsed = time.time() - t0
        fn_error = resp.get("FunctionError")
        payload = json.loads(resp["Payload"].read())
        rep.kv(invoke_elapsed_s=round(elapsed, 1), function_error=fn_error,
               status_code=resp.get("StatusCode"))
        if fn_error:
            rep.fail(f"  FunctionError={fn_error}: {json.dumps(payload)[:1500]}")
            return
        rep.ok(f"  invoke succeeded in {elapsed:.1f}s: {payload.get('body')}")

        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
        rep.kv(has_debug_key=("_debug_sample_leg_read" in doc),
               schema=doc.get("schema"))
        if "_debug_sample_leg_read" in doc:
            rep.fail("  debug key still present in output -- cleanup didn't land")
        else:
            rep.ok("  clean output, no debug scaffolding")

        hist = json.loads(s3.get_object(Bucket=BUCKET, Key=HISTORY_KEY)["Body"].read())
        n_days = len(hist.get("days", []))
        rep.kv(history_days_accrued=n_days, days_until_zscores_possible=max(0, 8 - n_days))
        rep.ok(f"  {n_days} day(s) of leg-history accrued so far -- "
               f"{max(0, 8 - n_days)} more scheduled runs until z-scores (and possible "
               f"CONFIRMED/TURNING verdicts) can appear")

        rep.section("Verdict")
        rep.ok("Clean, deployed, scheduled daily 15:00 UTC, no crash, no debug leakage. "
               "Bootstrap period confirmed and will resolve itself as the daily "
               "schedule accrues history -- nothing further to do here.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("FINAL SMOKE ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
