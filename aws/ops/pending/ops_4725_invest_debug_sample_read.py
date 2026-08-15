"""
ops/4725 — invoke the debug-instrumented justhodl-invest and read back
_debug_sample_leg_read from data/invest.json. Everything reproduced
outside the Lambda (ops 4721/4723: direct import, exact extracted zip)
resolves this exact leg to 47.96. If the in-Lambda capture shows
something different at doc_is_none / dig_result / read_leg_value_result,
that's the precise point of divergence.
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

s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("4725_invest_debug_sample_read") as rep:
        rep.heading("ops 4725 — invoke + read _debug_sample_leg_read")

        client = boto3.client(
            "lambda", region_name=REGION,
            config=Config(read_timeout=310, retries={"max_attempts": 0}),
        )
        rep.section("Invoke")
        t0 = time.time()
        resp = client.invoke(FunctionName=FUNCTION_NAME, InvocationType="RequestResponse",
                              Payload=b"{}")
        elapsed = time.time() - t0
        payload_raw = resp["Payload"].read()
        fn_error = resp.get("FunctionError")
        rep.kv(invoke_elapsed_s=round(elapsed, 1), status_code=resp.get("StatusCode"),
               function_error=fn_error)
        try:
            payload = json.loads(payload_raw)
        except Exception:
            payload = payload_raw[:3000].decode("utf-8", errors="replace")
        if fn_error:
            rep.fail(f"  invoke returned FunctionError={fn_error}")
            rep.log(f"  payload: {json.dumps(payload)[:3000] if isinstance(payload, dict) else payload}")
            return
        rep.ok(f"  invoke succeeded in {elapsed:.1f}s")
        body = payload.get("body") if isinstance(payload, dict) else None
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except Exception:
                pass
        rep.log(f"  handler response body: {json.dumps(body)[:1000] if isinstance(body, dict) else body}")

        rep.section("data/invest.json _debug_sample_leg_read")
        raw = s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read()
        doc = json.loads(raw)
        dbg = doc.get("_debug_sample_leg_read")
        if dbg is None:
            rep.fail("  no _debug_sample_leg_read key in the output at all -- "
                     "the deployed code may not actually be the debug version. "
                     "Full top-level keys: " + str(sorted(doc.keys())))
            return
        for k, v in dbg.items():
            rep.log(f"  {k} = {v!r}")

        rep.section("Cross-check against known-good values")
        expected_val = 47.96
        actual_val = dbg.get("read_leg_value_result")
        rep.kv(expected=expected_val, actual=actual_val, doc_is_none=dbg.get("doc_is_none"),
               dig_result=dbg.get("dig_result"))
        if actual_val is not None and abs(actual_val - expected_val) < 0.5:
            rep.ok("  MATCHES -- resolves correctly inside the real Lambda too. "
                   "If tier1 still showed 0 available legs, the bug is downstream "
                   "of read_leg_value (in run_tier1's loop, LegResult construction, "
                   "or confirm_indicator), not in fleet_io/S3 access at all.")
        elif dbg.get("doc_is_none"):
            rep.fail("  doc_is_none=True inside the real Lambda -- get_json() / the S3 "
                     "get_object call itself is failing silently inside the Lambda's own "
                     "execution, despite working from every out-of-Lambda reproduction. "
                     "This points at something about the boto3 client construction or "
                     "the S3 call specifically at Lambda runtime, not permissions/VPC/env "
                     "(all separately ruled out).")
        else:
            rep.fail(f"  doc loaded but resolved to {actual_val!r} instead of ~47.96 -- "
                     f"dig_result={dbg.get('dig_result')!r}, "
                     f"doc_top_level_keys={dbg.get('doc_top_level_keys')}")

        rep.section("Also: current tier1 status counts for context")
        inds = doc.get("leading_indicators", [])
        for i in inds:
            rep.log(f"    {i['indicator_id']:32s} {i['status']:16s} "
                     f"legs {i.get('confirmed_legs')}/{i.get('available_legs')}/{i.get('total_legs')}")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("DEBUG SAMPLE READ ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
