"""
ops/4720 — re-smoke-test justhodl-invest after the field-mismatch fixes.

ops 4717 already confirmed the function is Active and the schedule is
correct -- unchanged by this push (source-only, no config.json edit).
This just re-invokes now that get_spx_er()'s crash and the 15 leg-path
mismatches are fixed, and prints the real output shape so we can see
what Tier 1/2/3 actually produced against live data.
"""
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
FUNCTION_NAME = "justhodl-invest"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/invest.json"

s3 = boto3.client("s3", region_name=REGION)


def main():
    with report("4720_invest_resmoke") as rep:
        rep.heading("ops 4720 — re-smoke-test justhodl-invest after field fixes")

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
        body = payload.get("body") if isinstance(payload, dict) else None
        if isinstance(body, str):
            try:
                body = json.loads(body)
            except Exception:
                pass
        rep.ok(f"  invoke succeeded in {elapsed:.1f}s")
        rep.log(f"  handler response body: {json.dumps(body)[:2500] if isinstance(body, dict) else body}")

        rep.section("data/invest.json")
        try:
            raw = s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read()
            doc = json.loads(raw)
        except ClientError as e:
            rep.fail(f"  {OUT_KEY}: {e}")
            return
        rep.ok(f"  {len(raw)} bytes, schema={doc.get('schema')}, generated_at={doc.get('generated_at')}")

        inds = doc.get("leading_indicators", [])
        gates = doc.get("industry_gates", {})
        picks = doc.get("stock_picks", [])
        rep.kv(
            n_leading_indicators=len(inds),
            n_confirmed=sum(1 for i in inds if i.get("status") == "CONFIRMED"),
            n_turning=sum(1 for i in inds if i.get("status") == "TURNING"),
            n_insufficient=sum(1 for i in inds if i.get("status") == "INSUFFICIENT_DATA"),
            n_conflicting=sum(1 for i in inds if i.get("status") == "CONFLICTING"),
            n_industry_gates=len(gates),
            n_gates_pass=sum(1 for g in gates.values() if g.get("pass")),
            n_stock_picks=len(picks),
        )
        rep.log("  per-indicator status:")
        for i in inds:
            rep.log(f"    {i['indicator_id']:32s} {i['status']:16s} "
                     f"legs {i.get('confirmed_legs')}/{i.get('available_legs')}/{i.get('total_legs', '?')}")
        rep.log("  per-industry gate:")
        for k, g in gates.items():
            rep.log(f"    {k:24s} status={g.get('status','OK' if g.get('pass') is not None else '?')} "
                     f"pass={g.get('pass')} excess_pp={g.get('excess_return_pp')} "
                     f"ir={g.get('information_ratio')}")
        if picks:
            rep.log("  stock picks:")
            for p in picks[:20]:
                rep.log(f"    {p.get('ticker','?'):6s} {p.get('status'):10s} "
                        f"score={p.get('composite_score')} verdict={p.get('vs_industry_etf')}")

        rep.section("Verdict")
        rep.ok("justhodl-invest ran end-to-end against live data with no crash.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("RESMOKE ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
