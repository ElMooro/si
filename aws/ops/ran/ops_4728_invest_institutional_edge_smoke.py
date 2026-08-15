"""
ops/4728 — smoke test justhodl-invest after wiring in institutional-edge
data (sector-flow-state, insider-industry-cluster, stealth-accumulation,
credit-before-equity, finra-short, hiring-velocity, estimate-revisions).
Confirms no crash and reports whether the new fields actually populated
against live data, distinct from the FakeFleet fixtures the unit tests
already proved wire correctly in isolation.
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
    with report("4728_invest_institutional_edge_smoke") as rep:
        rep.heading("ops 4728 — smoke test institutional-edge wiring against live data")

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
            rep.fail(f"  FunctionError={fn_error}: {json.dumps(payload)[:2000]}")
            return
        rep.ok(f"  invoke succeeded in {elapsed:.1f}s: {payload.get('body')}")

        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
        rep.kv(schema=doc.get("schema"), generated_at=doc.get("generated_at"))

        rep.section("Tier 2: institutional_confirmation presence")
        gates = doc.get("industry_gates", {})
        for k, g in gates.items():
            ic = g.get("institutional_confirmation")
            rep.log(f"  {k}: status={g.get('status', 'OK' if g.get('pass') is not None else '?')} "
                     f"institutional_confirmation={ic}")

        rep.section("Tier 3: institutional components in stock_picks")
        picks = doc.get("stock_picks", [])
        rep.kv(n_picks=len(picks))
        inst_keys = {"smart_money_convergence", "credit_signal", "short_squeeze_setup",
                     "hiring_velocity", "estimate_revision_direction"}
        for p in picks:
            if p.get("status") != "OK":
                rep.log(f"  {p.get('ticker', p.get('industry'))}: {p.get('status')} — "
                         f"{p.get('reason', '')}")
                continue
            used = set(p.get("components_used") or [])
            inst_used = used & inst_keys
            rep.log(f"  {p['ticker']}: score={p.get('composite_score')} "
                     f"reweighted={p.get('reweighted')} "
                     f"institutional_components_used={sorted(inst_used) or 'none'} "
                     f"raw_institutional={ {k: v for k, v in (p.get('raw') or {}).items() if k in (
                         'credit_direction_delta', 'squeeze_score', 'hiring_expansion_score',
                         'estimate_revision_direction', 'dealer_gex', 'smart_money_13f_funds_long')} }")

        rep.section("Verdict")
        rep.ok("justhodl-invest ran end-to-end with the institutional-edge extension against "
               "live data, no crash. Whether any component actually POPULATED depends on "
               "today's real coverage in each source engine (see logs above) -- this is "
               "expected to be sparse/absent for most names today, same honest-gap behavior "
               "as the rest of the engine, not a defect.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("INSTITUTIONAL EDGE SMOKE ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
