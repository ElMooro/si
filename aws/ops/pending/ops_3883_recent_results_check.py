"""
ops_3883 — PROBE: read earnings-tracker's recent_results_30d directly (ops
3882's fallback logic grabbed the wrong list-bearing key, upcoming_14d
instead of recent_results_30d). If this already has real actuals for semi
tickers, that directly closes the "did TSM/INTC/etc already report and
miss/guide down" gap from the flow/price investigation without building
anything new — the data already exists, it just wasn't read correctly.
WRITES NO CODE.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
SEMI_TICKERS = {"NVDA","AMD","AVGO","TSM","MU","QCOM","TXN","INTC","LRCX","KLAC",
                "AMAT","ARM","ASML","MRVL","ON","SMCI"}


def main():
    with report("3883_recent_results_check") as rep:
        rep.heading("ops 3883 — read recent_results_30d + data_sources directly, by the RIGHT key")
        try:
            o = s3.get_object(Bucket=BUCKET, Key="data/earnings-tracker.json")
            et = json.loads(o["Body"].read())
            lm = o["LastModified"]
        except Exception as e:
            rep.fail(f"  earnings-tracker.json unreadable: {str(e)[:200]}")
            sys.exit(1)

        rep.section("1. the right field this time")
        recent = et.get("recent_results_30d") or []
        rep.kv(n_recent_field=et.get("n_recent"), len_recent_results_30d=len(recent),
               age_h=round((datetime.now(timezone.utc)-lm).total_seconds()/3600,1))
        if recent:
            rep.log(f"  sample record keys: {sorted(recent[0].keys())}")
            rep.log(f"  full sample record: {json.dumps(recent[0], default=str)[:600]}")

        rep.section("2. data_sources — what's ACTUALLY live vs dead right now")
        rep.log(f"  data_sources: {json.dumps(et.get('data_sources'), default=str)[:800]}")

        rep.section("3. semi tickers in recent_results_30d specifically")
        semi_hits = [r for r in recent if (r.get("ticker") or r.get("symbol")) in SEMI_TICKERS]
        rep.kv(semi_hits_in_recent_30d=len(semi_hits))
        for r in semi_hits:
            rep.log(f"    {json.dumps(r, default=str)[:500]}")
        if not semi_hits:
            all_tickers = sorted({r.get("ticker") or r.get("symbol") for r in recent if r.get("ticker") or r.get("symbol")})
            rep.log(f"  no semi tickers in the 30d window. ALL tickers actually present: {all_tickers}")

        rep.section("4. PEAD signals — same check, might carry surprise/drift even if recent_results doesn't")
        pead = et.get("pead_signals") or []
        rep.kv(n_pead_field=et.get("n_pead"), len_pead_signals=len(pead))
        pead_semi = [r for r in pead if (r.get("ticker") or r.get("symbol")) in SEMI_TICKERS]
        rep.kv(semi_hits_in_pead=len(pead_semi))
        for r in pead_semi:
            rep.log(f"    {json.dumps(r, default=str)[:500]}")

        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
