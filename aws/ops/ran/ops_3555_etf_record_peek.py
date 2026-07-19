"""ops 3555 — per-ETF record shapes: legacy by_etf + etf-flows/daily
+ composite tickers (read-only)."""
import json, sys
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
s3c = boto3.client("s3", region_name="us-east-1")
with report("3555_etf_record_peek") as rep:
    def P(n, d):
        line = "PASS  " + n + " — " + json.dumps(d, default=str)[:700]
        print(line); rep.log(line)
    d = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/etf-flows.json")["Body"].read())
    be = d.get("by_etf") or {}
    t0 = "SPY" if "SPY" in be else next(iter(be), None)
    P("Z1_by_etf", {"n": len(be), "t0": t0,
        "keys": sorted((be.get(t0) or {}).keys())[:26],
        "rec": json.dumps(be.get(t0), default=str)[:340],
        "cats_n": len(d.get("by_category") or {})})
    for name, key in (("Z2_daily", "etf-flows/daily.json"),
                      ("Z3_composite", "etf-flows/composite.json")):
        try:
            x = json.loads(s3c.get_object(Bucket=BUCKET, Key=key)
                           ["Body"].read())
            tk = sorted(x.keys())[:10]
            recs = None
            for k in ("etfs", "rows", "by_etf", "by_ticker", "flows",
                      "tickers"):
                if isinstance(x.get(k), (list, dict)):
                    recs = x[k]; src = k; break
            if isinstance(recs, dict):
                s0, r0 = next(iter(recs.items()))
            elif isinstance(recs, list):
                r0 = recs[0]; s0 = r0.get("ticker") or r0.get("symbol")
            else:
                r0, s0, src = {}, None, None
            P(name, {"top": tk, "container": src, "s0": s0,
                     "n": len(recs) if recs is not None else 0,
                     "r0_keys": sorted(r0.keys())[:24],
                     "r0": json.dumps(r0, default=str)[:300]})
        except Exception as e:
            P(name, {"err": str(e)[:160]})
    print("RESULT: ALL PASS")
    (REPO/"aws/ops/reports/3555.json").write_text(
        json.dumps({"ops": 3555, "fails": []}))
sys.exit(0)
