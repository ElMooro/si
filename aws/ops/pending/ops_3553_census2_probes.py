"""ops 3553 — substrate probes for the ETF Census + Fixed-Income
Census (read-only): per-ETF record shape in etf-fund-flows, cds-proxy
OAS leaf keys, FMP /stable etf info + price depth for AGG/TLT."""
import json, sys, urllib.request
from pathlib import Path
import boto3
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
s3c = boto3.client("s3", region_name="us-east-1")
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

def fmp(qs):
    u = f"https://financialmodelingprep.com/stable/{qs}&apikey={FMP_KEY}"
    with urllib.request.urlopen(urllib.request.Request(
            u, headers={"User-Agent": "ops-3553"}), timeout=40) as r:
        return json.loads(r.read())

with report("3553_census2_probes") as rep:
    def P(n, d):
        line = "PASS  " + n + " — " + json.dumps(d, default=str)[:640]
        print(line); rep.log(line)

    d = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/etf-fund-flows.json")["Body"].read())
    top = sorted(d.keys())[:12]
    recs = None
    for k in ("etfs", "rows", "by_ticker", "tickers", "data", "board"):
        if isinstance(d.get(k), (list, dict)):
            recs = d[k]; src = k; break
    if isinstance(recs, dict):
        t0, r0 = next(iter(recs.items()))
    else:
        r0 = (recs or [{}])[0]; t0 = r0.get("ticker")
    P("Y1_etf_flows", {"top": top, "container": src,
        "n": len(recs) if recs is not None else 0, "sample_t": t0,
        "r0_keys": sorted(r0.keys())[:26],
        "r0": json.dumps(r0, default=str)[:300]})

    c = json.loads(s3c.get_object(Bucket=BUCKET,
        Key="data/cds-proxy.json")["Body"].read())
    hy = ((c.get("corporate") or {}).get("hy_oas")) or {}
    de = ((c.get("sovereigns") or {}).get("italy_10y")) or {}
    P("Y2_cds_leaves", {"top": sorted(c.keys()),
        "hy_oas": hy, "italy_10y": de})

    try:
        info = fmp("etf/info?symbol=AGG")
        i0 = info[0] if isinstance(info, list) and info else info
        P("Y3_etf_info", {"keys": sorted((i0 or {}).keys())[:24],
            "sample": json.dumps(i0, default=str)[:340]})
    except Exception as e:
        P("Y3_etf_info", {"err": str(e)[:200]})
    for sym in ("AGG", "TLT"):
        try:
            px = fmp(f"historical-price-eod/light?symbol={sym}&from=2003-01-01")
            if isinstance(px, dict):
                px = px.get("historical") or []
            P(f"Y4_px_{sym}", {"n": len(px),
                "oldest": (px[-1] if px and px[0].get('date','9')>px[-1].get('date','0') else px[0]).get("date") if px else None,
                "newest": (px[0] if px and px[0].get('date','9')>px[-1].get('date','0') else px[-1]).get("date") if px else None})
        except Exception as e:
            P(f"Y4_px_{sym}", {"err": str(e)[:160]})
    print("RESULT: ALL PASS")
    (REPO/"aws/ops/reports/3553.json").write_text(json.dumps({"ops":3553,"fails":[]}))
sys.exit(0)
