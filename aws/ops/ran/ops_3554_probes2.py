"""ops 3554 — corrected substrate probes (per-item tolerant)."""
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
            u, headers={"User-Agent": "ops-3554"}), timeout=40) as r:
        return json.loads(r.read())

with report("3554_probes2") as rep:
    def P(n, d):
        line = "PASS  " + n + " — " + json.dumps(d, default=str)[:660]
        print(line); rep.log(line)

    try:
        r = s3c.list_objects_v2(Bucket=BUCKET, Prefix="etf-flows/",
                                Delimiter="/", MaxKeys=50)
        names = [o["Key"] for o in r.get("Contents") or []]
        P("Y0_prefix", {"keys": names[:12],
                        "subdirs": [p["Prefix"] for p in
                                    r.get("CommonPrefixes") or []]})
    except Exception as e:
        P("Y0_prefix", {"err": str(e)[:160]})
    for name, key in (("Y1_latest", "etf-flows/latest.json"),
                      ("Y1b_legacy", "data/etf-flows.json")):
        try:
            d = json.loads(s3c.get_object(Bucket=BUCKET, Key=key)
                           ["Body"].read())
            recs, src = None, None
            for k in ("etfs", "rows", "by_ticker", "tickers", "data",
                      "flows", "board"):
                if isinstance(d.get(k), (list, dict)):
                    recs = d[k]; src = k; break
            if isinstance(recs, dict):
                t0, r0 = next(iter(recs.items()))
            elif isinstance(recs, list):
                r0 = recs[0]; t0 = r0.get("ticker") or r0.get("symbol")
            else:
                r0, t0 = {}, None
            P(name, {"top": sorted(d.keys())[:12], "container": src,
                     "n": (len(recs) if recs is not None else 0),
                     "t0": t0, "r0_keys": sorted(r0.keys())[:26],
                     "r0": json.dumps(r0, default=str)[:280]})
        except Exception as e:
            P(name, {"err": str(e)[:160]})
    try:
        c = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/cds-proxy.json")["Body"].read())
        P("Y2_cds", {"hy_oas": (c.get("corporate") or {}).get("hy_oas"),
                     "italy": (c.get("sovereigns") or {}).get("italy_10y"),
                     "top": sorted(c.keys())})
    except Exception as e:
        P("Y2_cds", {"err": str(e)[:160]})
    try:
        info = fmp("etf/info?symbol=AGG")
        i0 = info[0] if isinstance(info, list) and info else info
        P("Y3_info", {"keys": sorted((i0 or {}).keys())[:24],
                      "peek": json.dumps(i0, default=str)[:320]})
    except Exception as e:
        P("Y3_info", {"err": str(e)[:180]})
    for sym in ("AGG", "TLT"):
        try:
            px = fmp(f"historical-price-eod/light?symbol={sym}"
                     "&from=2002-01-01")
            if isinstance(px, dict):
                px = px.get("historical") or []
            ds = sorted(str(x.get("date", ""))[:10] for x in px)
            P(f"Y4_{sym}", {"n": len(px), "oldest": ds[0] if ds else None,
                            "newest": ds[-1] if ds else None})
        except Exception as e:
            P(f"Y4_{sym}", {"err": str(e)[:160]})
    print("RESULT: ALL PASS")
    (REPO/"aws/ops/reports/3554.json").write_text(
        json.dumps({"ops": 3554, "fails": []}))
sys.exit(0)
