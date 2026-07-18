"""ops 3450 — share-flows feed reality: sizes, sample row, conviction count."""
import json, sys
from pathlib import Path
import boto3
from ops_report import report
S3C=boto3.client("s3","us-east-1")
def rj(k):
    try: return json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:60]}
with report("3450_sf_shape") as rep:
    d=rj("data/share-flows.json")
    tk=d.get("tickers") or {}
    line1=f"share-flows top={list(d.keys())[:10]} tickers_map={len(tk) if isinstance(tk,dict) else type(tk).__name__}"
    print(line1); rep.log(line1)
    rows=[]
    def walk(o):
        if isinstance(o,dict):
            for k,v in o.items():
                if isinstance(v,dict) and "flags" in v: rows.append((k,v))
                walk(v)
        elif isinstance(o,list):
            for v in o: walk(v)
    walk(d)
    conv=[ (k,v.get("sh_3y_cagr_pct")) for k,v in rows if "INSIDER_CONVICTION" in (v.get("flags") or [])]
    neg=[ (k,c) for k,c in conv if isinstance(c,(int,float)) and c<=-2.0]
    samp=rows[0][1] if rows else {}
    line2=f"flag_rows={len(rows)} conviction={len(conv)} conviction∩cagr<=-2={neg[:6]} sample_fields={sorted(list(samp.keys()))[:12]}"
    print(line2); rep.log(line2)
    for k in ("data/buyback-yield-ranking.json","data/buyback-yield.json"):
        j=rj(k)
        line=f"{k}: top={list(j.keys())[:8] if isinstance(j,dict) else 'ERR'} n={len(j.get('rows') or j.get('rankings') or j.get('tickers') or []) if isinstance(j,dict) else 0}"
        print(line); rep.log(line)
    Path("aws/ops/reports/3450.json").write_text("{}"); sys.exit(0)
