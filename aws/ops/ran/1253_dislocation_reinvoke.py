"""1253 — re-invoke dislocation-detector (full universe) + verify."""
import json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT="aws/ops/reports/1253_dislocation_reinvoke.json"
BUCKET="justhodl-dashboard-live"; LAMBDA="justhodl-dislocation-detector"; REGION="us-east-1"
cfg=Config(read_timeout=420,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name=REGION,config=cfg); s3=boto3.client("s3",region_name=REGION,config=cfg)
out={"started":datetime.now(timezone.utc).isoformat()}
time.sleep(60)  # let deploy land
try:
    t0=time.time()
    resp=lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    payload=resp.get("Payload").read().decode()
    out["invoke"]={"status":resp.get("StatusCode"),"elapsed_s":round(time.time()-t0,1),"fe":resp.get("FunctionError"),"body":payload[:500]}
    print(f"status={resp.get('StatusCode')} {round(time.time()-t0,1)}s body={payload[:250]}")
except Exception as e: out["invoke"]={"error":str(e)[:300]}
try:
    doc=json.loads(s3.get_object(Bucket=BUCKET,Key="data/dislocations.json")["Body"].read())
    lag=doc.get("buy_the_laggard",[])
    caps={}
    for s in doc.get("top_dislocations",[]): caps[s.get("cap_bucket")]=caps.get(s.get("cap_bucket"),0)+1
    out["output"]={"scored":doc.get("universe_scored"),"cohorts":doc.get("n_cohorts"),"laggards":len(lag),
        "cap_distribution":caps,
        "top12":[{"t":s["ticker"],"score":s["dislocation_score"],"cap":s.get("cap_bucket"),"r40":s.get("rule_of_40"),
                   "evs":round(s["ev_sales"],2) if s.get("ev_sales") else None,
                   "vs":(s.get("dislocated_vs") or {}).get("ticker"),"prem":(s.get("dislocated_vs") or {}).get("ev_sales_premium_pct")} for s in lag[:12]]}
    print(f"scored={doc.get('universe_scored')} cohorts={doc.get('n_cohorts')} laggards={len(lag)} caps={caps}")
    for s in lag[:12]:
        vs=s.get("dislocated_vs") or {}
        print(f"  {s['ticker']:<6s} {s['dislocation_score']:>5.1f} [{s.get('cap_bucket')}] R40={s.get('rule_of_40')} EV/S={round(s['ev_sales'],2) if s.get('ev_sales') else '—'} vs {vs.get('ticker','—')} +{vs.get('ev_sales_premium_pct','?')}%")
except Exception as e: out["output"]={"error":str(e)[:300]}
out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str))
print("DONE")
