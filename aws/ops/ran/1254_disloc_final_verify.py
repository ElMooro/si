"""1254 — verify pairing fix + DISLOCATION in conviction board."""
import json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT="aws/ops/reports/1254_disloc_final.json"
BUCKET="justhodl-dashboard-live"; REGION="us-east-1"
cfg=Config(read_timeout=420,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name=REGION,config=cfg); s3=boto3.client("s3",region_name=REGION,config=cfg)
out={"started":datetime.now(timezone.utc).isoformat()}
time.sleep(60)
# re-run detector
try:
    r=lam.invoke(FunctionName="justhodl-dislocation-detector",InvocationType="RequestResponse",Payload=b"{}")
    out["detector"]=r.get("Payload").read().decode()[:200]
except Exception as e: out["detector"]=str(e)[:200]
time.sleep(3)
try:
    doc=json.loads(s3.get_object(Bucket=BUCKET,Key="data/dislocations.json")["Body"].read())
    out["pairs"]=[{"t":s["ticker"],"score":s["dislocation_score"],"cap":s.get("cap_bucket"),
                    "vs":(s.get("dislocated_vs") or {}).get("ticker"),
                    "vs_cap":(s.get("dislocated_vs") or {}).get("cap_bucket"),
                    "prem":(s.get("dislocated_vs") or {}).get("ev_sales_premium_pct")}
                   for s in doc.get("buy_the_laggard",[])[:12]]
except Exception as e: out["pairs"]={"err":str(e)[:150]}
# re-run conviction board → check DISLOCATION tier present
try:
    r=lam.invoke(FunctionName="justhodl-best-setups",InvocationType="RequestResponse",Payload=b"{}")
    time.sleep(2)
    bs=json.loads(s3.get_object(Bucket=BUCKET,Key="data/best-setups.json")["Body"].read())
    disl=[s for s in bs.get("top_setups",[]) if "DISLOCATION" in (s.get("signal_keys") or [])]
    out["conviction"]={"total":bs.get("stats"),"dislocation_in_board":len(disl),
        "sample":[{"t":s["ticker"],"conv":s["conviction"],"sigs":s["signal_keys"]} for s in disl[:5]]}
except Exception as e: out["conviction"]={"err":str(e)[:150]}
out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str))
print(json.dumps(out,indent=2,default=str)[:1500])
