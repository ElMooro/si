import json, boto3
from datetime import datetime, timezone
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
r=lam.invoke(FunctionName="justhodl-retail-sentiment", InvocationType="RequestResponse", LogType="Tail")
import base64
log=base64.b64decode(r.get("LogResult","")).decode("utf-8","ignore")
print("invoke:", r.get("StatusCode"), "err:", r.get("FunctionError"))
# show any import/track errors in log tail
for ln in log.splitlines():
    if any(k in ln for k in ("retail-signals","retail-track","Traceback","Error","import","equity_enrich")): print("  LOG:",ln[:160])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/retail-sentiment.json")["Body"].read())
print("signals_logged:", d.get("signals_logged"))
tr=d.get("track_record") or {}
for k in ("momentum","divergence"):
    t=tr.get(k) or {}
    print(f"  {k}: n_calls={t.get('n_calls')} pending={t.get('n_pending')} windows={t.get('windows')}")
