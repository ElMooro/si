import json, time, boto3
from datetime import datetime, timezone
from botocore.config import Config
cfg=Config(read_timeout=300,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name="us-east-1",config=cfg); s3=boto3.client("s3",region_name="us-east-1",config=cfg)
out={"started":datetime.now(timezone.utc).isoformat()}
try:
    r=lam.invoke(FunctionName="justhodl-etf-true-flows",InvocationType="RequestResponse",Payload=b"{}")
    out["invoke"]=r.get("Payload").read().decode()[:200]
except Exception as e: out["invoke"]=str(e)[:200]
time.sleep(2)
try:
    tf=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/etf-true-flows.json")["Body"].read())
    out["n_etfs"]=tf.get("n_etfs"); out["maturity"]=tf.get("maturity")
    out["sample_aum"]=[{"t":e["ticker"],"aum":e.get("aum_est_b"),"so":e.get("shares_outstanding")} for e in list(tf.get("by_etf",{}).values())[:5]]
except Exception as e: out["err"]=str(e)[:150]
open("aws/ops/reports/1272_tf.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
