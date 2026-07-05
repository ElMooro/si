"""ops 2883 — workflow deployed the self-exclusion code (ResourceConflict in 2882 = workflow race);
just wait for update completion, invoke, verify behaviorally."""
import os, json, time, traceback, boto3
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-brain-compiler"
R={"ops":2883,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=330,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); B="justhodl-dashboard-live"
try:
    for _ in range(50):
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus")=="Successful": R["fn_state"]=c.get("LastModified"); break
        time.sleep(4)
    p=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
    R["invoke"]=p["Payload"].read().decode()[:140]
    time.sleep(2)
    d=json.loads(s3.get_object(Bucket=B,Key="data/brain-compiler.json")["Body"].read())
    R["summary"]={k:d["summary"][k] for k in ("n_notes","n_claims","covered","gaps","coverage_pct")}
    sw=[c for c in (d.get("claims") or []) if "swap lines" in (c.get("concepts") or [])]
    R["swap_claim"]={"status":sw[0]["status"],"eng":[e["engine"] for e in sw[0]["engines"][:3]]} if sw else "not-found"
    R["self_excluded"]=all("brain-compiler" not in e["engine"] for c in (d.get("claims") or []) for e in (c.get("engines") or []))
    R["generated_at"]=d.get("generated_at")
    R["status"]="LOOP_CLOSED" if (d["summary"]["gaps"]==0 and R["self_excluded"]) else "CHECK"
except Exception:
    R["errors"]["main"]=traceback.format_exc()[-400:]; R["status"]="FAILED"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:1600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2883_verify_final.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2883 COMPLETE")
