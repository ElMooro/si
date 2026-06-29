import boto3, json
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
r=lam.invoke(FunctionName="justhodl-bottleneck-boom",InvocationType="RequestResponse",Payload=b"{}")
print("err:",r.get("FunctionError"))
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom.json")["Body"].read())
print("version:",d.get("version"),"| dur:",d.get("duration_s"),"s")
t=d.get("constraint_language") or {}
print("=== #2 CONSTRAINT LANGUAGE (EDGAR full-text) ===")
for k,v in (t.get("phrases") or {}).items():
    print("  %-22s 90d=%-4s prior=%-4s %-8s  e.g. %s"%(k,v.get("hits_90d"),v.get("hits_prior_90d"),v.get("trend"),",".join(v.get("sample_tickers",[])[:5])))
print("on_allocation_names:",t.get("on_allocation_names"))
print("intensity_chg_pct:",t.get("intensity_chg_pct"),"| rising:",t.get("rising_phrases"),"| confirms:",t.get("text_confirms_bottleneck"))
print("names_by_breadth:",t.get("names_by_breadth"))
print("DONE 2466")
