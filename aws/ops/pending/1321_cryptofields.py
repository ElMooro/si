import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
out={}
try:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/crypto-intel.json")["Body"].read())
    out["top_keys"]=list(d.keys())[:30]
    oc=d.get("onchain_ratios") or {}
    out["onchain_keys"]=list(oc.keys()) if isinstance(oc,dict) else str(type(oc))
    fund=d.get("funding") or {}
    out["funding_keys"]=list(fund.keys()) if isinstance(fund,dict) else str(type(fund))
    if isinstance(fund,dict) and fund.get("rates"): out["funding_rate_sample"]=fund["rates"][:1]
    out["fear_greed"]=d.get("fear_greed")
    out["mvrv_paths"]={"onchain.mvrv_approx":oc.get("mvrv_approx"),"onchain.mvrv":oc.get("mvrv"),"risk_score":d.get("risk_score")}
except Exception as e: out["err"]=str(e)[:200]
open("aws/ops/reports/1321_cf.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
