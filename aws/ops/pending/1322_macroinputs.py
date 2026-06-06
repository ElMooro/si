import json, boto3
s3=boto3.client("s3",region_name="us-east-1")
out={}
def rd(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:60]}
# bond-vol has channels with z-scores for DGS10, HY spread, etc.
bv=rd("data/bond-vol.json")
out["bondvol_channels"]=[{"id":c.get("id"),"z":c.get("z_score"),"pct":c.get("percentile_1y")} for c in bv.get("channels",[])] if isinstance(bv,dict) else bv
# crypto-intel (correct key)
ci=rd("crypto-intel.json")
if isinstance(ci,dict):
    oc=ci.get("onchain_ratios") or {}
    out["crypto_keys"]=list(ci.keys())[:20]
    out["mvrv"]=oc.get("mvrv_approx") or oc.get("mvrv")
    out["fear_greed"]=ci.get("fear_greed")
    fund=ci.get("funding") or {}
    out["funding_rates_sample"]=(fund.get("rates") or [])[:1]
open("aws/ops/reports/1322_mi.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
