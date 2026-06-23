import boto3, json
s3=boto3.client("s3","us-east-1")
def g(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:50]}
et=g("data/engine-trust.json")
print("engine-trust top keys:",list(et.keys())[:15])
engines=et.get("engines") or et.get("all_engines") or []
print("n engines:",len(engines))
# alpha status distribution + the actual signal_type names
from collections import Counter
dist=Counter(e.get("alpha_status") for e in engines)
print("alpha_status dist:",dict(dist))
print("\nALPHA_PROVEN (lift):")
for e in engines:
    if e.get("alpha_status")=="ALPHA_PROVEN": print("  ",e.get("signal_type"),"eff_trust",e.get("effective_trust"),"status",e.get("status"))
print("\nALPHA_NEGATIVE (prune candidates):")
for e in engines:
    if e.get("alpha_status")=="ALPHA_NEGATIVE": print("  ",e.get("signal_type"),"eff_trust",e.get("effective_trust"))
print("\nALL signal_types (first 60):",[e.get("signal_type") for e in engines][:60])
print("DONE 2144")
