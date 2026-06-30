import boto3, json
s3=boto3.client("s3","us-east-1")
def rd(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:80]}
risk=rd("portfolio/risk.json")
pm=risk.get("position_metrics") or {}
print("position_metrics type:",type(pm).__name__,"| sample keys:",list(pm)[:8] if isinstance(pm,dict) else pm[:3])
dp=rd("data/dark-pool.json")
print("dark-pool top-level keys:",list(dp)[:12])
td=dp.get("top_distribution") or []
print("top_distribution n:",len(td),"| sample:",td[:3])
dist=dp.get("distribution") or {}
print("distribution block:",json.dumps(dist)[:160])
print("DONE 2540")
