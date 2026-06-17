import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
for fn in ["justhodl-sovereign-fiscal","justhodl-fed-collateral"]:
    print(f"== {fn} ==")
    print("  invoke:", lam.invoke(FunctionName=fn,InvocationType="RequestResponse")["Payload"].read().decode()[:260])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/sovereign-fiscal.json")["Body"].read())
print("\nSOVEREIGN-FISCAL reads:")
for r in d["reads"]: print("  -",r)
print("  TIC as_of:", d["tic"]["as_of"], "| holders:", len(d["tic"]["holders"]), "| ranking top3:", [(x["country"],x["holdings_bn"]) for x in d["tic"]["ranking"][:3]])
print("  deficit_monthly n:", len(d["fiscal"]["deficit_monthly"]), "latest:", d["fiscal"]["deficit_monthly"][-1] if d["fiscal"]["deficit_monthly"] else None, "| ttm latest:", d["fiscal"]["deficit_ttm"][-1] if d["fiscal"]["deficit_ttm"] else None)
print("  avg_interest keys:", list(d["debt_service"]["avg_interest"].keys()), "| debt_tn:", d["debt_service"]["total_debt_latest_tn"], "| implied interest bn:", d["debt_service"]["implied_annual_interest_bn"])
c=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/fed-collateral.json")["Body"].read())
print("\nFED-COLLATERAL reads:")
for r in c["reads"]: print("  -",r)
print("  seclending days:", len(c["securities_lending"]["daily_total_bn"]), "stats:", c["securities_lending"]["stats"])
