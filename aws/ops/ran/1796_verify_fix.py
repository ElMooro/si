import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=240,retries={"max_attempts":0}))
print("invoke:", lam.invoke(FunctionName="justhodl-sovereign-fiscal",InvocationType="RequestResponse")["Payload"].read().decode()[:280])
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/sovereign-fiscal.json")["Body"].read())
print("\nreads:"); [print("  -",r) for r in d["reads"]]
print("\nTIC as_of:", d["tic"]["as_of"])
print("ranking top6:", [(x["country"],x["holdings_bn"],x["chg_12m_bn"]) for x in d["tic"]["ranking"][:6]])
for h in d["tic"]["holders"]:
    if h["country"] in ("Grand Total","Japan","China, Mainland"):
        print(f"  {h['country']:16} latest={h['latest']} 12m_chg={h['chg_12m_bn']} ({h['chg_12m_pct']}%) n={h['n_obs']} range={h['start_date']}..{h['latest_date']}")
f=d["fiscal"]; print("\ndeficit latest:",f["deficit_monthly"][-1],"| TTM:",f["deficit_ttm"][-1] if f["deficit_ttm"] else None,"| receipts:",f["receipts_monthly"][-1],"| outlays:",f["outlays_monthly"][-1],"| n_months:",len(f["deficit_monthly"]))
ds=d["debt_service"]; print("debt_tn:",ds["total_debt_latest_tn"],"| implied interest bn:",ds["implied_annual_interest_bn"],"| avg-int TIB latest:",ds["avg_interest"].get("Total Interest-bearing Debt",[])[-1] if ds["avg_interest"].get("Total Interest-bearing Debt") else None)
