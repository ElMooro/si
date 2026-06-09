"""Verify which of the audit's Top-10 ECB indicators already exist. From AWS."""
import json, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=90))
def gj(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception: return None
def keys_deep(d,acc=None):
    acc=acc if acc is not None else set()
    if isinstance(d,dict):
        for k,v in d.items(): acc.add(k.lower()); keys_deep(v,acc)
    elif isinstance(d,list) and d and isinstance(d[0],dict): keys_deep(d[0],acc)
    return acc
# engines that might hold these
src={k:gj(f"data/{k}.json") for k in ["euro-fragmentation","ecb-detail","systemic-stress","eurodollar-stress","global-liquidity","fedwatch","bonds","macro-calendar","consumer-pulse","global-markets","cb-injection"]}
allk={k:keys_deep(v) for k,v in src.items() if v}
checks={
 "#1 BTP-Bund / periphery spreads":["btp","bund","oat","periphery","fragmentation","spread_to_bund","sovereign_spread"],
 "#2 5Y5Y EUR inflation breakeven":["5y5y","inflation_swap","breakeven","inflation_expectation"],
 "#3 ECB OIS-implied rate path":["ois","rate_path","meeting_prob","wirp","cut_prob","implied_rate"],
 "#4 3M Euribor / Euribor-OIS":["euribor","euribor_ois"],
 "#5 M3 money supply YoY":["m3","money_supply","broad_money"],
 "#6 €STR / €STR-DFR spread":["estr","str_dfr","ester"],
 "#7 TARGET2 by country":["target2","tgb","de_target2"],
 "#8 HICP headline/core/services":["hicp","cpi_ea","inflation_headline","core_inflation"],
 "#9 APP/PEPP roll-off":["app","pepp","reinvest","roll_off","qt_pace"],
 "#10 Sovereign/Bank CDS":["cds","sovereign_cds","bank_cds","credit_default"],
}
res={}
for ind,sigs in checks.items():
    hits=[eng for eng,ks in allk.items() if any(any(s in k for k in ks) for s in sigs)]
    res[ind]={"exists_in":hits,"status":"EXISTS" if hits else "GAP"}
out={"engines_checked":list(allk.keys()),"results":res,
     "GAPS":[i for i,r in res.items() if r["status"]=="GAP"],
     "EXISTS":[i for i,r in res.items() if r["status"]=="EXISTS"]}
open("aws/ops/reports/1522_t10.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
