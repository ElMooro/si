"""ops 2873 — find clean FRED substitutes for ISM PMI (proprietary) + Russell/S&P small-large breadth."""
import os, json, urllib.request, boto3
from datetime import datetime, timezone
R={"ops":2873,"ts":datetime.now(timezone.utc).isoformat()}
lam=boto3.client("lambda",region_name="us-east-1")
FRED=lam.get_function_configuration(FunctionName="justhodl-china-liquidity").get("Environment",{}).get("Variables",{}).get("FRED_API_KEY","")
def fred(sid,n=20):
    try:
        u="https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=%d"%(sid,FRED,n)
        d=json.loads(urllib.request.urlopen(u,timeout=15).read())
        o=[x for x in d.get("observations",[]) if x.get("value") not in(".","",None)]
        return {"latest":round(float(o[0]["value"]),2),"date":o[0]["date"],"n":len(o)} if o else {"empty":1}
    except Exception as e: return {"err":str(e)[:45]}
c={# ISM PMI proxies (ISM itself proprietary/off-FRED)
 "cfnai":"CFNAI","cfnai_diffusion":"CFNAIDIFF","chicago_fed_ma3":"CFNAIMA3",
 "philly_fed":"GACDFSA637MEI","empire_state":"GACDISA066MSFRBNY","dallas_fed":"BACTSAMFRBDAL","kc_fed":"FRBKCLMCILA",
 "richmond_fed":"AMBSL",
 # Russell/S&P small-vs-large breadth (Russell 2000 proprietary; Wilshire on FRED)
 "wilshire_smallcap":"WILLSMLCAPPR","wilshire_largecap":"WILLLRGCAPPR","wilshire_smallcap2":"WILLSMLCAP",
 "wilshire_largecap2":"WILLLRGCAP","wilshire_microcap":"WILLMICROCAPPR","sp600":"SP600"}
for name,sid in c.items():
    R.setdefault("fred",{})[name]={"sid":sid, **fred(sid)}
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2000])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2873_ism_russell.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2873 COMPLETE")
