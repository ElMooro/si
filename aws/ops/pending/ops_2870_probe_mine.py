"""ops 2870 — verify Finland/PPI-pulp/ISM-PMI/Russell/HQM/30Y FRED series + deep-mine brain for engine ideas."""
import os, json, re, urllib.request, boto3
from datetime import datetime, timezone
R={"ops":2870,"ts":datetime.now(timezone.utc).isoformat()}
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
FRED=lam.get_function_configuration(FunctionName="justhodl-china-liquidity").get("Environment",{}).get("Variables",{}).get("FRED_API_KEY","")
def fred(sid,n=30):
    try:
        u="https://api.stlouisfed.org/fred/series/observations?series_id=%s&api_key=%s&file_type=json&sort_order=desc&limit=%d"%(sid,FRED,n)
        d=json.loads(urllib.request.urlopen(u,timeout=15).read())
        o=[x for x in d.get("observations",[]) if x.get("value") not in(".","",None)]
        if not o: return {"empty":True}
        return {"latest":round(float(o[0]["value"]),3),"date":o[0]["date"],"n":len(o)}
    except Exception as e: return {"err":str(e)[:50]}
cands={
 # Finland (euro-area cyclical/paper bellwether)
 "finland_exports":"XTEXVA01FIM664S","finland_prod":"PRINTO01FIQ661S","finland_lead_cli":"FINLOLITONOSTSAM",
 # PPI pulp & paper
 "ppi_pulp_paper":"WPU0911","ppi_paper_mfg":"PCU322322","ppi_paperboard":"WPU0913",
 # ISM PMI + regional Fed proxies (ISM likely proprietary/discontinued on FRED)
 "ism_mfg_pmi":"NAPM","ism_new_orders":"NAPMNOI","empire_state":"GACDISA","philly_fed":"GACDFSA","chicago_pmi":"CHIBGDANNUS",
 # Russell 2000 vs S&P500 (small/large breadth)
 "russell2000":"RU2000PR","sp500":"SP500","russell_alt":"RUT",
 # HQM corporate bond (liquidity-reversal engine) + 30Y treasury (QE precursor)
 "hqm_10yr":"HQMCB10YR","hqm_30yr":"HQMCB30YR","hqm_2yr":"HQMCB2YR","treasury_30y":"DGS30","treasury_10y":"DGS10",
}
for name,sid in cands.items():
    R.setdefault("fred",{})[name]={"sid":sid, **fred(sid)}
# DEEP BRAIN MINE — engine-idea notes (specific indicator relationships)
try:
    b=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/brain.json")["Body"].read())
    notes=b if isinstance(b,list) else (b.get("notes") or b.get("entries") or [])
    R["brain_n"]=len(notes)
    # relationship/engine-idea pattern: X predicts/precedes/leads Y
    rel=re.compile(r"predict|precede|lead(s|ing)? |signal|reversal|front.?run|always |every time|historically|HQM|high.?quality market|30.?year|QE|quantitative eas|liquidity|RRP|reverse repo|inversion|re.?steepen|yield|spread|precedes a|before a (crash|dump|crisis|recession)",re.I)
    ideas=[]
    for n in notes:
        if not isinstance(n,dict): continue
        t=(n.get("text") or "")
        if len(t)<60 or t.strip().startswith(("#",'"""',"import","def ","*")): continue
        if rel.search(t):
            ideas.append({"cat":n.get("cat"),"pinned":bool(n.get("pinned")),"text":t[:300]})
    # prioritize pinned + those naming specific instruments
    inst=re.compile(r"HQM|30.?year|QE|liquidity|RRP|reverse repo|treasury|yield|spread|SOFR|repo",re.I)
    ideas.sort(key=lambda x:(not x["pinned"], not bool(inst.search(x["text"]))))
    R["brain_idea_count"]=len(ideas)
    R["brain_ideas"]=ideas[:22]
except Exception as e: R["brain_err"]=str(e)[:100]
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3800])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2870_probe_mine.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2870 COMPLETE")
