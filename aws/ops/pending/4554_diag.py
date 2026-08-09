"""ops 4554 — diagnose why series_seen(27) is nowhere near the real
category sizes Khalid quoted (Interest Rates alone = 1,080). Direct raw
API call, no abstractions, print everything."""
import json,os,urllib.request
FRED_KEY="2f057499936072679d8843d7fce99989"
R={}
def raw_get(u):
    req=urllib.request.Request(u,headers={"User-Agent":"JustHodl research admin@justhodl.ai"})
    with urllib.request.urlopen(req,timeout=25) as r:
        return r.read()
u=f"https://api.stlouisfed.org/fred/category/series?category_id=32145&api_key={FRED_KEY}&file_type=json&limit=1000&offset=0"
try:
    raw=raw_get(u)
    d=json.loads(raw)
    R["fx_intervention_raw_len"]=len(raw)
    R["fx_intervention_top_keys"]=list(d.keys())
    R["fx_intervention_count"]=d.get("count")
    R["fx_intervention_n_seriess"]=len(d.get("seriess",[]))
    R["fx_intervention_sample_ids"]=[s.get("id") for s in d.get("seriess",[])[:5]]
except Exception as e: R["fx_err"]=str(e)[:200]
u2=f"https://api.stlouisfed.org/fred/category/series?category_id=22&api_key={FRED_KEY}&file_type=json&limit=1000&offset=0"
try:
    raw2=raw_get(u2)
    d2=json.loads(raw2)
    R["interest_rates_count"]=d2.get("count")
    R["interest_rates_n_seriess"]=len(d2.get("seriess",[]))
except Exception as e: R["ir_err"]=str(e)[:200]
import boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
try:
    st=json.loads(s3.get_object(Bucket=B,Key="data/_state/fred-scoped-import.json")["Body"].read())
    R["state_cats_done"]=st.get("cats_done")
    R["state_series_seen"]=st.get("series_seen")
    R["state_errors"]=st.get("errors")
except Exception as e: R["state_err"]=str(e)[:100]
os.makedirs("aws/ops/reports",exist_ok=True)
open("aws/ops/reports/4554.md","w").write("# 4554 diag — "+json.dumps(R,indent=1,default=str)+"\n")
print(json.dumps(R,default=str)[:800])
