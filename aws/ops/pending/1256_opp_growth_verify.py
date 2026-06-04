"""1256 — invoke upgraded opportunity-engine + verify growth intelligence."""
import json, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REPORT="aws/ops/reports/1256_opp_growth.json"
BUCKET="justhodl-dashboard-live"; LAMBDA="justhodl-opportunity-engine"; REGION="us-east-1"
cfg=Config(read_timeout=600,retries={"max_attempts":1})
lam=boto3.client("lambda",region_name=REGION,config=cfg); s3=boto3.client("s3",region_name=REGION,config=cfg)
out={"started":datetime.now(timezone.utc).isoformat()}
time.sleep(75)  # let deploy land
try:
    t0=time.time()
    r=lam.invoke(FunctionName=LAMBDA,InvocationType="RequestResponse",Payload=b"{}")
    out["invoke"]={"status":r.get("StatusCode"),"elapsed_s":round(time.time()-t0,1),"fe":r.get("FunctionError"),"body":r.get("Payload").read().decode()[:300]}
    print(f"{out['invoke']['status']} {out['invoke']['elapsed_s']}s {out['invoke']['body'][:150]}")
except Exception as e: out["invoke"]={"error":str(e)[:300]}
time.sleep(3)
try:
    doc=json.loads(s3.get_object(Bucket=BUCKET,Key="data/opportunities.json")["Body"].read())
    ib=doc.get("industry_benchmarks",{})
    # find names with rich growth intel
    withgi=[r for r in doc.get("all",[]) if r.get("growth_intel") and r["growth_intel"].get("expected_company_growth_pct") is not None]
    withgi.sort(key=lambda r: -(r.get("growth_opportunity_score") or 0))
    out["result"]={"n_covered":doc.get("n_covered"),"n_industries":len(ib),
        "n_with_fwd_growth":len(withgi),
        "sample_industries":{k:{"g":v.get("industry_growth_pct"),"eg":v.get("expected_industry_growth_pct"),"pe":v.get("median_pe")} for k,v in list(ib.items())[:6]},
        "top_growth_ops":[{"t":r["ticker"],"goScore":r.get("growth_opportunity_score"),"verdict":r.get("verdict"),
            "co_g":r["growth_intel"].get("company_rev_growth_pct"),"exp_co":r["growth_intel"].get("expected_company_growth_pct"),
            "ind_g":r["growth_intel"].get("industry_growth_pct"),"exp_ind":r["growth_intel"].get("expected_industry_growth_pct"),
            "pe_vs_ind":r["growth_intel"].get("pe_vs_industry_pct"),"peg":r["growth_intel"].get("peg_forward"),
            "outgrow":r["growth_intel"].get("expected_to_outgrow_industry"),"backlog":bool(r.get("backlog"))} for r in withgi[:10]]}
    print(f"covered={doc.get('n_covered')} industries={len(ib)} with_fwd_growth={len(withgi)}")
    for r in withgi[:10]:
        g=r["growth_intel"]
        print(f"  {r['t']:<6s} GO={r.get('growth_opportunity_score')} [{r.get('verdict')}] co_g={g.get('company_rev_growth_pct')}% exp={g.get('expected_company_growth_pct')}% vs ind {g.get('industry_growth_pct')}%/{g.get('expected_industry_growth_pct')}% P/E vs ind {g.get('pe_vs_industry_pct')}% PEG={g.get('peg_forward')} {'📋bl' if r.get('backlog') else ''}")
except Exception as e: out["result"]={"error":str(e)[:300]}
out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str))
print("DONE")
