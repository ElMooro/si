"""Content-level bug hunt: unit inconsistencies, stale-row data, the prior-only
weights, and contradictions the spec audits flagged. From AWS."""
import json, boto3
from datetime import datetime, timezone
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1",config=Config(read_timeout=90))
B="justhodl-dashboard-live"
def gj(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"err":str(e)[:40]}
bugs=[]
# BUG 1: best-setups weight_source still prior-only? (validation not feeding in)
bs=gj("data/best-setups.json")
if isinstance(bs,dict):
    bugs.append({"check":"best-setups weight_source","value":bs.get("weight_source"),"bug":bs.get("weight_source")=="prior-only","note":"conviction uses hardcoded priors, not learned weights"})
# BUG 2: short-interest stale rows (old audit found 2018 data)
si=gj("data/short-interest.json")
if isinstance(si,dict):
    sit=si.get("by_ticker") or {}
    old=[]
    items=sit.items() if isinstance(sit,dict) else [(x.get("ticker"),x) for x in (sit if isinstance(sit,list) else [])]
    for t,v in list(items)[:200]:
        sd=str((v or {}).get("settlement_date") or (v or {}).get("date") or "")
        if sd and sd<"2025-01-01": old.append(t+":"+sd)
    bugs.append({"check":"short-interest stale rows","n_pre2025":len(old),"sample":old[:4],"bug":len(old)>0})
# BUG 3: opportunities unit inconsistency (revenue growth 0.32 vs 32.0)
opp=gj("data/opportunities.json")
if isinstance(opp,dict):
    tops=opp.get("top_opportunities",[])[:30]
    mixed=[]
    for o in tops:
        for k in ["revenue_growth","rev_growth","growth"]:
            if k in o and isinstance(o[k],(int,float)):
                mixed.append((o.get("ticker"),k,o[k]))
    # flag if some <1 and some >1 (decimal vs percent mix)
    vals=[v for _,_,v in mixed]
    inconsistent = vals and (min(abs(v) for v in vals)<1) and (max(abs(v) for v in vals)>5)
    bugs.append({"check":"opportunities growth units","sample":mixed[:5],"bug":bool(inconsistent)})
# BUG 4: signal-backtest maturity + the inverted verdict (known)
sbt=gj("data/signal-backtest.json")
if isinstance(sbt,dict):
    bv=sbt.get("by_verdict",{})
    so=(bv.get("STRONG OPPORTUNITY") or {})
    bugs.append({"check":"STRONG OPPORTUNITY win rate","value":so.get("win_rate"),"n":so.get("n"),"bug":(so.get("win_rate") or 100)<48,"note":"top verdict underperforming"})
# BUG 5: backlog rpo_minus_rev contradictions (demand_accelerating false but tagged accelerating?)
bk=gj("data/backlog.json")
if isinstance(bk,dict):
    acc=bk.get("accelerating",[])
    bugs.append({"check":"backlog accelerating list","n":len(acc) if isinstance(acc,list) else "?","bug":False})
out={"bugs_found":[b for b in bugs if b.get("bug")],"all_checks":bugs}
open("aws/ops/reports/1472_q.json","w").write(json.dumps(out,indent=2,default=str)); print("done")
