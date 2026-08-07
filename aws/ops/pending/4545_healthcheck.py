"""ops 4545 — READ-ONLY health sweep (Khalid: 'make sure it's still
working, change nothing'). Zero deploys, zero config: walker states,
freshness, failure ledgers, composites, smoke."""
import json,os,subprocess,time
from datetime import datetime,timezone
import boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
now=datetime.now(timezone.utc)
R={"ops":4545,"at":now.isoformat(),"mode":"READ-ONLY"}
def age_m(k):
    try:
        h=s3.head_object(Bucket=B,Key=k)
        return round((now-h["LastModified"]).total_seconds()/60,1)
    except Exception as e: return f"ERR {str(e)[:40]}"
W={}
for ag,total in (("eurostat",8146),("oecd",1542),("statcan",6335),("bis",29)):
    try:
        st=json.loads(s3.get_object(Bucket=B,Key=f"data/_state/sdmx-walk-{ag}.json")["Body"].read())
        dn=len(st.get("done") or []); nf=len(st.get("failures") or {})
        W[ag]={"done":dn,"failures":nf,"fail_pct":round(100*nf/max(dn,1),1),
               "pct_of_target":round(100*dn/total,1),
               "lease_active":bool((st.get("lease_until") or 0)>time.time()),
               "state_age_min":age_m(f"data/_state/sdmx-walk-{ag}.json")}
    except Exception as e: W[ag]=str(e)[:50]
R["walkers"]=W
R["freshness_min"]={k:age_m(k) for k in ("data/provider-catalog.json","data/canary-macro.json","data/crisis-plumbing.json","data/warm/canary-macro-summary.json")}
try:
    hot=json.loads(s3.get_object(Bucket=B,Key="data/canary-macro.json")["Body"].read())
    fl=hot.get("flags") or {}
    R["composites_live"]={k:fl.get(k) for k in ("floor_breach_bp","reserve_scarcity","curve_regime","credit_cycle_phase")}
    R["canary_live_series"]=sum(1 for v in hot.values() if isinstance(v,dict) and v.get("value") is not None)
except Exception as e: R["canary_err"]=str(e)[:60]
try:
    hub=json.loads(s3.get_object(Bucket=B,Key="data/provider-catalog.json")["Body"].read())
    R["hub"]={"totals":hub.get("totals"),"as_of":hub.get("as_of")}
    eu=next((p for p in hub["providers"] if p["slug"]=="eurostat"),{})
    R["eurostat_cov"]=eu.get("coverage_pct")
except Exception as e: R["hub_err"]=str(e)[:60]
try:
    sm=subprocess.run(["python3","tools/smoke_feeds.py"],capture_output=True,text=True,timeout=180)
    R["smoke"]={"exit":sm.returncode,"failures":json.loads(sm.stdout or "{}").get("failures")}
except Exception as e: R["smoke"]={"err":str(e)[:60]}
ok=(all(isinstance(v,dict) and v.get("state_age_min",999)!=None for v in W.values())
    and (R.get("smoke") or {}).get("exit")==0)
R["verdict"]=("ALL SYSTEMS ADVANCING" if ok else "ATTENTION")+f" | walkers={json.dumps({k:(v.get('done') if isinstance(v,dict) else v) for k,v in W.items()})} smoke={(R.get('smoke') or {}).get('exit')}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4545_health.json","w"),indent=1,default=str)
open("aws/ops/reports/4545_health.md","w").write("# 4545 READ-ONLY health — "+R["verdict"]+"\n- walkers: "+json.dumps(W,default=str)+"\n- freshness_min: "+json.dumps(R["freshness_min"])+"\n- composites: "+json.dumps(R.get("composites_live"),default=str)+"\n- canary_live: "+str(R.get("canary_live_series"))+" | eurostat_cov: "+str(R.get("eurostat_cov"))+"%\n- smoke: "+json.dumps(R.get("smoke"),default=str)[:200]+"\n")
print(R["verdict"][:300])
