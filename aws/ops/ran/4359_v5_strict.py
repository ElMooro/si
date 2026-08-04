"""ops 4359 — strict v5 verify (4358 raced the deploy: version=='5.0' matched
the pre-fix build). Success criterion is the join itself: invoke, read S3,
require cryptoquant.status=='ok' AND fleet joins >=6; otherwise record the
EXACT error strings and retry (max 6 x 40s)."""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-crypto-intel"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4359,"started":datetime.now(timezone.utc).isoformat(),"attempts":[]}
v=None
for i in range(6):
    at={"n":i+1}
    try:
        inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
        at["invoke"]=inv.get("StatusCode"); at["fn_err"]=inv.get("FunctionError")
        inv["Payload"].read()
    except Exception as e:
        at["invoke_err"]=str(e)[:120]
    try:
        d=json.loads(s3.get_object(Bucket=BUCKET,Key="crypto-intel.json")["Body"].read())
        q=d.get("cryptoquant") or {}; fl=d.get("fleet") or {}
        led=fl.get("ledger") or []
        at["cq_status"]=q.get("status"); at["cq_error"]=str(q.get("error"))[:150]
        at["fleet_status"]=fl.get("status"); at["fleet_error"]=str(fl.get("error"))[:150]
        at["fleet_joined"]=sum(1 for e in led if e.get("status")=="ok")
        if q.get("status")=="ok" and at["fleet_joined"]>=6:
            v={"generated_at":d.get("generated_at"),"fetch_time_s":d.get("fetch_time"),
               "cq_metrics":sorted((q.get("metrics") or {}).keys()),
               "cq_signals":q.get("signals"),
               "composite_z":q.get("composite_onchain_risk_z"),
               "cq_feed_age_h":q.get("feed_age_h"),"brief_chars":len(q.get("ai_master_brief") or ""),
               "fleet_ledger":led,
               "prices":{k:(d.get("prices_canonical") or {}).get("prices",{}).get(k)
                          for k in ("BTC","ETH")},
               "authority":(d.get("prices_canonical") or {}).get("authority"),
               "source_health":{"ok":(d.get("source_health") or {}).get("ok"),
                                 "total":(d.get("source_health") or {}).get("total"),
                                 "failing":[e for e in ((d.get("source_health") or {}).get("sections") or [])
                                            if e.get("status") not in ("ok","success","MINOR DRIFT")]}}
            R["attempts"].append(at); break
    except Exception as e:
        at["s3_err"]=str(e)[:120]
    R["attempts"].append(at); time.sleep(40)
R["verify"]=v
R["verdict"]="PASS — CryptoQuant + fleet joins live" if v else "FAIL — see attempts"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4359_v5_strict.json","w"),indent=1,default=str)
md=[f"# ops 4359 — strict v5 verify — {R['verdict']}"]
if v:
    md+=[f"- generated {v['generated_at']} fetch {v['fetch_time_s']}s",
         f"- CQ metrics({len(v['cq_metrics'])}): {v['cq_metrics']}",
         f"- CQ signals: {json.dumps(v['cq_signals'])}",
         f"- composite z: {v['composite_z']} | cq-feed age {v['cq_feed_age_h']}h | brief {v['brief_chars']}ch",
         f"- fleet: {sum(1 for e in v['fleet_ledger'] if e['status']=='ok')}/{len(v['fleet_ledger'])} "
         + ", ".join(e['feed']+':'+e['status'] for e in v['fleet_ledger']),
         f"- prices[{v['authority']}]: BTC {json.dumps(v['prices'].get('BTC'))} ETH {json.dumps(v['prices'].get('ETH'))}",
         f"- source_health {v['source_health']['ok']}/{v['source_health']['total']} "
         f"failing={[e['section'] for e in v['source_health']['failing']]}"]
else:
    md+=[f"- attempts: {json.dumps(R['attempts'])[:1200]}"]
open("aws/ops/reports/4359_v5_strict.md","w").write("\n".join(md)+"\n")
print(json.dumps(R,indent=1,default=str))
