"""ops 4363 — unified verify for the five rails (runs last in this batch).
Polls until engine v5.2 writes (ratchet block present), invokes twice for
history accumulation, then asserts: anomaly rail (warming ok), ratchet, lake
partition today, fan-in rule ENABLED with target, catalog growth reflected
in engine's catalog_metrics, push plumbing shape."""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-crypto-intel"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4363,"started":datetime.now(timezone.utc).isoformat(),"attempts":[]}
d=None
for i in range(7):
    at={"n":i+1}
    try:
        inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
        at["fn_err"]=inv.get("FunctionError"); inv["Payload"].read()
    except Exception as e:
        at["invoke_err"]=str(e)[:100]
    try:
        dd=json.loads(s3.get_object(Bucket=BUCKET,Key="crypto-intel.json")["Body"].read())
        at["version"]=dd.get("version")
        if dd.get("version")=="5.2" and (dd.get("coverage") or {}).get("ratchet"):
            d=dd; R["attempts"].append(at); break
    except Exception as e:
        at["s3_err"]=str(e)[:100]
    R["attempts"].append(at); time.sleep(40)

if d:
    time.sleep(8)
    try:
        lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")["Payload"].read()
        d=json.loads(s3.get_object(Bucket=BUCKET,Key="crypto-intel.json")["Body"].read())
    except Exception:
        pass
    hist={}
    try:
        hist=json.loads(s3.get_object(Bucket=BUCKET,Key="data/crypto-intel-history.json")["Body"].read())
    except Exception as e:
        R["hist_err"]=str(e)[:100]
    lake=[]
    try:
        pre=datetime.now(timezone.utc).strftime("lake/crypto-intel/dt=%Y-%m-%d/")
        lake=[o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET,Prefix=pre).get("Contents",[])]
    except Exception as e:
        R["lake_err"]=str(e)[:100]
    rule={}
    try:
        rr=ev.describe_rule(Name="justhodl-crypto-fanin")
        tg=ev.list_targets_by_rule(Rule="justhodl-crypto-fanin").get("Targets",[])
        rule={"state":rr.get("State"),"targets":len(tg),
              "hits_fn":any(FN in (x.get("Arn") or "") for x in tg)}
    except Exception as e:
        rule={"err":str(e)[:100]}
    cov=d.get("coverage") or {}; q=d.get("cryptoquant") or {}; an=d.get("anomaly") or {}
    R["verify"]={
        "version":d.get("version"),"generated_at":d.get("generated_at"),
        "coverage":{"total_leaves":cov.get("total_leaves"),
                     "ratchet":cov.get("ratchet")},
        "anomaly":{"status":an.get("status"),"history_points":an.get("history_points"),
                    "kpis_tracked":an.get("kpis_tracked"),
                    "flagged":an.get("anomalies")},
        "history_file_points":len(hist.get("points") or []),
        "lake_partitions_today":len(lake),"lake_sample":lake[:3],
        "fanin_rule":rule,
        "cq":{"catalog_metrics":len(q.get("catalog_metrics") or {}),
               "n_catalog":q.get("n_catalog_metrics")},
        "push":{"events_field":isinstance(d.get("push_events"),list),
                 "events":d.get("push_events"),"sent":d.get("push_sent"),
                 "error":d.get("push_error")},
        "v52_post_error":d.get("v52_post_error"),
    }
    v=R["verify"]
    ok=(v["coverage"]["ratchet"] is not None and v["history_file_points"]>=2
        and v["lake_partitions_today"]>=1 and rule.get("state")=="ENABLED"
        and rule.get("hits_fn") and v["cq"]["catalog_metrics"]>=22
        and v["push"]["events_field"] and not v["v52_post_error"])
    R["verdict"]="PASS — all five rails live" if ok else "PARTIAL — see verify"
else:
    R["verdict"]="FAIL — v5.2 never appeared"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4363_rails_verify.json","w"),indent=1,default=str)
v=R.get("verify",{})
open("aws/ops/reports/4363_rails_verify.md","w").write(
    f"# ops 4363 — rails verify — {R['verdict']}\n"
    f"- v{v.get('version')} @ {v.get('generated_at')} | leaves {v.get('coverage',{}).get('total_leaves')}\n"
    f"- ratchet: {json.dumps(v.get('coverage',{}).get('ratchet'))}\n"
    f"- anomaly: {json.dumps(v.get('anomaly'))}\n"
    f"- history points: {v.get('history_file_points')} | lake today: {v.get('lake_partitions_today')} {v.get('lake_sample')}\n"
    f"- fan-in: {json.dumps(v.get('fanin_rule'))}\n"
    f"- CQ catalog in engine: {json.dumps(v.get('cq'))}\n"
    f"- push: {json.dumps(v.get('push'))}\n"
    f"- v52_post_error: {v.get('v52_post_error')}\n")
print(json.dumps(R,indent=1,default=str))
