"""ops 2892 — closer: brief-router live traceback; secretary+ka/khalid metrics via scheduled payload
(+cache refresh proof); untriggered-269 output-freshness classification v2; delete tmp-* orphans."""
import os, json, time, traceback, boto3
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"
R={"ops":2892,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
def guard(n):
    def d(f):
        def r(*a,**k):
            try: return f(*a,**k)
            except Exception:
                R["errors"][n]=traceback.format_exc()[-380:]; return None
        return r
    return d
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=290,retries={"max_attempts":0}))
logs=boto3.client("logs",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
EVT={"source":"aws.events"}
def age(k):
    try:
        h=s3.head_object(Bucket=B,Key=k)
        return round((datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/3600,1)
    except Exception: return "missing"

@guard("brief_router_tail")
def brief_router_tail():
    st=logs.describe_log_streams(logGroupName="/aws/lambda/justhodl-ai-brief-router",orderBy="LastEventTime",descending=True,limit=2)["logStreams"]
    lines=[]
    for s_ in st:
        evs=logs.get_log_events(logGroupName="/aws/lambda/justhodl-ai-brief-router",logStreamName=s_["logStreamName"],limit=20,startFromHead=False)["events"]
        lines += [e["message"].strip()[:180] for e in evs]
    R["brief_router_tail"]=[l for l in lines if any(k in l for k in ("Error","Traceback","errorMessage","timed out","line "))][-8:] or lines[-6:]
    return True

@guard("metrics_refresh")
def metrics_refresh():
    out={}
    for fn,keys in (("justhodl-financial-secretary",["data/fred-cache-secretary.json","data/secretary-latest.json"]),
                    ("justhodl-khalid-metrics",["data/khalid-analysis.json"]),
                    ("justhodl-ka-metrics",["data/ka-analysis.json"])):
        rec={"before":{k:age(k) for k in keys}}
        try:
            p=lam.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=json.dumps(EVT).encode())
            rec["fn_error"]=p.get("FunctionError"); rec["resp"]=p["Payload"].read().decode()[:130]
        except Exception as e: rec["invoke_err"]=str(e)[:110]
        time.sleep(3)
        rec["after"]={k:age(k) for k in keys}
        out[fn]=rec
    R["metrics_refresh"]=out
    return True

@guard("classify_v2")
def classify_v2():
    reg=(json.loads(s3.get_object(Bucket=B,Key="data/engine-registry.json")["Body"].read()) or {}).get("engines",{})
    doc=json.loads(s3.get_object(Bucket=B,Key="data/_audit/unscheduled-classification.json")["Body"].read())
    unt=doc["classes"].get("repo_present_untriggered",[])
    fresh_sup=[]; stale_unique=[]; missing=[]; no_outs=[]
    for fn in unt:
        outs=[o for o in (reg.get(fn,{}).get("outs") or []) if o.startswith("data/") and not o.startswith("data/_")]
        if not outs: no_outs.append(fn); continue
        a=age(outs[0])
        if a=="missing": missing.append(fn)
        elif isinstance(a,float) and a<48: fresh_sup.append(fn)
        else: stale_unique.append({"fn":fn,"out":outs[0],"age_h":a})
    doc["classes"]["untriggered_fresh_superseded"]=sorted(fresh_sup)
    doc["classes"]["untriggered_missing_outputs"]=sorted(missing)
    doc["classes"]["untriggered_no_outs"]=sorted(no_outs)
    doc["classes"]["untriggered_stale_unique"]=stale_unique
    doc["counts_v2"]={"fresh_superseded":len(fresh_sup),"missing_outputs":len(missing),
                      "no_outs":len(no_outs),"stale_unique":len(stale_unique)}
    doc["v2_note"]=("fresh_superseded: another live engine writes the same feed (dormant duplicate). "
                    "missing_outputs/no_outs: never-productionized or read-only code. "
                    "stale_unique: would be real gaps — expected ~0 after the sweep.")
    s3.put_object(Bucket=B,Key="data/_audit/unscheduled-classification.json",Body=json.dumps(doc,ensure_ascii=False,default=str).encode(),ContentType="application/json")
    R["untriggered_v2"]=doc["counts_v2"]; R["stale_unique"]=stale_unique[:10]
    return True

@guard("orphans")
def orphans():
    deleted={}
    for fn in ("tmp-brain-purge","tmp-dd2","tmp-fbuild","tmp-rb","tmp-walk"):
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            lam.delete_function(FunctionName=fn)
            deleted[fn]={"deleted":True,"was":c.get("Runtime"),"last_mod":c.get("LastModified")}
        except Exception as e: deleted[fn]={"deleted":False,"err":str(e)[:80]}
    try:
        c=lam.get_function_configuration(FunctionName="justhodl-signal-registry-ingest")
        R["signal_registry_ingest_flag"]={"runtime":c.get("Runtime"),"last_mod":c.get("LastModified"),
            "size":c.get("CodeSize"),"desc":c.get("Description","")[:100],
            "action":"FLAGGED for Khalid — no repo dir/trigger; approve deletion or keep"}
    except Exception as e: R["signal_registry_ingest_flag"]="read-err:"+str(e)[:60]
    R["tmp_orphans_deleted"]=deleted
    return True

brief_router_tail(); metrics_refresh(); classify_v2(); orphans()
R["status"]="OK" if not R["errors"] else "PARTIAL"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2892_closer.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2892 COMPLETE")
