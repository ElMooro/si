"""ops 2891 — post-fix proof + unscheduled classification:
(a) per-fn Errors since fix-time (prove cascade dead); (b) secretary/ka/price-redundancy freshness;
(c) classify 280 unscheduled: event-source-mapped / s3-notified / url / sub-invoked / repo-present-untriggered / AWS-ONLY ORPHANS;
(d) fleet-monitor '1 dep down' identity. Writes data/_audit/unscheduled-classification.json."""
import os, json, time, traceback, boto3
from datetime import datetime, timezone, timedelta
REGION="us-east-1"; B="justhodl-dashboard-live"
FIX_TS=datetime(2026,7,5,17,45,tzinfo=timezone.utc)
R={"ops":2891,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
def guard(n):
    def d(f):
        def r(*a,**k):
            try: return f(*a,**k)
            except Exception:
                R["errors"][n]=traceback.format_exc()[-400:]; return None
        return r
    return d
lam=boto3.client("lambda",region_name=REGION); cw=boto3.client("cloudwatch",region_name=REGION)
s3=boto3.client("s3",region_name=REGION)
FULL=json.loads(s3.get_object(Bucket=B,Key="data/_audit/gap-sweep.json")["Body"].read())
CI=json.load(open("aws/ops/pending/_class_inputs.json"))
ERRFNS=[n for n,_ in (FULL.get("top_errors_72h") or [])]

@guard("error_delta")
def error_delta():
    end=datetime.now(timezone.utc)
    q=[{"Id":f"e{j}","MetricStat":{"Metric":{"Namespace":"AWS/Lambda","MetricName":"Errors",
        "Dimensions":[{"Name":"FunctionName","Value":n}]},"Period":86400,"Stat":"Sum"},"ReturnData":True}
       for j,n in enumerate(ERRFNS)]
    res=cw.get_metric_data(MetricDataQueries=q,StartTime=FIX_TS,EndTime=end)
    delta={}
    for r_ in res["MetricDataResults"]:
        n=ERRFNS[int(r_["Id"][1:])]; delta[n]=int(sum(r_.get("Values") or [0]))
    still=[(n,v) for n,v in delta.items() if v>0]
    R["postfix_window_hours"]=round((end-FIX_TS).total_seconds()/3600,1)
    R["errfns_still_erroring"]=sorted(still,key=lambda x:-x[1])
    R["errfns_now_clean"]=len([n for n,v in delta.items() if v==0])
    return True

@guard("freshness")
def freshness():
    now=datetime.now(timezone.utc); out={}
    for k in ("data/fred-cache-secretary.json","data/secretary-latest.json","data/ka-analysis.json",
              "data/khalid-analysis.json","data/price-redundancy.json","data/vix-backwardation-trigger.json"):
        try:
            h=s3.head_object(Bucket=B,Key=k)
            out[k]=round((now-h["LastModified"]).total_seconds()/3600,1)
        except Exception: out[k]="missing"
    R["ages_hours"]=out
    return True

@guard("classify")
def classify():
    unsched=sorted({o["fn"] for o in FULL.get("unscheduled") or []})
    urls=set(FULL.get("url_fns") or [])
    invoked=set(CI["invoked"]); repo=set(CI["repo_dirs"])
    esm=set()
    p=lam.get_paginator("list_event_source_mappings")
    for pg in p.paginate():
        for m in pg.get("EventSourceMappings",[]):
            arn=m.get("FunctionArn","")
            if ":function:" in arn: esm.add(arn.split(":function:")[-1].split(":")[0])
    s3fns=set()
    try:
        nc=s3.get_bucket_notification_configuration(Bucket=B)
        for c in nc.get("LambdaFunctionConfigurations",[]) or []:
            a=c.get("LambdaFunctionArn","")
            if ":function:" in a: s3fns.add(a.split(":function:")[-1])
    except Exception: pass
    cls={"event_source_mapped":[],"s3_notified":[],"url_serving":[],"sub_invoked":[],
         "repo_present_untriggered":[],"aws_only_orphans":[]}
    for fn in unsched:
        if fn in esm: cls["event_source_mapped"].append(fn)
        elif fn in s3fns: cls["s3_notified"].append(fn)
        elif fn in urls: cls["url_serving"].append(fn)
        elif fn in invoked: cls["sub_invoked"].append(fn)
        elif fn in repo: cls["repo_present_untriggered"].append(fn)
        else: cls["aws_only_orphans"].append(fn)
    doc={"generated_at":R["ts"],"counts":{k:len(v) for k,v in cls.items()},"classes":cls,
         "note":"aws_only_orphans = functions in AWS with no repo dir, no schedule, no trigger, no URL, not invoked — retirement candidates (require Khalid approval to delete)."}
    s3.put_object(Bucket=B,Key="data/_audit/unscheduled-classification.json",Body=json.dumps(doc,ensure_ascii=False).encode(),ContentType="application/json")
    R["classification_counts"]=doc["counts"]
    R["orphans"]=cls["aws_only_orphans"][:30]
    R["untriggered_sample"]=cls["repo_present_untriggered"][:20]
    return True

@guard("dep_down")
def dep_down():
    try:
        fm=json.loads(s3.get_object(Bucket=B,Key="data/_fleet-monitor.json")["Body"].read())
        deps=fm.get("deps") or fm.get("dependencies") or {}
        R["dep_down"]=[k for k,v in deps.items() if str(v.get("status") if isinstance(v,dict) else v).lower() in ("down","red","fail")][:5] or str(deps)[:200]
    except Exception as e: R["dep_down"]="read-err:"+str(e)[:60]
    return True

error_delta(); freshness(); classify(); dep_down()
R["status"]="OK" if not R["errors"] else "PARTIAL"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3400])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2891_postfix_verify.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2891 COMPLETE")
