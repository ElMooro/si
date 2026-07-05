"""ops 2901 — steady-state pulse + institutional hygiene: fleet errors post-propagation, periphery
freshness, ENABLE S3 versioning+lifecycle & DDB PITR where off, SSM census, alert-channel liveness."""
import os, json, time, traceback, boto3
from datetime import datetime, timezone, timedelta
REGION="us-east-1"; B="justhodl-dashboard-live"
R={"ops":2901,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
def guard(n):
    def d(f):
        def r(*a,**k):
            try: return f(*a,**k)
            except Exception:
                R["errors"][n]=traceback.format_exc()[-380:]; return None
        return r
    return d
lam=boto3.client("lambda",region_name=REGION); cw=boto3.client("cloudwatch",region_name=REGION)
s3=boto3.client("s3",region_name=REGION); ddb=boto3.client("dynamodb",region_name=REGION)
ssm=boto3.client("ssm",region_name=REGION)

@guard("errors_steady")
def errors_steady():
    names=[]
    p=lam.get_paginator("list_functions")
    for pg in p.paginate(): names+=[f["FunctionName"] for f in pg["Functions"]]
    end=datetime.now(timezone.utc); start=datetime(2026,7,5,19,15,tzinfo=timezone.utc)
    errs={}
    for i in range(0,len(names),240):
        chunk=names[i:i+240]
        q=[{"Id":f"e{j}","MetricStat":{"Metric":{"Namespace":"AWS/Lambda","MetricName":"Errors",
            "Dimensions":[{"Name":"FunctionName","Value":n}]},"Period":86400,"Stat":"Sum"},"ReturnData":True}
           for j,n in enumerate(chunk)]
        res=cw.get_metric_data(MetricDataQueries=q,StartTime=start,EndTime=end)
        for r_ in res["MetricDataResults"]:
            v=int(sum(r_.get("Values") or [0]))
            if v>0: errs[chunk[int(r_["Id"][1:])]]=v
    R["post_propagation_window_h"]=round((end-start).total_seconds()/3600,1)
    R["fns_erroring_now"]=sorted(errs.items(),key=lambda x:-x[1])[:10]
    R["n_erroring_now"]=len(errs)
    return True

@guard("periphery_fresh")
def periphery_fresh():
    now=datetime.now(timezone.utc); out={}
    for k in ("buyback-yield-ranking","capital-inflows","cta-trend-exhaust","divcut-warning",
              "earnings-quality","gap-fill-confirm","reit-nav-discount","spac-floor-warrant"):
        try:
            h=s3.head_object(Bucket=B,Key=f"data/{k}.json")
            out[k]=round((now-h["LastModified"]).total_seconds()/3600,1)
        except Exception: out[k]="missing"
    R["periphery_ages_h"]=out
    return True

@guard("s3_versioning")
def s3_versioning():
    v=s3.get_bucket_versioning(Bucket=B).get("Status")
    R["s3_versioning"]={"was":v or "Off"}
    if v!="Enabled":
        s3.put_bucket_versioning(Bucket=B,VersioningConfiguration={"Status":"Enabled"})
        R["s3_versioning"]["action"]="ENABLED"
    # lifecycle: expire noncurrent versions after 30d (cost cap), keep existing rules
    try: lc=s3.get_bucket_lifecycle_configuration(Bucket=B).get("Rules",[])
    except Exception: lc=[]
    if not any(r.get("ID")=="noncurrent-30d" for r in lc):
        lc.append({"ID":"noncurrent-30d","Status":"Enabled","Filter":{"Prefix":""},
                   "NoncurrentVersionExpiration":{"NoncurrentDays":30}})
        s3.put_bucket_lifecycle_configuration(Bucket=B,LifecycleConfiguration={"Rules":lc})
        R["s3_versioning"]["lifecycle"]="noncurrent-30d ADDED"
    return True

@guard("ddb_pitr")
def ddb_pitr():
    out={}
    tables=[]
    p=ddb.get_paginator("list_tables")
    for pg in p.paginate(): tables+=pg["TableNames"]
    for t in [x for x in tables if "justhodl" in x.lower()]:
        st=ddb.describe_continuous_backups(TableName=t)["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]["PointInTimeRecoveryStatus"]
        if st!="ENABLED":
            ddb.update_continuous_backups(TableName=t,PointInTimeRecoverySpecification={"PointInTimeRecoveryEnabled":True})
            out[t]="ENABLED-now"
        else: out[t]="already"
    R["ddb_pitr"]=out
    return True

@guard("ssm_census")
def ssm_census():
    names=[]
    p=ssm.get_paginator("describe_parameters")
    for pg in p.paginate(ParameterFilters=[{"Key":"Name","Option":"BeginsWith","Values":["/justhodl"]}]):
        names+=[x["Name"] for x in pg["Parameters"]]
    R["ssm"]={"n":len(names),"names":sorted(names)[:20]}
    return True

@guard("alert_liveness")
def alert_liveness():
    now=datetime.now(timezone.utc); out={}
    for k in ("data/_fleet-monitor-alert-history.json","data/_freshness-alert-history.json"):
        try:
            h=s3.head_object(Bucket=B,Key=k)
            out[k.split("/")[-1]]=round((now-h["LastModified"]).total_seconds()/3600,1)
        except Exception: out[k.split("/")[-1]]="missing"
    R["alert_history_ages_h"]=out
    return True

errors_steady(); periphery_fresh(); s3_versioning(); ddb_pitr(); ssm_census(); alert_liveness()
R["status"]="OK" if not R["errors"] else "PARTIAL"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3200])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2901_hygiene_pulse.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2901 COMPLETE")
