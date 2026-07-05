"""ops 2885 — GROUND-TRUTH GAP SWEEP: unscheduled fns, disabled/broken rules, silent-stale feeds,
zero-invocation scheduled fns, top error-ing fns. Full lists -> s3://.../data/_audit/gap-sweep.json."""
import os, json, time, traceback, boto3
from datetime import datetime, timezone, timedelta
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"
R={"ops":2885,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
FULL={"ts":R["ts"]}
def guard(name):
    def deco(fn):
        def run(*a,**k):
            try: return fn(*a,**k)
            except Exception:
                R["errors"][name]=traceback.format_exc()[-450:]; return None
        return run
    return deco
lam=boto3.client("lambda",region_name=REGION); ev=boto3.client("events",region_name=REGION)
cw=boto3.client("cloudwatch",region_name=REGION); s3=boto3.client("s3",region_name=REGION)

@guard("functions")
def functions():
    fns={}; p=lam.get_paginator("list_functions")
    for pg in p.paginate():
        for f in pg["Functions"]:
            fns[f["FunctionName"]]={"mod":f["LastModified"][:10]}
    FULL["functions"]=sorted(fns); R["n_functions"]=len(fns); return fns

@guard("rules")
def rules(fns):
    sched=set(); bad_targets=[]; disabled=[]
    p=ev.get_paginator("list_rules")
    for pg in p.paginate():
        for r in pg["Rules"]:
            if not r.get("ScheduleExpression"): continue
            tg=ev.list_targets_by_rule(Rule=r["Name"]).get("Targets",[])
            tfns=[t["Arn"].split(":function:")[-1] for t in tg if ":function:" in t.get("Arn","")]
            if r.get("State")=="DISABLED": disabled.append({"rule":r["Name"],"fns":tfns})
            for tf in tfns:
                if tf in fns: sched.add(tf)
                else: bad_targets.append({"rule":r["Name"],"missing_fn":tf})
            if not tg: bad_targets.append({"rule":r["Name"],"missing_fn":"(no targets)"})
    FULL["disabled_rules"]=disabled; FULL["bad_targets"]=bad_targets
    R["n_scheduled_fns"]=len(sched); R["n_disabled_rules"]=len(disabled); R["n_bad_targets"]=len(bad_targets)
    return sched

@guard("urls")
def urls(fns):
    u=set()
    for name in fns:
        try: lam.get_function_url_config(FunctionName=name); u.add(name)
        except Exception: pass
    FULL["url_fns"]=sorted(u); R["n_url_fns"]=len(u); return u

@guard("unscheduled")
def unscheduled(fns,sched,u):
    ex=("monitor","proxy","bot","webhook","api","ask","chat","auth","router","telegram")
    out=[]
    for name in fns:
        if name in sched or name in u: continue
        out.append({"fn":name,"exempt_hint":any(k in name for k in ex)})
    FULL["unscheduled"]=sorted(out,key=lambda x:x["fn"]); R["n_unscheduled"]=len(out)
    R["unscheduled_nonexempt_sample"]=[o["fn"] for o in out if not o["exempt_hint"]][:25]
    return out

@guard("freshness")
def freshness():
    mon=None
    try: mon=json.loads(s3.get_object(Bucket=B,Key="data/_freshness-monitor.json")["Body"].read())
    except Exception: pass
    R["freshness_monitor"]={"present":bool(mon),"stale_top":(mon or {}).get("stale",[])[:12] if isinstance((mon or {}).get("stale"),list) else str((mon or {}))[:200]}
    now=datetime.now(timezone.utc); stale7=[]; stale30=[]
    pg=s3.get_paginator("list_objects_v2")
    for page in pg.paginate(Bucket=B,Prefix="data/",Delimiter="/"):
        for o in page.get("Contents",[]):
            k=o["Key"]
            if not k.endswith(".json") or k.startswith("data/_"): continue
            age=(now-o["LastModified"]).days
            if age>=30: stale30.append({"k":k,"age_d":age})
            elif age>=7: stale7.append({"k":k,"age_d":age})
    stale30.sort(key=lambda x:-x["age_d"]); stale7.sort(key=lambda x:-x["age_d"])
    FULL["stale30"]=stale30; FULL["stale7"]=stale7
    R["n_stale_7_29d"]=len(stale7); R["n_stale_30d_plus"]=len(stale30)
    R["stale30_top"]=stale30[:15]
    return True

@guard("cw_metrics")
def cw_metrics(fns,sched):
    names=sorted(fns)
    end=datetime.now(timezone.utc); start=end-timedelta(days=3)
    errs={}; invs={}
    for i in range(0,len(names),240):
        chunk=names[i:i+240]; q=[]
        for j,n in enumerate(chunk):
            for met,store in (("Errors","e"),("Invocations","i")):
                q.append({"Id":f"{store}{j}","MetricStat":{"Metric":{"Namespace":"AWS/Lambda","MetricName":met,
                    "Dimensions":[{"Name":"FunctionName","Value":n}]},"Period":259200,"Stat":"Sum"},"ReturnData":True})
        res=cw.get_metric_data(MetricDataQueries=q,StartTime=start,EndTime=end)
        for r_ in res["MetricDataResults"]:
            j=int(r_["Id"][1:]); n=chunk[j]; v=sum(r_.get("Values") or [0])
            (errs if r_["Id"][0]=="e" else invs)[n]=v
    top_err=sorted(((n,int(v)) for n,v in errs.items() if v>0),key=lambda x:-x[1])
    zero_inv=[n for n in sched if invs.get(n,0)==0]
    FULL["top_errors_72h"]=top_err; FULL["scheduled_zero_invocations_72h"]=sorted(zero_inv)
    R["n_fns_with_errors_72h"]=len(top_err); R["top_errors_72h"]=top_err[:15]
    R["n_scheduled_zero_inv_72h"]=len(zero_inv); R["scheduled_zero_inv_sample"]=sorted(zero_inv)[:20]
    return True

fns=functions() or {}
sched=rules(fns) or set()
u=urls(fns) or set()
unscheduled(fns,sched,u); freshness(); cw_metrics(fns,sched)
s3.put_object(Bucket=B,Key="data/_audit/gap-sweep.json",Body=json.dumps(FULL,ensure_ascii=False,default=str).encode(),ContentType="application/json")
R["status"]="OK" if not R["errors"] else "PARTIAL"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3400])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2885_gap_sweep.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2885 COMPLETE")
