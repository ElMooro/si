"""ops 2886 — (a) repair 8 zero-invocation schedules (permission/target), (b) capture the actual
exception for each of the 34 error fns, (c) unscheduled∩stale-output candidates, (d) stale7 detail."""
import os, json, time, traceback, boto3
from datetime import datetime, timezone, timedelta
REGION="us-east-1"; ACCT="857687956942"; B="justhodl-dashboard-live"
R={"ops":2886,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
def guard(name):
    def deco(fn):
        def run(*a,**k):
            try: return fn(*a,**k)
            except Exception:
                R["errors"][name]=traceback.format_exc()[-400:]; return None
        return run
    return deco
lam=boto3.client("lambda",region_name=REGION); ev=boto3.client("events",region_name=REGION)
logs=boto3.client("logs",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
FULL=json.loads(s3.get_object(Bucket=B,Key="data/_audit/gap-sweep.json")["Body"].read())

@guard("repair_schedules")
def repair_schedules():
    zero=FULL.get("scheduled_zero_invocations_72h") or []
    out={}
    rules=[]
    p=ev.get_paginator("list_rules")
    for pg in p.paginate():
        rules += [r for r in pg["Rules"] if r.get("ScheduleExpression")]
    for fn in zero:
        info={"fixed":[]}
        # find rules targeting fn
        myrules=[]
        for r in rules:
            try: tg=ev.list_targets_by_rule(Rule=r["Name"]).get("Targets",[])
            except Exception: continue
            if any(t.get("Arn","").endswith(":function:"+fn) for t in tg): myrules.append(r["Name"])
        info["rules"]=myrules
        # permission present?
        has_perm=False
        try:
            pol=json.loads(lam.get_policy(FunctionName=fn)["Policy"])
            has_perm=any("events.amazonaws.com"==st.get("Principal",{}).get("Service") for st in pol.get("Statement",[]))
        except Exception: pass
        info["had_events_permission"]=has_perm
        if myrules and not has_perm:
            try:
                lam.add_permission(FunctionName=fn,StatementId="events-repair-2886",Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,myrules[0]))
                info["fixed"].append("added events permission")
            except Exception as e: info["perm_err"]=str(e)[:100]
        if myrules:
            # re-put target to refresh binding
            try:
                ev.put_targets(Rule=myrules[0],Targets=[{"Id":"1","Arn":"arn:aws:lambda:%s:%s:function:%s"%(REGION,ACCT,fn)}])
                info["fixed"].append("re-put target")
            except Exception as e: info["target_err"]=str(e)[:100]
        # test fire (async)
        try: lam.invoke(FunctionName=fn,InvocationType="Event"); info["fixed"].append("test-invoked")
        except Exception as e: info["invoke_err"]=str(e)[:100]
        out[fn]=info
    R["schedule_repairs"]=out
    return True

@guard("error_evidence")
def error_evidence():
    errfns=[n for n,_ in (FULL.get("top_errors_72h") or [])]
    start=int((datetime.now(timezone.utc)-timedelta(days=3)).timestamp()*1000)
    ev_out={}
    for fn in errfns[:34]:
        try:
            res=logs.filter_log_events(logGroupName="/aws/lambda/"+fn,startTime=start,
                filterPattern="?ERROR ?Traceback ?errorMessage ?Task timed out",limit=8)
            lines=[e["message"].strip()[:180] for e in res.get("events",[])]
            keep=[l for l in lines if any(k in l for k in ("Error","error","Traceback","timed out","Exception"))][-3:]
            ev_out[fn]=keep or lines[-2:]
        except Exception as e: ev_out[fn]=["log-read-err: "+str(e)[:80]]
    R["error_evidence"]=ev_out
    return True

@guard("unscheduled_stale")
def unscheduled_stale():
    reg=(json.loads(s3.get_object(Bucket=B,Key="data/engine-registry.json")["Body"].read()) or {}).get("engines",{})
    stale={x["k"]:x["age_d"] for x in (FULL.get("stale30") or [])+(FULL.get("stale7") or [])}
    unsched={o["fn"] for o in FULL.get("unscheduled") or []}
    cands=[]
    for eng,meta in reg.items():
        if eng not in unsched: continue
        souts=[o for o in meta.get("outs",[]) if o in stale]
        if souts: cands.append({"fn":eng,"stale_outs":[(o,stale[o]) for o in souts]})
    cands.sort(key=lambda c:-max(a for _,a in c["stale_outs"]))
    R["unscheduled_with_stale_outputs"]=cands[:20]; R["n_unsched_stale"]=len(cands)
    R["stale7_list"]=FULL.get("stale7")
    return True

repair_schedules(); error_evidence(); unscheduled_stale()
R["status"]="OK" if not R["errors"] else "PARTIAL"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3800])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2886_diagnose_repair.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2886 COMPLETE")
