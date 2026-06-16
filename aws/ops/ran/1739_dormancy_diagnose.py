import boto3, datetime, json
events=boto3.client("events",region_name="us-east-1")
cw=boto3.client("cloudwatch",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1")
logs=boto3.client("logs",region_name="us-east-1")
end=datetime.datetime.now(datetime.timezone.utc); start=end-datetime.timedelta(days=30)

TIER2=["justhodl-ecb-detail","justhodl-ecb-derived","justhodl-ecb-history","justhodl-euro-fragmentation",
       "ecb-auto-updater","cftc-futures-positioning-agent","justhodl-cot-extremes-scanner",
       "justhodl-backlog","justhodl-forward-orders","justhodl-bond-trace","justhodl-ignition",
       "justhodl-political-stocks","justhodl-fleet-monitor"]
TIER3=["justhodl-crypto-intel","justhodl-crypto-enricher","justhodl-crypto-funding","justhodl-onchain-ratios",
       "bea-economic-agent","justhodl-gdelt-sentiment","justhodl-aaii-sentiment","justhodl-buzz-velocity",
       "benzinga-news-agent","eia-energy-agent"]

# map fn -> rule
fn_rule={}
for pg in events.get_paginator("list_rules").paginate():
    for r in pg["Rules"]:
        try:
            for t in events.list_targets_by_rule(Rule=r["Name"])["Targets"]:
                if ":function:" in t.get("Arn",""):
                    f=t["Arn"].split(":function:")[1].split(":")[0]
                    fn_rule.setdefault(f,[]).append((r["Name"],r.get("State"),r.get("ScheduleExpression"),t.get("Arn")))
        except: pass

def metric(ns,name,dim,val):
    try:
        r=cw.get_metric_statistics(Namespace=ns,MetricName=name,Dimensions=[{"Name":dim,"Value":val}],
            StartTime=start,EndTime=end,Period=2592000,Statistics=["Sum"])
        return int(sum(d["Sum"] for d in r.get("Datapoints",[])))
    except: return -1

def diagnose(fn):
    out={"fn":fn}
    # function exists?
    try:
        c=lam.get_function_configuration(FunctionName=fn)
        out["state"]=c.get("State"); out["last_mod"]=c.get("LastModified","")[:10]; out["lastupd"]=c.get("LastUpdateStatus")
    except Exception as e:
        out["err"]="NO SUCH FUNCTION ("+type(e).__name__+")"; return out
    # rule
    rules=fn_rule.get(fn,[])
    if rules:
        rn,st,se,arn=rules[0]
        out["rule"]=f"{rn} [{st}] {se}"
        out["rule_failed_30d"]=metric("AWS/Events","FailedInvocations","RuleName",rn)
        out["rule_invoked_30d"]=metric("AWS/Events","Invocations","RuleName",rn)
        out["target_arn_tail"]=arn.split(":function:")[1] if ":function:" in arn else arn
    else:
        out["rule"]="NONE (not scheduled)"
    # permission for events to invoke
    try:
        pol=json.loads(lam.get_policy(FunctionName=fn)["Policy"])
        svcs=set()
        for s in pol.get("Statement",[]):
            pr=s.get("Principal",{})
            svcs.add(pr.get("Service") if isinstance(pr,dict) else str(pr))
        out["invoke_perms"]=",".join(sorted(x for x in svcs if x)) or "(none)"
    except lam.exceptions.ResourceNotFoundException:
        out["invoke_perms"]="NO RESOURCE POLICY (nothing may invoke it)"
    except Exception as e:
        out["invoke_perms"]="?"+type(e).__name__
    # fn invocations + errors
    out["fn_inv_30d"]=metric("AWS/Lambda","Invocations","FunctionName",fn)
    out["fn_err_30d"]=metric("AWS/Lambda","Errors","FunctionName",fn)
    # last log
    try:
        ls=logs.describe_log_streams(logGroupName=f"/aws/lambda/{fn}",orderBy="LastEventTime",descending=True,limit=1)
        streams=ls.get("logStreams",[])
        if streams and streams[0].get("lastEventTimestamp"):
            ts=datetime.datetime.fromtimestamp(streams[0]["lastEventTimestamp"]/1000,datetime.timezone.utc)
            out["last_log_days"]=round((end-ts).total_seconds()/86400,1)
            ev=logs.get_log_events(logGroupName=f"/aws/lambda/{fn}",logStreamName=streams[0]["logStreamName"],limit=25,startFromHead=False).get("events",[])
            errs=[e["message"].strip()[:120] for e in ev if any(k in e["message"] for k in ("Error","Traceback","Task timed out","errorMessage","Exception"))]
            if errs: out["last_error"]=errs[-1]
        else: out["last_log_days"]="no streams"
    except logs.exceptions.ResourceNotFoundException:
        out["last_log_days"]="NO LOG GROUP (never invoked)"
    except Exception as e:
        out["last_log_days"]="?"+type(e).__name__
    return out

def show(title,fns):
    print(f"\n{'='*22} {title} {'='*22}")
    for fn in fns:
        d=diagnose(fn)
        if d.get("err"): print(f"\n● {fn}\n    {d['err']}"); continue
        print(f"\n● {fn}  [state={d.get('state')} mod={d.get('last_mod')}]")
        print(f"    rule: {d.get('rule')}")
        if d.get('rule_failed_30d',-1)>=0: print(f"    rule 30d: invoked={d.get('rule_invoked_30d')} FAILED={d.get('rule_failed_30d')}")
        print(f"    invoke perms: {d.get('invoke_perms')}")
        print(f"    fn 30d: inv={d.get('fn_inv_30d')} err={d.get('fn_err_30d')} | last log: {d.get('last_log_days')} days ago")
        if d.get("last_error"): print(f"    LAST ERROR: {d['last_error']}")

show("TIER 2 — scheduled but silent",TIER2)
show("TIER 3 — fully dormant",TIER3)
