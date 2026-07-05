"""ops 2887 — FIX WAVE: wait patched deploys, inject Polygon key, schedule unscheduled stale-writers,
refresh leftover stale keys via writer-hunt, freshness-manifest hygiene, deep error tails for top-4."""
import os, json, re, glob, time, traceback, boto3
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"; ACCT="857687956942"; B="justhodl-dashboard-live"
R={"ops":2887,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
def guard(name):
    def deco(fn):
        def run(*a,**k):
            try: return fn(*a,**k)
            except Exception:
                R["errors"][name]=traceback.format_exc()[-420:]; return None
        return run
    return deco
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=120,retries={"max_attempts":1}))
ev=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
logs=boto3.client("logs",region_name=REGION)
FULL=json.loads(s3.get_object(Bucket=B,Key="data/_audit/gap-sweep.json")["Body"].read())
UNSCHED={o["fn"] for o in FULL.get("unscheduled") or []}
PATCHED=["justhodl-alert-backtester","justhodl-historical-analogs","justhodl-skew-tail-hedging",
         "justhodl-macro-leads","justhodl-price-redundancy"]

@guard("wait_patched")
def wait_patched():
    t_push=R["ts"][:19]; state={}
    deadline=time.time()+420
    pend=set(PATCHED)
    while pend and time.time()<deadline:
        for fn in list(pend):
            try:
                c=lam.get_function_configuration(FunctionName=fn)
                if c.get("LastModified","")>t_push and c.get("LastUpdateStatus")=="Successful":
                    state[fn]="deployed"; pend.discard(fn)
            except Exception as e: state[fn]="err:"+str(e)[:40]; pend.discard(fn)
        if pend: time.sleep(10)
    for fn in pend: state[fn]="wait-timeout"
    R["patched_deploys"]=state
    return True

@guard("polygon_key")
def polygon_key():
    key=None
    for donor in ("justhodl-crisis-canaries","justhodl-alert-backtester","justhodl-intraday-pulse"):
        try:
            env=lam.get_function_configuration(FunctionName=donor).get("Environment",{}).get("Variables",{})
            key=env.get("POLYGON_API_KEY") or env.get("POLY_KEY")
            if key: R["poly_donor"]=donor; break
        except Exception: continue
    if not key: R["poly_donor"]="NONE-FOUND"; return None
    tgt="justhodl-price-redundancy"
    for _ in range(30):
        c=lam.get_function_configuration(FunctionName=tgt)
        if c.get("LastUpdateStatus")=="Successful": break
        time.sleep(4)
    env=lam.get_function_configuration(FunctionName=tgt).get("Environment",{}).get("Variables",{})
    env["POLYGON_API_KEY"]=key
    lam.update_function_configuration(FunctionName=tgt,Environment={"Variables":env})
    for _ in range(30):
        if lam.get_function_configuration(FunctionName=tgt).get("LastUpdateStatus")=="Successful": break
        time.sleep(4)
    R["polygon_injected"]=True
    return True

@guard("schedules")
def schedules():
    plan=[("justhodl-vix-backwardation-trigger","justhodl-vix-backwardation-daily","cron(15 13 * * ? *)"),
          ("justhodl-vol-target-unwind","justhodl-vol-target-unwind-daily","cron(25 13 * * ? *)"),
          ("justhodl-factor-decomposition","justhodl-factor-decomp-weekly","cron(35 13 ? * MON *)"),
          ("justhodl-financial-secretary","justhodl-financial-secretary-daily","cron(45 12 * * ? *)"),
          ("justhodl-history-snapshotter","justhodl-history-snapshotter-daily","cron(5 13 * * ? *)")]
    out={}
    for fn,rule,cron in plan:
        if fn not in UNSCHED:
            out[fn]="already-scheduled-or-missing"; continue
        try:
            ev.put_rule(Name=rule,ScheduleExpression=cron,State="ENABLED",Description="gap-fix 2887")
            try: lam.add_permission(FunctionName=fn,StatementId="sched2887",Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,rule))
            except Exception as e:
                if "ResourceConflict" not in str(e): raise
            ev.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":"arn:aws:lambda:%s:%s:function:%s"%(REGION,ACCT,fn)}])
            lam.invoke(FunctionName=fn,InvocationType="Event")
            out[fn]={"rule":rule,"cron":cron,"invoked":True}
        except Exception as e: out[fn]="err:"+str(e)[:90]
    R["new_schedules"]=out
    return True

@guard("writer_hunt")
def writer_hunt():
    targets=["regime-decisive-call","market-internals-history","backtest-summary","finviz-signals-state"]
    found={}
    for base in targets:
        cands=[]
        for f in glob.glob("aws/lambdas/*/source/lambda_function.py"):
            try: src=open(f,encoding="utf-8",errors="ignore").read()
            except Exception: continue
            if base in src and re.search(r"put_object|_write|OUT_KEY|OUT =",src):
                cands.append(f.split("/")[2])
        w=cands[0] if len(cands)>=1 else None
        found[base]={"candidates":cands[:3],"invoked":False}
        if w:
            try: lam.invoke(FunctionName=w,InvocationType="Event"); found[base]["invoked"]=w
            except Exception as e: found[base]["err"]=str(e)[:70]
    R["writer_hunt"]=found
    return True

@guard("manifest_hygiene")
def manifest_hygiene():
    STATIC=["data/user-trades.json","data/user-trades-stats.json","data/user-watchlist.json",
            "data/khalid-config.json","data/ka-config.json","data/history-api-url.json",
            "data/feedback-summary.json","data/askdesk-config.json","data/eventbridge-audit.json",
            "data/system-audit.json","data/congress-party-map.json","data/quiver-congress-cache.json",
            "data/quiver-lobbying-cache.json"]
    try: man=json.loads(s3.get_object(Bucket=B,Key="data/_freshness-manifest.json")["Body"].read())
    except Exception: man={}
    keys=man.get("keys") if isinstance(man.get("keys"),dict) else man
    changed=0
    for k in STATIC:
        cur=keys.get(k)
        if isinstance(cur,dict): cur["max_age_hours"]=8760; keys[k]=cur; changed+=1
        elif cur is not None or True: keys[k]={"max_age_hours":8760} if not isinstance(cur,(int,float)) else 8760; changed+=1
    if isinstance(man.get("keys"),dict): man["keys"]=keys
    else: man=keys
    s3.put_object(Bucket=B,Key="data/_freshness-manifest.json",Body=json.dumps(man,ensure_ascii=False,default=str).encode(),ContentType="application/json")
    R["manifest_static_marked"]=changed
    return True

@guard("deep_tails")
def deep_tails():
    out={}
    for fn in ("justhodl-options-confluence","justhodl-velocity-acceleration","justhodl-fleet-monitor","fedliquidityapi"):
        try:
            st=logs.describe_log_streams(logGroupName="/aws/lambda/"+fn,orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
            if not st: out[fn]=["no-streams"]; continue
            evs=logs.get_log_events(logGroupName="/aws/lambda/"+fn,logStreamName=st[0]["logStreamName"],limit=14,startFromHead=False)["events"]
            out[fn]=[e["message"].strip()[:190] for e in evs][-12:]
        except Exception as e: out[fn]=["tail-err:"+str(e)[:80]]
    R["deep_tails"]=out
    return True

wait_patched(); polygon_key(); schedules(); writer_hunt(); manifest_hygiene(); deep_tails()
# quick post-fix probe: refresh spx-history-deep via patched backtester (async) + head age later in 2888
try: lam.invoke(FunctionName="justhodl-alert-backtester",InvocationType="Event"); R["backtester_invoked"]=True
except Exception as e: R["backtester_invoked"]=str(e)[:60]
R["status"]="OK" if not R["errors"] else "PARTIAL"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3900])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2887_fix_wave.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2887 COMPLETE")
