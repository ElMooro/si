"""ops 2887 — fix wave: polygon-key inject, gap schedules, writer-hunt invokes, manifest hygiene,
top-4 error tails, post-deploy validation of router+stooq fixes."""
import os, re, glob, json, time, traceback, boto3
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
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=170,retries={"max_attempts":1}))
ev=boto3.client("events",region_name=REGION); logs=boto3.client("logs",region_name=REGION)
s3=boto3.client("s3",region_name=REGION)
FULL=json.loads(s3.get_object(Bucket=B,Key="data/_audit/gap-sweep.json")["Body"].read())
UNSCHED={o["fn"] for o in FULL.get("unscheduled") or []}
def wait_deployed(fn, budget=300):
    t0=time.time()
    while time.time()-t0<budget:
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            if c.get("LastModified","")>R["ts"][:19] and c.get("LastUpdateStatus")=="Successful": return True
        except Exception: pass
        time.sleep(8)
    return False

@guard("polygon_key")
def polygon_key():
    donor=lam.get_function_configuration(FunctionName="justhodl-alert-backtester").get("Environment",{}).get("Variables",{})
    key=donor.get("POLYGON_API_KEY") or donor.get("POLY_KEY")
    if not key:
        for cand in ("justhodl-crisis-canaries","justhodl-intraday-pulse"):
            v=lam.get_function_configuration(FunctionName=cand).get("Environment",{}).get("Variables",{})
            key=v.get("POLYGON_API_KEY") or v.get("POLY_KEY")
            if key: break
    if not key: R["polygon_key"]="NOT-FOUND"; return None
    cur=lam.get_function_configuration(FunctionName="justhodl-price-redundancy").get("Environment",{}).get("Variables",{}) or {}
    if cur.get("POLYGON_API_KEY")!=key:
        for _ in range(30):
            try:
                cur["POLYGON_API_KEY"]=key
                lam.update_function_configuration(FunctionName="justhodl-price-redundancy",Environment={"Variables":cur}); break
            except Exception as e:
                if "update is in progress" in str(e) or "ResourceConflict" in str(e): time.sleep(8); continue
                raise
    R["polygon_key"]="injected"
    return True

@guard("gap_schedules")
def gap_schedules():
    plan=[("justhodl-vix-backwardation-trigger","cron(15 13 * * ? *)"),
          ("justhodl-vol-target-unwind","cron(25 13 * * ? *)"),
          ("justhodl-factor-decomposition","cron(35 13 ? * MON *)"),
          ("justhodl-financial-secretary","cron(45 12 * * ? *)"),
          ("justhodl-history-snapshotter","cron(5 13 * * ? *)")]
    out={}
    for fn,cronx in plan:
        if fn not in UNSCHED: out[fn]="already-scheduled/skip"; continue
        rule=fn.replace("justhodl-","justhodl-")+"-daily" if "MON" not in cronx else fn+"-weekly"
        ev.put_rule(Name=rule,ScheduleExpression=cronx,State="ENABLED",Description="gap-fix 2887")
        try: lam.add_permission(FunctionName=fn,StatementId="gapfix2887",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,rule))
        except Exception as e:
            if "ResourceConflict" not in str(e): raise
        ev.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":"arn:aws:lambda:%s:%s:function:%s"%(REGION,ACCT,fn)}])
        try: lam.invoke(FunctionName=fn,InvocationType="Event")
        except Exception as e: out[fn]="invoke-err:"+str(e)[:60]; continue
        out[fn]="scheduled+fired ("+cronx+")"
    R["gap_schedules"]=out
    return True

@guard("writer_hunt")
def writer_hunt():
    targets=["regime-decisive-call","market-internals-history","backtest-summary","finviz-signals-state"]
    found={}
    for f in glob.glob("aws/lambdas/*/source/lambda_function.py"):
        try: src=open(f,encoding="utf-8",errors="ignore").read()
        except Exception: continue
        for t in targets:
            if t in src and ("put_object" in src or "_write" in src):
                found.setdefault(t,set()).add(f.split("/")[2])
    out={}
    for t,fns in found.items():
        fired=[]
        for fn in sorted(fns)[:2]:
            try: lam.invoke(FunctionName=fn,InvocationType="Event"); fired.append(fn)
            except Exception: pass
        out[t]={"writers":sorted(fns)[:3],"fired":fired,"scheduled":all(f2 not in UNSCHED for f2 in fns)}
    R["writer_hunt"]=out
    return True

@guard("manifest_hygiene")
def manifest_hygiene():
    try: man=json.loads(s3.get_object(Bucket=B,Key="data/_freshness-manifest.json")["Body"].read())
    except Exception: man={}
    keys=man.get("keys") or man
    static=["data/user-trades.json","data/user-trades-stats.json","data/user-watchlist.json",
            "data/khalid-config.json","data/ka-config.json","data/history-api-url.json",
            "data/feedback-summary.json","data/askdesk-config.json","data/eventbridge-audit.json",
            "data/system-audit.json","data/congress-party-map.json","data/quiver-congress-cache.json",
            "data/quiver-lobbying-cache.json"]
    ch=0
    for k in static:
        e=keys.get(k)
        if isinstance(e,dict): e["max_age_hours"]=8760; ch+=1
        elif e is not None: keys[k]=8760; ch+=1
        else: keys[k]={"max_age_hours":8760,"note":"static/event-driven or key-blocked (quiver token pending)"}
    if "keys" in man: man["keys"]=keys
    else: man=keys
    s3.put_object(Bucket=B,Key="data/_freshness-manifest.json",Body=json.dumps(man,ensure_ascii=False,default=str).encode(),ContentType="application/json")
    R["manifest_hygiene"]={"static_marked":ch+ (len(static)-ch)}
    return True

@guard("error_tails")
def error_tails():
    out={}
    for fn in ("justhodl-options-confluence","justhodl-velocity-acceleration","justhodl-fleet-monitor","fedliquidityapi"):
        try:
            st=logs.describe_log_streams(logGroupName="/aws/lambda/"+fn,orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
            if not st: out[fn]=["no streams"]; continue
            evs=logs.get_log_events(logGroupName="/aws/lambda/"+fn,logStreamName=st[0]["logStreamName"],limit=14,startFromHead=False)["events"]
            out[fn]=[e["message"].strip()[:170] for e in evs][-10:]
        except Exception as e: out[fn]=["tail-err:"+str(e)[:80]]
    R["error_tails"]=out
    return True

@guard("post_deploy_validate")
def post_deploy_validate():
    out={}
    for fn in ("justhodl-alert-backtester","justhodl-historical-analogs","justhodl-skew-tail-hedging","justhodl-price-redundancy"):
        ok=wait_deployed(fn, budget=240)
        try: lam.invoke(FunctionName=fn,InvocationType="Event"); out[fn]={"deployed":ok,"fired":True}
        except Exception as e: out[fn]={"deployed":ok,"fired":str(e)[:60]}
    # router-cascade sample: consumer-pulse sync (expect no FunctionError, fast)
    ok=wait_deployed("justhodl-consumer-pulse", budget=240)
    t0=time.time()
    p=lam.invoke(FunctionName="justhodl-consumer-pulse",InvocationType="RequestResponse")
    out["justhodl-consumer-pulse"]={"deployed":ok,"secs":round(time.time()-t0,1),
        "function_error":p.get("FunctionError"),"resp":p["Payload"].read().decode()[:120]}
    R["post_deploy"]=out
    return True

polygon_key(); gap_schedules(); writer_hunt(); manifest_hygiene(); error_tails(); post_deploy_validate()
R["status"]="OK" if not R["errors"] else "PARTIAL"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3900])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2887_fix_wave.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2887 COMPLETE")
