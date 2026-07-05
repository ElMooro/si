"""ops 2891 — LLM FinOps: mode=on_demand, budget $8, per-engine caps, cadence rules,
30-day REAL spend ledger, news-sentiment cadence check, behavioral verification."""
import os, json, time, traceback, urllib.request, boto3
from datetime import datetime, timezone, timedelta
from botocore.config import Config
REGION="us-east-1"; ACCT="857687956942"; B="justhodl-dashboard-live"
R={"ops":2891,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
def guard(name):
    def deco(fn):
        def run(*a,**k):
            try: return fn(*a,**k)
            except Exception:
                R["errors"][name]=traceback.format_exc()[-420:]; return None
        return run
    return deco
ssm=boto3.client("ssm",region_name=REGION); ev=boto3.client("events",region_name=REGION)
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=150,retries={"max_attempts":1}))
ddb=boto3.client("dynamodb",region_name=REGION); logs=boto3.client("logs",region_name=REGION)

@guard("ssm_policy")
def ssm_policy():
    ssm.put_parameter(Name="/justhodl/llm/mode",Value="on_demand",Type="String",Overwrite=True)
    ssm.put_parameter(Name="/justhodl/llm/daily-budget-usd",Value="8",Type="String",Overwrite=True)
    caps={"justhodl-page-ai":40,"justhodl-ticket-ai-rationale":10,"justhodl-signal-board":8,
          "justhodl-upside-thesis":4,"justhodl-ask":80,"justhodl-ai-chat":80,"justhodl-strategist":3,
          "justhodl-morning-intelligence":3,"default":6}
    ssm.put_parameter(Name="/justhodl/llm/engine-daily-cap",Value=json.dumps(caps),Type="String",Overwrite=True)
    R["ssm"]={"mode":"on_demand","budget_usd":8,"caps":caps}
    return True

@guard("cadence_rules")
def cadence_rules():
    plan=[("justhodl-page-ai-wave","cron(10 5 * * ? *)"),
          ("justhodl-ticket-ai-rationale","cron(5 0/6 * * ? *)"),
          ("justhodl-signal-board","cron(15 0/6 * * ? *)")]
    out={}
    rules={}
    p=ev.get_paginator("list_rules")
    for pg in p.paginate():
        for r in pg["Rules"]:
            if r.get("ScheduleExpression"): rules[r["Name"]]=r
    for want_rule,cron in plan:
        # find rule by exact name or by target fn name prefix
        name=want_rule if want_rule in rules else None
        if not name:
            for rn in rules:
                if want_rule.replace("justhodl-","") in rn: name=rn; break
        if not name: out[want_rule]="rule-not-found"; continue
        ev.put_rule(Name=name,ScheduleExpression=cron,State="ENABLED")
        out[name]=cron
    R["cadence"]=out
    return True

@guard("news_check")
def news_check():
    hits=[]
    p=ev.get_paginator("list_rules")
    for pg in p.paginate():
        for r in pg["Rules"]:
            if not r.get("ScheduleExpression"): continue
            try: tg=ev.list_targets_by_rule(Rule=r["Name"]).get("Targets",[])
            except Exception: continue
            for t in tg:
                fn=t.get("Arn","").split(":function:")[-1]
                if "news" in fn or "sentiment" in fn:
                    hits.append({"rule":r["Name"],"fn":fn,"cron":r["ScheduleExpression"],"state":r["State"]})
    fixed=[]
    for h in hits:
        c=h["cron"]
        multi = ("rate(" in c and "hour" in c and int(c.split("(")[1].split()[0])<24) or ("* * ? *" in c and c.split()[1].replace("cron(","")=="*") or "*/" in c.split()[1] if c.startswith("cron(") else False
        # conservative: only flag obvious >1x/day
        if "rate(" in c:
            n=int(c.split("(")[1].split()[0]); u=c.split()[1].rstrip(")")
            if (u.startswith("minute")) or (u.startswith("hour") and n<24):
                ev.put_rule(Name=h["rule"],ScheduleExpression="cron(0 11 * * ? *)",State="ENABLED"); fixed.append(h["rule"])
        elif c.startswith("cron("):
            hh=c[5:-1].split()[1]
            if hh=="*" or "/" in hh or "," in hh:
                ev.put_rule(Name=h["rule"],ScheduleExpression="cron(0 11 * * ? *)",State="ENABLED"); fixed.append(h["rule"])
    R["news_rules"]=hits; R["news_fixed_to_daily"]=fixed
    return True

@guard("ledger_30d")
def ledger_30d():
    tot={"usd":0.0,"in_tok":0,"out_tok":0,"calls":0,"cached":0}
    by_engine={}; by_model={}; days_with_spend=0
    for d in range(0,30):
        day=(datetime.now(timezone.utc)-timedelta(days=d)).strftime("%Y-%m-%d")
        try:
            q=ddb.query(TableName="justhodl-llm-cost",KeyConditionExpression="#d = :d",
                        ExpressionAttributeNames={"#d":"date"},ExpressionAttributeValues={":d":{"S":day}})
        except Exception: continue
        day_usd=0.0
        for it in q.get("Items",[]):
            em=it.get("engine_model",{}).get("S","?|?"); eng,model=em.split("|",1) if "|" in em else (em,"?")
            def num(k):
                v=it.get(k,{}); return float(v.get("N",0)) if "N" in v else 0.0
            usd=num("usd") or num("cost_usd"); it_=num("in_tok") or num("input_tokens"); ot=num("out_tok") or num("output_tokens")
            calls=num("calls") or num("n"); cached=num("cached") or num("cache_hits")
            tot["usd"]+=usd; tot["in_tok"]+=it_; tot["out_tok"]+=ot; tot["calls"]+=calls; tot["cached"]+=cached
            day_usd+=usd
            e=by_engine.setdefault(eng,{"usd":0.0,"calls":0}); e["usd"]+=usd; e["calls"]+=calls
            m=by_model.setdefault(model,{"usd":0.0,"calls":0}); m["usd"]+=usd; m["calls"]+=calls
        if day_usd>0: days_with_spend+=1
    R["ledger_30d"]={"total_usd":round(tot["usd"],2),"calls":int(tot["calls"]),"cached_hits":int(tot["cached"]),
        "in_tok_M":round(tot["in_tok"]/1e6,2),"out_tok_M":round(tot["out_tok"]/1e6,2),
        "days_with_spend":days_with_spend,
        "top_engines":sorted(({"engine":k,"usd":round(v["usd"],2),"calls":int(v["calls"])} for k,v in by_engine.items()),key=lambda x:-x["usd"])[:12],
        "by_model":{k:{"usd":round(v["usd"],2),"calls":int(v["calls"])} for k,v in sorted(by_model.items(),key=lambda kv:-kv[1]["usd"])}}
    return True

@guard("verify_gate")
def verify_gate():
    # wait shared redeploy on a representative fn
    for _ in range(60):
        c=lam.get_function_configuration(FunctionName="justhodl-devils-advocate")
        if c.get("LastModified","")>R["ts"][:19] and c.get("LastUpdateStatus")=="Successful": break
        time.sleep(8)
    t0=time.time()
    p=lam.invoke(FunctionName="justhodl-devils-advocate",InvocationType="RequestResponse")
    p["Payload"].read()
    R["gate_scheduled_engine"]={"fn":"justhodl-devils-advocate","fn_error":p.get("FunctionError"),"secs":round(time.time()-t0,1)}
    time.sleep(3)
    try:
        st=logs.describe_log_streams(logGroupName="/aws/lambda/justhodl-devils-advocate",orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
        evs=logs.get_log_events(logGroupName="/aws/lambda/justhodl-devils-advocate",logStreamName=st[0]["logStreamName"],limit=14)["events"]
        R["gate_scheduled_engine"]["gated_log"]=any("mode=on_demand" in e["message"] for e in evs)
    except Exception as e: R["gate_scheduled_engine"]["log_err"]=str(e)[:60]
    # on-demand path still allowed
    cfg=json.loads(boto3.client("s3",region_name=REGION).get_object(Bucket=B,Key="data/page-ai-live.json")["Body"].read())
    req=urllib.request.Request(cfg["url"].rstrip("/")+"?mode=live&page=index",headers={"User-Agent":"Mozilla/5.0"})
    d=json.loads(urllib.request.urlopen(req,timeout=120).read())
    R["on_demand_path"]={"page":d.get("page"),"on_click":d.get("generated_on_click"),"keys":list(d.keys())[:8]}
    return True

ssm_policy(); cadence_rules(); news_check(); ledger_30d(); verify_gate()
R["status"]="OK" if not R["errors"] else "PARTIAL"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3800])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2891_llm_finops.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2891 COMPLETE")
