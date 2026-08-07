"""ops 4538 — Khalid: expedite the data import. Walker PER 2->12 + budget
deployed; cadence hourly->every 10 min; FLOWS_PER_AGENCY env=12 (beats any
stale env=2); provider-catalog daily->hourly so data.html tracks near-live;
kick 3 immediate walker runs; report the new velocity."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=560,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4538,"started":datetime.now(timezone.utc).isoformat()}
FN="justhodl-sdmx-walker"
def wait_ok(fn):
    for _ in range(20):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": return
        time.sleep(6)
wait_ok(FN)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f2 in os.listdir("aws/shared"):
        if f2.endswith(".py"): z.write("aws/shared/"+f2,f2)
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
wait_ok(FN)
cfg=lam.get_function_configuration(FunctionName=FN)
envv=(cfg.get("Environment") or {}).get("Variables") or {}
envv["FLOWS_PER_AGENCY"]="12"; envv["WALK_BUDGET_S"]="700"
lam.update_function_configuration(FunctionName=FN,Environment={"Variables":envv},Timeout=880)
wait_ok(FN)
# cadence: every 10 minutes
arn=cfg["FunctionArn"]
for rn in ev.list_rule_names_by_target(TargetArn=arn).get("RuleNames",[]):
    ev.put_rule(Name=rn,ScheduleExpression="rate(10 minutes)",State="ENABLED")
    R.setdefault("walker_rules",[]).append(rn)
# catalog hourly so the page tracks the growth
carn=lam.get_function_configuration(FunctionName="justhodl-provider-catalog")["FunctionArn"]
for rn in ev.list_rule_names_by_target(TargetArn=carn).get("RuleNames",[]):
    ev.put_rule(Name=rn,ScheduleExpression="rate(1 hour)",State="ENABLED")
    R.setdefault("catalog_rules",[]).append(rn)
def pcts():
    out={}
    for ag in ("bis","eurostat","oecd","statcan"):
        try:
            st=json.loads(s3.get_object(Bucket=B,Key=f"data/_state/sdmx-walk-{ag}.json")["Body"].read())
            out[ag]={"done":len(st.get("done") or []),"pct":st.get("pct")}
        except Exception as e: out[ag]=str(e)[:40]
    return out
R["before"]=pcts()
# kick three back-to-back accelerated runs
for i in range(3):
    inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
    _=inv["Payload"].read()
    R.setdefault("runs",[]).append(inv.get("FunctionError"))
R["after"]=pcts()
ic=lam.invoke(FunctionName="justhodl-provider-catalog",InvocationType="RequestResponse",Payload=b"{}")
_=ic["Payload"].read(); time.sleep(3)
hub=json.loads(s3.get_object(Bucket=B,Key="data/provider-catalog.json")["Body"].read())
R["totals"]=hub.get("totals")
gain=sum((R["after"].get(a,{}) or {}).get("done",0) for a in ("bis","eurostat","oecd","statcan") if isinstance(R["after"].get(a),dict)) - \
     sum((R["before"].get(a,{}) or {}).get("done",0) for a in ("bis","eurostat","oecd","statcan") if isinstance(R["before"].get(a),dict))
R["flows_gained_3runs"]=gain
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":("EXPEDITE (Khalid): walker PER 2->12, cadence hourly->10min, 700s budget, catalog hourly. "
  f"3 kicked runs gained {gain} flows: before={json.dumps(R['before'])[:180]} after={json.dumps(R['after'])[:180]} "
  f"totals={json.dumps(R['totals'])}. New velocity ~ 12x4agencies x 6runs/hr = up to ~288 flows/hr fleet-wide "
  "(vs 8). data.html now re-inventories hourly + 60s CDN. Verify pacing is polite (no 429s in states) + seal."),
 "evidence":[{"kind":"log","ref":"data/_state/sdmx-walk-eurostat.json","snippet":"pct"}]})
bus({"action":"fanout_pending"})
R["verdict"]=f"gain3runs={gain} after={json.dumps(R['after'])[:200]} totals={json.dumps(R['totals'])} rules10m={R.get('walker_rules')} cat_hourly={R.get('catalog_rules')}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4538_expedite.json","w"),indent=1,default=str)
open("aws/ops/reports/4538_expedite.md","w").write("# 4538 expedite — "+R["verdict"]+"\n")
print(R["verdict"][:340])
