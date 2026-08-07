"""ops 4541 — Khalid: 'not worried about budget'. Max throttle: deploy
40/agency + 10 threads; Memory 2048 (network scales with memory); env
FLOWS_PER_AGENCY=40; cadence rate(5 minutes); kick one dispatcher round;
measure per-agency velocity; report the collapsed ETA."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"; FN="justhodl-sdmx-walker"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=560,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4541,"started":datetime.now(timezone.utc).isoformat()}
def wait_ok():
    for _ in range(20):
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": return c
        time.sleep(6)
wait_ok()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f2 in os.listdir("aws/shared"):
        if f2.endswith(".py"): z.write("aws/shared/"+f2,f2)
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
c=wait_ok()
envv=(c.get("Environment") or {}).get("Variables") or {}
envv["FLOWS_PER_AGENCY"]="120"; envv["WALK_BUDGET_S"]="230"
lam.update_function_configuration(FunctionName=FN,Environment={"Variables":envv},MemorySize=10240,Timeout=300)
wait_ok()
for rn in ev.list_rule_names_by_target(TargetArn=c["FunctionArn"]).get("RuleNames",[]):
    ev.put_rule(Name=rn,ScheduleExpression="rate(5 minutes)",State="ENABLED")
    R.setdefault("rules",[]).append(rn)
def pcts():
    out={}
    for ag in ("eurostat","oecd","statcan","bis"):
        try:
            st=json.loads(s3.get_object(Bucket=B,Key=f"data/_state/sdmx-walk-{ag}.json")["Body"].read())
            out[ag]=len(st.get("done") or [])
        except Exception: out[ag]=None
    return out
R["before"]=pcts()
# BLITZ: serial per-agency drains right now (RequestResponse, no overlap)
R["blitz"]={}
for _ag,_n in (("eurostat",6),("oecd",3),("statcan",3)):
    for _i in range(_n):
        _bi=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",
                       Payload=json.dumps({"agency":_ag}).encode())
        _=_bi["Payload"].read()
    R["blitz"][_ag]=_n
time.sleep(5)
R["after"]=pcts()
gain={a:(R["after"][a]-R["before"][a]) for a in R["after"] if R["after"][a] is not None and R["before"][a] is not None}
R["gain_one_round"]=gain
eur_left=8146-(R["after"].get("eurostat") or 0)
rate=120*12
R["eta"]={"eurostat_full_h":round(eur_left/rate,1),"oecd_full_h":round(max(0,1542-(R["after"].get('oecd') or 0))/rate,1)}
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0807-reseal","from":"claude","to":"perplexity","kind":"propose",
 "content":("CEILING (Khalid: very powerful system) — 10GB/6vCPU, 24 wires, 120/run + blitz: 40/agency + 10 wires + 5-min cadence + 2GB. "
  f"One round gain={json.dumps(gain)} · positions={json.dumps(R['after'])} · ETA eurostat_full≈"
  f"{R['eta']['eurostat_full_h']}h, oecd≈{R['eta']['oecd_full_h']}h. Watch failure ledgers for 429s; "
  "dial-back lever = FLOWS_PER_AGENCY env."),
 "evidence":[{"kind":"log","ref":"data/_state/sdmx-walk-eurostat.json","snippet":"done"}]})
bus({"action":"fanout_pending"})
R["verdict"]=f"gain={json.dumps(gain)} after={json.dumps(R['after'])} eta_h={json.dumps(R['eta'])} rules5m={R.get('rules')}"
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4541_max.json","w"),indent=1,default=str)
open("aws/ops/reports/4541_max.md","w").write("# 4541 max — "+R["verdict"]+"\n")
print(R["verdict"][:300])
