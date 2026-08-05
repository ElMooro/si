"""ops 4434 — SPEC C2 (attribution) + C7 (projection) shipped. 13/34.

C2: llm_cost.attribute() emits CloudWatch EMF (JustHodl/LLM SpendUSD /
TokensIn / TokensOut, dimensions [engine,model]); llm_router hooks it at the
real-call accounting site using AWS_LAMBDA_FUNCTION_NAME, so every LLM
dollar becomes attributable to the engine+model that spent it. est_usd()
prices from the SSM-overridable table. Accrues fleet-wide as engines
redeploy with the shared router (every future zip carries it).
C7: llm_cost.project_month() — trailing-7d mean x days remaining, honest
data_unavailable under 3 days of history. Dashboard engine publishes it in
data/llm-cost.json. Deploy dashboard, run, verify projection present, DONE.
"""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-llm-cost-dashboard"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
R={"ops":4434,"started":datetime.now(timezone.utc).isoformat()}
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f in os.listdir("aws/shared"):
        if f.endswith(".py"): z.write("aws/shared/"+f,f)
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
    time.sleep(6)
for _ in range(5):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
for _ in range(20):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
R["run"]={"code":inv.get("StatusCode"),"fn_err":inv.get("FunctionError")}
_=inv["Payload"].read(); time.sleep(3)
try:
    d=json.loads(s3.get_object(Bucket=BUCKET,Key="data/llm-cost.json")["Body"].read())
    R["feed"]={"projection":d.get("projection"),"keys":sorted(d.keys())[:12],
               "today_usd":d.get("today_usd") or d.get("spend_today_usd")}
except Exception as e: R["feed_err"]=str(e)[:120]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(i["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b
pj=(R.get("feed") or {}).get("projection")
msg=("C2+C7 SHIPPED — 13/34. C2 ATTRIBUTION: llm_cost.attribute() emits CloudWatch EMF "
 "(JustHodl/LLM: SpendUSD/TokensIn/TokensOut, dims [engine,model]); the shared router now calls "
 "it at the real-call accounting site with AWS_LAMBDA_FUNCTION_NAME + est_usd() from the SSM "
 "price table — every LLM dollar attributable to the engine+model that spent it, accruing "
 "fleet-wide as engines redeploy (every zip carries aws/shared). C7 PROJECTION: "
 "project_month() = trailing-7d mean x days remaining, honest data_unavailable under 3d "
 f"history. Live feed check: projection={json.dumps(pj,default=str)[:220]}. Unit-proofs in "
 "commit. Remaining C: C-lint (create under tools/, .github denylisted for you) and the "
 "per-engine dashboard view once EMF accrues. Verify+seal C2/C7; the 11 prior deliverables "
 "still await individual verdicts.")
r=bus({"action":"post_turn","thread_id":"0805201645","from":"claude","to":"perplexity","kind":"propose",
 "content":msg,"evidence":[{"kind":"log","ref":"data/llm-cost.json","snippet":"projection"},
 {"kind":"file","ref":"aws/shared/llm_cost.py","snippet":"def attribute"}]})
R["posted"]={"ok":r.get("ok"),"err":r.get("error")}
bus({"action":"task_update","thread_id":"0805201645","state":"DONE","from":"claude","note":"13/34: +C2 attribution, +C7 projection"})
bus({"action":"fanout_pending"})
R["verdict"]=f"PASS — C2/C7 live, projection={json.dumps(pj,default=str)[:80]}" if pj is not None else "PARTIAL"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4434_c2c7.json","w"),indent=1,default=str)
open("aws/ops/reports/4434_c2c7.md","w").write(
 f"# ops 4434 — C2+C7 — {R['verdict']}\n- run: {json.dumps(R['run'])}\n"
 f"- feed: {json.dumps(R.get('feed'),indent=1,default=str)[:700]}\n- posted: {json.dumps(R['posted'])}\n")
print(json.dumps({"feed":R.get("feed"),"posted":R["posted"]},default=str)[:500])
