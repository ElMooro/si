"""1252 — deploy dislocation-detector + invoke + verify Buy-the-Laggard output."""
import json, os, time, zipfile, io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1252_dislocation_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-dislocation-detector"
SOURCE_DIR = "aws/lambdas/justhodl-dislocation-detector/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"; REGION = "us-east-1"
RULE = "justhodl-dislocation-detector-daily"; SCHEDULE = "cron(30 14 * * ? *)"
cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)
out = {"started": datetime.now(timezone.utc).isoformat()}

def zipit():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root,_,files in os.walk(SOURCE_DIR):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root: continue
                fp=os.path.join(root,f); zf.write(fp, arcname=os.path.relpath(fp,SOURCE_DIR))
    return buf.getvalue()

print(f"[1252] deploy {LAMBDA}")
try:
    zb=zipit()
    try:
        lam.get_function_configuration(FunctionName=LAMBDA)
        lam.update_function_code(FunctionName=LAMBDA, ZipFile=zb); action="updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=LAMBDA, Runtime="python3.12", Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler", Code={"ZipFile": zb},
            Description="Dislocation detector", Timeout=300, MemorySize=1024,
            Architectures=["x86_64"], Environment={"Variables":{"FMP_KEY":"wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}},
            Publish=False); action="created"
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out["deploy"]={"action":action,"sha":c.get("CodeSha256")[:16]}; print(f"  ✓ {action}")
except Exception as e:
    out["deploy_err"]=str(e)[:400]; print(f"  ❌ {e}")

try:
    events.put_rule(Name=RULE, ScheduleExpression=SCHEDULE, State="ENABLED", Description="Daily dislocation screen")
    fn=lam.get_function(FunctionName=LAMBDA)
    events.put_targets(Rule=RULE, Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try:
        lam.add_permission(FunctionName=LAMBDA, StatementId=f"EB-{RULE}", Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com", SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE}")
    except lam.exceptions.ResourceConflictException: pass
    out["schedule"]=SCHEDULE
except Exception as e: out["schedule_err"]=str(e)[:200]

print("[1252] invoke")
try:
    t0=time.time()
    resp=lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    payload=resp.get("Payload").read().decode()
    out["invoke"]={"status":resp.get("StatusCode"),"elapsed_s":round(time.time()-t0,1),
                    "function_error":resp.get("FunctionError"),"body":payload[:700]}
    print(f"  status={resp.get('StatusCode')} body={payload[:300]}")
    if resp.get("FunctionError"): print(f"  ⚠ {payload[:500]}")
except Exception as e: out["invoke"]={"error":str(e)[:300]}

print("[1252] verify dislocations.json")
try:
    doc=json.loads(s3.get_object(Bucket=BUCKET, Key="data/dislocations.json")["Body"].read())
    lag=doc.get("buy_the_laggard",[])
    out["output"]={"universe_scored":doc.get("universe_scored"),"n_cohorts":doc.get("n_cohorts"),
        "n_laggards":len(lag),
        "top10":[{"t":s["ticker"],"score":s["dislocation_score"],"cap":s.get("cap_bucket"),
                   "industry":(s.get("industry") or "")[:24],"cheap":s.get("cheapness"),"qual":s.get("quality"),
                   "ev_sales":s.get("ev_sales"),"r40":s.get("rule_of_40"),
                   "vs":(s.get("dislocated_vs") or {}).get("ticker"),
                   "vs_prem":(s.get("dislocated_vs") or {}).get("ev_sales_premium_pct"),
                   "caveats":s.get("caveats")} for s in lag[:10]]}
    print(f"  scored={doc.get('universe_scored')} cohorts={doc.get('n_cohorts')} laggards={len(lag)}")
    for s in lag[:10]:
        vs=s.get("dislocated_vs") or {}
        print(f"    {s['ticker']:<6s} score={s['dislocation_score']:>5.1f} cheap={s.get('cheapness')} qual={s.get('quality')} EV/S={s.get('ev_sales')} R40={s.get('rule_of_40')} vs {vs.get('ticker','—')} (+{vs.get('ev_sales_premium_pct','?')}%)")
except Exception as e: out["output"]={"error":str(e)[:300]}

out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str))
print("[1252] DONE")
