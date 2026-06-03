"""1243 — Deploy executive-intel (Trump/OGE PTR parser) + invoke + verify."""
import json, os, time, zipfile, io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1243_executive_intel_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-executive-intel"
SOURCE_DIR = "aws/lambdas/justhodl-executive-intel/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE = "justhodl-executive-intel-daily"
SCHEDULE = "cron(15 11 * * ? *)"
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

print(f"[1243] 1. Deploy {LAMBDA}")
try:
    zb = zipit()
    print(f"  zip size: {len(zb)//1024} KB")
    try:
        lam.get_function_configuration(FunctionName=LAMBDA)
        lam.update_function_code(FunctionName=LAMBDA, ZipFile=zb); action="updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=LAMBDA, Runtime="python3.12", Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler", Code={"ZipFile": zb},
            Description="Executive-branch (Trump/OGE) trade tracker", Timeout=300,
            MemorySize=1024, Architectures=["x86_64"], Publish=False); action="created"
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out["deploy"]={"action":action,"sha":c.get("CodeSha256")[:16]}
    print(f"  ✓ {action} sha={c.get('CodeSha256')[:16]}")
except Exception as e:
    out["deploy_err"]=str(e)[:400]; print(f"  ❌ {e}")

print(f"\n[1243] 2. Schedule")
try:
    events.put_rule(Name=RULE, ScheduleExpression=SCHEDULE, State="ENABLED", Description="Daily executive intel")
    fn=lam.get_function(FunctionName=LAMBDA)
    events.put_targets(Rule=RULE, Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try:
        lam.add_permission(FunctionName=LAMBDA, StatementId=f"EB-{RULE}", Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com", SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE}")
    except lam.exceptions.ResourceConflictException: pass
    out["schedule"]=SCHEDULE; print("  ✓ scheduled")
except Exception as e:
    out["schedule_err"]=str(e)[:300]

print(f"\n[1243] 3. Invoke (crawl WH + parse PTR PDFs)")
try:
    t0=time.time()
    resp=lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    payload=resp.get("Payload").read().decode()
    out["invoke"]={"status":resp.get("StatusCode"),"elapsed_s":round(time.time()-t0,1),
                    "function_error":resp.get("FunctionError"),"body":payload[:800]}
    print(f"  status={resp.get('StatusCode')} elapsed={round(time.time()-t0,1)}s")
    print(f"  body: {payload[:300]}")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:500]}")
except Exception as e:
    out["invoke"]={"error":str(e)[:300]}

print(f"\n[1243] 4. Verify executive-intel.json")
try:
    doc=json.loads(s3.get_object(Bucket=BUCKET, Key="data/executive-intel.json")["Body"].read())
    out["output"]={"schema":doc.get("schema_version"),"stats":doc.get("stats"),
        "filers":[{"filer":f["filer"],"position":f["position"],"n_tx":f["n_tx"]} for f in doc.get("filers",[])[:10]],
        "top":[{"ticker":r["ticker"],"conviction":r["conviction_score"],"n_buyers":r["n_buyers"],
                 "buyers":r.get("buyers",[]),"latest":r.get("latest_tx_date")} for r in doc.get("top_conviction_buys",[])[:12]]}
    print(f"  ✓ stats: {doc.get('stats')}")
    print(f"\n  FILERS (who filed PTRs):")
    for f in doc.get("filers",[])[:10]:
        print(f"    {f['filer']:<28s} {f['position'][:35]:<35s} {f['n_tx']} tx")
    print(f"\n  TOP EXECUTIVE CONVICTION BUYS:")
    for r in doc.get("top_conviction_buys",[])[:12]:
        print(f"    {r['ticker']:<6s} conv={r['conviction_score']:>6.1f}  {r['n_buyers']}buyers  {r.get('latest','')}  {', '.join(r.get('buyers',[])[:2])}")
except Exception as e:
    out["output"]={"error":str(e)[:300]}

out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str))
print("\n[1243] DONE")
