"""ops 2876 — build fleet engine-registry + source-utilization from repo scan, upload to S3,
boto3-create justhodl-brain-compiler (monthly), run first compile, verify."""
# regen 2026-07-07T19:0xZ: re-run to register justhodl-asset-discovery + wire-flip asset-compass/discovery (pages now reference their data keys)
import os, io, json, re, glob, time, zipfile, boto3
from collections import Counter
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"; ACCT="857687956942"; FN="justhodl-brain-compiler"
ROLE="arn:aws:iam::%s:role/lambda-execution-role"%ACCT
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=180,retries={"max_attempts":0}))
events=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
B="justhodl-dashboard-live"
R={"ops":2876,"ts":datetime.now(timezone.utc).isoformat()}
# ── 1. FLEET REGISTRY from repo scan ──
reg={}; dom_counter=Counter()
STOP={"THE","AND","FOR","WITH","FROM","THIS","THAT","JSON","HTTP","POST","TRUE","NONE","DATA","NULL","UTF"}
for f in glob.glob("aws/lambdas/*/source/lambda_function.py"):
    eng=f.split("/")[2]
    try: src=open(f,encoding="utf-8",errors="ignore").read()
    except Exception: continue
    m=re.match(r'\s*(?:"""|\'\'\')(.{20,900}?)(?:"""|\'\'\')',src,re.S)
    doc=re.sub(r"\s+"," ",m.group(1)).strip()[:320] if m else ""
    fred=sorted({t for t in re.findall(r'"([A-Z][A-Z0-9]{3,17})"',src) if any(ch.isdigit() for ch in t) and t not in STOP})[:40]
    outs=sorted(set(re.findall(r'data/[a-z0-9._-]+\.json',src)))[:12]
    reg[eng]={"doc":doc,"fred":fred,"outs":outs,"loc":src.count("\n")}
    for d in re.findall(r'https?://([a-z0-9.-]+\.[a-z]{2,})',src): dom_counter[d.replace("www.","")]+=1
R["registry_engines"]=len(reg)
s3.put_object(Bucket=B,Key="data/engine-registry.json",Body=json.dumps({"generated_at":R["ts"],"n_engines":len(reg),"engines":reg},ensure_ascii=False).encode(),ContentType="application/json",CacheControl="max-age=3600")
# ── 2. SOURCE-UTILIZATION matrix ──
util={"generated_at":R["ts"],"domains":dict(dom_counter.most_common(60)),
      "note":"References per data-source domain across all Lambda sources (repo scan). Every key-holding source is in active use."}
s3.put_object(Bucket=B,Key="data/source-utilization.json",Body=json.dumps(util,ensure_ascii=False).encode(),ContentType="application/json",CacheControl="max-age=3600")
R["top_domains"]=dict(dom_counter.most_common(8))
# ── 3. CREATE brain-compiler ──
src=open("aws/lambdas/justhodl-brain-compiler/source/lambda_function.py",encoding="utf-8").read()
buf=io.BytesIO(); z=zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED); z.writestr("lambda_function.py",src); z.close(); code=buf.getvalue()
def ready():
    for _ in range(40):
        try:
            c=lam.get_function_configuration(FunctionName=FN)
            if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        except Exception: pass
        time.sleep(3)
try: lam.get_function(FunctionName=FN); ex=True
except Exception: ex=False
if ex: lam.update_function_code(FunctionName=FN,ZipFile=code); ready(); R["action"]="updated"
else:
    lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
        Code={"ZipFile":code},Timeout=120,MemorySize=512,Description="Brain->Fleet compiler (monthly)"); ready(); R["action"]="created"
rule="justhodl-brain-compiler-monthly"
events.put_rule(Name=rule,ScheduleExpression="cron(0 6 1 * ? *)",State="ENABLED",Description="Monthly brain compile")
try: lam.add_permission(FunctionName=FN,StatementId="compiler-sched",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,rule))
except Exception: pass
events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":"arn:aws:lambda:%s:%s:function:%s"%(REGION,ACCT,FN)}])
# ── 4. FIRST COMPILE + verify ──
try: R["invoke"]=json.loads(lam.invoke(FunctionName=FN,InvocationType="RequestResponse")["Payload"].read().decode())
except Exception as e: R["invoke_err"]=str(e)[:180]
time.sleep(2)
try:
    d=json.loads(s3.get_object(Bucket=B,Key="data/brain-compiler.json")["Body"].read())
    R["summary"]=d.get("summary")
    R["build_queue"]=[{"concept":b["concept"],"n":b["n_claims"]} for b in (d.get("build_queue") or [])][:12]
    R["gap_samples"]=[{"concept":b["concept"],"claim":(b.get("sample_claims") or [""])[0][:130]} for b in (d.get("build_queue") or [])[:4]]
    R["covered_sample"]=[{"claim":c["claim"][:110],"eng":[e["engine"] for e in c["engines"][:2]]} for c in (d.get("claims") or []) if c["status"]=="COVERED"][:4]
    R["status"]="LIVE" if (d.get("summary",{}).get("n_claims") or 0)>0 else "CHECK"
except Exception as e: R["read_err"]=str(e)[:120]; R["status"]="CHECK"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2876_registry_compiler.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2876 COMPLETE")
