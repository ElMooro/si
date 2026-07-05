"""ops 2877 — v2 of registry+compiler bootstrap: every section guarded, report ALWAYS written."""
import os, io, json, re, glob, time, zipfile, traceback, boto3
from collections import Counter
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"; ACCT="857687956942"; FN="justhodl-brain-compiler"
ROLE="arn:aws:iam::%s:role/lambda-execution-role"%ACCT
R={"ops":2877,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
def guard(name):
    def deco(fn):
        def run(*a,**k):
            try: return fn(*a,**k)
            except Exception:
                R["errors"][name]=traceback.format_exc()[-600:]; return None
        return run
    return deco
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=170,retries={"max_attempts":0}))
events=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
B="justhodl-dashboard-live"

@guard("registry")
def build_registry():
    reg={}; dom=Counter(); STOP={"THE","AND","FOR","WITH","FROM","THIS","THAT","JSON","HTTP","POST","TRUE","NONE","DATA","NULL","UTF"}
    for f in glob.glob("aws/lambdas/*/source/lambda_function.py"):
        eng=f.split("/")[2]
        try: src=open(f,encoding="utf-8",errors="ignore").read()
        except Exception: continue
        m=re.match(r'\s*(?:"""|\'\'\')(.{20,900}?)(?:"""|\'\'\')',src,re.S)
        doc=re.sub(r"\s+"," ",m.group(1)).strip()[:320] if m else ""
        fred=sorted({t for t in re.findall(r'"([A-Z][A-Z0-9]{3,17})"',src) if any(ch.isdigit() for ch in t) and t not in STOP})[:40]
        outs=sorted(set(re.findall(r'data/[a-z0-9._-]+\.json',src)))[:12]
        reg[eng]={"doc":doc,"fred":fred,"outs":outs}
        for d in re.findall(r'https?://([a-z0-9.-]+\.[a-z]{2,})',src): dom[d.replace("www.","")]+=1
    s3.put_object(Bucket=B,Key="data/engine-registry.json",Body=json.dumps({"generated_at":R["ts"],"n_engines":len(reg),"engines":reg},ensure_ascii=False).encode(),ContentType="application/json",CacheControl="max-age=3600")
    s3.put_object(Bucket=B,Key="data/source-utilization.json",Body=json.dumps({"generated_at":R["ts"],"domains":dict(dom.most_common(60))},ensure_ascii=False).encode(),ContentType="application/json",CacheControl="max-age=3600")
    R["registry_engines"]=len(reg); R["top_domains"]=dict(dom.most_common(6))
    return True

@guard("create_fn")
def create_fn():
    src=open("aws/lambdas/justhodl-brain-compiler/source/lambda_function.py",encoding="utf-8").read()
    buf=io.BytesIO(); z=zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED); z.writestr("lambda_function.py",src); z.close()
    code=buf.getvalue()
    def ready():
        for _ in range(40):
            try:
                c=lam.get_function_configuration(FunctionName=FN)
                if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return True
            except Exception: pass
            time.sleep(3)
        return False
    try: lam.get_function(FunctionName=FN); ex=True
    except Exception: ex=False
    if ex:
        lam.update_function_code(FunctionName=FN,ZipFile=code); R["action"]="updated"
    else:
        lam.create_function(FunctionName=FN,Runtime="python3.12",Role=ROLE,Handler="lambda_function.lambda_handler",
            Code={"ZipFile":code},Timeout=120,MemorySize=512,Description="Brain->Fleet compiler (monthly)")
        R["action"]="created"
    R["fn_ready"]=ready()
    return True

@guard("schedule")
def schedule():
    rule="justhodl-brain-compiler-monthly"
    events.put_rule(Name=rule,ScheduleExpression="cron(0 6 1 * ? *)",State="ENABLED",Description="Monthly brain compile")
    try: lam.add_permission(FunctionName=FN,StatementId="compiler-sched",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,rule))
    except Exception as e:
        if "ResourceConflict" not in str(e): raise
    events.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":"arn:aws:lambda:%s:%s:function:%s"%(REGION,ACCT,FN)}])
    return True

@guard("compile_run")
def compile_run():
    p=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
    body=p["Payload"].read().decode()
    R["invoke"]=body[:250]
    if p.get("FunctionError"): R["errors"]["compiler_fn"]=body[:600]
    return True

@guard("verify")
def verify():
    time.sleep(2)
    d=json.loads(s3.get_object(Bucket=B,Key="data/brain-compiler.json")["Body"].read())
    R["summary"]=d.get("summary")
    R["build_queue"]=[{"concept":b["concept"],"n":b["n_claims"]} for b in (d.get("build_queue") or [])][:12]
    R["gap_samples"]=[{"c":b["concept"],"q":(b.get("sample_claims") or [""])[0][:120]} for b in (d.get("build_queue") or [])[:4]]
    R["covered_sample"]=[{"claim":c["claim"][:100],"eng":[e["engine"] for e in c["engines"][:2]]} for c in (d.get("claims") or []) if c["status"]=="COVERED"][:3]
    return True

build_registry(); create_fn(); schedule(); compile_run(); ok=verify()
R["status"]="LIVE" if (ok and not R["errors"] and (R.get("summary",{}) or {}).get("n_claims",0)>0) else ("PARTIAL" if R.get("summary") else "FAILED")
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3500])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2877_registry_compiler_v2.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2877 COMPLETE")
