"""ops 2879 — deploy fast compiler (indexed matcher, 300s/1GB), recompile, re-verify strategist capstones."""
import os, io, json, time, zipfile, traceback, boto3
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-brain-compiler"
R={"ops":2879,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
def guard(name):
    def deco(fn):
        def run(*a,**k):
            try: return fn(*a,**k)
            except Exception:
                R["errors"][name]=traceback.format_exc()[-450:]; return None
        return run
    return deco
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=330,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); B="justhodl-dashboard-live"
def wait_ok():
    for _ in range(50):
        c=lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus")=="Successful": return True
        time.sleep(3)
    return False

@guard("deploy")
def deploy():
    src=open("aws/lambdas/justhodl-brain-compiler/source/lambda_function.py",encoding="utf-8").read()
    buf=io.BytesIO(); z=zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED); z.writestr("lambda_function.py",src); z.close()
    lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); wait_ok()
    lam.update_function_configuration(FunctionName=FN,Timeout=300,MemorySize=1024); R["cfg"]=wait_ok()
    return True

@guard("recompile")
def recompile():
    t0=time.time()
    p=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
    body=p["Payload"].read().decode(); R["invoke"]=body[:200]; R["compile_secs"]=round(time.time()-t0,1)
    if p.get("FunctionError"): R["errors"]["compiler_fn"]=body[:400]; return None
    time.sleep(2)
    d=json.loads(s3.get_object(Bucket=B,Key="data/brain-compiler.json")["Body"].read())
    R["summary"]=d.get("summary")
    R["build_queue"]=[{"concept":b["concept"],"n":b["n_claims"]} for b in (d.get("build_queue") or [])]
    R["gap_claims"]=[{"c":b["concept"],"q":(b.get("sample_claims") or [""])[0][:120]} for b in (d.get("build_queue") or [])[:6]]
    R["covered_sample"]=[{"claim":c["claim"][:95],"eng":[e["engine"].replace("justhodl-","") for e in c["engines"][:2]]} for c in (d.get("claims") or []) if c["status"]=="COVERED"][:4]
    return True

@guard("strategist")
def strategist():
    p=lam.invoke(FunctionName="justhodl-strategist",InvocationType="RequestResponse"); p["Payload"].read()
    time.sleep(2)
    raw=s3.get_object(Bucket=B,Key="data/strategist.json")["Body"].read().decode()
    d=json.loads(raw)
    R["strategist"]={"generated_at":d.get("generated_at"),
        "reads_new_capstones":{k:(k in raw) for k in ("canary-warroom","liquidity-inflection","cycle-clock","nowcast-desk")}}
    return True

deploy(); ok=recompile(); strategist()
R["status"]="LIVE" if ok and not R["errors"] else ("PARTIAL" if R.get("summary") else "FAILED")
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3300])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2879_fastcompile.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2879 COMPLETE")
