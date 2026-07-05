"""Deploy both engines with keys inherited from justhodl-confluence-meta (has FMP+POLYGON+FRED).
Waits for function-active between code/config updates to avoid ResourceConflict."""
import json,io,zipfile,time,os,boto3
from datetime import datetime,timezone
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3")
log=[]; 
def L(m): log.append(m); print(m,flush=True)

SRC="justhodl-confluence-meta"
STD=["FMP_KEY","FRED_KEY","POLYGON_KEY","ALPHA_VANTAGE_KEY","CMC_KEY","ANTHROPIC_API_KEY","BLS_KEY","BEA_KEY","CENSUS_KEY"]
se=lam.get_function_configuration(FunctionName=SRC).get("Environment",{}).get("Variables",{}) or {}
bundle={k:se[k] for k in STD if se.get(k)}
L(f"bundle from {SRC}: {sorted(bundle.keys())}")

def wait_active(fn,tries=30):
    for _ in range(tries):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None):
            return True
        time.sleep(3)
    return False

def zsrc(p):
    b=io.BytesIO()
    with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as z: z.write(p,"lambda_function.py")
    b.seek(0); return b.read()

ENG=[("justhodl-investor-lenses","aws/lambdas/justhodl-investor-lenses/source/lambda_function.py",60,256,"data/investor-lenses/AAPL.json"),
     ("justhodl-technical-overlays","aws/lambdas/justhodl-technical-overlays/source/lambda_function.py",90,256,"data/technical-overlays/AAPL.json")]
rep={"generated":datetime.now(timezone.utc).isoformat(),"key_source":SRC,"engines":{}}
for fn,sp,to,mem,s3k in ENG:
    e={}
    try:
        code=zsrc(sp); env={"Variables":dict(bundle,JH_BUCKET=BUCKET)}
        exists=True
        try: lam.get_function_configuration(FunctionName=fn)
        except lam.exceptions.ResourceNotFoundException: exists=False
        if exists:
            wait_active(fn); lam.update_function_code(FunctionName=fn,ZipFile=code)
            wait_active(fn)
            lam.update_function_configuration(FunctionName=fn,Timeout=to,MemorySize=mem,
                Handler="lambda_function.lambda_handler",Runtime="python3.12",Environment=env)
            e["action"]="updated"
        else:
            lam.create_function(FunctionName=fn,Runtime="python3.12",Role=ROLE,
                Handler="lambda_function.lambda_handler",Code={"ZipFile":code},Timeout=to,
                MemorySize=mem,Environment=env,Architectures=["x86_64"])
            e["action"]="created"
        wait_active(fn)
        cfg=lam.get_function_configuration(FunctionName=fn)
        e["env_keys"]=sorted((cfg.get("Environment",{}).get("Variables",{}) or {}).keys())
        L(f"{fn}: {e['action']} env_keys={e['env_keys']}")
        r=lam.invoke(FunctionName=fn,Payload=json.dumps({"ticker":"AAPL"}).encode())
        body=json.loads(r["Payload"].read().decode())
        e["invoke_status"]=r["StatusCode"]
        e["invoke_body"]=body.get("body",body) if isinstance(body,dict) else body
        time.sleep(3)
        try:
            h=s3.head_object(Bucket=BUCKET,Key=s3k); e["s3_written"]=True; e["s3_bytes"]=h["ContentLength"]
            obj=s3.get_object(Bucket=BUCKET,Key=s3k)["Body"].read().decode()
            e["s3_sample"]=json.loads(obj).get("summary") or json.loads(obj).get("confluence") or "written"
        except Exception as se2: e["s3_written"]=False; e["s3_err"]=str(se2)[:120]
        L(f"{fn}: invoke={e['invoke_status']} s3_written={e.get('s3_written')}")
    except Exception as ex:
        e["error"]=f"{type(ex).__name__}: {ex}"; L(f"{fn}: ERROR {e['error']}")
    rep["engines"][fn]=e
rep["log"]=log
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(rep,open("aws/ops/reports/deploy_verify_engines2.json","w"),indent=2,default=str)
print("=== FINAL ==="); print(json.dumps(rep,indent=2,default=str))
