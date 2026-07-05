"""Direct deploy + env-inject + invoke + S3 verify for the two new engines.
Runs via run-ops.yml (in-account AWS creds). Commits full report back."""
import json, io, zipfile, time, os, boto3
from datetime import datetime, timezone

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
lam=boto3.client("lambda",region_name=REGION)
s3=boto3.client("s3")
log=[]
def L(m): log.append(m); print(m)

# 1. pull standard secrets bundle from a known-good live function
STD_KEYS=["FMP_KEY","FRED_KEY","POLYGON_KEY","ALPHA_VANTAGE_KEY","CMC_KEY",
          "ANTHROPIC_API_KEY","BLS_KEY","BEA_KEY","CENSUS_KEY"]
src=lam.get_function_configuration(FunctionName="justhodl-buyback-scanner")
src_env=src.get("Environment",{}).get("Variables",{}) or {}
bundle={k:src_env[k] for k in STD_KEYS if k in src_env and src_env[k]}
L(f"secrets bundle from buyback-scanner: {sorted(bundle.keys())}")

def zip_src(path):
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
        z.write(path, arcname="lambda_function.py")
    buf.seek(0); return buf.read()

ENGINES=[
    ("justhodl-investor-lenses","aws/lambdas/justhodl-investor-lenses/source/lambda_function.py",60,256,"data/investor-lenses/AAPL.json"),
    ("justhodl-technical-overlays","aws/lambdas/justhodl-technical-overlays/source/lambda_function.py",90,256,"data/technical-overlays/AAPL.json"),
]
report={"generated":datetime.now(timezone.utc).isoformat(),"engines":{}}

for fn,src_path,timeout,mem,s3key in ENGINES:
    e={}
    try:
        code=zip_src(src_path)
        env={"Variables":dict(bundle, JH_BUCKET=BUCKET)}
        try:
            lam.get_function_configuration(FunctionName=fn)
            lam.update_function_code(FunctionName=fn,ZipFile=code)
            time.sleep(6)
            lam.update_function_configuration(FunctionName=fn,Timeout=timeout,
                MemorySize=mem,Handler="lambda_function.lambda_handler",
                Runtime="python3.12",Environment=env)
            e["action"]="updated"
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(FunctionName=fn,Runtime="python3.12",Role=ROLE,
                Handler="lambda_function.lambda_handler",Code={"ZipFile":code},
                Timeout=timeout,MemorySize=mem,Environment=env,Architectures=["x86_64"])
            e["action"]="created"
        time.sleep(8)
        cfg=lam.get_function_configuration(FunctionName=fn)
        e["env_keys"]=sorted((cfg.get("Environment",{}).get("Variables",{}) or {}).keys())
        L(f"{fn}: {e['action']}, env_keys={e['env_keys']}")
        # invoke on AAPL
        r=lam.invoke(FunctionName=fn,Payload=json.dumps({"ticker":"AAPL"}).encode())
        body=json.loads(r["Payload"].read().decode())
        e["invoke_status"]=r["StatusCode"]
        e["invoke_body"]=body.get("body",body) if isinstance(body,dict) else body
        time.sleep(3)
        head=s3.head_object(Bucket=BUCKET,Key=s3key)
        e["s3_written"]=True; e["s3_bytes"]=head["ContentLength"]; e["s3_key"]=s3key
        L(f"{fn}: invoke={e['invoke_status']} s3={e['s3_bytes']}B")
    except Exception as ex:
        e["error"]=f"{type(ex).__name__}: {ex}"
        L(f"{fn}: ERROR {e['error']}")
    report["engines"][fn]=e

report["log"]=log
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(report,open("aws/ops/reports/deploy_verify_engines.json","w"),indent=2,default=str)
print("\n=== FINAL ===")
print(json.dumps(report,indent=2,default=str))
