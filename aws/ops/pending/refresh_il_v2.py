import json,io,zipfile,time,os,boto3,hashlib,subprocess
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3")
fn="justhodl-investor-lenses"; src="aws/lambdas/justhodl-investor-lenses/source/lambda_function.py"
# PROVE what the runner sees
txt=open(src).read()
print("RUNNER git HEAD:", subprocess.getoutput("git rev-parse HEAD")[:12], flush=True)
print("RUNNER file sha256:", hashlib.sha256(txt.encode()).hexdigest()[:16], flush=True)
print("RUNNER has bvps-derive block:", "derive from equity / diluted shares" in txt, flush=True)
print("RUNNER api/v3 count:", txt.count("api/v3"), flush=True)
def wait_active(t=40):
    for _ in range(t):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": return c
        time.sleep(3)
    return lam.get_function_configuration(FunctionName=fn)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.write(src,"lambda_function.py")
buf.seek(0); wait_active()
lam.update_function_code(FunctionName=fn,ZipFile=buf.read()); c=wait_active()
print("deployed CodeSha:",c.get("CodeSha256","")[:16],flush=True)
r=lam.invoke(FunctionName=fn,Payload=json.dumps({"ticker":"AAPL"}).encode())
body=json.loads(r["Payload"].read().decode())
out={"head":subprocess.getoutput("git rev-parse HEAD")[:12],"file_sha":hashlib.sha256(txt.encode()).hexdigest()[:16],"body":body}
try:
    time.sleep(3)
    obj=json.loads(s3.get_object(Bucket=BUCKET,Key="data/investor-lenses/AAPL.json")["Body"].read().decode())
    out["lens_fair_values"]={k:(v.get("fair_value") if isinstance(v,dict) else None) for k,v in obj.get("lenses",{}).items()}
    out["lens_why"]={k:(v.get("why","")[:80] if isinstance(v,dict) else None) for k,v in obj.get("lenses",{}).items()}
    out["summary_read"]=obj.get("summary",{}).get("read")
except Exception as e: out["s3_err"]=str(e)[:150]
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(out,open("aws/ops/reports/refresh_il_v2.json","w"),indent=2,default=str)
print(json.dumps(out,indent=2,default=str))
