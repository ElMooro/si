"""Force-refresh investor-lenses code from current tree, wait active, invoke, verify S3.
Prints the FMP base actually embedded in the uploaded code as proof."""
import json,io,zipfile,time,os,boto3,re
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3")
fn="justhodl-investor-lenses"
src="aws/lambdas/justhodl-investor-lenses/source/lambda_function.py"
code_txt=open(src).read()
# PROOF: show the FMP base in the code we're about to ship
base=re.search(r'FMP_BASE = "([^"]+)"',code_txt).group(1)
v3=code_txt.count("api/v3")
print(f"UPLOADING code with FMP_BASE={base}  api/v3_count={v3}",flush=True)

def wait_active(tries=40):
    for _ in range(tries):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": return c
        time.sleep(3)
    return lam.get_function_configuration(FunctionName=fn)

buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.write(src,"lambda_function.py")
buf.seek(0)
wait_active()
lam.update_function_code(FunctionName=fn,ZipFile=buf.read())
c=wait_active()
print(f"code updated; CodeSha={c.get('CodeSha256','')[:16]} LastModified={c.get('LastModified')}",flush=True)

r=lam.invoke(FunctionName=fn,Payload=json.dumps({"ticker":"AAPL"}).encode())
body=json.loads(r["Payload"].read().decode())
out={"invoke_status":r["StatusCode"],"body":body,"fmp_base_shipped":base}
try:
    time.sleep(3)
    h=s3.head_object(Bucket=BUCKET,Key="data/investor-lenses/AAPL.json")
    obj=json.loads(s3.get_object(Bucket=BUCKET,Key="data/investor-lenses/AAPL.json")["Body"].read().decode())
    out["s3_written"]=True; out["s3_bytes"]=h["ContentLength"]
    out["lenses_summary"]=obj.get("summary")
    out["lens_fair_values"]={k:(v.get("fair_value") if isinstance(v,dict) else None)
                             for k,v in obj.get("lenses",{}).items()}
except Exception as e:
    out["s3_written"]=False; out["s3_err"]=str(e)[:150]
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(out,open("aws/ops/reports/refresh_investor_lenses.json","w"),indent=2,default=str)
print(json.dumps(out,indent=2,default=str))
