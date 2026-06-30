"""ops 2582 — create justhodl-search-attention, async-invoke, poll S3 for output."""
import boto3, io, zipfile, json, time, datetime
REGION="us-east-1"; FN="justhodl-search-attention"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
SRC="aws/lambdas/justhodl-search-attention/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
zb=buf.getvalue()
try:
    lam.get_function(FunctionName=FN)
    lam.update_function_configuration(FunctionName=FN, Timeout=300, MemorySize=512,
        Environment={"Variables":{"S3_BUCKET":"justhodl-dashboard-live"}}); time.sleep(5)
    lam.update_function_code(FunctionName=FN, ZipFile=zb); print("UPDATED")
except lam.exceptions.ResourceNotFoundException:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=ROLE,
        Handler="lambda_function.lambda_handler", Code={"ZipFile":zb},
        Timeout=300, MemorySize=512, Environment={"Variables":{"S3_BUCKET":"justhodl-dashboard-live"}},
        Description="Per-company Wikipedia pageview attention"); print("CREATED")
for _ in range(20):
    if lam.get_function(FunctionName=FN)["Configuration"].get("LastUpdateStatus")=="Successful": break
    time.sleep(3)
# async invoke (first run resolves 226 titles -> can take >60s)
lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
print("async invoked; polling S3...")
prev=None
try: prev=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/search-attention.json")["LastModified"]
except Exception: pass
for i in range(9):
    time.sleep(18)
    try:
        h=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/search-attention.json")
        if prev is None or h["LastModified"]>prev:
            j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/search-attention.json")["Body"].read())
            print("OUTPUT n",j.get("n"),"with_data",j.get("n_with_data"),"resolved_new",j.get("n_resolved_new"))
            print("top spikes:", [(x["ticker"],x.get("trend_pct"),x.get("svi")) for x in j.get("top_attention_spikes",[])[:8]])
            # sample a couple known names
            bt=j.get("by_ticker",{})
            for s in ["NVDA","FCEL","META","ENR"]:
                if s in bt: print("  ",s,bt[s])
            break
    except Exception as e: print(f"  poll {i}: {str(e)[:50]}")
    print(f"  poll {i}: not ready")
print("DONE 2582")
