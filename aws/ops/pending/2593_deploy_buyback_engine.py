"""ops 2593 — create justhodl-buyback-engine, async-invoke (570 FMP calls), poll S3."""
import boto3, io, zipfile, json, time
REGION="us-east-1"; FN="justhodl-buyback-engine"
ROLE="arn:aws:iam::857687956942:role/lambda-execution-role"
SRC="aws/lambdas/justhodl-buyback-engine/source/lambda_function.py"
ENV={"Variables":{"S3_BUCKET":"justhodl-dashboard-live","FMP_API_KEY":"wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}}
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
zb=buf.getvalue()
try:
    lam.get_function(FunctionName=FN)
    lam.update_function_configuration(FunctionName=FN, Timeout=600, MemorySize=512, Environment=ENV); time.sleep(5)
    lam.update_function_code(FunctionName=FN, ZipFile=zb); print("UPDATED")
except lam.exceptions.ResourceNotFoundException:
    lam.create_function(FunctionName=FN, Runtime="python3.12", Role=ROLE,
        Handler="lambda_function.lambda_handler", Code={"ZipFile":zb},
        Timeout=600, MemorySize=512, Environment=ENV, Description="Unified buyback intelligence"); print("CREATED")
for _ in range(20):
    if lam.get_function(FunctionName=FN)["Configuration"].get("LastUpdateStatus")=="Successful": break
    time.sleep(3)
prev=None
try: prev=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/buyback-engine.json")["LastModified"]
except Exception: pass
lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
print("async invoked; polling S3 (FMP batch)...")
for i in range(12):
    time.sleep(20)
    try:
        h=s3.head_object(Bucket="justhodl-dashboard-live",Key="data/buyback-engine.json")
        if prev is None or h["LastModified"]>prev:
            j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/buyback-engine.json")["Body"].read())
            print("OUTPUT v",j.get("version"),"scored",j.get("n_scored"),"fmp_ok",j.get("n_fmp_resolved"))
            print("counts:",j.get("counts"))
            print("scanner_state:",j.get("scanner_state"))
            for sec in ["high_conviction_pumps","fresh_authorizations","net_shrinkers","high_shareholder_yield","cheap_repurchasers","dilution_offset_warnings"]:
                r=j.get(sec,[]); ex=[(x["symbol"],x.get("class"),f"sc{x.get('buyback_score')}",f"nbY{x.get('net_buyback_yield')}",f"sr{x.get('share_count_reduction_yoy')}") for x in r[:4]]
                print(f"  {sec} ({len(r)}): {ex}")
            break
    except Exception as e: print(f"  poll {i}: {str(e)[:45]}")
    print(f"  poll {i}: not ready")
print("DONE 2593")
