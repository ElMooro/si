import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-strategist")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful": break
    time.sleep(4)
lam.invoke(FunctionName="justhodl-strategist",InvocationType="RequestResponse")
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/strategist.json")["Body"].read())
print("fleet:",d["fleet"]["n_fresh"],"fresh | consensus",d["fleet"]["consensus"],"| model",d.get("model"))
it=d.get("interpretation") or {}
if it.get("raw") or it.get("error"):
    print("STILL NOT PARSED:",it.get("parse_note") or it.get("error")); print("raw head:",str(it.get("raw"))[:200])
else:
    print("\n=== STRATEGIST READ ===")
    print("DRIVER:",it.get("dominant_driver"))
    print("MECHANISM:",str(it.get("mechanism"))[:280])
    print("CONFIRMING:",it.get("confirming"))
    print("CONTRADICTING:",json.dumps(it.get("contradicting"))[:400])
    print("2ND-ORDER:",it.get("second_order"))
    print("CALL:",it.get("decisive_call"))
    print("CONVICTION:",it.get("conviction"),"| FALSIFIERS:",it.get("falsifiers"))
    print("CLAIMS:",json.dumps(it.get("key_claims"))[:300])
    today=time.strftime("%Y-%m-%d",time.gmtime())
    try:
        lg=json.loads(s3.get_object(Bucket=B,Key=f"data/strategist-log/{today}.json")["Body"].read())
        print("CLAIM LOG:",len(lg.get("key_claims") or []),"claims logged for grading")
    except Exception as e: print("claim log:",str(e)[:50])
print("DONE 2049")
