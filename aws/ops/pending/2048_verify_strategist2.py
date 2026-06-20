"""ops 2048: re-verify strategist after coverage + JSON fixes."""
import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-strategist")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful": break
    time.sleep(4)
r=lam.invoke(FunctionName="justhodl-strategist",InvocationType="RequestResponse")
print("invoke:",r["StatusCode"],"|",r["Payload"].read().decode()[:300])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/strategist.json")["Body"].read())
fl=d["fleet"]
print("\nFLEET: feeds_read",fl["n_feeds_read"],"fresh",fl["n_fresh"],"| consensus",fl["consensus"],
      "| +/-/neu",fl["n_positive"],fl["n_negative"],fl["n_neutral"],"| model",d.get("model"))
interp=d.get("interpretation") or {}
if interp.get("error"): print("INTERP ERROR:",interp["error"])
elif interp.get("raw"): print("INTERP RAW (parse failed but preserved):\n",interp["raw"][:600])
else:
    print("\n=== STRATEGIST READ (parsed) ===")
    print("DOMINANT DRIVER:",interp.get("dominant_driver"))
    print("MECHANISM:",str(interp.get("mechanism"))[:260])
    print("CONFIRMING:",interp.get("confirming"))
    print("CONTRADICTING:",json.dumps(interp.get("contradicting"))[:360])
    print("SECOND-ORDER:",interp.get("second_order"))
    print("DECISIVE CALL:",interp.get("decisive_call"))
    print("CONVICTION:",interp.get("conviction"))
    print("FALSIFIERS:",interp.get("falsifiers"))
    print("KEY_CLAIMS:",json.dumps(interp.get("key_claims"))[:360])
# claim log check
try:
    today=time.strftime("%Y-%m-%d",time.gmtime())
    lg=json.loads(s3.get_object(Bucket=B,Key=f"data/strategist-log/{today}.json")["Body"].read())
    print("\nCLAIM LOG written:",len(lg.get("key_claims") or []),"claims +",len(lg.get("falsifiers") or []),"falsifiers")
except Exception as e: print("\nno claim log (raw/parse fail):",str(e)[:60])
print("DONE 2048")
