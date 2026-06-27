import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
try:
    r=lam.invoke(FunctionName="justhodl-fedwatch-rate-probability",InvocationType="RequestResponse",Payload=b"{}")
    print("FunctionError:", r.get("FunctionError"))
    print("resp:", r["Payload"].read().decode()[:300])
    time.sleep(3)
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/fedwatch.json")["Body"].read())
    print("fedwatch.json keys:", list(d.keys())[:18])
    for k in ("base_target","current_target","next_meeting","meetings","implied_path","scenario_6mo","summary","headline"):
        if k in d: print(f"  {k}: {json.dumps(d[k])[:220]}")
except Exception as e: print("err:", str(e)[:160])
print("DONE 2336")
