import boto3, json, time
lam=boto3.client("lambda","us-east-1"); logs=boto3.client("logs","us-east-1")
for fn in ["justhodl-finviz-universe","justhodl-finviz-signals"]:
    print("=== %s ==="%fn)
    try:
        r=lam.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=b"{}")
        print("  FunctionError:",r.get("FunctionError"))
        print("  resp:",r["Payload"].read().decode()[:200])
    except Exception as e:
        print("  invoke err:",str(e)[:100])
    time.sleep(3)
    try:
        grp="/aws/lambda/"+fn
        st=logs.describe_log_streams(logGroupName=grp,orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
        if st:
            ev=logs.get_log_events(logGroupName=grp,logStreamName=st[0]["logStreamName"],limit=40,startFromHead=False)["events"]
            errl=[e["message"].strip()[:160] for e in ev if any(w in e["message"] for w in ("Error","error","Traceback","403","404","429","Exception","fail","FAIL"))]
            for l in errl[-6:]: print("   LOG:",l)
    except Exception as e: print("  logs err:",str(e)[:80])
print("DONE 2411")
