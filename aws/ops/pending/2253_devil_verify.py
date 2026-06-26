import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-equity-research")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
lam.invoke(FunctionName="justhodl-equity-research", InvocationType="Event",
           Payload=json.dumps({"ticker":"LDOS","force_refresh":True,"_internal":"1"}).encode())
print("regenerating LDOS; polling for devils_advocate...")
for i in range(20):
    time.sleep(13)
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read())
        da=d.get("devils_advocate"); v=d.get("verdict") or {}
        fresh = (d.get("executive_summary") or "") and v.get("rating")
        if fresh and da is not None:
            print(f"t+{(i+1)*13}s: devils_advocate PRESENT")
            print("  title:", da.get("title"))
            print("  short_thesis:", str(da.get("short_thesis"))[:340])
            print("  kill_points:", [(k.get("point"),k.get("evidence")) for k in (da.get("kill_points") or [])])
            print("  what_bulls_underestimate:", str(da.get("what_bulls_underestimate"))[:180])
            break
        elif fresh and da is None:
            print(f"t+{(i+1)*13}s: synthesis fresh (verdict={v.get('rating')}) but devils_advocate STILL ABSENT — model not emitting it")
            break
        else:
            print(f"t+{(i+1)*13}s: still generating...")
    except Exception as e:
        print(f"t+{(i+1)*13}s: {str(e)[:50]}")
print("DONE 2253")
