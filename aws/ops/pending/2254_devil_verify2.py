import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="equity-research/LDOS.json")["Body"].read())
before=doc().get("generated_at")
print("before generated_at:", before)
lam.invoke(FunctionName="justhodl-equity-research", InvocationType="Event",
           Payload=json.dumps({"ticker":"LDOS","force_refresh":True,"_internal":"1"}).encode())
print("regenerating; waiting for generated_at to CHANGE...")
for i in range(22):
    time.sleep(13)
    d=doc(); gen=d.get("generated_at")
    if gen!=before:
        da=d.get("devils_advocate"); v=d.get("verdict") or {}
        print(f"t+{(i+1)*13}s NEW doc gen={gen} verdict={v.get('rating')}")
        if da is not None:
            print("  >>> devils_advocate PRESENT")
            print("  title:", da.get("title"))
            print("  short_thesis:", str(da.get("short_thesis"))[:380])
            print("  kill_points:")
            for k in (da.get("kill_points") or []): print("     -", k.get("point"), "::", k.get("evidence"))
            print("  what_bulls_underestimate:", str(da.get("what_bulls_underestimate"))[:200])
        else:
            print("  >>> devils_advocate STILL ABSENT in fresh doc — model is not emitting the key; needs prompt hoist")
        break
    else:
        print(f"t+{(i+1)*13}s: not regenerated yet")
print("DONE 2254")
