import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-equity-research")["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    time.sleep(3)
def doc(t): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=f"equity-research/{t}.json")["Body"].read())
before=doc("LDOS").get("generated_at")
lam.invoke(FunctionName="justhodl-equity-research",InvocationType="Event",Payload=json.dumps({"ticker":"LDOS","force_refresh":True,"_internal":"1"}).encode())
print("regen LDOS for OMON data...")
d=None
for i in range(22):
    time.sleep(13); cur=doc("LDOS")
    if cur.get("generated_at")!=before: d=cur; print(f"t+{(i+1)*13}s WROTE"); break
    print(f"t+{(i+1)*13}s...")
if d:
    oe=d.get("options_expectations")
    print("\nOPTIONS_EXPECTATIONS:", json.dumps(oe, indent=1) if oe else "None/null")
    print("\nANR still intact:", bool((d.get('analyst_ratings') or {}).get('distribution')))
    print("price pts:", len(d.get('price_history') or []), "| business_mix:", bool((d.get('business_mix') or {}).get('segments')))
print("DONE 2279")
