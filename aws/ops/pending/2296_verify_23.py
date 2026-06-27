import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom-research.json")["Body"].read())
    except Exception as e: return {"_e":str(e)[:40]}
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-bottleneck-research",InvocationType="Event",Payload=b"{}")
print("regen; polling for target_record + maturity...")
d=None
for i in range(20):
    time.sleep(13); cur=doc()
    if cur.get("generated_at")!=b4 and ("target_record" in cur):
        d=cur; print(f"  t+{(i+1)*13}s updated"); break
    print(f"  t+{(i+1)*13}s...")
if d:
    tr=d.get("track_record") or {}
    print("\n#2 track windows:", list((tr.get('windows') or {}).keys()))
    print("#2 maturity:", json.dumps(tr.get("maturity")))
    print("\n#3 target_record:", json.dumps(d.get("target_record")))
print("DONE 2296")
