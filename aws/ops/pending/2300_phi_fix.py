import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom-research.json")["Body"].read())
    except Exception as e: return {"_e":str(e)[:40]}
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-bottleneck-research",InvocationType="Event",Payload=b"{}")
print("regen after _phi fix; polling...")
d=None
for i in range(18):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4: d=cur; print(f"  t+{(i+1)*12}s WROTE"); break
    print(f"  t+{(i+1)*12}s...")
if d:
    tr=d.get("track_record") or {}
    print("dur_s:", d.get("duration_s"), "new_theses:", d.get("new_theses"))
    print("#2 windows:", list((tr.get('windows') or {}).keys()), "| maturity:", json.dumps(tr.get("maturity")))
    print("#3 target_record:", json.dumps(d.get("target_record")))
    print("pressure_pctiles:", json.dumps(d.get("pressure_pctiles")))
    bt=d.get("by_ticker") or {}
    print("fwd_val on LDOS:", bool((bt.get('LDOS') or {}).get('fwd_val')), "| candidates:", len(bt))
else: print("STILL stale")
print("DONE 2300")
