import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc(): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
d=None
for i in range(18):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4 and cur.get("version")=="2.9": d=cur; print(f"wrote v2.9 dur {cur.get('duration_s')}s"); break
if not d: print("NO v2.9:",doc().get("version")); d=doc()
wf=((d.get("synthesis") or {}).get("what_flips_it")) or {}
di=d.get("data_integrity") or {}
print("\n=== WHAT FLIPS IT ===")
print("  direction:",wf.get("direction"))
for c in (wf.get("conditions") or []): print("   ·",c)
print("  note:",wf.get("note"))
print("\n=== DATA INTEGRITY ===")
print("  sources:",di.get("n_sources"),"| fresh:",di.get("n_fresh"),"| stale:",di.get("n_stale"),"| missing:",di.get("n_missing"),"| integrity:",di.get("integrity_pct"),"%")
print("  stale:",di.get("stale"))
print("  missing:",di.get("missing"))
print("DONE 2355")
