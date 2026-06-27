import boto3, json, time, urllib.request
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc(): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
d=None
for i in range(18):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4 and cur.get("version")=="3.0": d=cur; print(f"wrote v3.0 dur {cur.get('duration_s')}s"); break
if not d: print("NO v3.0:",doc().get("version")); d=doc()
yc=d.get("yield_curve") or {}
print("regime:",yc.get("regime"),"|",yc.get("regime_desc"))
print("curve points:",len(yc.get("curve_points") or []),"→",[(p["tenor"],p["yield_pct"]) for p in (yc.get("curve_points") or [])])
print("spreads:",yc.get("spreads_bps"))
print("term premium:",yc.get("term_premium_bps"),"| 10y real:",yc.get("real_10y_pct"),"| breakeven:",yc.get("breakeven_10y_pct"))
print("DONE 2359")
