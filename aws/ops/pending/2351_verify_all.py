import boto3, json, time, urllib.request
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc(): return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
d=None
for i in range(18):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4 and cur.get("version")=="2.8": d=cur; print(f"wrote v2.8 dur {cur.get('duration_s')}s"); break
if not d: print("NO v2.8:",doc().get("version")); d=doc()
fb=((d.get("stress_scenarios") or {}).get("firm_book")) or {}
print("\n=== FIRM BOOK ===")
print("  posture:",fb.get("posture"),"| net:",fb.get("net_pct"),"% | vol:",fb.get("annual_vol_pct"),"% | VaR99:",fb.get("var_99_1d_pct"),"% | limits:",fb.get("soft_pct"),"/",fb.get("hard_pct"))
print("  worst:",[(w["scenario"][:28],w["pnl_pct"]) for w in (fb.get("worst") or [])][:3])
print("  reverse to -15%:",(fb.get("reverse") or {}).get("to_soft_mult"),"x | to -25%:",(fb.get("reverse") or {}).get("to_hard_mult"),"x")
print("DONE 2351")
