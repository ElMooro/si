import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc():
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
    except Exception as e: return {"_e":str(e)[:50]}
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
print("regen; polling...")
d=None
for i in range(14):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4: d=cur; break
    print(f"  t+{(i+1)*12}s...")
if d:
    nl=(d.get("liquidity") or {}).get("net_liquidity") or {}
    print("net liquidity: net",nl.get("net_tn"),"T = WALCL",nl.get("walcl_tn"),"− RRP",nl.get("rrp_tn"),"− TGA",nl.get("tga_tn"),"| Δ13w",nl.get("net_13w_delta_bn"),"B")
    print("flickers:", (d.get('liquidity') or {}).get('flickers'))
print("DONE 2317")
