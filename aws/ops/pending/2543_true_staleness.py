import boto3, json, time
lam=boto3.client("lambda","us-east-1")
s3=boto3.client("s3","us-east-1")
def rd(k):
    try: return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:80]}
mon=rd("data/_freshness-monitor.json")
print("=== _freshness-monitor.json top-level keys ===")
print(list(mon)[:20])
for k in ("generated_at","summary","totals","stale_count","fresh_count","n_stale","stale","stale_keys","alerts","results"):
    if k in mon:
        v=mon[k]
        if isinstance(v,list): print(f"  {k}: list[{len(v)}] sample={v[:3]}")
        else: print(f"  {k}: {json.dumps(v)[:300]}")
# divergence-interpreter recheck (Anthropic is up now)
print("\n=== divergence-interpreter recheck ===")
r=lam.invoke(FunctionName="justhodl-divergence-interpreter",InvocationType="RequestResponse",Payload=b"{}")
print("FunctionError:",r.get("FunctionError"))
print("payload:",r["Payload"].read().decode()[:400])
out=rd("data/divergence-interpreter.json")
print("output _err:",out.get("_err"),"| has interpretation:",bool(out.get("interpretation") or out.get("ai") or out.get("read")),"| model:",out.get("model"))
print("DONE 2543")
