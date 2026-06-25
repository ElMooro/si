import boto3, json
s3=boto3.client("s3","us-east-1")
# 1) buyback-scanner real schema
try:
    bb=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/buyback-scanner.json")["Body"].read())
    print("buyback-scanner keys:", list(bb.keys())[:12])
    for k,v in bb.items():
        if isinstance(v,list) and v and isinstance(v[0],dict):
            print(f"  list '{k}' n={len(v)} item-keys={list(v[0].keys())[:7]}")
except Exception as e: print("buyback ERR",str(e)[:40])
# 2) scan flow-confluence for insider/buyback tags/engines anywhere
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/flow-confluence.json")["Body"].read())
found=set()
def scan(lst):
    for r in lst:
        if not isinstance(r,dict): continue
        for e in (r.get("engines") or []):
            if e in ("insider","buyback","insider-buyback"): found.add((r.get("ticker"),e))
        for t in (r.get("tags") or []):
            if "insider" in t or "buyback" in t: found.add((r.get("ticker"),t))
scan(d.get("multi_engine_confluence") or [])
for v in (d.get("by_posture") or {}).values():
    if isinstance(v,list): scan(v)
print("insider/buyback appearances in flow-confluence:", list(found)[:10] or "NONE")
# 3) insider-cluster tickers present in flow ticker_map?
ins=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/insider-clusters.json")["Body"].read())
itk=[c.get("ticker") for c in (ins.get("clusters") or [])][:8]
tm=d.get("ticker_map") or {}
print("insider tickers:", itk)
print("of those in flow ticker_map:", [t for t in itk if t in tm])
print("DONE 2206")
