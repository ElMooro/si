"""ops 2022: verify dark-pool ETF-filter + ignition dark wire + harvester + page."""
import boto3, json, time, urllib.request
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); ddb=boto3.client("dynamodb","us-east-1"); B="justhodl-dashboard-live"
r=lam.invoke(FunctionName="justhodl-dark-pool",InvocationType="RequestResponse")
print("dark-pool:",r["StatusCode"],"|",r["Payload"].read().decode()[:420])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/dark-pool.json")["Body"].read())
print("\nscored:",d.get("n_scored"),"accum:",d.get("distribution",{}).get("accumulation"),"picks:",len(d.get("top_picks") or []))
print("TOP ACCUMULATION (should be single stocks, no ETFs):")
for r in (d.get("top_accumulation") or [])[:12]:
    print(f"  {r['ticker']:<6} sc={r['score']:<5} dark%={r['dark_pool_pct']:<6} accel={r['dark_accel']} wkRet={r['week_return_pct']}%")
print("PICKS:",[p['ticker'] for p in d.get('top_picks',[])])
print("\ninvoking signal-harvester…")
try:
    rr=lam.invoke(FunctionName="justhodl-signal-harvester",InvocationType="RequestResponse"); print(" harvester:",rr["StatusCode"],"|",rr["Payload"].read().decode()[:160])
except Exception as e: print(" harvester err:",str(e)[:120])
time.sleep(3)
n=0
for pg in ddb.get_paginator("scan").paginate(TableName="justhodl-signals",
        FilterExpression="signal_type = :t",ExpressionAttributeValues={":t":{"S":"eng:dark-pool"}},
        ProjectionExpression="signal_id",Limit=200):
    n+=len(pg.get("Items",[]))
    if n>=200: break
print("eng:dark-pool rows in justhodl-signals:",n)
print("\ninvoking ignition (P4 dark wire check)…")
try:
    ri=lam.invoke(FunctionName="justhodl-ignition",InvocationType="RequestResponse"); print(" ignition:",ri["StatusCode"])
    time.sleep(2)
    ig=json.loads(s3.get_object(Bucket=B,Key="data/ignition.json")["Body"].read())
    probes=ig.get("probes") or ig.get("probe_status") or {}
    print(" ignition probes.dark:",probes.get("dark"),"| sample dark_to_adv on board:",
          [ (x.get("ticker"),x.get("dark_to_adv_w")) for x in (ig.get("board") or ig.get("rows") or [])[:5] if x.get("dark_to_adv_w") is not None ][:5])
except Exception as e: print(" ignition err:",str(e)[:160])
print("\npage:")
try:
    with urllib.request.urlopen(urllib.request.Request(f"https://justhodl.ai/dark-pool.html?t={int(time.time())}",headers={"User-Agent":"v"}),timeout=20) as resp:
        print("  dark-pool.html HTTP",resp.getcode(),"bytes",len(resp.read()))
except Exception as e: print("  page err",str(e)[:80])
print("DONE 2022")
