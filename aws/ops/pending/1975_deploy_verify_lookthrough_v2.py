import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-flow-lookthrough"
src=open(f"aws/lambdas/{FN}/source/lambda_function.py","rb").read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    zi=zipfile.ZipInfo("lambda_function.py"); zi.external_attr=0o644<<16; z.writestr(zi,src)
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(5)
for _ in range(30):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
print("v2 deployed; invoking (first run builds v2 constituent cache, may take ~2min)...")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("StatusCode:",r.get("StatusCode"),"FunctionError:",r.get("FunctionError"))
print("payload:",r["Payload"].read()[:400])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/flow-lookthrough.json")["Body"].read())
print(f"\nv{j.get('version')}: names={j.get('n_names')} etfs={j.get('n_etfs_used')} with_delta={j.get('n_etfs_with_delta')} elapsed={j.get('elapsed_s')}")
print("\n★ ACTUAL ACCUMULATION (real share-count buying, mcap-normalised):")
for x in j.get("actual_accumulation",[])[:8]:
    print(f"  {x['ticker']:<6} delta=${(x['shares_delta_usd'] or 0)/1e6:>8.1f}M  {x.get('delta_bps_mcap')} bps  flow5d=${x['net_flow_5d_usd']/1e6:.0f}M  {'✓conf' if x.get('confirmed') else ''}")
print("\n★ ACTUAL DISTRIBUTION (real selling):")
for x in j.get("actual_distribution",[])[:5]:
    print(f"  {x['ticker']:<6} delta=${(x['shares_delta_usd'] or 0)/1e6:>8.1f}M  {x.get('delta_bps_mcap')} bps")
print("\n★ INDEX EVENTS (reconstitution add/drop):")
for x in j.get("index_events",[])[:10]:
    print(f"  {x['ticker']:<6} {x['event']:<8} by {','.join(x['etfs'][:3])}")
print("\n★ CONFIRMED top_picks (flow + actual buying agree):", [(p['ticker'],p.get('confirmed')) for p in j.get('top_picks',[])])
print("DONE 1975")
