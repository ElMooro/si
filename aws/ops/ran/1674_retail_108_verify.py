import json, boto3
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
lam.invoke(FunctionName="justhodl-retail-sentiment", InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/retail-sentiment.json")["Body"].read())
r=d.get("ranked",{})
print("#10 lifecycle — igniting:",len(r.get("igniting",[])),"peaking:",len(r.get("peaking",[])),"fading:",len(r.get("fading",[])))
sp=d.get("signal_persistence",{}); print("  persistence:",sp.get("median_days_elevated"),"median days, measured on",sp.get("n_tickers_measured"),"tickers")
ig=r.get("igniting",[])
if ig: print("  igniting sample:",[(e.get("ticker"),e.get("lifecycle")) for e in ig[:4]])
print("#8 theme_rollup:",len(d.get("theme_rollup",[])),"themes")
for t in d.get("theme_rollup",[])[:6]:
    print(f"   {t['theme']:22} ment={t['total_mentions']:5} names={t['n_names']} avgVel={t.get('avg_velocity')} avgChg={t.get('avg_change')} top={[n['ticker'] for n in t['names'][:3]]}")
