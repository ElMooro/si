import json, boto3
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
lam.invoke(FunctionName="justhodl-retail-sentiment", InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/retail-sentiment.json")["Body"].read())
hot=(d.get("ranked") or {}).get("hottest",[])
print("hot sample stages/hist:")
for e in hot[:6]:
    print(f"  {e.get('ticker'):6} stage={e.get('attention_stage')} days={e.get('days_tracked')} hist={e.get('mentions_hist')} trend7d={e.get('trend_7d_pct')}")
try:
    h=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/retail-attention-history.json")["Body"].read())
    bt=h.get("by_ticker",{})
    print(f"history file: {len(bt)} tickers; sample MU points:", (bt.get('MU') or [])[-3:])
except Exception as e: print("hist file:",str(e)[:80])
