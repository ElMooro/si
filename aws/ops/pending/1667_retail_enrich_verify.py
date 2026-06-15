import json, time, boto3
from datetime import datetime, timezone
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
B="justhodl-dashboard-live"; K="data/retail-sentiment.json"
n0=datetime.now(timezone.utc)
r=lam.invoke(FunctionName="justhodl-retail-sentiment", InvocationType="RequestResponse")
print("invoke status:", r.get("StatusCode"), "err:", r.get("FunctionError"))
print("body:", r["Payload"].read().decode()[:280])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key=K)["Body"].read())
rk=d.get("ranked",{})
print("\ngenerated_at:", d.get("generated_at","")[:19], "| n_with_price:", d.get("n_with_price"))
print("ranked keys:", list(rk.keys()))
def show(lbl,arr,n=5):
    print(f"\n{lbl} (n={len(arr)}):")
    for e in arr[:n]:
        print(f"  {e.get('ticker'):6} heat={e.get('heat')} buzz={e.get('buzz_state'):10} mentions={e.get('mentions')} vel={e.get('velocity_pct')}% chg={e.get('change_pct')}% bull={e.get('stwt_bull_pct')} relvol={e.get('rel_volume')}")
show("HOTTEST", rk.get("hottest",[]))
show("MOMENTUM_CONFIRMED (buzz+price up)", rk.get("momentum_confirmed",[]))
show("FADING_DIVERGENCE (buzz, price down)", rk.get("fading_divergence",[]))
# coverage of new fields on top_30
t30=d.get("top_30_by_mentions",[])
print(f"\ntop_30 coverage: price {sum(1 for e in t30 if e.get('price') is not None)}/{len(t30)} | heat {sum(1 for e in t30 if e.get('heat') is not None)}/{len(t30)} | buzz {sum(1 for e in t30 if e.get('buzz_state'))}/{len(t30)}")
