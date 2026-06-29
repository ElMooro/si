import boto3, json
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k): return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
print("===== dark-pool.json =====")
d=g("data/dark-pool.json"); print("keys:",list(d.keys()),"| thesis:",str(d.get("thesis"))[:110],"| latest_week:",d.get("latest_week"))
for fld in ["board","top_picks"]:
    v=d.get(fld)
    if isinstance(v,list) and v: print(f" .{fld}[0]:",json.dumps(v[0])[:320])
print("\n===== 13f-positions.json =====")
t=g("data/13f-positions.json"); print("keys:",list(t.keys()),"| as_of_quarter:",t.get("as_of_quarter"))
for fld in ["most_bought","most_sold"]:
    v=t.get(fld)
    if isinstance(v,list) and v: print(f" .{fld}[0]:",json.dumps(v[0])[:300])
ab=t.get("aggregate_by_ticker")
if isinstance(ab,dict):
    k0=list(ab.keys())[:1]; print(" aggregate_by_ticker sample:",json.dumps({k0[0]:ab[k0[0]]})[:300] if k0 else "{}")
print("\n===== insider-aggregate-history.json =====")
i=g("data/insider-aggregate-history.json"); print("keys:",list(i.keys()))
sn=i.get("snapshots")
if isinstance(sn,list) and sn: print(" snapshots[-1]:",json.dumps(sn[-1])[:400])
elif isinstance(sn,dict): 
    k0=list(sn.keys())[-1:]; print(" snapshots dict last:",json.dumps({k0[0]:sn[k0[0]]})[:400] if k0 else "{}")
print("DONE 2497")
