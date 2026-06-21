import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
for _ in range(20):
    c=lam.get_function(FunctionName="justhodl-regime-map")["Configuration"]
    if c.get("LastUpdateStatus")=="Successful": break
    time.sleep(3)
print("run1 (sets baseline):", lam.invoke(FunctionName="justhodl-regime-map",InvocationType="RequestResponse")["StatusCode"])
time.sleep(3)
print("run2 (diffs vs baseline):", lam.invoke(FunctionName="justhodl-regime-map",InvocationType="RequestResponse")["StatusCode"])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/regime-map.json")["Body"].read())
print("regime:",d["regime"]["label"],"| regime_changed:",d.get("regime_changed"),"| prev_regime:",d.get("prev_regime"),"| n_transitions:",d.get("n_transitions"))
print("transitions sample:",[f"{t['ticker']}:{t['from']}→{t['to']}({t['kind']})" for t in d.get("transitions",[])[:5]] or "none (stable since baseline — expected on same-day re-run)")
prev=json.loads(s3.get_object(Bucket=B,Key="data/regime-map-prev.json")["Body"].read())
print("prev snapshot persisted:",bool(prev.get("states")),"| n states:",len(prev.get("states",{})))
print("DONE 2061")
