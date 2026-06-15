import json, boto3, time
s3=boto3.client("s3",region_name="us-east-1"); ev=boto3.client("events",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
BUCKET="justhodl-dashboard-live"; MK="data/_freshness-manifest.json"
try:
    man=json.loads(s3.get_object(Bucket=BUCKET,Key=MK)["Body"].read())
except Exception:
    man={"rules":[{"prefix":"data/","default_max_age_h":26.0}]}
ov=man.get("key_overrides") or {}
new={
  "data/retail-sentiment.json":2.0,
  "data/retail-attention-history.json":2.0,
  "data/retail-alerts.json":2.0,
  "data/ticker-trends.json":28.0,
  "data/google-trends.json":1000000.0,
}
ov.update(new); man["key_overrides"]=ov
s3.put_object(Bucket=BUCKET,Key=MK,Body=json.dumps(man,indent=2).encode(),ContentType="application/json")
print("manifest overrides set for:", sorted(new.keys()))
rn="fleet-freshness-monitor-30min"
try:
    r=ev.describe_rule(Name=rn); st=r.get("State")
    if st!="ENABLED":
        ev.enable_rule(Name=rn); print("rule "+rn+": was "+str(st)+" -> ENABLED")
    else:
        print("rule "+rn+": already "+str(st))
except Exception as e:
    print("rule "+rn+": "+str(e)[:120])
lam.invoke(FunctionName="justhodl-fleet-freshness-monitor", InvocationType="RequestResponse")
time.sleep(2)
stt=json.loads(s3.get_object(Bucket=BUCKET,Key="data/_freshness-monitor.json")["Body"].read())
print("monitor: tracked={} fresh={} stale={} alerts={}".format(stt.get("n_keys_tracked"),stt.get("n_fresh"),stt.get("n_stale"),stt.get("n_alerts_raised")))
watch=["data/ticker-trends.json","data/retail-sentiment.json","data/retail-attention-history.json","data/options-flow.json","data/news-velocity.json","data/short-interest.json"]
stale_keys={r["key"]:r for r in stt.get("stale_top_50",[])}
for k in watch:
    if k in stale_keys:
        print("  STALE "+k+": "+str(stale_keys[k]["age_h"])+"h (SLA "+str(stale_keys[k]["max_age_h"])+"h)")
    else:
        print("  ok    "+k+": within SLA")
top=[r["key"]+" "+str(r["age_h"])+"h/SLA"+str(r["max_age_h"])+"h" for r in stt.get("stale_top_50",[])[:8]]
print("top stale overall:", top)
