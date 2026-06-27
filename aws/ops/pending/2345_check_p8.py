import boto3, json, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
def doc():
    return json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/cycle-clock.json")["Body"].read())
b4=doc().get("generated_at")
lam.invoke(FunctionName="justhodl-cycle-clock",InvocationType="Event",Payload=b"{}")
d=None
for i in range(16):
    time.sleep(12); cur=doc()
    if cur.get("generated_at")!=b4: d=cur; print(f"wrote v{cur.get('version')} dur {cur.get('duration_s')}s"); break
if not d: d=doc()
sy=d.get("synthesis") or {}; tr=d.get("trajectory") or {}
print("VERSION:",d.get("version"))
if sy:
    print("\nSYNTHESIS: posture",sy.get("posture"),"score",sy.get("score"),"conviction",sy.get("conviction"),"|",sy.get("n_risk_off"),"off vs",sy.get("n_risk_on"),"on")
    print("bottom_line:",sy.get("bottom_line"))
    print("bearish:",[c["label"] for c in (sy.get("bearish_drivers") or [])])
    print("bullish:",[c["label"] for c in (sy.get("bullish_drivers") or [])])
    print("own:",sy.get("own_whats_leading"),"| reduce:",sy.get("reduce_whats_lagging"))
    print("key_risk:",(sy.get("key_risk") or "")[:90])
    print("\nTRAJECTORY: days",tr.get("n_days_logged"),"| series",len(tr.get("series") or []))
else:
    print("NO SYNTHESIS — still",d.get("version"))
print("DONE 2345")
