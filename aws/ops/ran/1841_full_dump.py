import json, boto3
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
B="justhodl-dashboard-live"
lam.invoke(FunctionName="justhodl-eurodollar-plumbing",InvocationType="RequestResponse")
d=json.loads(s3.get_object(Bucket=B,Key="data/eurodollar-plumbing.json")["Body"].read())
print("HEALTH=%s  VERDICT=%s  composite_score=%s"%(d.get("plumbing_health"),d.get("verdict"),d.get("composite_score")))
for lk,lv in (d.get("layers") or {}).items():
    ms=lv.get("metrics") or []
    print("\n== L:%s  (%s)  [%d metrics] =="%(lk, lv.get("title"), len(ms)))
    for m in ms:
        print("   [%s] %-46s = %s%s"%(m.get("status"),(m.get("label") or "")[:46],m.get("value"),m.get("unit","")))
