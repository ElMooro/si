import boto3, json, time
s3=boto3.client("s3","us-east-1")
d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/bottleneck-boom-research.json")["Body"].read())
print("research gen:", d.get("generated_at"), "| has target_record key:", "target_record" in d)
tr=d.get("track_record") or {}
print("#2 windows:", list((tr.get('windows') or {}).keys()), "| maturity:", json.dumps(tr.get("maturity")))
print("#3 target_record:", json.dumps(d.get("target_record")))
# DDB: were targets logged?
try:
    from boto3.dynamodb.conditions import Attr
    tbl=boto3.resource("dynamodb","us-east-1").Table("justhodl-signals")
    items=[]; lek=None
    for _ in range(4):
        kw=dict(FilterExpression=Attr("signal_type").eq("bottleneck_target"),Limit=300)
        if lek: kw["ExclusiveStartKey"]=lek
        r=tbl.scan(**kw); items+=r.get("Items",[]); lek=r.get("LastEvaluatedKey")
        if not lek: break
    print(f"\nDDB bottleneck_target signals: {len(items)}")
    for it in items[:6]:
        print(f"  {it.get('ticker')} logged={str(it.get('logged_at'))[:10]} base@{it.get('baseline_price')} -> tp_base {it.get('target_base')} bull {it.get('target_bull')}")
except Exception as e:
    print("ddb err",str(e)[:80])
print("DONE 2297")
