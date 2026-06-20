"""1967 — definitive: paginated DDB scan for eng:flow-lookthrough + replicate extractor."""
import boto3, json, re
ddb=boto3.resource("dynamodb","us-east-1"); s3=boto3.client("s3","us-east-1")
# 1. replicate extract_picks on live file
doc=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/flow-lookthrough.json")["Body"].read())
LIST_KEYS=["top_picks"]; SYM=("symbol","ticker","t","sym"); TRE=re.compile(r"^[A-Z][A-Z.\-]{0,6}$")
picks=[]
for lk in LIST_KEYS:
    v=doc.get(lk)
    if isinstance(v,list):
        for it in v:
            for sk in SYM:
                if it.get(sk):
                    s=str(it[sk]).strip().upper()
                    if TRE.match(s): picks.append(s)
                    break
print("extractor would yield from top_picks:", picks)

# 2. paginated scan
t=ddb.Table("justhodl-signals")
found=[]; lek=None; pages=0
while pages<25:
    kw={"FilterExpression":"signal_type = :s","ExpressionAttributeValues":{":s":"eng:flow-lookthrough"}}
    if lek: kw["ExclusiveStartKey"]=lek
    resp=t.scan(**kw); found+=resp.get("Items",[]); pages+=1
    lek=resp.get("LastEvaluatedKey")
    if not lek: break
print(f"\neng:flow-lookthrough rows in justhodl-signals: {len(found)} (scanned {pages} pages)")
for it in found[:10]:
    print("  ", it.get("measure_against"), it.get("predicted_direction"), "base=",it.get("baseline_price"),"conf=",it.get("confidence"),"logged=",str(it.get("logged_at"))[:10])
print("DONE 1967")
