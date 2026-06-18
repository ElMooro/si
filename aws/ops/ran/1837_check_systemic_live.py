import json, boto3, time
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
B="justhodl-dashboard-live"; FN="justhodl-systemic-stress"
c=lam.get_function_configuration(FunctionName=FN)
print("live fn LastModified:",c["LastModified"][:19]," CodeSize:",c["CodeSize"])
try:
    r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
    print("invoke:",r["Payload"].read().decode()[:160])
except Exception as e: print("invoke err:",str(e)[:160])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/systemic-stress.json")["Body"].read())
comp=d.get("composite") or {}
cross=d.get("cross_reference") or d.get("cross") or {}
print("composite score:",comp.get("score") if isinstance(comp,dict) else comp)
# search anywhere in payload for eurodollar evidence
blob=json.dumps(d)
print("payload mentions 'eurodollar':", "eurodollar" in blob)
print("cross.eurodollar_verdict:",cross.get("eurodollar_verdict"),
      "| cross.eurodollar_funding_stress:",cross.get("eurodollar_funding_stress"),
      "| cross.eurodollar_health:",cross.get("eurodollar_health"))
