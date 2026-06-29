import boto3, json, io, zipfile, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k):
    try: return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
    except Exception as e: return {"_err":str(e)[:60]}
before=g("data/master-allocation.json").get("target_allocation") or {}
print("BEFORE target_allocation:",json.dumps(before))
SRC="aws/lambdas/justhodl-master-allocator/source/lambda_function.py"
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC).read())
lam.update_function_code(FunctionName="justhodl-master-allocator",ZipFile=buf.getvalue())
print("code updated"); time.sleep(8)
r=lam.invoke(FunctionName="justhodl-master-allocator",InvocationType="RequestResponse",Payload=b"{}")
print("invoke err:",r.get("FunctionError"))
time.sleep(3)
d=g("data/master-allocation.json"); after=d.get("target_allocation") or {}
print("AFTER  target_allocation:",json.dumps(after))
print("=== DELTA (pp) ===")
for a in sorted(set(list(before)+list(after))):
    db=before.get(a,0); da=after.get(a,0)
    if abs((da or 0)-(db or 0))>=0.01: print("  %-12s %+.2f -> %+.2f  (%+.2f)"%(a,db,da,da-db))
# show new signal contributions
sigs=d.get("signals") or d.get("signal_detail") or {}
print("=== new signals present? ===")
rationale=json.dumps(d)[:5]  # touch
for nm in ["gold_rotation","foreign_bond_demand","Gold/Metals Rotation","Foreign UST Demand"]:
    print("  %-22s %s"%(nm,"FOUND" if nm in json.dumps(d) else "absent"))
print("DONE 2516")
