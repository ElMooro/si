import boto3, json, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=290,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
def g(k): return json.loads(s3.get_object(Bucket=B,Key=k)["Body"].read())
# ensure not mid-update
for _ in range(20):
    st=lam.get_function_configuration(FunctionName="justhodl-master-allocator").get("LastUpdateStatus")
    if st=="Successful": break
    time.sleep(5)
print("LastUpdateStatus:",st)
before={"us_equity":39.75,"intl_dev_eq":14.58,"em_equity":4.38,"ust_short":15.46,"ust_long":10.48,"ig_credit":4.96,"gold":4.98,"btc":1.74,"cash":3.69}
r=lam.invoke(FunctionName="justhodl-master-allocator",InvocationType="RequestResponse",Payload=b"{}")
print("invoke err:",r.get("FunctionError")); time.sleep(3)
d=g("data/master-allocation.json"); after=d.get("target_allocation") or {}
print("AFTER target_allocation:",json.dumps(after))
print("=== DELTA (pp vs before) ===")
for a in sorted(set(list(before)+list(after))):
    db=before.get(a,0); da=after.get(a,0)
    if abs((da or 0)-(db or 0))>=0.01: print("  %-12s %+6.2f -> %+6.2f  (%+.2f)"%(a,db,da,da-db))
blob=json.dumps(d)
for nm in ["Gold/Metals Rotation","Foreign UST Demand"]:
    print("  signal '%s': %s"%(nm,"FOUND" if nm in blob else "ABSENT"))
# show gold + ust contributions
for a in ("gold","ust_long","ust_short"):
    cs=(d.get("rationale") or d.get("contributions") or {}).get(a) if isinstance(d.get("rationale") or d.get("contributions"),dict) else None
    if cs: print(" ",a,"contributors:",[ (c.get("signal"),c.get("tilt_pp")) for c in cs ][:6])
print("DONE 2517")
