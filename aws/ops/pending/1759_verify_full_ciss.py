import json, time, boto3
from botocore.config import Config
s3=boto3.client("s3",region_name="us-east-1")
lam=boto3.client("lambda",region_name="us-east-1",config=Config(read_timeout=300,retries={"max_attempts":0}))
try: before=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ciss-stress.json")["Body"].read()).get("generated_at")
except: before=None
lam.invoke(FunctionName="justhodl-ciss-stress",InvocationType="Event")
print("invoked; polling...")
d=None
for i in range(14):
    time.sleep(20)
    try:
        d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ciss-stress.json")["Body"].read())
        if d.get("generated_at")!=before: break
    except: pass
if not d or d.get("generated_at")==before: print("no refresh"); raise SystemExit
print("v=%s n_series=%s elapsed=%ss" % (d.get("version"),d.get("n_series"),d.get("elapsed_s")))
print("categories:", d.get("categories"))
ser=d.get("series",[])
print("\nALL areas present:", sorted(set(s["area"] for s in ser)))
for want in ["US","CN","GB","CZ","DK","HU","PL","SE","GR"]:
    hits=[s for s in ser if s["area"]==want]
    parts=["%s (%s%s)" % (s["indicator"], s["latest_date"], " DISC" if s.get("discontinued") else "") for s in hits]
    print("  %s: %s" % (want, "; ".join(parts) if parts else "MISSING"))
print("\ncountry_ciss:", sorted(s["country"] for s in ser if s["category"]=="country_ciss"))
print("sovereign_country:", sorted(s["country"] for s in ser if s["category"]=="sovereign_country"))
print("discontinued:", sum(1 for s in ser if s.get("discontinued")), "| payload MB:", round(len(json.dumps(d))/1e6,2))
