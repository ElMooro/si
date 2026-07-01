"""ops 2674 — redeploy with sibling-pair filtering, use proper long client read_timeout
this time (the 60s default bit us on the previous run)."""
import boto3, io, zipfile, json, time
from botocore.config import Config
REGION="us-east-1"; FN="justhodl-signal-genealogy"; SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION, config=Config(read_timeout=200, connect_timeout=10, retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION)
def wait():
    for _ in range(30):
        c=lam.get_function(FunctionName=FN)["Configuration"]
        if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": return
        time.sleep(4)
wait()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); print("deployed"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
print("INVOKE:",r.get("StatusCode"),r.get("FunctionError"))
body = r["Payload"].read().decode()
print("BODY:", body[:400])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/signal-genealogy.json")["Body"].read())
print("\nversion:", j.get("version"), "elapsed_s:", j.get("elapsed_s"))
print("n_pairs_tested:", j.get("n_pairs_tested"), "| n_sibling_pairs_excluded:", j.get("n_sibling_pairs_excluded"))
print("n_significant_pairs (final):", j.get("n_significant_pairs"))
rate = j.get("n_significant_pairs",0)/max(j.get("n_pairs_tested",1)-j.get("n_sibling_pairs_excluded",0),1)*100
print(f"significant rate (of non-sibling pairs): {rate:.1f}%")
print("\ntop 12 EARLIEST signals:")
for r2 in (j.get("earliest_signals") or [])[:12]:
    print(f"  {r2['signal_type']:35s} earliness={r2['earliness_index']:+.1f} n={r2['n_firings']} leads_spy={r2['leads_spy']}")
print("\ntop 12 significant cascades (post sibling-filter + FDR):")
for p in (j.get("significant_cascades") or [])[:12]:
    print(f"  {p['leader']:28s} -> {p['follower']:28s}  lag={p['lag_days']}d corr={p['corr']} t={p['t']} n={p['n']}")
print("DONE 2674")
