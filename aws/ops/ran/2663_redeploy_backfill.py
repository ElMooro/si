"""ops 2663 — redeploy with sector/industry backfill for missed AI-cohort names."""
import boto3, io, zipfile, json, time
REGION="us-east-1"; FN="justhodl-ai-rerating-radar"; SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
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
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/ai-rerating-radar.json")["Body"].read())
print("\nversion:", j.get("version"), "elapsed_s:", j.get("elapsed_s"))
cov = j.get("coverage", {})
print("universe:", cov.get("n_universe"), "AI cohort:", cov.get("n_ai_cohort"), "sectors:", cov.get("n_sectors"))
print("by_sector:", cov.get("by_sector"))
print("candidates_by_sector:", cov.get("candidates_by_sector"))
s = j.get("summary", {})
print("\nn_priced:", s.get("n_priced"), "n_candidates:", s.get("n_candidates"))
print("\ntop 15 setups:")
for r2 in (s.get("top_setups") or [])[:15]:
    print(f"  {r2['symbol']:6s} {str(r2.get('sector')):22s} {str(r2.get('industry')):24s} score={r2['composite']:.1f} ai={r2['in_ai_cohort']} disc={r2.get('discount_to_implied_pct')}")
print("\ndeepest_discounts (non-AI check):")
for r2 in (s.get("deepest_discounts") or [])[:8]:
    print(f"  {r2['symbol']:6s} {str(r2.get('sector')):20s} ai={r2['in_ai_cohort']} disc={r2.get('discount_to_implied_pct')}%")
print("DONE 2663")
