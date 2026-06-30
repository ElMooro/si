"""ops 2617 — deploy liquidity-inflection v1.4.0 (leverage stress + dealer survey)."""
import boto3, io, zipfile, json, time
REGION="us-east-1"; FN="justhodl-liquidity-inflection"
SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
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
    try: lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); print("deployed"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("INVOKE:", r.get("StatusCode"), r.get("FunctionError"), r["Payload"].read().decode()[:120])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/liquidity-inflection.json")["Body"].read())
print("version:", j.get("version"))
ls=j.get("leverage_stress") or {}
print("LEVERAGE STRESS:", ls.get("score"), ls.get("regime"),"| margin $",ls.get("margin_debt_bn"),"bn (",ls.get("margin_pct_mcap"),"% mcap, yoy",ls.get("margin_yoy_pct"),"%) danger",ls.get("margin_danger"),"| repo_score",ls.get("repo_score"))
dv=j.get("dealer_survey") or {}
print("DEALER SURVEY:", dv.get("status"),"| last", dv.get("last_survey"))
co=j.get("composite") or {}
print("COMPOSITE:", co.get("liquidity_score"), co.get("regime"),"n",co.get("n_components"),"comps:",[c['name'] for c in co.get("components",[])])
print("DONE 2617")
