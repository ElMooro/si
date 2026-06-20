import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1")
FN="justhodl-prepump-alerts-router"
b=io.BytesIO()
with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as z:
    zi=zipfile.ZipInfo("lambda_function.py"); zi.external_attr=0o644<<16
    z.writestr(zi,open(f"aws/lambdas/{FN}/source/lambda_function.py","rb").read())
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN,ZipFile=b.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(5)
for _ in range(30):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("State")=="Active" and c.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
print("router code updated; invoking (will dedup-send any NEW boom alerts)...")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("invoke:",r.get("StatusCode"),r.get("FunctionError"))
body=r["Payload"].read().decode()
print(body[:600])
try:
    j=json.loads(json.loads(body)["body"]) if '"body"' in body else json.loads(body)
    print("\ncounts.boom_radar:",j.get("counts",{}).get("boom_radar"))
    print("messages sent:",j.get("n_messages_sent"))
except Exception as e: print("parse note:",e)
print("DONE 1992")
