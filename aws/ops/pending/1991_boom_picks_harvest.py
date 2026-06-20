import boto3, json, io, zipfile, time
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1")
FN="justhodl-boom-radar"
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
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse")
print("boom-radar invoke:",r.get("StatusCode"),r.get("FunctionError"))
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/boom-radar.json")["Body"].read())
print(f"  2way={j['n_2way']} 3way={j['n_3way']} picks={len(j.get('top_picks',[]))}")
print("  TOP PICKS:",[(p['ticker'],p['convergence'],p['score'],'+'.join(p['dimensions'])) for p in j.get('top_picks',[])])
# harvest now so picks enter the scorecard pipeline immediately
HV="justhodl-signal-harvester"
try:
    rh=lam.invoke(FunctionName=HV,InvocationType="RequestResponse")
    body=rh["Payload"].read().decode()
    print("harvester:",rh.get("StatusCode"),rh.get("FunctionError"))
    print("  ",body[:400])
    # confirm boom-radar logged
    import re
    print("  boom-radar mentioned:", "boom-radar" in body.lower())
except Exception as e: print("harvester invoke err",e)
print("DONE 1991")
