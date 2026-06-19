import boto3, json, zipfile, io, glob, time
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
FN="justhodl-ai-infra-stack"
src=open(glob.glob("**/justhodl-ai-infra-stack/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
for _ in range(60):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN,ZipFile=code); break
    except ClientError as e:
        if "ResourceConflict" in str(e): time.sleep(5); continue
        raise
for _ in range(60):
    st=lam.get_function_configuration(FunctionName=FN)
    if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": break
    time.sleep(3)
print("deployed")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:200])
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/ai-infra-stack.json")["Body"].read())
layers=[(l["layer"],l["n_names"]) for l in d.get("stack",[])]
print("\nLAYERS now:",layers)
# verify MU/SNDK in memory, miners/neocloud populated
loc={}
for l in d.get("stack",[]):
    for n in l.get("names",[]): loc.setdefault(n["symbol"],l["layer"])
for t in ["MU","SNDK","WDC","CRWV","NBIS","IREN","CORZ","RIOT","CLSK","APLD","HUT","BTDR"]:
    print("  %-6s -> %s"%(t,loc.get(t,"(not in universe)")))
print("\nminers_to_ai names:",[n["symbol"] for l in d.get("stack",[]) if l["layer"]=="miners_to_ai" for n in l["names"]])
print("neocloud names:",[n["symbol"] for l in d.get("stack",[]) if l["layer"]=="neocloud" for n in l["names"]])
print("memory names:",[n["symbol"] for l in d.get("stack",[]) if l["layer"]=="memory" for n in l["names"]])
