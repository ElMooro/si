import boto3, io, zipfile, json, time
REGION="us-east-1"; FN="justhodl-liquidity-inflection"; SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
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
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/liquidity-inflection.json")["Body"].read())
fe=j.get("forward_expectation") or {}
fec=j.get("forward_expectation_composite") or {}
print("\nNET-LIQ FWD EXP: state",fe.get("state"),"z",fe.get("impulse_z"))
for a,v in (fe.get("assets") or {}).items(): print(f"   {a}: {v['mean']}% (exc {v['excess']}%, n{v['n']}{' *' if v['sig'] else ''})")
print("\nCOMPOSITE FWD EXP: state",fec.get("state"),"comp_z",fec.get("composite_z"),"src",fec.get("source"))
for a,v in (fec.get("assets") or {}).items(): print(f"   {a}: {v['mean']}% (exc {v['excess']}%, n{v['n']}{' *' if v['sig'] else ''})")
print("   components:",fec.get("components_used"))
an=j.get("analogs") or {}
print("\nANALOGS composite_aware:",an.get("composite_aware"),"| features:",an.get("features"))
print("  fingerprint_now:",an.get("fingerprint_now"))
for a in (an.get("analogs") or [])[:3]: print(f"   {a['date']} sim {a['similarity_pct']}% SPX +21d {a['spx_fwd_21d']}% / +63d {a['spx_fwd_63d']}%")
rrc=j.get("regime_returns_composite") or {}
print("\nCOMPOSITE REGIME STUDY assets:",list(rrc.keys()),"| SPX n_total:",(rrc.get("SPX_proxy") or {}).get("n_total"))
print("DONE 2636")
