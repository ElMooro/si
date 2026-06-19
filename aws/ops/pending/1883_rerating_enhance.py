import boto3, json, zipfile, io, glob, time
from botocore.exceptions import ClientError
lam=boto3.client("lambda","us-east-1"); s3=boto3.client("s3","us-east-1"); B="justhodl-dashboard-live"
FN="justhodl-ai-rerating-radar"
src=open(glob.glob("**/justhodl-ai-rerating-radar/source/lambda_function.py",recursive=True)[0]).read()
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",src)
code=buf.getvalue()
def wait():
    for _ in range(90):
        st=lam.get_function_configuration(FunctionName=FN)
        if st.get("State")=="Active" and st.get("LastUpdateStatus")!="InProgress": return st
        time.sleep(3)
    return st
wait()
for _ in range(24):
    try: lam.update_function_code(FunctionName=FN,ZipFile=code); break
    except ClientError as e:
        if "ResourceConflict" in str(e): time.sleep(5); continue
        raise
wait(); print("deployed")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse"); print("INVOKE:",r["Payload"].read().decode()[:220])
d=json.loads(s3.get_object(Bucket=B,Key="data/ai-rerating-radar.json")["Body"].read()); sm=d["summary"]
print("universe=%s priced=%s candidates=%s rising=%s contagion=%s"%(sm.get("n_universe"),sm.get("n_priced"),sm.get("n_candidates"),sm.get("n_rising"),sm.get("n_contagion")))
print("\nTOP SETUPS (now with revision-velocity + contagion):")
for r in (sm.get("top_setups") or [])[:8]:
    print("  %-7s %-6s g=%-5s ev/s=%-5s z=%-6s rising=%-5s vel=%-5s contag=%-5s | %s"%(
        r.get("symbol"),r.get("cap_bucket"),r.get("growth_pct"),r.get("ev_sales"),r.get("unpriced_z"),
        str(r.get("estimates_rising")),r.get("revision_velocity"),str(r.get("contagion")),(r.get("why") or "")[:46]))
print("\n★ CONTAGION CANDIDATES (layer leader rising, this laggard hasn't):")
for r in (sm.get("contagion_candidates") or [])[:8]:
    print("  %-7s %-6s layer=%-14s leader=%-6s g=%-5s ev/s=%-5s | %s"%(
        r.get("symbol"),r.get("cap_bucket"),r.get("layer"),r.get("layer_leader"),r.get("growth_pct"),r.get("ev_sales"),(r.get("name") or "")[:20]))
if not sm.get("contagion_candidates"): print("  (none in current window — no layer leader is mid-revision-up right now)")
