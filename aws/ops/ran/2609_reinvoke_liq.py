"""ops 2609 — redeploy liquidity-inflection fix + invoke + verify all blocks populate."""
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
for a in range(6):
    try: lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue()); print("deployed"); break
    except lam.exceptions.ResourceConflictException: time.sleep(12); wait()
wait()
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("INVOKE:", r.get("StatusCode"), r.get("FunctionError"), r["Payload"].read().decode()[:170])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/liquidity-inflection.json")["Body"].read())
print("version:", j.get("version"))
c=j.get("composite") or {}
print("COMPOSITE: score", c.get("liquidity_score"), "| regime", c.get("regime"), "| z", c.get("composite_z"), "| n", c.get("n_components"))
print("  components:", [(x['name'],x['eff_z'],x['weight']) for x in c.get("components",[])])
rs=j.get("reserves") or {}; print("RESERVES: $bn", rs.get("level_usd_bn"), "dir", rs.get("direction"), "z", rs.get("impulse_z"),"|", rs.get("scarcity_note"))
rp=j.get("rrp") or {}; print("RRP: $bn", rp.get("level_usd_bn"),"|", rp.get("buffer_note"))
tg=j.get("tga") or {}; print("TGA: $bn", tg.get("level_usd_bn"),"dir",tg.get("direction"))
fs=j.get("funding_stress") or {}; si=fs.get("sofr_iorb") or {}
print("SOFR-IORB:", si.get("spread_bps"),"bps trend20d",si.get("trend_20d_bps"),"|", si.get("stress"))
print("  funding_plumbing:", fs.get("funding_plumbing"))
print("  eurodollar_plumbing:", fs.get("eurodollar_plumbing"))
print("  move:", fs.get("move"))
gl=j.get("global_liquidity") or {}; print("GLOBAL LIQ: idx", gl.get("index"),"impulse13w%",gl.get("impulse_13w_pct"),gl.get("regime"))
dl=j.get("dollar") or {}; print("DOLLAR:", dl.get("level"),"dir",dl.get("direction"),"eff_z",dl.get("eff_z"))
cr=j.get("credit") or {}; print("CREDIT:", cr.get("regime"), cr.get("hy_oas_bps"),"bps")
fc=j.get("financial_conditions") or {}; print("NFCI:", fc.get("nfci"), fc.get("read"),"tightening",fc.get("tightening"))
ss=j.get("systemic_stress") or {}; print("SYSTEMIC:", ss.get("global_stress_index"), ss.get("level"))
print("DONE 2609")
