"""ops 2608 — deploy liquidity-inflection v1.2.0 (institutional layers), invoke, verify."""
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
# bump timeout (more FRED+S3 calls now)
try: lam.update_function_configuration(FunctionName=FN, Timeout=180, MemorySize=512); wait()
except Exception as e: print("cfg:",str(e)[:60])
r=lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("INVOKE:", r.get("StatusCode"), r.get("FunctionError"), r["Payload"].read().decode()[:160])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/liquidity-inflection.json")["Body"].read())
print("version:", j.get("version"), "duration:", j.get("duration_s"))
c=j.get("composite") or {}
print("COMPOSITE: score", c.get("liquidity_score"), "regime", c.get("regime"), "z", c.get("composite_z"), "n", c.get("n_components"))
print("  components:", [(x['name'],x['eff_z']) for x in c.get("components",[])])
print("USD:", (j.get('usd') or {}).get('state'), "z", (j.get('usd') or {}).get('impulse_z'), "netliq$bn", (j.get('usd') or {}).get('net_liq_usd_bn'))
rs=j.get("reserves") or {}; print("RESERVES: $bn", rs.get("level_usd_bn"), "dir", rs.get("direction"), rs.get("scarcity_note"))
rp=j.get("rrp") or {}; print("RRP: $bn", rp.get("level_usd_bn"), rp.get("buffer_note"))
fs=j.get("funding_stress") or {}
si=fs.get("sofr_iorb") or {}; print("SOFR-IORB:", si.get("spread_bps"),"bps |", si.get("stress"))
print("  funding_plumbing:", fs.get("funding_plumbing"))
print("  move:", fs.get("move"))
gl=j.get("global_liquidity") or {}; print("GLOBAL LIQ: idx", gl.get("index"), "impulse13w%", gl.get("impulse_13w_pct"), gl.get("regime"))
dl=j.get("dollar") or {}; print("DOLLAR:", dl.get("level"), "dir", dl.get("direction"), "eff_z", dl.get("eff_z"))
cr=j.get("credit") or {}; print("CREDIT:", cr.get("regime"), cr.get("hy_oas_bps"),"bps")
fc=j.get("financial_conditions") or {}; print("NFCI:", fc.get("nfci"), fc.get("read"), "tightening", fc.get("tightening"))
print("DONE 2608")
