"""ops 2614 — deploy liquidity-inflection v1.3.0 (dollar shortage/fails/swaps/flow-divergence/trajectory)."""
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
print("INVOKE:", r.get("StatusCode"), r.get("FunctionError"), r["Payload"].read().decode()[:150])
time.sleep(2)
j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/liquidity-inflection.json")["Body"].read())
print("version:", j.get("version"))
ds=j.get("dollar_shortage") or {}; print("DOLLAR SHORTAGE:", ds.get("status"),"| swaps $",ds.get("fed_swap_lines_bn"),"bn · CP-OIS",ds.get("cp_ois_bps"),"bp · synthUSD",ds.get("usd_synthetic_20d_pct"),"%")
print("  flags:", ds.get("flags"))
sf=j.get("settlement_fails") or {}; print("FAILS:", sf.get("regime"),"| FtD $",sf.get("ust_ftd_bn"),"+ FtR $",sf.get("ust_ftr_bn"),"= $",sf.get("ust_combined_bn"),"bn ·",sf.get("pctile"),"%ile z",sf.get("z"))
sl=j.get("swap_lines") or {}; print("SWAPS:", sl.get("status"),"| fed swaps $",sl.get("fed_swaps_bn"),"bn · discount window $",sl.get("discount_window_bn"),"bn")
fd=j.get("flow_divergence") or {}; print("FLOW DIVERGENCE:", fd.get("regime"),"|", fd.get("flows_5d_usd_bn"))
tr=j.get("trajectory") or {}; print("TRAJECTORY:", tr.get("heading"),"vote",tr.get("vote"),"n",tr.get("n_signals"))
print("  drivers:", tr.get("drivers"))
co=j.get("composite") or {}; print("COMPOSITE:", co.get("liquidity_score"), co.get("regime"),"n",co.get("n_components"),"comps:",[c['name'] for c in co.get("components",[])])
print("DONE 2614")
