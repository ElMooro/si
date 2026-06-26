import boto3, json, io, zipfile, time
from botocore.config import Config
lam=boto3.client("lambda","us-east-1",config=Config(read_timeout=890,retries={"max_attempts":0}))
s3=boto3.client("s3","us-east-1")
si=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/supply-inflection.json")["Body"].read())
keys=list((si.get("signals") or {}).keys())
print("signal keys in output (%d):"%len(keys), keys)
has_new = "COPPER_SPOT" in keys
print("new signals present in output:", has_new)
if not has_new:
    print("-> deploy no-op'd; force-updating code via boto3")
    SRC=open("aws/lambdas/justhodl-supply-inflection-scanner/source/lambda_function.py").read()
    # confirm repo source HAS the new signals
    print("   repo source has COPPER_SPOT:", "COPPER_SPOT" in SRC, "| PCOPPUSDM:", "PCOPPUSDM" in SRC)
    b=io.BytesIO()
    with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",SRC)
    lam.update_function_code(FunctionName="justhodl-supply-inflection-scanner",ZipFile=b.getvalue())
    for _ in range(30):
        c=lam.get_function(FunctionName="justhodl-supply-inflection-scanner")["Configuration"]
        if c.get("LastUpdateStatus") in ("Successful",None) and c.get("State")=="Active": break
        time.sleep(3)
    lam.invoke(FunctionName="justhodl-supply-inflection-scanner",InvocationType="RequestResponse")
    si=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/supply-inflection.json")["Body"].read())
    sig=si.get("signals") or {}
    print("\nAFTER force-update — new signals:")
    for n in ["COPPER_SPOT","URANIUM_SPOT","NICKEL_SPOT","IRON_ORE_SPOT","PPI_SEMIS","PPI_GRID_EQUIPMENT","DELIVERY_TIME_NY","PRICES_PAID_PHILLY"]:
        sg=sig.get(n) or {}
        m=sg.get("metrics") or {}
        print(f"  {n:<20} score={sg.get('score')} flag={sg.get('flag')} latest={m.get('latest_value')} chg90={m.get('pct_change_90d')} pctl365={m.get('percentile_365d')}")
    print("n_scored:", (si.get('summary') or {}).get('n_signals_scored'))
print("DONE 2230")
