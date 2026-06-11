# ops 1590 — deploy recalibrated signal-board, invoke, print compass row
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=600, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
out = {"ops": 1590}
src = open("aws/lambdas/justhodl-signal-board/source/lambda_function.py").read()
out["checkout_has_new_normalizer"] = "top_minus_bottom" in src
def zs(d):
    b=io.BytesIO()
    with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(d):
            for f in fs:
                if "__pycache__" not in r: zf.write(os.path.join(r,f),arcname=os.path.relpath(os.path.join(r,f),d))
    return b.getvalue()
for i in range(8):
    try:
        lam.update_function_code(FunctionName="justhodl-signal-board",
                                  ZipFile=zs("aws/lambdas/justhodl-signal-board/source"))
        break
    except ClientError:
        time.sleep(8)
for _ in range(40):
    c=lam.get_function_configuration(FunctionName="justhodl-signal-board")
    if c.get("LastUpdateStatus")=="Successful": break
    time.sleep(3)
r=lam.invoke(FunctionName="justhodl-signal-board",InvocationType="RequestResponse",Payload=b"{}")
out["fn_err"]=r.get("FunctionError","NONE")
time.sleep(2)
sb=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/signal-board.json")["Body"].read())
out["row"]=next(({k:e.get(k) for k in ("signal","signal_label","read")}
                 for e in sb.get("engines",[]) if e.get("engine")=="Episode Compass"),"MISSING")
out["n_engines"]=sb.get("n_engines")
open("aws/ops/reports/1590_board_cal.json","w").write(json.dumps(out,indent=2))
print(json.dumps(out))
