# ops 1557 — create+schedule+run+verify justhodl-ignition; manifest override; DDB log check
import json, os, time, zipfile, io, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=600, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ddb = boto3.client("dynamodb", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"; ACC = "857687956942"
out = {"ops": 1557, "errors": []}

def retry_conflict(fn, tries=10, wait=8):
    for i in range(tries):
        try: return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException","TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries")

def zip_src(src):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if "__pycache__" not in r: zf.write(os.path.join(r,f),arcname=os.path.relpath(os.path.join(r,f),src))
    return buf.getvalue()

role = lam.get_function(FunctionName="justhodl-historical-analogs")["Configuration"]["Role"]
code = zip_src("aws/lambdas/justhodl-ignition/source")
try:
    lam.get_function(FunctionName="justhodl-ignition")
    retry_conflict(lambda: lam.update_function_code(FunctionName="justhodl-ignition", ZipFile=code))
    out["fn"]="updated"
except ClientError:
    retry_conflict(lambda: lam.create_function(FunctionName="justhodl-ignition", Runtime="python3.12", Role=role,
        Handler="lambda_function.lambda_handler", Code={"ZipFile": code}, Timeout=600, MemorySize=1024,
        Environment={"Variables":{"FMP_KEY":"wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb","POLYGON_KEY":"zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"}},
        Description="Ignition pre-pump composite: 8 accumulation pillars, coverage-renormalized, closed-loop logged"))
    out["fn"]="created"
for _ in range(40):
    c=lam.get_function_configuration(FunctionName="justhodl-ignition")
    if c.get("LastUpdateStatus") in ("Successful",None) and c.get("State") in ("Active",None): break
    time.sleep(3)
arn = lam.get_function(FunctionName="justhodl-ignition")["Configuration"]["FunctionArn"]
ev.put_rule(Name="justhodl-ignition-daily", ScheduleExpression="cron(30 10 * * ? *)", State="ENABLED")
ev.put_targets(Rule="justhodl-ignition-daily", Targets=[{"Id":"1","Arn":arn}])
try:
    lam.add_permission(FunctionName="justhodl-ignition", StatementId=f"ign-{int(time.time())}",
        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
        SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/justhodl-ignition-daily")
except ClientError: pass
out["rule"]="cron(30 10 * * ? *)"

r = retry_conflict(lambda: lam.invoke(FunctionName="justhodl-ignition", InvocationType="RequestResponse", Payload=b"{}"))
out["fn_err"]=r.get("FunctionError","NONE")
if out["fn_err"]!="NONE": out["payload"]=r["Payload"].read().decode()[:500]
time.sleep(2)
ig = json.loads(s3.get_object(Bucket=B, Key="data/ignition.json")["Body"].read())
out["verify"]={"scored_n":ig.get("scored_n"),"duration_s":ig.get("duration_s"),
  "pillar_availability":ig.get("pillar_availability"),"probes":ig.get("probes"),
  "ftd_files":ig.get("ftd_files"),"signals_logged":ig.get("signals_logged"),
  "top_calls":ig.get("top_calls"),"rank1":(ig.get("ranks") or [{}])[0]}
tc=(ig.get("top_calls") or [None])[0]
if tc:
    d0=datetime.now(timezone.utc).strftime("%Y-%m-%d")
    it=ddb.get_item(TableName="justhodl-signals",Key={"signal_id":{"S":f"ignition#{tc}#{d0}"}}).get("Item")
    out["ddb"]={"found":bool(it),"conf":(it or {}).get("confidence",{}).get("N"),"n_fields":len(it or {})}
# manifest override merge
m=json.loads(s3.get_object(Bucket=B,Key="data/_freshness-manifest.json")["Body"].read())
m.setdefault("key_overrides",{})["data/ignition.json"]=30
m["updated_at"]=datetime.now(timezone.utc).isoformat(); m["updated_by"]="ops-1557"
s3.put_object(Bucket=B,Key="data/_freshness-manifest.json",Body=json.dumps(m,indent=1).encode(),ContentType="application/json")
open("aws/ops/reports/1557_ignition.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps({"fn":out["fn"],"err":out["fn_err"],"avail":out["verify"]["pillar_availability"],
  "top":out["verify"]["top_calls"],"logged":out["verify"]["signals_logged"],"ddb":out.get("ddb")},default=str)[:700])
