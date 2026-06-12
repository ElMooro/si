# ops 1586 — create+verify regime-engine; board +1; registry; manifest
import json, os, time, zipfile, io, boto3
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=900, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"; ACC = "857687956942"
out = {"ops": 1608, "errors": []}

def rc(fn, tries=10, wait=8):
    for i in range(tries):
        try: return fn()
        except ClientError as e:
            if e.response["Error"]["Code"] in ("ResourceConflictException","TooManyRequestsException"):
                time.sleep(wait); continue
            raise
    raise RuntimeError("retries")

def zs(src):
    b=io.BytesIO()
    with zipfile.ZipFile(b,"w",zipfile.ZIP_DEFLATED) as zf:
        for r,_,fs in os.walk(src):
            for f in fs:
                if "__pycache__" not in r: zf.write(os.path.join(r,f),arcname=os.path.relpath(os.path.join(r,f),src))
    return b.getvalue()

def ready(fn):
    for _ in range(60):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in ("Successful",None) and c.get("State") in ("Active",None): return
        time.sleep(3)

code = zs("aws/lambdas/justhodl-market-map/source")
try:
    lam.get_function(FunctionName="justhodl-market-map")
    rc(lambda: lam.update_function_code(FunctionName="justhodl-market-map", ZipFile=code)); out["fn"]="updated"
except ClientError:
    role = lam.get_function(FunctionName="justhodl-historical-analogs")["Configuration"]["Role"]
    rc(lambda: lam.create_function(FunctionName="justhodl-market-map", Runtime="python3.12", Role=role,
        Handler="lambda_function.lambda_handler", Code={"ZipFile":code}, Timeout=900, MemorySize=1024,
        Environment={"Variables":{"FRED_KEY":"2f057499936072679d8843d7fce99989",
            "POLYGON_KEY":"zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d","FMP_KEY":"wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}},
        Description="Macro regime conductor: GxI quadrants since 1960s, measured playbooks, transition matrix")); out["fn"]="created"
ready("justhodl-market-map")
arn=lam.get_function(FunctionName="justhodl-market-map")["Configuration"]["FunctionArn"]
ev.put_rule(Name="justhodl-market-map-daily", ScheduleExpression="cron(25 14 * * ? *)", State="ENABLED")
ev.put_targets(Rule="justhodl-market-map-daily", Targets=[{"Id":"1","Arn":arn}])
try:
    lam.add_permission(FunctionName="justhodl-market-map", StatementId=f"mm{int(time.time())%99999}",
        Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
        SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/justhodl-market-map-daily")
except ClientError: pass

r = rc(lambda: lam.invoke(FunctionName="justhodl-market-map", InvocationType="RequestResponse", Payload=b"{}"))
out["fn_err"]=r.get("FunctionError","NONE")
if out["fn_err"]!="NONE": out["payload"]=r["Payload"].read().decode()[:600]
time.sleep(2)
d=json.loads(s3.get_object(Bucket=B,Key="data/market-map.json")["Body"].read())
out["verify"]={"diagnostics":d.get("diagnostics"),
  "n_tiles":d.get("n_tiles"),"size_mode":d.get("size_mode"),
  "as_of":d.get("as_of_session"),"breadth":d.get("breadth"),
  "tiles_top":sorted((d.get("tiles") or []),key=lambda x:-(x.get("mc") or 0))[:6],
  "sector_agg":d.get("sector_agg")}
g2=json.loads(s3.get_object(Bucket=B,Key="data/sector-groups.json")["Body"].read())
out["verify"]["groups_head"]=(g2.get("groups") or [])[:6]
out["verify"]["leadership"]=g2.get("leadership")
out["verify"]["groups_breadth_join"]=[{"etf":g.get("etf"),"sector":g.get("sector"),
  "breadth":g.get("breadth"),"c":(g.get("perf") or {}).get("c")} for g in (g2.get("groups") or [])[:11]]
out["verify"]["goog_present"]=any(t.get("t")=="GOOG" for t in (d.get("tiles") or []))
out["verify"]["sector_names"]=sorted((d.get("sector_agg") or {}).keys())
out["verify"]["tile_sample_new"]=[{k:t.get(k) for k in ("t","c","m3","hi","rs")}
  for t in sorted((d.get("tiles") or []),key=lambda x:-(x.get("mc") or 0))[:4]]
out["verify"]["industries_head"]=(g2.get("industries") or [])[:8]
g3=json.loads(s3.get_object(Bucket=B,Key="data/themes.json")["Body"].read())
out["verify"]["themes_head"]=[{k:t.get(k) for k in ("theme","n","perf","rs_3m_vs_spy","breadth_1d","leader_1m","laggard_1m")}
  for t in (g3.get("themes") or [])[:8]]
# ── redeploy + refresh rotation-radar (SMH/SPY ratio) ──
import zipfile, io as _io, time as _time, os as _os
def _ready(fn):
    for _ in range(40):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus")!="InProgress" and c.get("State")!="Pending":
            return
        _time.sleep(3)
buf=_io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    for root,_,fs in _os.walk("aws/lambdas/justhodl-rotation-radar/source"):
        for f in fs:
            fp=_os.path.join(root,f)
            z.write(fp,_os.path.relpath(fp,"aws/lambdas/justhodl-rotation-radar/source"))
for _try in range(6):
    try:
        lam.update_function_code(FunctionName="justhodl-rotation-radar",ZipFile=buf.getvalue()); break
    except Exception as _e:
        if "ResourceConflict" in str(_e): _time.sleep(8)
        else: raise
_ready("justhodl-rotation-radar")
lam.invoke(FunctionName="justhodl-rotation-radar",InvocationType="Event",Payload=b"{}")
rot=None
for _ in range(36):
    _time.sleep(8)
    try:
        rj=json.loads(s3.get_object(Bucket=B,Key="data/rotation-radar.json")["Body"].read())
        if rj.get("version")=="1.2.0":
            rot=rj; break
    except Exception: pass
if rot:
    rr=(rot.get("ratios") or {})
    sm=rr.get("semis_mkt") or {}
    out["verify"]["rotation"]={"version":rot.get("version"),"ratio_keys":list(rr.keys()),
      "semis_mkt":{"n_events":len(sm.get("events") or []),
                    "events_tail":(sm.get("events") or [])[-3:],
                    "live":sm.get("live")},
      "diag_tail":(rot.get("diagnostics") or [])[-3:]}
else:
    out["verify"]["rotation"]="REFRESH PENDING (async invoke fired)"

rc(lambda: lam.update_function_code(FunctionName="justhodl-signal-board", ZipFile=zs("aws/lambdas/justhodl-signal-board/source")))
ready("justhodl-signal-board")
reg=json.load(open("config/ai-brief-contexts.json"))
s3.put_object(Bucket=B,Key="config/ai-brief-contexts.json",Body=json.dumps(reg,indent=1).encode(),
              ContentType="application/json",CacheControl="no-cache")
r2 = rc(lambda: lam.invoke(FunctionName="justhodl-signal-board", InvocationType="RequestResponse", Payload=b"{}"))
out["sb_err"]=r2.get("FunctionError","NONE")
sb=json.loads(s3.get_object(Bucket=B,Key="data/signal-board.json")["Body"].read())
out["board_row_groups"]=next(({k:e2.get(k) for k in ("signal","signal_label","read")}
          for e2 in (sb.get("engines") or []) if e2.get("engine")=="Sector Groups"), "MISSING")
out["board_row"]=next(({k:e2.get(k) for k in ("signal","signal_label","read")}
          for e2 in (sb.get("engines") or []) if e2.get("engine")=="Market Map (S&P)"), "MISSING")
out["board_n"]=sb.get("n_engines"); out["registry_n"]=len(reg["contexts"])
m=json.loads(s3.get_object(Bucket=B,Key="data/_freshness-manifest.json")["Body"].read())
m.setdefault("key_overrides",{})["data/market-map.json"]=30; m["updated_by"]="ops-1586"
s3.put_object(Bucket=B,Key="data/_freshness-manifest.json",Body=json.dumps(m,indent=1).encode(),ContentType="application/json")
open("aws/ops/reports/1608_finviz_breadth.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps({"fn":out["fn"],"err":out["fn_err"],"current":(out["verify"]["current"] or {}).get("quadrant"),
  "n_months":out["verify"]["n_months"],"board":out["board_n"]},default=str)[:400])
