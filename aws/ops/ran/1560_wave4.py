# ops 1560 — Wave-4 mega-deploy: create confluence-meta + kb-matcher + index-inclusion;
# redeploy ignition v1.0.2 / liq v1.0.2 / canaries v1.0.1 / analogs v2.4; invoke+verify ALL;
# manifest overrides; transcript-store probe (item 11 dependency)
import json, os, time, zipfile, io, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=600, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"; ACC = "857687956942"
out = {"ops": 1560, "errors": []}

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
    for _ in range(40):
        c=lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") in ("Successful",None) and c.get("State") in ("Active",None): return
        time.sleep(3)

def ensure(fn_name, src, sched, rule, desc, timeout=300, mem=512):
    code = zs(src)
    try:
        lam.get_function(FunctionName=fn_name)
        rc(lambda: lam.update_function_code(FunctionName=fn_name, ZipFile=code)); st="updated"
    except ClientError:
        role = lam.get_function(FunctionName="justhodl-historical-analogs")["Configuration"]["Role"]
        rc(lambda: lam.create_function(FunctionName=fn_name, Runtime="python3.12", Role=role,
            Handler="lambda_function.lambda_handler", Code={"ZipFile":code}, Timeout=timeout, MemorySize=mem,
            Environment={"Variables":{"FRED_KEY":"2f057499936072679d8843d7fce99989",
                "POLYGON_KEY":"zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d","FMP_KEY":"wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"}},
            Description=desc)); st="created"
    ready(fn_name)
    arn=lam.get_function(FunctionName=fn_name)["Configuration"]["FunctionArn"]
    ev.put_rule(Name=rule, ScheduleExpression=sched, State="ENABLED")
    ev.put_targets(Rule=rule, Targets=[{"Id":"1","Arn":arn}])
    try:
        lam.add_permission(FunctionName=fn_name, StatementId=f"x{int(time.time())%100000}",
            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:us-east-1:{ACC}:rule/{rule}")
    except ClientError: pass
    return st

def inv(fn_name):
    r = rc(lambda: lam.invoke(FunctionName=fn_name, InvocationType="RequestResponse", Payload=b"{}"))
    err = r.get("FunctionError","NONE")
    pay = r["Payload"].read().decode()[:400] if err!="NONE" else None
    return err, pay

# A) create new engines
out["cm"]=ensure("justhodl-confluence-meta","aws/lambdas/justhodl-confluence-meta/source",
  "cron(50 11 * * ? *)","justhodl-confluence-meta-daily",
  "Confluence edge curves + fade index + provenance ledger (items 15/17/18-lite)",300,768)
out["kb"]=ensure("justhodl-kb-matcher","aws/lambdas/justhodl-kb-matcher/source",
  "cron(0 12 * * ? *)","justhodl-kb-matcher-daily",
  "Crisis-KB live pattern match: today's tape vs 16 codified frameworks (item 3)",300,512)
out["ii"]=ensure("justhodl-index-inclusion","aws/lambdas/justhodl-index-inclusion/source",
  "cron(0 13 ? * MON *)","justhodl-index-inclusion-weekly",
  "S&P 500 inclusion-eligibility watch list, rule-based (item 14)",600,512)

# B) redeploy patched engines
for fn,src in (("justhodl-ignition","aws/lambdas/justhodl-ignition/source"),
               ("justhodl-liquidity-inflection","aws/lambdas/justhodl-liquidity-inflection/source"),
               ("justhodl-crisis-canaries","aws/lambdas/justhodl-crisis-canaries/source"),
               ("justhodl-historical-analogs","aws/lambdas/justhodl-historical-analogs/source")):
    rc(lambda: lam.update_function_code(FunctionName=fn, ZipFile=zs(src))); ready(fn)
out["patched"]="ignition,liq,canaries,analogs"

# C) invoke + verify everything
v = {}
e,p = inv("justhodl-ignition"); v["ig_err"]=e
if p: v["ig_payload"]=p
ig=json.loads(s3.get_object(Bucket=B,Key="data/ignition.json")["Body"].read())
v["ig"]={"pillars":ig.get("pillar_availability"),"top":ig.get("top_calls"),"logged":ig.get("signals_logged")}

e,p = inv("justhodl-liquidity-inflection"); v["li_err"]=e
if p: v["li_payload"]=p
li=json.loads(s3.get_object(Bucket=B,Key="data/liquidity-inflection.json")["Body"].read())
es=li.get("event_study_after_flips") or {}
v["li"]={"flips10y":(li.get("usd") or {}).get("n_flips_10y"),"last_flip":(li.get("usd") or {}).get("last_flip"),
  "stablecoin":li.get("stablecoin"),"spx_d21":(es.get("SPX_proxy") or {}).get("d21"),
  "hyg_d63":(es.get("HYG") or {}).get("d63"),"leads":li.get("lead_estimates")}

e,p = inv("justhodl-crisis-canaries"); v["cn_err"]=e
if p: v["cn_payload"]=p
cn=json.loads(s3.get_object(Bucket=B,Key="data/crisis-canaries.json")["Body"].read())
v["cn"]={"avail":cn.get("availability"),"score":cn.get("composite_score"),"level":cn.get("level"),
  "rev":(cn.get("canaries") or {}).get("revision_nowcast")}

e,p = inv("justhodl-historical-analogs"); v["an_err"]=e
if p: v["an_payload"]=p
an=json.loads(s3.get_object(Bucket=B,Key="data/historical-analogs.json")["Body"].read())
v["an"]={"version":an.get("version"),"pool":an.get("n_historical_dates_evaluated"),
  "unprecedentedness":an.get("unprecedentedness")}

e,p = inv("justhodl-confluence-meta"); v["cm_err"]=e
if p: v["cm_payload"]=p
cm=json.loads(s3.get_object(Bucket=B,Key="data/confluence-meta.json")["Body"].read())
v["cm"]={"n_signals":cm.get("n_signals"),"days":cm.get("n_active_days"),
  "up_curve":(cm.get("confluence_curves") or {}).get("UP"),
  "fade":[f.get("engine") for f in (cm.get("fade_index") or [])],
  "fade0":(cm.get("fade_index") or [None])[0],"ledger_ok":cm.get("ledger_briefs_ok")}

e,p = inv("justhodl-kb-matcher"); v["kb_err"]=e
if p: v["kb_payload"]=p
kb=json.loads(s3.get_object(Bucket=B,Key="data/kb-match.json")["Body"].read())
v["kb"]={"method":kb.get("method"),"stats":kb.get("kb_stats"),
  "top":[(f.get("framework"),f.get("match_pct"),f.get("n_evaluable")) for f in (kb.get("top_matches") or [])[:3]],
  "state_keys":sorted((kb.get("today_state") or {}).keys())}

e,p = inv("justhodl-index-inclusion"); v["ii_err"]=e
if p: v["ii_payload"]=p
ii=json.loads(s3.get_object(Bucket=B,Key="data/index-inclusion.json")["Body"].read())
v["ii"]={"avail":ii.get("availability"),"members":ii.get("n_members"),"checked":ii.get("n_candidates_checked"),
  "eligible":ii.get("n_eligible"),"head":[(w.get("ticker"),w.get("mcap_bn"),w.get("passes_profit_rule"))
  for w in (ii.get("watch_list") or [])[:6]]}

# D) transcript store probe (item 11 dependency for bottleneck v1.1)
probe = {}
for pref in ("data/transcripts/","data/_transcripts/","data/transcript-index","data/research/transcripts"):
    r = s3.list_objects_v2(Bucket=B, Prefix=pref, MaxKeys=5)
    probe[pref] = [o["Key"] for o in r.get("Contents",[])]
v["transcript_probe"] = probe

# E) manifest
m=json.loads(s3.get_object(Bucket=B,Key="data/_freshness-manifest.json")["Body"].read())
ko=m.setdefault("key_overrides",{})
ko["data/confluence-meta.json"]=30; ko["data/kb-match.json"]=30; ko["data/index-inclusion.json"]=200
m["updated_by"]="ops-1560"
s3.put_object(Bucket=B,Key="data/_freshness-manifest.json",Body=json.dumps(m,indent=1).encode(),ContentType="application/json")

out["verify"]=v
open("aws/ops/reports/1560_wave4.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps({"new":[out["cm"],out["kb"],out["ii"]],
  "errs":{k:v[k] for k in ("ig_err","li_err","cn_err","an_err","cm_err","kb_err","ii_err")},
  "ig_pillars":v["ig"]["pillars"],"li_flips":v["li"]["flips10y"],"cn_rev":bool(v["cn"]["rev"]),
  "an_unprec":v["an"]["unprecedentedness"],"cm_fade":v["cm"]["fade"],
  "kb_top":v["kb"]["top"],"ii_eligible":v["ii"]["eligible"]},default=str)[:900])
