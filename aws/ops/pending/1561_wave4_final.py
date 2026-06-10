# ops 1561 — deploy kb v1.0.1 / confluence v1.1 / inclusion v1.0.1; invoke+verify;
# FINAL ACCEPTANCE: freshness of all 7 new briefs + 7 rules enabled + signal-type census
import json, os, time, zipfile, io, boto3
from datetime import datetime, timezone
from botocore.config import Config
from botocore.exceptions import ClientError
cfg = Config(read_timeout=600, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
ev = boto3.client("events", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ddb = boto3.client("dynamodb", region_name="us-east-1", config=cfg)
B = "justhodl-dashboard-live"
out = {"ops": 1561, "errors": []}

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

def inv(fn):
    r = rc(lambda: lam.invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}"))
    e = r.get("FunctionError","NONE")
    return e, (r["Payload"].read().decode()[:400] if e!="NONE" else None)

for fn,src in (("justhodl-kb-matcher","aws/lambdas/justhodl-kb-matcher/source"),
               ("justhodl-confluence-meta","aws/lambdas/justhodl-confluence-meta/source"),
               ("justhodl-index-inclusion","aws/lambdas/justhodl-index-inclusion/source")):
    rc(lambda: lam.update_function_code(FunctionName=fn, ZipFile=zs(src))); ready(fn)

v = {}
e,p = inv("justhodl-kb-matcher"); v["kb_err"]=e
if p: v["kb_payload"]=p
kb=json.loads(s3.get_object(Bucket=B,Key="data/kb-match.json")["Body"].read())
v["kb"]={"schema_map":kb.get("schema_map"),"stats":kb.get("kb_stats"),"method":kb.get("method"),
  "top":[(f.get("framework"),f.get("match_pct"),f.get("n_evaluable"),f.get("n_rules")) for f in (kb.get("top_matches") or [])[:5]],
  "top1_next":((kb.get("top_matches") or [{}])[0]).get("next_triggers")}

e,p = inv("justhodl-confluence-meta"); v["cm_err"]=e
if p: v["cm_payload"]=p
cm=json.loads(s3.get_object(Bucket=B,Key="data/confluence-meta.json")["Body"].read())
v["cm"]={"version":cm.get("version"),"net_today":cm.get("net_today"),
  "curves":cm.get("confluence_curves_net"),"fade":[(f.get("engine"),f.get("hit_rate"),f.get("n_scored")) for f in (cm.get("fade_index") or [])]}

e,p = inv("justhodl-index-inclusion"); v["ii_err"]=e
if p: v["ii_payload"]=p
ii=json.loads(s3.get_object(Bucket=B,Key="data/index-inclusion.json")["Body"].read())
v["ii"]={"version":ii.get("version"),"eligible":ii.get("n_eligible"),
  "head":[(w.get("ticker"),w.get("name"),w.get("mcap_bn"),w.get("passes_profit_rule")) for w in (ii.get("watch_list") or [])[:8]]}

# FINAL ACCEPTANCE
now = datetime.now(timezone.utc)
keys = ["data/ignition.json","data/liquidity-inflection.json","data/crisis-canaries.json",
        "data/confluence-meta.json","data/kb-match.json","data/index-inclusion.json",
        "data/historical-analogs.json"]
fresh = {}
for k in keys:
    try:
        h = s3.head_object(Bucket=B, Key=k)
        fresh[k] = round((now - h["LastModified"]).total_seconds()/3600, 2)
    except Exception as ex:
        fresh[k] = f"MISSING {str(ex)[:40]}"
v["freshness_hours"] = fresh
rules = {}
for r in ("justhodl-ignition-daily","justhodl-liquidity-inflection-daily","justhodl-crisis-canaries-daily",
          "justhodl-confluence-meta-daily","justhodl-kb-matcher-daily","justhodl-index-inclusion-weekly"):
    try:
        d = ev.describe_rule(Name=r)
        t = ev.list_targets_by_rule(Rule=r).get("Targets",[])
        rules[r] = {"state": d["State"], "sched": d["ScheduleExpression"], "targets": len(t)}
    except Exception as ex:
        rules[r] = f"ERR {str(ex)[:40]}"
v["rules"] = rules
# new signal types in DDB today
d0 = now.strftime("%Y-%m-%d")
census = {}
for st_, sid in (("ignition", None), ("liquidity_inflection", None), ("crisis_canary", None)):
    r = ddb.scan(TableName="justhodl-signals",
        FilterExpression="signal_type = :t AND begins_with(logged_at, :d)",
        ExpressionAttributeValues={":t":{"S":st_},":d":{"S":d0}}, Select="COUNT")
    census[st_] = r.get("Count",0)
v["signal_census_today"] = census

out["verify"]=v
open("aws/ops/reports/1561_wave4_final.json","w").write(json.dumps(out,indent=2,default=str))
print(json.dumps({"errs":{k:v[k] for k in ("kb_err","cm_err","ii_err")},
  "kb_stats":v["kb"]["stats"],"kb_top":v["kb"]["top"][:3],
  "cm_fade":v["cm"]["fade"],"ii_head":v["ii"]["head"][:5],
  "fresh":v["freshness_hours"],"census":v["signal_census_today"]},default=str)[:900])
