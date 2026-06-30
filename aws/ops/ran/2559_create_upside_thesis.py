"""ops 2559 — create justhodl-upside-thesis (bundle aws/shared, inherit Anthropic key),
schedule, invoke, and verify the theses output."""
import boto3, json, io, zipfile, time, glob, os
from botocore.config import Config
lam = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
events = boto3.client("events", "us-east-1")
s3 = boto3.client("s3", "us-east-1")
FN = "justhodl-upside-thesis"

# Anthropic key from an existing AI engine's live env
akey = ""
for src in ["justhodl-brain-sync", "justhodl-strategist", "justhodl-ai-brief", "justhodl-llm-health"]:
    try:
        env = lam.get_function_configuration(FunctionName=src).get("Environment", {}).get("Variables", {})
        akey = env.get("ANTHROPIC_API_KEY") or env.get("ANTHROPIC_KEY") or ""
        if akey:
            print(f"got Anthropic key from {src} ({akey[:10]}…)"); break
    except Exception as e:
        print(f"  {src}: {str(e)[:40]}")
if not akey:
    print("WARN: no Anthropic key found — engine will emit deterministic theses only")

# build zip: engine + all shared modules
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open("aws/lambdas/justhodl-upside-thesis/source/lambda_function.py").read())
    for p in glob.glob("aws/shared/*.py"):
        z.writestr(os.path.basename(p), open(p).read())
code = buf.getvalue()
print("zip bytes:", len(code))

ENV = {"Variables": {"TOP_AI": "14"}}
if akey: ENV["Variables"]["ANTHROPIC_API_KEY"] = akey
try:
    lam.get_function(FunctionName=FN)
    lam.update_function_code(FunctionName=FN, ZipFile=code)
    time.sleep(6)
    lam.update_function_configuration(FunctionName=FN, Timeout=600, MemorySize=512, Environment=ENV)
    print("updated existing")
except lam.exceptions.ResourceNotFoundException:
    lam.create_function(FunctionName=FN, Runtime="python3.12",
        Role="arn:aws:iam::857687956942:role/lambda-execution-role",
        Handler="lambda_function.lambda_handler", Code={"ZipFile": code},
        Timeout=600, MemorySize=512, Environment=ENV,
        Description="Per-ticker multibagger thesis (CAN SLIM/SQGLP/Lynch + AI)")
    print("created")
for _ in range(30):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus") == "Successful": break
    time.sleep(5)

RULE = "justhodl-upside-thesis-6h"
events.put_rule(Name=RULE, ScheduleExpression="rate(6 hours)", State="ENABLED")
arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN, StatementId="ut-evt", Action="lambda:InvokeFunction",
                       Principal="events.amazonaws.com", SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{RULE}")
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=RULE, Targets=[{"Id": "ut", "Arn": arn}])
print("scheduled rate(6h)")

print("\ninvoking (may take ~1-2 min for AI)…")
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("FunctionError:", r.get("FunctionError"))
print("result:", r["Payload"].read().decode()[:200])
time.sleep(2)
try:
    out = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/upside-theses.json")["Body"].read())
    print(f"\nn_candidates={out.get('n_candidates')} · n_ai={out.get('n_ai')} · regime={out.get('market_regime')}")
    print("top_ranked:", out.get("top_ranked", [])[:12])
    for t in out.get("top_ranked", [])[:3]:
        d = out["theses"][t]
        print(f"\n── {t} (disc {d['discovery_score']}, {d['n_engines']} engines) ──")
        print("  CAN SLIM:", d["canslim"]["score"], d["canslim"]["checks"])
        print("  SQGLP:", d["sqglp"]["score"], "· Lynch:", d["lynch"]["score"])
        print("  why:", d["why"][:160])
        if d.get("ai"):
            print("  AI headline:", d["ai"].get("headline"))
            print("  AI why_boom:", str(d["ai"].get("why_boom"))[:160])
            print("  AI best_framework:", d["ai"].get("best_framework"), "· conviction", d["ai"].get("conviction"))
        else:
            print("  AI: (none — deterministic only)")
except Exception as e:
    print("read err:", str(e)[:100])
print("\nDONE 2559")
