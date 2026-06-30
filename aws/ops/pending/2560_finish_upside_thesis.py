"""ops 2560 — wait for justhodl-upside-thesis Active, ensure bundled code + Anthropic
env + schedule, invoke, verify theses."""
import boto3, json, io, zipfile, time, glob, os
from botocore.config import Config
lam = boto3.client("lambda", "us-east-1", config=Config(read_timeout=300, retries={"max_attempts": 0}))
events = boto3.client("events", "us-east-1")
s3 = boto3.client("s3", "us-east-1")
FN = "justhodl-upside-thesis"

def wait_ready(timeout=180):
    for _ in range(timeout // 5):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("State") == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
            return True
        time.sleep(5)
    return False

wait_ready()
akey = ""
for src in ["justhodl-brain-sync", "justhodl-strategist", "justhodl-llm-health"]:
    try:
        env = lam.get_function_configuration(FunctionName=src).get("Environment", {}).get("Variables", {})
        akey = env.get("ANTHROPIC_API_KEY") or env.get("ANTHROPIC_KEY") or ""
        if akey: break
    except Exception: pass
print("anthropic key:", "yes" if akey else "NO")

buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
    z.writestr("lambda_function.py", open("aws/lambdas/justhodl-upside-thesis/source/lambda_function.py").read())
    for p in glob.glob("aws/shared/*.py"):
        z.writestr(os.path.basename(p), open(p).read())
lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
wait_ready()
ENV = {"Variables": {"TOP_AI": "14"}}
if akey: ENV["Variables"]["ANTHROPIC_API_KEY"] = akey
lam.update_function_configuration(FunctionName=FN, Timeout=600, MemorySize=512, Environment=ENV)
wait_ready()
print("code+env ensured")

RULE = "justhodl-upside-thesis-6h"
events.put_rule(Name=RULE, ScheduleExpression="rate(6 hours)", State="ENABLED")
arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN, StatementId="ut-evt", Action="lambda:InvokeFunction",
                       Principal="events.amazonaws.com", SourceArn=f"arn:aws:events:us-east-1:857687956942:rule/{RULE}")
except lam.exceptions.ResourceConflictException: pass
events.put_targets(Rule=RULE, Targets=[{"Id": "ut", "Arn": arn}])
print("scheduled")

print("invoking…")
r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
print("FunctionError:", r.get("FunctionError"), "| result:", r["Payload"].read().decode()[:160])
time.sleep(2)
out = json.loads(s3.get_object(Bucket="justhodl-dashboard-live", Key="data/upside-theses.json")["Body"].read())
print(f"\nn_candidates={out.get('n_candidates')} · n_ai={out.get('n_ai')} · regime={out.get('market_regime')}")
print("top_ranked:", out.get("top_ranked", [])[:12])
for t in out.get("top_ranked", [])[:3]:
    d = out["theses"][t]
    print(f"\n── {t} (disc {d['discovery_score']}, {d['n_engines']} eng) CANSLIM {d['canslim']['score']} SQGLP {d['sqglp']['score']} Lynch {d['lynch']['score']} ──")
    print("  why:", d["why"][:170])
    if d.get("ai"):
        print("  AI:", d["ai"].get("headline"), "|", d["ai"].get("best_framework"), "conv", d["ai"].get("conviction"))
        print("      why_boom:", str(d["ai"].get("why_boom"))[:150])
        print("      catalysts:", d["ai"].get("catalysts"))
    else: print("  AI: none")
print("\nDONE 2560")
