"""ops 2750 — APAC FLOW RADAR Pillar 1 (Khalid: Korea/Taiwan/Japan → US).

Brand-new Lambda dir => GitHub deploy no-ops on it => this ops boto3-CREATES
the function (reusing justhodl-cryptoquant's IAM role + layers), wires an
EventBridge daily rule at 09:30 UTC (after TW ~07:00 and KR ~09:00 publish),
invokes it, and PROBES every source live. Taiwan (TWSE OpenAPI T86/BFI82U) is
the verified core; Korea (KRX) is best-effort + honestly reported; Japan is
v1.1. US catch-up bridges read the existing etf-flows feed.
Report: aws/ops/reports/2750_apac_flows.json.
"""
import os, io, json, time, zipfile
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION, BUCKET, ACCT = "us-east-1", "justhodl-dashboard-live", "857687956942"
FN = "justhodl-apac-flows"
lam = boto3.client("lambda", region_name=REGION, config=Config(read_timeout=300, retries={"max_attempts": 1}))
ev = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
R = {"ops": 2750, "ts": datetime.now(timezone.utc).isoformat()}

def zip_fn(fn):
    src = "aws/lambdas/%s/source" % fn
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(src):
            if "__pycache__" in root: continue
            for f in files:
                z.write(os.path.join(root, f), os.path.relpath(os.path.join(root, f), src))
        for f in sorted(os.listdir("aws/shared")):
            if f.endswith(".py"): z.write(os.path.join("aws/shared", f), f)
    return buf.getvalue()

def wait_ok(fn, budget=200):
    t0 = time.time()
    while time.time() - t0 < budget:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"): return
        except ClientError: pass
        time.sleep(5)

print("settling 15s…"); time.sleep(15)
print("== 1/4 create/update Lambda (reuse cryptoquant role+layers) ==")
base = lam.get_function_configuration(FunctionName="justhodl-cryptoquant")
role = base["Role"]; runtime = base["Runtime"]
layers = [l["Arn"] for l in base.get("Layers", [])] if base.get("Layers") else []
print("  role=%s runtime=%s layers=%d" % (role.split("/")[-1], runtime, len(layers)))
code = zip_fn(FN)
exists = True
try:
    lam.get_function(FunctionName=FN)
except ClientError:
    exists = False
if exists:
    wait_ok(FN)
    lam.update_function_code(FunctionName=FN, ZipFile=code); wait_ok(FN)
    lam.update_function_configuration(FunctionName=FN, Timeout=300, MemorySize=512,
                                      Handler="lambda_function.lambda_handler", Runtime=runtime)
    print("  updated existing function")
else:
    kw = dict(FunctionName=FN, Runtime=runtime, Role=role, Handler="lambda_function.lambda_handler",
              Code={"ZipFile": code}, Timeout=300, MemorySize=512, Publish=True,
              Environment={"Variables": {"TZ": "UTC"}})
    if layers: kw["Layers"] = layers
    for i in range(6):
        try:
            lam.create_function(**kw); break
        except ClientError as e:
            if "role" in str(e).lower() and i < 5: time.sleep(10); continue
            raise
    print("  created new function")
wait_ok(FN)
R["function"] = "ready"

print("== 2/4 EventBridge daily 09:30 UTC ==")
rule = "justhodl-apac-flows-daily"
ev.put_rule(Name=rule, ScheduleExpression="cron(30 9 * * ? *)", State="ENABLED",
            Description="APAC foreign-flow radar — after TW/KR market data publishes")
arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
try:
    lam.add_permission(FunctionName=FN, StatementId="apac-flows-eventbridge",
                       Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                       SourceArn="arn:aws:events:%s:%s:rule/%s" % (REGION, ACCT, rule))
except ClientError as e:
    if "ResourceConflict" not in str(e): raise
ev.put_targets(Rule=rule, Targets=[{"Id": "apac-flows", "Arn": arn}])
tg = ev.list_targets_by_rule(Rule=rule).get("Targets", [])
r = ev.describe_rule(Name=rule)
assert r["State"] == "ENABLED" and len(tg) >= 1, (r["State"], len(tg))
print("  rule ENABLED, target attached")
R["schedule"] = "cron(30 9 * * ? *)"

print("== 3/4 invoke + PROBE all sources ==")
resp = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
pay = json.loads(resp["Payload"].read() or b"{}")
print("  invoke ->", json.dumps(pay, default=str)[:300])
assert not resp.get("FunctionError"), pay
d = json.loads(s3.get_object(Bucket=BUCKET, Key="data/apac-flows.json")["Body"].read())
tw = d.get("taiwan", {})
print("  SOURCES:", json.dumps(d.get("sources")))
print("  TW: %s stocks, foreign col '%s', net NT$%sB" % (
    tw.get("n_stocks"), tw.get("foreign_col_detected"), tw.get("foreign_net_twd_bn")))
print("  TW top foreign BUY:", [(x["name"], x["foreign_net_shares"]) for x in (tw.get("top_buy") or [])[:5]])
print("  TW sector flows:", json.dumps(tw.get("sector_flows_shares") or {}))
print("  KR:", json.dumps(d.get("korea")))
print("  read:", d.get("read"))
# Taiwan is the required core
assert d["sources"].get("twse_t86") and tw.get("n_stocks", 0) > 100, "Taiwan T86 core failed"
assert tw.get("top_buy"), "no ranked flows"
R["sources"] = d["sources"]
R["taiwan"] = {"n_stocks": tw.get("n_stocks"), "foreign_col": tw.get("foreign_col_detected"),
               "foreign_net_twd_bn": tw.get("foreign_net_twd_bn"),
               "top_buy": [(x["name"], x["sector"], x["foreign_net_shares"]) for x in (tw.get("top_buy") or [])[:6]],
               "top_sell": [(x["name"], x["foreign_net_shares"]) for x in (tw.get("top_sell") or [])[:4]],
               "sector_flows": tw.get("sector_flows_shares")}
R["korea"] = d.get("korea")
R["bridges"] = [(b["name"], b.get("tw_foreign_net_shares"), b.get("us_etf_5d_flow_usd")) for b in d.get("bridges", [])]
R["us_feed_available"] = d.get("us_feed_available")

print("== 4/4 domain check ==")
import urllib.request
def pub(path):
    req = urllib.request.Request("https://justhodl.ai/" + path + "?a=%d" % int(time.time()),
                                 headers={"User-Agent": "Mozilla/5.0", "Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=25) as rr:
        return rr.read()
ok_edge = False
for a in range(5):
    time.sleep(20)
    try:
        json.loads(pub("data/apac-flows.json").decode(),
                   parse_constant=lambda x: (_ for _ in ()).throw(ValueError(x)))
        ok_edge = True; break
    except Exception as e:
        print("  edge attempt %d: %s" % (a + 1, str(e)[:60]))
assert ok_edge, "feed not strict at edge"
R["feed_edge"] = "LIVE"
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/2750_apac_flows.json", "w") as f:
    json.dump(R, f, indent=1, default=str)
print("OPS 2750 COMPLETE — Taiwan foreign-flow radar live; Korea probed")
