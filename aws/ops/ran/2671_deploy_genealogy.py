"""ops 2671 — create + deploy justhodl-signal-genealogy (new Lambda) + EventBridge schedule."""
import boto3, io, zipfile, json, time
REGION="us-east-1"; FN="justhodl-signal-genealogy"; SRC=f"aws/lambdas/{FN}/source/lambda_function.py"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
ev=boto3.client("events",region_name=REGION)

buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z: z.writestr("lambda_function.py",open(SRC,"rb").read())

try:
    lam.get_function(FunctionName=FN)
    exists = True
except lam.exceptions.ResourceNotFoundException:
    exists = False

if not exists:
    r = lam.create_function(FunctionName=FN, Runtime="python3.12", Role="arn:aws:iam::857687956942:role/lambda-execution-role",
        Handler="lambda_function.lambda_handler", Code={"ZipFile": buf.getvalue()},
        Timeout=180, MemorySize=512,
        Description="Signal Genealogy: which signal families lead vs confirm")
    print("created:", r.get("FunctionArn"))
else:
    lam.update_function_code(FunctionName=FN, ZipFile=buf.getvalue())
    print("code updated")

for _ in range(30):
    c=lam.get_function(FunctionName=FN)["Configuration"]
    if c.get("State")=="Active" and c.get("LastUpdateStatus")=="Successful": break
    time.sleep(4)

rule_name = "signal-genealogy-daily"
ev.put_rule(Name=rule_name, ScheduleExpression="cron(40 6 * * ? *)", State="ENABLED")
try:
    lam.add_permission(FunctionName=FN, StatementId="EventBridgeInvoke2", Action="lambda:InvokeFunction",
                       Principal="events.amazonaws.com", SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule_name}")
except Exception:
    pass
fn_arn = lam.get_function(FunctionName=FN)["Configuration"]["FunctionArn"]
ev.put_targets(Rule=rule_name, Targets=[{"Id":"1","Arn":fn_arn}])
print("schedule wired:", rule_name)

print("\ninvoking (full DynamoDB scan of ~40K records)...")
r=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
print("INVOKE:",r.get("StatusCode"),r.get("FunctionError"))
body = r["Payload"].read().decode()
print("BODY:", body[:400])
time.sleep(2)

j=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/signal-genealogy.json")["Body"].read())
print("\n=== LIVE OUTPUT ===")
print("version:", j.get("version"), "elapsed_s:", j.get("elapsed_s"))
w = j.get("window", {})
print(f"window: {w.get('start')} to {w.get('end')} ({w.get('n_days')} days) | qualifying: {w.get('n_signal_types_qualifying')}/{w.get('n_signal_types_total')}")
print(f"pairs tested: {j.get('n_pairs_tested')} | significant cascades: {j.get('n_significant_pairs')}")
print("\ntop 8 EARLIEST signals:")
for r2 in (j.get("earliest_signals") or [])[:8]:
    print(f"  {r2['signal_type']:35s} earliness={r2['earliness_index']:+.1f} n={r2['n_firings']} leads_spy={r2['leads_spy']}")
print("\ntop 8 MOST CONFIRMATORY signals:")
for r2 in (j.get("most_confirmatory_signals") or [])[:8]:
    print(f"  {r2['signal_type']:35s} earliness={r2['earliness_index']:+.1f} n={r2['n_firings']}")
print("\ntop 8 significant cascades:")
for p in (j.get("significant_cascades") or [])[:8]:
    print(f"  {p['leader']:28s} -> {p['follower']:28s}  lag={p['lag_days']}d corr={p['corr']} t={p['t']} n={p['n']}")
print("DONE 2671")
