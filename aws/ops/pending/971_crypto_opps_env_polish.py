"""ops 971: patch justhodl-crypto-opportunities env vars from donor + ensure 4h schedule.

Prior deploy used inline defaults (functional but not env-set). This op:
1. Reads donor Lambda (buyback-scanner) env vars
2. Patches crypto-opportunities to have the full secrets bundle
3. Confirms EventBridge 4h schedule exists; creates if missing
4. Re-invokes to confirm everything works post-patch
5. Final unified verification
"""
import boto3, datetime as dt, json, os, time, urllib.request
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
PAGES = "https://justhodl.ai"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, connect_timeout=10))
s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)

FN = "justhodl-crypto-opportunities"
DONOR = "justhodl-buyback-scanner"
STANDARD_KEYS = [
    "FMP_KEY","FRED_KEY","POLYGON_KEY","ALPHA_VANTAGE_KEY","CMC_KEY",
    "ANTHROPIC_API_KEY","TELEGRAM_BOT_TOKEN","TELEGRAM_CHAT_ID",
    "TELEGRAM_TOKEN","NEWSAPI_KEY","BLS_KEY","BEA_KEY","CENSUS_KEY",
]
RULE_NAME = "justhodl-crypto-opportunities-4h"
CRON = "cron(5 0,4,8,12,16,20 * * ? *)"

CHECKS=[]
def add(n, ok, d): CHECKS.append({"name":n,"passed":ok,"detail":str(d)[:280]})

# 1. Donor env
try:
    donor_env = lam.get_function_configuration(FunctionName=DONOR)["Environment"]["Variables"]
    add("donor.env_loaded", True, f"n_keys={len(donor_env)} sample={sorted(donor_env.keys())[:6]}")
except ClientError as e:
    add("donor.env_loaded", False, str(e)[:200])
    donor_env = {}

# 2. Current Lambda env
try:
    current_cfg = lam.get_function_configuration(FunctionName=FN)
    current_env = current_cfg.get("Environment", {}).get("Variables", {})
    add("target.current_env", True,
        f"n_keys={len(current_env)} keys={sorted(current_env.keys())[:6]}")
except ClientError as e:
    add("target.current_env", False, str(e)[:200])
    current_env = {}

# 3. Merge: keep existing + add standard keys from donor
new_env = dict(current_env)
new_env.setdefault("S3_BUCKET", S3_BUCKET)
keys_added = []
for k in STANDARD_KEYS:
    if k not in new_env and k in donor_env and donor_env[k]:
        new_env[k] = donor_env[k]
        keys_added.append(k)
add("env.merged", True, f"n_after_merge={len(new_env)} added={keys_added}")

# 4. Patch
try:
    lam.update_function_configuration(
        FunctionName=FN,
        Environment={"Variables": new_env},
    )
    for _ in range(15):
        v = lam.get_function_configuration(FunctionName=FN)
        if v.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    add("env.patched", True, f"n_vars={len(new_env)} added={len(keys_added)}")
except ClientError as e:
    add("env.patched", False, str(e)[:200])

# Verify env is now complete
try:
    final_env = lam.get_function_configuration(FunctionName=FN)["Environment"]["Variables"]
    missing_critical = [k for k in ["CMC_KEY","TELEGRAM_TOKEN","S3_BUCKET"]
                        if k not in final_env or not final_env.get(k)]
    add("env.critical_keys_set", len(missing_critical) == 0,
        f"missing={missing_critical} n_total={len(final_env)}")
except ClientError as e:
    add("env.critical_keys_set", False, str(e)[:200])

# 5. Confirm/create EventBridge schedule
try:
    rule = events.describe_rule(Name=RULE_NAME)
    add("schedule.eventbridge_exists",
        rule.get("State") == "ENABLED",
        f"state={rule.get('State')} expr={rule.get('ScheduleExpression')}")
except ClientError as e:
    if "ResourceNotFoundException" in str(e):
        # Create it
        try:
            events.put_rule(
                Name=RULE_NAME,
                ScheduleExpression=CRON,
                State="ENABLED",
                Description="justhodl-crypto-opportunities every 4h"
            )
            try:
                lam.add_permission(
                    FunctionName=FN,
                    StatementId=f"EventBridge-{RULE_NAME}",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{RULE_NAME}",
                )
            except ClientError:
                pass
            fn_arn = f"arn:aws:lambda:{REGION}:857687956942:function:{FN}"
            events.put_targets(Rule=RULE_NAME, Targets=[{"Id":"target1","Arn":fn_arn}])
            add("schedule.eventbridge_created", True, f"rule={RULE_NAME} cron={CRON}")
        except ClientError as e2:
            add("schedule.eventbridge_created", False, str(e2)[:200])
    else:
        add("schedule.eventbridge_exists", False, str(e)[:200])

# Also verify target is wired to crypto-opportunities Lambda
try:
    tgts = events.list_targets_by_rule(Rule=RULE_NAME)
    has_target = any(FN in t.get("Arn","") for t in tgts.get("Targets",[]))
    add("schedule.target_wired", has_target,
        f"targets={[t['Arn'].split(':')[-1] for t in tgts.get('Targets',[])]}")
except ClientError as e:
    add("schedule.target_wired", False, str(e)[:200])

# 6. Re-invoke to confirm env+code work together
print("Re-invoking with new env (60-180s)...")
t0 = time.time()
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    dur = round(time.time()-t0, 1)
    payload = r["Payload"].read().decode()
    body = json.loads(payload) if payload else {}
    inner = body.get("statusCode", 200)
    body_data = json.loads(body.get("body","{}")) if isinstance(body.get("body"),str) else {}
    add("invoke.post_env_patch",
        r["StatusCode"]==200 and inner==200 and not r.get("FunctionError"),
        f"dur={dur}s state={body_data.get('state')} "
        f"n_conv={body_data.get('n_convergence')} "
        f"n_vol={body_data.get('n_volume_surge')} "
        f"n_soc={body_data.get('n_social_velocity')} "
        f"n_stab={body_data.get('n_stable_inflows')}")
except ClientError as e:
    add("invoke.post_env_patch", False, str(e)[:200])

time.sleep(3)

# 7. S3 output sanity
try:
    obj = s3.get_object(Bucket=S3_BUCKET, Key="data/crypto-opportunities.json")
    d = json.loads(obj["Body"].read())
    add("s3.output_complete",
        obj["ContentLength"] > 1000 and d.get("state"),
        f"size={obj['ContentLength']}B state={d.get('state')} "
        f"signal_strength={d.get('signal_strength')}")
except ClientError as e:
    add("s3.output_complete", False, str(e)[:200])

# 8. Page wired
try:
    req = urllib.request.Request(f"{PAGES}/crypto-opportunities.html",
                                  headers={"User-Agent":"ops/971"})
    with urllib.request.urlopen(req, timeout=15) as r:
        body = r.read().decode("utf-8","ignore")
    markers = ["crypto-opportunities.json","convergence","volume","social","stable"]
    found = sum(1 for m in markers if m.lower() in body.lower())
    add("page.live_with_4_tables",
        r.status == 200 and found >= 4,
        f"status={r.status} size={len(body)} markers={found}/{len(markers)}")
except Exception as e:
    add("page.live_with_4_tables", False, str(e)[:200])

rep = {
    "ops": 971,
    "title": "crypto-opportunities env polish + schedule confirm + final verify",
    "run_at": dt.datetime.utcnow().isoformat()+"Z",
    "checks": CHECKS,
    "summary": {"total":len(CHECKS),
                "passed":sum(1 for c in CHECKS if c["passed"]),
                "failed":sum(1 for c in CHECKS if not c["passed"])},
    "overall_ok": all(c["passed"] for c in CHECKS),
}
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/971_crypto_opps_env_polish.json","w") as f:
    json.dump(rep, f, indent=2)
p,t = rep["summary"]["passed"], rep["summary"]["total"]
print(f"\n=== {p}/{t} ({100*p//max(t,1)}%) ===")
for c in CHECKS:
    print(f"  [{'OK' if c['passed'] else 'X '}] {c['name']:35} {c['detail'][:160]}")
