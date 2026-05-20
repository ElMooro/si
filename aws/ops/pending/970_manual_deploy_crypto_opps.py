"""ops 970: manually create-function justhodl-crypto-opportunities via Boto3.

Mirrors the ops 963 pattern that succeeded for edges 1-3. Reads config.json,
zips source/, create_function with full env (inherit_env:true semantics
implemented inline: pull standard secrets bundle from justhodl-buyback-scanner).
Then invoke + verify S3 output. Optionally schedule via EventBridge.
"""
import boto3, datetime as dt, io, json, os, time, zipfile
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=600, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)

FN = "justhodl-crypto-opportunities"
SRC = "aws/lambdas/justhodl-crypto-opportunities/source"
CFG_PATH = "aws/lambdas/justhodl-crypto-opportunities/config.json"
DONOR = "justhodl-buyback-scanner"  # source of standard secrets bundle
STANDARD_KEYS = [
    "FMP_KEY", "FRED_KEY", "POLYGON_KEY", "ALPHA_VANTAGE_KEY", "CMC_KEY",
    "ANTHROPIC_API_KEY", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID",
    "TELEGRAM_TOKEN", "NEWSAPI_KEY", "BLS_KEY", "BEA_KEY", "CENSUS_KEY",
]

CHECKS = []
def add(n, ok, d): CHECKS.append({"name": n, "passed": ok, "detail": str(d)[:280]})

# 1. Read config
cfg = json.load(open(CFG_PATH))
add("config.loaded", True,
    f"runtime={cfg.get('runtime')} mem={cfg.get('memory')} "
    f"timeout={cfg.get('timeout')} inherit_env={cfg.get('inherit_env')}")

# 2. Build env: start with config.env, layer standard secrets bundle from donor
env_vars = dict(cfg.get("env", {}))
try:
    donor_env = lam.get_function_configuration(
        FunctionName=DONOR)["Environment"]["Variables"]
    for k in STANDARD_KEYS:
        if k in donor_env and donor_env[k]:
            env_vars[k] = donor_env[k]
    add("env.bundled_from_donor", True,
        f"n_keys={len(env_vars)} keys={sorted(env_vars.keys())[:10]}")
except ClientError as e:
    add("env.bundled_from_donor", False, str(e)[:200])

# 3. Zip source
def zip_dir(src):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(src):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root: continue
                full = os.path.join(root, f)
                arc = os.path.relpath(full, src)
                zf.write(full, arc)
    buf.seek(0); return buf.getvalue()

z = zip_dir(SRC)
add("zip.built", True, f"size={len(z)}B")

# 4. Create or update
desc = (cfg.get("description") or "")[:255]
try:
    lam.get_function(FunctionName=FN)
    # Exists -- update code
    lam.update_function_code(FunctionName=FN, ZipFile=z, Publish=False)
    for _ in range(15):
        v = lam.get_function_configuration(FunctionName=FN)
        if v.get("LastUpdateStatus") == "Successful": break
        time.sleep(2)
    lam.update_function_configuration(
        FunctionName=FN,
        Runtime=cfg.get("runtime","python3.12"),
        MemorySize=int(cfg.get("memory",512)),
        Timeout=int(cfg.get("timeout",240)),
        Handler=cfg.get("handler","lambda_function.lambda_handler"),
        Description=desc,
        Environment={"Variables": env_vars},
    )
    add("lambda.updated_existing", True, "code+config updated")
except ClientError as e:
    if "ResourceNotFoundException" in str(e):
        # Create new
        try:
            lam.create_function(
                FunctionName=FN,
                Runtime=cfg.get("runtime","python3.12"),
                Role=cfg.get("role_arn", "arn:aws:iam::857687956942:role/lambda-execution-role"),
                Handler=cfg.get("handler","lambda_function.lambda_handler"),
                Code={"ZipFile": z},
                MemorySize=int(cfg.get("memory",512)),
                Timeout=int(cfg.get("timeout",240)),
                Description=desc,
                Environment={"Variables": env_vars},
                Publish=False,
            )
            add("lambda.created", True, f"{FN} created with {len(env_vars)} env vars")
        except ClientError as e2:
            add("lambda.created", False, str(e2)[:200])
    else:
        add("lambda.lookup_error", False, str(e)[:200])

# 5. Wait for ready
for _ in range(30):
    try:
        v = lam.get_function_configuration(FunctionName=FN)
        if v.get("State") == "Active" and v.get("LastUpdateStatus") == "Successful":
            break
    except ClientError: pass
    time.sleep(2)

# 6. Invoke
print("Invoking (may take 60-180s due to CoinGecko enrichment)...")
t0 = time.time()
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    dur = round(time.time() - t0, 1)
    payload = r["Payload"].read().decode()
    try:
        body = json.loads(payload)
        inner = body.get("statusCode", 200)
        body_data = json.loads(body.get("body","{}")) if isinstance(body.get("body"),str) else body.get("body",{})
    except Exception:
        inner, body_data = "n/a", {}
    ok = r["StatusCode"] == 200 and not r.get("FunctionError") and inner == 200
    add("lambda.invoke", ok,
        f"dur={dur}s outer={r['StatusCode']} inner={inner} body={payload[:200]}")
except ClientError as e:
    add("lambda.invoke", False, str(e)[:240])

time.sleep(3)

# 7. S3 output
try:
    obj = s3.get_object(Bucket=S3_BUCKET, Key="data/crypto-opportunities.json")
    d = json.loads(obj["Body"].read())
    add("s3.output_fresh",
        obj["ContentLength"] > 500,
        f"size={obj['ContentLength']}B state={d.get('state')} "
        f"n_conv={(d.get('summary') or {}).get('n_convergence')} "
        f"n_vol={(d.get('summary') or {}).get('n_volume_surge')}")
    # Schema sanity
    schema_ok = all(k in d for k in ["state","summary","top_volume_surge",
                                       "top_social_velocity","top_stable_inflows",
                                       "convergence"])
    add("s3.schema_complete", schema_ok,
        f"keys={sorted(d.keys())[:12]}")
except ClientError as e:
    add("s3.output_fresh", False, str(e)[:200])

# 8. EventBridge schedule (4h cron: 0,4,8,12,16,20 UTC)
RULE_NAME = "justhodl-crypto-opportunities-4h"
CRON = "cron(5 0,4,8,12,16,20 * * ? *)"  # 5 min after the hour
try:
    events.put_rule(
        Name=RULE_NAME,
        ScheduleExpression=CRON,
        State="ENABLED",
        Description="justhodl-crypto-opportunities every 4h (5 min past)",
    )
    # Permission for EventBridge to invoke
    try:
        lam.add_permission(
            FunctionName=FN,
            StatementId=f"EventBridge-{RULE_NAME}",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{RULE_NAME}",
        )
    except ClientError as e:
        if "ResourceConflictException" not in str(e):
            print(f"add_permission: {e}")
    fn_arn = f"arn:aws:lambda:{REGION}:857687956942:function:{FN}"
    events.put_targets(Rule=RULE_NAME, Targets=[{"Id":"target1","Arn":fn_arn}])
    add("schedule.eventbridge_4h", True, f"rule={RULE_NAME} cron={CRON}")
except ClientError as e:
    add("schedule.eventbridge_4h", False, str(e)[:200])

rep = {
    "ops": 970,
    "title": "manual deploy justhodl-crypto-opportunities (Boto3) + invoke + S3 + EventBridge 4h schedule",
    "run_at": dt.datetime.utcnow().isoformat()+"Z",
    "checks": CHECKS,
    "summary": {"total":len(CHECKS),
                "passed":sum(1 for c in CHECKS if c["passed"]),
                "failed":sum(1 for c in CHECKS if not c["passed"])},
    "overall_ok": all(c["passed"] for c in CHECKS),
}
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/970_manual_deploy_crypto_opps.json","w") as f:
    json.dump(rep, f, indent=2)
p, t = rep["summary"]["passed"], rep["summary"]["total"]
print(f"\n=== {p}/{t} ===")
for c in CHECKS:
    print(f"  [{'OK' if c['passed'] else 'X '}] {c['name']:32} {c['detail'][:160]}")
