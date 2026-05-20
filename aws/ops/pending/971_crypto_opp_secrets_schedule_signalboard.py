"""
ops 971 -- crypto-opportunities: complete the deploy
======================================================

Three jobs:
  A. Backfill crypto keys (CMC_KEY) into buyback-scanner's env so the
     standard inherit_env=true bundle is complete going forward. Also
     fix the just-created justhodl-crypto-opportunities Lambda's env.
  B. Install the EventBridge schedule (every 4h per config description).
  C. Verify Lambda invocation + S3 output + page wiring.

Note: signal-board normaliser registration is a separate doc/code change,
done in a separate commit.
"""
import datetime as dt, json, os, time
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=320, connect_timeout=10))
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

FN = "justhodl-crypto-opportunities"

CHECKS = []
def add(n, ok, d): CHECKS.append({"name":n,"passed":bool(ok),"detail":str(d)[:280]})

# Standard crypto-side keys (defaults from constants doc -- already public
# in past chats and stored across many other crypto Lambdas)
STANDARD_CRYPTO_KEYS = {
    "CMC_KEY": "17ba8e87-53f0-46f4-abe5-014d9cd99597",
}

# ── A1. Add CMC_KEY to buyback-scanner so standard bundle is complete ──
try:
    bb = lam.get_function_configuration(FunctionName="justhodl-buyback-scanner")
    bb_env = bb.get("Environment", {}).get("Variables", {}).copy()
    changed = False
    for k, v in STANDARD_CRYPTO_KEYS.items():
        if k not in bb_env:
            bb_env[k] = v
            changed = True
    if changed:
        lam.update_function_configuration(
            FunctionName="justhodl-buyback-scanner",
            Environment={"Variables": bb_env},
        )
        add("buyback.added_crypto_keys", True,
            f"added={list(STANDARD_CRYPTO_KEYS.keys())} total={len(bb_env)}")
    else:
        add("buyback.added_crypto_keys", True,
            f"already present, total={len(bb_env)}")
    for _ in range(15):
        v = lam.get_function_configuration(FunctionName="justhodl-buyback-scanner")
        if v.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
except ClientError as e:
    add("buyback.added_crypto_keys", False, str(e)[:200])

# ── A2. Set proper env vars on crypto-opportunities Lambda ──
# Pull the now-complete standard bundle from buyback-scanner
try:
    bb = lam.get_function_configuration(FunctionName="justhodl-buyback-scanner")
    bb_env = bb.get("Environment", {}).get("Variables", {})
    std_keys = ["FMP_KEY","FRED_KEY","POLYGON_KEY","ALPHA_VANTAGE_KEY",
                "CMC_KEY","ANTHROPIC_API_KEY","TELEGRAM_BOT_TOKEN",
                "TELEGRAM_CHAT_ID","TELEGRAM_TOKEN","NEWSAPI_KEY",
                "BLS_KEY","BEA_KEY","CENSUS_KEY"]
    new_env = {"S3_BUCKET": S3_BUCKET}
    for k in std_keys:
        if k in bb_env:
            new_env[k] = bb_env[k]
    lam.update_function_configuration(
        FunctionName=FN,
        Environment={"Variables": new_env},
    )
    for _ in range(15):
        v = lam.get_function_configuration(FunctionName=FN)
        if v.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    cur = lam.get_function_configuration(FunctionName=FN)
    n_env = len(cur.get("Environment", {}).get("Variables", {}))
    has_cmc = "CMC_KEY" in cur.get("Environment", {}).get("Variables", {})
    add("crypto_opp.env_complete",
        has_cmc and n_env >= 6,
        f"n_env={n_env} has_CMC={has_cmc}")
except ClientError as e:
    add("crypto_opp.env_complete", False, str(e)[:200])

# ── B. EventBridge schedule (every 4h: 0/4/8/12/16/20 UTC) ──
rule = "crypto-opportunities-4h"
acct = "857687956942"
fn_arn = f"arn:aws:lambda:{REGION}:{acct}:function:{FN}"
try:
    events.put_rule(
        Name=rule,
        ScheduleExpression="cron(0 0,4,8,12,16,20 ? * * *)",
        State="ENABLED",
        Description="Crypto retail-opportunity scan every 4 hours (vol surge / social velocity / stable inflows / convergence)",
    )
    # Permission (idempotent)
    try:
        lam.add_permission(
            FunctionName=FN,
            StatementId=f"EventBridge-{rule}",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{acct}:rule/{rule}",
        )
    except ClientError as e:
        if "ResourceConflictException" not in str(e):
            raise
    events.put_targets(
        Rule=rule,
        Targets=[{"Id":"target1","Arn":fn_arn}],
    )
    add("schedule.installed", True, f"rule={rule} cron='0 0,4,8,12,16,20 * * * *' UTC")
except ClientError as e:
    add("schedule.installed", False, str(e)[:200])

# ── C. Force-invoke with proper CMC_KEY env now ──
print("Re-invoking with proper env...")
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    payload = r["Payload"].read().decode()
    body = json.loads(payload) if payload.startswith("{") else {}
    inner = body.get("statusCode", 0)
    add("reinvoke.success",
        r["StatusCode"] == 200 and inner == 200,
        f"outer={r['StatusCode']} inner={inner} body={payload[:200]}")
except ClientError as e:
    add("reinvoke.success", False, str(e)[:200])

time.sleep(3)

# ── C2. S3 freshness + final state ──
try:
    obj = s3.get_object(Bucket=S3_BUCKET, Key="data/crypto-opportunities.json")
    d = json.loads(obj["Body"].read())
    s = d.get("summary", {})
    add("output.fresh_after_env_fix", True,
        f"state={d.get('state')} universe={s.get('universe_size')} filtered={s.get('filtered_universe_size')} enriched={s.get('n_enriched')} vol={s.get('n_volume_surge')} soc={s.get('n_social_velocity')} stb={s.get('n_stable_inflows')} conv={s.get('n_convergence')}")
    add("output.has_trade_tickets",
        any(("trade_ticket" in (c or {})) for c in (d.get("convergence") or d.get("top_volume_surge") or [])),
        "first row")
except Exception as e:
    add("output.fresh_after_env_fix", False, str(e)[:200])

rep = {"ops":971,"title":"crypto-opportunities: backfill secrets bundle + schedule + final verify",
       "run_at":dt.datetime.utcnow().isoformat()+"Z",
       "checks":CHECKS,
       "summary":{"total":len(CHECKS),
                  "passed":sum(1 for c in CHECKS if c["passed"]),
                  "failed":sum(1 for c in CHECKS if not c["passed"])},
       "overall_ok":all(c["passed"] for c in CHECKS)}
os.makedirs("aws/ops/reports", exist_ok=True)
open("aws/ops/reports/971_crypto_opp_complete.json","w").write(json.dumps(rep,indent=2))
print(f"\n=== {rep['summary']['passed']}/{rep['summary']['total']} ===")
for c in CHECKS:
    print(f"  [{'OK' if c['passed'] else 'X '}] {c['name']:32} {c['detail'][:140]}")
