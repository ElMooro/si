"""
ops 969 -- verify justhodl-crypto-opportunities deploy + page + S3 output
==========================================================================

Checks:
  1. Lambda deployed (justhodl-crypto-opportunities)
  2. Lambda has FMP/CMC env vars set (inherit_env:true working)
  3. Lambda invokes successfully (force-run)
  4. S3 output present + schema valid (state, signals, convergence)
  5. crypto-opportunities.html page live at justhodl.ai + wired to data
  6. Schedule configured
"""
import boto3, datetime as dt, json, os, time, urllib.request
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
PAGES = "https://justhodl.ai"

lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=300, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name=REGION)
events = boto3.client("events", region_name=REGION)
scheduler = boto3.client("scheduler", region_name=REGION)

FN = "justhodl-crypto-opportunities"
S3_KEY = "data/crypto-opportunities.json"
PAGE = "crypto-opportunities.html"

CHECKS = []
def add(n, ok, d): CHECKS.append({"name": n, "passed": ok, "detail": str(d)[:280]})

# 1. Lambda deployed
try:
    info = lam.get_function(FunctionName=FN)
    cfg = info["Configuration"]
    add("lambda.deployed", True,
        f"runtime={cfg.get('Runtime')} mem={cfg.get('MemorySize')} "
        f"timeout={cfg.get('Timeout')} mod={cfg.get('LastModified','')[:19]}")
    env_vars = cfg.get("Environment", {}).get("Variables", {})
    add("lambda.has_cmc_key", "CMC_KEY" in env_vars and len(env_vars.get("CMC_KEY","")) > 10,
        f"n_env={len(env_vars)} CMC_KEY_set={bool(env_vars.get('CMC_KEY'))}")
    add("lambda.has_s3_bucket", env_vars.get("S3_BUCKET") == S3_BUCKET,
        f"S3_BUCKET={env_vars.get('S3_BUCKET')}")
except ClientError as e:
    add("lambda.deployed", False, str(e)[:200])

# 2. Invoke (force-run to populate fresh S3 output)
print("Invoking Lambda (may take 90-180s)...")
t0 = time.time()
try:
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                   LogType="Tail", Payload=b"{}")
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
        f"dur={dur}s outer={r['StatusCode']} inner={inner} "
        f"state={body_data.get('state')} n_conv={body_data.get('n_convergence')} "
        f"n_vol={body_data.get('n_volume_surge')} n_soc={body_data.get('n_social_velocity')} "
        f"n_stab={body_data.get('n_stable_inflows')}")
except ClientError as e:
    add("lambda.invoke", False, str(e)[:200])

time.sleep(3)

# 3. S3 output
try:
    obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
    d = json.loads(obj["Body"].read())
    add("s3.output_present", True, f"size={obj['ContentLength']}B")
    # Schema checks
    add("s3.has_engine", d.get("engine","").startswith("justhodl-crypto"),
        d.get("engine"))
    state = d.get("state")
    add("s3.has_state", state in ("OPPORTUNITY_RICH","ACTIVE","NORMAL","QUIET"),
        f"state={state}")
    add("s3.has_signal_strength",
        isinstance(d.get("signal_strength"), (int,float)),
        f"signal_strength={d.get('signal_strength')}")
    summary = d.get("summary") or {}
    add("s3.has_summary",
        all(k in summary for k in ["n_volume_surge","n_social_velocity",
                                    "n_stable_inflows","n_convergence"]),
        f"summary_keys={list(summary.keys())[:8]}")
    add("s3.has_4_tables",
        all(k in d for k in ["top_volume_surge","top_social_velocity",
                              "top_stable_inflows","convergence"]),
        f"vol={len(d.get('top_volume_surge',[]))} soc={len(d.get('top_social_velocity',[]))} "
        f"stab={len(d.get('top_stable_inflows',[]))} conv={len(d.get('convergence',[]))}")
    add("s3.has_forward_expectations",
        "forward_expectations" in d,
        f"keys={list((d.get('forward_expectations') or {}).keys())[:4]}")
    add("s3.has_methodology", bool(d.get("methodology")), "")
    add("s3.has_why_explainer",
        bool(d.get("why_now_explainer")) and len(d.get("why_now_explainer","")) > 100,
        f"len={len(d.get('why_now_explainer',''))}")
except ClientError as e:
    add("s3.output_present", False, str(e)[:200])

# 4. Page live + wired
try:
    req = urllib.request.Request(f"{PAGES}/{PAGE}",
                                  headers={"User-Agent": "ops/969 (verify)"})
    with urllib.request.urlopen(req, timeout=20) as r:
        body = r.read().decode("utf-8","ignore")
    add("page.reachable", r.status == 200 and len(body) > 5000,
        f"status={r.status} size={len(body)}")
    add("page.wired_to_s3", "crypto-opportunities.json" in body,
        f"json_ref_present={'crypto-opportunities.json' in body}")
    # Look for the 4-table structure
    markers = ["convergence", "volume", "social", "stable"]
    found = sum(1 for m in markers if m.lower() in body.lower())
    add("page.has_4_section_structure", found >= 3,
        f"markers_found={found}/{len(markers)}")
except Exception as e:
    add("page.reachable", False, str(e)[:200])

# 5. Schedule
sched_found = False
try:
    paginator = events.get_paginator("list_rules")
    for page in paginator.paginate():
        for rule in page.get("Rules", []):
            if "crypto-opportunit" in rule.get("Name","").lower():
                add("schedule.eventbridge_rule",
                    rule.get("State") == "ENABLED",
                    f"name={rule['Name']} state={rule['State']} expr={rule.get('ScheduleExpression')}")
                sched_found = True
                break
        if sched_found: break
except ClientError as e:
    add("schedule.eventbridge_rule", False, str(e)[:200])

if not sched_found:
    try:
        paginator = scheduler.get_paginator("list_schedules")
        for page in paginator.paginate():
            for s in page.get("Schedules", []):
                if "crypto-opportunit" in s.get("Name","").lower():
                    add("schedule.scheduler_schedule", True,
                        f"name={s['Name']} state={s.get('State')}")
                    sched_found = True
                    break
            if sched_found: break
    except ClientError:
        pass

if not sched_found:
    add("schedule.no_schedule_found", False,
        "no EventBridge rule or Scheduler schedule found -- engine will not auto-run")

# Report
rep = {
    "ops": 969,
    "title": "verify justhodl-crypto-opportunities full stack (Lambda + invoke + S3 schema + page + schedule)",
    "run_at": dt.datetime.utcnow().isoformat() + "Z",
    "checks": CHECKS,
    "summary": {"total": len(CHECKS),
                "passed": sum(1 for c in CHECKS if c["passed"]),
                "failed": sum(1 for c in CHECKS if not c["passed"])},
    "overall_ok": all(c["passed"] for c in CHECKS),
}
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/969_crypto_opportunities_verify.json","w") as f:
    json.dump(rep, f, indent=2)

p = rep["summary"]["passed"]; t = rep["summary"]["total"]
print(f"\n=== {p}/{t} ({100*p//max(t,1)}%) ===")
for c in CHECKS:
    flag = "OK " if c["passed"] else "FAIL"
    print(f"  [{flag}] {c['name']:38} {c['detail'][:120]}")
