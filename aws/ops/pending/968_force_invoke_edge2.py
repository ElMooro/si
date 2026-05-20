"""ops 968: force-invoke Edge #2 to surface new state field."""
import datetime as dt, json, os, time
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=180, connect_timeout=10,
                                 retries={"max_attempts": 1}))
s3 = boto3.client("s3", region_name="us-east-1")
CHECKS = []
def add(n, ok, d=""): CHECKS.append({"name": n, "passed": ok, "detail": str(d)[:280]})

FN = "justhodl-insider-buys-enriched"
S3_KEY = "data/insider-buys-enriched.json"
BUCKET = "justhodl-dashboard-live"

# Wait briefly for any pending deploy to settle
for _ in range(20):
    try:
        v = lam.get_function_configuration(FunctionName=FN)
        if v.get("LastUpdateStatus") == "Successful": break
    except ClientError: pass
    time.sleep(2)

# Invoke
try:
    t0 = time.time()
    r = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
    dur = round(time.time() - t0, 1)
    payload = r["Payload"].read().decode()
    try: body = json.loads(payload); inner = body.get("statusCode", 200)
    except Exception: inner = "n/a"
    ok = r["StatusCode"] == 200 and not r.get("FunctionError") and inner == 200
    add("invoke_ok", ok, f"dur={dur}s outer={r['StatusCode']} inner={inner} body={payload[:200]}")
except ClientError as ex:
    add("invoke_ok", False, str(ex)[:200])

time.sleep(2)

# Check new S3 file has state
try:
    obj = s3.get_object(Bucket=BUCKET, Key=S3_KEY)
    d = json.loads(obj["Body"].read())
    age = (dt.datetime.now(dt.timezone.utc) - obj["LastModified"]).total_seconds()
    state = d.get("state")
    sig = d.get("signal_strength")
    add("has_state", state is not None, f"state={state}")
    add("has_signal_strength", isinstance(sig, (int, float)), f"signal_strength={sig}")
    add("fresh_post_invoke", age < 60, f"age_s={int(age)}")
    add("state_enum_valid",
        state in ("FRESH_HIGH_CONVICTION", "ELEVATED", "NORMAL", "QUIET"),
        f"state={state}")
except (ClientError, json.JSONDecodeError) as ex:
    add("check_s3", False, str(ex)[:120])

rep = {"ops": 968, "title": "force-invoke Edge #2 to surface new state field",
       "run_at": dt.datetime.utcnow().isoformat() + "Z",
       "checks": CHECKS,
       "summary": {"total": len(CHECKS), "passed": sum(1 for c in CHECKS if c["passed"])},
       "overall_ok": all(c["passed"] for c in CHECKS)}
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/968_force_invoke_edge2.json", "w") as f:
    json.dump(rep, f, indent=2)
for c in CHECKS:
    print(f"  [{'OK' if c['passed'] else 'X '}] {c['name']:24} {c['detail'][:120]}")
