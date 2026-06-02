"""1222 — Deploy AI digest narrator + invoke + verify."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1222_ai_digest_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-digest-trends-ai"
SOURCE_DIR = "aws/lambdas/justhodl-digest-trends-ai/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE_NAME = "justhodl-digest-trends-ai-daily"
SCHEDULE = "cron(0 13 * * MON-FRI *)"

cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)
ssm = boto3.client("ssm", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(SOURCE_DIR):
            for f in files:
                if f.startswith("__") or f.endswith(".pyc"):
                    continue
                fpath = os.path.join(root, f)
                rel = os.path.relpath(fpath, SOURCE_DIR)
                zf.write(fpath, arcname=rel)
    return buf.getvalue()


# Check SSM for Anthropic API key
print("[1222] 0. Check Anthropic API key")
api_key_found = False
api_key_path = None
for path in ["/justhodl/anthropic/api_key", "/anthropic/api_key",
              "/justhodl/anthropic_api_key"]:
    try:
        v = ssm.get_parameter(Name=path, WithDecryption=True)
        if v.get("Parameter", {}).get("Value"):
            api_key_found = True
            api_key_path = path
            print(f"  ✓ Found at {path}")
            break
    except Exception:
        continue
if not api_key_found:
    # List SSM params with 'anthropic' or 'claude' in name
    try:
        params = ssm.describe_parameters(
            ParameterFilters=[{"Key": "Name", "Option": "Contains", "Values": ["anthropic"]}]
        ).get("Parameters", [])
        out["ssm_anthropic_params"] = [p["Name"] for p in params]
        print(f"  SSM params with 'anthropic': {[p['Name'] for p in params]}")
    except Exception as e:
        print(f"  SSM list err: {e}")

# Create or update
print(f"\n[1222] 1. Deploy {LAMBDA}")
try:
    lam.get_function_configuration(FunctionName=LAMBDA)
    zip_bytes = build_zip()
    lam.update_function_code(FunctionName=LAMBDA, ZipFile=zip_bytes)
    for _ in range(15):
        time.sleep(2)
        c = lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("LastUpdateStatus") == "Successful":
            break
    out["create"] = "updated"
    print("  ✓ updated")
except lam.exceptions.ResourceNotFoundException:
    zip_bytes = build_zip()
    lam.create_function(
        FunctionName=LAMBDA, Runtime="python3.12", Role=ROLE_ARN,
        Handler="lambda_function.lambda_handler", Code={"ZipFile": zip_bytes},
        Description="AI digest narrator", Timeout=90, MemorySize=512,
        Architectures=["x86_64"], Publish=False,
    )
    for _ in range(30):
        time.sleep(2)
        c = lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("State") == "Active":
            break
    out["create"] = "created"
    print("  ✓ created")

# Schedule
print(f"\n[1222] 2. Schedule {SCHEDULE}")
try:
    events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                    Description="Daily AI digest 9:00 ET")
    fn = lam.get_function(FunctionName=LAMBDA)
    events.put_targets(Rule=RULE_NAME, Targets=[{"Id": "1", "Arn": fn["Configuration"]["FunctionArn"]}])
    try:
        lam.add_permission(FunctionName=LAMBDA, StatementId=f"EBInvoke-{RULE_NAME}",
                            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}")
    except lam.exceptions.ResourceConflictException:
        pass
    out["schedule"] = SCHEDULE
    print("  ✓ scheduled")
except Exception as e:
    out["schedule_err"] = str(e)[:300]

# Sync invoke
print(f"\n[1222] 3. Invoke for first AI digest")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["invoke"] = {"elapsed_s": elapsed, "status": resp.get("StatusCode"),
                      "function_error": resp.get("FunctionError"),
                      "body": payload[:2000]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:600]}")
    else:
        try:
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  preds={inner.get('n_predictions')} has_overview={inner.get('has_overview')}")
        except Exception:
            pass
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

# Read AI digest
print(f"\n[1222] 4. Read AI digest output")
try:
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/digest-trends-ai.json")["Body"].read())
    narrative = doc.get("narrative", {})
    out["digest_doc"] = {
        "generated_at": doc.get("generated_at"),
        "date": doc.get("date"),
        "narrative_keys": list(narrative.keys()),
        "has_overview": "overview" in narrative,
        "has_fallback": narrative.get("fallback") or narrative.get("error"),
        "narrative_preview": {
            k: (v[:200] if isinstance(v, str) else v) for k, v in narrative.items()
        },
    }
    print(f"  ✓ AI doc keys: {list(narrative.keys())}")
    if narrative.get("overview"):
        print(f"\n  OVERVIEW: {narrative['overview'][:200]}")
except Exception as e:
    out["digest_doc"] = {"error": str(e)[:200]}

# Verify digest-trends.html was deployed
print(f"\n[1222] 5. Verify digest-trends.html deployed to GitHub Pages")
try:
    import urllib.request
    req = urllib.request.Request("https://justhodl.ai/digest-trends.html",
                                    headers={"Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=15) as r:
        html = r.read().decode()
    out["html_check"] = {
        "size_kb": round(len(html) / 1024, 1),
        "has_ai_narrative": "ai-narrative-grid" in html,
        "has_calibration_grid": "cal-grid" in html,
        "has_retail_integration": "retail-row" in html,
        "has_live_kpis": "kpi-strip" in html,
    }
    print(f"  ✓ digest-trends.html: {out['html_check']['size_kb']} KB")
    print(f"    AI narrative grid: {out['html_check']['has_ai_narrative']}")
    print(f"    Calibration grid: {out['html_check']['has_calibration_grid']}")
    print(f"    Retail integration: {out['html_check']['has_retail_integration']}")
except Exception as e:
    out["html_check"] = {"error": str(e)[:200]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1222] DONE")
