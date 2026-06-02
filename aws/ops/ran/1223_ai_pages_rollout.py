"""1223 — Deploy justhodl-page-ai-commentary + invoke for all 5 pages + verify panels live."""
import json
import os
import time
import zipfile
import io
import urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1223_ai_pages_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-page-ai-commentary"
SOURCE_DIR = "aws/lambdas/justhodl-page-ai-commentary/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE_NAME = "justhodl-page-ai-commentary-daily"
SCHEDULE = "cron(0 14 * * MON-FRI *)"
PAGES = ["pre-pump-radar", "signal-board", "risk-desk", "liquidity", "fundamentals"]

cfg = Config(read_timeout=600, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)

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


# Deploy Lambda
print(f"[1223] 1. Deploy {LAMBDA}")
try:
    lam.get_function_configuration(FunctionName=LAMBDA)
    zip_bytes = build_zip()
    lam.update_function_code(FunctionName=LAMBDA, ZipFile=zip_bytes)
    for _ in range(15):
        time.sleep(2)
        c = lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("LastUpdateStatus") == "Successful":
            break
    # Also update memory/timeout (in case different from before)
    lam.update_function_configuration(FunctionName=LAMBDA, Timeout=300, MemorySize=512)
    out["deploy"] = "updated"
    print("  ✓ updated")
except lam.exceptions.ResourceNotFoundException:
    zip_bytes = build_zip()
    lam.create_function(
        FunctionName=LAMBDA, Runtime="python3.12", Role=ROLE_ARN,
        Handler="lambda_function.lambda_handler", Code={"ZipFile": zip_bytes},
        Description="Universal AI commentary engine for all pages",
        Timeout=300, MemorySize=512, Architectures=["x86_64"], Publish=False,
    )
    for _ in range(30):
        time.sleep(2)
        c = lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("State") == "Active":
            break
    out["deploy"] = "created"
    print("  ✓ created")

# Schedule
print(f"\n[1223] 2. Schedule {SCHEDULE}")
try:
    events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                    Description="Daily AI commentary 10:00 ET")
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

# Invoke for ALL pages
print(f"\n[1223] 3. Invoke to generate AI commentary for all 5 pages")
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
            print(f"  pages_generated: {inner.get('pages_generated')}")
            for p, r in (inner.get("results") or {}).items():
                print(f"    {p}: has_content={r.get('has_content')} keys={r.get('keys')}")
        except Exception:
            pass
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

# Read each page's AI commentary
print(f"\n[1223] 4. Read each page's AI commentary")
out["page_commentaries"] = {}
for page in PAGES:
    try:
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=f"data/ai-commentary/{page}.json")["Body"].read())
        commentary = doc.get("commentary", {})
        out["page_commentaries"][page] = {
            "generated_at": doc.get("generated_at"),
            "has_content": "error" not in commentary,
            "keys": list(commentary.keys()),
            "headline_preview": (commentary.get("headline") or "")[:300],
            "score": commentary.get("confidence_score") or commentary.get("regime_score")
                      or commentary.get("risk_score") or commentary.get("liquidity_score")
                      or commentary.get("value_score"),
        }
        print(f"  ✓ {page}: {out['page_commentaries'][page]['headline_preview'][:80]}…")
    except Exception as e:
        out["page_commentaries"][page] = {"error": str(e)[:200]}
        print(f"  ✗ {page}: {e}")

# Verify pages deployed to GitHub Pages
print(f"\n[1223] 5. Verify pages deployed with AI panels")
out["page_deployments"] = {}
for page in PAGES:
    page_file = "pre-pump-radar.html" if page == "pre-pump-radar" else f"{page}.html"
    url = f"https://justhodl.ai/{page_file}"
    try:
        req = urllib.request.Request(url, headers={"Cache-Control": "no-cache"})
        with urllib.request.urlopen(req, timeout=10) as r:
            html = r.read().decode()
        has_panel = "ai-brief-panel" in html
        has_page_var = f'__AI_BRIEF_PAGE__ = "{page}"' in html
        out["page_deployments"][page] = {
            "url": url, "size_kb": round(len(html) / 1024, 1),
            "has_panel": has_panel, "has_page_var": has_page_var,
        }
        print(f"  {page}: {'✓' if (has_panel and has_page_var) else '⚠'} "
              f"panel={has_panel} var={has_page_var} ({out['page_deployments'][page]['size_kb']} KB)")
    except Exception as e:
        out["page_deployments"][page] = {"error": str(e)[:120]}
        print(f"  {page}: ⚠ {e}")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1223] DONE")
