"""1185 — AI Flow Strategist Lambda rollout.

Same pattern as 1180/1181 (deploy pipeline gap on new Lambda creation).
Direct boto3 create_function + env from equity-research + invoke +
verify the structured analyst note.
"""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1185_ai_flows_rollout.json"
BUCKET = "justhodl-dashboard-live"
AI_LAMBDA = "justhodl-flows-ai-analysis"
SOURCE_DIR = "aws/lambdas/justhodl-flows-ai-analysis/source"
SHARED_DIR = "aws/shared"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE_NAME = "justhodl-flows-ai-analysis-daily"
SCHEDULE = "cron(30 22 * * ? *)"

cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        if os.path.isdir(SHARED_DIR):
            for f in os.listdir(SHARED_DIR):
                if f.endswith(".py") and not f.startswith("__"):
                    fpath = os.path.join(SHARED_DIR, f)
                    if os.path.isfile(fpath):
                        zf.write(fpath, arcname=f)
        for root, _, files in os.walk(SOURCE_DIR):
            for f in files:
                if f.startswith("__") or f.endswith(".pyc"):
                    continue
                fpath = os.path.join(root, f)
                rel = os.path.relpath(fpath, SOURCE_DIR)
                zf.write(fpath, arcname=rel)
    return buf.getvalue()


def pull_anthropic():
    try:
        cfg_resp = lam.get_function_configuration(FunctionName="justhodl-equity-research")
        env = (cfg_resp.get("Environment") or {}).get("Variables", {})
        return env.get("ANTHROPIC_API_KEY")
    except Exception:
        return None


# Step 1: create
print("[1185] 1. Check / create AI Lambda")
exists = False
try:
    lam.get_function_configuration(FunctionName=AI_LAMBDA)
    exists = True
    out["steps"]["check"] = {"exists": True}
    print("  ✓ exists")
except lam.exceptions.ResourceNotFoundException:
    print("  ✗ creating")
    try:
        zip_bytes = build_zip()
        anth = pull_anthropic()
        env_vars = {"ANTHROPIC_API_KEY": anth} if anth else {}
        resp = lam.create_function(
            FunctionName=AI_LAMBDA,
            Runtime="python3.12",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            Description="AI Flow Strategist. Reads flows + research + critique + EDGAR + crisis KB, calls Sonnet 4.6 with 1h cache, outputs decisive cross-feed ticker calls.",
            Timeout=300,
            MemorySize=1024,
            Environment={"Variables": env_vars},
            Architectures=["x86_64"],
            Publish=False,
        )
        out["steps"]["create"] = {"created": True, "arn": resp.get("FunctionArn"), "anthropic_set": bool(anth)}
        print(f"  ✓ created · anthropic_set={bool(anth)}")
        for _ in range(30):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=AI_LAMBDA)
            if c.get("State") == "Active":
                break
        exists = True
    except Exception as e:
        out["steps"]["create"] = {"error": str(e)[:400]}
        print(f"  ❌ {e}")

# Step 2: Function URL
if exists:
    print(f"\n[1185] 2. Function URL")
    try:
        try:
            url = lam.get_function_url_config(FunctionName=AI_LAMBDA)["FunctionUrl"]
        except lam.exceptions.ResourceNotFoundException:
            r = lam.create_function_url_config(
                FunctionName=AI_LAMBDA,
                AuthType="NONE",
                Cors={"AllowOrigins": ["*"], "AllowMethods": ["GET","POST"], "AllowHeaders": ["Content-Type"], "MaxAge": 86400},
            )
            url = r["FunctionUrl"]
            try:
                lam.add_permission(
                    FunctionName=AI_LAMBDA, StatementId="FunctionURLAllowPublicAccess",
                    Action="lambda:InvokeFunctionUrl", Principal="*", FunctionUrlAuthType="NONE",
                )
            except lam.exceptions.ResourceConflictException:
                pass
        out["steps"]["url"] = {"url": url}
        print(f"  ✓ {url}")
    except Exception as e:
        out["steps"]["url"] = {"error": str(e)[:200]}

# Step 3: Schedule
if exists:
    print(f"\n[1185] 3. Schedule")
    try:
        events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                        Description="17:30 ET daily AI flow analysis")
        fn = lam.get_function(FunctionName=AI_LAMBDA)
        events.put_targets(Rule=RULE_NAME, Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
        try:
            lam.add_permission(
                FunctionName=AI_LAMBDA, StatementId=f"EBInvoke-{RULE_NAME}",
                Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}",
            )
        except lam.exceptions.ResourceConflictException:
            pass
        out["steps"]["schedule"] = {"created": True}
        print(f"  ✓ {SCHEDULE}")
    except Exception as e:
        out["steps"]["schedule"] = {"error": str(e)[:300]}

# Step 4: Sync invoke (~30-90s for Sonnet generation)
if exists:
    print(f"\n[1185] 4. Sync invoke (Sonnet 4.6 cross-feed analysis)")
    try:
        invoke_t0 = time.time()
        resp = lam.invoke(FunctionName=AI_LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
        elapsed = round(time.time() - invoke_t0, 1)
        payload = resp.get("Payload").read().decode()
        out["steps"]["invoke_raw"] = {
            "elapsed_s": elapsed,
            "status_code": resp.get("StatusCode"),
            "function_error": resp.get("FunctionError"),
            "body": payload[:1000],
        }
        print(f"  StatusCode={resp.get('StatusCode')} FunctionError={resp.get('FunctionError')} elapsed={elapsed}s")

        # Read the AI analysis from S3
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="etf-flows/ai-analysis.json")
            doc = json.loads(obj["Body"].read())
            a = doc.get("analysis", {})
            out["steps"]["ai_output"] = {
                "model": doc.get("model"),
                "claude_elapsed_s": doc.get("claude_elapsed_s"),
                "usage": doc.get("usage"),
                "input_summary": doc.get("input_summary"),
                "regime_call": a.get("regime_call"),
                "macro_narrative_chars": len(a.get("macro_narrative", "") or ""),
                "n_divergences": len(a.get("key_divergences", []) or []),
                "n_ticker_calls": len(a.get("ticker_calls", []) or []),
                "n_pair_trades": len(a.get("pair_trades", []) or []),
                "ticker_calls_summary": [
                    {
                        "t": c.get("ticker"),
                        "call": c.get("call"),
                        "conv": c.get("conviction"),
                        "tf": c.get("timeframe_days"),
                        "n_aligned": (c.get("signal_alignment") or {}).get("n_signals_aligned"),
                        "thesis": (c.get("thesis_1liner") or "")[:120],
                    }
                    for c in (a.get("ticker_calls") or [])
                ],
                "pair_trades": a.get("pair_trades", []),
                "watchlist": a.get("watchlist"),
                "regime_alpha_note": a.get("regime_alpha_note"),
                "self_assessment": a.get("self_assessment"),
                "narrative_preview": (a.get("macro_narrative") or "")[:500],
            }
            print(f"  ✓ regime={a.get('regime_call',{}).get('regime')} · {len(a.get('ticker_calls') or [])} calls · {len(a.get('pair_trades') or [])} pairs")
        except Exception as e:
            out["steps"]["ai_output"] = {"error": str(e)[:300]}
    except Exception as e:
        out["steps"]["invoke_raw"] = {"error": str(e)[:300]}

# Step 5: bucket policy already covers etf-flows/* from ops 1180
out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1185] DONE")
