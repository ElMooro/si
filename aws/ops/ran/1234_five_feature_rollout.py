"""1234 — Rollout 5-feature batch:
  1. Deploy new ticket-ai-rationale Lambda + schedule
  2. Verify existing Lambdas got code updates (cascade-recalibrator,
     trade-tickets, trade-ticket-monitor, prepump-alerts-router)
  3. Invoke each in order to test end-to-end
  4. Verify all output files updated correctly
"""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1234_five_feature_rollout.json"
BUCKET = "justhodl-dashboard-live"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"

cfg = Config(read_timeout=600, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}


def build_zip(source_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(source_dir):
            for f in files:
                if f.startswith("__") or f.endswith(".pyc"):
                    continue
                fpath = os.path.join(root, f)
                rel = os.path.relpath(fpath, source_dir)
                zf.write(fpath, arcname=rel)
    return buf.getvalue()


# ──────────────────────────────────────────────────
# 1. NEW: justhodl-ticket-ai-rationale (create + schedule)
# ──────────────────────────────────────────────────
NEW_LAMBDA = "justhodl-ticket-ai-rationale"
NEW_SOURCE = "aws/lambdas/justhodl-ticket-ai-rationale/source"
NEW_RULE = "justhodl-ticket-ai-rationale-hourly"
NEW_SCHEDULE = "cron(5 * * * ? *)"

print(f"[1234] 1. Create new Lambda {NEW_LAMBDA}")
try:
    try:
        lam.get_function_configuration(FunctionName=NEW_LAMBDA)
        # exists, update
        zip_bytes = build_zip(NEW_SOURCE)
        lam.update_function_code(FunctionName=NEW_LAMBDA, ZipFile=zip_bytes)
        for _ in range(20):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=NEW_LAMBDA)
            if c.get("LastUpdateStatus") == "Successful":
                break
        out["new_lambda"] = {"action": "updated", "code_sha": c.get("CodeSha256")[:16]}
        print(f"  ✓ updated")
    except lam.exceptions.ResourceNotFoundException:
        zip_bytes = build_zip(NEW_SOURCE)
        lam.create_function(
            FunctionName=NEW_LAMBDA, Runtime="python3.12", Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler", Code={"ZipFile": zip_bytes},
            Description="AI-generated rationale for trade tickets",
            Timeout=180, MemorySize=512, Architectures=["x86_64"], Publish=False,
        )
        for _ in range(30):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=NEW_LAMBDA)
            if c.get("State") == "Active":
                break
        out["new_lambda"] = {"action": "created", "code_sha": c.get("CodeSha256")[:16]}
        print(f"  ✓ created")
except Exception as e:
    out["new_lambda"] = {"error": str(e)[:300]}
    print(f"  ❌ {e}")

print(f"\n[1234] 2. Schedule {NEW_LAMBDA}: {NEW_SCHEDULE}")
try:
    events.put_rule(Name=NEW_RULE, ScheduleExpression=NEW_SCHEDULE, State="ENABLED",
                    Description="Hourly AI rationales for trade tickets")
    fn = lam.get_function(FunctionName=NEW_LAMBDA)
    events.put_targets(Rule=NEW_RULE, Targets=[{"Id": "1", "Arn": fn["Configuration"]["FunctionArn"]}])
    try:
        lam.add_permission(FunctionName=NEW_LAMBDA, StatementId=f"EBInvoke-{NEW_RULE}",
                            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{NEW_RULE}")
    except lam.exceptions.ResourceConflictException:
        pass
    out["schedule"] = NEW_SCHEDULE
    print("  ✓ scheduled")
except Exception as e:
    out["schedule_err"] = str(e)[:300]

# ──────────────────────────────────────────────────
# 3. Verify other Lambdas got code updates via deploy-lambdas
# ──────────────────────────────────────────────────
print(f"\n[1234] 3. Verify Lambda updates")
UPDATED_LAMBDAS = [
    "justhodl-cascade-recalibrator",
    "justhodl-trade-tickets",
    "justhodl-trade-ticket-monitor",
    "justhodl-prepump-alerts-router",
]
out["lambda_states"] = {}
for L in UPDATED_LAMBDAS:
    try:
        info = lam.get_function_configuration(FunctionName=L)
        out["lambda_states"][L] = {
            "state": info.get("State"),
            "last_update": info.get("LastUpdateStatus"),
            "code_sha": info.get("CodeSha256")[:16],
            "modified": info.get("LastModified")[:19],
        }
        print(f"  {L}: state={info.get('State')} sha={info.get('CodeSha256')[:16]}")
    except Exception as e:
        out["lambda_states"][L] = {"error": str(e)[:120]}

# ──────────────────────────────────────────────────
# 4. Invoke trade-tickets to generate fresh tickets (with conf weighting)
# ──────────────────────────────────────────────────
print(f"\n[1234] 4. Invoke trade-tickets (with conf weighting)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-trade-tickets",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["trade_tickets_invoke"] = {"status": resp.get("StatusCode"),
                                     "elapsed_s": elapsed,
                                     "function_error": resp.get("FunctionError"),
                                     "body": payload[:1000]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:400]}")
except Exception as e:
    out["trade_tickets_invoke"] = {"error": str(e)[:200]}

# ──────────────────────────────────────────────────
# 5. Invoke AI rationale Lambda
# ──────────────────────────────────────────────────
print(f"\n[1234] 5. Invoke ticket-ai-rationale (generate Claude rationales)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName=NEW_LAMBDA,
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["rationale_invoke"] = {"status": resp.get("StatusCode"),
                                 "elapsed_s": elapsed,
                                 "function_error": resp.get("FunctionError"),
                                 "body": payload[:1200]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:400]}")
    else:
        try:
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  n_generated={inner.get('n_generated')} n_cached={inner.get('n_cached')} n_errors={inner.get('n_errors')}")
        except: pass
except Exception as e:
    out["rationale_invoke"] = {"error": str(e)[:200]}

# ──────────────────────────────────────────────────
# 6. Invoke recalibrator (with guardrails)
# ──────────────────────────────────────────────────
print(f"\n[1234] 6. Invoke cascade-recalibrator (with anti-overfit guardrails)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-cascade-recalibrator",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["recal_invoke"] = {"status": resp.get("StatusCode"),
                             "elapsed_s": elapsed,
                             "function_error": resp.get("FunctionError"),
                             "body": payload[:1000]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:400]}")
except Exception as e:
    out["recal_invoke"] = {"error": str(e)[:200]}

# ──────────────────────────────────────────────────
# 7. Invoke monitor (horizon-aware)
# ──────────────────────────────────────────────────
print(f"\n[1234] 7. Invoke trade-ticket-monitor (horizon-aware urgency)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-trade-ticket-monitor",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["monitor_invoke"] = {"status": resp.get("StatusCode"),
                               "elapsed_s": elapsed,
                               "function_error": resp.get("FunctionError"),
                               "body": payload[:1200]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if not resp.get("FunctionError"):
        try:
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  watched={inner.get('n_watched')} checked={inner.get('n_checked')} skipped={inner.get('n_skipped')} alerts={inner.get('n_alerts')}")
        except: pass
except Exception as e:
    out["monitor_invoke"] = {"error": str(e)[:200]}

# ──────────────────────────────────────────────────
# 8. Verify S3 outputs
# ──────────────────────────────────────────────────
print(f"\n[1234] 8. Verify output files")

# Trade tickets schema check
try:
    tickets_doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/trade-tickets.json")["Body"].read())
    tickets = (tickets_doc.get("tickets") or [])
    sample = next((t for t in tickets if not t.get("error")), {})
    out["tickets_schema"] = {
        "n_tickets": len(tickets),
        "sample_has_horizon_days": "expected_horizon_days" in sample,
        "sample_has_conf_mult": "position_confidence_multiplier" in sample,
        "sample_conf_mult": sample.get("position_confidence_multiplier"),
        "sample_horizon": sample.get("expected_horizon_days"),
        "sample_regime": sample.get("horizon_regime"),
        "sample_setup": sample.get("setup_type"),
    }
    print(f"  ✓ trade-tickets.json: {len(tickets)} tickets, horizon={sample.get('expected_horizon_days')}d, conf_mult={sample.get('position_confidence_multiplier')}")
except Exception as e:
    out["tickets_schema"] = {"error": str(e)[:120]}

# AI rationale check
try:
    rat_doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/trade-tickets-ai-rationale.json")["Body"].read())
    by_ticker = rat_doc.get("by_ticker") or {}
    out["rationale_doc"] = {
        "generated_at": rat_doc.get("generated_at"),
        "n_tickets_total": rat_doc.get("n_tickets_total"),
        "n_generated": rat_doc.get("n_generated"),
        "n_cached": rat_doc.get("n_cached"),
        "n_in_cache": len(by_ticker),
        "sample_rationales": {
            tk: (info.get("rationale", "")[:200])
            for tk, info in list(by_ticker.items())[:5]
        },
    }
    print(f"  ✓ trade-tickets-ai-rationale.json: {len(by_ticker)} cached rationales")
    for tk, info in list(by_ticker.items())[:3]:
        print(f"    {tk}: {info.get('rationale', '')[:150]}")
except Exception as e:
    out["rationale_doc"] = {"error": str(e)[:120]}

# Recalibration audit (guardrails)
try:
    audit = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cascade-recalibration-audit.json")["Body"].read())
    out["audit_guardrails"] = {
        "status": audit.get("guardrails_status"),
        "guardrails_count": len(audit.get("guardrails") or []),
        "sample_guardrails": (audit.get("guardrails") or [])[:3],
    }
    print(f"  ✓ guardrails: status={audit.get('guardrails_status')}, count={len(audit.get('guardrails') or [])}")
except Exception as e:
    out["audit_guardrails"] = {"error": str(e)[:120]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1234] DONE")
