"""1221 — Self-improvement loop rollout + DLQ purge + inaugural snapshot."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1221_self_improvement_rollout.json"
BUCKET = "justhodl-dashboard-live"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"

cfg = Config(read_timeout=600, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)
sqs = boto3.client("sqs", region_name=REGION, config=cfg)

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


def create_or_update_lambda(name, source_dir, memory, timeout, description):
    try:
        existing = lam.get_function_configuration(FunctionName=name)
        # Update existing
        zip_bytes = build_zip(source_dir)
        lam.update_function_code(FunctionName=name, ZipFile=zip_bytes)
        for _ in range(15):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=name)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") == "Successful":
                break
        return "updated"
    except lam.exceptions.ResourceNotFoundException:
        zip_bytes = build_zip(source_dir)
        lam.create_function(
            FunctionName=name, Runtime="python3.12", Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler", Code={"ZipFile": zip_bytes},
            Description=description, Timeout=timeout, MemorySize=memory,
            Architectures=["x86_64"], Publish=False,
        )
        for _ in range(30):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=name)
            if c.get("State") == "Active":
                break
        return "created"


def schedule_lambda(name, rule_name, schedule_expr):
    events.put_rule(Name=rule_name, ScheduleExpression=schedule_expr,
                    State="ENABLED", Description=f"Schedule for {name}")
    fn = lam.get_function(FunctionName=name)
    events.put_targets(Rule=rule_name,
                        Targets=[{"Id": "1", "Arn": fn["Configuration"]["FunctionArn"]}])
    try:
        lam.add_permission(FunctionName=name, StatementId=f"EBInvoke-{rule_name}",
                            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{rule_name}")
    except lam.exceptions.ResourceConflictException:
        pass


LAMBDAS = [
    {"name": "justhodl-prediction-snapshotter",
     "source_dir": "aws/lambdas/justhodl-prediction-snapshotter/source",
     "memory": 512, "timeout": 60,
     "description": "Daily prediction snapshot with full features",
     "rule": "justhodl-prediction-snapshotter-daily",
     "schedule": "cron(0 21 * * MON-FRI *)"},
    {"name": "justhodl-self-improvement",
     "source_dir": "aws/lambdas/justhodl-self-improvement/source",
     "memory": 1024, "timeout": 180,
     "description": "Self-improvement loop: score yesterday + calibrate",
     "rule": "justhodl-self-improvement-daily",
     "schedule": "cron(30 12 * * MON-FRI *)"},
]

# ── Step 1: Deploy + schedule both Lambdas ──
print("[1221] 1. Deploy + schedule self-improvement Lambdas")
out["lambdas"] = {}
for cfg_l in LAMBDAS:
    name = cfg_l["name"]
    lam_out = {}
    try:
        lam_out["create"] = create_or_update_lambda(
            name, cfg_l["source_dir"], cfg_l["memory"], cfg_l["timeout"],
            cfg_l["description"],
        )
        print(f"  ✓ {name}: {lam_out['create']}")
    except Exception as e:
        lam_out["create_err"] = str(e)[:300]
        print(f"  ❌ {name}: {e}")
        out["lambdas"][name] = lam_out
        continue
    try:
        schedule_lambda(name, cfg_l["rule"], cfg_l["schedule"])
        lam_out["schedule"] = cfg_l["schedule"]
        print(f"    scheduled: {cfg_l['schedule']}")
    except Exception as e:
        lam_out["schedule_err"] = str(e)[:300]
    out["lambdas"][name] = lam_out


# ── Step 2: Invoke prediction-snapshotter for today's data ──
print("\n[1221] 2. Invoke prediction-snapshotter (captures today's predictions + features)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-prediction-snapshotter",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["snapshot_invoke"] = {
        "elapsed_s": elapsed, "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "body": payload[:1500],
    }
    if not resp.get("FunctionError"):
        try:
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  ✓ captured {inner.get('n_tickers')} tickers · "
                  f"distribution: {inner.get('alert_distribution')}")
        except Exception:
            pass
except Exception as e:
    out["snapshot_invoke"] = {"error": str(e)[:300]}

# ── Step 3: Invoke self-improvement (will check latest snapshot since no yesterday snapshot yet) ──
print("\n[1221] 3. Invoke self-improvement Lambda (inaugural run)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-self-improvement",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["si_invoke"] = {
        "elapsed_s": elapsed, "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "body": payload[:1500],
    }
    if not resp.get("FunctionError"):
        try:
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  ✓ scored={inner.get('n_predictions_scored')} valid={inner.get('n_valid_outcomes')} "
                  f"features={inner.get('n_features_attributed')} calibrated={inner.get('calibrated')}")
        except Exception:
            pass
except Exception as e:
    out["si_invoke"] = {"error": str(e)[:300]}

# ── Step 4: PURGE DLQ (81 stale messages) ──
print("\n[1221] 4. Purge stale DLQ messages")
try:
    dlqs = [q for q in sqs.list_queues().get("QueueUrls", [])
            if "dlq" in q.lower() or "dead" in q.lower()]
    out["dlq_purge"] = []
    for url in dlqs:
        name = url.split("/")[-1]
        try:
            attrs_before = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"]).get("Attributes", {})
            n_before = int(attrs_before.get("ApproximateNumberOfMessages", 0))
            if n_before == 0:
                out["dlq_purge"].append({"name": name, "before": 0, "purged": False})
                continue
            # PurgeQueue clears all messages (can only be called once per 60s)
            sqs.purge_queue(QueueUrl=url)
            time.sleep(2)
            attrs_after = sqs.get_queue_attributes(QueueUrl=url, AttributeNames=["All"]).get("Attributes", {})
            n_after = int(attrs_after.get("ApproximateNumberOfMessages", 0))
            out["dlq_purge"].append({"name": name, "before": n_before,
                                      "after": n_after, "purged": True})
            print(f"  ✓ {name}: purged {n_before} messages → {n_after} remaining")
        except sqs.exceptions.PurgeQueueInProgress:
            out["dlq_purge"].append({"name": name, "error": "purge_in_progress (try again in 60s)"})
            print(f"  ⚠ {name}: purge already in progress")
        except Exception as e:
            out["dlq_purge"].append({"name": name, "error": str(e)[:120]})
            print(f"  ❌ {name}: {e}")
except Exception as e:
    out["dlq_purge"] = {"error": str(e)[:300]}

# ── Step 5: Read calibration file ──
print("\n[1221] 5. Read cascade-calibration.json")
try:
    cal = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cascade-calibration.json")["Body"].read())
    out["calibration"] = {
        "last_updated": cal.get("last_updated"),
        "current_weights": cal.get("current_weights"),
        "n_data_points": (cal.get("feature_attribution") or {}).get("n_predictions_analyzed", 0),
        "history_entries": len(cal.get("history") or []),
    }
    print(f"  ✓ calibration: {len(cal.get('current_weights') or {})} weights, "
          f"{out['calibration']['n_data_points']} data points")
except Exception as e:
    out["calibration"] = {"error": str(e)[:200]}

# ── Step 6: Read snapshot ──
print("\n[1221] 6. Read latest snapshot")
try:
    snap = json.loads(s3.get_object(Bucket=BUCKET, Key="data/predictions-snapshots/latest.json")["Body"].read())
    out["snapshot"] = {
        "snapshot_date": snap.get("snapshot_date"),
        "n_tickers": snap.get("n_tickers"),
        "alert_distribution": snap.get("alert_distribution"),
        "sample_predictions": [
            {"ticker": p.get("ticker"),
             "n_alerts": len(p.get("alerts", [])),
             "n_features": len(p.get("features", {}))}
            for p in (snap.get("predictions") or [])[:5]
        ],
    }
    print(f"  ✓ snapshot: {snap.get('n_tickers')} tickers, {len(snap.get('alert_distribution', {}))} alert types")
except Exception as e:
    out["snapshot"] = {"error": str(e)[:200]}


out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1221] DONE")
