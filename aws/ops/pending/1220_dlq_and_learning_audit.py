"""1220 — Investigate DLQ (80 messages) + audit existing signal-logger schema."""
import json
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1220_dlq_and_learning_audit.json"
BUCKET = "justhodl-dashboard-live"

cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
sqs = boto3.client("sqs", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ddb = boto3.client("dynamodb", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}

# Step 1: List all SQS queues — find DLQ
print("[1220] 1. List SQS queues + check DLQ contents")
try:
    qs = sqs.list_queues().get("QueueUrls", [])
    dlqs = [q for q in qs if "dlq" in q.lower() or "dead" in q.lower()]
    out["dlqs"] = []
    for q in dlqs:
        try:
            attrs = sqs.get_queue_attributes(QueueUrl=q, AttributeNames=["All"]).get("Attributes", {})
            out["dlqs"].append({
                "url": q,
                "name": q.split("/")[-1],
                "approx_messages": int(attrs.get("ApproximateNumberOfMessages", 0)),
                "approx_messages_not_visible": int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0)),
                "approx_messages_delayed": int(attrs.get("ApproximateNumberOfMessagesDelayed", 0)),
                "created": attrs.get("CreatedTimestamp"),
            })
        except Exception as e:
            out["dlqs"].append({"url": q, "error": str(e)[:100]})
    print(f"  Found {len(dlqs)} DLQ(s):")
    for d in out["dlqs"]:
        print(f"    {d.get('name','?')}: {d.get('approx_messages',0)} messages " +
              f"({d.get('approx_messages_not_visible', 0)} in-flight)")

    # Sample messages from the DLQ with 80+ messages
    main_dlq = next((d for d in out["dlqs"] if d.get("approx_messages", 0) >= 50), None)
    if main_dlq:
        url = main_dlq["url"]
        print(f"\n  Sampling messages from {main_dlq['name']}:")
        # Receive without deleting
        sample_msgs = []
        for i in range(5):
            resp = sqs.receive_message(
                QueueUrl=url, MaxNumberOfMessages=3,
                VisibilityTimeout=5, WaitTimeSeconds=1,
                MessageAttributeNames=["All"], AttributeNames=["All"],
            )
            for m in (resp.get("Messages") or []):
                body = m.get("Body", "")[:600]
                attrs = m.get("Attributes", {})
                msg_attrs = m.get("MessageAttributes", {})
                sample_msgs.append({
                    "body_preview": body,
                    "attributes": attrs,
                    "msg_attrs": {k: v.get("StringValue", "") for k, v in msg_attrs.items()},
                })
        out["dlq_samples"] = sample_msgs[:10]
        print(f"    Got {len(sample_msgs)} samples")
        for s in sample_msgs[:3]:
            print(f"      body: {s.get('body_preview', '')[:200]}")

except Exception as e:
    out["dlq_err"] = str(e)[:300]
    print(f"  ❌ {e}")

# Step 2: Audit existing learning system tables + signal-logger
print(f"\n[1220] 2. Existing learning system — DDB tables + signal-logger")
try:
    tables = ddb.list_tables().get("TableNames", [])
    out["learning_tables"] = [t for t in tables if "signal" in t or "outcome" in t]
    print(f"  Tables: {out['learning_tables']}")

    # Read signal-logger source briefly
    sl = lam.get_function(FunctionName="justhodl-signal-logger")
    out["signal_logger"] = {
        "last_modified": sl["Configuration"]["LastModified"][:19],
        "timeout": sl["Configuration"]["Timeout"],
    }
    print(f"  signal-logger last modified: {sl['Configuration']['LastModified'][:19]}")
except Exception as e:
    out["learning_err"] = str(e)[:300]

# Step 3: Check cascade-validator + see if cascade signals are in signal-logger
print(f"\n[1220] 3. Check cascade-validator coverage")
try:
    val = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cascade-validation-log.json")["Body"].read())
    out["cascade_validator"] = {
        "n_validated": val.get("n_predictions_validated"),
        "by_tier_stats": val.get("by_tier_stats"),
        "lookback_days": val.get("lookback_days"),
    }
    print(f"  Cascade validator: {val.get('n_predictions_validated')} predictions tracked")
except Exception as e:
    out["cascade_validator"] = {"error": str(e)[:200]}

# Step 4: Quick S3 check for predictions / scored data files
print(f"\n[1220] 4. Existing prediction tracking files")
for key in ["data/cascade-validation-log.json", "data/pnl-stats.json",
            "data/cascade-validation-history/", "data/simulated-portfolio.json"]:
    try:
        if key.endswith("/"):
            paginator = s3.get_paginator("list_objects_v2")
            n = 0
            for page in paginator.paginate(Bucket=BUCKET, Prefix=key):
                n += len(page.get("Contents") or [])
            print(f"  {key}: {n} files")
        else:
            head = s3.head_object(Bucket=BUCKET, Key=key)
            print(f"  {key}: {head['ContentLength']} bytes, modified {head['LastModified'].isoformat()[:19]}")
    except Exception:
        print(f"  {key}: not found")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1220] DONE")
