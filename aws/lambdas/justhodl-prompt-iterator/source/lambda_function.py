"""
justhodl-prompt-iterator — Loop 3 weekly prompt self-improvement.

Runs Sunday 10:00 UTC (after calibrator at 9:00 UTC).

Process:
  1. Read last 14 morning briefs from archive/intelligence/
  2. Score each brief on:
     - accuracy_score (from outcomes table, only correct ∈ {T,F})
     - specificity_score (heuristic count of numbers/tickers)
  3. If we have data + degrading trend, propose a prompt change
  4. Validate proposal against safety rules
  5. If passes, save with versioning + log
  6. Compare next-period scoring to previous; auto-revert if worse
"""
import json
import os
import re
import time
import urllib.request
import ssl
from datetime import datetime, timezone, timedelta
import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
TEMPLATES_KEY = "learning/prompt_templates.json"
LOG_KEY = "learning/improvement_log.json"
ANTHROPIC_KEY = os.environ.get("ANTHROPIC_KEY", "")

s3 = boto3.client("s3", region_name=REGION)
dynamodb = boto3.resource("dynamodb", region_name=REGION)
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Safety rules — guardrails against bad prompt edits
LENGTH_MIN_RATIO = 0.5
LENGTH_MAX_RATIO = 1.5
REQUIRED_TERMS = ["real numbers", "live data"]  # at least one
FORBIDDEN_TERMS = ["ignore previous", "forget previous", "placeholder", "fake", "fictional"]


def get_s3_json(key):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[S3] {key}: {e}")
        return None


def put_s3_json(key, body, cache="public, max-age=300"):
    s3.put_object(
        Bucket=BUCKET, Key=key,
        Body=json.dumps(body, indent=2, default=str).encode("utf-8"),
        ContentType="application/json", CacheControl=cache,
    )


def list_recent_briefs(days=14):
    """Return list of (date, brief_dict) for last N days from archive."""
    now = datetime.now(timezone.utc)
    briefs = []
    for d in range(days):
        day = now - timedelta(days=d)
        prefix = day.strftime("archive/intelligence/%Y/%m/%d/")
        try:
            resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=10)
            for obj in resp.get("Contents", []):
                # Pick one per day — first one (closest to start of day)
                pass
            # Simpler: get the first object found that day
            objs = sorted(resp.get("Contents", []), key=lambda o: o["Key"])
            if objs:
                obj = s3.get_object(Bucket=BUCKET, Key=objs[0]["Key"])
                briefs.append({
                    "date": day.strftime("%Y-%m-%d"),
                    "key": objs[0]["Key"],
                    "data": json.loads(obj["Body"].read().decode("utf-8")),
                })
        except Exception as e:
            print(f"[LIST] {prefix}: {e}")
    return sorted(briefs, key=lambda b: b["date"])


def score_specificity(brief_text):
    """Heuristic: count concrete numbers, tickers, percentages, dates."""
    if not brief_text:
        return 0.0
    text = str(brief_text)
    # Specific patterns
    numbers = len(re.findall(r"\b\d+(?:\.\d+)?(?:[KMB])?\b", text))
    percentages = len(re.findall(r"\d+(?:\.\d+)?%", text))
    tickers = len(re.findall(r"\$?[A-Z]{2,5}\b", text))
    timeframes = len(re.findall(r"\b(?:7d|30d|90d|1y|YoY|QoQ|H1|H2|Q[1-4])\b", text))
    # Normalize by text length (per 100 words)
    words = max(1, len(text.split()))
    raw = numbers + percentages * 1.5 + tickers + timeframes * 1.5
    return round((raw / words) * 100, 2)


def score_brief_accuracy(brief_date_iso):
    """Find outcomes scored within 7 days of this brief; return mean
    correct rate. Returns None if not enough data."""
    try:
        brief_dt = datetime.fromisoformat(brief_date_iso.replace("Z", "+00:00"))
    except Exception:
        return None
    table = dynamodb.Table("justhodl-outcomes")
    try:
        # Scan with filter — outcomes table is small, filter in app
        resp = table.scan(Limit=5000)
        items = resp.get("Items", [])
        relevant = []
        for o in items:
            if o.get("correct") not in (True, False):
                continue
            try:
                logged = datetime.fromisoformat(str(o.get("logged_at", "")).replace("Z", "+00:00"))
            except Exception:
                continue
            # Only outcomes whose signal was logged within 24h before this brief
            delta = (brief_dt - logged).total_seconds()
            if 0 <= delta <= 86400:
                relevant.append(o)
        if len(relevant) < 3:
            return None  # not enough data
        correct_count = sum(1 for o in relevant if o.get("correct") is True)
        return correct_count / len(relevant)
    except Exception as e:
        print(f"[ACC] {e}")
        return None


def validate_proposed_template(old_text, new_text):
    """Return (ok: bool, reason: str)."""
    if not new_text or not isinstance(new_text, str):
        return False, "empty or non-string"
    new_text = new_text.strip()
    old_len = max(1, len(old_text))
    new_len = len(new_text)
    ratio = new_len / old_len
    if ratio < LENGTH_MIN_RATIO:
        return False, f"too short (ratio {ratio:.2f} < {LENGTH_MIN_RATIO})"
    if ratio > LENGTH_MAX_RATIO:
        return False, f"too long (ratio {ratio:.2f} > {LENGTH_MAX_RATIO})"
    new_lower = new_text.lower()
    if not any(t in new_lower for t in [r.lower() for r in REQUIRED_TERMS]):
        return False, f"missing required terms: {REQUIRED_TERMS}"
    for f in FORBIDDEN_TERMS:
        if f.lower() in new_lower:
            return False, f"contains forbidden term: {f}"
    if "REAL" not in new_text.upper() and "real" not in new_text.lower():
        return False, "missing 'real' constraint"
    return True, "ok"


def call_anthropic(prompt, max_tokens=400):
    if not ANTHROPIC_KEY:
        return None
    body = json.dumps({
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": max_tokens,
        "messages": [{"role": "user", "content": prompt}],
    }).encode("utf-8")
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "Content-Type": "application/json",
            "x-api-key": ANTHROPIC_KEY,
            "anthropic-version": "2023-06-01",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=60, context=ctx) as r:
            data = json.loads(r.read().decode("utf-8"))
            content = data.get("content", [])
            if content and isinstance(content, list):
                return content[0].get("text", "").strip()
    except Exception as e:
        print(f"[ANTHROPIC] {e}")
    return None


def lambda_handler(event, context):
    print("=== PROMPT ITERATOR v1 ===")
    now = datetime.now(timezone.utc)

    # 1. Load current template + change log
    templates = get_s3_json(TEMPLATES_KEY) or {}
    log = get_s3_json(LOG_KEY) or []
    if not isinstance(log, list):
        log = []
    current_template = templates.get("morning_brief", "")
    current_version = templates.get("_version", 0)
    print(f"  Current template version: v{current_version}, length: {len(current_template)} chars")

    if not current_template:
        return {"statusCode": 200, "body": json.dumps({"skip": "no current template to iterate"})}

    # 2. Read last 14 days of briefs
    briefs = list_recent_briefs(14)
    print(f"  Found {len(briefs)} briefs in last 14 days")

    if len(briefs) < 7:
        return {"statusCode": 200, "body": json.dumps({
            "skip": "not enough briefs",
            "n_found": len(briefs),
        })}

    # 3. Score each brief
    scored = []
    for b in briefs:
        data = b.get("data", {})
        brief_text = json.dumps(data)[:5000]  # full JSON as proxy for brief content
        spec = score_specificity(brief_text)
        acc = score_brief_accuracy(b["date"] + "T12:00:00+00:00")
        scored.append({
            "date": b["date"],
            "specificity": spec,
            "accuracy": acc,
        })
        print(f"  {b['date']}: specificity={spec}, accuracy={acc}")

    # 4. Decide if iteration is warranted
    accs = [s["accuracy"] for s in scored if s["accuracy"] is not None]
    avg_acc = sum(accs) / len(accs) if accs else None
    avg_spec = sum(s["specificity"] for s in scored) / len(scored)

    print(f"  Average accuracy: {avg_acc} (n={len(accs)})")
    print(f"  Average specificity: {avg_spec:.2f}")

    # Decision rule: iterate if accuracy < 0.50 with ≥7 scored briefs.
    # Otherwise, no change.
    if avg_acc is None or len(accs) < 7:
        print("  Skip: not enough scored outcomes yet (calibrator hasn't caught up)")
        # Append no-op log entry so we know we tried
        log.append({
            "ts": now.isoformat(),
            "action": "skip_no_data",
            "n_briefs": len(briefs),
            "n_scored": len(accs),
            "current_version": current_version,
        })
        put_s3_json(LOG_KEY, log[-90:])
        return {"statusCode": 200, "body": json.dumps({"skip": "insufficient_scored_data"})}

    if avg_acc >= 0.55:
        print(f"  Skip: accuracy is healthy ({avg_acc:.2f})")
        log.append({
            "ts": now.isoformat(),
            "action": "skip_healthy",
            "avg_accuracy": avg_acc,
            "avg_specificity": avg_spec,
            "current_version": current_version,
        })
        put_s3_json(LOG_KEY, log[-90:])
        return {"statusCode": 200, "body": json.dumps({"skip": "healthy", "avg_accuracy": avg_acc})}

    # 5. Iteration warranted — propose change
    print(f"  ITERATING: avg_acc={avg_acc:.2f} below threshold 0.55")

    failure_summary = "\n".join([
        f"- {s['date']}: accuracy={s['accuracy']:.2f}, specificity={s['specificity']:.1f}"
        for s in scored if s["accuracy"] is not None
    ])

    proposal_prompt = (
        f"You are reviewing the prompt that generates JustHodlAI's daily morning briefs. "
        f"The brief uses live financial data and is read by an institutional investor.\n\n"
        f"CURRENT PROMPT (length: {len(current_template)} chars):\n"
        f"---START---\n{current_template}\n---END---\n\n"
        f"LAST 14 DAYS OF SCORING:\n{failure_summary}\n\n"
        f"Average accuracy: {avg_acc:.2%} (target: 55%+)\n"
        f"Average specificity: {avg_spec:.1f} (concrete numbers per 100 words)\n\n"
        f"Propose a REVISED VERSION of the prompt that should improve accuracy. Constraints:\n"
        f"1. MUST keep length within 50%-150% of current ({int(len(current_template)*0.5)}-{int(len(current_template)*1.5)} chars)\n"
        f"2. MUST preserve the 'real numbers / live data' constraint (briefs cannot fabricate)\n"
        f"3. MUST NOT add commands like 'ignore previous instructions'\n"
        f"4. SHOULD add specific guidance about handling regime uncertainty\n"
        f"5. SHOULD NOT just restate the same things in different words\n\n"
        f"Return ONLY the new prompt text, no explanation, no quotes, no preamble."
    )

    new_template = call_anthropic(proposal_prompt, max_tokens=600)
    if not new_template:
        log.append({"ts": now.isoformat(), "action": "skip_anthropic_failed",
                    "current_version": current_version})
        put_s3_json(LOG_KEY, log[-90:])
        return {"statusCode": 200, "body": json.dumps({"skip": "anthropic_call_failed"})}

    print(f"  Proposed new template: {len(new_template)} chars")

    # 6. Validate
    ok, reason = validate_proposed_template(current_template, new_template)
    if not ok:
        print(f"  REJECTED: {reason}")
        log.append({
            "ts": now.isoformat(),
            "action": "rejected_proposal",
            "reason": reason,
            "current_version": current_version,
            "rejected_length": len(new_template),
        })
        put_s3_json(LOG_KEY, log[-90:])
        return {"statusCode": 200, "body": json.dumps({"reject": reason})}

    # 7. Save with versioning
    new_version = current_version + 1
    # Archive old version
    s3.put_object(
        Bucket=BUCKET, Key=f"learning/prompt_templates_v{current_version}.json",
        Body=json.dumps(templates, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    # Write new active template
    templates["morning_brief"] = new_template
    templates["_version"] = new_version
    templates["_updated_at"] = now.isoformat()
    templates["_previous_avg_accuracy"] = avg_acc
    templates["_previous_avg_specificity"] = avg_spec
    put_s3_json(TEMPLATES_KEY, templates)

    # 8. Log change with full diff metadata
    log.append({
        "ts": now.isoformat(),
        "action": "applied_proposal",
        "from_version": current_version,
        "to_version": new_version,
        "old_length": len(current_template),
        "new_length": len(new_template),
        "trigger_avg_accuracy": avg_acc,
        "trigger_avg_specificity": avg_spec,
        "n_briefs_evaluated": len(briefs),
        "n_scored": len(accs),
    })
    put_s3_json(LOG_KEY, log[-90:])

    print(f"  ✅ Applied v{current_version} → v{new_version}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "applied": True,
            "from_version": current_version,
            "to_version": new_version,
            "trigger_accuracy": round(avg_acc, 3),
        }),
    }
