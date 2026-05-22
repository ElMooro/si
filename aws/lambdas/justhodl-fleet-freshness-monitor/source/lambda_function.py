
import json, os, boto3, urllib.request, re
from datetime import datetime, timezone

REGION = "us-east-1"
BUCKET = os.environ.get("BUCKET", "justhodl-dashboard-live")
SNS_ARN = os.environ["SNS_ARN"]
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
DEFAULT_MAX_AGE_H = float(os.environ.get("DEFAULT_MAX_AGE_H", "26"))
MANIFEST_KEY = "data/_freshness-manifest.json"

s3 = boto3.client("s3", region_name=REGION)
sns = boto3.client("sns", region_name=REGION)

# Default manifest with known schedule cadences for critical feeds.
# Override these by uploading data/_freshness-manifest.json.
DEFAULT_MANIFEST = {
    "rules": [
        {"prefix": "data/", "default_max_age_h": 26.0},
    ],
    "exclude_prefixes": [
        "data/archive/", "data/_archive/", "data/snapshots/",
        "data/secretary-history/", "data/calibration-history/",
    ],
    "admin_only_keys": [
        "data/khalid-config.json", "data/ka-config.json",
    ],
    "key_overrides": {
        # Hourly
        "data/report.json": 2.0,
        # Every 5 min
        "data/options-flow.json": 0.2,
        # Weekly (Sunday)
        "data/factor-decomposition.json": 192.0,
        "data/cftc-deep-view.json": 192.0,
    },
}


def load_manifest():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        # Create default
        s3.put_object(
            Bucket=BUCKET, Key=MANIFEST_KEY,
            Body=json.dumps(DEFAULT_MANIFEST, indent=2).encode(),
            ContentType="application/json",
        )
        return DEFAULT_MANIFEST
    except Exception as e:
        print(f"manifest load failed: {e}")
        return DEFAULT_MANIFEST


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    manifest = load_manifest()
    
    exclude = manifest.get("exclude_prefixes", [])
    admin_only = set(manifest.get("admin_only_keys", []))
    overrides = manifest.get("key_overrides", {})
    default_max = float(manifest.get("rules", [{}])[0].get("default_max_age_h", DEFAULT_MAX_AGE_H))
    
    stale = []
    scanned = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix="data/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            scanned += 1
            if any(key.startswith(p) for p in exclude):
                continue
            if key in admin_only:
                continue
            age_h = (now - obj["LastModified"]).total_seconds() / 3600
            threshold = overrides.get(key, default_max)
            if age_h > threshold:
                stale.append({
                    "key": key, "age_h": round(age_h, 1),
                    "threshold_h": threshold,
                    "ratio": round(age_h / threshold, 2),
                    "size": obj["Size"],
                })
    
    stale.sort(key=lambda x: -x["ratio"])
    
    # Critical = >3x threshold
    critical = [s for s in stale if s["ratio"] >= 3.0]
    
    summary = {
        "checked_at": now.isoformat(),
        "n_keys_scanned": scanned,
        "n_stale_total": len(stale),
        "n_critical": len(critical),
        "stale_top_20": stale[:20],
        "thresholds_used": {"default_h": default_max, "n_overrides": len(overrides)},
    }
    
    # Alert if 5+ critical
    if len(critical) >= 5:
        lines = [f"\u26a0\ufe0f *JustHodl Freshness Alert*",
                 f"_{len(critical)} keys staler than 3x expected_", ""]
        for s in critical[:10]:
            lines.append(f"\u2022 `{s['key']}`: {s['age_h']}h (threshold {s['threshold_h']}h, {s['ratio']}x)")
        if len(critical) > 10:
            lines.append(f"...and {len(critical)-10} more")
        msg = "\n".join(lines)
        
        try:
            sns.publish(TopicArn=SNS_ARN, Subject=f"Freshness alert: {len(critical)} stale keys",
                        Message=msg)
        except Exception as e:
            summary["sns_error"] = str(e)[:200]
        
        if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
            try:
                url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
                data = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"}).encode()
                req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
                urllib.request.urlopen(req, timeout=10)
            except Exception as e:
                summary["telegram_error"] = str(e)[:200]
    
    # Save summary to S3 for the alarms.html or fleet-status page
    s3.put_object(
        Bucket=BUCKET,
        Key="data/_freshness-status.json",
        Body=json.dumps(summary, default=str, indent=2).encode(),
        ContentType="application/json",
        CacheControl="max-age=60",
    )
    return summary
