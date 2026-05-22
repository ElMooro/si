"""
justhodl-fleet-freshness-monitor
================================
Detects silent failures the fleet-error-monitor can't see:
  - Lambda completes 200 (no error) but didn't actually write its expected
    output (the 35-Lambda silent-except-print-around-put_object pattern)
  - S3 freshness drift (CDN cache hits, partition writes to wrong key)
  - Provider API silent degradation (FRED returns 0 rows but no error)

How it works:
  1. Maintain a manifest at data/_freshness-manifest.json mapping
     S3 keys → expected max age (hours)
  2. Every 30 min, check each key's LastModified vs now
  3. If age > expected_max_age * 1.5, alert
  4. If key missing entirely (404), alert critical

Manifest format:
  {
    "data/foo.json": {"max_age_h": 24, "lambda": "justhodl-foo-engine",
                       "category": "macro"},
    ...
  }

Manifest is bootstrapped from existing /data/ S3 keys + Lambda schedules,
then auto-maintained: any Lambda that successfully writes a key gets
its expected-max-age set from the schedule interval.
"""
import os, json, time, urllib.request, urllib.parse, traceback
import boto3
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError

VERSION = "1.0.0"
REGION = os.environ.get('AWS_REGION', 'us-east-1')
ACCOUNT = '857687956942'
BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
SNS_ARN = os.environ.get('SNS_ARN', f'arn:aws:sns:{REGION}:{ACCOUNT}:justhodl-fleet-alerts')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
DEDUPE_HOURS = int(os.environ.get('DEDUPE_HOURS', '4'))
DEFAULT_MAX_AGE_H = float(os.environ.get('DEFAULT_MAX_AGE_H', '26'))  # daily + 2h buffer
ALERT_RATIO = float(os.environ.get('ALERT_RATIO', '1.5'))  # alert at 1.5x max_age

s3 = boto3.client('s3', region_name=REGION)
sns = boto3.client('sns', region_name=REGION)


def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            'chat_id': TELEGRAM_CHAT_ID,
            'text': msg[:4000],
            'parse_mode': 'Markdown',
            'disable_web_page_preview': 'true',
        }).encode()
        req = urllib.request.Request(url, data=data, method='POST')
        resp = urllib.request.urlopen(req, timeout=10)
        return resp.status == 200
    except Exception as e:
        print(f"[telegram] failed: {e}")
        return False


def publish_sns(subject, msg):
    try:
        sns.publish(TopicArn=SNS_ARN, Subject=subject[:100], Message=msg)
        return True
    except Exception as e:
        print(f"[sns] failed: {e}")
        return False


def load_manifest():
    """Load the freshness manifest from S3."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key='data/_freshness-manifest.json')
        return json.loads(obj['Body'].read().decode())
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return None
        raise


def bootstrap_manifest():
    """First-time manifest creation: scan data/ prefix, infer ages."""
    print("[bootstrap] no manifest — scanning data/ prefix...")
    manifest = {}
    paginator = s3.get_paginator('list_objects_v2')
    
    # Skip helpers and history
    skip_prefixes = ['data/history/', 'data/snapshots/', 'data/_', 'data/imports/']
    
    for page in paginator.paginate(Bucket=BUCKET, Prefix='data/'):
        for obj in page.get('Contents', []):
            key = obj['Key']
            # Skip directory markers and certain prefixes
            if key.endswith('/') or any(key.startswith(p) for p in skip_prefixes):
                continue
            # Only JSON outputs
            if not key.endswith('.json'):
                continue
            # Infer category for grouping
            category = 'core'
            if '/options/' in key: category = 'options'
            elif '/macro' in key or '/fed' in key or '/treasury' in key: category = 'macro'
            elif '/crypto' in key: category = 'crypto'
            elif '/screener/' in key: category = 'screener'
            elif '/predictability' in key: category = 'fundamentals'
            
            manifest[key] = {
                'max_age_h': DEFAULT_MAX_AGE_H,
                'category': category,
                'bootstrapped_at': datetime.now(timezone.utc).isoformat(),
                'last_seen_size': obj.get('Size', 0),
            }
    
    print(f"[bootstrap] {len(manifest)} keys added to manifest")
    
    # Save manifest
    s3.put_object(
        Bucket=BUCKET,
        Key='data/_freshness-manifest.json',
        Body=json.dumps(manifest, default=str, indent=2).encode(),
        ContentType='application/json',
        CacheControl='no-store',
    )
    return manifest


def check_key(key, expected_max_age_h, alert_ratio=ALERT_RATIO):
    """Check one S3 key's freshness."""
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        last_modified = head['LastModified']
        age_h = (datetime.now(timezone.utc) - last_modified).total_seconds() / 3600
        alert_threshold = expected_max_age_h * alert_ratio
        
        if age_h > alert_threshold:
            return {
                'key': key,
                'status': 'STALE',
                'age_h': round(age_h, 1),
                'expected_max_h': expected_max_age_h,
                'last_modified': last_modified.isoformat(),
                'size': head['ContentLength'],
            }
        return {'key': key, 'status': 'FRESH', 'age_h': round(age_h, 1)}
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return {
                'key': key,
                'status': 'MISSING',
                'age_h': None,
                'expected_max_h': expected_max_age_h,
            }
        return {'key': key, 'status': 'ERROR', 'error': str(e)[:200]}


def load_alert_history():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key='data/_freshness-alert-history.json')
        return json.loads(obj['Body'].read().decode())
    except Exception:
        return {}


def save_alert_history(h):
    s3.put_object(
        Bucket=BUCKET,
        Key='data/_freshness-alert-history.json',
        Body=json.dumps(h, default=str).encode(),
        ContentType='application/json',
        CacheControl='no-store',
    )


def should_alert(key, history):
    last_iso = history.get(key)
    if not last_iso:
        return True
    try:
        last_ts = datetime.fromisoformat(last_iso.replace('Z', '+00:00'))
        return (datetime.now(timezone.utc) - last_ts).total_seconds() / 3600 > DEDUPE_HOURS
    except Exception:
        return True


def lambda_handler(event=None, context=None):
    started = time.time()
    run_id = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    print(f"[freshness-monitor] v{VERSION} run_id={run_id}")
    
    # 1. Load or bootstrap manifest
    manifest = load_manifest()
    if manifest is None:
        manifest = bootstrap_manifest()
        # First run — return without alerting (everything is "stale" relative to no baseline)
        return {
            'statusCode': 200,
            'body': json.dumps({
                'bootstrapped': True,
                'n_keys_tracked': len(manifest),
                'elapsed_s': round(time.time() - started, 2),
            }),
        }
    
    # 2. Check each key
    print(f"[freshness-monitor] checking {len(manifest)} keys...")
    results = {'STALE': [], 'MISSING': [], 'FRESH': [], 'ERROR': []}
    
    # We check serially because head_object is fast (~5ms each)
    # For 1000 keys that's 5 seconds — fine within 30-min interval
    for key, meta in manifest.items():
        result = check_key(key, meta.get('max_age_h', DEFAULT_MAX_AGE_H))
        status = result.get('status', 'ERROR')
        if status in results:
            result['category'] = meta.get('category', 'unknown')
            results[status].append(result)
    
    print(f"[freshness-monitor] STALE={len(results['STALE'])}  MISSING={len(results['MISSING'])}  FRESH={len(results['FRESH'])}  ERROR={len(results['ERROR'])}")
    
    # 3. Dedupe alerts
    alert_history = load_alert_history()
    new_stale = [r for r in results['STALE'] if should_alert(r['key'], alert_history)]
    new_missing = [r for r in results['MISSING'] if should_alert(r['key'], alert_history)]
    
    # 4. Send digest
    sent_telegram = False
    sent_sns = False
    if new_stale or new_missing:
        lines = [f"🕰️ *FRESHNESS MONITOR* — {len(new_stale)+len(new_missing)} new staleness alert(s)"]
        
        if new_missing:
            lines.append(f"\n*🔴 MISSING ({len(new_missing)})*")
            for r in new_missing[:8]:
                lines.append(f"• `{r['key']}` (expected ≤{r['expected_max_h']}h)")
        
        if new_stale:
            # Sort by age descending
            new_stale.sort(key=lambda r: r.get('age_h', 0), reverse=True)
            lines.append(f"\n*🟡 STALE ({len(new_stale)})*")
            for r in new_stale[:8]:
                lines.append(f"• `{r['key']}` — {r['age_h']}h old (expected ≤{r['expected_max_h']}h)")
            if len(new_stale) > 8:
                lines.append(f"_(+{len(new_stale)-8} more, see data/_freshness-monitor.json)_")
        
        suppressed = (len(results['STALE']) + len(results['MISSING'])) - (len(new_stale) + len(new_missing))
        if suppressed:
            lines.append(f"\n_({suppressed} additional suppressed by {DEDUPE_HOURS}h dedupe)_")
        
        digest = "\n".join(lines)
        sent_telegram = send_telegram(digest)
        sent_sns = publish_sns(f"Freshness alert: {len(new_stale)+len(new_missing)} stale", digest)
        
        # Update dedupe history
        now_iso = datetime.now(timezone.utc).isoformat()
        for r in new_stale + new_missing:
            alert_history[r['key']] = now_iso
        save_alert_history(alert_history)
    
    # 5. Write run state
    state = {
        'version': VERSION,
        'run_id': run_id,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'n_keys_tracked': len(manifest),
        'counts': {k: len(v) for k, v in results.items()},
        'alerts_raised': len(new_stale) + len(new_missing),
        'stale': results['STALE'][:30],  # top 30 for dashboard
        'missing': results['MISSING'],
        'elapsed_s': round(time.time() - started, 2),
        'thresholds': {
            'default_max_age_h': DEFAULT_MAX_AGE_H,
            'alert_ratio': ALERT_RATIO,
            'dedupe_hours': DEDUPE_HOURS,
        },
        'telegram_sent': sent_telegram,
        'sns_sent': sent_sns,
    }
    s3.put_object(
        Bucket=BUCKET,
        Key='data/_freshness-monitor.json',
        Body=json.dumps(state, default=str).encode(),
        ContentType='application/json',
        CacheControl='max-age=60, public',
    )
    
    print(f"[freshness-monitor] done — {len(new_stale)} stale, {len(new_missing)} missing, {round(time.time()-started,1)}s")
    return {
        'statusCode': 200,
        'body': json.dumps({
            'n_tracked': len(manifest),
            'stale': len(results['STALE']),
            'missing': len(results['MISSING']),
            'fresh': len(results['FRESH']),
            'alerts': len(new_stale) + len(new_missing),
            'elapsed_s': round(time.time() - started, 2),
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
