"""
justhodl-fleet-freshness-monitor
================================
Detects silent failures the fleet-error-monitor can't see:
  - Lambda completes 200 (no error) but didn't actually write its expected
    output (the 35-Lambda silent-except-print-around-put_object pattern)
  - S3 freshness drift (CDN cache hits, partition writes to wrong key)
  - Provider API silent degradation (FRED returns 0 rows but no error)

Uses the EXISTING manifest schema at data/_freshness-manifest.json:
{
  "rules": [{"prefix": "data/", "default_max_age_h": 26.0}],
  "exclude_prefixes": ["data/archive/", "data/_archive/", ...],
  "admin_only_keys": ["data/khalid-config.json", ...],
  "key_overrides": {"data/options-flow.json": 0.2, ...}
}

Logic:
  - Walk every rule's prefix via list_objects_v2
  - Skip excluded prefixes + admin_only_keys
  - Lookup max_age_h from key_overrides first, else rule's default
  - head_object + compare LastModified
  - Alert if age > max_age_h * ALERT_RATIO

Output:
  data/_freshness-monitor.json with last run state
  Telegram + SNS alerts (deduped 4h per key)
"""
import os, json, time, urllib.request, urllib.parse
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed

VERSION = "1.1.0"
REGION = os.environ.get('AWS_REGION', 'us-east-1')
ACCOUNT = '857687956942'
BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
SNS_ARN = os.environ.get('SNS_ARN', f'arn:aws:sns:{REGION}:{ACCOUNT}:justhodl-fleet-alerts')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
DEDUPE_HOURS = int(os.environ.get('DEDUPE_HOURS', '4'))
DEFAULT_MAX_AGE_H = float(os.environ.get('DEFAULT_MAX_AGE_H', '26'))
ALERT_RATIO = float(os.environ.get('ALERT_RATIO', '1.5'))
MAX_KEYS_PER_RULE = int(os.environ.get('MAX_KEYS_PER_RULE', '10000'))

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
        urllib.request.urlopen(req, timeout=10)
        return True
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
    """Load the rules-based manifest from S3. Returns None if missing."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key='data/_freshness-manifest.json')
        return json.loads(obj['Body'].read().decode())
    except ClientError as e:
        if e.response['Error']['Code'] in ('NoSuchKey', '404'):
            return None
        raise


def is_excluded(key, manifest):
    """Check if a key should be skipped (excluded prefix or admin-only)."""
    excl_prefixes = manifest.get('exclude_prefixes', []) or []
    if any(key.startswith(p) for p in excl_prefixes):
        return True
    admin_only = set(manifest.get('admin_only_keys', []) or [])
    if key in admin_only:
        return True
    # Skip the monitor's own state files (would never go stale by themselves)
    self_keys = (
        'data/_freshness-manifest.json',
        'data/_freshness-monitor.json',
        'data/_freshness-alert-history.json',
        'data/_fleet-monitor.json',
        'data/_fleet-monitor-alert-history.json',
    )
    if key in self_keys:
        return True
    return False


def resolve_max_age(key, rule, manifest):
    """Lookup the max-age threshold for a key. Override > rule default."""
    overrides = manifest.get('key_overrides', {}) or {}
    if key in overrides:
        try:
            return float(overrides[key])
        except Exception:
            pass
    return float(rule.get('default_max_age_h', DEFAULT_MAX_AGE_H))


def list_keys_under_rule(rule):
    """Enumerate all keys under a rule's prefix (cap at MAX_KEYS_PER_RULE)."""
    prefix = rule.get('prefix', 'data/')
    keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get('Contents', []):
            keys.append(obj)
            if len(keys) >= MAX_KEYS_PER_RULE:
                return keys
    return keys


def evaluate_key(obj, rule, manifest):
    """Evaluate one S3 object's freshness."""
    key = obj['Key']
    if is_excluded(key, manifest):
        return None
    # Skip directory markers / non-JSON outputs
    if key.endswith('/'):
        return None
    if not key.endswith('.json'):
        return None
    
    max_age_h = resolve_max_age(key, rule, manifest)
    last_modified = obj['LastModified']
    age_h = (datetime.now(timezone.utc) - last_modified).total_seconds() / 3600
    alert_threshold = max_age_h * ALERT_RATIO
    
    result = {
        'key': key,
        'max_age_h': max_age_h,
        'age_h': round(age_h, 2),
        'last_modified': last_modified.isoformat(),
        'size': obj.get('Size', 0),
    }
    if age_h > alert_threshold:
        result['status'] = 'STALE'
    else:
        result['status'] = 'FRESH'
    return result


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
    
    manifest = load_manifest()
    if manifest is None:
        print("[freshness-monitor] manifest missing — cannot run")
        return {'statusCode': 500, 'body': json.dumps({'error': 'manifest missing'})}
    
    rules = manifest.get('rules', []) or []
    if not rules:
        return {'statusCode': 500, 'body': json.dumps({'error': 'no rules in manifest'})}
    print(f"[freshness-monitor] {len(rules)} rule(s) in manifest")
    
    # Walk each rule
    all_results = []
    for rule in rules:
        objs = list_keys_under_rule(rule)
        print(f"[freshness-monitor] rule prefix={rule.get('prefix')} → {len(objs)} objects")
        for obj in objs:
            r = evaluate_key(obj, rule, manifest)
            if r is not None:
                all_results.append(r)
    
    stale = [r for r in all_results if r.get('status') == 'STALE']
    fresh = [r for r in all_results if r.get('status') == 'FRESH']
    print(f"[freshness-monitor] tracked={len(all_results)}  stale={len(stale)}  fresh={len(fresh)}")
    
    # Dedupe alerts
    history = load_alert_history()
    new_alerts = [r for r in stale if should_alert(r['key'], history)]
    suppressed = len(stale) - len(new_alerts)
    
    # Send digest
    sent_telegram = False
    sent_sns = False
    if new_alerts:
        new_alerts.sort(key=lambda r: r['age_h'] / r['max_age_h'], reverse=True)
        lines = [f"🕰️ *FRESHNESS MONITOR* — {len(new_alerts)} new stale key(s)"]
        for r in new_alerts[:12]:
            ratio = r['age_h'] / r['max_age_h']
            severity = "🔴" if ratio > 3 else "🟡"
            lines.append(f"{severity} `{r['key']}`")
            lines.append(f"     {r['age_h']}h old (max {r['max_age_h']}h, ratio {ratio:.1f}×)")
        if len(new_alerts) > 12:
            lines.append(f"\n_+{len(new_alerts)-12} more, see data/_freshness-monitor.json_")
        if suppressed:
            lines.append(f"\n_({suppressed} suppressed by {DEDUPE_HOURS}h dedupe)_")
        digest = "\n".join(lines)
        sent_telegram = send_telegram(digest)
        sent_sns = publish_sns(f"Freshness: {len(new_alerts)} stale", digest)
        
        now_iso = datetime.now(timezone.utc).isoformat()
        for r in new_alerts:
            history[r['key']] = now_iso
        save_alert_history(history)
    
    # Run state
    # Sort stale by ratio (most-stale first) for the dashboard
    stale_sorted = sorted(stale, key=lambda r: r['age_h'] / r['max_age_h'], reverse=True)
    state = {
        'version': VERSION,
        'run_id': run_id,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'n_keys_tracked': len(all_results),
        'n_stale': len(stale),
        'n_fresh': len(fresh),
        'n_alerts_raised': len(new_alerts),
        'n_alerts_suppressed': suppressed,
        'stale_top_50': stale_sorted[:50],
        'elapsed_s': round(time.time() - started, 2),
        'thresholds': {
            'default_max_age_h': DEFAULT_MAX_AGE_H,
            'alert_ratio': ALERT_RATIO,
            'dedupe_hours': DEDUPE_HOURS,
        },
        'manifest_rules': rules,
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
    
    print(f"[freshness-monitor] done — {len(new_alerts)} alerts, {suppressed} suppressed, {round(time.time()-started,1)}s")
    return {
        'statusCode': 200,
        'body': json.dumps({
            'n_tracked': len(all_results),
            'stale': len(stale),
            'fresh': len(fresh),
            'alerts': len(new_alerts),
            'suppressed': suppressed,
            'elapsed_s': round(time.time() - started, 2),
        }),
    }


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
