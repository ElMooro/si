"""
justhodl-fleet-error-monitor
============================
Runs every 5 minutes. For each Lambda in the account:
  1. Pull CloudWatch Invocations + Errors over last 15 min
  2. Compute error rate
  3. If rate > 5% AND invocations >= 5, emit alert
  4. Pull the latest ERROR log line via filter_log_events
  5. Send digest via Telegram + SNS

Also monitors:
  - DLQ depth (justhodl-dlq-default ApproximateNumberOfMessages > 0)
  - Throttle count on any Lambda

Output:
  - data/_fleet-monitor.json with the last run state
  - data/history/_fleet-monitor-history.jsonl (append-only)
  - Telegram alert if any alarms triggered
  - SNS publish to justhodl-fleet-alerts

Idempotent — alert dedupe within the same 1-hour window based on Lambda name + error type.
"""
import os, json, time, urllib.request, urllib.parse, traceback
import boto3
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

VERSION = "1.0.0"
REGION = os.environ.get('AWS_REGION', 'us-east-1')
ACCOUNT = '857687956942'
BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
SNS_ARN = os.environ.get('SNS_ARN', f'arn:aws:sns:{REGION}:{ACCOUNT}:justhodl-fleet-alerts')
DLQ_NAME = os.environ.get('DLQ_NAME', 'justhodl-dlq-default')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')

# Thresholds
ERROR_RATE_THRESHOLD = float(os.environ.get('ERROR_RATE_THRESHOLD', '5.0'))  # %
MIN_INVOCATIONS = int(os.environ.get('MIN_INVOCATIONS', '5'))
LOOKBACK_MINUTES = int(os.environ.get('LOOKBACK_MINUTES', '15'))
DLQ_DEPTH_THRESHOLD = int(os.environ.get('DLQ_DEPTH_THRESHOLD', '1'))
DEDUPE_WINDOW_MINUTES = int(os.environ.get('DEDUPE_WINDOW_MINUTES', '60'))

# Lambdas to exclude (known noisy or test-only)
EXCLUDE = set([
    'justhodl-fleet-error-monitor',  # don't alert on self
])

lam = boto3.client('lambda', region_name=REGION)
cw = boto3.client('cloudwatch', region_name=REGION)
logs = boto3.client('logs', region_name=REGION)
sqs = boto3.client('sqs', region_name=REGION)
sns = boto3.client('sns', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)


def fetch_cw_metric(metric_name, namespace, dim_value, dim_name='FunctionName',
                    period=900, lookback_min=LOOKBACK_MINUTES):
    """Get a metric sum over the lookback window."""
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=lookback_min)
    try:
        resp = cw.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            Dimensions=[{'Name': dim_name, 'Value': dim_value}],
            StartTime=start,
            EndTime=end,
            Period=period,
            Statistics=['Sum'],
        )
        return sum(p['Sum'] for p in resp.get('Datapoints', []))
    except Exception:
        return 0


def fetch_last_error_log(fn_name, lookback_min=LOOKBACK_MINUTES):
    """Pull the most recent ERROR log line for context."""
    end = int(time.time() * 1000)
    start = end - lookback_min * 60 * 1000
    try:
        resp = logs.filter_log_events(
            logGroupName=f'/aws/lambda/{fn_name}',
            startTime=start, endTime=end,
            filterPattern='?ERROR ?Exception ?Traceback',
            limit=3,
        )
        events = resp.get('events', [])
        if not events:
            return None
        # Return most recent
        return events[-1]['message'].strip()[:400]
    except Exception:
        return None


def check_lambda(fn_name):
    """Check one Lambda. Returns alert dict or None."""
    if fn_name in EXCLUDE:
        return None
    
    invocations = fetch_cw_metric('Invocations', 'AWS/Lambda', fn_name)
    errors = fetch_cw_metric('Errors', 'AWS/Lambda', fn_name)
    throttles = fetch_cw_metric('Throttles', 'AWS/Lambda', fn_name)
    
    if invocations < MIN_INVOCATIONS and throttles == 0:
        return None
    
    error_rate = (errors / invocations * 100) if invocations > 0 else 0
    
    if error_rate < ERROR_RATE_THRESHOLD and throttles == 0:
        return None
    
    # Alert! Fetch context.
    last_error = fetch_last_error_log(fn_name)
    return {
        'lambda': fn_name,
        'invocations': int(invocations),
        'errors': int(errors),
        'throttles': int(throttles),
        'error_rate_pct': round(error_rate, 1),
        'last_error_log': last_error,
        'severity': 'CRITICAL' if error_rate > 50 or throttles > 0 else 'WARNING',
    }


def check_dlq_depth():
    """Check DLQ depth — if any messages, something failed asynchronously."""
    try:
        url = sqs.get_queue_url(QueueName=DLQ_NAME)['QueueUrl']
        attrs = sqs.get_queue_attributes(
            QueueUrl=url,
            AttributeNames=['ApproximateNumberOfMessages',
                            'ApproximateNumberOfMessagesNotVisible'],
        )['Attributes']
        visible = int(attrs.get('ApproximateNumberOfMessages', 0))
        inflight = int(attrs.get('ApproximateNumberOfMessagesNotVisible', 0))
        total = visible + inflight
        return {'visible': visible, 'inflight': inflight, 'total': total}
    except Exception as e:
        return {'error': str(e)[:200]}


def load_alert_history():
    """Load recent alert history for dedupe."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key='data/_fleet-monitor-alert-history.json')
        return json.loads(obj['Body'].read().decode())
    except Exception:
        return {}


def save_alert_history(history):
    s3.put_object(
        Bucket=BUCKET,
        Key='data/_fleet-monitor-alert-history.json',
        Body=json.dumps(history, default=str).encode(),
        ContentType='application/json',
        CacheControl='no-store',
    )


def should_alert(alert, history):
    """Dedupe: only alert if last alert for this Lambda was > DEDUPE_WINDOW_MINUTES ago."""
    key = f"{alert['lambda']}:{alert['severity']}"
    last_ts_str = history.get(key)
    if not last_ts_str:
        return True
    try:
        last_ts = datetime.fromisoformat(last_ts_str.replace('Z', '+00:00'))
        elapsed = (datetime.now(timezone.utc) - last_ts).total_seconds() / 60
        return elapsed > DEDUPE_WINDOW_MINUTES
    except Exception:
        return True


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


def lambda_handler(event=None, context=None):
    started = time.time()
    run_id = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    print(f"[fleet-monitor] v{VERSION} run_id={run_id} starting")
    
    # 1. List all Lambdas
    all_lambdas = []
    paginator = lam.get_paginator('list_functions')
    for page in paginator.paginate():
        all_lambdas.extend(page['Functions'])
    print(f"[fleet-monitor] scanning {len(all_lambdas)} Lambdas")
    
    # 2. Check each Lambda in parallel
    alerts = []
    with ThreadPoolExecutor(max_workers=12) as ex:
        futures = {ex.submit(check_lambda, fn['FunctionName']): fn['FunctionName']
                   for fn in all_lambdas}
        for fut in as_completed(futures):
            try:
                result = fut.result()
                if result:
                    alerts.append(result)
            except Exception as e:
                print(f"[fleet-monitor] check error: {e}")
    
    # 3. Check DLQ depth
    dlq = check_dlq_depth()
    if dlq.get('total', 0) >= DLQ_DEPTH_THRESHOLD:
        alerts.append({
            'lambda': '<DLQ>',
            'severity': 'CRITICAL',
            'dlq_depth': dlq,
            'note': f"DLQ has {dlq.get('total')} messages — async failures detected",
        })
    
    # 4. Dedupe via history
    history = load_alert_history()
    new_alerts = [a for a in alerts if should_alert(a, history)]
    suppressed = len(alerts) - len(new_alerts)
    
    # 5. Send alerts
    sent_telegram = False
    sent_sns = False
    if new_alerts:
        # Build digest
        lines = [f"🚨 *FLEET MONITOR* — {len(new_alerts)} new alert(s)"]
        critical = [a for a in new_alerts if a.get('severity') == 'CRITICAL']
        warning = [a for a in new_alerts if a.get('severity') == 'WARNING']
        if critical:
            lines.append(f"\n*🔴 CRITICAL ({len(critical)})*")
            for a in critical[:5]:
                if 'dlq_depth' in a:
                    lines.append(f"• DLQ: {a['note']}")
                else:
                    lines.append(f"• `{a['lambda']}` — {a['errors']}/{a['invocations']} errors ({a['error_rate_pct']}%)")
                    if a.get('last_error_log'):
                        lines.append(f"  `{a['last_error_log'][:160]}`")
        if warning:
            lines.append(f"\n*🟡 WARNING ({len(warning)})*")
            for a in warning[:5]:
                lines.append(f"• `{a['lambda']}` — {a['errors']}/{a['invocations']} errors ({a['error_rate_pct']}%)")
        if suppressed:
            lines.append(f"\n_({suppressed} additional alerts suppressed by 60-min dedupe)_")
        lines.append(f"\n[CloudWatch](https://console.aws.amazon.com/cloudwatch/home?region={REGION}#metricsV2)")
        
        digest = "\n".join(lines)
        sent_telegram = send_telegram(digest)
        sent_sns = publish_sns(f"Fleet alert: {len(new_alerts)} issue(s)", digest)
        
        # Update dedupe history
        now_iso = datetime.now(timezone.utc).isoformat()
        for a in new_alerts:
            key = f"{a['lambda']}:{a.get('severity','WARNING')}"
            history[key] = now_iso
        save_alert_history(history)
    
    # 6. Write run state to S3
    state = {
        'version': VERSION,
        'run_id': run_id,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'n_lambdas_scanned': len(all_lambdas),
        'n_alerts_raised': len(new_alerts),
        'n_alerts_suppressed': suppressed,
        'dlq_status': dlq,
        'alerts': new_alerts,
        'elapsed_s': round(time.time() - started, 2),
        'thresholds': {
            'error_rate_pct': ERROR_RATE_THRESHOLD,
            'min_invocations': MIN_INVOCATIONS,
            'lookback_minutes': LOOKBACK_MINUTES,
            'dlq_depth': DLQ_DEPTH_THRESHOLD,
            'dedupe_minutes': DEDUPE_WINDOW_MINUTES,
        },
        'telegram_sent': sent_telegram,
        'sns_sent': sent_sns,
    }
    s3.put_object(
        Bucket=BUCKET,
        Key='data/_fleet-monitor.json',
        Body=json.dumps(state, default=str).encode(),
        ContentType='application/json',
        CacheControl='max-age=60, public',
    )
    
    print(f"[fleet-monitor] done — {len(new_alerts)} alerts, {suppressed} suppressed, {round(time.time()-started,1)}s")
    return {
        'statusCode': 200,
        'body': json.dumps({
            'n_scanned': len(all_lambdas),
            'n_alerts': len(new_alerts),
            'suppressed': suppressed,
            'elapsed_s': round(time.time() - started, 2),
        }),
    }


if __name__ == "__main__":
    import sys
    print(json.dumps(lambda_handler(), indent=2))
