"""
justhodl-fleet-error-monitor
============================
Runs once a day (cadence floor, 2026-10-20; hourly since ops 6528; every 5 minutes before). For each Lambda in the account:
  1. Pull CloudWatch Errors + Throttles over the last hour (Invocations only when either is non-zero)
  2. Compute error rate
  3. If rate > 5% AND invocations >= 5, emit alert
  4. Pull the latest ERROR log line via filter_log_events
  5. Send digest via Telegram + SNS

Also monitors:
  - Dead letters (v1.3.0): what ARRIVED in justhodl-dlq-default since the last run, per function (CloudWatch
    AsyncEventsDropped), split into running engines (failures - alerted by name) and engines deliberately held at
    reserved concurrency 0 (refusals - reported, never alerted). The queue's depth is a 14-day stock, not a signal:
    on 2026-11-20 it read 3,255 and paged CRITICAL every day while 2,845 of those were bus events refused by the
    governor-held event-coordinator and the other ~400 were ~25 engines nobody was told the names of (ops 6856).
  - Throttle count on any Lambda

Output:
  - data/_fleet-monitor.json with the last run state
  - Telegram alert if any alarms triggered
  - SNS publish to justhodl-fleet-alerts

Idempotent — alert dedupe within the same 1-hour window based on Lambda name + error type.
"""
import os, json, time, urllib.request, urllib.parse, traceback
import boto3
from public_brain_projection import fleet_error_category, sanitize_public
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

VERSION = "1.3.0"
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
# Dead letters (v1.3.0)
DLQ_LOOKBACK_HOURS = int(os.environ.get('DLQ_LOOKBACK_HOURS', '25'))    # the monitor runs once a day (+1 h of overlap)
DLQ_CRITICAL_COUNT = int(os.environ.get('DLQ_CRITICAL_COUNT', '100'))   # dead letters from RUNNING engines in the window
DLQ_REALERT_HOURS = int(os.environ.get('DLQ_REALERT_HOURS', '168'))     # an engine already reported is repeated weekly
DLQ_ENGINES_KEY = '<DLQ>:engines'                                       # alert history: {function: last reported}

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
        return None


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


def is_paused(fn_name):
    """Reserved concurrency 0 = deliberately paused (a quarantine ledger or the spend governor): throttles are expected."""
    try:
        return lam.get_function_concurrency(FunctionName=fn_name).get('ReservedConcurrentExecutions') == 0
    except Exception:
        return False


def check_lambda(fn_name, include_log_context=True):
    """Check one Lambda. Returns alert dict or None.

    v1.2.0 (ops 6528): every GetMetricStatistics call is billed ($0.01 per 1,000) and the 5-minute cadence read
    3 metrics x ~924 functions about 288 times a day (~$6/day, most of the CloudWatch bill).  Errors and Throttles
    are read first; when both are zero no alert is possible, so Invocations is only read for the few functions
    that need an error rate.  Same alerts, one third fewer reads, at an hourly cadence."""
    if fn_name in EXCLUDE:
        return None

    errors = fetch_cw_metric('Errors', 'AWS/Lambda', fn_name)
    throttles = fetch_cw_metric('Throttles', 'AWS/Lambda', fn_name)
    invocations = 0
    if errors is not None and throttles is not None:
        if errors == 0 and throttles == 0:
            return None
        if errors == 0 and is_paused(fn_name):
            return None
        invocations = fetch_cw_metric('Invocations', 'AWS/Lambda', fn_name)
    if any(value is None for value in (invocations, errors, throttles)):
        return {'lambda': fn_name, 'severity': 'WARNING', 'metric_status': 'UNAVAILABLE',
                'error_category': 'METRIC_READ_UNAVAILABLE', 'invocations': invocations,
                'errors': errors, 'throttles': throttles, 'error_rate_pct': None}
    
    if invocations < MIN_INVOCATIONS and throttles == 0:
        return None
    
    error_rate = (errors / invocations * 100) if invocations > 0 else 0
    
    if error_rate < ERROR_RATE_THRESHOLD and throttles == 0:
        return None
    
    # Alert! Fetch context.
    last_error = fetch_last_error_log(fn_name) if include_log_context else None
    return {
        'lambda': fn_name,
        'invocations': int(invocations),
        'errors': int(errors),
        'throttles': int(throttles),
        'error_rate_pct': round(error_rate, 1),
        'last_error_log': last_error,
        'error_category': fleet_error_category(last_error, throttles=throttles, errors=errors),
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
        return {'available': True, 'visible': visible, 'inflight': inflight, 'total': total}
    except Exception:
        return {'available': False, 'error': 'DLQ_STATUS_UNAVAILABLE'}


def fetch_dead_letters(fn_names, hours=None):
    """{function: n} - the async events Lambda gave up on in the last `hours` (AsyncEventsDropped: every retry failed,
    the event aged out, or the function sits at reserved concurrency 0).  Each one is a message in the DLQ, and unlike
    the message the metric names the function.  One GetMetricData call per 500 functions (~$0.01 a run).
    None when CloudWatch cannot be read - the caller then falls back to the queue's depth."""
    hours = DLQ_LOOKBACK_HOURS if hours is None else hours
    names = sorted(n for n in fn_names if n not in EXCLUDE)
    out = {}
    try:
        end = datetime.now(timezone.utc)
        start = (end - timedelta(hours=hours)).replace(minute=0, second=0, microsecond=0)
        for i in range(0, len(names), 500):
            chunk = names[i:i + 500]
            queries = [{'Id': 'd%d' % j, 'ReturnData': True, 'MetricStat': {
                'Metric': {'Namespace': 'AWS/Lambda', 'MetricName': 'AsyncEventsDropped',
                           'Dimensions': [{'Name': 'FunctionName', 'Value': name}]},
                'Period': 3600, 'Stat': 'Sum'}} for j, name in enumerate(chunk)]
            token = None
            while True:
                kwargs = {'MetricDataQueries': queries, 'StartTime': start, 'EndTime': end}
                if token:
                    kwargs['NextToken'] = token
                resp = cw.get_metric_data(**kwargs)
                for result in resp.get('MetricDataResults', []):
                    total = sum(result.get('Values') or [])
                    if total > 0:
                        name = chunk[int(result['Id'][1:])]
                        out[name] = out.get(name, 0) + int(round(total))
                token = resp.get('NextToken')
                if not token:
                    break
        return out
    except Exception:
        return None


def split_dead_letters(dropped, paused):
    """(failing, held).  A running engine's dead letter is a failure.  An engine held at reserved concurrency 0 (the
    spend governor's CAP / FREEZE, a quarantine) refuses every event it is sent: those are expected for as long as
    the hold lasts and are reported, never alerted.  Pure."""
    failing = {fn: n for fn, n in dropped.items() if fn not in paused}
    held = {fn: n for fn, n in dropped.items() if fn in paused}
    return failing, held


def dead_letter_alert(failing, held, depth, history, now=None):
    """The run's one dead-letter alert, or None when no running engine dead-lettered anything (whatever the queue's
    depth).  It names the engines; `new` are those not reported in the last DLQ_REALERT_HOURS.  CRITICAL from
    DLQ_CRITICAL_COUNT dead letters in the window (a storm), WARNING otherwise.  Pure."""
    if not failing:
        return None
    now = now or datetime.now(timezone.utc)
    seen = history.get(DLQ_ENGINES_KEY) if isinstance(history.get(DLQ_ENGINES_KEY), dict) else {}

    def reported_recently(fn):
        try:
            last = datetime.fromisoformat(str(seen[fn]).replace('Z', '+00:00'))
            return (now - last).total_seconds() < DLQ_REALERT_HOURS * 3600
        except Exception:
            return False

    short = lambda fn: fn.replace('justhodl-', '', 1)   # noqa: E731
    new = sorted(fn for fn in failing if not reported_recently(fn))
    total, n_eng = sum(failing.values()), len(failing)
    ranked = sorted(failing.items(), key=lambda kv: (kv[0] not in new, -kv[1], kv[0]))   # not yet reported first
    mark = lambda fn: ' (new)' if fn in new and len(new) < n_eng else ''   # noqa: E731
    note = "%d dead letter%s in the last %d h from %d failing engine%s" % (
        total, '' if total == 1 else 's', DLQ_LOOKBACK_HOURS, n_eng, '' if n_eng == 1 else 's')
    if 0 < len(new) < n_eng:
        note += " (%d not reported before)" % len(new)
    note += ": " + ", ".join("`%s` %d%s" % (short(fn), n, mark(fn)) for fn, n in ranked[:15])
    if n_eng > 15:
        note += " +%d more" % (n_eng - 15)
    if held:
        note += ". Not failures: %d event%s refused by held engine%s at concurrency 0 (%s)" % (
            sum(held.values()), '' if sum(held.values()) == 1 else 's', '' if len(held) == 1 else 's',
            ", ".join("`%s` %d" % (short(fn), n) for fn, n in sorted(held.items(), key=lambda kv: (-kv[1], kv[0]))[:5]))
    if isinstance(depth, dict) and depth.get('available'):
        note += ". Queue holds %s (14-day stock)" % depth.get('total')
    return {'lambda': '<DLQ>', 'severity': 'CRITICAL' if total >= DLQ_CRITICAL_COUNT else 'WARNING',
            'dlq_depth': depth, 'note': note,
            'dead_letters': {'window_hours': DLQ_LOOKBACK_HOURS, 'failing': dict(failing), 'held': dict(held), 'new': new}}


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
    """Dedupe: only alert if last alert for this Lambda was > DEDUPE_WINDOW_MINUTES ago.

    Dead letters (v1.3.0): an engine not reported in the last DLQ_REALERT_HOURS is reported at once; a standing set of
    failing engines is repeated weekly instead of daily; a storm (CRITICAL) follows the ordinary window."""
    if 'dead_letters' in alert:
        if alert['dead_letters'].get('new'):
            return True
        if alert.get('severity') != 'CRITICAL':
            return False
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
    except Exception:
        print("[telegram] delivery unavailable")
        return False


def publish_sns(subject, msg):
    try:
        sns.publish(TopicArn=SNS_ARN, Subject=subject[:100], Message=msg)
        return True
    except Exception:
        print("[sns] delivery unavailable")
        return False


def lambda_handler(event=None, context=None):
    # A reviewed IAM invocation can refresh public metadata without sending or
    # consuming alert dedupe. This mode also avoids reading CloudWatch bodies.
    quiet = isinstance(event, dict) and event.get('mode') == 'audit_refresh'
    if quiet and ('requestContext' in event or 'headers' in event):
        return {'statusCode': 403, 'body': json.dumps({'error': 'iam_invocation_required'})}
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
        futures = {ex.submit(check_lambda, fn['FunctionName'], include_log_context=not quiet): fn['FunctionName']
                   for fn in all_lambdas}
        for fut in as_completed(futures):
            try:
                result = fut.result()
                if result:
                    alerts.append(result)
            except Exception:
                alerts.append({'lambda': futures[fut], 'severity': 'WARNING', 'metric_status': 'UNAVAILABLE',
                               'error_category': 'METRIC_READ_UNAVAILABLE', 'invocations': None,
                               'errors': None, 'throttles': None, 'error_rate_pct': None})
                print("[fleet-monitor] function check unavailable")
    
    # 3. Dead letters: what arrived since the last run and from whom (v1.3.0), not the queue's standing depth
    dlq = check_dlq_depth()
    history = load_alert_history()
    dropped = fetch_dead_letters([fn['FunctionName'] for fn in all_lambdas])
    dead_letters = None
    if dropped is None:
        # CloudWatch unreadable: keep the old depth rule so the check is never silently lost
        if dlq.get('total', 0) >= DLQ_DEPTH_THRESHOLD:
            alerts.append({
                'lambda': '<DLQ>',
                'severity': 'CRITICAL',
                'dlq_depth': dlq,
                'note': f"DLQ has {dlq.get('total')} messages — async failures detected (per-engine arrivals unavailable)",
            })
    else:
        failing, held = split_dead_letters(dropped, {fn for fn in dropped if is_paused(fn)})
        dead_letters = {'window_hours': DLQ_LOOKBACK_HOURS, 'failing': failing, 'held': held}
        print(f"[fleet-monitor] dead letters {DLQ_LOOKBACK_HOURS}h: {sum(failing.values())} from {len(failing)} failing engine(s), "
              f"{sum(held.values())} refused by {len(held)} held engine(s); queue depth {dlq.get('total')}")
        dlq_alert = dead_letter_alert(failing, held, dlq, history)
        if dlq_alert:
            alerts.append(dlq_alert)
    
    # 4. Dedupe via history
    new_alerts = [a for a in alerts if should_alert(a, history)]
    suppressed = len(alerts) - len(new_alerts)
    
    # 5. Send alerts
    sent_telegram = False
    sent_sns = False
    if new_alerts and not quiet:
        # Build digest
        lines = [f"🚨 *FLEET MONITOR* — {len(new_alerts)} new alert(s)"]
        new_alerts.sort(key=lambda a: 0 if 'dlq_depth' in a else 1)   # the dead-letter line is never cut by the [:5]
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
                if 'dlq_depth' in a:
                    lines.append(f"• DLQ: {a['note']}")
                else:
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
            if 'dead_letters' in a:   # every engine the digest just named starts its weekly repeat window
                seen = history.get(DLQ_ENGINES_KEY) if isinstance(history.get(DLQ_ENGINES_KEY), dict) else {}
                floor = (datetime.now(timezone.utc) - timedelta(hours=2 * DLQ_REALERT_HOURS)).isoformat()
                seen = {fn: ts for fn, ts in seen.items() if str(ts) >= floor}
                seen.update({fn: now_iso for fn in a['dead_letters']['failing']})
                history[DLQ_ENGINES_KEY] = seen
        save_alert_history(history)
    
    # 6. Write run state to S3
    state = {
        'version': VERSION,
        'run_id': run_id,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'n_lambdas_scanned': len(all_lambdas),
        'n_alerts_detected': len(alerts),
        'n_alerts_raised': len(new_alerts),
        'n_alerts_suppressed': suppressed,
        'dlq_status': dlq,
        'alerts': alerts,
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
        'notification_mode': 'suppressed_for_audit' if quiet else 'normal',
    }
    state = sanitize_public('data/_fleet-monitor.json', state)
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
            'dead_letters': dead_letters,
            'dlq_depth': dlq.get('total'),
            'elapsed_s': round(time.time() - started, 2),
        }),
    }


if __name__ == "__main__":
    import sys
    print(json.dumps(lambda_handler(), indent=2))
