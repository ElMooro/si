"""Bounded storage activity and named schedule monitor, not observation freshness.

The configured legacy artifacts are retained. No provider or application body is
read, no producer is invoked, and no scheduling operation is written. A complete
listing is not an atomic namespace snapshot. Controls/bindings were inspected in
read-only baseline 6446; the monitor itself had no observed schedule binding.
"""
import json,math,os,time
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION='us-east-1'
BUCKET=os.environ.get('S3_BUCKET','justhodl-dashboard-live')
OUT_KEY='data/feed-heartbeat.json'
CLIENT_CONFIG=Config(connect_timeout=2,read_timeout=3,retries={'total_max_attempts':1})
s3,events,scheduler=[boto3.client(service,region_name=REGION,config=CLIENT_CONFIG) for service in ('s3','events','scheduler')]

FEEDS = [('ticker-360', 'data/ticker-360.json', 720, False), ('short-interest', 'data/short-book.json', 1440, False), ('sec-8k', 'data/sec-filings-intel.json', 60, False), ('xbrl-fundamentals', 'data/xbrl-fundamentals/', 10080, True), ('corporate-actions', 'data/corporate-actions-index.json', 1440, False), ('etf-holdings', 'data/etf-issuer-holdings.json', 1440, False), ('13f-holdings', 'data/13f-by-ticker.json', 129600, False), ('insider-trading', 'data/insider-trades.json', 1440, False), ('earnings', 'data/earnings-tracker.json', 1440, False), ('dark-pool', 'data/dark-pool.json', 1440, False), ('cboe-options', 'data/cboe-options-chain.json', 60, False), ('macro-regime', 'data/cross-asset-regime.json', 1440, False), ('flow-confluence', 'data/flow-confluence.json', 1440, False), ('best-ideas', 'data/best-ideas.json', 1440, False), ('master-ranker', 'data/master-ranker.json', 60, False), ('conviction-engine', 'data/conviction.json', 60, False), ('options-confluence', 'data/options-confluence.json', 1440, False), ('schedules', None, 60, False), ('finra-research', 'data/short-interest.json', 1440, False), ('8k-enriched', 'data/8k-filings-enriched.json', 60, False), ('xbrl-index', 'data/xbrl-fundamentals-index.json', 10080, False)]
SCHEDULE_BINDINGS = [{'kind': 'EventBridge rule', 'name': 'justhodl-ticker-360-schedule', 'group': 'default', 'function': 'justhodl-ticker-360', 'target_arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-ticker-360', 'expression': 'cron(20 6,18 * * ? *)', 'timezone': 'UTC'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-sec-8k-enrich-schedule', 'group': 'default', 'function': 'justhodl-sec-8k-enrich', 'target_arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-sec-8k-enrich', 'expression': 'cron(10,40 * * * ? *)', 'timezone': 'UTC'}, {'kind': 'EventBridge rule', 'name': 'justhodl-sec-8k-enrich-schedule', 'group': 'default', 'function': 'justhodl-sec-8k-enrich', 'target_arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-sec-8k-enrich', 'expression': 'cron(10,40 * * * ? *)', 'timezone': 'UTC'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-short-interest-schedule', 'group': 'default', 'function': 'justhodl-short-interest', 'target_arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-short-interest', 'expression': 'cron(15 7 ? * * *)', 'timezone': 'UTC'}, {'kind': 'EventBridge rule', 'name': 'justhodl-short-interest-6h', 'group': 'default', 'function': 'justhodl-short-interest', 'target_arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-short-interest', 'expression': 'cron(20 12 * * ? *)', 'timezone': 'UTC'}, {'kind': 'EventBridge rule', 'name': 'justhodl-short-interest-sched', 'group': 'default', 'function': 'justhodl-short-interest', 'target_arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-short-interest', 'expression': 'cron(15 21 ? * MON,WED *)', 'timezone': 'UTC'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-xbrl-fundamentals-schedule', 'group': 'default', 'function': 'justhodl-xbrl-fundamentals', 'target_arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-xbrl-fundamentals', 'expression': 'cron(0 6 ? * SUN *)', 'timezone': 'UTC'}, {'kind': 'EventBridge rule', 'name': 'justhodl-xbrl-fundamentals-schedule', 'group': 'default', 'function': 'justhodl-xbrl-fundamentals', 'target_arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-xbrl-fundamentals', 'expression': 'cron(0 6 ? * SUN *)', 'timezone': 'UTC'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-corporate-actions-schedule', 'group': 'default', 'function': 'justhodl-corporate-actions', 'target_arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-corporate-actions', 'expression': 'cron(30 6 ? * * *)', 'timezone': 'UTC'}, {'kind': 'EventBridge rule', 'name': 'justhodl-corporate-actions-schedule', 'group': 'default', 'function': 'justhodl-corporate-actions', 'target_arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-corporate-actions', 'expression': 'cron(30 6 ? * * *)', 'timezone': 'UTC'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-etf-issuer-holdings-daily', 'group': 'default', 'function': 'justhodl-etf-issuer-holdings', 'target_arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-etf-issuer-holdings', 'expression': 'cron(0 4 * * ? *)', 'timezone': 'UTC'}, {'kind': 'EventBridge rule', 'name': 'justhodl-etf-issuer-holdings-daily', 'group': 'default', 'function': 'justhodl-etf-issuer-holdings', 'target_arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-etf-issuer-holdings', 'expression': 'cron(0 4 * * ? *)', 'timezone': 'UTC'}]
FEED_SOURCE_BINDINGS = {'ticker-360': {'function': 'justhodl-ticker-360', 'repository_source': 'aws/lambdas/justhodl-ticker-360/source/lambda_function.py', 'source_sha256': '149b41948a31749bddc361b78de8556a133e3cd1dccc1848e774f0c6f36817e6', 'artifact': 'data/ticker-360.json', 'basis': 'Reviewed repository output declaration; last actual writer unverified'}, 'short-interest': {'function': 'justhodl-short-book', 'repository_source': 'aws/lambdas/justhodl-short-book/source/lambda_function.py', 'source_sha256': '3ef785c82b34af1716a22c1f11c9938034bb622ae09a07cf6374fbf1b3d2d194', 'artifact': 'data/short-book.json', 'basis': 'Reviewed repository output declaration; last actual writer unverified'}, 'sec-8k': {'function': 'justhodl-sec-filings-intel', 'repository_source': 'aws/lambdas/justhodl-sec-filings-intel/source/lambda_function.py', 'source_sha256': 'd6cc1eae93e0442813096b6da2b23bdc4de15ef13681b22102444bbc2396e5ed', 'artifact': 'data/sec-filings-intel.json', 'basis': 'Reviewed repository output declaration; last actual writer unverified'}, 'xbrl-fundamentals': {'function': 'justhodl-xbrl-fundamentals', 'repository_source': 'aws/lambdas/justhodl-xbrl-fundamentals/source/lambda_function.py', 'source_sha256': 'af242550bfc04835c9e70957ee52b341400f015d9156df52435172124c61232a', 'artifact': 'data/xbrl-fundamentals/', 'basis': 'Reviewed repository output declaration; last actual writer unverified'}, 'corporate-actions': {'function': 'justhodl-corporate-actions', 'repository_source': 'aws/lambdas/justhodl-corporate-actions/source/lambda_function.py', 'source_sha256': '4d61c828f9a2b33528d06fbb2ce12379caf4fc2330d5bb47d5e877b282bc5e81', 'artifact': 'data/corporate-actions-index.json', 'basis': 'Reviewed repository output declaration; last actual writer unverified'}, 'etf-holdings': {'function': 'justhodl-etf-issuer-holdings', 'repository_source': 'aws/lambdas/justhodl-etf-issuer-holdings/source/lambda_function.py', 'source_sha256': '25f550f189e4b0d56b184db9c28863ca15a79a2dae348675c27b9e997eccea6d', 'artifact': 'data/etf-issuer-holdings.json', 'basis': 'Reviewed repository output declaration; last actual writer unverified'}, 'finra-research': {'function': 'justhodl-short-interest', 'repository_source': 'aws/lambdas/justhodl-short-interest/source/lambda_function.py', 'source_sha256': '570f68ab5ad828e20ef4c2ec74552df59add7d2a82cc91ff0d4480a8f7661d68', 'artifact': 'data/short-interest.json', 'basis': 'Reviewed repository output declaration; last actual writer unverified'}, '8k-enriched': {'function': 'justhodl-sec-8k-enrich', 'repository_source': 'aws/lambdas/justhodl-sec-8k-enrich/source/lambda_function.py', 'source_sha256': '4337d3be625be8e97c47b39470eb2de15cc73f4ef805aedebeffb70af29533d7', 'artifact': 'data/8k-filings-enriched.json', 'basis': 'Reviewed repository output declaration; last actual writer unverified'}, 'xbrl-index': {'function': 'justhodl-xbrl-fundamentals', 'repository_source': 'aws/lambdas/justhodl-xbrl-fundamentals/source/lambda_function.py', 'source_sha256': 'af242550bfc04835c9e70957ee52b341400f015d9156df52435172124c61232a', 'artifact': 'data/xbrl-fundamentals-index.json', 'basis': 'Reviewed repository output declaration; last actual writer unverified'}}

class BudgetExhausted(Exception):
    pass


class IncompleteInventory(Exception):
    pass


class Budget:
    def __init__(self, context):
        self.context, self.started = context, time.monotonic()

    def check(self):
        if time.monotonic() - self.started > 85:
            raise BudgetExhausted()
        method = getattr(self.context, 'get_remaining_time_in_millis', None)
        if method is not None:
            remaining = method()
            if type(remaining) not in (int, float) or not math.isfinite(remaining) or remaining < 20000:
                raise BudgetExhausted()


def utc_clock(value):
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError('Aware storage timestamp required')
    return value.astimezone(timezone.utc)


def error_code(exc):
    response = getattr(exc, 'response', None)
    error = response.get('Error') if isinstance(response, dict) else None
    code = error.get('Code') if isinstance(error, dict) else None
    return code if isinstance(code, str) else ''


def error_kind(exc):
    code = error_code(exc)
    if code in ('AccessDenied', 'AccessDeniedException', 'UnauthorizedOperation'):
        return 'access_denied'
    if isinstance(exc, BudgetExhausted):
        return 'time_budget_exhausted'
    if isinstance(exc, IncompleteInventory):
        return 'incomplete_inventory'
    if isinstance(exc, (ValueError, TypeError, KeyError)):
        return 'invalid_metadata'
    return 'metadata_request_failed'


def metadata_row(key, modified, size):
    if not isinstance(key, str) or not key or type(size) is not int or size < 0:
        raise ValueError('Complete typed object metadata required')
    return {'key': key, 'last_modified': utc_clock(modified).isoformat(), 'size_bytes': size}


def check_artifact(key, is_prefix, budget):
    """Storage metadata only; complete prefix population or explicit unknown."""
    rows, pages, seen, token, tokens = [], [], set(), None, set()
    result = {'exists': None, 'inventory_complete': False, 'inventory': rows,
              'inventory_pages': pages, 'observation_freshness_verified': False,
              'inventory_snapshot_consistency_verified': False}
    try:
        if not is_prefix:
            budget.check()
            response = s3.head_object(Bucket=BUCKET, Key=key)
            rows.append(metadata_row(key, response.get('LastModified'), response.get('ContentLength')))
        else:
            while True:
                budget.check()
                request = {'Bucket': BUCKET, 'Prefix': key, 'MaxKeys': 1000}
                if token is not None:
                    request['ContinuationToken'] = token
                response = s3.list_objects_v2(**request)
                contents = response.get('Contents', [])
                if (response.get('Name') != BUCKET or response.get('Prefix') != key
                        or type(response.get('IsTruncated')) is not bool
                        or not isinstance(contents, list) or response.get('CommonPrefixes')
                        or type(response.get('KeyCount')) is not int or response['KeyCount'] != len(contents)):
                    raise IncompleteInventory()
                page = {'members': len(contents), 'is_truncated': response['IsTruncated']}
                pages.append(page)
                for item in contents:
                    member = item.get('Key')
                    if not isinstance(member, str) or not member.startswith(key) or member in seen:
                        raise IncompleteInventory()
                    seen.add(member)
                    rows.append(metadata_row(member, item.get('LastModified'), item.get('Size')))
                    if len(rows) > 50000:
                        raise IncompleteInventory()
                if not response['IsTruncated']:
                    break
                token = response.get('NextContinuationToken')
                if not isinstance(token, str) or not token or token in tokens or len(pages) >= 128:
                    raise IncompleteInventory()
                tokens.add(token)
        result.update(exists=bool(rows), inventory_complete=True)
    except Exception as exc:
        code = error_code(exc)
        if not is_prefix and code in ('NoSuchKey', 'NotFound', '404'):
            result.update(exists=False, inventory_complete=True, reason='head_object_not_found')
        else:
            result['reason'] = error_kind(exc)
    result['checked_at'] = datetime.now(timezone.utc).isoformat()
    return result


def storage_assessment(check, expected_minutes):
    """Configured elapsed storage budget is not a release-calendar data SLA."""
    out = {'status': 'UNKNOWN', 'last_modified': None, 'age_minutes': None,
           'size_bytes': None, 'oldest_last_modified': None,
           'object_count': None, 'recent_storage_objects': None, 'stale_storage_objects': None,
           'storage_budget_minutes': expected_minutes * 2,
           'observation_freshness_verified': False, **check}
    if not check['inventory_complete']:
        return out
    if check['exists'] is False:
        out.update(status='MISSING', object_count=0, recent_storage_objects=0, stale_storage_objects=0)
        return out
    now = utc_clock(datetime.fromisoformat(check['checked_at']))
    rows = check['inventory']
    clocks = [utc_clock(datetime.fromisoformat(row['last_modified'])) for row in rows]
    if any(stamp > now for stamp in clocks):
        out.update(status='INVALID_METADATA', reason='future_storage_timestamp')
        return out
    latest, oldest = max(clocks), min(clocks)
    ages = [(now - stamp).total_seconds() / 60 for stamp in clocks]
    stale = sum(age > expected_minutes * 2 for age in ages)
    out.update(status='STALE_STORAGE' if stale else 'RECENT_STORAGE',
               last_modified=latest.isoformat(), oldest_last_modified=oldest.isoformat(),
               age_minutes=round((now - latest).total_seconds() / 60, 1),
               oldest_age_minutes=round((now - oldest).total_seconds() / 60, 1),
               size_bytes=sum(row['size_bytes'] for row in rows), object_count=len(rows),
               recent_storage_objects=len(rows)-stale, stale_storage_objects=stale)
    return out


def check_schedules(budget):
    """Inspect only reviewed real bindings; never infer names from a function."""
    checks = []
    for binding in SCHEDULE_BINDINGS:
        row = {**binding, 'status': 'UNKNOWN'}
        try:
            budget.check()
            if binding['kind'] not in ('EventBridge Scheduler', 'EventBridge rule'):
                raise ValueError('Reviewed schedule service required')
            if binding['kind'] == 'EventBridge Scheduler':
                result = scheduler.get_schedule(Name=binding['name'], GroupName=binding['group'])
                if result.get('Name') != binding['name'] or result.get('GroupName', 'default') != binding['group']:
                    raise ValueError('Named schedule response required')
                targets = [result.get('Target', {}).get('Arn')]
                state, expression = result.get('State'), result.get('ScheduleExpression')
                timezone_name = result.get('ScheduleExpressionTimezone')
            else:
                result = events.describe_rule(Name=binding['name'])
                if result.get('Name') != binding['name']:
                    raise ValueError('Named rule response required')
                targets, ids, token, tokens = [], set(), None, set()
                while True:
                    budget.check()
                    request = {'Rule': binding['name']}
                    if token is not None:
                        request['NextToken'] = token
                    page = events.list_targets_by_rule(**request)
                    if not isinstance(page.get('Targets'), list):
                        raise ValueError('Complete target metadata required')
                    for target in page['Targets']:
                        identifier, arn = target.get('Id'), target.get('Arn')
                        if not isinstance(identifier, str) or not identifier or identifier in ids or not isinstance(arn, str):
                            raise ValueError('Typed unique target required')
                        ids.add(identifier); targets.append(arn)
                    token = page.get('NextToken')
                    if not token:
                        break
                    if not isinstance(token, str) or token in tokens or len(tokens) >= 128:
                        raise IncompleteInventory()
                    tokens.add(token)
                state, expression = result.get('State'), result.get('ScheduleExpression')
                timezone_name = 'UTC'
            if state not in ('ENABLED', 'DISABLED'):
                raise ValueError('Typed enablement state required')
            row.update(observed_state=state, observed_expression=expression, observed_timezone=timezone_name,
                       matching_targets=sum(target == binding['target_arn'] for target in targets))
            if row['matching_targets'] != 1:
                row['status'] = 'MISBOUND'
            elif state != 'ENABLED':
                row['status'] = 'DISABLED'
            elif expression != binding['expression'] or timezone_name != binding['timezone']:
                row['status'] = 'DRIFT'
            else:
                row['status'] = 'BINDING_VERIFIED'
        except Exception as exc:
            code = error_code(exc)
            if code in ('ResourceNotFoundException', 'ResourceNotFound'):
                row.update(status='MISSING', reason='named_binding_not_found')
            else:
                row['reason'] = error_kind(exc)
        checks.append(row)
    states = {row['status'] for row in checks}
    status = ('CRITICAL' if states & {'MISSING', 'MISBOUND', 'DISABLED', 'DRIFT'} else
              'UNKNOWN' if not checks or states != {'BINDING_VERIFIED'} else 'BINDINGS_VERIFIED')
    return {'status': status, 'checks': checks, 'healthy': status == 'BINDINGS_VERIFIED',
            'scope': 'Reviewed named bindings only; no full fleet or delivery verification',
            'missing': [row['name'] for row in checks if row['status'] == 'MISSING'],
            'disabled': [row['name'] for row in checks if row['status'] == 'DISABLED'],
            'unknown': [row['name'] for row in checks if row['status'] == 'UNKNOWN'],
            'delivery_verified': False}


def lambda_handler(event, context):
    started = datetime.now(timezone.utc).isoformat()
    budget, feeds, alerts = Budget(context), {}, []
    for name, key, expected, prefix in FEEDS:
        if name == 'schedules':
            detail = check_schedules(budget)
            feeds[name] = {'artifact': 'reviewed-eventbridge-bindings', 'status': detail['status'],
                           'detail': detail, 'expected_interval_minutes': expected}
        else:
            feeds[name] = {'artifact': key, 'expected_interval_minutes': expected,
                           'freshness_basis': 'S3 object storage activity only',
                           **storage_assessment(check_artifact(key, prefix, budget), expected)}
            feeds[name]['declared_producer'] = FEED_SOURCE_BINDINGS.get(name)
            feeds[name]['last_writer_identity_verified'] = False
        status = feeds[name]['status']
        if status not in ('RECENT_STORAGE', 'BINDINGS_VERIFIED'):
            alerts.append({'feed': name, 'status': status,
                           'message': 'Storage or binding check needs review; source observation freshness is unverified.'})
    statuses = [value['status'] for value in feeds.values()]
    critical = statuses.count('CRITICAL')
    missing, stale = statuses.count('MISSING'), statuses.count('STALE_STORAGE')
    unknown = sum(status in ('UNKNOWN', 'INVALID_METADATA') for status in statuses)
    monitor = ('CRITICAL' if critical or missing > 2 else 'DEGRADED' if missing or stale else
               'UNKNOWN' if unknown or not statuses else 'HEALTHY')
    packet = {'contract': 'storage-heartbeat.v1', 'version': '1.1.0',
              'generated_at': datetime.now(timezone.utc).isoformat(), 'acquisition_started_at': started,
              'system_status': 'UNKNOWN', 'storage_monitor_status': monitor,
              'n_feeds': len(feeds), 'n_fresh': None, 'n_stale': None,
              'n_missing': missing, 'n_critical': critical, 'n_unknown': unknown,
              'n_unknown_storage_or_binding': unknown,
              'n_schedule_sections_verified': statuses.count('BINDINGS_VERIFIED'),
              'n_bindings_verified': sum(row['status']=='BINDING_VERIFIED' for row in feeds.get('schedules',{}).get('detail',{}).get('checks',[])),
              'n_observation_freshness_unknown': sum(name != 'schedules' for name in feeds),
              'n_recent_storage': statuses.count('RECENT_STORAGE'), 'n_stale_storage': stale,
              'feeds': feeds, 'alerts': alerts, 'observation_freshness_verified': False,
              'scope': 'Configured artifacts and reviewed named bindings only; not fleet coverage or investment-data validation.',
              'cadence_definition': 'Two times configured elapsed storage interval; not a market calendar or provider publication SLA.',
              'inventory_definition': 'Complete bounded listing response when marked complete; not an atomic namespace snapshot or proof of source-population completeness.',
              'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False}
    s3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(packet, indent=2, allow_nan=False).encode('utf-8'),
                  ContentType='application/json', CacheControl='max-age=300')
    print(json.dumps({'storage_monitor_status': monitor, 'n_feeds': len(feeds), 'alerts': len(alerts)}))
    return {'statusCode': 200, 'body': json.dumps({'system_status': 'UNKNOWN', 'storage_monitor_status': monitor,
                                                 'n_feeds': len(feeds), 'alerts': len(alerts)})}
