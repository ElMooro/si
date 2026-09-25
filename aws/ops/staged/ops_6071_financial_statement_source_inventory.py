"""Retain complete current public accounting inputs before any native changes.

No provider request, producer invocation, account read, signal write or public
mutation. Field presence is inventoried, never treated as qualification.
"""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, json, re, subprocess, sys
import boto3
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/staged', 'aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import runtime, bounded
from ops_5998_option_population_retained_acceptance import denied_with_retry
from ops_6053_short_interest_source_baseline import bindings
from financial_statement_inventory import inventory
BUCKET = 'justhodl-dashboard-live'
PRIVATE = 'audit-private/20260909-originals/financial-statement-research/'
CURRENT = {'data/forensic-screen.json': 'forensic', 'data/share-flows.json': 'share_flows',
           'data/short-book.json': 'short_book', 'screener/data.json': 'universe'}
FUNCTIONS = ('justhodl-forensic-screen', 'justhodl-short-book')
REQUEST = 'chatgpt-financial-statement-baseline-6071'
sha = lambda body: hashlib.sha256(body).hexdigest()
STATUS = PRIVATE + 'requests/' + sha(REQUEST.encode()) + '.json'
encoded = lambda value: json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
now = lambda: datetime.now(timezone.utc).isoformat()


def read(s3, key):
    if key not in CURRENT and key != STATUS and not re.fullmatch(re.escape(PRIVATE) + r'[a-f0-9]{64}\.bin', key):
        raise ValueError('Reviewed accounting baseline path required')
    return bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body'], 32 * 1024 * 1024)


def protect(s3, body):
    if not isinstance(body, bytes) or not 0 < len(body) <= 32 * 1024 * 1024:
        raise ValueError('Whole bounded predecessor required')
    ref = {'key': PRIVATE + sha(body) + '.bin', 'sha256': sha(body), 'bytes': len(body)}
    try:
        s3.put_object(Bucket=BUCKET, Key=ref['key'], Body=body, ContentType='application/octet-stream', CacheControl='no-store', IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
            raise
    assert read(s3, ref['key']) == body
    return ref


def journal(s3, value, claim=False):
    body = encoded(value)
    s3.put_object(Bucket=BUCKET, Key=STATUS, Body=body, ContentType='application/json', CacheControl='no-store', **({'IfNoneMatch': '*'} if claim else {}))
    assert read(s3, STATUS) == body


def main():
    s3, lam = boto3.client('s3', region_name='us-east-1'), boto3.client('lambda', region_name='us-east-1')
    events, scheduler = boto3.client('events', region_name='us-east-1'), boto3.client('scheduler', region_name='us-east-1')
    with report('ops_6071_financial_statement_source_inventory') as r:
        subprocess.run([sys.executable, str(ROOT / 'tests/test_financial_statement_inventory.py')], cwd=ROOT, check=True)
        commit = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/ops/staged/ops_6071_financial_statement_source_inventory.py'], cwd=ROOT, text=True).strip()
        prior = {fn: runtime(lam, s3, events, scheduler, fn) for fn in FUNCTIONS}
        schedules = {fn: bindings(scheduler, events, lam.get_function_configuration(FunctionName=fn)['FunctionArn']) for fn in FUNCTIONS}
        progress = {'contract': 'financial-statement-baseline.v1', 'request_id': REQUEST, 'source_commit': commit,
            'generated_at': now(), 'status': 'claimed', 'captures': {}, 'repo_predecessors': {}}
        journal(s3, progress, True)
        try:
            for key, kind in CURRENT.items():
                response = s3.get_object(Bucket=BUCKET, Key=key)
                body = bounded(response['Body'], 32 * 1024 * 1024)
                captured = {'source_key': key, 'original': protect(s3, body), 'captured_at': now(),
                    'etag': response['ETag'], 'last_modified': response['LastModified'].isoformat(),
                    'version_id': response.get('VersionId'), 'cache_control': response.get('CacheControl')}
                progress['captures'][key] = captured
                journal(s3, progress)
                captured['inventory'] = inventory(json.loads(body), kind)
                journal(s3, progress)
            for fn in FUNCTIONS:
                for suffix in ('source/lambda_function.py', 'config.json'):
                    path = 'aws/lambdas/' + fn + '/' + suffix
                    progress['repo_predecessors'][path] = protect(s3, (ROOT / path).read_bytes())
            for path in ('short-book.html', 'forensic.html'):
                if (ROOT / path).is_file():
                    progress['repo_predecessors'][path] = protect(s3, (ROOT / path).read_bytes())
            progress.update(runtime=prior, bindings=schedules, status='complete', completed_at=now(),
                provider_requests=0, producer_invocations=0, consumer_invocations=0, private_account_reads=0,
                signal_writes=0, paid_ai_calls=0, public_writes=0, notifications_sent=0, schedules_changed=0,
                original_statement_responses_verified=False, forecast_qualified=False, sizing_qualified=False)
            manifest = protect(s3, encoded(progress))
            for fn in FUNCTIONS:
                assert runtime(lam, s3, events, scheduler, fn) == prior[fn]
                after = bindings(scheduler, events, lam.get_function_configuration(FunctionName=fn)['FunctionArn'])
                for field in ('schedules', 'classic_default_bus_rules'):
                    assert after[field] == schedules[fn][field]
            protected = {STATUS, manifest['key'], *(v['original']['key'] for v in progress['captures'].values()),
                         *(v['key'] for v in progress['repo_predecessors'].values())}
            for key in sorted(protected):
                assert denied_with_retry('https://justhodl.ai/' + key)
                assert denied_with_retry('https://' + BUCKET + '.s3.amazonaws.com/' + key)
            journal(s3, {**progress, 'manifest': manifest, 'protected_artifacts_checked': len(protected)})
            r.kv(source_commit=commit, manifest=manifest, captured_inputs=progress['captures'],
                repo_predecessors=progress['repo_predecessors'], runtime=prior, bindings=schedules,
                protected_artifacts_checked=len(protected), provider_requests=0, producer_invocations=0,
                consumer_invocations=0, private_account_reads=0, signal_writes=0, paid_ai_calls=0,
                public_writes=0, notifications_sent=0, schedules_changed=0,
                original_statement_responses_verified=False, forecast_qualified=False, sizing_qualified=False)
        except Exception as exc:
            journal(s3, {**progress, 'status': 'failed', 'error_type': type(exc).__name__})
            raise


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
