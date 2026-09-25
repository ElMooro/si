"""Accept exact native packages, source replay, mirror, pages and preserved schedules.

Only the two reviewed public-market producers may be invoked, once per durable
identity. No consumer, account, paid AI, notification or portfolio operation.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json, re, subprocess, sys, time, urllib.request
import boto3
from botocore.config import Config
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/shared', 'aws/ops', 'aws/ops/staged', 'aws/ops/checks', 'scripts')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from market_runtime_evidence import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
from ops_5966_sector_tilt_runtime_diagnosis import parse_runtime
from shared_dependents import dependents
import ops_6050_short_volume_schedule_baseline as schedules
import short_volume_research_model as model
import short_volume_research_store as store
import short_volume_producer as producer
import short_volume_context as boundary
import short_volume_evidence as evidence
BUCKET = 'justhodl-dashboard-live'
FUNCTIONS = ('justhodl-finra-short', 'justhodl-short-pressure')
PROOF = 'data/short-volume-research-verification.json'
BASELINE = {'key': model.PRIVATE + '494c3166776891a534b1bf020969c32a2986b71ce582f4ff5ad9582d2d360faf.bin',
            'sha256': '494c3166776891a534b1bf020969c32a2986b71ce582f4ff5ad9582d2d360faf', 'bytes': 5922}
SCHEDULES = {'key': model.PRIVATE + 'f44d5935f9f40108ad046342cf465c6e9a458b762cff8971954b9f161a1332b5.bin',
             'sha256': 'f44d5935f9f40108ad046342cf465c6e9a458b762cff8971954b9f161a1332b5', 'bytes': 1489}
CANDIDATE = {'manifest_key': model.PREFIX + 'runs/230999e8c666bef9059abaa8127f90c1b9d8dabfa847887f8400adfdd0b677ca.json',
             'output_sha256': '2107c3c98214886b818ce01056ac13f97501112e38c0a3dac5654b1ca61eb4f6'}
ASSETS = ('short/index.html', 'short-pressure.html', 'short-volume-research.html', 'jh-short-volume-research.js',
          'jh-short-volume-page.js', 'jh-option-research.css', 'market-evidence.html', 'chart.html', 'chart-pro.html',
          'jh-chart-instvol.js', 'why.html', 'squeeze.html')
SHARED = ('aws/shared/short_volume_context.py', 'aws/shared/short_volume_producer.py',
          'aws/shared/short_volume_reference.py', 'aws/shared/holdings_derived_boundary.py')


def public(key, method='GET', byte_range=None):
    headers = {'User-Agent': 'justhodl-verify-release/1.0', 'Cache-Control': 'no-cache'}
    if byte_range:
        headers['Range'] = byte_range
    response = urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/' + key, method=method, headers=headers), timeout=40)
    status, headers = response.status, {k.lower(): v for k, v in response.headers.items()}
    return store.bounded(response), headers, status


def packet(s3, key):
    return model.strict(producer.raw_read(s3, BUCKET, key))


def completed_request(s3, current, function):
    items = []
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=model.PRIVATE + 'requests/'):
        items.extend(page.get('Contents', []))
        assert len(items) <= 4000, 'Bounded native request discovery'
    prefix = 'chatgpt-short-volume-native-' if function == FUNCTIONS[0] else 'chatgpt-short-volume-reference-'
    scheduled = 'scheduled-short-volume:' if function == FUNCTIONS[0] else 'scheduled-short-volume-reference:'
    for item in sorted(items, key=lambda v: v['LastModified'], reverse=True)[:300]:
        value = packet(s3, item['Key'])
        result = value.get('result', {})
        request = value.get('request_id', '')
        if (request.startswith((prefix, scheduled)) and value.get('status') == 'complete'
                and result.get('published') is True and result.get('replay') == current['replay']):
            assert value.get('execution_id')
            return {'status_key': item['Key'], 'status': value, 'invoke_sent': False, 'adopted_completed_native_request': True}
    raise AssertionError('Qualified head has no matching completed native request; inspect before invoking')


def invoke(lam, s3, commit, function, key):
    current = packet(s3, key)
    if boundary.context(current)['native_reference_available']:
        if key == model.CURRENT or producer.raw_read(s3, BUCKET, key) == producer.raw_read(s3, BUCKET, model.CURRENT):
            return completed_request(s3, current, function)
    label = 'native' if function == FUNCTIONS[0] else 'reference'
    request = 'chatgpt-short-volume-' + label + '-' + commit[:12] + '-1'
    status_key, dispatch_key = producer.request_key(request), producer.request_key(request + '-dispatch')
    claim = {'contract': 'short-volume-native-dispatch.v1', 'request_id': request, 'function': function,
             'started_at': producer.now(), 'status': 'claimed'}
    sent = False
    try:
        producer.journal(s3, BUCKET, dispatch_key, claim, True)
    except Exception as exc:
        if not store.conflict(exc):
            raise
        prior = packet(s3, dispatch_key)
        assert prior['contract'] == claim['contract'] and prior['request_id'] == request and prior['function'] == function
    else:
        response, rejected = invoke_when_available(lam, {'FunctionName': function, 'InvocationType': 'Event',
                                                        'Payload': model.encoded({'request_id': request})}, wait_seconds=45)
        assert response['StatusCode'] == 202
        sent = True
        claim.update(status='accepted_async', throttle_rejections_before_acceptance=rejected)
        producer.journal(s3, BUCKET, dispatch_key, claim)
    status = None
    deadline = time.monotonic() + 330
    while time.monotonic() < deadline:
        try:
            status = packet(s3, status_key)
        except Exception as exc:
            if not producer.missing(exc):
                raise
        if status and status.get('status') in ('complete', 'failed'):
            break
        time.sleep(3)
    assert status and status['status'] == 'complete', 'Inspect retained attempt; never blindly reinvoke'
    assert status['result']['published'] is True, status['result'].get('reason')
    assert packet(s3, key)['replay'] == status['result']['replay'], 'Publication advanced; inspect completed request'
    return {'request_id': request, 'status_key': status_key, 'dispatch_key': dispatch_key, 'status': status, 'invoke_sent': sent}


def profile(logs, function, status):
    execution = status['execution_id']
    assert re.fullmatch('[a-f0-9-]{36}', execution)
    start = model.clock(status['result']['generated_at'])
    deadline = time.monotonic() + 45
    while True:
        rows = logs.filter_log_events(logGroupName='/aws/lambda/' + function, filterPattern='"' + execution + '"',
                                     startTime=int(start.timestamp() * 1000) - 350000, limit=50).get('events', [])
        values = [parse_runtime(row.get('message', ''), execution) for row in rows]
        values = [v for v in values if v]
        if values:
            break
        assert time.monotonic() < deadline, 'Read completed execution report; do not reinvoke'
        time.sleep(3)
    allocation = 1024 if function == FUNCTIONS[0] else 512
    assert all(v.get('status') not in ('error', 'timeout') and v.get('memory_mb') == allocation
               and v.get('max_memory_mb', allocation) < allocation and v.get('duration_ms', 300000) < 300000 for v in values)
    return {'execution_id': execution, 'managed_reports': values}


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(connect_timeout=10, read_timeout=60, retries={'max_attempts': 0}))
    events, scheduler = boto3.client('events', region_name='us-east-1'), boto3.client('scheduler', region_name='us-east-1')
    read = store.reader(s3, BUCKET)
    with report('ops_6052_short_volume_native_acceptance') as r:
        subprocess.run([sys.executable, str(ROOT / 'aws/lambdas/justhodl-finra-short/tests/run_tests.py')], cwd=ROOT, check=True)
        commit = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/justhodl-finra-short/source',
                                         'aws/lambdas/justhodl-finra-short/config.json'], cwd=ROOT, text=True).strip()
        assert re.fullmatch('[a-f0-9]{40}', commit)
        baseline, schedule_baseline = [model.strict(model.original(ref, read)) for ref in (BASELINE, SCHEDULES)]
        actual = {}
        for name in dependents(ROOT, SHARED):
            actual[name] = runtime(lam, s3, events, scheduler, name)
            assert actual[name]['receipt'] == {'status': 'matched', 'commit': commit}, name
        for name in FUNCTIONS:
            for field in ('timeout', 'memory_mb', 'runtime', 'handler', 'architectures', 'role', 'ephemeral_storage_mb'):
                assert actual[name][field] == baseline['runtime'][name][field], (name, field)
        arns = {name: lam.get_function_configuration(FunctionName=name)['FunctionArn'] for name in FUNCTIONS}
        schedule_now = schedules.scan(scheduler, events, arns)
        for field in ('schedules', 'classic_default_bus_rules'):
            assert schedule_now[field] == schedule_baseline[field], field
        store.verified_run(CANDIDATE, read)
        build = model.strict(public('build-manifest.json')[0])
        pages_commit = build['commit_sha']
        subprocess.run(['git', 'merge-base', '--is-ancestor', commit, pages_commit], cwd=ROOT, check=True)
        subprocess.run(['git', 'diff', '--quiet', 'HEAD', pages_commit, '--', *ASSETS], cwd=ROOT, check=True)
        for name in ASSETS:
            raw = public(name)[0]
            clean, n = re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?', b'', raw)
            assert n <= 1 and model.sha(clean) == build['files_sha256'][name], name
            if name in ('short/index.html', 'short-pressure.html', 'short-volume-research.html'):
                assert b'/jh-short-volume-research.js' in raw and b'/jh-short-volume-page.js' in raw
        r.kv(commit=commit, runtime_packages=actual, schedules=schedule_now, pages_commit=pages_commit)
        primary = invoke(lam, s3, commit, FUNCTIONS[0], model.CURRENT)
        r.kv(primary_request=primary)
        mirror = invoke(lam, s3, commit, FUNCTIONS[1], boundary.ALIAS)
        r.kv(mirror_request=mirror)
        logs = boto3.client('logs', region_name='us-east-1')
        profiles = {name: profile(logs, name, item['status']) for name, item in zip(FUNCTIONS, (primary, mirror))}
        raw = public(model.CURRENT)[0]
        current = model.strict(raw)
        assert raw == producer.raw_read(s3, BUCKET, model.CURRENT) == producer.raw_read(s3, BUCKET, boundary.ALIAS) == public(boundary.ALIAS)[0]
        assert current['replay'] == primary['status']['result']['replay'] == mirror['status']['result']['replay']
        compiled = store.replay(current['replay'], read)
        assert compiled['packet'] == {k: v for k, v in current.items() if k != 'replay'}
        run = store.verified_run(current['replay'], read)
        inputs = store.checked(run['input'], 'inputs', read)
        qualification = evidence.qualify(inputs, compiled, lambda ref: model.original(ref, read))
        keys = {current['replay']['manifest_key'], run['input']['key'], run['output']['key'],
                *(v['key'] for v in run['compilers'].values()), *(v['key'] for v in current['record_shards'].values())}
        def check(key):
            assert public(key)[0] == store.reader(s3, BUCKET)(key), key
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(check, sorted(keys)):
                pass
        for key in (model.CURRENT, boundary.ALIAS):
            assert s3.head_object(Bucket=BUCKET, Key=key)['CacheControl'] == 'no-store'
            for method in ('GET', 'HEAD'):
                assert 'no-store' in public(key, method)[1].get('cache-control', '')
            ranged, headers, code = public(key, byte_range='bytes=0-99')
            assert code == 206 and ranged == raw[:100] and 'no-store' in headers.get('cache-control', '')
        protected = {BASELINE['key'], SCHEDULES['key'], inputs['predecessor']['key'],
                     *(v['original']['key'] for v in inputs['captures'].values())}
        for request in (primary, mirror):
            protected.add(request['status_key'])
            if request.get('dispatch_key'):
                protected.add(request['dispatch_key'])
            if request['status'].get('predecessor'):
                protected.add(request['status']['predecessor']['key'])
        def deny(key):
            assert denied_with_retry('https://justhodl.ai/' + key) and denied_with_retry('https://' + BUCKET + '.s3.amazonaws.com/' + key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny, sorted(protected)):
                pass
        for name in FUNCTIONS:
            assert runtime(lam, s3, events, scheduler, name) == actual[name]
        final_schedules = schedules.scan(scheduler, events, arns)
        for field in ('schedules', 'classic_default_bus_rules'):
            assert final_schedules[field] == schedule_now[field]
        proof = {'contract': 'short-volume-native-acceptance.v1', 'generated_at': producer.now(), 'commit': commit,
                 'runtime_packages': actual, 'schedules': final_schedules, 'execution_profiles': profiles,
                 'qualified_candidate': CANDIDATE, 'primary_request': primary, 'mirror_request': mirror,
                 'publication': {'key': model.CURRENT, 'mirror_key': boundary.ALIAS, 'sha256': model.sha(raw),
                                 'bytes': len(raw), 'replay': current['replay'], 'generated_at': current['generated_at']},
                 'counts': current['counts'], 'qualification': qualification,
                 'source_original_replay_matches': True, 'mirror_bytes_equal_canonical': True,
                 'complete_predecessors_retained': True, 'public_artifacts_checked': len(keys),
                 'protected_artifacts_checked': len(protected), 'originals_anonymously_denied': True,
                 'pages_commit': pages_commit, 'assets': {name: build['files_sha256'][name] for name in ASSETS},
                 'producer_invocations_this_acceptance': sum(int(v['invoke_sent']) for v in (primary, mirror)),
                 'acceptance_provider_requests': 0, 'producer_provider_requests': len(inputs['captures']) if primary['invoke_sent'] else 0,
                 'consumer_invocations': 0, 'private_account_reads': 0, 'paid_ai_calls': 0, 'notifications_sent': 0,
                 'portfolio_writes': 0, 'schedules_changed': 0}
        body = model.encoded(proof)
        s3.put_object(Bucket=BUCKET, Key=PROOF, Body=body, ContentType='application/json', CacheControl='no-store')
        assert public(PROOF)[0] == body
        r.kv(proof_key=PROOF, publication=proof['publication'], counts=current['counts'], qualification=qualification,
             execution_profiles=profiles, public_artifacts_checked=len(keys), protected_artifacts_checked=len(protected),
             runtime_packages_checked=len(actual), producer_invocations_this_acceptance=proof['producer_invocations_this_acceptance'],
             acceptance_provider_requests=0, producer_provider_requests=proof['producer_provider_requests'],
             consumer_invocations=0, private_account_reads=0, paid_ai_calls=0, notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
