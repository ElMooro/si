"""Verify exact packages, invoke one public-source producer, replay and inspect.

No consumer invocation, account read, paid AI, notification or portfolio action.
An accepted or ambiguous execution is never blindly repeated.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import re, subprocess, sys, time, urllib.request
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
import ops_6057_squeeze_settlement_source_baseline as baseline_ops
import sec_ftd_research_model as model
import sec_ftd_research_store as store
import sec_ftd_producer as producer
import sec_ftd_context as boundary
import sec_ftd_evidence as evidence
BUCKET = 'justhodl-dashboard-live'
FUNCTION = 'justhodl-squeeze-fuel'
PROOF = 'data/sec-ftd-research-verification.json'
BASELINE = {'key': model.PRIVATE + '9be70adb544267c4e1cb8f8eee652736c61a44a9f555e8ec0e2a6a0298e24476.bin',
            'sha256': '9be70adb544267c4e1cb8f8eee652736c61a44a9f555e8ec0e2a6a0298e24476', 'bytes': 8951}
CANDIDATE = {'manifest_key': 'data/sec-ftd-research/runs/1ebc6c03f4f86b38a524aece0ccad7ca7b597617e22ad10b20c9683391b12f3c.json', 'output_sha256': '5e4ad495f184f8264e16d4969711ddc43c1d78b5276e5d90d14871112bec36e9'}
SHARED = ('aws/shared/sec_ftd_context.py', 'aws/shared/sec_ftd_producer.py')
ASSETS = ('squeeze-fuel.html', 'jh-sec-ftd-research.js', 'jh-sec-ftd-page.js', 'jh-sec-ftd-research.css',
          'jh-option-research.css', 'market-evidence.html')


def public(key, method='GET', byte_range=None):
    headers = {'User-Agent': 'justhodl-verify-release/1.0', 'Cache-Control': 'no-cache'}
    if byte_range:
        headers['Range'] = byte_range
    response = urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/' + key, method=method, headers=headers), timeout=40)
    status, headers = response.status, {k.lower(): v for k, v in response.headers.items()}
    return store.bounded(response), headers, status


def packet(s3, key):
    return model.strict(producer.raw_read(s3, BUCKET, key))


def invoke(lam, s3, commit):
    current = packet(s3, model.CURRENT)
    receipt = packet(s3, 'data/ops/releases/' + FUNCTION + '.json')
    if boundary.context(current)['native_reference_available'] and model.clock(current['generated_at']) >= model.clock(receipt['deployed_at']):
        read = store.reader(s3, BUCKET)
        run = store.verified_run(current['replay'], read)
        inputs = store.checked(run['input'], 'inputs', read)
        key = producer.request_key(inputs['request_id'])
        existing = packet(s3, key)
        assert existing['status'] == 'complete' and existing['execution_id'] == inputs['execution_id']
        assert existing['result']['published'] is True and existing['result']['replay'] == current['replay']
        return {'request_id': inputs['request_id'], 'status_key': key, 'status': existing, 'invoke_sent': False, 'adopted_completed_native_request': True}
    request = 'chatgpt-sec-ftd-native-' + commit[:12] + '-1'
    status_key, dispatch_key = producer.request_key(request), producer.request_key(request + '-dispatch')
    claim = {'contract': 'sec-ftd-native-dispatch.v1', 'request_id': request, 'function': FUNCTION, 'started_at': producer.now(), 'status': 'claimed'}
    sent = False
    try:
        producer.journal(s3, BUCKET, dispatch_key, claim, True)
    except Exception as exc:
        if not store.conflict(exc):
            raise
        prior = packet(s3, dispatch_key)
        assert prior['contract'] == claim['contract'] and prior['request_id'] == request and prior['function'] == FUNCTION
    else:
        response, rejected = invoke_when_available(lam, {'FunctionName': FUNCTION, 'InvocationType': 'Event', 'Payload': model.encoded({'request_id': request})}, wait_seconds=45)
        assert response['StatusCode'] == 202
        sent = True
        claim.update(status='accepted_async', throttle_rejections_before_acceptance=rejected)
        producer.journal(s3, BUCKET, dispatch_key, claim)
    status, deadline = None, time.monotonic() + 330
    while time.monotonic() < deadline:
        try:
            status = packet(s3, status_key)
        except Exception as exc:
            if not producer.missing(exc):
                raise
        if status and status.get('status') in ('complete', 'failed'):
            break
        time.sleep(3)
    assert status and status['status'] == 'complete', 'Inspect retained execution; never blindly reinvoke'
    assert status['result']['published'] is True, status['result'].get('reason')
    assert packet(s3, model.CURRENT)['replay'] == status['result']['replay']
    return {'request_id': request, 'status_key': status_key, 'dispatch_key': dispatch_key, 'status': status, 'invoke_sent': sent}


def profile(logs, status):
    execution = status['execution_id']
    assert re.fullmatch('[a-f0-9-]{36}', execution)
    start, deadline = model.clock(status['result']['generated_at']), time.monotonic() + 45
    while True:
        rows = logs.filter_log_events(logGroupName='/aws/lambda/' + FUNCTION, filterPattern='"' + execution + '"', startTime=int(start.timestamp() * 1000) - 350000, limit=50).get('events', [])
        values = [parse_runtime(row.get('message', ''), execution) for row in rows]
        values = [v for v in values if v]
        if values:
            break
        assert time.monotonic() < deadline, 'Read completed execution profile; do not reinvoke'
        time.sleep(3)
    assert all(v.get('status') not in ('error', 'timeout') and v.get('memory_mb') == 1024 and v.get('max_memory_mb', 1024) < 1024 and v.get('duration_ms', 300000) < 270000 for v in values)
    return {'execution_id': execution, 'managed_reports': values}


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(connect_timeout=10, read_timeout=60, retries={'max_attempts': 0}))
    events, scheduler = boto3.client('events', region_name='us-east-1'), boto3.client('scheduler', region_name='us-east-1')
    read = store.reader(s3, BUCKET)
    with report('ops_6070_sec_ftd_native_acceptance') as r:
        subprocess.run([sys.executable, str(ROOT / 'aws/lambdas/justhodl-squeeze-fuel/tests/run_tests.py')], cwd=ROOT, check=True)
        commit = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/justhodl-squeeze-fuel/source', 'aws/lambdas/justhodl-squeeze-fuel/config.json'], cwd=ROOT, text=True).strip()
        assert re.fullmatch('[a-f0-9]{40}', commit)
        baseline = model.strict(model.original(BASELINE, read))
        actual = {}
        for name in dependents(ROOT, SHARED):
            actual[name] = runtime(lam, s3, events, scheduler, name)
            assert actual[name]['receipt'] == {'status': 'matched', 'commit': commit}, name
        assert set(actual) == {'justhodl-squeeze-fuel', 'justhodl-ai-rerating-radar', 'justhodl-best-setups', 'justhodl-equity-confluence', 'justhodl-master-ranker', 'justhodl-signal-fabric', 'justhodl-short-book'}
        for field in ('timeout', 'memory_mb', 'runtime', 'handler', 'architectures', 'role', 'ephemeral_storage_mb'):
            assert actual[FUNCTION][field] == baseline['runtime'][field], field
        arn = lam.get_function_configuration(FunctionName=FUNCTION)['FunctionArn']
        schedules = baseline_ops.bindings(scheduler, events, arn)
        for field in ('schedules', 'classic_default_bus_rules'):
            assert schedules[field] == baseline['bindings'][field]
        store.verified_run(CANDIDATE, read)
        build = model.strict(public('build-manifest.json')[0]); pages_commit = build['commit_sha']
        subprocess.run(['git', 'merge-base', '--is-ancestor', commit, pages_commit], cwd=ROOT, check=True)
        subprocess.run(['git', 'diff', '--quiet', 'HEAD', pages_commit, '--', *ASSETS], cwd=ROOT, check=True)
        for name in ASSETS:
            raw = public(name)[0]
            clean, n = re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?', b'', raw)
            assert n <= 1 and model.sha(clean) == build['files_sha256'][name], name
            if name == 'squeeze-fuel.html':
                assert b'/jh-sec-ftd-research.js' in raw and b'/jh-sec-ftd-page.js' in raw
        r.kv(commit=commit, runtime_packages=actual, schedules=schedules, pages_commit=pages_commit)
        primary = invoke(lam, s3, commit)
        r.kv(primary_request=primary)
        execution = profile(boto3.client('logs', region_name='us-east-1'), primary['status'])
        raw = public(model.CURRENT)[0]; current = model.strict(raw)
        assert raw == producer.raw_read(s3, BUCKET, model.CURRENT)
        assert current['replay'] == primary['status']['result']['replay']
        assert model.clock(current['generated_at']) >= model.clock(packet(s3, 'data/ops/releases/' + FUNCTION + '.json')['deployed_at'])
        compiled = store.replay(current['replay'], read)
        assert compiled['packet'] == {k: v for k, v in current.items() if k != 'replay'}
        run = store.verified_run(current['replay'], read); inputs = store.checked(run['input'], 'inputs', read)
        qualification = evidence.qualify(inputs, compiled, lambda ref: model.original(ref, read))
        keys = {current['replay']['manifest_key'], run['input']['key'], run['output']['key'], *(v['key'] for v in run['compilers'].values()), *(v['key'] for v in current['record_shards'].values())}
        def check(key):
            assert public(key)[0] == store.reader(s3, BUCKET)(key), key
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(check, sorted(keys)):
                pass
        assert s3.head_object(Bucket=BUCKET, Key=model.CURRENT)['CacheControl'] == 'no-store'
        for method in ('GET', 'HEAD'):
            assert 'no-store' in public(model.CURRENT, method)[1].get('cache-control', '')
        ranged, headers, code = public(model.CURRENT, byte_range='bytes=0-99')
        assert code == 206 and ranged == raw[:100] and 'no-store' in headers.get('cache-control', '')
        protected = {BASELINE['key'], inputs['predecessor']['key'], inputs['source_manifest']['key'], inputs['index']['original']['key'], primary['status_key'], *(v['original']['key'] for v in inputs['captures'].values())}
        if primary.get('dispatch_key'):
            protected.add(primary['dispatch_key'])
        protected.update(producer.request_key(model.sha((inputs['request_id'] + ':' + label).encode())) for label in ['index', *inputs['captures']])
        def deny(key):
            assert denied_with_retry('https://justhodl.ai/' + key) and denied_with_retry('https://' + BUCKET + '.s3.amazonaws.com/' + key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny, sorted(protected)):
                pass
        assert runtime(lam, s3, events, scheduler, FUNCTION) == actual[FUNCTION]
        final_schedules = baseline_ops.bindings(scheduler, events, arn)
        for field in ('schedules', 'classic_default_bus_rules'):
            assert final_schedules[field] == schedules[field]
        proof = {'contract': 'sec-ftd-native-acceptance.v1', 'generated_at': producer.now(), 'commit': commit,
            'runtime_packages': actual, 'schedules': final_schedules, 'execution_profile': execution,
            'qualified_candidate': CANDIDATE, 'primary_request': primary,
            'publication': {'key': model.CURRENT, 'sha256': model.sha(raw), 'bytes': len(raw), 'replay': current['replay'], 'generated_at': current['generated_at']},
            'counts': current['counts'], 'qualification': qualification, 'source_original_replay_matches': True,
            'complete_predecessors_retained': True, 'public_artifacts_checked': len(keys), 'protected_artifacts_checked': len(protected),
            'originals_anonymously_denied': True, 'pages_commit': pages_commit, 'assets': {name: build['files_sha256'][name] for name in ASSETS},
            'producer_invocations_this_acceptance': int(primary['invoke_sent']), 'acceptance_provider_requests': 0,
            'producer_provider_requests': len(inputs['captures']) + 1 if primary['invoke_sent'] else 0,
            'consumer_invocations': 0, 'private_account_reads': 0, 'paid_ai_calls': 0, 'notifications_sent': 0, 'portfolio_writes': 0, 'schedules_changed': 0}
        try:
            previous_proof = producer.raw_read(s3, BUCKET, PROOF)
        except Exception as exc:
            if not producer.missing(exc):
                raise
        else:
            proof['previous_verification'] = producer.protect(s3, BUCKET, previous_proof)
        body = model.encoded(proof)
        s3.put_object(Bucket=BUCKET, Key=PROOF, Body=body, ContentType='application/json', CacheControl='no-store')
        assert public(PROOF)[0] == body
        r.kv(proof_key=PROOF, publication=proof['publication'], counts=current['counts'], qualification=qualification,
             execution_profile=execution, public_artifacts_checked=len(keys), protected_artifacts_checked=len(protected),
             runtime_packages_checked=len(actual), producer_invocations_this_acceptance=int(primary['invoke_sent']),
             acceptance_provider_requests=0, producer_provider_requests=proof['producer_provider_requests'],
             consumer_invocations=0, private_account_reads=0, paid_ai_calls=0, notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
