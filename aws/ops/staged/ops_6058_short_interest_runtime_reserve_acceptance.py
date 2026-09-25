"""Verify one additional minute of runtime reserve; unchanged source and memory.

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
import ops_6053_short_interest_source_baseline as baseline_ops
import short_interest_research_model as model
import short_interest_research_store as store
import short_interest_producer as producer
import short_interest_context as boundary
import short_interest_evidence as evidence
BUCKET = 'justhodl-dashboard-live'
FUNCTION = 'justhodl-short-interest'
PROOF = 'data/short-interest-research-verification.json'
BASELINE = {'key': model.PRIVATE + '2b166e05d09ea599c6408ca5c031a26c0a5b67d5df4953d7fa8442514d34e579.bin',
            'sha256': '2b166e05d09ea599c6408ca5c031a26c0a5b67d5df4953d7fa8442514d34e579', 'bytes': 13545}
CANDIDATE = {'manifest_key': model.PREFIX + 'runs/5ac778143d30112bf8abc04f50d697aba86b1bd026d3cb3d1dce5b01473d1724.json',
             'output_sha256': 'b5765b28c8f712a42cc31e208ca8136a604b2bdae5d7602f72f3946e2e4c757a'}
SHARED = ('aws/shared/short_interest_context.py', 'aws/shared/short_interest_producer.py', 'aws/shared/equity_enrich.py')
ASSETS = ('short-interest-research.html', 'jh-short-interest-research.js', 'jh-short-interest-page.js', 'jh-option-research.css',
          'market-evidence.html', 'short-volume-research.html', 'short/index.html', 'short-pressure.html',
          'chart.html', 'chart-pro.html', 'jh-chart-instvol.js', 'ticker.html', 'why.html', 'signal-replay.html')


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
    request = 'chatgpt-short-interest-native-' + commit[:12] + '-1'
    status_key, dispatch_key = producer.request_key(request), producer.request_key(request + '-dispatch')
    claim = {'contract': 'short-interest-native-dispatch.v1', 'request_id': request, 'function': FUNCTION, 'started_at': producer.now(), 'status': 'claimed'}
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
    status, deadline = None, time.monotonic() + 390
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
    assert all(v.get('status') not in ('error', 'timeout') and v.get('memory_mb') == 512 and v.get('max_memory_mb', 512) < 512 and v.get('duration_ms', 360000) < 360000 for v in values)
    return {'execution_id': execution, 'managed_reports': values}


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(connect_timeout=10, read_timeout=60, retries={'max_attempts': 0}))
    events, scheduler = boto3.client('events', region_name='us-east-1'), boto3.client('scheduler', region_name='us-east-1')
    read = store.reader(s3, BUCKET)
    with report('ops_6058_short_interest_runtime_reserve_acceptance') as r:
        subprocess.run([sys.executable, str(ROOT / 'aws/lambdas/justhodl-short-interest/tests/run_tests.py')], cwd=ROOT, check=True)
        commit = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', 'aws/lambdas/justhodl-short-interest/source', 'aws/lambdas/justhodl-short-interest/config.json'], cwd=ROOT, text=True).strip()
        assert re.fullmatch('[a-f0-9]{40}', commit)
        baseline = model.strict(model.original(BASELINE, read))
        actual = {}
        for name in dependents(ROOT, SHARED):
            actual[name] = runtime(lam, s3, events, scheduler, name)
            assert actual[name]['receipt'] == {'status': 'matched', 'commit': commit if name == FUNCTION else '728fd2e426e904c4ad34245b441e9e6563c0b379'}, name
        assert actual[FUNCTION]['timeout'] == 360
        for field in ('memory_mb', 'runtime', 'handler', 'architectures', 'role', 'ephemeral_storage_mb'):
            assert actual[FUNCTION][field] == baseline['runtime'][field], field
        arn = lam.get_function_configuration(FunctionName=FUNCTION)['FunctionArn']
        schedules = baseline_ops.bindings(scheduler, events, arn)
        for field in ('schedules', 'classic_default_bus_rules'):
            assert schedules[field] == baseline['bindings'][field]
        store.verified_run(CANDIDATE, read)
        build = model.strict(public('build-manifest.json')[0]); pages_commit = build['commit_sha']
        subprocess.run(['git', 'merge-base', '--is-ancestor', '728fd2e426e904c4ad34245b441e9e6563c0b379', pages_commit], cwd=ROOT, check=True)
        subprocess.run(['git', 'diff', '--quiet', 'HEAD', pages_commit, '--', *ASSETS], cwd=ROOT, check=True)
        for name in ASSETS:
            raw = public(name)[0]
            clean, n = re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?', b'', raw)
            assert n <= 1 and model.sha(clean) == build['files_sha256'][name], name
            if name == 'short-interest-research.html':
                assert b'/jh-short-interest-research.js' in raw and b'/jh-short-interest-page.js' in raw
        r.kv(commit=commit, runtime_packages=actual, schedules=schedules, pages_commit=pages_commit)
        preceding_proof = producer.raw_read(s3, BUCKET, PROOF)
        assert model.strict(preceding_proof)['commit'] == '728fd2e426e904c4ad34245b441e9e6563c0b379'
        preceding_proof_ref = producer.protect(s3, BUCKET, preceding_proof)
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
        protected = {preceding_proof_ref['key'], BASELINE['key'], inputs['predecessor']['key'], primary['status_key'], *(v['original']['key'] for v in inputs['captures'].values())}
        if primary.get('dispatch_key'):
            protected.add(primary['dispatch_key'])
        protected.update(producer.request_key(model.sha((inputs['request_id'] + ':' + label).encode())) for label in inputs['captures'])
        def deny(key):
            assert denied_with_retry('https://justhodl.ai/' + key) and denied_with_retry('https://' + BUCKET + '.s3.amazonaws.com/' + key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny, sorted(protected)):
                pass
        assert runtime(lam, s3, events, scheduler, FUNCTION) == actual[FUNCTION]
        final_schedules = baseline_ops.bindings(scheduler, events, arn)
        for field in ('schedules', 'classic_default_bus_rules'):
            assert final_schedules[field] == schedules[field]
        proof = {'contract': 'short-interest-native-acceptance.v1', 'generated_at': producer.now(), 'commit': commit,
            'previous_acceptance_original': preceding_proof_ref, 'runtime_reserve_change': {'timeout_seconds_before': 300, 'timeout_seconds_after': 360, 'memory_mb_unchanged': 512},
            'runtime_packages': actual, 'schedules': final_schedules, 'execution_profile': execution,
            'qualified_candidate': CANDIDATE, 'primary_request': primary,
            'publication': {'key': model.CURRENT, 'sha256': model.sha(raw), 'bytes': len(raw), 'replay': current['replay'], 'generated_at': current['generated_at']},
            'counts': current['counts'], 'qualification': qualification, 'source_original_replay_matches': True,
            'complete_predecessors_retained': True, 'public_artifacts_checked': len(keys), 'protected_artifacts_checked': len(protected),
            'originals_anonymously_denied': True, 'pages_commit': pages_commit, 'assets': {name: build['files_sha256'][name] for name in ASSETS},
            'producer_invocations_this_acceptance': int(primary['invoke_sent']), 'acceptance_provider_requests': 0,
            'producer_provider_requests': len(inputs['captures']) if primary['invoke_sent'] else 0,
            'consumer_invocations': 0, 'private_account_reads': 0, 'paid_ai_calls': 0, 'notifications_sent': 0, 'portfolio_writes': 0, 'schedules_changed': 0}
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
