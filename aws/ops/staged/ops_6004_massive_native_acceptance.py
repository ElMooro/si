"""Exact native composite/consumer packages, one recovery, live bytes and replay."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import json, re, subprocess, sys, time, urllib.request
import boto3
from botocore.config import Config
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/ops/staged'), str(ROOT/'aws/ops/checks'), str(ROOT/'aws/shared')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from release_package_evidence import check_packages, shared_imports
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5966_sector_tilt_runtime_diagnosis import parse_runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
from ops_6001_massive_dependency_preflight import CONSUMERS
from ops_6003_massive_composite_candidate import independent
import massive_research_model as model
import massive_research_store as store
BUCKET = 'justhodl-dashboard-live'; FUNCTION = 'justhodl-massive-signals'
QUALIFIED = {'key': 'audit-private/20260909-originals/options-research/2e846ccde8151689993cb8bf20b6871a6485abdfced381c1a3348c48cd7b9869.bin',
    'sha256': '2e846ccde8151689993cb8bf20b6871a6485abdfced381c1a3348c48cd7b9869', 'bytes': 15381}
RECOVERY = {'manifest_key': 'data/massive-research/runs/d0b18627c3456391888efe97dc4eeaf551f6c2086f97ab987954960f45c0f398.json',
    'output_sha256': 'ec366829c6acc86526046f25b1693612c5201b7ece1347e492a7f42b99756c86'}
ASSETS = ('market-evidence.html', 'jh-massive-research.js', 'jh-massive-research-page.js', 'jh-massive-research.css',
    'jh-option-research.js', 'jh-option-research.css', 'jh-etf-desk-research.js', 'jh-etf-desk-page.js', 'etf.html', 'options.html', 'jh-data-feeds.js')
PROOF = 'data/massive-research-verification.json'


def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,
            headers={'User-Agent': 'justhodl-verify-release/1.0'}), timeout=40) as response: return store.bounded(response)


def source_commit(function):
    assert function in (*CONSUMERS, FUNCTION), 'Reviewed function required'
    source = 'aws/lambdas/'+function+'/source'
    files = [ROOT/p for p in subprocess.check_output(['git', 'ls-files', source], cwd=ROOT, text=True).splitlines()]
    paths = [source, 'aws/lambdas/'+function+'/config.json', *[p.relative_to(ROOT).as_posix() for p in shared_imports(ROOT, files)]]
    commit = subprocess.check_output(['git', 'log', '-1', '--format=%H', '--', *paths], cwd=ROOT, text=True).strip()
    assert re.fullmatch('[a-f0-9]{40}', commit)
    return commit


def receipts(packages):
    commits = {}
    for row in packages:
        expected = source_commit(row['function']); receipt = json.loads(public('data/ops/releases/'+row['function']+'.json'))
        assert receipt['commit'] == expected and receipt['code_sha256'] == row['code_sha256'], row['function']
        commits[row['function']] = expected
    return commits


def current(s3):
    try: packet = model.strict(store.bounded(s3.get_object(Bucket=BUCKET, Key=model.CURRENT)['Body']))
    except Exception as exc:
        if store.missing(exc): return None
        raise
    assert packet['contract'] == model.CONTRACT
    return packet


def completed_request(s3, packet):
    items = []
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=model.PRIVATE+'requests/'):
        items.extend(page.get('Contents', [])); assert len(items) <= 1000, 'Bounded native request discovery'
    for item in sorted(items, key=lambda x: x['LastModified'], reverse=True)[:100]:
        doc = model.strict(store.bounded(s3.get_object(Bucket=BUCKET, Key=item['Key'])['Body']))
        if (doc.get('contract') == 'massive-composite-request.v1' and doc.get('status') == 'complete'
                and doc.get('replay') == packet['replay'] and doc.get('published') is True):
            assert set(doc['aliases']) == set(model.PREDECESSORS) and all(doc['aliases'].values())
            return {'status_key': item['Key'], 'status': doc, 'invoke_sent': False, 'adopted_completed_native_request': True}
    raise AssertionError('Inspect current publication request; do not invoke again')


def invoke(lam, s3, commit):
    existing = current(s3)
    if existing is not None: return completed_request(s3, existing)
    request = 'chatgpt-massive-native-'+commit[:12]+'-1'; key = store.request_key(request); dispatch = store.request_key(request+'-dispatch')
    claim = {'contract': 'massive-composite-native-dispatch.v1', 'request_id': request,
        'started_at': store.now(), 'status': 'claimed', 'recovery': RECOVERY}; sent = False
    try: store.status_write(s3, BUCKET, dispatch, claim, IfNoneMatch='*')
    except Exception as exc:
        if not store.conflict(exc): raise
        previous = model.strict(store.bounded(s3.get_object(Bucket=BUCKET, Key=dispatch)['Body']))
        assert previous['contract'] == claim['contract'] and previous['request_id'] == request and previous['recovery'] == RECOVERY
    else:
        response, rejected = invoke_when_available(lam, {'FunctionName': FUNCTION, 'InvocationType': 'Event',
            'Payload': model.encoded({'request_id': request, 'recover_run': RECOVERY})})
        assert response['StatusCode'] == 202; sent = True
        claim.update(status='accepted_async', throttle_rejections_before_acceptance=rejected); store.status_write(s3, BUCKET, dispatch, claim)
    status = None; deadline = time.monotonic()+180
    while time.monotonic() < deadline:
        try: status = model.strict(store.bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body']))
        except Exception as exc:
            if not store.missing(exc): raise
        if status and status.get('status') in ('complete', 'failed'): break
        time.sleep(3)
    assert status and status['status'] == 'complete', 'Inspect retained attempt; never blindly reinvoke'
    assert status['provider_requests'] == 0 and status['recovered_from'] == RECOVERY and status['replay'] == RECOVERY
    result = {'request_id': request, 'status_key': key, 'dispatch_key': dispatch, 'status': status, 'invoke_sent': sent}
    packet = current(s3); assert packet is not None
    if packet['replay'] != status['replay']:
        assert model.clock(packet['generated_at']) > model.clock(status['generated_at'])
        adopted = completed_request(s3, packet); adopted.update(invoke_sent=sent, earlier_completed_request=result); return adopted
    assert status['published'] is True and set(status['aliases']) == set(model.PREDECESSORS) and all(status['aliases'].values())
    return result


def profile(logs, status):
    execution = status['execution_id']; assert re.fullmatch('[a-f0-9-]{36}', execution)
    deadline = time.monotonic()+45; start = model.clock(status['started_at'])
    while True:
        rows = logs.filter_log_events(logGroupName='/aws/lambda/'+FUNCTION, filterPattern='"'+execution+'"',
            startTime=int(start.timestamp()*1000)-5000, limit=50).get('events', [])
        values = [parse_runtime(row.get('message', ''), execution) for row in rows]; values = [v for v in values if v]
        if values: break
        assert time.monotonic() < deadline, 'Read completed execution report; do not reinvoke'; time.sleep(3)
    assert all(v.get('status') not in ('error', 'timeout') and v.get('memory_mb') == 512
        and v.get('max_memory_mb', 512) < 512 and v.get('duration_ms', 120000) < 120000 for v in values)
    return {'execution_id': execution, 'managed_reports': values, 'completed_request': True}


def main():
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=60, connect_timeout=10, retries={'max_attempts': 0}))
    events = boto3.client('events', region_name='us-east-1'); scheduler = boto3.client('scheduler', region_name='us-east-1')
    with report('ops_6004_massive_native_acceptance') as r:
        subprocess.run([sys.executable, str(ROOT/'aws/lambdas'/FUNCTION/'tests/run_tests.py')], cwd=ROOT, check=True)
        commit = source_commit(FUNCTION); actual = runtime(lam, s3, events, scheduler, FUNCTION)
        assert actual['receipt'] == {'status': 'matched', 'commit': commit} and actual['memory_mb'] == 512 and actual['timeout'] == 120
        raw = store.bounded(s3.get_object(Bucket=BUCKET, Key=QUALIFIED['key'])['Body'])
        assert len(raw) == QUALIFIED['bytes'] and model.sha(raw) == QUALIFIED['sha256']; qualification = model.strict(raw)
        assert qualification['privacy_verified'] is True and qualification['candidate_replay'] == RECOVERY
        assert actual['schedules'] == qualification['predecessor_runtime']['schedules']
        for field in ('function_name', 'runtime', 'handler', 'architectures', 'role', 'ephemeral_storage_mb'):
            assert actual[field] == qualification['predecessor_runtime'][field]
        read = store.reader(s3, BUCKET); store.verified_run(RECOVERY, read)
        packages = check_packages(lam, ROOT, CONSUMERS)
        failures = [{k: v for k, v in row.items() if k != 'files'} | {'source_mismatches': [f for f in row['files'] if not f['match']]} for row in packages if not row['pass']]
        assert not failures, failures
        commits = receipts(packages)
        build = json.loads(public('build-manifest.json')); pages_commit = build['commit_sha']
        subprocess.run(['git', 'merge-base', '--is-ancestor', commit, pages_commit], cwd=ROOT, check=True)
        subprocess.run(['git', 'diff', '--quiet', 'HEAD', pages_commit, '--', *ASSETS], cwd=ROOT, check=True)
        for name in ASSETS:
            body = public(name); clean, n = re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?', b'', body)
            assert n <= 1 and model.sha(clean) == build['files_sha256'][name], name
            if name == 'market-evidence.html': assert b'/jh-massive-research-page.js' in body
        r.kv(commit=commit, runtime=actual, consumer_packages=packages, consumer_release_commits=commits,
            pages_commit=pages_commit, qualified_replay=RECOVERY)
        request = invoke(lam, s3, commit); r.kv(completed_request=request)
        execution = profile(boto3.client('logs', region_name='us-east-1'), request['status'])
        raw = public(model.CURRENT); packet = model.strict(raw)
        assert packet == current(s3) and packet['replay'] == request['status']['replay']
        output = store.replay(packet['replay'], read); assert output == {k: v for k, v in packet.items() if k != 'replay'}
        run = store.verified_run(packet['replay'], read); inputs = store.checked(run['input'], 'inputs', read)
        counts = independent(output, read)
        protected = {QUALIFIED['key'], request['status_key'], *(c['original']['key'] for c in [*inputs['sources'].values(), *inputs['predecessors'].values()] if c['original'])}
        if request.get('dispatch_key'): protected.add(request['dispatch_key'])
        for key in model.PREDECESSORS:
            expected = store.compatibility(packet, key)
            assert model.strict(public(key)) == expected == model.strict(store.bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body']))
        for key in (model.CURRENT, *model.PREDECESSORS):
            assert s3.head_object(Bucket=BUCKET, Key=key)['CacheControl'] == 'no-store'
            with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key, method='HEAD', headers={'User-Agent': 'justhodl-verify-release/1.0'}), timeout=30) as response:
                assert 'no-store' in response.headers.get('Cache-Control', ''), key
        def deny(key): assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny, sorted(protected)): pass
        assert runtime(lam, s3, events, scheduler, FUNCTION) == actual
        proof = {'contract': 'massive-native-acceptance.v1', 'generated_at': store.now(), 'commit': commit,
            'runtime_package': actual, 'consumer_packages': packages, 'consumer_release_commits': commits,
            'request': request, 'execution_profile': execution, 'qualified_candidate': RECOVERY,
            'publication': {'key': model.CURRENT, 'sha256': model.sha(raw), 'bytes': len(raw), 'replay': packet['replay'], 'generated_at': packet['generated_at']},
            'composition_replay_matches': True, 'parent_outputs_independently_checked': True,
            'original_provider_replay_performed_by_this_acceptance': False, 'counts': counts,
            'compatibility_aliases_verified': list(model.PREDECESSORS), 'originals_anonymously_denied': True,
            'protected_artifacts_checked': len(protected), 'pages_commit': pages_commit,
            'assets': {name: build['files_sha256'][name] for name in ASSETS},
            'producer_invocations_this_acceptance': int(request['invoke_sent']), 'provider_requests': 0, 'consumer_invocations': 0,
            'private_account_reads': 0, 'paid_ai_calls': 0, 'notifications_sent': 0, 'portfolio_writes': 0, 'schedules_changed': 0}
        body = model.encoded(proof); s3.put_object(Bucket=BUCKET, Key=PROOF, Body=body, ContentType='application/json', CacheControl='no-store')
        assert public(PROOF) == body
        r.kv(proof_key=PROOF, publication=proof['publication'], counts=counts, execution_profile=execution,
            protected_artifacts_checked=len(protected), consumer_packages_checked=len(packages),
            producer_invocations_this_acceptance=proof['producer_invocations_this_acceptance'], provider_requests=0,
            consumer_invocations=0, private_account_reads=0, paid_ai_calls=0, notifications_sent=0, portfolio_writes=0, schedules_changed=0)


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
