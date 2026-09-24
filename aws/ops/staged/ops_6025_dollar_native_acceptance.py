"""Exact Dollar and consumer packages, one recovery, live evidence and original replay."""
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
from ops_6020_dollar_source_preflight import CONSUMERS
from dollar_original_audit import independent
import dollar_research_model as model
import dollar_research_store as store
BUCKET='justhodl-dashboard-live';FUNCTION='justhodl-dollar-radar'
QUALIFIED={'key':model.PRIVATE+'7fcce19066a2c3eb03f98719ce9bdb65271da7a301710b9fee7529d209c120b0.bin',
    'sha256':'7fcce19066a2c3eb03f98719ce9bdb65271da7a301710b9fee7529d209c120b0','bytes':2427}
RECOVERY={'manifest_key':'data/dollar-research/runs/a828a9bed62d713bbeee5e121833197954e1c0de05bf8d69f45760a9900cfd64.json',
    'output_sha256':'aa871597f3adfa7f417a568806b582ffae34524d52172d67de6d894b63c816d5'}
ASSETS=('dollar.html','jh-dollar-research.js','jh-dollar-research-page.js','jh-dollar-research.css','jh-option-research.js','jh-option-research.css')
PROOF='data/dollar-research-verification.json'
# Explicit intended deployments: Crisis was recovered after its source commit;
# the unchanged snapshotter is repackaged to establish its missing receipt.
RECORDED_RELEASES={
    'justhodl-crisis-composite':'0fdc2c779a1ea95a04c035e37ce8294699567cac',
    'justhodl-history-snapshotter':'1e597815292baa8dc4c6ad3ea050a427fa23480a',
}


def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,
            headers={'User-Agent': 'justhodl-verify-release/1.0'}), timeout=40) as response: return store.bounded(response)


def release_history():
    """Path history must not mistake a shallow boundary for a source change."""
    command=['git','rev-parse','--is-shallow-repository']
    if subprocess.check_output(command,cwd=ROOT,text=True).strip()=='true':
        subprocess.run(['git','fetch','--unshallow','--filter=blob:none','origin','main'],cwd=ROOT,check=True)
    assert subprocess.check_output(command,cwd=ROOT,text=True).strip()=='false', 'Complete commit history required for exact release attribution'


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
        expected = RECORDED_RELEASES.get(row['function']) or source_commit(row['function'])
        receipt = json.loads(public('data/ops/releases/'+row['function']+'.json'))
        assert receipt['commit'] == expected and receipt['code_sha256'] == row['code_sha256'], row['function']
        commits[row['function']] = expected
    return commits


def current(s3):
    try: packet = json.loads(store.bounded(s3.get_object(Bucket=BUCKET, Key=model.CURRENT)['Body']))
    except Exception as exc:
        if store.missing(exc): return None
        raise
    if packet.get('engine') == FUNCTION and str(packet.get('schema_version')) == '3.0': return None
    assert packet['contract'] == model.CONTRACT
    return packet


def completed_request(s3, packet):
    items = []
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket=BUCKET, Prefix=model.PRIVATE+'requests/'):
        items.extend(page.get('Contents', [])); assert len(items) <= 1000, 'Bounded native request discovery'
    for item in sorted(items, key=lambda x: x['LastModified'], reverse=True)[:100]:
        doc = json.loads(store.bounded(s3.get_object(Bucket=BUCKET, Key=item['Key'])['Body']))
        if (doc.get('contract') == 'dollar-original-request.v1' and doc.get('status') == 'complete'
                and doc.get('replay') == packet['replay'] and doc.get('published') is True):
            return {'status_key': item['Key'], 'status': doc, 'invoke_sent': False, 'adopted_completed_native_request': True}
    raise AssertionError('Inspect current publication request; do not invoke again')


def invoke(lam, s3, commit):
    existing = current(s3)
    if existing is not None: return completed_request(s3, existing)
    request = 'chatgpt-dollar-native-'+commit[:12]+'-1'; key = store.request_key(request); dispatch = store.request_key(request+'-dispatch')
    claim = {'contract': 'dollar-native-dispatch.v1', 'request_id': request,
        'started_at': store.now(), 'status': 'claimed', 'recovery': RECOVERY}; sent = False
    try: store.status_write(s3, BUCKET, dispatch, claim, IfNoneMatch='*')
    except Exception as exc:
        if not store.conflict(exc): raise
        previous = json.loads(store.bounded(s3.get_object(Bucket=BUCKET, Key=dispatch)['Body']))
        assert previous['contract'] == claim['contract'] and previous['request_id'] == request and previous['recovery'] == RECOVERY
    else:
        response, rejected = invoke_when_available(lam, {'FunctionName': FUNCTION, 'InvocationType': 'Event',
            'Payload': model.encoded({'request_id': request, 'recover_run': RECOVERY})})
        assert response['StatusCode'] == 202; sent = True
        claim.update(status='accepted_async', throttle_rejections_before_acceptance=rejected); store.status_write(s3, BUCKET, dispatch, claim)
    status = None; deadline = time.monotonic()+180
    while time.monotonic() < deadline:
        try: status = json.loads(store.bounded(s3.get_object(Bucket=BUCKET, Key=key)['Body']))
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
    assert status['published'] is True
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
    assert all(v.get('status') not in ('error', 'timeout') and v.get('memory_mb') == 256
        and v.get('max_memory_mb', 256) < 256 and v.get('duration_ms', 180000) < 180000 for v in values)
    return {'execution_id': execution, 'managed_reports': values, 'completed_request': True}


def publication_request(lam,s3,commit,require_existing):
    if not require_existing:return invoke(lam,s3,commit)
    packet=current(s3)
    assert packet is not None, 'Existing native publication required; verification cannot invoke'
    return completed_request(s3,packet)


def main(report_name='ops_6025_dollar_native_acceptance',require_existing=False):
    s3 = boto3.client('s3', region_name='us-east-1')
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=60, connect_timeout=10, retries={'max_attempts': 0}))
    events = boto3.client('events', region_name='us-east-1'); scheduler = boto3.client('scheduler', region_name='us-east-1')
    with report(report_name) as r:
        subprocess.run([sys.executable, str(ROOT/'aws/lambdas'/FUNCTION/'tests/run_tests.py')], cwd=ROOT, check=True)
        release_history()
        commit = source_commit(FUNCTION); actual = runtime(lam, s3, events, scheduler, FUNCTION)
        assert actual['receipt'] == {'status': 'matched', 'commit': commit} and actual['memory_mb'] == 256 and actual['timeout'] == 180
        raw = store.bounded(s3.get_object(Bucket=BUCKET, Key=QUALIFIED['key'])['Body'])
        assert len(raw) == QUALIFIED['bytes'] and model.sha(raw) == QUALIFIED['sha256']; qualification = json.loads(raw)
        assert qualification['recorded_replay_verified'] is True and qualification['original_arithmetic_independently_checked'] is True and qualification['prior_output_byte_identical'] is True and qualification['candidate_replay'] == RECOVERY
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
            if name == 'dollar.html': assert b'/jh-dollar-research-page.js' in body and b'/jh-dollar-research.js' in body
        r.kv(commit=commit, runtime=actual, consumer_packages=packages, consumer_release_commits=commits,
            pages_commit=pages_commit, qualified_replay=RECOVERY)
        request = publication_request(lam,s3,commit,require_existing); r.kv(completed_request=request)
        execution = profile(boto3.client('logs', region_name='us-east-1'), request['status'])
        raw = public(model.CURRENT); packet = json.loads(raw)
        assert packet == current(s3) and packet['replay'] == request['status']['replay']
        output = store.replay(packet['replay'], read); assert output == {k: v for k, v in packet.items() if k != 'replay'}
        run = store.verified_run(packet['replay'], read); inputs = store.checked(run['input'], 'inputs', read)
        canonical=json.loads(store.original(inputs['captures']['data/report-measurements.json']['original'],read))
        originals=store.canonical_fred_replay.restore(canonical,store.catalog.SERIES,read)
        counts=independent(output,originals,canonical,store.catalog)
        assert store.bounded(s3.get_object(Bucket=BUCKET,Key=model.HISTORY)['Body'])==store.original(inputs['captures'][model.HISTORY]['original'],read),'Complete legacy history changed'
        protected={QUALIFIED['key'],request['status_key'],*(row['original']['key'] for row in inputs['captures'].values() if row['original'])}
        if request.get('dispatch_key'):protected.add(request['dispatch_key'])
        assert s3.head_object(Bucket=BUCKET,Key=model.CURRENT)['CacheControl']=='no-store'
        with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+model.CURRENT,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:
            assert 'no-store' in response.headers.get('Cache-Control','')
        def deny(key): assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny, sorted(protected)): pass
        assert runtime(lam, s3, events, scheduler, FUNCTION) == actual
        proof = {'contract': 'dollar-native-acceptance.v1', 'generated_at': store.now(), 'commit': commit,
            'runtime_package': actual, 'consumer_packages': packages, 'consumer_release_commits': commits,
            'request': request, 'execution_profile': execution, 'qualified_candidate': RECOVERY,
            'publication': {'key': model.CURRENT, 'sha256': model.sha(raw), 'bytes': len(raw), 'replay': packet['replay'], 'generated_at': packet['generated_at']},
            'original_replay_matches': True, 'complete_legacy_history_preserved': True, 'arithmetic_independently_checked': True,
            'original_provider_replay_performed_by_this_acceptance': True, 'counts': counts,
            'originals_anonymously_denied': True,
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
