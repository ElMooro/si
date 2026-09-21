"""Verify exact holdings packages, complete original replay and two public producers.

No decision consumer, private account, paid AI, notification or portfolio mutation.
Reuses the already retained source run; no additional provider requests.
"""
from pathlib import Path
from datetime import datetime,timezone
from decimal import Decimal,localcontext
from concurrent.futures import ThreadPoolExecutor
from collections import Counter,defaultdict
import base64,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts')]
from ops_report import report
from release_package_evidence import shared_imports
from acceptance_invoke import invoke_when_available
import etf_holdings_store as store
import etf_holdings_model as model
import etf_holdings_native as native
from replay_etf_holdings_research import verify as replay_verify
BUCKET='justhodl-dashboard-live'
COMMIT='bfa8c58127b88b0d5d2760539ca9f10cb0c01111'
ORIGINAL_COMMIT='a073f17597a303e1cf27cee408d0d42837b257a7'
PAGE_COMMIT=ORIGINAL_COMMIT
RECOVERY={'manifest_key':'data/etf-holdings-research/runs/731604a3b4d38a6b8d140e708e695452457bbac50af3643b054ac082d00b4dc8.json',
    'output_sha256':'e4ec6ad4af488a5013d39fd048a81b46ed854a27df69f78ac2aac5deddc951ad'}
FUNCTIONS=('justhodl-equity-confluence','justhodl-etf-constituents','justhodl-flow-lookthrough')
PRODUCERS=('justhodl-etf-constituents','justhodl-flow-lookthrough')
KINDS=dict(zip(PRODUCERS,('holdings','lookthrough')))
ASSETS=('etf-holdings.html','flow-lookthrough.html','jh-etf-holdings.js','jh-sector-research.css','flows.html','capital-flow-radar.html')
AUDIT=('9fcdfada29f0e2a54ff504b2dc0c14f83e3a4b51e9d16cf72c379e4c2035e772',43114)

def expected_commit(fn):
    if fn not in FUNCTIONS:raise ValueError('Unreviewed runtime')
    return ORIGINAL_COMMIT if fn=='justhodl-equity-confluence' else COMMIT

def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as r:return store.bounded(r)

def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)

def runtime(lam,fn):
    source=ROOT/'aws/lambdas'/fn/'source';cfgpath=source.parent/'config.json'
    configuration=json.loads(cfgpath.read_bytes()) if cfgpath.exists() else {};request={'FunctionName':fn}
    if configuration.get('release_validation'):request['Qualifier']='live'
    deployed=lam.get_function(**request);cfg=deployed['Configuration'];receipt=json.loads(public('data/ops/releases/'+fn+'.json'))
    assert receipt['commit']==expected_commit(fn) and receipt['code_sha256']==cfg['CodeSha256'],'Exact receipt required: '+fn
    assert cfg['State']=='Active' and cfg['LastUpdateStatus']=='Successful'
    raw=store.bounded(urllib.request.urlopen(deployed['Code']['Location'],timeout=45))
    assert base64.b64encode(hashlib.sha256(raw).digest()).decode()==cfg['CodeSha256']
    paths=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(source.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
    expected={p.relative_to(source).as_posix():p for p in paths}
    expected.update({p.name:p for p in shared_imports(ROOT,paths) if not (source/p.name).exists()})
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        for name,p in expected.items():assert z.read(name)==p.read_bytes(),'Packaged bytes differ: '+fn+'/'+name
    alias=None
    if request.get('Qualifier'):
        a=lam.get_alias(FunctionName=fn,Name='live');assert a['FunctionVersion']==cfg['Version'] and not (a.get('RoutingConfig') or {}).get('AdditionalVersionWeights')
        alias={'name':'live','version':cfg['Version'],'weighted_secondary_versions':0}
    if fn in PRODUCERS:assert cfg['MemorySize']==3072 and cfg['Timeout']==840
    return {'commit':expected_commit(fn),'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}

def invoke_public(lam,s3,module,fn):
    assert fn in KINDS,'Only the two reviewed public producers may be invoked'
    kind=KINDS[fn]
    payload={'request_id':'chatgpt-'+fn+'-'+COMMIT[:12]+'-1'}
    if kind=='holdings':payload['recover_run']=RECOVERY
    request='chatgpt-'+fn+'-'+COMMIT[:12]+'-1';key=module.request_key(kind,request);dispatch_key=module.request_key(kind,request+'-dispatch');sent=False
    claim={'contract':'etf-holdings-native-dispatch.v1','request_id':request,'started_at':datetime.now(timezone.utc).isoformat(),'status':'claimed'}
    try:module.status_write(s3,BUCKET,dispatch_key,claim,IfNoneMatch='*')
    except Exception as exc:
        if not module.conflict(exc):raise
        claim=json.loads(module.bounded(s3.get_object(Bucket=BUCKET,Key=dispatch_key)['Body']))
        assert claim['request_id']==request and claim['contract']=='etf-holdings-native-dispatch.v1'
    else:
        response,rejected=invoke_when_available(lam,{'FunctionName':fn,'InvocationType':'Event','Payload':model.encoded(payload)})
        assert response['StatusCode']==202;sent=True
        claim.update(status='accepted_async',throttle_rejections_before_acceptance=rejected);module.status_write(s3,BUCKET,dispatch_key,claim)
    deadline=time.monotonic()+960;status=None
    while time.monotonic()<deadline:
        try:status=json.loads(module.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
        except Exception as exc:
            if not module.missing(exc):raise
        if status and status.get('status') in ('complete','failed'):break
        time.sleep(3)
    assert status and status.get('status')=='complete' and status.get('published') is True,'Native producer did not publish; inspect durable request without reinvoking'
    assert status['provider_requests_this_execution']==0,'Recovery must not collect provider data'
    if kind=='holdings':
        assert status['recovered_from']==RECOVERY and status['replay']['output_sha256']==RECOVERY['output_sha256']
        assert status['generated_at']=='2026-09-21T08:33:09.319214+00:00' and status['provider_requests']==1194
    return {'request_id':request,'status_key':key,'invoke_sent':sent,'status':status}

def execution_profile(logs,fn,status):
    sys.path.insert(0,str(ROOT/'aws/ops/staged'))
    from ops_5966_sector_tilt_runtime_diagnosis import parse_runtime
    execution=status['execution_id'];assert re.fullmatch(r'[a-f0-9-]{36}',execution)
    start=model.clock(status['started_at']);deadline=time.monotonic()+45
    while True:
        page=logs.filter_log_events(logGroupName='/aws/lambda/'+fn,filterPattern='"'+execution+'"',startTime=int(start.timestamp()*1000)-5000,limit=50)
        values=[parse_runtime(row.get('message',''),execution) for row in page.get('events',[])]
        values=[v for v in values if v]
        if values:break
        assert time.monotonic()<deadline,'Managed execution profile unavailable; do not reinvoke a completed request'
        time.sleep(3)
    assert all(v.get('status') not in ('error','timeout') for v in values)
    assert all(v.get('memory_mb')==3072 and v.get('max_memory_mb',3072)<3072 and v.get('duration_ms',840000)<840000 for v in values)
    return {'execution_id':execution,'managed_reports':values,'completed_request':True}

def independent_arithmetic(packet, read):
    """Check original row positions and derived associations without normalizer calls."""
    counts = Counter(); expected_members = defaultdict(list); samples = {}
    def artifact(ref):
        raw = read(ref['key'])
        assert len(raw) == ref['bytes'] and native.sha(raw) == ref['sha256']
        return json.loads(raw)
    def decimal(value): return None if value is None else Decimal(str(value))
    with localcontext() as ctx:
        ctx.prec = 50
        for ticker, fund in packet['funds'].items():
            snapshots = {}; indexed = {}
            for role in ('current', 'prior'):
                meta = fund[role]; doc = artifact(meta['snapshot']); rows = []; index = []
                assert doc['ticker'] == ticker and doc['contract'] == 'etf-holdings-snapshot.v1'
                assert doc['processed_date'] == meta['processed_date'] and doc['effective_dates'] == meta['effective_dates']
                for ref in doc['parts']:
                    page = artifact(ref)
                    assert page['row_offset'] == len(rows) and page['ticker'] == ticker and len(page['rows']) <= 250
                    assert page['processed_date'] == doc['processed_date']; rows.extend(page['rows'])
                for ref in doc['index_parts']:
                    page = artifact(ref)
                    assert page['row_offset'] == len(index) and page['ticker'] == ticker and len(page['rows']) <= 500
                    index.extend(page['rows'])
                assert len(rows) == len(index) == doc['indexed_rows'] == doc['quality']['returned_rows']
                assert [r['row_id'] for r in rows] == [r['row_id'] for r in index]
                assert len({r['row_id'] for r in rows}) == len(rows)
                original_pages = {}
                for source in doc['originals']:
                    original_pages[source['page']] = (source, json.loads(store.protected(source, read), parse_float=Decimal)['results'])
                by_identity = defaultdict(list)
                for ordinal, (row, item) in enumerate(zip(rows, index)):
                    source, original_rows = original_pages[row['source']['page']]
                    assert source['sha256'] == row['source']['sha256']
                    original = original_rows[row['source']['row_index']]
                    assert original['composite_ticker'] == ticker and original['processed_date'] == row['processed_date'] == doc['processed_date']
                    assert original['effective_date'] == row['effective_date']
                    for name in ('weight', 'shares_held', 'market_value'):
                        if name in row['field_errors']: continue
                        assert decimal(original.get(name)) == decimal(row[name + '_raw_decimal'])
                    for name in ('constituent_ticker', 'figi', 'isin', 'us_code', 'sedol', 'exchange', 'currency_traded', 'asset_class', 'security_type'):
                        if name in row['field_errors']: continue
                        value = original.get(name)
                        assert row[name] == (value.strip() or None if isinstance(value, str) else None)
                    assert row['row_id'] == native.sha(native.encoded(row['source']))
                    for key in item:
                        if key != 'part': assert item[key] == row[key]
                    assert item['part'] == ordinal // 250
                    fields = ('figi', 'isin', 'us_code', 'sedol', 'exchange', 'currency_traded', 'asset_class', 'security_type')
                    if row['identity_key']:
                        assert any(row[k] for k in ('figi', 'isin', 'us_code', 'sedol'))
                        assert row['identity_key'] == native.sha(native.encoded({k: row[k] for k in fields}))
                        by_identity[row['identity_key']].append(row)
                        if role == 'current': expected_members[row['identity_key']].append((ticker, row, meta['snapshot'], doc))
                    counts['original_row_positions'] += 1
                if rows:
                    assert dict(Counter(r['effective_date'] for r in rows)) == doc['effective_dates']
                    weights = [Decimal(r['weight_raw_decimal']) for r in rows if r['weight_raw_decimal'] is not None]
                    assert decimal(doc['weight_audit']['raw_observed_sum_decimal']) == (sum(weights, Decimal(0)) if weights else None)
                    assert doc['quality']['missing_ticker_rows'] == sum(r['constituent_ticker'] is None for r in rows)
                    assert doc['quality']['missing_identity_rows'] == sum(r['identity_key'] is None for r in rows)
                    assert doc['quality']['duplicate_identity_rows'] == sum(len(v)-1 for v in by_identity.values())
                for flag in ('weight_unit_certified', 'market_value_currency_certified', 'current_holdings_confirmed', 'corporate_actions_verified'):
                    assert doc['quality'][flag] is False
                snapshots[role] = doc; indexed[role] = by_identity; counts['snapshots'] += 1
                if role == 'current': counts['current_rows'] += len(rows)
            summary = artifact(fund['comparison']); compared = []
            for ref in summary['parts']:
                page = artifact(ref); assert page['ticker'] == ticker and page['row_offset'] == len(compared)
                compared.extend(page['rows'])
            assert len(compared) == summary['compared_identities']
            current, prior = snapshots['current'], snapshots['prior']
            valid = all(s['quality']['status'] == 'complete_returned_snapshot' and len(s['effective_dates']) == 1 for s in (current, prior))
            valid = bool(valid and next(iter(current['effective_dates'])) > next(iter(prior['effective_dates'])))
            assert summary['comparable_snapshots'] is valid
            assert summary['current_snapshot'] == fund['current']['snapshot'] and summary['prior_snapshot'] == fund['prior']['snapshot']
            now, before = indexed['current'], indexed['prior']
            assert [r['identity_key'] for r in compared] == sorted(set(now) | set(before))
            for row in compared:
                a, b = now.get(row['identity_key'], []), before.get(row['identity_key'], [])
                status = ('not_comparable_snapshots' if not valid else 'ambiguous_identity' if len(a) > 1 or len(b) > 1 else
                    'observed_in_both' if a and b else 'observed_only_in_current' if a else 'observed_only_in_prior')
                assert row['status'] == status and row['inferred_trade_usd'] is None
                assert row['current_rows'] == [r['row_id'] for r in a] and row['prior_rows'] == [r['row_id'] for r in b]
                for field, delta in [('shares_held_raw_decimal', 'shares_held_change_raw_decimal'), ('weight_raw_decimal', 'weight_change_raw_decimal')]:
                    expected = Decimal(a[0][field]) - Decimal(b[0][field]) if status == 'observed_in_both' and a[0][field] is not None and b[0][field] is not None else None
                    assert decimal(row[delta]) == expected
                counts['identity_comparisons'] += 1
            assert dict(Counter(r['status'] for r in compared)) == summary['identity_status_counts']
            if ticker in ('SPY', 'VOO', 'TLT', 'BND', 'SOXL', 'EFA'):
                samples[ticker] = {'current_rows': current['indexed_rows'], 'current_effective_dates': current['effective_dates'],
                    'prior_effective_dates': prior['effective_dates'], 'processed_date': current['processed_date'],
                    'weight_audit': current.get('weight_audit'), 'quality': current['quality'], 'comparison_counts': summary['identity_status_counts']}
        directory = artifact(packet['security_directory']); listed = []
        for ref in directory['parts']:
            page = artifact(ref); assert page['row_offset'] == len(listed); listed.extend(page['securities'])
        assert len(listed) == directory['security_count'] == len(expected_members)
        assert [r['identity_key'] for r in listed] == sorted(expected_members)
        found_members = {}; expected_directory = {r['identity_key']: r for r in listed}
        for bucket, ref in directory['buckets'].items():
            group = artifact(ref); assert group['bucket'] == bucket
            for rec in group['securities']:
                identity = rec['identity_key']; assert identity[:2] == bucket and identity not in found_members
                found_members[identity] = rec
                expected = {(t, r['row_id']): (r, s, d) for t, r, s, d in expected_members[identity]}
                actual = {(r['fund'], r['row_id']): r for r in rec['memberships']}
                assert len(actual) == len(rec['memberships']) == len(expected) and set(actual) == set(expected)
                assert rec['observed_funds'] == sorted({t for t, _ in expected})
                assert rec['effective_dates'] == sorted({r['effective_date'] for r, _, _ in expected.values()})
                assert rec['portfolio_weight'] is None and rec['inferred_trade_usd'] is None and rec['additional_independent_investment_votes'] == 0
                assert expected_directory[identity]['observed_fund_count'] == len(rec['observed_funds'])
                for key, member in actual.items():
                    row, ref, doc = expected[key]
                    assert member['snapshot'] == ref and member['source'] == row['source']
                    for name in ('effective_date', 'processed_date', 'weight_raw_decimal', 'market_value_raw_decimal', 'shares_held_raw_decimal'):
                        assert member[name] == row[name]
                    assert member['snapshot_complete'] == doc['quality']['pagination_complete']
                    assert member['source_valid_until'] == doc['source_valid_until']
                    assert member['weight_unit_certified'] is False and member['market_value_currency_certified'] is False
                    counts['fund_memberships'] += 1
        assert set(found_members) == set(expected_members)
        assert counts['current_rows'] == packet['quality']['reconstructed_current_rows']
    return {'counts': dict(counts), 'samples': samples, 'all_source_positions_and_arithmetic_match': True}


def preflight_replay(read, protected_keys):
    digest, size = AUDIT; ref = {'key': store.PRIVATE + digest + '.bin', 'sha256': digest, 'bytes': size}
    audit = json.loads(store.protected(ref, read)); protected_keys.add(ref['key'])
    assert audit['contract'] == 'etf-constituent-source-preflight.v1'
    parent = audit['recovered_selection_manifest']; parent = {**parent, 'key': store.PRIVATE + parent['sha256'] + '.bin'}
    store.protected(parent, read); protected_keys.add(parent['key'])
    for ref in audit['packets'].values(): store.protected(ref, read); protected_keys.add(ref['key'])
    results = {}
    for role in ('current', 'prior'):
        for ticker, collection in audit[role + '_probes'].items():
            assert collection['status'] == 'complete_returned_snapshot'
            for page in [collection['selection'], *collection['pages']]:
                store.protected(page['original'], read); protected_keys.add(page['original']['key'])
            restored = native.reconstruct(collection, read, audit['generated_at'])
            assert restored['quality']['status'] == 'complete_returned_snapshot'
            assert len(restored['rows']) == collection['rows'] and restored['effective_dates'] == collection['effective_dates']
            assert restored['quality']['missing_ticker_rows'] == collection['missing_ticker']
            assert Decimal(restored['weight_audit']['raw_observed_sum_decimal']) == Decimal(collection['raw_weight_sum'])
            results[role + '_' + ticker] = {'rows': len(restored['rows']), 'effective_dates': restored['effective_dates'],
                'weight_audit': restored['weight_audit'], 'original_pages': len(collection['pages']), 'replayed': True}
    assert len(results) == 8
    return audit, results


def main():
    lam = boto3.client('lambda', region_name='us-east-1', config=Config(read_timeout=60, connect_timeout=10, retries={'max_attempts': 0}, tcp_keepalive=True))
    s3 = boto3.client('s3', region_name='us-east-1'); events = boto3.client('events', region_name='us-east-1')
    with report('ops_5980_etf_holdings_recovery_acceptance') as r:
        subprocess.run([sys.executable, str(ROOT/'tests/test_etf_holdings_acceptance.py')], cwd=ROOT, check=True)
        for fn in PRODUCERS: subprocess.run([sys.executable, str(ROOT/'aws/lambdas'/fn/'tests/run_tests.py')], cwd=ROOT, check=True)
        runtimes = {fn: runtime(lam, fn) for fn in FUNCTIONS}; r.kv(runtimes=runtimes)
        read = store.reader(s3, BUCKET); protected_keys = set(); audit, audit_replays = preflight_replay(read, protected_keys)
        r.kv(audit_originals_replayed=audit_replays, retained_audit_manifest=AUDIT[0])
        for fn, name in [('justhodl-etf-constituents', 'etf_constituents'), ('justhodl-flow-lookthrough', 'flow_lookthrough')]:
            original = subprocess.check_output(['git', 'show', 'f49255bd7:aws/lambdas/'+fn+'/source/lambda_function.py'], cwd=ROOT)
            assert (ROOT/'aws/lambdas'/fn/'source'/('legacy_'+name+'.py')).read_bytes() == original
        assert (ROOT/'docs/legacy/etf-holdings-flow-lookthrough-pre-native-20260921.html.txt').read_bytes() == subprocess.check_output(['git', 'show', 'f49255bd7:flow-lookthrough.html'], cwd=ROOT)
        build = json.loads(public('build-manifest.json')); pages_commit = build['commit_sha']
        subprocess.run(['git', 'merge-base', '--is-ancestor', PAGE_COMMIT, pages_commit], cwd=ROOT, check=True)
        subprocess.run(['git', 'diff', '--quiet', 'HEAD', pages_commit, '--', *ASSETS], cwd=ROOT, check=True)
        for name in ASSETS:
            served = public(name); clean, count = re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?', b'', served)
            assert count <= 1 and model.sha(clean) == build['files_sha256'][name], 'Built asset differs: '+name
            if name in ('etf-holdings.html', 'flow-lookthrough.html'): assert b'jh-etf-holdings.js?v=20260921-native1' in served
        schedules = []
        for fn in PRODUCERS:
            cfg = lam.get_function_configuration(FunctionName=fn); names = []
            for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']): names.extend(page['RuleNames'])
            expected = {s['name']: s for s in audit['runtimes'][fn]['schedules'] if s['kind'] == 'EventBridge rule'}
            assert len(expected) == len(audit['runtimes'][fn]['schedules']) and set(names) == set(expected)
            for name in sorted(names):
                rule = events.describe_rule(Name=name); assert rule['State'] == expected[name]['state'] and rule['ScheduleExpression'] == expected[name]['expression']
                schedules.append({'function': fn, 'name': name, 'state': rule['State'], 'expression': rule['ScheduleExpression'], 'changed': False})
        requests = {}; profiles = {}; packets = {}; replays = {}; hashes = {}
        for fn in PRODUCERS:
            kind = KINDS[fn]; prefix, current, contract = store.KINDS[kind]
            request = invoke_public(lam, s3, store, fn); requests[fn] = request; r.kv(**{kind+'_request': request})
            profiles[fn] = execution_profile(boto3.client('logs', region_name='us-east-1'), fn, request['status'])
            if kind == 'holdings': assert request['status']['compatibility_publications'] == dict.fromkeys(store.ALIASES, True)
            raw = public(current); packet = json.loads(raw); assert json.loads(read(current)) == packet
            assert packet['contract'] == contract and model.clock(packet['generated_at']) >= model.clock(request['status']['generated_at']) > model.clock(audit['generated_at'])
            replay = replay_verify(packet, read); assert replay['no_allocation_authority'] and replay['configured_funds'] == 300
            requested = store.replay(request['status']['replay'], read); assert requested['generated_at'] == request['status']['generated_at']
            run = json.loads(read(packet['replay']['manifest_key'])); inputs = store.checked(run['input'], prefix, 'inputs', read)
            refs = ([*inputs['contexts'].values(), inputs['previous']] if kind == 'holdings' else [inputs['canonical_source'], inputs['previous']])
            for ref in refs:
                if ref: store.protected(ref, read); protected_keys.add(ref['key'])
            if kind == 'holdings':
                for pair in inputs['collections'].values():
                    for collection in pair.values():
                        for page in [collection.get('selection'), *collection.get('pages', [])]:
                            if page and page.get('original'): store.protected(page['original'], read); protected_keys.add(page['original']['key'])
                        if collection.get('rejected_original'):
                            ref = collection['rejected_original']; store.protected(ref, read); protected_keys.add(ref['key'])
                migration = json.loads(read(store.MIGRATION)); store.protected(migration, read); protected_keys.add(migration['key'])
            packets[kind] = packet; replays[kind] = replay; hashes[kind] = {'key': current, 'sha256': model.sha(raw), 'bytes': len(raw), 'replay': packet['replay']}
        canonical = store.replay(packets['lookthrough']['canonical_replay'], read)
        for field in ('funds', 'security_directory', 'quality', 'source_valid_until', 'methodology'):
            assert packets['lookthrough'][field] == canonical[field] == packets['holdings'][field]
        assert packets['lookthrough']['provider_requests'] == 0 and packets['lookthrough']['additional_independent_investment_votes'] == 0
        aliases = {}
        for key in store.ALIASES:
            expected = store.compatibility(packets['holdings'], key)
            assert json.loads(read(key)) == expected and json.loads(public(key)) == expected
            aliases[key] = {'contract': expected['contract'], 'canonical': expected['canonical'], 'matches': True}
        independent = independent_arithmetic(packets['holdings'], read)
        def check_denial(key):
            assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key), 'Protected original anonymously accessible'
        with ThreadPoolExecutor(max_workers=8) as pool: list(pool.map(check_denial, sorted(protected_keys)))
        proof = {'contract': 'etf-holdings-native-acceptance.v1', 'generated_at': datetime.now(timezone.utc).isoformat(), 'commit': COMMIT,
            'runtime_packages': runtimes, 'publications': hashes, 'requests': requests, 'execution_profiles': profiles, 'source_replays': replays,
            'independent_source_arithmetic': independent, 'lookthrough_is_same_canonical_evidence': True, 'schedules': schedules, 'compatibility_aliases': aliases,
            'protected_artifacts_checked': len(protected_keys), 'protected_artifacts_anonymously_denied': True,
            'audit_originals_replayed': audit_replays, 'whole_predecessors_preserved': True,
            'producer_invocations_this_acceptance': sum(v['invoke_sent'] for v in requests.values()),
            'provider_requests': requests[PRODUCERS[0]]['status']['provider_requests'],
            'provider_requests_this_acceptance': sum(v['status']['provider_requests_this_execution'] for v in requests.values()),
            'recovered_original_run': RECOVERY, 'private_account_reads': 0,
            'paid_ai_calls': 0, 'notifications_sent': 0, 'signals_emitted': 0, 'portfolio_writes': 0, 'unreviewed_consumer_invocations': 0,
            'pages_commit': pages_commit, 'assets': {name: build['files_sha256'][name] for name in ASSETS}}
        raw = model.encoded(proof); key = 'data/etf-holdings-research-verification.json'
        s3.put_object(Bucket=BUCKET, Key=key, Body=raw, ContentType='application/json', CacheControl='no-store')
        assert public(key) == raw
        r.kv(proof_key=key, source_replays=replays, publications=hashes, independent_source_arithmetic=independent,
            execution_profiles=profiles, compatibility_aliases_verified=len(aliases), protected_artifacts_checked=len(protected_keys),
            originals_anonymously_denied=True, whole_predecessors_preserved=True, schedules=schedules,
            provider_requests=proof['provider_requests'], provider_requests_this_acceptance=proof['provider_requests_this_acceptance'],
            recovered_original_run=RECOVERY, private_account_reads=0, paid_ai_calls=0,
            producer_invocations_this_acceptance=proof['producer_invocations_this_acceptance'],
            notifications_sent=0, signals_emitted=0, portfolio_writes=0, unreviewed_consumer_invocations=0)


if __name__ == '__main__':
    try: main()
    except Exception: sys.exit(1)
