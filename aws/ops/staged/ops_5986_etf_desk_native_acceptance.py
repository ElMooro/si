"""Publish the source-qualified ETF desk using retained originals only.

Runner IAM only. One durable public-producer invocation; zero recollection,
private-account reads, paid AI, trading consumers, notifications or orders.
"""
from pathlib import Path
from collections import Counter,defaultdict
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal,localcontext
import json,re,subprocess,sys,time,urllib.request
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from ops_5975_etf_constituent_source_preflight import denied,runtime
from ops_5984_etf_profile_original_replay import independent as independent_profiles
from ops_5974_provider_fund_flow_native_acceptance import independent_arithmetic as independent_flows
from ops_5966_sector_tilt_runtime_diagnosis import parse_runtime
import etf_desk_store as store
import etf_desk_model as model
import etf_holdings_native as native
BUCKET='justhodl-dashboard-live'
FUNCTION='justhodl-etf-global-desk'
PREDECESSOR='22fdf4bc814b1dec081c2a46f913bf1bd416f17a'
RECOVERY={'manifest_key':'data/etf-desk-research/runs/1ed69e0427db3d4a13b73b766d2289f034aa61ce1fb5b11806e93652a94b14f9.json',
    'output_sha256':'3d1c7cadb4e1f159ce558b4bada79f221697ce20374440669f00c9ff7a285537'}
QUALIFICATION={'key':store.PRIVATE+'7565de21b422f84d16c62551adc110c544d8fd9d8aae278128c793b34b2dc5ae.bin',
    'sha256':'7565de21b422f84d16c62551adc110c544d8fd9d8aae278128c793b34b2dc5ae','bytes':86783}
PROOF='data/etf-desk-research-verification.json'
ASSETS=('etf.html','jh-etf-desk-research.js','jh-etf-desk-page.js','jh-etf-holdings.js','jh-sector-research.css',
    'jh-etf-fuse.js','jh-etf-derived.js','jh-chart-etf-desk.js','jh-chart-tvsearch.js','jh-strong-engine.js',
    'bonds.html','chart.html','credit-desk.html','crypto/index.html','factor-regime.html','strong.html','jh-data-feeds.js')


def original_keys(value):
    found=set()
    if isinstance(value,dict):
        for child in value.values():found.update(original_keys(child))
    elif isinstance(value,list):
        for child in value:found.update(original_keys(child))
    elif isinstance(value,str) and value.startswith('audit-private/') and value.endswith('.bin'):found.add(value)
    return found


def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,
            headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as response:
        return store.bounded(response)


def runtime_commit():
    paths=['aws/lambdas/'+FUNCTION+'/source','aws/lambdas/'+FUNCTION+'/config.json']
    paths += ['aws/shared/'+m.__name__+'.py' for m in store.COMPILERS]
    commit=subprocess.check_output(['git','log','-1','--format=%H','--',*paths],cwd=ROOT,text=True).strip()
    assert re.fullmatch('[a-f0-9]{40}',commit)
    return commit


def invoke(lam,s3,commit):
    request='chatgpt-'+FUNCTION+'-'+commit[:12]+'-1'
    key=store.request_key(request);dispatch_key=store.request_key(request+'-dispatch');sent=False
    claim={'contract':'etf-desk-native-dispatch.v1','request_id':request,'started_at':store.now(),'status':'claimed','recovery':RECOVERY}
    try:store.status_write(s3,BUCKET,dispatch_key,claim,IfNoneMatch='*')
    except Exception as exc:
        if not store.conflict(exc):raise
        old=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=dispatch_key)['Body']))
        assert old['request_id']==request and old['contract']==claim['contract'] and old['recovery']==RECOVERY
    else:
        response,rejected=invoke_when_available(lam,{'FunctionName':FUNCTION,'InvocationType':'Event',
            'Payload':model.encoded({'request_id':request,'recover_run':RECOVERY})})
        assert response['StatusCode']==202;sent=True
        claim.update(status='accepted_async',throttle_rejections_before_acceptance=rejected)
        store.status_write(s3,BUCKET,dispatch_key,claim)
    status=None;deadline=time.monotonic()+960
    while time.monotonic()<deadline:
        try:status=json.loads(store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
        if status and status.get('status') in ('complete','failed'):break
        time.sleep(3)
    assert status and status.get('status')=='complete' and status.get('published') is True,'Inspect retained request; never blindly reinvoke'
    assert status['provider_requests_this_execution']==0 and status['provider_requests']==549
    assert status['recovered_from']==RECOVERY and status['replay']['output_sha256']==RECOVERY['output_sha256']
    assert status['generated_at']=='2026-09-21T10:56:47.664299+00:00'
    assert status['compatibility_publications']==dict.fromkeys(store.ALIASES,True)
    return {'request_id':request,'status_key':key,'dispatch_key':dispatch_key,'invoke_sent':sent,'status':status}


def execution_profile(logs,status):
    execution=status['execution_id'];assert re.fullmatch('[a-f0-9-]{36}',execution)
    deadline=time.monotonic()+45;start=model.clock(status['started_at'])
    while True:
        rows=logs.filter_log_events(logGroupName='/aws/lambda/'+FUNCTION,filterPattern='"'+execution+'"',
            startTime=int(start.timestamp()*1000)-5000,limit=50).get('events',[])
        values=[parse_runtime(row.get('message',''),execution) for row in rows];values=[v for v in values if v]
        if values:break
        assert time.monotonic()<deadline,'Read completed execution profile; do not reinvoke'
        time.sleep(3)
    assert all(v.get('status') not in ('error','timeout') and v.get('memory_mb')==4096
        and v.get('max_memory_mb',4096)<4096 and v.get('duration_ms',900000)<900000 for v in values)
    return {'execution_id':execution,'managed_reports':values,'completed_request':True}


def independent_holdings(packet, read):
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
                    original_pages[source['page']] = (source, json.loads(store.holdings_store.protected(source, read), parse_float=Decimal)['results'])
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
    return {"counts": dict(counts), "samples": samples, "all_source_positions_and_arithmetic_match": True}


def independent_desk(packet,inputs,read):
    counts=Counter()
    for ticker,fund in packet['funds'].items():
        for role in ('current','prior'):
            doc=model.checked(fund['profiles'][role]['snapshot'],read)
            counts.update(independent_profiles(inputs['profiles'][ticker][role],doc,read))
    # Adapt only actual fund and total objects to the existing independent arithmetic
    # checker. The desk does not publish sector or bull/bear comparison objects.
    flows=independent_flows({'funds':{t:f['flows'] for t,f in packet['funds'].items()},
        'unique_configured_universe':packet['matched_desk_totals'],'categories':{},'complexes':[],
        'comparisons':[],'unique_leveraged':{'windows':{}}},read)
    holdings=independent_holdings({'funds':{t:f['holdings'] for t,f in packet['funds'].items()}},read)
    return {'profiles':dict(counts),'flows':flows,'holdings':holdings}


def main():
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=24))
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=60,connect_timeout=10,retries={'max_attempts':0}))
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5986_etf_desk_native_acceptance') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_etf_desk_acceptance.py')],cwd=ROOT,check=True)
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/FUNCTION/'tests/run_tests.py')],cwd=ROOT,check=True)
        commit=runtime_commit();actual=runtime(lam,s3,events,scheduler,FUNCTION)
        assert actual['receipt']=={'status':'matched','commit':commit} and actual['timeout']==900 and actual['memory_mb']==4096
        assert json.loads(public('data/ops/releases/'+FUNCTION+'.json'))['commit']==commit
        read=store.reader(s3,BUCKET);qualification=json.loads(store.protected(QUALIFICATION,read))
        assert qualification['candidate_replay']==RECOVERY and actual['schedules']==qualification['predecessor_runtime']['schedules']
        assert (ROOT/'aws/lambdas'/FUNCTION/'source/legacy_etf_global_desk.py').read_bytes()==subprocess.check_output(
            ['git','show',PREDECESSOR+':aws/lambdas/'+FUNCTION+'/source/lambda_function.py'],cwd=ROOT)
        for name in ('etf.html','jh-etf-fuse.js','jh-chart-etf-desk.js','jh-etf-derived.js','jh-etf-engine.js'):
            archived=ROOT/('docs/legacy/etf-desk-'+name+'-pre-native-20260921.txt')
            assert archived.read_bytes()==subprocess.check_output(['git','show',PREDECESSOR+':'+name],cwd=ROOT)
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',commit,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet','HEAD',pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            raw=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',raw)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Live asset differs: '+name
            if name.endswith('.html'):assert b'jh-etf-desk-research.js?v=20260921-native1' in raw
        r.kv(commit=commit,runtime_package=actual,pages_commit=pages_commit,qualified_output_sha256=RECOVERY['output_sha256'])
        # Resolve preserved originals before publishing. No fresh source request is permitted.
        inputs,candidate=store.recovery_inputs(RECOVERY,read)
        canonical_before={key:public(key) for key in (store.flow_model.CURRENT,store.holdings_model.CURRENT,store.holdings_model.LOOK_CURRENT)}
        request=invoke(lam,s3,commit)
        profile=execution_profile(boto3.client('logs',region_name='us-east-1'),request['status'])
        raw=public(model.CURRENT);packet=json.loads(raw)
        assert read(model.CURRENT)==raw and packet['replay']==request['status']['replay']
        assert {k:v for k,v in packet.items() if k!='replay'}==candidate
        assert store.replay(packet['replay'],read)==candidate
        assert len(packet['funds'])==116 and all(packet[k] is False for k in model.PERMISSIONS)
        assert packet['call'] is None and packet['portfolio_action']=='WAIT' and packet['quality']['independent_investment_votes']==0
        arithmetic=independent_desk(packet,inputs,read)
        assert arithmetic['profiles']==qualification['independent_profile_counts']
        protected=original_keys(inputs)|{QUALIFICATION['key'],request['status_key'],request['dispatch_key']}
        for ref in (inputs['canonical_flows'],inputs['canonical_holdings']):
            doc=json.loads(store.protected(ref,read));run=json.loads(read(doc['replay']['manifest_key']))
            protected.update(original_keys(model.checked(run['input'],read)))
        unchanged=0
        for key,ref in inputs['contexts'].items():
            if ref:
                original=store.protected(ref,read)
                if key not in store.ALIASES:
                    assert store.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])==original
                    unchanged+=1
        for key,previous in canonical_before.items():assert public(key)==previous and read(key)==previous
        aliases={}
        for key in store.ALIASES:
            expected=store.compatibility(packet,key)
            assert json.loads(public(key))==expected and json.loads(read(key))==expected
            aliases[key]={'contract':expected['contract'],'canonical':expected['canonical'],'matches':True}
        for key in (model.CURRENT,*store.ALIASES):
            assert s3.head_object(Bucket=BUCKET,Key=key)['CacheControl']=='no-store'
            with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:
                assert 'no-store' in response.headers.get('Cache-Control',''),'Mutable edge response cached: '+key
        def check(key):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=8) as pool:
            for _ in pool.map(check,sorted(protected)):pass
        assert runtime(lam,s3,events,scheduler,FUNCTION)==actual
        proof={'contract':'etf-desk-native-acceptance.v1','generated_at':store.now(),'commit':commit,
            'runtime_package':actual,'request':request,'execution_profile':profile,
            'publication':{'key':model.CURRENT,'sha256':model.sha(raw),'bytes':len(raw),'replay':packet['replay'],'generated_at':packet['generated_at']},
            'qualified_candidate':RECOVERY,'original_replay_matches':True,'independent_source_arithmetic':arithmetic,
            'configured_funds':len(packet['funds']),'whole_predecessors_retained':True,'unchanged_contexts':unchanged,
            'canonical_flow_holdings_and_lookthrough_unchanged':True,'compatibility_aliases':aliases,'unchanged_schedules':actual['schedules'],
            'protected_artifacts_checked':len(protected),'originals_and_requests_anonymously_denied':True,
            'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in ASSETS},
            'producer_invocations_this_acceptance':int(request['invoke_sent']),'provider_requests_this_acceptance':0,
            'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'signals_emitted':0,'portfolio_writes':0,'unreviewed_consumer_invocations':0}
        proof_raw=model.encoded(proof)
        s3.put_object(Bucket=BUCKET,Key=PROOF,Body=proof_raw,ContentType='application/json',CacheControl='no-store')
        assert public(PROOF)==proof_raw
        r.kv(proof_key=PROOF,publication=proof['publication'],independent_source_arithmetic=arithmetic,
            execution_profile=profile,protected_artifacts_checked=len(protected),unchanged_contexts=unchanged,
            compatibility_aliases_verified=len(aliases),provider_requests_this_acceptance=0,
            producer_invocations_this_acceptance=int(request['invoke_sent']),private_account_reads=0,paid_ai_calls=0,
            notifications_sent=0,signals_emitted=0,portfolio_writes=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
