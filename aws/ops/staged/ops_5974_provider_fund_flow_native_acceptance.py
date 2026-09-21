"""Verify exact provider-flow packages, original replay and two public producers.

The existing provider subscription is read by one durable fund-flow request.
No paid AI, private account, portfolio mutation or decision consumer invocation.
"""
from pathlib import Path
from datetime import datetime,timezone
from decimal import Decimal,localcontext
from concurrent.futures import ThreadPoolExecutor
import base64,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts')]
from ops_report import report
from release_package_evidence import shared_imports
from acceptance_invoke import invoke_when_available
import provider_flow_store as store
import provider_flow_model as model
import provider_flow_native as native
import provider_flow_catalog as catalog
from replay_provider_flow_research import verify as replay_verify
BUCKET='justhodl-dashboard-live'
COMMIT='f93fac34e83a2296f969f67694befe1e7b92d771'
PAGE_COMMIT=COMMIT
FUNCTIONS=('justhodl-ai-rerating-radar', 'justhodl-analytics-snapshot', 'justhodl-apac-flows', 'justhodl-attention-confluence', 'justhodl-best-ideas', 'justhodl-best-setups', 'justhodl-bond-desk', 'justhodl-boom-radar', 'justhodl-bottom', 'justhodl-capital-flow-radar', 'justhodl-compound-aggregator', 'justhodl-equity-research', 'justhodl-etf-census', 'justhodl-etf-constituents', 'justhodl-etf-fund-flows', 'justhodl-etf-global-desk', 'justhodl-flow-anomaly-detector', 'justhodl-flow-confluence', 'justhodl-flow-lookthrough', 'justhodl-flows-ai-analysis', 'justhodl-fortress', 'justhodl-impact-graph', 'justhodl-index-recon', 'justhodl-industry-rotation', 'justhodl-katlin', 'justhodl-macro-confluence', 'justhodl-master-ranker', 'justhodl-quantum-desk', 'justhodl-research-critique', 'justhodl-sector-emergence', 'justhodl-stealth-flow', 'justhodl-theme-cascade', 'justhodl-theme-cascade-backtest', 'justhodl-theme-rotation')
PRODUCERS=('justhodl-etf-fund-flows','justhodl-capital-flow-radar')
KINDS=dict(zip(PRODUCERS,('flow','radar')))
ASSETS=('flows.html','capital-flow-radar.html','jh-provider-flows.js','jh-sector-research.css')
AUDIT=('7045bf8d32c3b13e4ef15a22066164e1bc8e8defe737932ec9cc31d4a49ea1b1',22744)

def expected_commit(fn):
    if fn not in FUNCTIONS:raise ValueError('Unreviewed runtime')
    return COMMIT

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
    if fn in PRODUCERS:assert cfg['MemorySize']==2048 and cfg['Timeout']==300
    return {'commit':expected_commit(fn),'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}

def invoke_public(lam,s3,module,fn):
    assert fn in KINDS,'Only the two reviewed public producers may be invoked'
    kind=KINDS[fn]
    request='chatgpt-'+fn+'-'+COMMIT[:12]+'-1';key=module.request_key(kind,request);dispatch_key=module.request_key(kind,request+'-dispatch');sent=False
    claim={'contract':'provider-flow-native-dispatch.v1','request_id':request,'started_at':datetime.now(timezone.utc).isoformat(),'status':'claimed'}
    try:module.status_write(s3,BUCKET,dispatch_key,claim,IfNoneMatch='*')
    except Exception as exc:
        if not module.conflict(exc):raise
        claim=json.loads(module.bounded(s3.get_object(Bucket=BUCKET,Key=dispatch_key)['Body']))
        assert claim['request_id']==request and claim['contract']=='provider-flow-native-dispatch.v1'
    else:
        response,rejected=invoke_when_available(lam,{'FunctionName':fn,'InvocationType':'Event','Payload':model.encoded({'request_id':request})})
        assert response['StatusCode']==202;sent=True
        claim.update(status='accepted_async',throttle_rejections_before_acceptance=rejected);module.status_write(s3,BUCKET,dispatch_key,claim)
    deadline=time.monotonic()+360;status=None
    while time.monotonic()<deadline:
        try:status=json.loads(module.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
        except Exception as exc:
            if not module.missing(exc):raise
        if status and status.get('status') in ('complete','failed'):break
        time.sleep(3)
    assert status and status.get('status')=='complete' and status.get('published') is True,'Native producer did not publish; inspect durable request without reinvoking'
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
    assert all(v.get('memory_mb')==2048 and v.get('max_memory_mb',2048)<2048 and v.get('duration_ms',300000)<300000 for v in values)
    return {'execution_id':execution,'managed_reports':values,'completed_request':True}


def independent_arithmetic(packet,read):
    """Check row identity and arithmetic without using native window/group code."""
    counts={'normalized_rows':0,'original_row_positions':0,'complete_windows':0,
            'incomplete_windows':0,'reconciliations':0,'group_windows':0,'comparison_windows':0}
    samples={}
    with localcontext() as ctx:
        ctx.prec=50
        for ticker,fund in packet['funds'].items():
            ref=fund['history'];raw=read(ref['key'])
            assert len(raw)==ref['bytes'] and model.sha(raw)==ref['sha256']
            history=json.loads(raw);assert history['ticker']==ticker
            originals={}
            for source in history['originals']:
                raw=store.protected(source,read)
                originals[source['page']]={'sha256':source['sha256'],'rows':json.loads(raw,parse_float=Decimal)['results']}
            by_date={row['date']:row for row in history['history']}
            assert len(by_date)==len(history['history'])
            for row in history['history']:
                counts['normalized_rows']+=1
                for position in row['source_rows']:
                    page=originals[position['page']];assert page['sha256']==position['sha256']
                    original=page['rows'][position['row_index']]
                    assert original['composite_ticker']==ticker and original['effective_date']==row['date'] and original['processed_date']==row['processed_date']
                    counts['original_row_positions']+=1
                    if row.get('conflicting_version'):continue
                    for source_name,name in [('fund_flow','flow_decimal'),('nav','nav_decimal'),('shares_outstanding','shares_decimal')]:
                        if source_name in row['invalid_fields']:continue
                        value=original.get(source_name)
                        assert (None if value is None else Decimal(str(value)))==(None if row[name] is None else Decimal(row[name]))
            for family in ('aligned_windows','latest_windows'):
                for window in fund[family].values():
                    if window['status']!='matched_reporting_window':
                        assert window['flow_usd_decimal'] is None;counts['incomplete_windows']+=1;continue
                    dates=window['dates'];assert len(dates)==window['requested_observations'] and len(set(dates))==len(dates)
                    assert dates==sorted(dates) and dates[-1]==window['end_date']
                    rows=[by_date[d] for d in dates]
                    value=sum((Decimal(row['flow_decimal']) for row in rows),Decimal(0))
                    assert value==Decimal(window['flow_usd_decimal'])
                    end=rows[-1];assets=window['end_reported_assets_usd_decimal']
                    if assets is not None:
                        aum=Decimal(end['nav_decimal'])*Decimal(end['shares_decimal'])
                        assert aum==Decimal(assets) and value/aum*100==Decimal(window['flow_to_end_assets_pct_decimal'])
                    else:assert window['flow_to_end_assets_pct_decimal'] is None
                    assert window['source_rows']==[p for row in rows for p in row['source_rows']]
                    counts['complete_windows']+=1
            for row in history['reconciliation']:
                current=by_date[row['date']];previous=by_date[row['previous_date']]
                change=Decimal(current['shares_decimal'])-Decimal(previous['shares_decimal'])
                prior=change*Decimal(previous['nav_decimal']);present=change*Decimal(current['nav_decimal'])
                reported=Decimal(current['flow_decimal'])
                for key,value in {'share_change_decimal':change,'reported_flow_usd_decimal':reported,
                    'prior_nav_valued_change_usd_decimal':prior,'current_nav_valued_change_usd_decimal':present,
                    'reported_minus_prior_nav_change_usd_decimal':reported-prior,
                    'reported_minus_current_nav_change_usd_decimal':reported-present}.items():
                    assert Decimal(row[key])==value
                assert row['corporate_actions_verified'] is False;counts['reconciliations']+=1
            if ticker in ('SPY','VOO','TLT','XLC','SOXL','SQQQ'):
                samples[ticker]={'latest':fund['latest_observation'],'quality':fund['quality'],
                    'common_five_window':{k:fund['aligned_windows']['5'][k] for k in ('status','dates','flow_usd_decimal','reasons')},
                    'latest_reconciliation':history['reconciliation'][-1] if history['reconciliation'] else None}

        def group(window):
            counts['group_windows']+=1
            required=window['required'];included=window['included'];excluded=window['excluded']
            assert required==sorted(set(required)) and included==sorted(set(included))
            assert set(required)==set(included)|set(excluded) and not set(included)&set(excluded)
            values=[];assets=[]
            for ticker in included:
                fund=packet['funds'][ticker]['aligned_windows'][str(window['observations'])]
                assert fund['status']=='matched_reporting_window' and fund['dates']==window['dates']
                values.append(Decimal(fund['flow_usd_decimal']));assets.append(fund['end_reported_assets_usd_decimal'])
            subtotal=sum(values,Decimal(0)) if values else None
            observed=window['observed_subset_flow_usd_decimal']
            assert (None if observed is None else Decimal(observed))==subtotal
            if window['status']=='complete_matched_group':
                assert required and not excluded and Decimal(window['flow_usd_decimal'])==subtotal
                if all(v is not None for v in assets):
                    aum=sum((Decimal(v) for v in assets),Decimal(0));assert Decimal(window['reported_assets_usd_decimal'])==aum
                    assert Decimal(window['flow_to_end_assets_pct_decimal'])==subtotal/aum*100
                else:assert window['reported_assets_usd_decimal'] is None and window['flow_to_end_assets_pct_decimal'] is None
            else:assert window['flow_usd_decimal'] is None
        for window in packet['unique_configured_universe'].values():group(window)
        for windows in packet['categories'].values():
            for window in windows.values():group(window)
        for complex_ in packet['complexes']:
            for window in complex_['windows'].values():group(window)
            five=complex_['windows']['5'];full=complex_['windows']['21'];pace=complex_['recent_minus_prior_pace_usd_per_observation_decimal']
            if pace is not None:
                assert full['dates'][-5:]==five['dates']
                recent=Decimal(five['flow_usd_decimal']);total=Decimal(full['flow_usd_decimal'])
                assert Decimal(pace)==recent/5-(total-recent)/16
        for comparison in [*packet['comparisons'],packet['unique_leveraged']]:
            for window in comparison['windows'].values():
                a,b=window['bull'],window['bear'];group(a);group(b)
                assert not set(a['required'])&set(b['required'])
                if window['status']=='matched':
                    assert a['dates']==b['dates'];left=Decimal(a['flow_usd_decimal']);right=Decimal(b['flow_usd_decimal'])
                    assert Decimal(window['total_reported_fund_flow_usd_decimal'])==left+right
                    assert Decimal(window['bull_minus_bear_flow_usd_decimal'])==left-right
                else:assert window['total_reported_fund_flow_usd_decimal'] is None and window['bull_minus_bear_flow_usd_decimal'] is None
                counts['comparison_windows']+=1
    return {'counts':counts,'samples':samples,'all_source_positions_and_arithmetic_match':True}


def preflight_replay(read,protected_keys):
    digest,size=AUDIT;ref={'key':store.PRIVATE+digest+'.bin','sha256':digest,'bytes':size}
    audit=json.loads(store.protected(ref,read));protected_keys.add(ref['key'])
    assert audit['contract']=='provider-fund-flow-preflight.v1'
    for ref in (*audit['packets'].values(),audit['catalog'],audit['radar_catalog']):
        store.protected(ref,read);protected_keys.add(ref['key'])
    assert json.loads(store.protected(audit['catalog'],read))==catalog.ETF_UNIVERSE
    assert json.loads(store.protected(audit['radar_catalog'],read))=={'COMPLEXES':catalog.COMPLEXES,'SINGLE_STOCK_LEV':catalog.SINGLE_STOCK_LEV}
    assert (len(catalog.ETF_UNIVERSE),len(catalog.COMPLEXES),len(catalog.SINGLE_STOCK_LEV))==(300,46,20)
    results={}
    for ticker,probe in audit['provider_probes'].items():
        assert probe['status']=='retained' and not probe['next_page_present']
        ref=probe['original'];store.protected(ref,read);protected_keys.add(ref['key'])
        query_date=probe['request']['processed_date.lte']
        collection={'ticker':ticker,'query_date':query_date,'completed_at':probe['acquired_at'],'status':'retained',
            'pages':[{'url':native.initial_url(ticker,query_date),'acquired_at':probe['acquired_at'],'original':ref}]}
        restored=native.reconstruct(ticker,collection,read,audit['generated_at'])
        assert restored['quality']['status']=='complete_acquired_history' and len(restored['history'])==probe['rows']
        results[ticker]={'source_rows':len(restored['history']),'latest_effective_date':restored['latest_effective_date'],'replayed':True}
    assert len(results)==6
    return audit,results


def completed_request(s3,module,fn):
    """Recovery reads a completed request; it has no Lambda client or dispatch."""
    assert fn in KINDS,'Only reviewed producer evidence may be read'
    kind=KINDS[fn];request='chatgpt-'+fn+'-'+COMMIT[:12]+'-1'
    key=module.request_key(kind,request)
    claim=json.loads(module.bounded(s3.get_object(Bucket=BUCKET,Key=module.request_key(kind,request+'-dispatch'))['Body']))
    status=json.loads(module.bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
    assert claim.get('contract')=='provider-flow-native-dispatch.v1' and claim.get('request_id')==request
    assert claim.get('status')=='accepted_async' and status.get('request_id')==request and status.get('kind')==kind
    assert status.get('status')=='complete' and status.get('published') is True,'Recovery requires completed publication evidence'
    return {'request_id':request,'status_key':key,'invoke_sent':False,'status':status}


def public_alias_target(key):
    assert key in store.ALIASES,'Only reviewed compatibility aliases'
    # reviewed-artifacts.js intentionally serves both daily aliases from the
    # inspected root object. Preserve that existing diagnostic privacy check.
    return 'etf-flows/daily.json' if key=='data/etf-flows/daily.json' else key


def main(report_name='ops_5974_provider_fund_flow_native_acceptance',resume_only=False):
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=310,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report(report_name) as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_provider_flow_acceptance.py')],cwd=ROOT,check=True)
        for fn in PRODUCERS:subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/fn/'tests/run_tests.py')],cwd=ROOT,check=True)
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS};r.kv(runtimes=runtimes)
        read=store.reader(s3,BUCKET);protected_keys=set();audit,audit_replays=preflight_replay(read,protected_keys)
        r.kv(audit_originals_replayed=audit_replays,retained_audit_manifest=AUDIT[0])
        for fn,name in [('justhodl-etf-fund-flows','etf_fund_flows'),('justhodl-capital-flow-radar','capital_flow_radar')]:
            original=subprocess.check_output(['git','show','98e1ad68a:aws/lambdas/'+fn+'/source/lambda_function.py'],cwd=ROOT)
            assert (ROOT/'aws/lambdas'/fn/'source'/('legacy_'+name+'.py')).read_bytes()==original
        for page in ('flows','capital-flow-radar'):
            assert (ROOT/('docs/legacy/provider-flow-'+page+'-pre-native-20260921.html.txt')).read_bytes()==subprocess.check_output(['git','show','98e1ad68a:'+page+'.html'],cwd=ROOT)
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',PAGE_COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',PAGE_COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name.endswith('.html'):assert b'jh-provider-flows.js?v=20260921-native1' in served
        schedules=[]
        for fn in PRODUCERS:
            cfg=lam.get_function_configuration(FunctionName=fn);names=[]
            for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):names.extend(page['RuleNames'])
            expected={s['name']:s for s in audit['runtimes'][fn]['schedules'] if s['kind']=='EventBridge rule'}
            assert len(expected)==len(audit['runtimes'][fn]['schedules']) and set(names)==set(expected)
            for name in sorted(names):
                rule=events.describe_rule(Name=name);assert rule['State']==expected[name]['state'] and rule['ScheduleExpression']==expected[name]['expression']
                schedules.append({'function':fn,'name':name,'state':rule['State'],'expression':rule['ScheduleExpression'],'changed':False})
        requests={};profiles={};packets={};replays={};hashes={}
        for fn in PRODUCERS:
            kind=KINDS[fn];prefix,current,contract=store.KINDS[kind]
            request=completed_request(s3,store,fn) if resume_only else invoke_public(lam,s3,store,fn)
            requests[fn]=request;r.kv(**{kind+'_request':request})
            profiles[fn]=execution_profile(boto3.client('logs',region_name='us-east-1'),fn,request['status'])
            if kind=='flow':assert request['status']['compatibility_publications']==dict.fromkeys(store.ALIASES,True)
            raw=public(current);packet=json.loads(raw);assert json.loads(read(current))==packet
            assert packet['contract']==contract and model.clock(packet['generated_at'])>=model.clock(request['status']['generated_at'])
            assert datetime.now(timezone.utc)<model.clock(packet['source_valid_until'])
            replay=replay_verify(packet,read);assert replay['no_allocation_authority'] and replay['configured_funds']==300 and replay['configured_complexes']==46
            requested=store.replay(request['status']['replay'],read);assert requested['generated_at']==request['status']['generated_at']
            run=json.loads(read(packet['replay']['manifest_key']));inputs=store.checked(run['input'],prefix,'inputs',read)
            refs=([*inputs['contexts'].values(),inputs['previous']] if kind=='flow' else [inputs['canonical_source'],inputs['previous']])
            for ref in refs:
                if ref:store.protected(ref,read);protected_keys.add(ref['key'])
            if kind=='flow':
                for collection in inputs['collections'].values():
                    for page in collection['pages']:store.protected(page['original'],read);protected_keys.add(page['original']['key'])
                    if collection.get('rejected_original'):
                        ref=collection['rejected_original'];store.protected(ref,read);protected_keys.add(ref['key'])
                migration=json.loads(read(store.MIGRATION));store.protected(migration,read);protected_keys.add(migration['key'])
            packets[kind]=packet;replays[kind]=replay;hashes[kind]={'key':current,'sha256':model.sha(raw),'bytes':len(raw),'replay':packet['replay']}
        canonical=store.replay(packets['radar']['canonical_replay'],read)
        for field in ('funds','complexes','comparisons','unique_leveraged','unique_configured_universe','reference'):
            assert packets['radar'][field]==canonical[field]==packets['flow'][field]
        aliases={}
        for key in store.ALIASES:
            assert json.loads(read(key))==model.compatibility(packets['flow'],key)
            target=public_alias_target(key);actual=json.loads(public(key))
            assert actual==model.compatibility(packets['flow'],target)
            aliases[key]={'contract':actual['contract'],'canonical':actual['canonical'],'stored_key':key,
                'public_source_key':target,'all_300_identities_preserved':len(actual['inventory'])==300}
        independent=independent_arithmetic(packets['flow'],read)
        def check_denial(key):
            assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key),'Protected original anonymously accessible'
        with ThreadPoolExecutor(max_workers=8) as pool:list(pool.map(check_denial,sorted(protected_keys)))
        proof={'contract':'provider-flow-native-acceptance.v1','generated_at':datetime.now(timezone.utc).isoformat(),'commit':COMMIT,
            'runtime_packages':runtimes,'publications':hashes,'requests':requests,'execution_profiles':profiles,'source_replays':replays,
            'independent_source_arithmetic':independent,'radar_is_same_canonical_evidence':True,'schedules':schedules,'compatibility_aliases':aliases,
            'protected_artifacts_checked':len(protected_keys),'protected_artifacts_anonymously_denied':True,
            'audit_originals_replayed':audit_replays,'whole_predecessors_preserved':True,
            'provider_requests':requests[PRODUCERS[0]]['status']['provider_requests'],
            'acceptance_mode':'completed_publication_replay' if resume_only else 'initial_publication',
            'producer_invocations_this_acceptance':sum(v['invoke_sent'] for v in requests.values()),
            'provider_requests_this_acceptance':0 if resume_only else requests[PRODUCERS[0]]['status']['provider_requests'],'private_account_reads':0,
            'paid_ai_calls':0,'notifications_sent':0,'signals_emitted':0,'portfolio_writes':0,'unreviewed_consumer_invocations':0,
            'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in ASSETS}}
        raw=model.encoded(proof);key='data/provider-flow-research-verification.json'
        s3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType='application/json',CacheControl='no-store')
        assert public(key)==raw
        r.kv(proof_key=key,source_replays=replays,publications=hashes,independent_source_arithmetic=independent,
            execution_profiles=profiles,compatibility_aliases_verified=len(aliases),protected_artifacts_checked=len(protected_keys),
            originals_anonymously_denied=True,whole_predecessors_preserved=True,schedules=schedules,
            provider_requests=proof['provider_requests'],private_account_reads=0,paid_ai_calls=0,
            producer_invocations_this_acceptance=proof['producer_invocations_this_acceptance'],
            provider_requests_this_acceptance=proof['provider_requests_this_acceptance'],
            notifications_sent=0,signals_emitted=0,portfolio_writes=0,unreviewed_consumer_invocations=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
