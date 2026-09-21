"""Verify three exact packages and invoke only the reviewed public Money Flow producer.

No provider refresh, account or consumer call, paid AI, notification, portfolio
write or cadence mutation. Durable dispatch identity precedes the only invocation.
"""
from pathlib import Path
from datetime import datetime,timezone
from decimal import Decimal,localcontext
import base64,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts')]
from ops_report import report
from release_package_evidence import shared_imports
from acceptance_invoke import invoke_when_available
import money_volume_store as store
import money_volume_model as model
import money_volume_source as source
from replay_money_volume_research import verify as replay_verify
BUCKET='justhodl-dashboard-live'
COMMIT='e4a51f15a09817e976f7038f0315594ab263fd2e'
PAGE_COMMIT=COMMIT
FUNCTIONS=('justhodl-money-flow-state','justhodl-sector-flow-state','justhodl-sector-capital-fusion')
ASSETS=('money-flow.html','jh-money-volume.js','jh-sector-research.css','sector-flow.html')
AUDIT=('2d43877681ce71bc82a51eee1eaff5487ad909921038c2e7b9c2ff9ad22687ba',67052)

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
    if fn=='justhodl-money-flow-state':assert cfg['MemorySize']==1024 and cfg['Timeout']==300
    return {'commit':expected_commit(fn),'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}

def protected(ref,read):
    assert ref['key']==model.PRIVATE+ref['sha256']+'.bin'
    raw=read(ref['key']);assert len(raw)==ref['bytes'] and model.sha(raw)==ref['sha256']
    return raw

def invoke_public(lam,s3,module,fn):
    assert fn=='justhodl-money-flow-state','Only the reviewed public producer may be invoked'
    request='chatgpt-'+fn+'-'+COMMIT[:12]+'-1';key=module.request_key(request);dispatch_key=module.request_key(request+'-dispatch');sent=False
    claim={'contract':'money-volume-native-dispatch.v1','request_id':request,'started_at':datetime.now(timezone.utc).isoformat(),'status':'claimed'}
    try:module.status_write(s3,BUCKET,dispatch_key,claim,IfNoneMatch='*')
    except Exception as exc:
        if not module.conflict(exc):raise
        claim=json.loads(module.bounded(s3.get_object(Bucket=BUCKET,Key=dispatch_key)['Body']))
        assert claim['request_id']==request and claim['contract']=='money-volume-native-dispatch.v1'
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
    assert all(v.get('memory_mb')==1024 and v.get('max_memory_mb',1024)<1024 and v.get('duration_ms',300000)<300000 for v in values)
    return {'execution_id':execution,'managed_reports':values,'completed_request':True}


def independent_sample(packet,read):
    selected=[r for r in packet['stocks'] if r['calculation_complete']][:3]
    assert len(selected)==3
    samples={r['ticker']:[] for r in selected}
    for day,item in packet['source_evidence'].items():
        ref=item['source'];raw=read(ref['key']);assert model.sha(raw)==ref['sha256'] and len(raw)==ref['bytes']
        doc=json.loads(raw,parse_float=Decimal)
        for row in selected:
            idx=row['source_row_indices'][packet['period']['session_dates'].index(day)]
            original=doc['results'][idx];assert original['T']==row['ticker']
            samples[row['ticker']].append(original)
    out={}
    with localcontext() as ctx:
        ctx.prec=40
        for row in selected:
            bars=samples[row['ticker']];ret=Decimal(str(bars[-1]['c']))/Decimal(str(bars[0]['c']))-1
            turnovers=[Decimal(str(v['v']))*Decimal(str(v['vw'])) if v['v'] else Decimal(0) for v in bars]
            mean=sum(turnovers)/6;pressure=ret*mean
            assert abs(pressure-Decimal(row['exact']['price_volume_pressure_usd_proxy']))<Decimal('0.000001')
            assert abs(ret*100-Decimal(row['exact']['price_return_pct']))<Decimal('0.000001')
            buckets={'up':Decimal(0),'down':Decimal(0),'unchanged':Decimal(0)}
            for i in range(1,6):buckets['up' if bars[i]['c']>bars[i-1]['c'] else 'down' if bars[i]['c']<bars[i-1]['c'] else 'unchanged']+=turnovers[i]
            for name,v in buckets.items():assert abs(v-Decimal(row['exact'][name+'_close_turnover_usd_proxy']))<Decimal('0.000001')
            out[row['ticker']]={'price_return_percent':str(ret*100),'mean_turnover_proxy_usd':str(mean),'pressure_proxy_usd':str(pressure),
                'illustrative_positive_100000_usd_price_pnl':str(ret*100000),'illustrative_negative_100000_usd_price_pnl':str(-ret*100000)}
    assert sum(r['included_count'] for r in packet['sector_measurements'])==packet['quality']['complete_stocks']
    return out


def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=310,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5969_money_volume_native_acceptance') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_money_volume_acceptance.py')],cwd=ROOT,check=True)
        subprocess.run([sys.executable,str(ROOT/'aws/lambdas/justhodl-money-flow-state/tests/run_tests.py')],cwd=ROOT,check=True)
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS};r.kv(runtimes=runtimes,acceptance_consumer_invocations=0)
        read=store.reader(s3,BUCKET);digest,size=AUDIT;key=model.PRIVATE+digest+'.bin';raw=read(key)
        assert len(raw)==size and model.sha(raw)==digest
        audit=json.loads(raw);protected_keys={key}
        for ref in (*audit['packets'].values(),*audit['source_artifacts'].values()):protected(ref,read);protected_keys.add(ref['key'])
        assert len(audit['packets'])==15 and len(audit['source_artifacts'])==26
        old_tape=json.loads(protected(audit['packets']['data/market-internals.json'],read))
        audit_read=lambda key:protected(audit['source_artifacts'][key],read)
        old_plan=source.prepare(old_tape,audit_read);old_count=0
        for day in old_plan['days']:
            rows,meta=source.restore_day(old_plan,day,audit_read);assert rows is not None and meta['status']=='original_replayed';old_count+=1
        old_handler=subprocess.check_output(['git','show','b24801bc3:aws/lambdas/justhodl-money-flow-state/source/lambda_function.py'],cwd=ROOT)
        assert len(old_handler)==6199 and (ROOT/'aws/lambdas/justhodl-money-flow-state/source/legacy_money_flow_state.py').read_bytes()==old_handler
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',PAGE_COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',PAGE_COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name=='money-flow.html':assert b'jh-money-volume.js?v=20260921-native1' in served
        schedule=[]
        for fn in FUNCTIONS:
            cfg=lam.get_function_configuration(FunctionName=fn);names=[]
            for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):names.extend(page['RuleNames'])
            expected={s['name']:s for s in audit['runtimes'][fn]['schedules'] if s['kind']=='EventBridge rule'}
            assert set(names)==set(expected)
            for name in sorted(names):
                rule=events.describe_rule(Name=name)
                assert rule['State']==expected[name]['state'] and rule['ScheduleExpression']==expected[name]['expression']
                schedule.append({'function':fn,'name':name,'state':rule['State'],'expression':rule['ScheduleExpression'],'changed':False})
        request=invoke_public(lam,s3,store,'justhodl-money-flow-state');r.kv(request=request)
        profile=execution_profile(boto3.client('logs',region_name='us-east-1'),'justhodl-money-flow-state',request['status'])
        raw=public(store.CURRENT);packet=json.loads(raw);assert json.loads(read(store.CURRENT))==packet
        assert packet['contract']==model.CONTRACT and model.clock(packet['generated_at'])>=model.clock(request['status']['generated_at'])
        assert datetime.now(timezone.utc)<model.clock(packet['source_valid_until'])
        requested=store.replay(request['status']['replay'],read);assert requested['generated_at']==request['status']['generated_at']
        replay=replay_verify(packet,read);assert replay['no_allocation_authority'] and packet['quality']['replayed_sessions']==6
        assert len(packet['stocks'])==packet['quality']['configured_stocks'] and 0<packet['quality']['complete_stocks']<=len(packet['stocks'])
        assert packet['period']['price_intervals']==5 and packet['period']['turnover_sessions']==6
        independent=independent_sample(packet,read)
        run=json.loads(read(packet['replay']['manifest_key']));inputs=store.checked(run['input'],'inputs',read)
        for ref in (inputs['market'],*inputs['legacy'].values()):
            if ref:protected(ref,read);protected_keys.add(ref['key'])
        for item in packet['source_evidence'].values():protected_keys.add(item['source']['key'])
        for key in sorted(protected_keys):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        for fn in FUNCTIONS:
            cfg=lam.get_function_configuration(FunctionName=fn);receipt=json.loads(public('data/ops/releases/'+fn+'.json'))
            assert receipt['commit']==COMMIT and receipt['code_sha256']==cfg['CodeSha256']==runtimes[fn]['code_sha256']
        publication={'generated_at':packet['generated_at'],'source_generated_at':packet['source_generated_at'],'source_valid_until':packet['source_valid_until'],
            'replay':packet['replay'],'quality':packet['quality'],'public_sha256':model.sha(raw),'public_bytes':len(raw)}
        proof={'contract':'money-volume-native-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),'runtime_commit':COMMIT,
            'runtimes':runtimes,'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in ASSETS},'publication':publication,
            'request':request,'execution_profile':profile,'source_replay':replay,'independent_calculations':independent,'schedules':schedule,
            'whole_predecessor':{'bytes':len(old_handler),'sha256':model.sha(old_handler)},'preflight_original_sessions_replayed':old_count,
            'protected_artifacts_checked':len(protected_keys),'whole_originals_anonymously_denied':True,'acceptance_public_producer_invocations':int(request['invoke_sent']),
            'acceptance_consumer_invocations':0,'provider_requests':0,'private_account_reads':0,'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,
            'remaining':'Current retrieved split-adjusted price and turnover proxies with current configured classifications. Not investor net capital flow, order identity, historical availability, forecasting or execution capacity.'}
        key='data/money-volume-research-verification.json';s3.put_object(Bucket=BUCKET,Key=key,Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public(key))==proof
        r.kv(accepted=True,proof_key=key,publication=publication,source_replay=replay,independent_calculations=independent,
            execution_profile=profile,schedules=schedule,protected_artifacts_checked=len(protected_keys),provider_requests=0,
            private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
