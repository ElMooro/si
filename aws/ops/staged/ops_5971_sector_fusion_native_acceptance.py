"""Verify native sector packages and two reviewed public research publications.

No provider refresh, private account, paid AI, notification, portfolio write or
cadence mutation. Each allowed invocation has a durable dispatch claim first.
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
import sector_fusion_store as store
import sector_fusion_model as model
from replay_sector_fusion_research import verify as replay_verify
BUCKET='justhodl-dashboard-live'
COMMIT='830ef1a195e591fd56658dbd03c87581f6ca0955'
PAGE_COMMIT=COMMIT
FUNCTIONS=('justhodl-sector-flow-state','justhodl-sector-capital-fusion')
KINDS=dict(zip(FUNCTIONS,('flow','capital')))
ASSETS=('sector-flow.html','jh-sector-fusion.js','jh-sector-research.css')
AUDIT=('fac46105f766fb79c7d749995287e1e7085b2b8562f241bac1837c5016a14b75',83027)

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
    if fn in FUNCTIONS:assert cfg['MemorySize']==2048 and cfg['Timeout']==300
    return {'commit':expected_commit(fn),'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}

def protected(ref,read):
    assert ref['key']==model.PRIVATE+ref['sha256']+'.bin'
    raw=read(ref['key']);assert len(raw)==ref['bytes'] and model.sha(raw)==ref['sha256']
    return raw

def invoke_public(lam,s3,module,fn):
    assert fn in KINDS,'Only the two reviewed public producers may be invoked'
    kind=KINDS[fn]
    request='chatgpt-'+fn+'-'+COMMIT[:12]+'-1';key=module.request_key(kind,request);dispatch_key=module.request_key(kind,request+'-dispatch');sent=False
    claim={'contract':'sector-fusion-native-dispatch.v1','request_id':request,'started_at':datetime.now(timezone.utc).isoformat(),'status':'claimed'}
    try:module.status_write(s3,BUCKET,dispatch_key,claim,IfNoneMatch='*')
    except Exception as exc:
        if not module.conflict(exc):raise
        claim=json.loads(module.bounded(s3.get_object(Bucket=BUCKET,Key=dispatch_key)['Body']))
        assert claim['request_id']==request and claim['contract']=='sector-fusion-native-dispatch.v1'
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


def independent_sample(packet,read):
    rotation=store.prices.replay(packet['source_clocks']['prices']['replay'],read)
    rows={r['symbol']:r for r in packet['sectors']};out={};returns={}
    with localcontext() as ctx:
        ctx.prec=50
        for ticker,row in rows.items():
            w=row['windows']['5d'];price=w['price'];issue=w['issuer'];source=row['issuer_source']['source']
            raw=store.issuer.native.original(source,read,packet['source_clocks']['issuer']['generated_at'],row['issuer_identity']['history_url'])
            original=store.issuer.native.ssga_history(raw,row['issuer_identity'],packet['source_clocks']['issuer']['generated_at'])['rows']
            selected=[r for r in original if issue['start_date']<=r['date']<=issue['end_date']]
            assert len(selected)==6 and selected[0]['date']==issue['start_date'] and selected[-1]['date']==issue['end_date']
            value=sum((Decimal(r['nav_decimal'])*(Decimal(r['shares_decimal'])-Decimal(p['shares_decimal'])) for p,r in zip(selected,selected[1:])),Decimal(0))
            sensitivity=sum(((Decimal(r['share_display_quantum_decimal'])+Decimal(p['share_display_quantum_decimal']))*Decimal(r['nav_decimal'])+abs(Decimal(r['shares_decimal'])-Decimal(p['shares_decimal']))*Decimal(r['nav_display_quantum_decimal']) for p,r in zip(selected,selected[1:])),Decimal(0))
            assert value==Decimal(issue['value_decimal']) and sensitivity==Decimal(issue['precision_sensitivity_decimal'])
            source_price=rotation['observations'][ticker]['source'];bars=json.loads(read(source_price['key']),parse_float=Decimal)['results']
            points=price['endpoints'];a=bars[points['fund_start']['original_row_index']]['c'];b=bars[points['fund_end']['original_row_index']]['c']
            assert Decimal(str(a))==Decimal(str(points['fund_start']['close'])) and Decimal(str(b))==Decimal(str(points['fund_end']['close']))
            ret=Decimal(str(b))/Decimal(str(a))-1;assert abs(ret*100-Decimal(price['exact']['price_return_pct']))<Decimal('1e-25');returns[ticker]=ret
            out[ticker]={'period':[issue['start_date'],issue['end_date']],'issuer_value_usd':str(value),'display_sensitivity_usd':str(sensitivity),
                'price_return_percent':str(ret*100),'comparison':w['comparison']['status'],'exact_source_rows_checked':True}
        pnl=returns['XLK']*100000-returns['XLF']*50000
        out['illustrative_portfolio']={'XLK_starting_usd':100000,'XLF_starting_usd':-50000,'historical_price_pnl_usd':str(pnl),'reversed_exposures_price_pnl_usd':str(-pnl),
            'gross_starting_usd':150000,'net_starting_usd':50000,'scope':'Retrospective price-only sample before dividends and all costs; no investment instruction.'}
    return out


def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=310,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5971_sector_fusion_native_acceptance') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_sector_fusion_acceptance.py')],cwd=ROOT,check=True)
        for fn in FUNCTIONS:subprocess.run([sys.executable,str(ROOT/'aws/lambdas'/fn/'tests/run_tests.py')],cwd=ROOT,check=True)
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS};r.kv(runtimes=runtimes)
        read=store.reader(s3,BUCKET);digest,size=AUDIT;key=model.PRIVATE+digest+'.bin';raw=read(key)
        assert len(raw)==size and model.sha(raw)==digest; audit=json.loads(raw);protected_keys={key}
        for ref in (*audit['packets'].values(),*audit['source_artifacts'].values()):protected(ref,read);protected_keys.add(ref['key'])
        assert len(audit['packets'])==27 and len(audit['source_artifacts'])==134
        references={ref['key']:ref for ref in (*audit['packets'].values(),*audit['source_artifacts'].values())}
        def audit_read(key):
            ref=audit['source_artifacts'].get(key) or references.get(key)
            assert ref,'Unretained audit source path';return protected(ref,read)
        inputs={'contract':'sector-fusion-inputs.v1','kind':'flow','generated_at':audit['generated_at'],
            'sources':{key:{**ref,'source_key':key} for key,ref in audit['packets'].items()}}
        restored=store.compile_output(inputs,audit_read);assert restored['quality']['issuer_price_five_window_available']==11
        r.kv(audited_native_candidate_replayed=True,retained_audit_manifest=digest)
        for fn,name in [('justhodl-sector-flow-state','sector_flow_state'),('justhodl-sector-capital-fusion','sector_capital_fusion')]:
            original=subprocess.check_output(['git','show','5900e6837:aws/lambdas/'+fn+'/source/lambda_function.py'],cwd=ROOT)
            assert (ROOT/'aws/lambdas'/fn/'source'/('legacy_'+name+'.py')).read_bytes()==original
        assert (ROOT/'docs/legacy/sector-flow-pre-native-20260921.html.txt').read_bytes()==subprocess.check_output(['git','show','5900e6837:sector-flow.html'],cwd=ROOT)
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',PAGE_COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',PAGE_COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name=='sector-flow.html':assert b'jh-sector-fusion.js?v=20260921-native1' in served and b'/insider-research.html' in served
        schedules=[]
        for fn in FUNCTIONS:
            cfg=lam.get_function_configuration(FunctionName=fn);names=[]
            for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):names.extend(page['RuleNames'])
            expected={s['name']:s for s in audit['runtimes'][fn]['schedules'] if s['kind']=='EventBridge rule'}
            assert set(names)==set(expected)
            for name in sorted(names):
                rule=events.describe_rule(Name=name);assert rule['State']==expected[name]['state'] and rule['ScheduleExpression']==expected[name]['expression']
                schedules.append({'function':fn,'name':name,'state':rule['State'],'expression':rule['ScheduleExpression'],'changed':False})
        requests={};profiles={};packets={};replays={};hashes={}
        for fn in FUNCTIONS:
            kind=KINDS[fn];prefix,current,contract=store.KINDS[kind]
            request=invoke_public(lam,s3,store,fn);requests[fn]=request;r.kv(**{kind+'_request':request})
            profiles[fn]=execution_profile(boto3.client('logs',region_name='us-east-1'),fn,request['status'])
            raw=public(current);packet=json.loads(raw);assert json.loads(read(current))==packet
            assert packet['contract']==contract and model.clock(packet['generated_at'])>=model.clock(request['status']['generated_at'])
            assert datetime.now(timezone.utc)<model.clock(packet['source_valid_until'])
            replay=replay_verify(packet,read);assert replay['no_allocation_authority'] and replay['selected_issuer_price_windows']==11
            assert packet['quality']['independent_investment_votes']==0 and len(packet['sectors'])==11
            requested=store.replay(request['status']['replay'],read);assert requested['generated_at']==request['status']['generated_at']
            run=json.loads(read(packet['replay']['manifest_key']));inputs=store.checked(run['input'],prefix,'inputs',read)
            for ref in inputs['sources'].values():
                if ref:protected(ref,read);protected_keys.add(ref['key'])
            packets[kind]=packet;replays[kind]=replay;hashes[kind]={'key':current,'sha256':model.sha(raw),'bytes':len(raw),'replay':packet['replay']}
        capital=packets['capital'];canonical=store.replay(capital['canonical_replay'],read)
        assert capital['sectors']==canonical['sectors'] and capital['source_clocks']==canonical['source_clocks']
        independent=independent_sample(packets['flow'],read)
        for key in sorted(protected_keys):assert denied('https://justhodl.ai/'+key) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        proof={'contract':'sector-fusion-native-acceptance.v1','generated_at':datetime.now(timezone.utc).isoformat(),'commit':COMMIT,
            'runtime_packages':runtimes,'publications':hashes,'requests':requests,'execution_profiles':profiles,'source_replays':replays,
            'independent_source_arithmetic':independent,'capital_is_same_canonical_evidence':True,'schedules':schedules,
            'protected_artifacts_checked':len(protected_keys),'protected_artifacts_anonymously_denied':True,
            'audit_originals_replayed':True,'whole_predecessors_preserved':True,'provider_requests':0,'private_account_reads':0,
            'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,'unreviewed_consumer_invocations':0,
            'pages_commit':pages_commit,'assets':{name:build['files_sha256'][name] for name in ASSETS}}
        raw=model.encoded(proof);key='data/sector-fusion-research-verification.json'
        s3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType='application/json',CacheControl='no-store')
        assert public(key)==raw
        r.kv(proof_key=key,source_replays=replays,publications=hashes,independent_source_arithmetic=independent,
            execution_profiles=profiles,protected_artifacts_checked=len(protected_keys),originals_anonymously_denied=True,
            whole_predecessors_preserved=True,schedules=schedules,provider_requests=0,private_account_reads=0,paid_ai_calls=0,
            notifications_sent=0,portfolio_writes=0,unreviewed_consumer_invocations=0)

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
