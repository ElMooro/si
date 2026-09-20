"""Accept exact native breadth runtime and one idempotent public-only acquisition."""
from datetime import datetime, timezone
from pathlib import Path
import base64, hashlib, io, json, re, subprocess, sys, time, urllib.request, urllib.error, zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
SOURCE=ROOT/'aws/lambdas/justhodl-market-internals/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(SOURCE)]
from ops_report import report
from release_package_evidence import shared_imports
from replay_breadth_research import verify
import breadth_research_model as model
import breadth_research_store as store
from breadth_research_store import bounded
COMMIT='245e5e386bca56324da8db2a5468296fc27972ba'
ASSET_COMMIT='3a3408be40f4c60c41beab29bdbab6952894534e'
BUCKET='justhodl-dashboard-live';FN='justhodl-market-internals'
FUNCTIONS=('justhodl-market-internals','justhodl-playbook-engine','justhodl-signal-board','justhodl-symbol-dictionary','justhodl-thesis-engine','justhodl-wl-engines')
ASSETS=('market-internals.html','jh-breadth-research.js')
REQUEST_ID='chatgpt-native-breadth-'+COMMIT[:12]+'-1'


def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as response:
        return bounded(response)


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def runtime(lam,fn):
    source=ROOT/'aws/lambdas'/fn/'source';config_path=source.parent/'config.json'
    configuration=json.loads(config_path.read_bytes()) if config_path.exists() else {}
    request={'FunctionName':fn}
    if configuration.get('release_validation') or fn in ('justhodl-engine-fusion','justhodl-khalid-risk'):request['Qualifier']='live'
    deployed=lam.get_function(**request);cfg=deployed['Configuration'];receipt=json.loads(public('data/ops/releases/'+fn+'.json'))
    expected_commit=COMMIT if fn==FN else ASSET_COMMIT
    assert receipt['commit']==expected_commit and receipt['code_sha256']==cfg['CodeSha256'],'Exact receipt required: '+fn
    assert cfg['State']=='Active' and cfg['LastUpdateStatus']=='Successful'
    archive=bounded(urllib.request.urlopen(deployed['Code']['Location'],timeout=45))
    assert base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256'],'AWS archive hash differs: '+fn
    files=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(source.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
    expected={p.relative_to(source).as_posix():p for p in files}
    expected.update({p.name:p for p in shared_imports(ROOT,files) if not (source/p.name).exists()})
    with zipfile.ZipFile(io.BytesIO(archive)) as z:
        for name,path in expected.items():assert z.read(name)==path.read_bytes(),'Packaged source differs: '+fn+'/'+name
    alias=None
    if request.get('Qualifier'):
        a=lam.get_alias(FunctionName=fn,Name='live')
        assert a['FunctionVersion']==cfg['Version'] and not (a.get('RoutingConfig') or {}).get('AdditionalVersionWeights')
        alias={'name':'live','version':cfg['Version'],'weighted_secondary_versions':0}
    if fn==FN:assert cfg['MemorySize']==3008 and cfg['Timeout']==900
    return {'commit':expected_commit,'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}


def schedules(events):
    records={};inventory={}
    for name in ('market-internals-daily','justhodl-market-internals-daily','market-internals-hourly'):
        rule=events.describe_rule(Name=name);targets=[]
        for page in events.get_paginator('list_targets_by_rule').paginate(Rule=name):targets+=page.get('Targets',[])
        bound=[t for t in targets if t['Arn'].split(':function:')[-1].split(':')[0]==FN]
        assert all(t['Arn'].endswith(':function:'+FN) for t in bound),'Unexpected qualified breadth target; inspect before mutation'
        inventory[name]=(rule,targets,bound)
    # All bindings are inspected before any target is removed.
    rule,targets,bound=inventory['market-internals-daily']
    assert rule['State']=='ENABLED' and rule['ScheduleExpression']=='cron(40 12 ? * TUE-SAT *)' and len(bound)==1
    assert len(targets)==1 and not bound[0].get('InputTransformer')
    payload=json.loads(bound[0].get('Input') or '{}');assert not payload.get('request_id') and not payload.get('validate_only')
    records['market-internals-daily']={'state':rule['State'],'expression':rule['ScheduleExpression'],'bound_targets':1}
    for name in ('justhodl-market-internals-daily','market-internals-hourly'):
        rule,targets,bound=inventory[name]
        if bound:
            response=events.remove_targets(Rule=name,Ids=[t['Id'] for t in bound]);assert response.get('FailedEntryCount',0)==0
        after=[]
        for page in events.get_paginator('list_targets_by_rule').paginate(Rule=name):after+=page.get('Targets',[])
        expected=[t for t in targets if t not in bound]
        assert sorted(after,key=lambda t:t['Id'])==sorted(expected,key=lambda t:t['Id'])
        if not after:events.disable_rule(Name=name)
        records[name]={'removed_breadth_targets':len(bound),'remaining_other_targets':len(after),'state':events.describe_rule(Name=name)['State']}
    return records


def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=45,connect_timeout=10,retries={'max_attempts':0},tcp_keepalive=True))
    s3=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=8,read_timeout=45,tcp_keepalive=True))
    events=boto3.client('events',region_name='us-east-1')
    with report('ops_5924_breadth_corrected_acceptance') as r:
        subprocess.run([sys.executable,str(SOURCE.parent/'tests/run_tests.py')],cwd=ROOT,check=True)
        portable=(SOURCE.parent/'tests/fixtures/expected-output.sha256').read_text().strip()
        assert portable=='7319864a6d79a0357cafcb2a3dd07f8d3434958f1c13ffcb384d5d3b74ac7c7b'
        runtimes={fn:runtime(lam,fn) for fn in FUNCTIONS}
        r.kv(runtimes=runtimes,portable_reference_digest=portable,linux_fixture_replay='exact_match',
            acceptance_consumer_invocations=0,private_account_reads=0,notifications_sent=0,portfolio_writes=0,paid_ai_calls=0)
        legacy=subprocess.check_output(['git','show','aa514476671f6e7f36dc2bf3ff5f8af0a14b230d:aws/lambdas/justhodl-market-internals/source/lambda_function.py'],cwd=ROOT)
        assert (SOURCE/'legacy_market_internals.py').read_bytes()==legacy
        for digest,count in [('57c3c60c1aed3cbd23398a824747ce68c60288450b4a86f9b3dbc8805b573eb7',293677),
            ('0210171775f01fdc56da41ae94c875a1b5f1f673fd5ba2af28e152827113ba5e',4435276)]:
            key='audit-private/20260909-originals/market-cycle/'+digest+'.bin'
            raw=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']);assert len(raw)==count and model.sha(raw)==digest
            assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',ASSET_COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',ASSET_COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.sha(clean)==build['files_sha256'][name],'Built asset differs: '+name
            if name=='market-internals.html':assert b'src="/jh-breadth-research.js?v=20260920-native1"' in served
        schedule=schedules(events);r.kv(pages_commit=pages_commit,schedules=schedule)
        # Exercise the corrected compiler on the complete original response set
        # that triggered the first failure, before another provider acquisition.
        original_run='data/breadth-research/runs/2afad1ac241b022981b96d7433ed33fd418177fe7f88731f32b3ba6158dde65d.json'
        read=store.reader(s3,BUCKET);old_raw=read(original_run);old_manifest=json.loads(old_raw)
        assert original_run==store.PREFIX+'runs/'+model.sha(old_raw)+'.json'
        old_inputs=store.checked(old_manifest['input'],'inputs',read)
        reconstructed=store.compile_output(old_inputs,read)
        r.kv(corrected_retained_reconstruction={'quality':reconstructed['quality'],
            'source_errors':reconstructed['source_errors'],'latest':reconstructed['latest'],
            'current_population':reconstructed['coverage'][reconstructed['as_of']]})
        assert reconstructed['quality']['status']=='fresh' and len(reconstructed['source_evidence'])==253,'Inspect retained originals; no new invocation has occurred'
        key=store.request_key(REQUEST_ID)
        try:status=json.loads(bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
            # One acceptance attempt. SDK retries disabled; an uncertain response
            # is investigated through the exact request status, never a fresh id.
            response=lam.invoke(FunctionName=FN,InvocationType='Event',Payload=model.encoded({'request_id':REQUEST_ID}))
            assert response['StatusCode']==202
            r.kv(accepted_public_request={'request_id':REQUEST_ID,'status_key':key,'aws_transport_request_id':response.get('ResponseMetadata',{}).get('RequestId')})
            status=None
        deadline=time.monotonic()+930
        while time.monotonic()<deadline:
            try:status=json.loads(public(key))
            except urllib.error.HTTPError as exc:
                if exc.code not in (401,403,404):raise
            if status and status.get('status') in ('complete','failed'):break
            time.sleep(8)
        r.kv(exact_public_request=status)
        assert status and status.get('request_id')==REQUEST_ID and status.get('status')=='complete' and status.get('published') is True,'Inspect request and CloudWatch execution before any further invocation'
        raw=public(store.CURRENT);packet=json.loads(raw)
        assert packet['replay']==status['replay'] and packet['as_of']==model.sessions(packet['generated_at'])[-1]
        replayed=verify(packet,store.reader(s3,BUCKET))
        assert packet['quality']['status']=='fresh' and len(packet['source_evidence'])==253 and not packet['source_errors']
        assert len(packet['current_constituents'])>5000
        c=packet['coverage'][packet['as_of']]
        assert c['sma50_denominator']>0 and c['sma200_denominator']>0 and c['prior252_denominator']>0
        assert all(packet[k] is False for k in ('calls_eligible','forecast_eligible','sizing_eligible','execution_eligible'))
        for name in ('ADVANCERS','DECLINERS','UNCHANGED','PCT_ABOVE_50DMA','PCT_ABOVE_200DMA','NEW_HIGHS','NEW_LOWS'):
            assert packet['latest'][name][1] is not None
        newest=packet['source_evidence'][packet['as_of']]
        assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+newest['key']) and denied('https://justhodl.ai/'+newest['key'])
        for path in (store.CURRENT,key):
            with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+path,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as response:
                assert response.headers.get('Cache-Control')=='no-store'
        # Recheck runtime, exact receipt and current bytes after retained-source replay.
        assert runtime(lam,FN)==runtimes[FN] and public(store.CURRENT)==raw
        proof={'contract':'breadth-native-acceptance.v1','verified_at':datetime.now(timezone.utc).isoformat(),
            'runtime_commit':COMMIT,'runtimes':runtimes,'pages_commit':pages_commit,'assets':{k:build['files_sha256'][k] for k in ASSETS},
            'portable_reference_digest':portable,'linux_fixture_replay':'exact_match','source_replay':replayed,
            'public_sha256':model.sha(raw),'replay':packet['replay'],'current_population':c,'latest':packet['latest'],
            'source_originals_private':True,'source_request':status,'schedules':schedule,'controlled_consumer_invocations':0,
            'private_account_reads':0,'notifications_sent':0,'portfolio_writes':0,'paid_ai_calls':0,
            'remaining':'253-session native current-vintage reconstruction; older rolling history remains archived and unqualified. Full historical identity and point-in-time forecast qualification remain open.'}
        s3.put_object(Bucket=BUCKET,Key='data/breadth-research-verification.json',Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public('data/breadth-research-verification.json'))==proof
        r.kv(accepted=True,proof_key='data/breadth-research-verification.json',native_replay=replayed,current_population=c,latest=packet['latest'])


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
