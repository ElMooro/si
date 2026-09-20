"""Accept exact Crisis source/consumer deployment and one public-only native publication."""
from datetime import datetime,timezone
from pathlib import Path
import base64,hashlib,io,json,re,subprocess,sys,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
SOURCE=ROOT/'aws/lambdas/justhodl-crisis-composite/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(SOURCE)]
from ops_report import report
from acceptance_invoke import invoke_when_available
from replay_crisis_research import verify
from release_package_evidence import shared_imports
from crisis_research_store import PRIVATE,bounded
import crisis_research_model as model
COMMIT='0fdc2c779a1ea95a04c035e37ce8294699567cac'
ASSET_COMMIT='64f023cf8f26c99f7e561e97da14689de20ab3d6'
BUCKET='justhodl-dashboard-live';FN='justhodl-crisis-composite'
FUNCTIONS=('justhodl-ai','justhodl-ai-chat','justhodl-allocator','justhodl-calibration-fleet','justhodl-capitulation',
 'justhodl-conviction-engine',FN,'justhodl-cycle-clock','justhodl-desk-allocator','justhodl-engine-fusion','justhodl-global-stress',
 'justhodl-jhsignal-bridge','justhodl-katlin','justhodl-khalid','justhodl-khalid-risk','justhodl-master-allocator','justhodl-master-ranker',
 'justhodl-morning-intelligence','justhodl-near-miss-monitor','justhodl-page-ai-commentary','justhodl-pm-decision',
 'justhodl-prepump-alerts-router','justhodl-regime-conditional-router','justhodl-signal-orthogonality','justhodl-sovereign-stress',
 'justhodl-strategist','justhodl-streaming-fanout','justhodl-wl-fusion')
ASSETS=('defcon.html','crisis.html','jh-crisis-research.js')
PRESERVED={'crisis-composite':('58315b71413891458f3071576b4ead907af2b58b5470d6927753306632580620',4785),
 'defcon':('58315b71413891458f3071576b4ead907af2b58b5470d6927753306632580620',4785),
 'crisis-composite-history':('a3f04ecfa233ecdc7164f1256ffbd8d7aa6d2244bc9b4705d7bc05a5bea87fb0',51905)}


def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as response:
        return bounded(response)


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def runtime(lam,fn):
    source=ROOT/'aws/lambdas'/fn/'source';configuration=json.loads((source.parent/'config.json').read_bytes())
    request={'FunctionName':fn}
    if configuration.get('release_validation') or fn in ('justhodl-engine-fusion','justhodl-khalid-risk'):request['Qualifier']='live'
    deployed=lam.get_function(**request);cfg=deployed['Configuration'];receipt=json.loads(public('data/ops/releases/'+fn+'.json'))
    assert receipt['commit']==COMMIT and receipt['code_sha256']==cfg['CodeSha256'],'Exact receipt required: '+fn
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
    if fn==FN:assert cfg['MemorySize']==512 and cfg['Timeout']==60
    return {'commit':COMMIT,'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}


def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=180,retries={'max_attempts':0}))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5911_crisis_research_acceptance') as r:
        subprocess.run([sys.executable,str(SOURCE.parent/'tests/run_tests.py')],cwd=ROOT,check=True)
        portable=(SOURCE.parent/'tests/fixtures/expected-output.sha256').read_text().strip()
        assert portable=='824e8102ab3ade1438004e23d828bdbb3d3fd49c1a1875c19a081ed8d11154ee'
        r.kv(portable_reference_digest=portable,linux_fixture_replay='exact_match')
        runtimes={}
        for fn in FUNCTIONS:
            runtimes[fn]=runtime(lam,fn)
            r.kv(verified_function=fn,exact_commit=COMMIT,packaged_files_checked=runtimes[fn]['packaged_files_checked'])
        r.kv(runtimes=runtimes,acceptance_consumer_invocations=0,private_account_reads=0,notifications_sent=0,portfolio_writes=0,paid_ai_calls=0)
        preserved={}
        for leaf,(sha,n) in PRESERVED.items():
            key=PRIVATE+sha+'.bin';raw=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
            assert len(raw)==n and hashlib.sha256(raw).hexdigest()==sha
            assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
            preserved[leaf]={'sha256':sha,'bytes':n,'anonymous_denied':True}
        r.kv(whole_preceding_products=preserved)
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',ASSET_COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',ASSET_COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and hashlib.sha256(clean).hexdigest()==build['files_sha256'][name],'Built page differs: '+name
            if name=='defcon.html':assert re.search(rb'<script\b[^>]*src="/jh-crisis-research\.js\?v=[a-f0-9]{8}"',served)
        for key in ('data/crisis-composite.json','data/defcon.json','data/crisis-composite-history.json'):
            with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as response:
                assert response.headers.get('Cache-Control')=='no-store','Current publication cache differs'
                if key=='data/defcon.json':assert response.headers.get('X-JH-Artifact-Key')==model.CURRENT,'Canonical edge alias required'
        rule=events.describe_rule(Name='crisis-composite-hourly');targets=[]
        for page in events.get_paginator('list_targets_by_rule').paginate(Rule='crisis-composite-hourly'):targets+=page.get('Targets',[])
        bound=[t for t in targets if t['Arn'].split(':function:')[-1].split(':')[0]==FN]
        assert rule['State']=='ENABLED' and len(bound)==1 and rule['ScheduleExpression']=='cron(15 * * * ? *)'
        schedule={'enabled':True,'expression':rule['ScheduleExpression'],'bound_targets':len(bound),'natural_native_publication_observed':False}
        r.kv(pages_commit=pages_commit,schedule=schedule,cache_control='no-store',canonical_alias=model.CURRENT)
        # Only this reviewed producer is invoked, once execution is accepted.
        response,rejections=invoke_when_available(lam,dict(FunctionName=FN,InvocationType='RequestResponse',Payload=b'{"public_research_only":true,"suppress_alerts":true}'),wait_seconds=90)
        result=json.loads(response['Payload'].read());invocation={'request_id':response.get('ResponseMetadata',{}).get('RequestId'),'throttle_rejections':rejections,'result':result,'function_error':response.get('FunctionError')}
        r.kv(controlled_public_only_invocation=invocation)
        assert not response.get('FunctionError') and result.get('statusCode')==200,'Inspect report before any repeat invocation'
        produced=json.loads(result['body']);assert produced['published'] is True
        raw=public(model.CURRENT);packet=json.loads(raw);replayed=verify(packet)
        assert packet['replay']==produced['replay'] and packet['version']=='2.0.0'
        assert packet['quality']['fresh_native_series']==23 and packet['quality']['ecb_headline_status']=='fresh'
        assert packet['master_crisis_score'] is None and packet['defcon_level'] is None and packet['decision']['verb']=='WAIT'
        assert all(packet[k] is False for k in model.PERMISSIONS) and packet['portfolio_consequences']['forced_liquidation'] is False
        assert len(packet['context'])==14 and all(row['independent_votes']==0 for row in packet['context'].values())
        assert public('data/defcon.json')==raw and bounded(s3.get_object(Bucket=BUCKET,Key='data/defcon.json')['Body'])==raw
        history=json.loads(public('data/crisis-composite-history.json'))
        assert history['native_history_run']==packet['replay'] and history['snapshots']==[]
        summary={'generated_at':packet['generated_at'],'source_replay':replayed,'source_generated_at':packet['source_generated_at'],
            'measurements':{sid:{k:row.get(k) for k in ('value_decimal','unit','observation_date','acquired_at','source_row_index','quality')} for sid,row in packet['measurements'].items()},
            'ciss':{k:packet['ciss'].get(k) for k in ('series_id','value_decimal','unit','observation_date','acquired_at','source_row_index','quality')},
            'comparisons':packet['comparisons'],'dependency_roots':len(packet['dependency_graph']['roots']),'context_sources':len(packet['context'])}
        r.kv(original_source_replay=summary)
        for fn,accepted in runtimes.items():
            request={'FunctionName':fn,**({'Qualifier':'live'} if accepted['alias'] else {})}
            assert lam.get_function_configuration(**request)['CodeSha256']==accepted['code_sha256']
            assert json.loads(public('data/ops/releases/'+fn+'.json'))['commit']==COMMIT
        proof={'contract':'crisis-research-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),'runtimes':runtimes,
            'portable_reference_digest':portable,'linux_fixture_replay':'exact_match','pages_commit':pages_commit,
            'assets':{k:build['files_sha256'][k] for k in ASSETS},'current_packet_cache_control':'no-store','canonical_alias':model.CURRENT,
            'whole_preceding_products':preserved,'observations':summary,'replay':packet['replay'],'public_sha256':hashlib.sha256(raw).hexdigest(),
            'public_only':True,'controlled_invocation':invocation,'acceptance_consumer_invocations':0,'private_account_reads':0,
            'notifications_sent':0,'portfolio_writes':0,'paid_ai_calls':0,'schedule':schedule,
            'remaining':'Native current-vintage research only. No Crisis forecasting or sizing policy is qualified. Private/narrative consumers were tested offline and deployed, not invoked by acceptance; governed release validators only use their existing read-only public-input validation mode.'}
        s3.put_object(Bucket=BUCKET,Key='data/crisis-research-verification.json',Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public('data/crisis-research-verification.json'))==proof
        r.kv(proof_key='data/crisis-research-verification.json',remaining=proof['remaining'])


if __name__=='__main__':
    try:main()
    except Exception:
        print('Crisis research acceptance failed; inspect its committed report before any repeat invocation.')
        sys.exit(1)
