"""Accept native Plumbing after uncertain source-invocation transport; never invoke that source twice."""
from datetime import datetime,timezone
from pathlib import Path
import base64,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
SOURCE=ROOT/'aws/lambdas/justhodl-crisis-plumbing/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(SOURCE)]
from ops_report import report
from acceptance_invoke import invoke_when_available
from replay_plumbing_research import verify
from release_package_evidence import shared_imports
from plumbing_research_store import PRIVATE,bounded
import plumbing_research_model as model
COMMIT='3a35ad51c049cbf57b5e43fa732a8c308e8aad5c'
ASSET_COMMIT=COMMIT
BUCKET='justhodl-dashboard-live';FN='justhodl-crisis-plumbing'
FUNCTIONS=('justhodl-ai-brief-router','justhodl-bond-warroom','justhodl-calibration-snapshotter','justhodl-calibrator',FN,
 'justhodl-daily-report-v3','justhodl-master-ranker','justhodl-morning-intelligence','justhodl-signal-logger')
ASSETS=('crisis.html','jh-plumbing-research.js','jh-crisis-research.js','jh-pd-fails-strip.js')
PREDECESSOR=('c9ba6e12508eae1431e9fde1c031491374b72026ab960f428832801aef6b72a0',168362)


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
    if fn==FN:assert cfg['MemorySize']==768 and cfg['Timeout']==240
    return {'commit':COMMIT,'code_sha256':cfg['CodeSha256'],'packaged_files_checked':len(expected),'all_packaged_sources_match':True,'alias':alias}


def controlled(lam,fn,payload,r):
    # Retry only explicit capacity rejection, never uncertain execution.
    response,rejections=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',Payload=json.dumps(payload).encode()),wait_seconds=90)
    result=json.loads(response['Payload'].read())
    invocation={'function':fn,'request_id':response.get('ResponseMetadata',{}).get('RequestId'),'throttle_rejections':rejections,
                'function_error':response.get('FunctionError'),'result':result}
    r.kv(controlled_public_only_invocation=invocation)
    assert not response.get('FunctionError') and result.get('statusCode')==200,'Inspect committed report before any repeat invocation'
    body=json.loads(result['body']);assert body.get('published') is True,'Publication was not accepted'
    return invocation,body


def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=960,tcp_keepalive=True,retries={'max_attempts':0}))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5916_plumbing_publication_acceptance') as r:
        subprocess.run([sys.executable,str(SOURCE.parent/'tests/run_tests.py')],cwd=ROOT,check=True)
        portable=(SOURCE.parent/'tests/fixtures/expected-output.sha256').read_text().strip()
        assert portable=='aa275f9916ddb722527e5b0290c483fab7659a2ee108a8b100e09442f6775b36'
        r.kv(portable_reference_digest=portable,linux_fixture_replay='exact_match')
        runtimes={}
        for fn in FUNCTIONS:
            runtimes[fn]=runtime(lam,fn)
            r.kv(verified_function=fn,exact_commit=COMMIT,packaged_files_checked=runtimes[fn]['packaged_files_checked'])
        r.kv(runtimes=runtimes,acceptance_consumer_invocations=0,private_account_reads=0,notifications_sent=0,portfolio_writes=0,paid_ai_calls=0)
        old_source=subprocess.check_output(['git','show','39fbfcbc175c40a24d996c862c77d70cf4e875ff:aws/lambdas/justhodl-crisis-plumbing/source/lambda_function.py'],cwd=ROOT)
        assert (SOURCE/'legacy_crisis_plumbing.py').read_bytes()==old_source,'Complete legacy source differs'
        sha,n=PREDECESSOR;key=PRIVATE+sha+'.bin';old=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
        assert len(old)==n and hashlib.sha256(old).hexdigest()==sha
        assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
        preserved={'sha256':sha,'bytes':n,'anonymous_denied':True,'legacy_source_sha256':hashlib.sha256(old_source).hexdigest()}
        r.kv(whole_preceding_product=preserved)
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',ASSET_COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',ASSET_COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);clean,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and hashlib.sha256(clean).hexdigest()==build['files_sha256'][name],'Built asset differs: '+name
            if name=='crisis.html':assert re.search(rb'<script\b[^>]*src="/jh-plumbing-research\.js\?v=[a-f0-9]{8}"',served)
        with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+model.CURRENT,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as response:
            assert response.headers.get('Cache-Control')=='no-store'
        rule=events.describe_rule(Name='justhodl-crisis-plumbing-refresh');targets=[]
        for page in events.get_paginator('list_targets_by_rule').paginate(Rule='justhodl-crisis-plumbing-refresh'):targets+=page.get('Targets',[])
        bound=[t for t in targets if t['Arn'].split(':function:')[-1].split(':')[0]==FN]
        assert rule['State']=='ENABLED' and len(bound)==1 and rule['ScheduleExpression']=='cron(54 14 * * ? *)'
        schedule={'enabled':True,'expression':rule['ScheduleExpression'],'bound_targets':1,'natural_native_publication_observed':False}
        r.kv(pages_commit=pages_commit,schedule=schedule)
        # Only the reviewed public source branch and native public producer run.
        # Ops5914 lost its synchronous connection after submission. Never repeat that invocation.
        # Observe its public publication; this acceptance invokes only native Plumbing below.
        deadline=time.monotonic()+720;macro=None
        while time.monotonic()<deadline:
            candidate=json.loads(public('data/report-measurements.json'))
            if candidate.get('generated_at','')>='2026-09-20T11:46:26' and set(model.SERIES)<=set(candidate.get('catalog',{})):
                macro=candidate;break
            time.sleep(20)
        assert macro is not None,'No complete new public macro publication observed; inspect original invocation before retrying'
        acquisition={'function':'justhodl-daily-report-v3','submitted_by':'ops5914','submission_run':35508759832,
            'transport_outcome':'ConnectionClosedError; request ID and synchronous result unavailable',
            'repeat_invocations':0,'public_publication_observed':True,'generated_at':macro['generated_at'],'replay':macro['replay']}
        r.kv(observed_public_source_publication=acquisition)
        assert set(model.SERIES)<=set(macro['catalog'])
        added=('BAMLEM4BRRBLCRPIOAS','ECBESTRVOLWGTTRMDMNRT','RECPROUSM156N','SAHMREALTIME')
        assert all(macro['measurements'].get(sid,{}).get('quality',{}).get('status')=='fresh' for sid in added),'Valid missing series not collected'
        r.kv(source_catalog_complete=True,added_series={sid:{k:macro['measurements'][sid].get(k) for k in ('current_decimal','date','unit','frequency','acquired_at')} for sid in added},
             unavailable_source_identities={sid:macro.get('errors',{}).get(sid) for sid in model.SERIES if sid not in macro.get('measurements',{})})
        invocation,produced=controlled(lam,FN,{'public_research_only':True},r)
        raw=public(model.CURRENT);packet=json.loads(raw);replayed=verify(packet)
        assert packet['replay']==produced['replay'] and packet['version']=='2.0.0'
        assert set(model.SERIES)<=set(packet['measurements']) and len(packet['groups']['ofr_publisher'])==9
        assert packet['composite']['composite_stress_score'] is None and packet['decision']['verb']=='WAIT'
        assert all(packet[k] is False for k in model.AUTHORITY) and packet['portfolio_consequences']['forced_liquidation'] is False
        assert packet['comparisons']['usd_jpy_three_month']['value_decimal'] is None
        assert packet['measurements']['DPCREDIT']['unit']=='Percent' and packet['measurements']['BUSLOANS']['frequency']=='M'
        assert packet['measurements']['USSLIND']['value_decimal'] is None
        for sid in ('DEXJPUS','NFCICREDIT','TOTBKCR'):
            assert packet['measurements'][sid]['observation_date']>='2026-09-01','Earliest-page regression: '+sid
        assert all(packet['measurements'][sid]['quality']['status']=='fresh' for sid in added)
        summary={'generated_at':packet['generated_at'],'source_replay':replayed,'quality':packet['quality'],'source_clocks':packet['source_clocks'],
            'measurements':{sid:{k:row.get(k) for k in ('value_decimal','unit','frequency','observation_date','acquired_at','source_row_index','quality')} for sid,row in packet['measurements'].items()},
            'comparisons':packet['comparisons'],'context':packet['context']}
        r.kv(original_source_replay=summary)
        for fn,accepted in runtimes.items():
            request={'FunctionName':fn,**({'Qualifier':'live'} if accepted['alias'] else {})}
            assert lam.get_function_configuration(**request)['CodeSha256']==accepted['code_sha256']
            assert json.loads(public('data/ops/releases/'+fn+'.json'))['commit']==COMMIT
        proof={'contract':'plumbing-research-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),'runtimes':runtimes,
            'portable_reference_digest':portable,'linux_fixture_replay':'exact_match','pages_commit':pages_commit,
            'assets':{k:build['files_sha256'][k] for k in ASSETS},'current_packet_cache_control':'no-store','whole_preceding_product':preserved,
            'observations':summary,'replay':packet['replay'],'public_sha256':hashlib.sha256(raw).hexdigest(),
            'public_only':True,'source_invocations_this_acceptance':0,'observed_source_acquisition':acquisition,'controlled_invocation':invocation,
            'acceptance_consumer_invocations':0,'private_account_reads':0,'notifications_sent':0,'portfolio_writes':0,'paid_ai_calls':0,'schedule':schedule,
            'remaining':'Current-vintage native research only. No qualified forecast or portfolio policy. Unavailable identities, discontinued history and shared index families are explicit. Consumers were tested offline and deployed, not invoked.'}
        s3.put_object(Bucket=BUCKET,Key='data/plumbing-research-verification.json',Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public('data/plumbing-research-verification.json'))==proof
        r.kv(proof_key='data/plumbing-research-verification.json',remaining=proof['remaining'])


if __name__=='__main__':
    try:main()
    except Exception:
        print('Plumbing acceptance failed; inspect its committed report before any repeat invocation.')
        sys.exit(1)
