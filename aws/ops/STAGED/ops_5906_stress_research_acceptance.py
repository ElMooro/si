"""Accept exact Stress Research runtimes and controlled public-only source/status publication."""
from datetime import datetime,timezone
from pathlib import Path
import base64,hashlib,io,json,re,subprocess,sys,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
SOURCE=ROOT/'aws/lambdas/justhodl-stress-index/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(SOURCE)]
from ops_report import report
from acceptance_invoke import invoke_when_available
from replay_stress_research import verify
import stress_model as model
from stress_store import PRIVATE,bounded
COMMIT='bed2d509082f5d0d54ad6083dc2a0e5cb0932558'
BUCKET='justhodl-dashboard-live';FN='justhodl-stress-index'
FUNCTIONS=(FN,'justhodl-jsi-calibrator','justhodl-proven-portfolio','justhodl-alert-sentinel')
PRESERVED={
    'jsi':('3fa221693651bba58676fbf267050a95ea01cb2ac1074a428ee84c1739f1711d',123917),
    'jsi-history':('61badfbf310e6ae66907afc889dac3efd94f61138e8e21a7fea5bdeea07d0fb1',313114),
    'jsi-overlay-history':('a56238aa86cdce9d1de353aad396ce9d1607ebb812dfeb9ca1ba26277bd9cb86',42565),
    'jsi-calibration':('0d91d5e2226df0e349348b5e2ddae632b9f0e96f312d128d07e8de4683a45851',4292),
}


def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as response:raw=response.read(32*1024*1024+1)
    assert len(raw)<=32*1024*1024
    return raw


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=900,retries={'max_attempts':0}))
    s3=boto3.client('s3',region_name='us-east-1');events=boto3.client('events',region_name='us-east-1')
    with report('ops_5906_stress_research_acceptance') as r:
        runtimes={}
        for fn in FUNCTIONS:
            receipt=json.loads(public('data/ops/releases/'+fn+'.json'));cfg=lam.get_function_configuration(FunctionName=fn)
            assert receipt['commit']==COMMIT and receipt['code_sha256']==cfg['CodeSha256'],'Exact runtime receipt required: '+fn
            assert cfg['State']=='Active' and cfg['LastUpdateStatus']=='Successful'
            with urllib.request.urlopen(lam.get_function(FunctionName=fn)['Code']['Location'],timeout=45) as response:archive=response.read(32*1024*1024+1)
            assert len(archive)<=32*1024*1024 and base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256']
            with zipfile.ZipFile(io.BytesIO(archive)) as z:
                for name in ('lambda_function.py','stress_model.py','stress_store.py') if fn==FN else ('lambda_function.py',):
                    assert z.read(name)==(ROOT/'aws/lambdas'/fn/'source'/name).read_bytes(),'Packaged source differs: '+fn+'/'+name
                if fn!=FN:assert z.read('jsi_authority.py')==(ROOT/'aws/shared/jsi_authority.py').read_bytes()
            runtimes[fn]={'commit':COMMIT,'code_sha256':cfg['CodeSha256'],'packaged_sources_match':True}
        r.kv(runtimes=runtimes,consumer_invocations=0,private_account_reads=0,notifications_sent=0,portfolio_writes=0,paid_ai_calls=0)
        preserved={}
        for leaf,(sha,n) in PRESERVED.items():
            key=PRIVATE+sha+'.bin';raw=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])
            assert len(raw)==n and hashlib.sha256(raw).hexdigest()==sha
            assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
            preserved[leaf]={'sha256':sha,'bytes':n,'anonymous_denied':True}
        r.kv(whole_preceding_products=preserved)
        # Validate delivery prerequisites before any controlled invocation.
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',COMMIT,pages_commit,'--','jsi.html','jh-stress-research.js'],cwd=ROOT,check=True)
        for name in ('jsi.html','jh-stress-research.js'):
            served=public(name);built,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and hashlib.sha256(built).hexdigest()==build['files_sha256'][name],'Built page differs: '+name
            if name=='jsi.html':assert re.search(rb'<script\b[^>]*src="/jh-stress-research\.js\?v=[a-f0-9]{8}"',served)
        schedules={}
        for name,fn,cadence in [('jsi-6h',FN,'rate(6 hours)'),('jsi-calibrator-weekly','justhodl-jsi-calibrator','cron(30 9 ? * SUN *)')]:
            rule=events.describe_rule(Name=name);targets=[]
            for page in events.get_paginator('list_targets_by_rule').paginate(Rule=name):targets+=page.get('Targets',[])
            bound=[t for t in targets if t['Arn'].split(':function:')[-1].split(':')[0]==fn]
            assert rule['State']=='ENABLED' and rule['ScheduleExpression']==cadence and len(bound)==1,'Existing cadence/binding differs: '+name
            schedules[name]={'expression':cadence,'enabled':True,'bound_targets':1,'natural_new_publication_observed':False}
        for key in ('data/jsi.json','data/jsi-history.json','data/jsi-calibration.json'):
            with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:assert response.headers.get('Cache-Control')=='no-store'
        r.kv(pages_commit=pages_commit,schedules=schedules,cache_control='no-store')
        invocations={}
        for fn in (FN,'justhodl-jsi-calibrator'):
            response,rejections=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',
                Payload=model.encoded({'public_research_only':True,'suppress_alerts':True})),wait_seconds=90)
            result=json.loads(response['Payload'].read());invocations[fn]={'request_id':response.get('ResponseMetadata',{}).get('RequestId'),
                'throttle_rejections':rejections,'result':result,'function_error':response.get('FunctionError')}
            r.kv(controlled_invocation=invocations[fn],function=fn)
            assert not response.get('FunctionError') and result.get('statusCode')==200,'Read report before any repeat invocation'
        raw=public(model.CURRENT);packet=json.loads(raw);replayed=verify(packet,read=public)
        native_result=json.loads(invocations[FN]['result']['body'])
        assert native_result['published'] is True and native_result['replay']==packet['replay']
        assert not packet['source_failures'] and packet['quality']['fresh_native_series']==13
        assert packet['jsi'] is None and packet['decision']['verb']=='WAIT' and all(packet[k] is False for k in model.PERMISSIONS)
        assert packet['measurements']['BAMLH0A0HYM2']['percentiles']['5y']['value'] is None
        assert packet['measurements']['KCFSI']['frequency_short']=='M' and packet['measurements']['WALCL']['frequency_short']=='W'
        history=json.loads(public('data/jsi-history.json'));calibration=json.loads(public('data/jsi-calibration.json'))
        assert history['native_history_run']==packet['replay'] and history['series']==[]
        assert calibration['contract']=='jsi-qualification-status.v1' and calibration['source_replay']==packet['replay']
        assert calibration['ssm_weight_writes']==0 and calibration['spine']['weights']=={} and calibration['sizing_eligible'] is False
        # Verify the overlay input ledger remains untouched, including its complete original bytes.
        overlay=bounded(s3.get_object(Bucket=BUCKET,Key='data/jsi-overlay-history.json')['Body'])
        assert hashlib.sha256(overlay).hexdigest()==PRESERVED['jsi-overlay-history'][0]
        summary={'generated_at':packet['generated_at'],'source_replay':replayed,
            'native_rows':sum(len(row['history']) for row in packet['measurements'].values()),
            'series':{sid:{k:row.get(k) for k in ('value','unit','observation_date','frequency','history_span','change')} for sid,row in packet['measurements'].items()},
            'spreads':packet['spreads']}
        r.kv(original_source_replay=summary)
        for fn,accepted in runtimes.items():
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==accepted['code_sha256']
            assert json.loads(public('data/ops/releases/'+fn+'.json'))['commit']==COMMIT
        proof={'contract':'stress-research-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'runtimes':runtimes,'pages_commit':pages_commit,'assets':{k:build['files_sha256'][k] for k in ('jsi.html','jh-stress-research.js')},
            'current_packet_cache_control':'no-store','whole_preceding_products':preserved,'observations':summary,'replay':packet['replay'],
            'public_sha256':hashlib.sha256(raw).hexdigest(),'public_only':True,'controlled_invocations':invocations,
            'consumer_invocations':0,'private_account_reads':0,'notifications_sent':0,'portfolio_writes':0,'paid_ai_calls':0,'ssm_weight_writes':0,
            'schedules':schedules,'remaining':'Descriptive current-vintage research. No JSI forecasting or sizing model is qualified. Direct consumers tested offline and deployed, not invoked.'}
        s3.put_object(Bucket=BUCKET,Key='data/stress-research-verification.json',Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public('data/stress-research-verification.json'))==proof
        r.kv(proof_key='data/stress-research-verification.json',remaining=proof['remaining'])


if __name__=='__main__':
    try:main()
    except Exception:
        print('Stress acceptance failed; inspect its committed report before any repeat invocation.')
        sys.exit(1)
