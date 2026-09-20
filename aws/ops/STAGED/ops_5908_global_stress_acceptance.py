"""Accept exact Global Stress Research runtimes and controlled public-only source/status publication."""
from datetime import datetime,timezone
from pathlib import Path
import base64,hashlib,io,json,re,subprocess,sys,urllib.request,urllib.error,zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
SOURCE=ROOT/'aws/lambdas/justhodl-global-stress/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(SOURCE)]
from ops_report import report
from acceptance_invoke import invoke_when_available
from replay_global_stress_research import verify
import global_research_model as model
from global_research_store import PRIVATE,bounded
COMMIT='e349fe43a1e3038a17e2e8b95e1140711652698e'
BUCKET='justhodl-dashboard-live';FN='justhodl-global-stress'
FUNCTIONS=(FN,'justhodl-gsi-calibrator','justhodl-gsi-horizons','justhodl-ai-website-synthesis','justhodl-calibration-fleet',
    'justhodl-canary-warroom','justhodl-crisis-composite','justhodl-cycle-clock','justhodl-live-pulse','justhodl-master-allocator',
    'justhodl-morning-intelligence','justhodl-regime-conditional-router','justhodl-signal-board','justhodl-signal-orthogonality','justhodl-streaming-fanout')
ASSETS=('global-stress.html','gsi-calibration.html','horizons-gsi.html','crisis.html','jh-global-stress-research.js')
PRESERVED={
    'global-stress':('d8371eeb9c638e74d3a306ba14b8c8bfaceaead83ddc2f782ce79c6520abfbe5',35200),
    'global-stress-history':('3d3e1927cccb8669fc05f0b558fc89280e038b4ad5ccce0f66e5c7faf2dde4d7',45615),
    'gsi-dim-history':('74ce148d573b9dcf156f6459370df4cac1ce64d480dc9719e23c0a394155f049',78030),
    'gsi-calibration':('975882830999be421b97107c59d4e24f8f6490a4561abbefe15a0590289561d4',15267),
    'gsi-horizons':('ffd235da6523d0b96fea2c64ab10a3006bca4dac8aee61cd87ab3dc0cb13e9c9',5933),
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
    s3=boto3.client('s3',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5908_global_stress_acceptance') as r:
        runtimes={}
        for fn in FUNCTIONS:
            receipt=json.loads(public('data/ops/releases/'+fn+'.json'));cfg=lam.get_function_configuration(FunctionName=fn)
            assert receipt['commit']==COMMIT and receipt['code_sha256']==cfg['CodeSha256'],'Exact runtime receipt required: '+fn
            assert cfg['State']=='Active' and cfg['LastUpdateStatus']=='Successful'
            with urllib.request.urlopen(lam.get_function(FunctionName=fn)['Code']['Location'],timeout=45) as response:archive=response.read(32*1024*1024+1)
            assert len(archive)<=32*1024*1024 and base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256']
            with zipfile.ZipFile(io.BytesIO(archive)) as z:
                for name in ('lambda_function.py','global_research_model.py','global_research_store.py') if fn==FN else ('lambda_function.py',):
                    assert z.read(name)==(ROOT/'aws/lambdas'/fn/'source'/name).read_bytes(),'Packaged source differs: '+fn+'/'+name
                if fn!=FN:assert z.read('gsi_authority.py')==(ROOT/'aws/shared/gsi_authority.py').read_bytes()
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
        subprocess.run(['git','diff','--quiet',COMMIT,pages_commit,'--',*ASSETS],cwd=ROOT,check=True)
        for name in ASSETS:
            served=public(name);built,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and hashlib.sha256(built).hexdigest()==build['files_sha256'][name],'Built page differs: '+name
            if name in ASSETS[:3]:assert re.search(rb'<script\b[^>]*src="/jh-global-stress-research\.js\?v=[a-f0-9]{8}"',served)
        schedules={}
        for fn in (FN,'justhodl-gsi-calibrator','justhodl-gsi-horizons'):
            cfg=json.loads((ROOT/'aws/lambdas'/fn/'config.json').read_text())['eventbridge_scheduler']
            schedule=scheduler.get_schedule(Name=cfg['schedule_name'],GroupName='default')
            target=schedule['Target']['Arn'].split(':function:')[-1].split(':')[0]
            assert schedule['State']=='ENABLED' and schedule['ScheduleExpression']==cfg['cron'] and target==fn,'Existing schedule differs: '+fn
            schedules[fn]={'expression':cfg['cron'],'timezone':schedule.get('ScheduleExpressionTimezone'),'enabled':True,'natural_new_publication_observed':False}
        for key in ('data/global-stress.json','data/global-stress-history.json','data/gsi-calibration.json','data/gsi-horizons.json'):
            with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:assert response.headers.get('Cache-Control')=='no-store'
        r.kv(pages_commit=pages_commit,schedules=schedules,cache_control='no-store')
        invocations={}
        for fn in (FN,'justhodl-gsi-calibrator','justhodl-gsi-horizons'):
            response,rejections=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',
                Payload=model.encoded({'public_research_only':True,'suppress_alerts':True})),wait_seconds=90)
            result=json.loads(response['Payload'].read());invocations[fn]={'request_id':response.get('ResponseMetadata',{}).get('RequestId'),
                'throttle_rejections':rejections,'result':result,'function_error':response.get('FunctionError')}
            r.kv(controlled_invocation=invocations[fn],function=fn)
            assert not response.get('FunctionError') and result.get('statusCode')==200,'Read report before any repeat invocation'
        raw=public(model.CURRENT);packet=json.loads(raw);replayed=verify(packet,read=public)
        native_result=json.loads(invocations[FN]['result']['body'])
        assert native_result['published'] is True and native_result['replay']==packet['replay']
        assert not packet['source_failures'] and packet['quality']['fresh_instruments']==14 and packet['quality']['fresh_native_series']==10
        assert packet['global_stress_index'] is None and packet['decision']['verb']=='WAIT' and all(packet[k] is False for k in model.PERMISSIONS)
        assert packet['instruments']['SPY']['returns']['21']['price']['value']!=packet['instruments']['SPY']['returns']['21']['dividend_adjusted']['value']
        assert len(packet['correlations']['pairs'])==91 and all(row['n_matched_intervals']>=40 for row in packet['correlations']['pairs'])
        assert packet['measurements']['VIXCLS']['unit']=='index_points' and packet['measurements']['DGS10']['unit']=='percent'
        history=json.loads(public('data/global-stress-history.json'))
        assert history['native_history_run']==packet['replay'] and history['snapshots']==[]
        statuses={}
        for name in ('gsi-calibration','gsi-horizons'):
            status=json.loads(public('data/'+name+'.json'));assert status['contract']=='gsi-qualification-status.v1' and status['source_replay']==packet['replay']
            assert status['ssm_weight_writes']==0 and status['weights']=={} and status['term_structure']==[] and status['sizing_eligible'] is False
            statuses[name]=status
        dimension_history=bounded(s3.get_object(Bucket=BUCKET,Key='data/gsi-dim-history.json')['Body'])
        assert hashlib.sha256(dimension_history).hexdigest()==PRESERVED['gsi-dim-history'][0]
        summary={'generated_at':packet['generated_at'],'source_replay':replayed,
            'native_price_rows':sum(len(row['history']) for row in packet['instruments'].values()),
            'native_fred_rows':sum(len(row['history']) for row in packet['measurements'].values()),
            'instruments':{sid:{k:row.get(k) for k in ('isin','exchange','currency','close','adjusted_close','observation_date','coverage','returns','volatility','drawdown')} for sid,row in packet['instruments'].items()},
            'fred':{sid:{k:row.get(k) for k in ('value','unit','observation_date','frequency','change')} for sid,row in packet['measurements'].items()},
            'credit_dispersion':packet['credit_dispersion'],'correlation_pairs':len(packet['correlations']['pairs']),
            'qualification_statuses':{k:{f:v[f] for f in ('contract','generated_at','status','ssm_weight_writes')} for k,v in statuses.items()}}
        r.kv(original_source_replay=summary)
        for fn,accepted in runtimes.items():
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==accepted['code_sha256']
            assert json.loads(public('data/ops/releases/'+fn+'.json'))['commit']==COMMIT
        proof={'contract':'global-stress-research-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'runtimes':runtimes,'pages_commit':pages_commit,'assets':{k:build['files_sha256'][k] for k in ASSETS},
            'current_packet_cache_control':'no-store','whole_preceding_products':preserved,'observations':summary,'replay':packet['replay'],
            'public_sha256':hashlib.sha256(raw).hexdigest(),'public_only':True,'controlled_invocations':invocations,
            'consumer_invocations':0,'private_account_reads':0,'notifications_sent':0,'portfolio_writes':0,'paid_ai_calls':0,'ssm_weight_writes':0,
            'schedules':schedules,'remaining':'Descriptive current-vintage research. No Global Stress forecasting or sizing model is qualified. Direct consumers tested offline and deployed, not invoked.'}
        s3.put_object(Bucket=BUCKET,Key='data/global-stress-research-verification.json',Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public('data/global-stress-research-verification.json'))==proof
        r.kv(proof_key='data/global-stress-research-verification.json',remaining=proof['remaining'])


if __name__=='__main__':
    try:main()
    except Exception:
        print('Global Stress acceptance failed; inspect its committed report before any repeat invocation.')
        sys.exit(1)
