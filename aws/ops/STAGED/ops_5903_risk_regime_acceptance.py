"""Accept exact Risk Regime/consumer runtimes and one public-only research publication."""
from datetime import datetime, timezone
from pathlib import Path
import base64, hashlib, io, json, re, subprocess, sys, urllib.error, urllib.request, zipfile
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
SOURCE=ROOT/'aws/lambdas/justhodl-risk-regime/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(SOURCE)]
from ops_report import report
from acceptance_invoke import invoke_when_available
from replay_risk_regime import verify
import regime_model as model
from regime_store import immutable, bounded, PRIVATE

COMMIT='69301b983b71cbc2b303f832efdcd6b61a0a9b5a'
FN='justhodl-risk-regime';BUCKET='justhodl-dashboard-live'
CONSUMERS=('alpha-decay','best-setups','conviction-engine','cross-asset-flow-state','cycle-clock','hedge-planner',
    'industry-rotation','master-ranker','morning-intelligence','rotation-dashboard','sector-capital-fusion',
    'signal-board','strategist','stress-index')


def public(key):
    with urllib.request.urlopen(urllib.request.Request('https://justhodl.ai/'+key,headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=45) as response:
        raw=response.read(32*1024*1024+1)
    assert len(raw)<=32*1024*1024
    return raw


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def main():
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=900,retries={'max_attempts':0}))
    s3=boto3.client('s3',region_name='us-east-1')
    with report('ops_5903_risk_regime_acceptance') as r:
        runtimes={}
        for fn in (FN,)+tuple('justhodl-'+name for name in CONSUMERS):
            receipt=json.loads(public('data/ops/releases/'+fn+'.json'));cfg=lam.get_function_configuration(FunctionName=fn)
            assert receipt['commit']==COMMIT and receipt['code_sha256']==cfg['CodeSha256'], 'Exact release receipt required for '+fn
            assert cfg['State']=='Active' and cfg['LastUpdateStatus']=='Successful'
            with urllib.request.urlopen(lam.get_function(FunctionName=fn)['Code']['Location'],timeout=45) as response:archive=response.read(32*1024*1024+1)
            assert len(archive)<=32*1024*1024 and base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256']
            with zipfile.ZipFile(io.BytesIO(archive)) as z:
                for name in ('lambda_function.py','regime_model.py','regime_store.py') if fn==FN else ('lambda_function.py',):
                    assert z.read(name)==(ROOT/'aws/lambdas'/fn/'source'/name).read_bytes(), 'Packaged source differs '+fn+'/'+name
                shared='pd_fails_context.py' if fn==FN else 'risk_regime_authority.py'
                assert z.read(shared)==(ROOT/'aws/shared'/shared).read_bytes()
            runtimes[fn]={'commit':COMMIT,'code_sha256':cfg['CodeSha256'],'packaged_sources_match':True}
        r.kv(runtimes=runtimes,consumer_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        old=bounded(s3.get_object(Bucket=BUCKET,Key=model.CURRENT)['Body'])
        assert json.loads(public(model.CURRENT))==json.loads(old)
        old_sha=hashlib.sha256(old).hexdigest();archive_key=PRIVATE+old_sha+'.bin'
        immutable(s3,BUCKET,archive_key,old,'application/octet-stream',True)
        assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+archive_key) and denied('https://justhodl.ai/'+archive_key)
        preserved={'sha256':old_sha,'bytes':len(old),'anonymous_denied':True};r.kv(whole_preceding_public_product=preserved)
        started=datetime.now(timezone.utc)
        response,rejections=invoke_when_available(lam,dict(FunctionName=FN,InvocationType='RequestResponse',
            Payload=model.encoded({'public_research_only':True,'suppress_alerts':True})),wait_seconds=90)
        result=json.loads(response['Payload'].read())
        r.kv(engine_invocations=1,result=result,function_error=response.get('FunctionError'),
            request_id=response.get('ResponseMetadata',{}).get('RequestId'),throttle_rejections=rejections)
        assert not response.get('FunctionError') and result.get('statusCode')==200, 'Read report before any repeat invocation'
        body=json.loads(result['body']);assert body['published'] is True
        raw=public(model.CURRENT);packet=json.loads(raw)
        assert model.clock(packet['generated_at'])>=started and packet['contract']==model.CONTRACT
        assert body['replay']==packet['replay'] and not packet['source_failures']
        assert packet['quality']['fresh_native_series']==4
        assert all(row['pagination_complete'] for row in packet['option_cohorts'].values())
        assert packet['risk_regime_score'] is None and packet['decision']['verb']=='WAIT'
        assert packet['posture']['size_mult'] is None and all(packet[key] is False for key in model.PERMISSIONS)
        replayed=verify(packet,read=public)
        fails=packet['pd_settlement_fails'];assert fails['scope_id']=='treasury_incl_tips' and fails['ust_ex_tips']['scope_id']=='ust_ex_tips'
        summary={'generated_at':packet['generated_at'],'source_replay':replayed,
            'fred':{k:{field:v.get(field) for field in ('value','unit','observation_date')} for k,v in packet['measurements'].items()},
            'term_structure':packet['term_structure'],
            'options':{k:{'contracts':v['contracts'],'pages':v['pages'],'complete':v['pagination_complete'],
                'expiries':[{field:row.get(field) for field in ('expiry','volume_coverage','put_call_volume_ratio','put_call_open_interest_ratio','skew_25delta_vol_points','iv_timing_verified')} for row in v['expiries']]} for k,v in packet['option_cohorts'].items()},
            'fx':{k:packet['fx_measurement'].get(k) for k in ('value','unit','observation_date','change_7_calendar_days_pct')},
            'settlement_scopes':{k:fails.get(k) for k in ('as_of','ftd_bn','ftr_bn','combined_bn','scope_id')},
            'headline_scope':{k:fails['ust_ex_tips'].get(k) for k in ('as_of','combined_bn','scope_id')}}
        r.kv(original_source_replay=summary)
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',COMMIT,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',COMMIT,pages_commit,'--','risk-regime.html','jh-risk-regime-research.js'],cwd=ROOT,check=True)
        for name in ('risk-regime.html','jh-risk-regime-research.js'):
            served=public(name);built,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and hashlib.sha256(built).hexdigest()==build['files_sha256'][name]
            if name=='risk-regime.html':assert b'jh-risk-regime-research.js?v=50' in served
        schedule=boto3.client('scheduler',region_name='us-east-1').get_schedule(Name='justhodl-risk-regime-daily',GroupName='default')
        assert schedule['State']=='ENABLED' and schedule['ScheduleExpression']=='cron(45 12 ? * MON-FRI *)'
        for fn,accepted in runtimes.items():
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==accepted['code_sha256']
            assert json.loads(public('data/ops/releases/'+fn+'.json'))['commit']==COMMIT
        proof={'contract':'risk-regime-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'runtimes':runtimes,'public_only':True,'engine_invocations':1,'consumer_invocations':0,
            'whole_preceding_public_product':preserved,'public_sha256':hashlib.sha256(raw).hexdigest(),
            'replay':packet['replay'],'observations':summary,'pages_commit':pages_commit,
            'assets':{k:build['files_sha256'][k] for k in ('risk-regime.html','jh-risk-regime-research.js')},
            'schedule':{'name':schedule['Name'],'expression':schedule['ScheduleExpression'],'natural_new_publication_observed':False},
            'remaining':'Current-vintage descriptive research, not a calibrated decision model. IV/OI clocks remain unverified. Direct consumers deployed and tested offline; their producers were not invoked.'}
        s3.put_object(Bucket=BUCKET,Key='data/risk-regime-verification.json',Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public('data/risk-regime-verification.json'))==proof
        r.kv(proof_key='data/risk-regime-verification.json',pages_commit=pages_commit,remaining=proof['remaining'])


if __name__=='__main__':
    try:main()
    except Exception:
        print('Risk Regime acceptance failed; inspect its committed report before another invocation.')
        sys.exit(1)
