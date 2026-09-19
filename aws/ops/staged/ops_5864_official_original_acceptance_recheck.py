"""Accept original weekly official-sector balances against exact runtime evidence."""
from datetime import datetime,timezone
import hashlib,json,subprocess,sys,time,urllib.request,urllib.error
from pathlib import Path
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(ROOT/'aws/lambdas/justhodl-official-pulse/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
import official_research as model,official_store as store
from replay_official_research import verify_current

def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:return response.status in (401,403,404)
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)

def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/justhodl-official-pulse/source/official_store.py'],text=True).strip()
    bucket='justhodl-dashboard-live';s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    raw=store.raw_reader(s3,bucket)
    def read(key):return json.loads(raw(key))
    names=('justhodl-official-pulse','justhodl-tic-flows')
    expected_by_fn={fn:subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+fn],text=True).strip() for fn in names}
    with report('ops_5864_official_original_acceptance_recheck') as r:
        policy=json.loads(s3.get_bucket_policy(Bucket=bucket)['Policy'])
        deny=next(v for v in policy['Statement'] if v.get('Sid')=='Audit20260909ImmutableOriginalBackups')
        assert deny['Effect']=='Deny' and deny['Principal']=='*'
        assert {'s3:GetObject','s3:GetObjectVersion'}<=set(deny['Action'])
        assert deny['Condition']=={'StringNotEquals':{'aws:PrincipalAccount':'857687956942'}}
        assert 'arn:aws:s3:::'+bucket+'/audit-private/20260909-originals/*' in deny['Resource']
        runtimes={};deadline=time.monotonic()+2400
        while len(runtimes)!=len(names):
            for fn in names:
                if fn in runtimes:continue
                try:receipt=read('data/ops/releases/'+fn+'.json')
                except Exception as exc:
                    if store.code(exc) in ('404','NoSuchKey'):continue
                    raise
                if receipt.get('commit')!=expected_by_fn[fn]:continue
                conf=lam.get_function_configuration(FunctionName=fn)
                assert conf['CodeSha256']==receipt['code_sha256'],fn+' runtime differs'
                runtimes[fn]={'commit':expected_by_fn[fn],'code_sha256':receipt['code_sha256']}
            assert time.monotonic()<deadline,'exact runtime timeout'
            if len(runtimes)!=len(names):time.sleep(15)
        r.kv(runtimes=runtimes);started=datetime.now(timezone.utc).isoformat();invoke_results={}
        for fn in names[:1]:
            response,rejected=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',Payload=model.encoded({'suppress_alerts':True})))
            result=json.loads(response['Payload'].read());r.kv(function=fn,invoke_status=result.get('statusCode'),throttle_rejections=rejected)
            assert not response.get('FunctionError') and result.get('statusCode')==200,fn+' invocation failed'
            result=json.loads(result['body']);assert result.get('published') is True,fn+' publication missing';invoke_results[fn]=result
        packet=read(model.CURRENT);output=verify_current(raw)
        assert packet['generated_at']>started and output['quality']['status']=='fresh' and output['source_status_codes']=={},'source validation needs review'
        assert output['call'] is None and all(output[k] is False for k in ('calls_eligible','sizing_eligible','execution_eligible'))
        assert output['custody']['id']=='WMTSECL1' and output['foreign_rrp']['id']=='WLRRAFOIAL'
        assert output['custody']['latest_date']>='2026-09-16' and output['custody']['n_obs']>=1240
        assert output['dollar_leg']['status']=='UNKNOWN' and output['dollar_leg']['available']==0 and output['dollar_leg']['legs_firing']==0
        assert output['dollar_leg']['tic_context']['status']=='immutable_descriptive_context'
        assert len(output['measurements'])==8 and all(v['calls_eligible'] is False and v['z_13wchg_10y'] is None for v in output['measurements'].values())
        assert all(v['matches']>=1240 and v['different_values']==v['missing_dates']==0 for v in output['distribution_checks'].values())
        assert all(v['latest']['status']=='within_reporting_rounding' for v in output['reconciliations'].values())
        assert all(v['matches'] for v in output['release']['checks'])
        native_rows=0
        for value in output['measurements'].values():
            history=read(value['history']['key']);assert len(history['rows'])==value['n_obs']
            assert len({row['date'] for row in history['rows']})==len(history['rows']);native_rows+=len(history['rows'])
        marker=read(model.PREFIX+'migration.json');key=store.PRIVATE+marker['sha256']+'.bin';body=s3.get_object(Bucket=bucket,Key=key)['Body'].read(marker['bytes']+1)
        assert len(body)==marker['bytes'] and hashlib.sha256(body).hexdigest()==marker['sha256']
        assert denied('https://'+bucket+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
        before=read(model.CURRENT)
        response,_=invoke_when_available(lam,dict(FunctionName=names[0],InvocationType='RequestResponse',Payload=model.encoded({'requestContext':{'http':{'method':'GET'}}})))
        result=json.loads(response['Payload'].read())
        assert not response.get('FunctionError') and result['statusCode']==200 and json.loads(result['body'])==before and read(model.CURRENT)==before
        assert len(result['body'].encode())<4*1024*1024
        scheduler=boto3.client('scheduler',region_name='us-east-1');schedule=scheduler.get_schedule(Name='justhodl-official-pulse-weekly',GroupName='default')
        assert schedule['ScheduleExpression']=='cron(0 9 * * ? *)' and schedule['State']=='ENABLED'
        assert schedule['Target']['Arn'].split(':function:')[-1] in ('justhodl-official-pulse','justhodl-official-pulse:$LATEST')
        for fn in names:
            receipt=read('data/ops/releases/'+fn+'.json')
            assert receipt['commit']==expected_by_fn[fn] and receipt['code_sha256']==runtimes[fn]['code_sha256']
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==runtimes[fn]['code_sha256']
        proof={'contract':'official-research-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),'commit':expected,'runtimes':runtimes,
            'research_generated_at':output['generated_at'],'quality':output['quality'],'original_replay_reproduced':True,'replay':packet['replay'],
            'output_sha256':model.digest(output),'native_observations':native_rows,'reviewed_measurements':len(output['measurements']),
            'matching_distribution_observations':sum(v['matches'] for v in output['distribution_checks'].values()),
            'printed_release_matches':all(v['matches'] for v in output['release']['checks']),
            'protected_legacy_verified':True,'http_reads_did_not_recollect':True,'source_errors':output['source_status_codes'],
            'schedule':{'name':schedule['Name'],'cron':schedule['ScheduleExpression'],'state':schedule['State']},
            'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0,'calls_eligible':False,'sizing_eligible':False,
            'acceptance_scope':'Exact Official Pulse and unchanged TIC runtimes. Only original Official Pulse collector and read-only HTTP branch invoked. Complete source replay and preservation; no qualified return strategy.'}
        s3.put_object(Bucket=bucket,Key='data/official-research-verification.json',Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store');r.kv(**proof)

if __name__=='__main__':
    try:main()
    except Exception:
        print('Official original acceptance failed; inspect committed runner report.')
        sys.exit(1)
