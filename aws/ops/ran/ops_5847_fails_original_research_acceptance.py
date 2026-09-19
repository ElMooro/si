"""Four exact runtimes; original FR2004 collection and canonical dealer readthrough."""
from datetime import datetime,timezone
import hashlib,json,subprocess,sys,time,urllib.request,urllib.error
from pathlib import Path
import boto3
from botocore.config import Config

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),
             str(ROOT/'aws/lambdas/justhodl-settlement-fails/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from fails_native import encoded,digest,KEYS
from fails_research import CONTRACT,CURRENT,PREFIX
from fails_store import raw_reader,PRIVATE
from replay_fails_research import verify_current


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:
            return response.status in (401,403,404)
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--',
        'aws/lambdas/justhodl-settlement-fails/source/fails_store.py'],text=True).strip()
    bucket='justhodl-dashboard-live';s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    raw=raw_reader(s3,bucket)
    def read(key):return json.loads(raw(key))
    names=('justhodl-settlement-fails','justhodl-nyfed-pd','justhodl-treasury-rehypo','justhodl-eurodollar-plumbing')
    with report('ops_5847_fails_original_research_acceptance') as r:
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
                    if str(getattr(exc,'response',{}).get('Error',{}).get('Code','')) in ('404','NoSuchKey'):continue
                    raise
                if receipt.get('commit')!=expected:continue
                conf=lam.get_function_configuration(FunctionName=fn)
                assert conf['CodeSha256']==receipt['code_sha256'],fn+' runtime differs'
                runtimes[fn]={'commit':expected,'code_sha256':receipt['code_sha256']}
            assert time.monotonic()<deadline,'exact runtime timeout'
            if len(runtimes)!=len(names):time.sleep(15)
        r.kv(runtimes=runtimes);started=datetime.now(timezone.utc).isoformat();invoked=[]
        for fn in names[:2]:
            response,rejected=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',Payload=encoded({'suppress_alerts':True})))
            result=json.loads(response['Payload'].read())
            assert not response.get('FunctionError'),fn+' invocation error'
            assert (result.get('statusCode')==200 if fn==names[0] else result.get('ok') is True),fn+' collection failed'
            invoked.append({'function':fn,'throttle_rejections':rejected})
        packet=read(CURRENT);output=verify_current(raw)
        assert packet['contract']==CONTRACT and packet['generated_at']>started and packet['source_generated_at']>started
        assert output['call'] is None and all(output[k] is False for k in ('calls_eligible','sizing_eligible','execution_eligible'))
        assert output['signal']['emission_enabled'] is False and output['allocation_pct'] is None
        assert set(output['series_coverage'])==set(KEYS) and len(output['classes'])==6
        assert all(v['observations']>=650 for v in output['series_coverage'].values()),'unexpected truncated original history'
        for scope in [output['headline'],output['treasury'],*output['classes'],output['totals']]:
            assert scope['quality']['status']=='fresh' and scope['unit']=='usd_bn'
            assert scope['ftd_usd_mn']+scope['ftr_usd_mn']==scope['gross_usd_mn']
            assert scope['statistics']['baseline']['current_excluded'] is True
        assert output['headline']['gross_usd_mn']+output['classes'][1]['gross_usd_mn']==output['treasury']['gross_usd_mn']
        assert sum(c['gross_usd_mn'] for c in output['classes'])==output['totals']['gross_usd_mn']
        dealer=read('data/nyfed-primary-dealer.json');join=dealer['settlement_fails']
        assert dealer['generated_at']>started and join['usable'] is True
        assert join['treasury']==output['treasury'] and join['source_replay']==packet['replay']
        assert join['calls_eligible'] is False and join['sizing_eligible'] is False
        marker=read(PREFIX+'migration.json');backup=PRIVATE+marker['sha256']+'.bin'
        body=s3.get_object(Bucket=bucket,Key=backup)['Body'].read(marker['bytes']+1)
        assert len(body)==marker['bytes'] and hashlib.sha256(body).hexdigest()==marker['sha256']
        assert denied('https://'+bucket+'.s3.amazonaws.com/'+backup) and denied('https://justhodl.ai/'+backup)
        # Exercise the actual read-only HTTP branch through authenticated invoke;
        # the packet must remain identical and no collection clock may advance.
        response,_=invoke_when_available(lam,dict(FunctionName=names[0],InvocationType='RequestResponse',
            Payload=encoded({'requestContext':{'http':{'method':'GET'}}})))
        result=json.loads(response['Payload'].read())
        assert not response.get('FunctionError') and result['statusCode']==200
        assert json.loads(result['body'])==packet and read(CURRENT)==packet
        for fn in names:
            receipt=read('data/ops/releases/'+fn+'.json')
            assert receipt['commit']==expected and receipt['code_sha256']==runtimes[fn]['code_sha256']
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==runtimes[fn]['code_sha256']
        proof={'contract':'fails-research-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'commit':expected,'runtimes':runtimes,'research_generated_at':output['generated_at'],'quality':output['quality'],
            'original_replay_reproduced':True,'replay':packet['replay'],'output_sha256':digest(output),
            'original_series':len(output['series_coverage']),'retained_observations':sum(v['observations'] for v in output['series_coverage'].values()),
            'all_six_classes_reconciled':True,'protected_legacy_backup_verified':True,'dealer_context_verified':True,
            'http_read_did_not_recollect':True,'calls_eligible':False,'sizing_eligible':False,'invocations':invoked,
            'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0,
            'acceptance_scope':'Four exact runtimes; original fails collector, dealer collector and read-only HTTP branch. Eurodollar and collateral handlers tested offline, not invoked. No strategy or portfolio qualification.'}
        s3.put_object(Bucket=bucket,Key='data/fails-research-verification.json',Body=encoded(proof),ContentType='application/json',CacheControl='no-store')
        r.kv(**proof)


if __name__=='__main__':
    try:main()
    except Exception:
        print('FR2004 original acceptance failed; inspect committed runner report.')
        sys.exit(1)
