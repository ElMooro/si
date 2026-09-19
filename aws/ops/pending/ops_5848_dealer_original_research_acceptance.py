"""Verify seven runtimes, original dealer replay and safe credit-composite abstention."""
from datetime import datetime,timezone
import hashlib,json,subprocess,sys,time,urllib.request,urllib.error
from pathlib import Path
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(ROOT/'aws/lambdas/justhodl-nyfed-pd/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from dealer_original import encoded,digest,KEYS,REGISTRY_PATH
from dealer_research import CONTRACT,CURRENT,PREFIX
from dealer_research_store import raw_reader,PRIVATE
from replay_dealer_research import verify_current
from dealer_research_context import project


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:
            return response.status in (401,403,404)
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/justhodl-nyfed-pd/source/dealer_research_store.py'],text=True).strip()
    bucket='justhodl-dashboard-live';s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    raw=raw_reader(s3,bucket)
    def read(key):return json.loads(raw(key))
    names=('justhodl-nyfed-pd','justhodl-credit-composite','justhodl-credit-stress','justhodl-institutional-footprint',
           'justhodl-alert-sentinel','justhodl-morning-intelligence','justhodl-treasury-rehypo')
    with report('ops_5848_dealer_original_research_acceptance') as r:
        policy=json.loads(s3.get_bucket_policy(Bucket=bucket)['Policy'])
        deny=next(v for v in policy['Statement'] if v.get('Sid')=='Audit20260909ImmutableOriginalBackups')
        assert deny['Effect']=='Deny' and deny['Principal']=='*'
        assert {'s3:GetObject','s3:GetObjectVersion'}<=set(deny['Action'])
        assert deny['Condition']=={'StringNotEquals':{'aws:PrincipalAccount':'857687956942'}}
        assert 'arn:aws:s3:::'+bucket+'/audit-private/20260909-originals/*' in deny['Resource']
        assert REGISTRY_PATH.read_bytes()==REGISTRY_PATH.parent.parent.joinpath('config/dealer-native-registry.json').read_bytes()
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
            assert not response.get('FunctionError') and result.get('statusCode')==200,fn+' invocation failed'
            body=json.loads(result['body'])
            assert (body.get('published') is True if fn==names[0] else body.get('logged')==0),fn+' expected publication absent'
            invoked.append({'function':fn,'throttle_rejections':rejected})
        packet=read(CURRENT);output=verify_current(raw)
        assert packet['contract']==CONTRACT and packet['generated_at']>started and packet['source_generated_at']>started
        assert output['call'] is None and all(output[k] is False for k in ('calls_eligible','sizing_eligible','execution_eligible'))
        assert set(output['native_series'])==set(KEYS) and len(KEYS)==70
        rows=sum(len(v['history']) for v in output['native_series'].values());assert rows>=45000,'unexpected truncated original history'
        assert output['quality']['counts']['definition_unverified']==14
        assert output['corporate']['reconciliation']['status']=='matched'
        assert output['corporate']['reconciliation']['difference_usd_mn']==0
        for group in output['groups'].values():
            if group['usd_mn'] is not None:assert group['usd_mn']==sum(v['usd_mn'] for v in group['components'].values())
        context=project(packet);assert context['corporate'] and context['treasury_financing']
        assert context['corporate']['squeeze_setup'] is None and context['sizing_eligible'] is False
        assert all(v['daily_average_b'] is None and v['weekly_b'] is None for v in output['transactions'].values())
        assert output['positions_ledger']['TREASURY_EXTIPS']['latest_b'] is None
        marker=read(PREFIX+'migration.json');retained=[]
        for item in marker['artifacts']:
            if item['status']!='retained':continue
            key=PRIVATE+item['sha256']+'.bin';body=s3.get_object(Bucket=bucket,Key=key)['Body'].read(item['bytes']+1)
            assert len(body)==item['bytes'] and hashlib.sha256(body).hexdigest()==item['sha256']
            assert denied('https://'+bucket+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
            retained.append(item['source'])
        assert CURRENT in retained and 'data/history/nyfed-pd.json' in retained and 'data/history/nyfed-pd-corp.json' in retained
        credit=read('data/credit-composite.json')
        assert credit['contract']=='credit-composite-abstention.v1' and credit['generated_at']>started
        assert credit['composite'] is None and credit['plans']==[] and credit['logged']==0 and credit['portfolio_action']=='WAIT'
        assert all(v['pts'] is None for v in credit['lenses'].values())
        legacy=credit['legacy_retention'];key='audit-private/20260909-originals/credit-composite/'+legacy['sha256']+'.bin'
        body=s3.get_object(Bucket=bucket,Key=key)['Body'].read(legacy['bytes']+1)
        assert len(body)==legacy['bytes'] and hashlib.sha256(body).hexdigest()==legacy['sha256']
        assert denied('https://'+bucket+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
        for fn,key in ((names[0],CURRENT),(names[1],'data/credit-composite.json')):
            before=read(key)
            response,_=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',Payload=encoded({'requestContext':{'http':{'method':'GET'}}})))
            result=json.loads(response['Payload'].read())
            assert not response.get('FunctionError') and result['statusCode']==200 and json.loads(result['body'])==before and read(key)==before
        for fn in names:
            receipt=read('data/ops/releases/'+fn+'.json')
            assert receipt['commit']==expected and receipt['code_sha256']==runtimes[fn]['code_sha256']
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==runtimes[fn]['code_sha256']
        proof={'contract':'dealer-research-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
               'commit':expected,'runtimes':runtimes,'research_generated_at':output['generated_at'],'quality':output['quality'],
               'original_replay_reproduced':True,'replay':packet['replay'],'output_sha256':digest(output),
               'original_series':70,'retained_observations':rows,'protected_legacy_artifacts':retained,
               'registry_bytes':len(REGISTRY_PATH.read_bytes()),'registry_twins_identical':True,'same_date_sums_reconciled':True,
               'source_definition_conflicts_withheld':True,'legacy_credit_claim_protected':True,'credit_composite_abstains':True,
               'http_reads_did_not_recollect':True,'invocations':invoked,'paid_ai_calls':0,'notifications_sent':0,
               'private_account_reads':0,'portfolio_writes':0,'calls_eligible':False,'sizing_eligible':False,
               'acceptance_scope':'Seven exact runtimes; original dealer collector and credit abstention only. Five other consumers tested offline, not invoked. Credit full research replay and strategy qualification remain pending.'}
        s3.put_object(Bucket=bucket,Key='data/dealer-research-verification.json',Body=encoded(proof),ContentType='application/json',CacheControl='no-store')
        r.kv(**proof)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Dealer original acceptance failed; inspect committed runner report.')
        sys.exit(1)
