"""Accept exact ETF runtime, complete original replay, preservation and permissions."""
from datetime import datetime,timezone
from pathlib import Path
import hashlib,json,subprocess,sys,time,urllib.request,urllib.error
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(ROOT/'aws/lambdas/justhodl-etf-true-flows/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
import etf_native as native,etf_research as model,etf_store as store
from replay_etf_research import verify_current

def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:return response.status in (401,403,404)
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)

def main():
    fn='justhodl-etf-true-flows';bucket='justhodl-dashboard-live'
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+fn],text=True).strip()
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    raw=store.raw_reader(s3,bucket)
    def read(key):return json.loads(raw(key))
    with report('ops_5869_etf_original_research_acceptance') as r:
        policy=json.loads(s3.get_bucket_policy(Bucket=bucket)['Policy']);deny=next(v for v in policy['Statement'] if v.get('Sid')=='Audit20260909ImmutableOriginalBackups')
        assert deny['Effect']=='Deny' and deny['Principal']=='*'
        assert {'s3:GetObject','s3:GetObjectVersion'}<=set(deny['Action'])
        assert deny['Condition']=={'StringNotEquals':{'aws:PrincipalAccount':'857687956942'}}
        assert 'arn:aws:s3:::'+bucket+'/audit-private/20260909-originals/*' in deny['Resource']
        deadline=time.monotonic()+2400
        while True:
            receipt=read('data/ops/releases/'+fn+'.json')
            if receipt.get('commit')==expected:break
            assert time.monotonic()<deadline,'exact runtime timeout';time.sleep(15)
        conf=lam.get_function_configuration(FunctionName=fn);assert conf['CodeSha256']==receipt['code_sha256']
        assert conf['Timeout']==900 and conf['MemorySize']==1536
        r.kv(commit=expected,code_sha256=conf['CodeSha256']);started=datetime.now(timezone.utc).isoformat()
        response,rejected=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',Payload=model.encoded({'suppress_alerts':True})))
        result=json.loads(response['Payload'].read());r.kv(invoke_status=result.get('statusCode'),throttle_rejections=rejected)
        assert not response.get('FunctionError') and result.get('statusCode')==200,'ETF source invocation failed'
        result=json.loads(result['body']);r.kv(invocation=result);assert result.get('published') is True
        packet=read(model.CURRENT);output=verify_current(raw)
        assert output['generated_at']>started and output['contract']==model.CONTRACT
        assert output['call'] is None and all(output[k] is False for k in ('calls_eligible','sizing_eligible','execution_eligible'))
        assert output['additional_independent_calls_votes']==0 and output['portfolio_impact'] is None
        quality=output['quality'];assert quality['native_histories']==81 and quality['aligned_five_observation_estimates']==81 and quality['native_observations']>=429188
        assert quality['status']=='partial' and not quality['all_funds_covered'] and len(output['gaps'])==47
        assert output['source_status_codes']=={},'source errors need explicit review'
        assert output['aggregation_period']['end_date']>='2026-09-17'
        assert output['ground_truth']['status']=='not_reconciled' and output['by_stock']['status']=='not_measured'
        histories=0;native_rows=0;proshares_rows=0;zero_rows=0
        for ticker,value in output['by_etf'].items():
            assert value['calls_eligible'] is False and value['sizing_eligible'] is False
            if not value['history']:
                assert all(value['net_flow_'+k+'_usd'] is None for k in ('1d','5d','20d'));continue
            history=read(value['history']['key']);rows=history['rows'];histories+=1;native_rows+=len(rows)
            assert len(rows)==value['history_observations'] and len({v['date'] for v in rows})==len(rows)
            if value['identity']['issuer']=='ProShares':
                for row in rows:
                    assert native.dec(row['shares_decimal'])==native.dec(row['shares_original_thousands_decimal'])*1000
                    proshares_rows+=1
            if ticker in ('KRE','XOP'):
                zeros=[v for v in rows if v['nav_decimal']=='0'];assert zeros and all(v['nav_valued_share_change_decimal'] is None for v in zeros);zero_rows+=len(zeros)
            if value['net_flow_5d_usd'] is not None:assert value['flow_windows']['5d']['end_date']==output['aggregation_period']['end_date']
        assert histories==81 and native_rows==quality['native_observations'] and proshares_rows>1000
        marker=read(model.PREFIX+'migration.json');backups=[]
        assert len(marker['objects'])==3
        for value in marker['objects']:
            assert value.get('protected_backup') is True,'expected whole legacy source missing'
            key=store.PRIVATE+value['sha256']+'.bin';body=s3.get_object(Bucket=bucket,Key=key)['Body'].read(value['bytes']+1)
            assert len(body)==value['bytes'] and hashlib.sha256(body).hexdigest()==value['sha256']
            assert denied('https://'+bucket+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key);backups.append(value['source'])
        before=read(model.CURRENT)
        response,_=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',Payload=model.encoded({'requestContext':{'http':{'method':'GET'}}})))
        result=json.loads(response['Payload'].read());assert not response.get('FunctionError') and result['statusCode']==200 and json.loads(result['body'])==before and read(model.CURRENT)==before
        assert len(result['body'].encode())<4*1024*1024
        scheduler=boto3.client('scheduler',region_name='us-east-1');schedule=scheduler.get_schedule(Name='justhodl-etf-true-flows-daily',GroupName='default')
        assert schedule['ScheduleExpression']=='cron(45 15 * * ? *)' and schedule['State']=='ENABLED'
        assert schedule['Target']['Arn'].split(':function:')[-1] in (fn,fn+':$LATEST')
        after=read('data/ops/releases/'+fn+'.json');assert after['commit']==expected and after['code_sha256']==conf['CodeSha256']
        assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==conf['CodeSha256']
        proof={'contract':'etf-research-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),'commit':expected,'code_sha256':conf['CodeSha256'],
          'research_generated_at':output['generated_at'],'quality':quality,'original_replay_reproduced':True,'replay':packet['replay'],'output_sha256':model.digest(output),
          'native_histories':histories,'native_observations':native_rows,'proshares_unit_verified_rows':proshares_rows,'zero_nav_source_rows_preserved':zero_rows,
          'protected_legacy_verified':backups,'http_reads_did_not_recollect':True,'source_errors':output['source_status_codes'],
          'schedule':{'name':schedule['Name'],'cron':schedule['ScheduleExpression'],'state':schedule['State']},
          'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0,'calls_eligible':False,'sizing_eligible':False,
          'acceptance_scope':'Exact ETF runtime, original-source replay and preserved complete legacy objects. Dated issuer estimates for 81 of 128 configured funds; no qualified return strategy or independent calendar completeness.'}
        s3.put_object(Bucket=bucket,Key='data/etf-research-verification.json',Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store');r.kv(**proof)

if __name__=='__main__':
    try:main()
    except Exception:
        print('ETF original acceptance failed; inspect committed runner report.')
        sys.exit(1)
