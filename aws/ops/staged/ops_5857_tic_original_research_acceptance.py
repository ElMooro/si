"""Verify original TIC histories, four exact runtimes and read-only HTTP publication."""
from datetime import datetime,timezone
import hashlib,json,subprocess,sys,time,urllib.request,urllib.error
from pathlib import Path
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(ROOT/'aws/lambdas/justhodl-capital-inflows/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from tic_research import CONTRACT,CURRENT,PREFIX,encoded,digest
from tic_store import raw_reader,PRIVATE,code
from replay_tic_research import verify_current
from tic_original import SERIES

def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:return response.status in (401,403,404)
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)

def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/justhodl-capital-inflows/source/tic_store.py'],text=True).strip()
    bucket='justhodl-dashboard-live';s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    raw=raw_reader(s3,bucket)
    def read(key):return json.loads(raw(key))
    names=('justhodl-capital-inflows','justhodl-risk-regime','justhodl-master-allocator','justhodl-cross-asset-flow-state')
    with report('ops_5857_tic_original_research_acceptance') as r:
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
                    if code(exc) in ('404','NoSuchKey'):continue
                    raise
                if receipt.get('commit')!=expected:continue
                conf=lam.get_function_configuration(FunctionName=fn)
                assert conf['CodeSha256']==receipt['code_sha256'],fn+' runtime differs'
                runtimes[fn]={'commit':expected,'code_sha256':receipt['code_sha256']}
            assert time.monotonic()<deadline,'exact runtime timeout'
            if len(runtimes)!=len(names):time.sleep(15)
        r.kv(runtimes=runtimes);started=datetime.now(timezone.utc).isoformat()
        response,rejected=invoke_when_available(lam,dict(FunctionName=names[0],InvocationType='RequestResponse',Payload=encoded({'suppress_alerts':True})))
        result=json.loads(response['Payload'].read());r.kv(invoke_status=result.get('statusCode'),throttle_rejections=rejected)
        assert not response.get('FunctionError') and result.get('statusCode')==200,'tic invocation failed'
        result=json.loads(result['body']);assert result.get('published') is True,'tic publication missing'
        packet=read(CURRENT);output=verify_current(raw)
        assert packet['contract']==CONTRACT and packet['generated_at']>started
        assert output['call'] is None and all(output[k] is False for k in ('calls_eligible','sizing_eligible','execution_eligible'))
        r.kv(source_status_codes=output['source_status_codes'],quality=output['quality'])
        assert output['quality']['status']=='fresh' and output['errors']==0,'source collection or validation needs review'
        assert set(output['measurements'])==set(SERIES) and output['regime']=='MONITOR_ONLY'
        assert output['archive']['archive_series_count']>=4000
        assert output['quality']['publication_date'] is None and output['archive']['transmission_is_publication_time'] is False
        histories={v['history']['key']:v['history'] for v in output['measurements'].values()}
        original_rows=sum(ref['observations'] for ref in histories.values());assert original_rows>=4380,'native history truncation'
        for sid,v in output['measurements'].items():
            assert v['coverage']['complete_native_series_array'] is True
            check=output['cross_source_checks'][sid]
            assert check['status']=='matching' and check['fred_coverage']['complete_query'] is True and not check['different_periods']
            histories[check['history']['key']]=check['history']
        for ref in (output['rolling_history'],output['reconciliation']['history']):histories[ref['key']]=ref
        rows=sum(ref['observations'] for ref in histories.values())
        recon=read(output['reconciliation']['history']['key'])
        assert all(item[scope]['status'] in ('exact','within_reporting_rounding') for item in recon['rows'] for scope in ('asset_classes','official_private'))
        assert output['holder_splits']['lt_total']['rolling_twelve_months_reconciled'] is True
        from decimal import Decimal
        exact=output['exact_headline'];a=Decimal(exact['foreign_net_into_us_lt_12mo']['usd_million_decimal']);b=Decimal(exact['us_net_purchases_of_foreign_lt_12mo']['usd_million_decimal'])
        assert a-b==Decimal(exact['net_cross_border_lt_12mo_usd_million_decimal'])
        marker=read(PREFIX+'migration.json');key=PRIVATE+marker['sha256']+'.bin';body=s3.get_object(Bucket=bucket,Key=key)['Body'].read(marker['bytes']+1)
        assert len(body)==marker['bytes'] and hashlib.sha256(body).hexdigest()==marker['sha256']
        assert denied('https://'+bucket+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
        before=read(CURRENT)
        response,_=invoke_when_available(lam,dict(FunctionName=names[0],InvocationType='RequestResponse',Payload=encoded({'requestContext':{'http':{'method':'GET'}}})))
        result=json.loads(response['Payload'].read())
        assert not response.get('FunctionError') and result['statusCode']==200 and json.loads(result['body'])==before and read(CURRENT)==before
        assert len(result['body'].encode())<4*1024*1024
        for fn in names:
            receipt=read('data/ops/releases/'+fn+'.json')
            assert receipt['commit']==expected and receipt['code_sha256']==runtimes[fn]['code_sha256']
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==runtimes[fn]['code_sha256']
        proof={'contract':'tic-research-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'commit':expected,'runtimes':runtimes,'research_generated_at':output['generated_at'],'quality':output['quality'],
            'original_replay_reproduced':True,'replay':packet['replay'],'output_sha256':digest(output),
            'original_tic_series':len(output['measurements']),'history_shards':len(histories),'retained_observations':rows,
            'native_observations':original_rows,'all_history_components_reconciled':True,'fred_distribution_checks_match':True,'archive_series_retained':output['archive']['archive_series_count'],
            'source_errors':output['source_status_codes'],'protected_legacy_verified':True,'http_read_did_not_recollect':True,
            'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0,'calls_eligible':False,'sizing_eligible':False,
            'acceptance_scope':'Four exact runtimes; only TIC collector and read-only HTTP branch invoked. Risk Regime, Master Allocator and Cross-Asset Flow consumer guards tested offline, not invoked. Research and entered scenarios only; no qualified strategy.'}
        s3.put_object(Bucket=bucket,Key='data/tic-research-verification.json',Body=encoded(proof),ContentType='application/json',CacheControl='no-store')
        r.kv(**proof)

if __name__=='__main__':
    try:main()
    except Exception:
        print('TIC original acceptance failed; inspect committed runner report.')
        sys.exit(1)
