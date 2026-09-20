"""Accept exact price runtime, public-only refresh, original replay and bounded rotation."""
from datetime import datetime, timezone
from pathlib import Path
import base64, hashlib, io, json, re, subprocess, sys, urllib.error, urllib.request, zipfile
import boto3
from botocore.config import Config

ROOT=Path(__file__).resolve().parents[3]
SOURCE=ROOT/'aws/lambdas/justhodl-tradingview/source'
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(SOURCE)]
from ops_report import report
from acceptance_invoke import invoke_when_available
from replay_price_observations import verify_vault
from fred_level_io import body, immutable
import price_model as model

FN='justhodl-tradingview';BUCKET='justhodl-dashboard-live';KEY='data/tradingview.json'
SELECTED=['fmp:AAPL','fmp:MSFT','fmp:NVDA','yahoo:DX-Y.NYB','yahoo:^MOVE','yahoo:GC=F',
          'yahoo:BTC-USD','yahoo:000001.SS','yahoo:USCA','polygon:EFS']


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
    s3=boto3.client('s3',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_5901_price_vault_acceptance') as r:
        commit=subprocess.check_output(['git','log','-1','--format=%H','--',str(SOURCE/'lambda_function.py')],cwd=ROOT,text=True).strip()
        runtimes={}
        for fn,names in ((FN,('lambda_function.py','fred_level_io.py','fred_level_model.py','price_io.py','price_model.py')),
                         ('justhodl-domain-barometers',('lambda_function.py','barometer_integrity.py'))):
            receipt=json.loads(public('data/ops/releases/'+fn+'.json'));cfg=lam.get_function_configuration(FunctionName=fn)
            assert receipt['commit']==commit and receipt['code_sha256']==cfg['CodeSha256']
            assert cfg['State']=='Active' and cfg['LastUpdateStatus']=='Successful'
            with urllib.request.urlopen(lam.get_function(FunctionName=fn)['Code']['Location'],timeout=45) as response:archive=response.read(32*1024*1024+1)
            assert len(archive)<=32*1024*1024 and base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256']
            with zipfile.ZipFile(io.BytesIO(archive)) as z:
                for name in names:assert z.read(name)==(ROOT/'aws/lambdas'/fn/'source'/name).read_bytes()
                if fn==FN:
                    for name in ('evidence_store.py','daily_market_model.py','public_brain_projection.py'):assert z.read(name)==(ROOT/'aws/shared'/name).read_bytes()
            runtimes[fn]={'commit':commit,'code_sha256':cfg['CodeSha256'],'packaged_sources_match':True}
        cfg=lam.get_function_configuration(FunctionName=FN)
        r.kv(runtimes=runtimes,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        before=body(s3.get_object(Bucket=BUCKET,Key=KEY));previous=json.loads(before)
        assert json.loads(public(KEY))==previous
        archive_key='audit-private/20260909-originals/price-vault/'+model.digest(before)+'.bin'
        immutable(s3,BUCKET,archive_key,before,private=True)
        assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+archive_key) and denied('https://justhodl.ai/'+archive_key)
        preserved={'sha256':model.digest(before),'bytes':len(before),'anonymous_denied':True}
        r.kv(whole_preceding_public_product=preserved)
        started=datetime.now(timezone.utc)
        response,rejections=invoke_when_available(lam,dict(FunctionName=FN,InvocationType='RequestResponse',
            Payload=model.encoded({'public_price_refresh':True,'instruments':SELECTED})),wait_seconds=90)
        result=json.loads(response['Payload'].read())
        r.kv(engine_invocations=1,mode='public_price_refresh; no private registry',result=result,
             function_error=response.get('FunctionError'),request_id=response.get('ResponseMetadata',{}).get('RequestId'),throttle_rejections=rejections)
        assert not response.get('FunctionError') and result.get('ok') is True, 'Read the report before another invocation'
        raw=public(KEY);packet=json.loads(raw)
        assert packet['generated_at']==previous['generated_at'], 'Partial update renewed whole-catalog clock'
        assert model.clock(packet['price_observation_refresh']['generated_at'])>=started
        selected_rows=[row for row in packet['symbols'] if row.get('instrument_id') in SELECTED and row.get('contract_version')==model.CONTRACT]
        assert set(row['instrument_id'] for row in selected_rows)==set(SELECTED)
        assert len(packet['symbols'])==len(previous['symbols'])
        originals={row['symbol']:row for row in previous['symbols']};selected_symbols={row['symbol'] for row in selected_rows}
        assert all(row==originals[row['symbol']] for row in packet['symbols'] if row['symbol'] not in selected_symbols)
        verification=verify_vault(packet)
        for identity in SELECTED:
            assert len({row['replay']['sha256'] for row in selected_rows if row['instrument_id']==identity})==1
        observations=[{k:row.get(k) for k in ('symbol','instrument_id','value','prev','observation_date','previous_observation_date','unit','status')}
                      for row in selected_rows]
        r.kv(original_source_replay=verification,observations=observations,whole_catalog_clock_preserved=True)
        build=json.loads(public('build-manifest.json'));pages_commit=build['commit_sha']
        subprocess.run(['git','merge-base','--is-ancestor',commit,pages_commit],cwd=ROOT,check=True)
        subprocess.run(['git','diff','--quiet',commit,pages_commit,'--','.',':(exclude)aws/ops/**',':(exclude)docs/**'],cwd=ROOT,check=True)
        for name in ('tradingview.html','jh-domain-monitor.js'):
            served=public(name);built,count=re.subn(rb'<script\b[^>]*\bsrc="https://static\.cloudflareinsights\.com/beacon\.min\.js/v[a-f0-9]+"[^>]*></script>\n?',b'',served)
            assert count<=1 and model.digest(built)==build['files_sha256'][name]
            if name=='tradingview.html':assert b'jh-domain-monitor.js' in served and b'Retained inputs and replay' in served
        legacy=scheduler.get_schedule(Name='tradingview-vault-daily',GroupName='default')
        assert legacy['State']=='ENABLED' and legacy['ScheduleExpression']=='cron(35 11 * * ? *)'
        assert legacy['Target']['Arn'] in (cfg['FunctionArn'],cfg['FunctionArn']+':$LATEST')
        name='tradingview-native-price-rotation';payload='{"public_price_refresh":true}'
        target={'Arn':cfg['FunctionArn'],'RoleArn':legacy['Target']['RoleArn'],'Input':payload,
                'RetryPolicy':{'MaximumEventAgeInSeconds':900,'MaximumRetryAttempts':0}}
        try:existing=scheduler.get_schedule(Name=name,GroupName='default')
        except scheduler.exceptions.ResourceNotFoundException:
            scheduler.create_schedule(Name=name,GroupName='default',ScheduleExpression='cron(15 * * * ? *)',
                ScheduleExpressionTimezone='UTC',FlexibleTimeWindow={'Mode':'OFF'},State='ENABLED',Target=target,
                Description='Bounded public-only price rotation with core instruments each hour; existing market-data providers, no AI/private inputs')
            existing=scheduler.get_schedule(Name=name,GroupName='default')
        assert existing['State']=='ENABLED' and existing['ScheduleExpression']=='cron(15 * * * ? *)' and existing['ScheduleExpressionTimezone']=='UTC'
        assert existing['Target']==target
        for fn,accepted in runtimes.items():
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==accepted['code_sha256']
            assert json.loads(public('data/ops/releases/'+fn+'.json'))['commit']==commit
        proof={'contract':'price-vault-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'runtimes':runtimes,'engine_invocations':1,'public_only':True,'whole_preceding_public_product':preserved,
            'catalog_generated_at':packet['generated_at'],'price_refresh_generated_at':packet['price_observation_refresh']['generated_at'],
            'public_sha256':model.digest(raw),'source_replay':verification,'selected_instruments':SELECTED,
            'observations':observations,'pages_commit':pages_commit,
            'built_assets':{k:build['files_sha256'][k] for k in ('tradingview.html','jh-domain-monitor.js')},
            'rotation':{'name':name,'schedule':'hourly at minute 15 UTC','max_instruments_per_run':64,'natural_run_observed':False},
            'consumer':'Domain Barometers deployed with unqualified price-comparison exclusion; natural publication pending, producer not invoked',
            'remaining':'Full quote-universe rotation, historical security identity and exchange calendars remain unqualified. Provider units and partial/undated comparisons are explicit. No forecasting, sizing or execution authority.'}
        s3.put_object(Bucket=BUCKET,Key='data/price-vault-verification.json',Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public('data/price-vault-verification.json'))==proof
        r.kv(proof_key='data/price-vault-verification.json',pages_commit=pages_commit,rotation=proof['rotation'],remaining=proof['remaining'])


if __name__=='__main__':
    try:main()
    except Exception:
        print('Price vault acceptance failed; inspect its committed report before another invocation.')
        sys.exit(1)
