"""Accept exact FRED vault runtime, public-only refresh, source replay and bounded rotation."""
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
from replay_fred_levels import verify_vault, read_public
from fred_level_io import body, immutable
import fred_level_model as model

FN='justhodl-tradingview';BUCKET='justhodl-dashboard-live';KEY='data/tradingview.json'
SELECTED=['CFNAI','DGS10','DGS2','WALCL','WTREGEN','RRPONTSYD','SOFR','VIXCLS','PAYEMS','DFF','ICSA','M2SL','NFCI','DTWEXBGS']


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
    with report('ops_5899_fred_vault_acceptance') as r:
        commit=subprocess.check_output(['git','log','-1','--format=%H','--',str(SOURCE/'lambda_function.py')],cwd=ROOT,text=True).strip()
        receipt=json.loads(public('data/ops/releases/'+FN+'.json'));cfg=lam.get_function_configuration(FunctionName=FN)
        assert receipt['commit']==commit and receipt['code_sha256']==cfg['CodeSha256']
        assert cfg['State']=='Active' and cfg['LastUpdateStatus']=='Successful'
        with urllib.request.urlopen(lam.get_function(FunctionName=FN)['Code']['Location'],timeout=45) as response:archive=response.read(32*1024*1024+1)
        assert len(archive)<=32*1024*1024 and base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256']
        with zipfile.ZipFile(io.BytesIO(archive)) as z:
            for name in ('lambda_function.py','fred_level_io.py','fred_level_model.py'):assert z.read(name)==(SOURCE/name).read_bytes()
            for name in ('evidence_store.py','macro_observations.py','public_brain_projection.py'):assert z.read(name)==(ROOT/'aws/shared'/name).read_bytes()
        r.kv(runtime_commit=commit,code_sha256=cfg['CodeSha256'],private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0)
        before=body(s3.get_object(Bucket=BUCKET,Key=KEY));previous=json.loads(before)
        assert json.loads(public(KEY))==previous
        archive_key='audit-private/20260909-originals/fred-vault/'+model.digest(before)+'.bin'
        immutable(s3,BUCKET,archive_key,before,private=True)
        assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+archive_key) and denied('https://justhodl.ai/'+archive_key)
        preserved={'sha256':model.digest(before),'bytes':len(before),'anonymous_denied':True}
        r.kv(whole_preceding_public_product=preserved)
        started=datetime.now(timezone.utc)
        response,rejections=invoke_when_available(lam,dict(FunctionName=FN,InvocationType='RequestResponse',
            Payload=model.encoded({'public_fred_refresh':True,'series':SELECTED})),wait_seconds=90)
        result=json.loads(response['Payload'].read())
        r.kv(engine_invocations=1,mode='public_fred_refresh; no private registry',result=result,
             function_error=response.get('FunctionError'),request_id=response.get('ResponseMetadata',{}).get('RequestId'),throttle_rejections=rejections)
        assert not response.get('FunctionError') and result.get('ok') is True, 'Read the report before another invocation'
        raw=public(KEY);packet=json.loads(raw)
        assert packet['generated_at']==previous['generated_at'], 'Partial update renewed whole-catalog clock'
        assert model.clock(packet['fred_level_refresh']['generated_at'])>=started
        selected_rows=[row for row in packet['symbols'] if row.get('series_id') in SELECTED and row.get('contract_version')==model.CONTRACT]
        assert set(row['series_id'] for row in selected_rows)==set(SELECTED)
        assert len(packet['symbols'])==len(previous['symbols'])
        originals={row['symbol']:row for row in previous['symbols']}
        selected_symbols={row['symbol'] for row in selected_rows}
        assert all(row==originals[row['symbol']] for row in packet['symbols'] if row['symbol'] not in selected_symbols)
        verification=verify_vault(packet)
        # Every currently refreshed alias has the same complete replay reference and comparison.
        for sid in SELECTED:
            refs={json.dumps(row['replay'],sort_keys=True) for row in selected_rows if row['series_id']==sid}
            assert len(refs)==1
        r.kv(original_source_replay=verification,selected_series=len(SELECTED),selected_alias_rows=len(selected_rows),whole_catalog_clock_preserved=True)
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
        name='tradingview-fred-native-rotation';payload='{"public_fred_refresh":true}'
        target={'Arn':cfg['FunctionArn'],'RoleArn':legacy['Target']['RoleArn'],'Input':payload,
                'RetryPolicy':{'MaximumEventAgeInSeconds':900,'MaximumRetryAttempts':0}}
        try:
            existing=scheduler.get_schedule(Name=name,GroupName='default')
        except scheduler.exceptions.ResourceNotFoundException:
            scheduler.create_schedule(Name=name,GroupName='default',ScheduleExpression='rate(30 minutes)',
                ScheduleExpressionTimezone='UTC',FlexibleTimeWindow={'Mode':'OFF'},State='ENABLED',Target=target,
                Description='Bounded public-only native FRED rotation; no Brain, paid AI, notifications or portfolio inputs')
            existing=scheduler.get_schedule(Name=name,GroupName='default')
        assert existing['State']=='ENABLED' and existing['ScheduleExpression']=='rate(30 minutes)' and existing['ScheduleExpressionTimezone']=='UTC'
        assert existing['Target']==target
        assert lam.get_function_configuration(FunctionName=FN)['CodeSha256']==cfg['CodeSha256']
        assert json.loads(public('data/ops/releases/'+FN+'.json'))['commit']==commit
        proof={'contract':'fred-vault-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'runtime_commit':commit,'code_sha256':cfg['CodeSha256'],'engine_invocations':1,'public_only':True,
            'whole_preceding_public_product':preserved,'catalog_generated_at':packet['generated_at'],
            'fred_refresh_generated_at':packet['fred_level_refresh']['generated_at'],'public_sha256':model.digest(raw),
            'source_replay':verification,'selected_series':SELECTED,'selected_alias_rows':len(selected_rows),
            'pages_commit':pages_commit,'built_assets':{k:build['files_sha256'][k] for k in ('tradingview.html','jh-domain-monitor.js')},
            'rotation':{'name':name,'schedule':'rate(30 minutes)','max_series_per_run':64,'natural_run_observed':False},
            'remaining':'Full candidate rotation and natural daily reuse not yet observed. Age ceilings are not release calendars; current vintages are not backtest availability. No predictive or portfolio authority.'}
        s3.put_object(Bucket=BUCKET,Key='data/fred-vault-verification.json',Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store')
        assert json.loads(public('data/fred-vault-verification.json'))==proof
        r.kv(proof_key='data/fred-vault-verification.json',pages_commit=pages_commit,rotation=proof['rotation'],remaining=proof['remaining'])


if __name__=='__main__':
    try:main()
    except Exception:
        print('FRED vault acceptance failed; inspect its committed report before another invocation.')
        sys.exit(1)
