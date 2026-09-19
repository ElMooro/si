"""Five exact runtimes; invoke only original public warehouse and deterministic research."""
from datetime import datetime,timezone
import hashlib,json,subprocess,sys,time,urllib.request,urllib.error
from pathlib import Path
import boto3
from botocore.config import Config

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),
             str(ROOT/'aws/lambdas/justhodl-liquidity-reversal/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from reversal_research import CONTRACT,CURRENT,PREFIX,SERIES,encoded,digest
from reversal_store import raw_reader,PRIVATE
from replay_reversal_research import verify_current
from reversal_consumer_context import native_yield


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:
            return response.status in (401,403,404)
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--',
        'aws/lambdas/justhodl-liquidity-reversal/source/reversal_store.py'],text=True).strip()
    bucket='justhodl-dashboard-live';s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    raw=raw_reader(s3,bucket)
    def read(key):return json.loads(raw(key))
    names=('justhodl-daily-report-v3','justhodl-liquidity-reversal','justhodl-catalyst','justhodl-physical-econ','justhodl-stock-buying')
    with report('ops_5845_reversal_research_acceptance') as r:
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
        for fn,payload in ((names[0],{'action':'research_measurements','suppress_alerts':True}),(names[1],{'suppress_alerts':True})):
            response,rejected=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',Payload=encoded(payload)))
            result=json.loads(response['Payload'].read())
            assert not response.get('FunctionError') and result.get('statusCode')==200,fn+' research invocation failed'
            invoked.append({'function':fn,'throttle_rejections':rejected})
        packet=read(CURRENT);output=verify_current(raw)
        assert packet['contract']==CONTRACT and packet['generated_at']>started and packet['source_generated_at']>started
        assert output['call'] is None and all(output[k] is False for k in ('calls_eligible','sizing_eligible','execution_eligible'))
        assert output['liquidity']['trend_score'] is None and output['liquidity']['reversal_score'] is None
        assert output['portfolio_consequences']['allocation'] is None
        marker=read(PREFIX+'migration.json');inventory=read(marker['inventory']['key'])
        actual={row['symbol'] for row in output['rows'] if row['inventory_entry']}
        assert actual=={row['symbol'] for row in inventory['rows']}
        assert len(actual)==marker['inventory_entries']>=1086,'legacy membership lost'
        assert {row['symbol'] for row in output['rows']}>=set('FRED:'+sid for sid in SERIES)
        originals={row['symbol'][5:]:row for row in output['rows'] if row['original_provider_verified']}
        required=('CPFF','DPSACBW027SBOG','FEDFUNDS','MMMFFAQ027S','NFCILEVERAGE','RMFSL','TOTBKCR','TREASURY','WLCFLL','DGS10','WALCL')
        assert set(required)<=set(originals),'required exact native source missing'
        assert originals['TREASURY']['status']=='historical_discontinued' and not originals['TREASURY']['measurement_eligible']
        assert originals['FEDFUNDS']['measurement']['frequency']=='M'
        assert originals['MMMFFAQ027S']['measurement']['frequency']=='Q'
        assert all(row['polarity'] is None and row['calls_eligible'] is False for row in output['rows'])
        assert output['dependency_map']['effective_independent_inputs'] is None
        source=read('data/report-measurements.json')
        assert source['net_liquidity']['formula']=='WALCL - WTREGEN - 1000 * RRPONTSYD'
        yield_row=native_yield(s3,bucket)
        assert yield_row['status']=='verified_native_measurement' and yield_row['unit']=='Percent'
        assert yield_row['date']==originals['DGS10']['date'] and yield_row['value']==originals['DGS10']['last']
        backup=PRIVATE+marker['sha256']+'.bin'
        body=s3.get_object(Bucket=bucket,Key=backup)['Body'].read(marker['bytes']+1)
        assert len(body)==marker['bytes'] and hashlib.sha256(body).hexdigest()==marker['sha256']
        assert denied('https://'+bucket+'.s3.amazonaws.com/'+backup) and denied('https://justhodl.ai/'+backup)
        for fn in names:
            receipt=read('data/ops/releases/'+fn+'.json')
            assert receipt['commit']==expected and receipt['code_sha256']==runtimes[fn]['code_sha256']
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==runtimes[fn]['code_sha256']
        proof={'contract':'reversal-research-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'commit':expected,'runtimes':runtimes,'research_generated_at':output['generated_at'],
            'quality':output['quality'],'inventory_entries':len(actual),'all_inventory_entries_retained':True,
            'original_replay_reproduced':True,'replay':packet['replay'],'output_sha256':digest(output),
            'protected_legacy_backup_verified':True,'native_dgs10':yield_row,'calls_eligible':False,'sizing_eligible':False,
            'invocations':invoked,'consumer_handlers_invoked':False,'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0,
            'acceptance_scope':'Five exact runtimes; original public source collection and deterministic Reversal only. Three consumer boundaries tested offline; no strategy or portfolio qualification.'}
        s3.put_object(Bucket=bucket,Key='data/reversal-research-verification.json',Body=encoded(proof),ContentType='application/json',CacheControl='no-store')
        r.kv(**proof)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Reversal research acceptance failed; inspect committed runner report.')
        sys.exit(1)
