"""Accept original foreign research and its TIC view against five exact runtimes."""
from datetime import datetime,timezone
import hashlib,json,subprocess,sys,time,urllib.request,urllib.error
from pathlib import Path
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(ROOT/'aws/lambdas/justhodl-foreign-flows/source'),str(ROOT/'aws/lambdas/justhodl-tic-flows/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
import foreign_research as model,foreign_store as store,tic_view
from replay_foreign_research import verify_current,verify_view

def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:return response.status in (401,403,404)
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)

def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/justhodl-foreign-flows/source/foreign_store.py'],text=True).strip()
    bucket='justhodl-dashboard-live';s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    raw=store.raw_reader(s3,bucket)
    def read(key):return json.loads(raw(key))
    names=('justhodl-foreign-flows','justhodl-tic-flows','justhodl-official-pulse','justhodl-global-flow-desk','justhodl-cycle-clock')
    with report('ops_5860_foreign_original_research_acceptance') as r:
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
                if receipt.get('commit')!=expected:continue
                conf=lam.get_function_configuration(FunctionName=fn)
                assert conf['CodeSha256']==receipt['code_sha256'],fn+' runtime differs'
                runtimes[fn]={'commit':expected,'code_sha256':receipt['code_sha256']}
            assert time.monotonic()<deadline,'exact runtime timeout'
            if len(runtimes)!=len(names):time.sleep(15)
        r.kv(runtimes=runtimes);started=datetime.now(timezone.utc).isoformat();invoke_results={}
        for fn in names[:2]:
            response,rejected=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',Payload=model.encoded({'suppress_alerts':True})))
            result=json.loads(response['Payload'].read());r.kv(function=fn,invoke_status=result.get('statusCode'),throttle_rejections=rejected)
            assert not response.get('FunctionError') and result.get('statusCode')==200,fn+' invocation failed'
            result=json.loads(result['body']);assert result.get('published') is True,fn+' publication missing';invoke_results[fn]=result
        packet=read(model.CURRENT);output=verify_current(raw);view=verify_view(raw)
        assert packet['generated_at']>started and output['quality']['status']=='fresh' and output['errors']==0,'source validation needs review'
        assert view['generated_at']>started and view['quality']['status']=='fresh'
        assert view['foreign_output']['sha256']==model.digest(output),'view bound to a different publication'
        for doc in (output,view):assert doc['call'] is None and all(doc[k] is False for k in ('calls_eligible','sizing_eligible','execution_eligible'))
        assert output['archive']['archive_series_count']>=4260 and output['archive']['verified_foreign_series']>=2511
        assert output['native_observations']>=780556 and len(output['groups'])>=156
        assert output['groups']['10308']['scope']=='historical_group'
        assert output['country_lt_treasury']['belgium']['code']=='10251' and output['country_lt_treasury']['luxembourg']['code']=='11703'
        assert output['country_lt_treasury']['china_plus_belgium']['ultimate_owner_attribution_verified'] is False
        assert output['holdings_table']['matching_checks']==output['holdings_table']['distribution_checks']>=325
        assert all(v['rolling_twelve_months_reconciled'] for v in output['holder_splits'].values())
        assert output['absorption']['complete_query'] and output['absorption']['agg_12m']['pct'] is None
        assert output['auctions']['complete_auction_universe_verified'] is False
        assert output['auctions']['eligible_returned_auctions']==output['auctions']['n_auctions_60d']>=15
        assert view['regime']=='MONITOR_ONLY' and view['composite_tic_stress'] is None and view['additional_independent_votes']==0 and view['n_holders']==24
        native_rows=0
        for group in output['groups'].values():
            doc=read(group['history']['key'])
            for series in doc['series'].values():
                assert series['coverage']['complete_native_series_array'] is True and len(series['rows'])==series['coverage']['observations']
                assert len({row['date'] for row in series['rows']})==len(series['rows']);native_rows+=len(series['rows'])
        assert native_rows==output['native_observations']
        for prefix,private,current,fn in ((model.PREFIX,store.PRIVATE,model.CURRENT,names[0]),(tic_view.PREFIX,tic_view.PRIVATE,tic_view.CURRENT,names[1])):
            marker=read(prefix+'migration.json');key=private+marker['sha256']+'.bin';body=s3.get_object(Bucket=bucket,Key=key)['Body'].read(marker['bytes']+1)
            assert len(body)==marker['bytes'] and hashlib.sha256(body).hexdigest()==marker['sha256']
            assert denied('https://'+bucket+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
            before=read(current)
            response,_=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',Payload=model.encoded({'requestContext':{'http':{'method':'GET'}}})))
            result=json.loads(response['Payload'].read())
            assert not response.get('FunctionError') and result['statusCode']==200 and json.loads(result['body'])==before and read(current)==before
            assert len(result['body'].encode())<4*1024*1024
        for fn in names:
            receipt=read('data/ops/releases/'+fn+'.json')
            assert receipt['commit']==expected and receipt['code_sha256']==runtimes[fn]['code_sha256']
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==runtimes[fn]['code_sha256']
        proof={'contract':'foreign-research-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),'commit':expected,'runtimes':runtimes,
            'research_generated_at':output['generated_at'],'view_generated_at':view['generated_at'],'quality':output['quality'],
            'original_replay_reproduced':True,'view_replay_reproduced':True,'replay':packet['replay'],'view_replay':read(tic_view.CURRENT)['replay'],
            'output_sha256':model.digest(output),'view_output_sha256':tic_view.digest(view),
            'native_observations':native_rows,'verified_foreign_series':output['archive']['verified_foreign_series'],'reported_groups':len(output['groups']),
            'holdings_comparisons':output['holdings_table']['distribution_checks'],'all_comparisons_match':True,
            'protected_legacy_verified':True,'http_reads_did_not_recollect':True,'source_errors':output['source_status_codes'],
            'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0,'calls_eligible':False,'sizing_eligible':False,
            'acceptance_scope':'Five exact runtimes. Only original foreign collector, verified TIC view and read-only HTTP branches invoked. Official Pulse, Global Flow Desk and Cycle Clock boundaries tested offline, not invoked. Research and entered scenarios; no qualified strategy.'}
        s3.put_object(Bucket=bucket,Key='data/foreign-research-verification.json',Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store');r.kv(**proof)

if __name__=='__main__':
    try:main()
    except Exception:
        print('Foreign original acceptance failed; inspect committed runner report.')
        sys.exit(1)
