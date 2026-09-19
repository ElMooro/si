"""Verify carry originals, corporate actions, two exact runtimes and read-only publication."""
from datetime import datetime,timezone
from decimal import Decimal
import base64,hashlib,json,subprocess,sys,time,urllib.request,urllib.error
from pathlib import Path
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(ROOT/'aws/lambdas/justhodl-carry-surface/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from carry_research import CONTRACT,CURRENT,PREFIX,encoded,digest,build
from carry_original import strict_json
from carry_store import raw_reader,PRIVATE
from replay_carry_research import verify_current

def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:return response.status in (401,403,404)
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)

def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/justhodl-carry-surface/source/carry_equity.py'],text=True).strip()
    bucket='justhodl-dashboard-live';s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    raw=raw_reader(s3,bucket)
    def read(key):return json.loads(raw(key))
    names=('justhodl-carry-surface','justhodl-stress-index')
    expected_commits={names[0]:expected,names[1]:'8b8c0f1e3ed8a51d9eacdefb366006a5b1d16650'}
    with report('ops_5854_carry_portable_replay_acceptance') as r:
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
                if receipt.get('commit')!=expected_commits[fn]:continue
                conf=lam.get_function_configuration(FunctionName=fn)
                assert conf['CodeSha256']==receipt['code_sha256'],fn+' runtime differs'
                runtimes[fn]={'commit':expected_commits[fn],'code_sha256':receipt['code_sha256']}
            assert time.monotonic()<deadline,'exact runtime timeout'
            if len(runtimes)!=len(names):time.sleep(15)
        r.kv(runtimes=runtimes)
        previous_key=PREFIX+'runs/6a083d9c711fc9e620b129a6b4f2d051cfaa21f82d6e170b85e933341dc41c4c.json'
        previous=read(previous_key)
        assert previous_key==PREFIX+'runs/'+digest(previous)+'.json'
        original=raw(previous['input']['key'])
        assert len(original)==previous['input']['bytes'] and hashlib.sha256(original).hexdigest()==previous['input']['sha256']
        portable,_=build(strict_json(original),raw,previous['generated_at'])
        portable_hash=digest(portable)
        assert portable_hash=='d3828f530d45678bdb58a38f97d956b0d225da317b5d074c5344b93f26928303','portable compiler differs from independently computed Windows output'
        r.kv(portable_original_run=previous_key,portable_output_sha256=portable_hash,windows_linux_exact_match=True)
        started=datetime.now(timezone.utc).isoformat()
        response,rejected=invoke_when_available(lam,dict(FunctionName=names[0],InvocationType='RequestResponse',LogType='Tail',Payload=encoded({'suppress_alerts':True})))
        result=json.loads(response['Payload'].read());r.kv(invoke_status=result.get('statusCode'),throttle_rejections=rejected)
        if result.get('statusCode')!=200:
            # This active handler logs only error classes and sanitized validation counts.
            tail=base64.b64decode(response.get('LogResult','')).decode('utf-8',errors='replace')
            r.kv(active_carry_diagnostics=[line[:1500] for line in tail.splitlines() if '[carry-research]' in line])
        assert not response.get('FunctionError') and result.get('statusCode')==200,'carry invocation failed'
        result=json.loads(result['body']);assert result.get('published') is True,'carry publication missing'
        packet=read(CURRENT);output=verify_current(raw)
        assert packet['contract']==CONTRACT and packet['generated_at']>started
        assert output['methodology_version']=='original_income_and_matched_rates.v2'
        assert output['call'] is None and all(output[k] is False for k in ('calls_eligible','sizing_eligible','execution_eligible'))
        assert output['unwind_overlay']['cohort_fragility'] is None and not output['cross_asset_top']
        assert len(output['equities'])>=100 and len(output['measurements'])>=35,'source coverage needs review'
        assert [len(output['by_class'][k]) for k in ('equity','fx','fixed_income','commodity')]==[110,18,18,14]
        histories={}
        for group in ('measurements','comparisons','equities'):
            for v in output[group].values():
                for key in ('history','prices_history','distributions_history','splits_history'):
                    if v.get(key):histories[v[key]['key']]=v[key]
        rows=sum(ref['observations'] for ref in histories.values());assert rows>200000,'unexpected history truncation'
        hdv=output['equities']['HDV'];income=hdv['trailing_distribution']
        assert income['status']=='reported_window_comparison' and hdv['price_adjustment_status']=='split_reconciled'
        assert income['yield_pct_decimal'] is not None and Decimal(income['yield_pct_decimal'])<Decimal('5'),'HDV split reconciliation regressed'
        splits=read(hdv['splits_history']['key'])['rows']
        assert any(row['date']=='2026-04-29' and Decimal(row['numerator_decimal'])/Decimal(row['denominator_decimal'])==5 for row in splits)
        assert all(v['annualized_pct_decimal'] is None or len(v['annualized_pct_decimal'].split('.')[-1])==8 for item in output['equities'].values() for v in item['realized_price_volatility'].values())
        assert all(r['carry_pct'] is None for r in output['all_assets'])
        assert output['comparisons']['DFII10:DFF_DAILY_GAP']['status']=='real_nominal_comparison_prohibited'
        for sid,v in output['measurements'].items():
            if sid.startswith('FM.'):continue
            assert v['coverage']['complete_bounded_queries'] is True and all(c['complete_query'] for c in v['coverage']['segments'])
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
            assert receipt['commit']==expected_commits[fn] and receipt['code_sha256']==runtimes[fn]['code_sha256']
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==runtimes[fn]['code_sha256']
        proof={'contract':'carry-research-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'commit':expected,'runtimes':runtimes,'research_generated_at':output['generated_at'],'quality':output['quality'],
            'original_replay_reproduced':True,'windows_linux_exact_match':True,'portable_original_run':previous_key,'portable_output_sha256':portable_hash,'replay':packet['replay'],'output_sha256':digest(output),
            'verified_equities':len(output['equities']),'verified_rate_sources':len(output['measurements']),'history_shards':len(histories),'retained_observations':rows,
            'hdv_distribution_income':income,'source_errors':output['source_status_codes'],'protected_legacy_verified':True,'http_read_did_not_recollect':True,
            'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0,'calls_eligible':False,'sizing_eligible':False,
            'acceptance_scope':'Two exact runtimes; carry collector and HTTP read invoked. Stress-index abstention guard tested offline, not invoked. Research and user-entered scenarios only; no qualified strategy.'}
        s3.put_object(Bucket=bucket,Key='data/carry-research-verification.json',Body=encoded(proof),ContentType='application/json',CacheControl='no-store')
        r.kv(**proof)

if __name__=='__main__':
    try:main()
    except Exception:
        print('Carry original acceptance failed; inspect committed runner report.')
        sys.exit(1)
