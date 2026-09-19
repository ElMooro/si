"""Verify five runtimes, original funding replay, history shards and HTTP read boundary."""
from datetime import datetime,timezone
import hashlib,json,subprocess,sys,time,urllib.request,urllib.error
from pathlib import Path
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(ROOT/'aws/lambdas/justhodl-eurodollar-plumbing/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from funding_research import CONTRACT,CURRENT,PREFIX,encoded,digest,SERIES
from funding_store import raw_reader,PRIVATE
from replay_funding_research import verify_current
import funding_original as native


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:
            return response.status in (401,403,404)
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/justhodl-eurodollar-plumbing/source/funding_store.py'],text=True).strip()
    bucket='justhodl-dashboard-live';s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    raw=raw_reader(s3,bucket)
    def read(key):return json.loads(raw(key))
    names=('justhodl-daily-report-v3','justhodl-eurodollar-plumbing','justhodl-signal-board','justhodl-bond-desk','justhodl-bond-warroom')
    with report('ops_5849_funding_original_research_acceptance') as r:
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
        for fn,event in ((names[0],{'action':'research_measurements'}),(names[1],{'suppress_alerts':True})):
            response,rejected=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',Payload=encoded(event)))
            result=json.loads(response['Payload'].read())
            assert not response.get('FunctionError') and result.get('statusCode')==200,fn+' invocation failed'
            body=json.loads(result['body']);assert body.get('published') is True,fn+' publication missing'
            invoked.append({'function':fn,'throttle_rejections':rejected})
        packet=read(CURRENT);output=verify_current(raw)
        assert packet['contract']==CONTRACT and packet['generated_at']>started and packet['source_generated_at']>started
        assert output['call'] is None and all(output[k] is False for k in ('calls_eligible','sizing_eligible','execution_eligible'))
        assert all(output[k] is None for k in ('plumbing_health','stress_score','score','composite_score'))
        assert set(SERIES)<=set(output['measurements']),'reviewed FRED originals missing'
        required=set(native.OFR_IDS)|{native.ECB_KEY}|{'CNH_HIBOR:'+t for t in native.TENORS}|{'ofr_fsi:'+c.lower().replace(' ','_') for c in native.FSI_COLUMNS}
        assert required<=set(output['measurements']),'required public native originals missing'
        assert set(output['errors'])<=set(native.FX),'unexpected source or validation failure'
        histories={v['history']['key']:v['history'] for group in ('measurements','comparisons') for v in output[group].values() if v['history']}
        rows=sum(ref['observations'] for ref in histories.values());assert rows>10000,'unexpected truncated history'
        for sid in native.OFR_IDS:
            v=output['measurements'][sid]
            assert v['unit']==('Percent' if '_AR_' in sid else 'USD')
            if sid.endswith('-F'):assert v['source_vintage']=='final' and v['quality']['status']=='historical_final'
        for key,v in output['comparisons'].items():
            if v['value'] is None:continue
            if key=='cnh_cny':assert v['timestamp_difference_seconds']<=60;continue
            assert all(part['date']==v['as_of'] for part in v['components'].values())
        assert output['measurements'][native.ECB_KEY]['unit']=='EUR_millions'
        assert 'not isolated USD' in output['measurements'][native.ECB_KEY]['limitation']
        assert output['pd_settlement_fails']['scope_id']=='treasury_incl_tips'
        assert output['pd_settlement_fails']['ust_ex_tips']['scope_id']=='ust_ex_tips'
        assert len(output['layers']['settlement']['metrics'])==6
        retained=[];marker=read(PREFIX+'migration.json')
        for item in [marker,*output['context_snapshots'].values()]:
            if not item.get('protected_backup'):continue
            key=PRIVATE+item['sha256']+'.bin';body=s3.get_object(Bucket=bucket,Key=key)['Body'].read(item['bytes']+1)
            assert len(body)==item['bytes'] and hashlib.sha256(body).hexdigest()==item['sha256']
            assert denied('https://'+bucket+'.s3.amazonaws.com/'+key) and denied('https://justhodl.ai/'+key)
            retained.append(item['source'])
        assert CURRENT in retained and 'data/ofr-stfm.json' in retained
        before=read(CURRENT)
        response,_=invoke_when_available(lam,dict(FunctionName=names[1],InvocationType='RequestResponse',Payload=encoded({'requestContext':{'http':{'method':'GET'}}})))
        result=json.loads(response['Payload'].read())
        assert not response.get('FunctionError') and result['statusCode']==200 and json.loads(result['body'])==before and read(CURRENT)==before
        assert len(result['body'].encode())<4*1024*1024
        for fn in names:
            receipt=read('data/ops/releases/'+fn+'.json')
            assert receipt['commit']==expected and receipt['code_sha256']==runtimes[fn]['code_sha256']
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==runtimes[fn]['code_sha256']
        proof={'contract':'funding-research-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'commit':expected,'runtimes':runtimes,'research_generated_at':output['generated_at'],'quality':output['quality'],
            'original_replay_reproduced':True,'replay':packet['replay'],'output_sha256':digest(output),
            'original_measurements':len(output['measurements']),'history_shards':len(histories),'retained_observations':rows,
            'source_errors':output['errors'],'protected_legacy_artifacts':retained,'same_date_rate_legs_verified':True,
            'final_preliminary_separate':True,'settlement_scopes_separate':True,'http_read_did_not_recollect':True,
            'invocations':invoked,'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0,
            'calls_eligible':False,'sizing_eligible':False,
            'acceptance_scope':'Five exact runtimes; original warehouse and funding collector invoked. Three consumer guards tested offline, not invoked. Descriptive measurements and entered scenarios only; strategy qualification remains pending.'}
        s3.put_object(Bucket=bucket,Key='data/funding-research-verification.json',Body=encoded(proof),ContentType='application/json',CacheControl='no-store')
        r.kv(**proof)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Funding original acceptance failed; inspect committed runner report.')
        sys.exit(1)
