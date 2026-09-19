"""Verify exact Alpha runtimes, protected retention and deterministic public replay."""
from datetime import datetime,timezone
import json
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request
import boto3
from botocore.config import Config
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'scripts')]
from acceptance_invoke import invoke_when_available
import alpha_research as model
from alpha_research_store import raw_reader,PRIVATE_BACKUP
from replay_alpha_research import verify_current
from ops_report import report


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=30) as response:
            return response.status in (401,403,404)
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/shared/alpha_research.py'],text=True).strip()
    bucket='justhodl-dashboard-live';s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=300,retries={'max_attempts':0}))
    raw=raw_reader(s3,bucket)
    def read(key):return json.loads(raw(key))
    names=('justhodl-alpha-compass','justhodl-alpha-daily-brief')
    with report('ops_5843_alpha_field_units_acceptance') as r:
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
            assert time.monotonic()<deadline,'Alpha exact runtime timeout'
            if len(runtimes)!=len(names):time.sleep(15)
        r.kv(runtimes=runtimes);started=datetime.now(timezone.utc).isoformat();results=[]
        for fn in names:
            response,rejected=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}'))
            result=json.loads(response['Payload'].read());assert not response.get('FunctionError') and result.get('statusCode')==200,(fn,result)
            result=json.loads(result['body']);results.append({'function':fn,'replay':result['replay'],'throttle_rejections':rejected})
        compass=verify_current(model.CURRENT,raw);brief=verify_current(model.BRIEF,raw)
        assert compass['generated_at']>started and brief['generated_at']>started
        assert compass['contract']==model.CONTRACT and len(compass['research_ideas'])==7
        assert compass['quality']['expected_packets']==12 and compass['quality']['current_packets']==12
        fails=compass['pd_settlement_fails']
        assert fails['display_values'] is not None,'explicit Treasury field units must retain valid gross context'
        assert fails['scope_id']=='treasury_incl_tips' and fails['ust_ex_tips']['scope_id']=='ust_ex_tips'
        assert fails['unit']=='USD_bn_par' and all(v=='usd_bn' for v in fails['field_units'].values())
        if 'gross_reconciliation_failed' in fails['ust_ex_tips']['reasons']:
            assert fails['ust_ex_tips']['display_values'] is None
        assert brief['source_replay']==read(model.CURRENT)['replay']
        assert brief['brief_markdown'].endswith('**WAIT**') and len(brief['brief_markdown'])>=120
        for packet in (compass,brief):
            assert packet['call'] is None and all(packet[k] is False for k in ('calls_eligible','sizing_eligible','execution_eligible'))
            assert packet['decision']['meaning']=='abstain' and packet['decision']['portfolio_change'] is None
            assert packet['paid_ai_calls']==packet['notifications_sent']==0
        for idea in compass['research_ideas']:
            assert idea['sizing']['kelly_pct'] is None and idea['stop_pct'] is None and idea['target_pct'] is None
            assert idea['stats']['n'] is None and idea['stats']['win_rate'] is None
            assert idea['decision_id'] in brief['brief_markdown']
        protected=0
        for kind in ('compass','brief'):
            marker=read(model.PREFIX+'migration-'+kind+'.json')
            for original in marker['objects']:
                key=PRIVATE_BACKUP+original['sha256']+'.bin'
                assert denied('https://'+bucket+'.s3.amazonaws.com/'+key),'legacy backup publicly readable'
                assert denied('https://justhodl.ai/'+key),'legacy edge backup publicly readable'
                protected+=1
        for fn in names:
            receipt=read('data/ops/releases/'+fn+'.json')
            assert receipt['commit']==expected and receipt['code_sha256']==runtimes[fn]['code_sha256']
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==runtimes[fn]['code_sha256']
        proof={'contract':'alpha-research-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'commit':expected,'runtimes':runtimes,'invocations':results,'replay':read(model.CURRENT)['replay'],
            'brief_replay':read(model.BRIEF)['replay'],'research_generated_at':compass['generated_at'],
            'brief_generated_at':brief['generated_at'],'projection_replay_reproduced':True,'provider_originals_verified':False,
            'ideas':len(compass['research_ideas']),'qualified_votes':0,'nonnull_sizes':0,'protected_legacy_objects':protected,
            'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0,
            'acceptance_scope':'Two exact runtimes; deterministic public projections and brief only. No original-source or strategy validation claim.'}
        s3.put_object(Bucket=bucket,Key='data/alpha-research-verification.json',Body=model.encoded(proof),ContentType='application/json',CacheControl='no-store');r.kv(**proof)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Alpha research acceptance failed; inspect committed runner report.');sys.exit(1)
