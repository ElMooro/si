"""Nine exact runtimes, active governed aliases and public-source global replay."""
from datetime import datetime,timezone
from decimal import Decimal
import json
from pathlib import Path
import subprocess,sys,time
import boto3
from botocore.config import Config

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),
             str(ROOT/'aws/lambdas/justhodl-global-liquidity/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from release_runtime_identity import active_alias
from governed_targets import GOVERNED_FUNCTIONS
from global_liquidity_research import CONTRACT,SERIES,encoded,digest
from global_liquidity_store import raw_reader,CURRENT,HISTORY,PREFIX
from replay_global_liquidity import replay


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--',
        'aws/ops/pending/ops_5839_global_liquidity_acceptance.py'],text=True).strip()
    s3=boto3.client('s3',region_name='us-east-1');bucket='justhodl-dashboard-live'
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    raw=raw_reader(s3,bucket)
    def read(key):return json.loads(raw(key))
    names=tuple('justhodl-'+name for name in ('global-liquidity','allocator','crisis-composite','cycle-clock',
        'katlin','liquidity-agent','quantum-desk','risk-regime','wl-fusion'))
    with report('ops_5839_global_liquidity_acceptance') as r:
        runtimes={};aliases={};deadline=time.monotonic()+3000
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
                if fn in GOVERNED_FUNCTIONS:
                    alias=active_alias(lam,fn,receipt)
                    if alias is None:continue
                    aliases[fn]=alias
                runtimes[fn]={'commit':expected,'code_sha256':receipt['code_sha256']}
            assert time.monotonic()<deadline,'runtime/alias acceptance timeout'
            if len(runtimes)!=len(names):time.sleep(15)
        r.kv(runtimes=runtimes,active_aliases=aliases)
        started=datetime.now(timezone.utc).isoformat()
        response,rejected=invoke_when_available(lam,dict(FunctionName=names[0],InvocationType='RequestResponse',Payload=encoded({'suppress_alerts':True})))
        result=json.loads(response['Payload'].read())
        assert not response.get('FunctionError'), 'Research invocation failed'
        assert result.get('statusCode')==200,result
        result=json.loads(result['body']);packet=read(CURRENT);history=read(HISTORY)
        assert packet['contract']==CONTRACT and packet['generated_at']>started
        assert packet['replay']==result['replay']
        assert history['source_replay']==packet['replay'] and history['generated_at']==packet['generated_at']
        assert packet['call'] is None and packet['calls_eligible'] is False and packet['sizing_eligible'] is False
        assert packet['global_impulse_13w_pct'] is None and packet['global_liquidity_index']['total_usd_bn'] is None
        assert set(packet['series'])==set(SERIES) and packet['decision']['meaning']=='abstain'
        current=packet['three_bank_subtotal']
        assert current['status']=='descriptive' and not current['missing_components']
        assert set(current['components'])=={'WALCL','ECBASSETSW','JPNASSETS'}
        assert sum(Decimal(v['usd_millions_decimal']) for v in current['components'].values())==Decimal(current['total_usd_millions_decimal'])
        cal=packet['calendar_research'];assert cal['expected_weekly_slots']>=260 and cal['point_in_time'] is False
        for day,row in cal['history'].items():
            for c in row['components'].values():
                for choice in (c['balance'],c.get('fx')):
                    if choice is not None and choice.get('selected'):
                        assert choice['selected']['effective_observation_date']<=day
        for horizon,change in cal['latest_changes'].items():
            assert change['calendar_days']==(91 if horizon=='13w' else 364)
            if change['status']=='descriptive':
                assert sum(Decimal(change[key]) for key in ('balance_effect_usd_millions_decimal','fx_effect_usd_millions_decimal','rounding_residual_usd_millions_decimal'))==Decimal(change['change_usd_millions_decimal'])
        key=packet['replay']['manifest_key'];manifest=read(key)
        assert key==PREFIX+'runs/'+digest(manifest)+'.json'
        assert replay(manifest,raw)=={k:v for k,v in packet.items() if k!='replay'},'original replay differs'
        for fn in names:
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==runtimes[fn]['code_sha256'],'runtime moved during acceptance'
            if fn in aliases:assert active_alias(lam,fn,read('data/ops/releases/'+fn+'.json'))==aliases[fn],'active alias moved'
        proof={'contract':'global-liquidity-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),'commit':expected,
            'runtimes':runtimes,'active_aliases':aliases,'research_generated_at':packet['generated_at'],'quality':packet['quality'],
            'replay':packet['replay'],'immutable_output':manifest['output'],'original_replay_reproduced':True,
            'scope':'Three-bank subtotal only; no future observation joins or inferred allocation authority.',
            'weekly_calendar_slots':cal['expected_weekly_slots'],'complete_weekly_slots':cal['complete_weekly_slots'],
            'window_status':{key:value['status'] for key,value in cal['latest_changes'].items()},
            'calls_eligible':False,'sizing_eligible':False,'throttle_rejections':rejected,
            'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0,
            'consumers_invoked':False,'acceptance_scope':'Prove nine runtime identities and governed alias; invoke only public-source global research, verify output and full original replay. Consumer vote guards are tested offline.'}
        s3.put_object(Bucket=bucket,Key='data/global-liquidity-verification.json',Body=encoded(proof),ContentType='application/json',CacheControl='no-store')
        r.kv(**proof)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Global liquidity acceptance failed; inspect runner report.')
        sys.exit(1)
