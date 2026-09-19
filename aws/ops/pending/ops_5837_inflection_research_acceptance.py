"""Exact five runtimes, public-only source refresh and original liquidity replay."""
from datetime import datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
import subprocess, sys, time
import boto3
from botocore.config import Config

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'scripts'),str(ROOT/'aws/shared'),
             str(ROOT/'aws/lambdas/justhodl-liquidity-inflection/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from release_runtime_identity import active_alias
from governed_targets import GOVERNED_FUNCTIONS
from inflection_research_catalog import SERIES
from inflection_research_store import raw_reader, CURRENT, PREFIX
from inflection_research_model import digest, encoded, CONTRACT
from replay_inflection_research import replay


def pending_receipt(read, key):
    try: return read(key)
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code', '')) in ('NoSuchKey', '404'):
            return None
        raise


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--',
        'aws/lambdas/justhodl-liquidity-inflection/source/inflection_research_store.py'],text=True).strip()
    s3=boto3.client('s3',region_name='us-east-1');bucket='justhodl-dashboard-live'
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    raw=raw_reader(s3,bucket)
    def read(key):return json.loads(raw(key))
    names=('justhodl-daily-report-v3','justhodl-liquidity-inflection','justhodl-signal-board',
           'justhodl-risk-regime','justhodl-master-ranker')
    with report('ops_5837_inflection_research_acceptance') as r:
        expected_by_function={fn:expected if fn == names[1] else '1c7695b4d46441d6b60a770e93f044094d9e7ac4' for fn in names}
        runtimes={};aliases={};deadline=time.monotonic()+2700
        while len(runtimes)!=len(names):
            for fn in names:
                if fn in runtimes:continue
                receipt=pending_receipt(read,'data/ops/releases/'+fn+'.json')
                if receipt is None or receipt.get('commit')!=expected_by_function[fn]:continue
                conf=lam.get_function_configuration(FunctionName=fn)
                assert conf['CodeSha256']==receipt['code_sha256'],fn+' runtime differs'
                if fn in GOVERNED_FUNCTIONS:
                    alias=active_alias(lam,fn,receipt)
                    if alias is None:continue
                    aliases[fn]=alias
                runtimes[fn]={'commit':expected_by_function[fn],'code_sha256':receipt['code_sha256']}
            assert time.monotonic()<deadline,'intended runtime/alias timeout'
            if len(runtimes)!=len(names):time.sleep(15)
        r.kv(runtimes=runtimes,active_aliases=aliases)
        throttle_rejections={}
        r.kv(reserved_concurrency={fn:lam.get_function_concurrency(FunctionName=fn).get('ReservedConcurrentExecutions') for fn in names[:2]})
        def invoke(fn,payload):
            response,rejected=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',Payload=encoded(payload),
                                **({'Qualifier':'live'} if fn in aliases else {})))
            throttle_rejections[fn]=throttle_rejections.get(fn,0)+rejected
            result=json.loads(response['Payload'].read())
            assert not response.get('FunctionError'),{'function':fn,'result':result}
            assert result.get('statusCode')==200,{'function':fn,'result':result}
            if fn in aliases:assert response.get('ExecutedVersion')==aliases[fn]['version']
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==runtimes[fn]['code_sha256'],'runtime moved during verification'
            return json.loads(result['body'])
        capacity=lam.get_function_configuration(FunctionName=names[1])
        assert capacity['MemorySize']==1536 and capacity['Timeout']==600, 'research capacity differs'
        started=datetime.now(timezone.utc).isoformat()
        invoke(names[0],{'action':'research_measurements','suppress_alerts':True})
        source=read('data/report-measurements.json')
        assert source['generated_at']>started and set(SERIES)<=set(source['catalog'])
        result=invoke(names[1],{'suppress_alerts':True})
        packet=read(CURRENT)
        assert packet['contract']==CONTRACT and packet['generated_at']>started
        assert packet['source_replay']==source['replay'] and packet['replay']==result['replay']
        assert packet['call'] is None and packet['calls_eligible'] is False and packet['sizing_eligible'] is False
        assert packet['usd']['impulse_z'] is None and packet['composite']['liquidity_score'] is None
        assert packet['signals_logged']==0 and packet['decision']['meaning']=='abstain'
        key=packet['replay']['manifest_key'];manifest=read(key)
        assert key==PREFIX+'runs/'+digest(manifest)+'.json'
        assert replay(manifest,raw)=={k:v for k,v in packet.items() if k!='replay'},'original replay differs'
        current=packet['net_liquidity'];components=current['components']
        assert not current['missing_or_ineligible'],'core current components unavailable'
        values={sid:Decimal(row['value_decimal'])*row['multiplier_to_usd_millions'] for sid,row in components.items()}
        assert Decimal(current['net_decimal'])==values['WALCL']-values['WTREGEN']-values['RRPONTSYD']
        feature=packet['calendar_research'];latest=feature['latest']
        assert feature['status']=='ARCHIVE_RESEARCH' and feature['weekly_observations']>=500
        assert latest['status']=='descriptive' and latest['calendar_days']==91 and latest['present_weekly_samples']==14
        assert latest['end'] in feature['levels_usd_mn_decimal']
        for row in feature['history'].values():
            change=row['endpoint_change_decomposition']
            if change['status']=='descriptive':
                assert sum(Decimal(c['signed_change_usd_mn_decimal']) for c in change['components'].values())==Decimal(change['net_change_usd_mn_decimal'])
        brief=read('data/liquidity-inflection-decisive-call.json')
        assert brief['generated_at']==packet['generated_at'] and brief['source_replay']==packet['replay']
        assert brief['paid_ai_calls']==0 and brief['calls_eligible'] is False
        proof={'contract':'liquidity-inflection-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'commit':expected,'runtimes':runtimes,'active_aliases':aliases,'research_generated_at':packet['generated_at'],
            'source_generated_at':source['generated_at'],'quality':packet['quality'],'replay':packet['replay'],
            'native_series':len(packet['series']),'source_errors':{sid:source['errors'].get(sid) for sid in SERIES if sid in source['errors']},
            'archive_collection_id':feature['archive_collection']['collection_id'],'weekly_calendar_slots':feature['weekly_observations'],
            'latest_calendar_window':{'start':latest['start'],'end':latest['end'],'days':latest['calendar_days'],'samples':latest['present_weekly_samples']},
            'stage_elapsed_seconds':result['stage_elapsed_seconds'],'runtime_capacity':{'memory_mb':capacity['MemorySize'],'timeout_s':capacity['Timeout']},
            'throttle_rejections':throttle_rejections,'original_replay_reproduced':True,'component_changes_reconciled':True,'calls_eligible':False,'sizing_eligible':False,
            'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0,
            'acceptance_scope':'Five exact runtimes; invoke only deterministic public-source warehouse and liquidity research. Investment validation remains open.'}
        s3.put_object(Bucket=bucket,Key='data/liquidity-inflection-verification.json',Body=encoded(proof),ContentType='application/json',CacheControl='no-store')
        r.kv(**proof)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Liquidity research acceptance failed; inspect runner report.')
        sys.exit(1)
