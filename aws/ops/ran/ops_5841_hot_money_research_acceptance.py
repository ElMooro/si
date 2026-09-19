"""Verify five exact runtimes and replay public exchange originals; invoke only Hot Money."""
from datetime import datetime, timezone
import json
from pathlib import Path
import subprocess
import sys
import time
import boto3
from botocore.config import Config
ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/shared'), str(ROOT/'scripts'),
               str(ROOT/'aws/lambdas/justhodl-hot-money/source')]
from acceptance_invoke import invoke_when_available
from hot_research import CONTRACT, encoded, digest
from hot_store import CURRENT, PREFIX, LEDGERS, raw_reader
from replay_hot_money import replay
from ops_report import report


def main():
    expected = subprocess.check_output(['git','log','-1','--format=%H','--',
        'aws/lambdas/justhodl-hot-money/source/hot_store.py'],text=True).strip()
    s3=boto3.client('s3',region_name='us-east-1');bucket='justhodl-dashboard-live'
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=340,retries={'max_attempts':0}))
    raw=raw_reader(s3,bucket)
    def read(key):return json.loads(raw(key))
    names=('justhodl-hot-money','justhodl-risk-regime','justhodl-accumulation-radar','justhodl-morning-intelligence','justhodl-etf-global-desk')
    with report('ops_5841_hot_money_research_acceptance') as r:
        deadline=time.monotonic()+2400;runtimes={}
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
        r.kv(runtimes=runtimes);started=datetime.now(timezone.utc).isoformat();invocations=[]
        for attempt in range(8):
            response,rejected=invoke_when_available(lam,dict(FunctionName=names[0],InvocationType='RequestResponse',
                Payload=encoded({'max_backfill_requests':45,'suppress_alerts':True})))
            result=json.loads(response['Payload'].read());assert not response.get('FunctionError'),'exchange invocation failed'
            assert result.get('statusCode')==200,result
            result=json.loads(result['body']);packet=read(CURRENT)
            assert packet['contract']==CONTRACT and packet['generated_at']>started
            assert packet['replay']==result['replay']
            rows=packet['quality']['original_verified_rows'];tw=packet['countries']['taiwan'];otc=tw['otc']
            invocations.append({'attempt':attempt+1,'throttle_rejections':rejected,'verified_rows':rows,
                'fresh_boards':packet['quality']['fresh_boards'],'backfill_attempts':result['backfill_attempts'],
                'backfill_errors':result['backfill_errors']})
            r.kv(invocation=invocations[-1])
            if (packet['quality']['fresh_boards']==2 and min(rows.values())>=61
                    and tw['windows']['60']['status']==otc['windows']['60']['status']=='observed_window'
                    and tw['combined']['windows']['60']['status']=='observed_window'):break
        else:raise AssertionError('Bounded historical acquisition did not establish 61 original dates per board; partial research is not accepted as complete')
        assert packet['call'] is None and packet['calls_eligible'] is False and packet['sizing_eligible'] is False and packet['execution_eligible'] is False
        assert packet['source_comparisons']['tpex']['status']=='agree'
        assert packet['decision']['meaning']=='abstain' and packet['decision']['portfolio_change'] is None
        for board in (tw,otc):
            history={v['date']:v for v in board['history']}
            for row in history.values():assert int(row['buy_twd'])-int(row['sell_twd'])==int(row['net_twd'])
            for n in ('5','20','60'):
                window=board['windows'][n]
                assert not window['known_unverified_dates'] and window['exchange_calendar_complete'] is False
                assert sum(int(history[d]['net_twd']) for d in window['dates'])==int(window['sum_twd'])
                assert board['sum_'+n+'d_bn'] is None
            assert board['z_definition']['sample_size']==60 and board['z_definition']['clipped'] is False
            assert board['latest_day'] not in board['z_definition']['baseline_dates']
        for n in ('5','20','60'):
            a,b=tw['windows'][n],otc['windows'][n];combined=tw['combined']['windows'][n]
            assert a['dates']==b['dates']==combined['dates']
            assert int(a['sum_twd'])+int(b['sum_twd'])==int(combined['sum_twd'])
        for key in LEDGERS.values():
            projection=read(key);assert projection['replay']==packet['replay']
            assert projection['contract']=='exchange-ledger-projection.v1' and len(projection['original_verified_dates'])>=61
        key=packet['replay']['manifest_key'];manifest=read(key);assert key==PREFIX+'runs/'+digest(manifest)+'.json'
        assert replay(manifest,raw)=={k:v for k,v in packet.items() if k!='replay'},'original exchange replay differs'
        for fn in names:
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==runtimes[fn]['code_sha256'],fn+' runtime moved during acceptance'
            receipt=read('data/ops/releases/'+fn+'.json');assert receipt['commit']==expected and receipt['code_sha256']==runtimes[fn]['code_sha256']
        proof={'contract':'hot-money-research-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),'commit':expected,
            'runtimes':runtimes,'research_generated_at':packet['generated_at'],'quality':packet['quality'],'replay':packet['replay'],
            'immutable_output':manifest['output'],'original_replay_reproduced':True,'invocations':invocations,
            'original_verified_rows':packet['quality']['original_verified_rows'],'exact_window_sums_reconciled':True,
            'exchange_calendar_completeness_verified':False,'historical_publication_time_verified':False,
            'fr2004_original_provider_verified':False,'calls_eligible':False,'sizing_eligible':False,
            'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0,
            'acceptance_scope':'Five runtime identities; invoke only deterministic public exchange research. Four consumers are guard-tested offline and are not invoked.'}
        s3.put_object(Bucket=bucket,Key='data/hot-money-research-verification.json',Body=encoded(proof),ContentType='application/json',CacheControl='no-store')
        r.kv(**proof)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Hot Money original research acceptance failed; inspect committed runner report.')
        sys.exit(1)
