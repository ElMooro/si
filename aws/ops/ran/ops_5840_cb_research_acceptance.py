"""Prove three runtimes and original CB research; public deterministic invocations only."""
from datetime import datetime,timezone
from decimal import Decimal
import json
from pathlib import Path
import subprocess,sys,time
import boto3
from botocore.config import Config

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'scripts'),
             str(ROOT/'aws/lambdas/justhodl-cb-injection/source')]
from ops_report import report
from acceptance_invoke import invoke_when_available
from cb_native import POLICY,ECB
from cb_research import CONTRACT,encoded,digest
from cb_store import raw_reader,CURRENT,PREFIX
from replay_cb_research import replay


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--',
        'aws/lambdas/justhodl-cb-injection/source/cb_store.py'],text=True).strip()
    s3=boto3.client('s3',region_name='us-east-1');bucket='justhodl-dashboard-live'
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    raw=raw_reader(s3,bucket)
    def read(key):return json.loads(raw(key))
    names=('justhodl-daily-report-v3','justhodl-cb-injection','justhodl-risk-regime')
    with report('ops_5840_cb_research_acceptance') as r:
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
        r.kv(runtimes=runtimes);rejections={}
        def invoke(fn,payload):
            response,rejected=invoke_when_available(lam,dict(FunctionName=fn,InvocationType='RequestResponse',Payload=encoded(payload)))
            rejections[fn]=rejected;result=json.loads(response['Payload'].read())
            assert not response.get('FunctionError'),fn+' invocation failed'
            assert result.get('statusCode')==200,{'function':fn,'result':result}
            return json.loads(result['body'])
        started=datetime.now(timezone.utc).isoformat()
        invoke(names[0],{'action':'research_measurements','suppress_alerts':True})
        result=invoke(names[1],{'suppress_alerts':True})
        packet=read(CURRENT)
        assert packet['contract']==CONTRACT and packet['generated_at']>started
        assert packet['source_generated_at']>started and packet['replay']==result['replay']
        assert set(packet['measurements'])==set(POLICY) and set(packet['ecb_components'])==set(ECB)
        assert packet['quality']['fresh_native_series']==len(POLICY) and packet['quality']['fresh_ecb_series']==len(ECB)
        assert packet['call'] is None and packet['calls_eligible'] is False and packet['sizing_eligible'] is False
        assert packet['global_injection_impulse']['score'] is None and packet['decision']['meaning']=='abstain'
        for bank in packet['central_banks'][:2]:
            d=bank['decomposition'];assert d['status']=='partial_attribution'
            assert Decimal(d['reconciliation_residual_decimal'])==0
            attributed=sum(Decimal(m['aligned_change_1m_decimal']) for k,m in d['components'].items() if k!='total_assets')
            assert attributed+Decimal(d['other_assets_and_adjustments_change_1m_decimal'])==Decimal(d['stock_change_1m_decimal'])
            assert d['net_injection_estimate'] is None and d['policy_purchase_transactions_1m'] is None
        for bank in packet['central_banks'][2:]:assert bank['policy_rate_pct'] is None
        fails=packet['pd_settlement_fails'];assert fails['scope_id']=='treasury_incl_tips' and fails['ust_ex_tips']['scope_id']=='ust_ex_tips'
        assert fails['original_provider_verified'] is False and fails['ust_ex_tips']['original_provider_verified'] is False
        for family in ('snapshots','measurements'):
            saved=read('data/cb-injection/'+family+'/'+packet['generated_at'][:10]+'.json')
            assert saved['replay']==packet['replay'] and saved['impulse'] is None
        key=packet['replay']['manifest_key'];manifest=read(key)
        assert key==PREFIX+'runs/'+digest(manifest)+'.json'
        assert replay(manifest,raw)=={k:v for k,v in packet.items() if k!='replay'},'original replay differs'
        for fn in names:
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==runtimes[fn]['code_sha256'],fn+' runtime moved during acceptance'
            receipt=read('data/ops/releases/'+fn+'.json')
            assert receipt['commit']==expected and receipt['code_sha256']==runtimes[fn]['code_sha256'],fn+' receipt changed'
        proof={'contract':'cb-research-verification.v1','generated_at':datetime.now(timezone.utc).isoformat(),'commit':expected,
            'runtimes':runtimes,'research_generated_at':packet['generated_at'],'quality':packet['quality'],'replay':packet['replay'],
            'immutable_output':manifest['output'],'original_replay_reproduced':True,'fred_identities':len(POLICY),'ecb_series':len(ECB),
            'stock_reconciliations':['Fed','ECB'],'net_policy_injection_identified':False,'fr2004_original_provider_verified':False,
            'calls_eligible':False,'sizing_eligible':False,'throttle_rejections':rejections,
            'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0,
            'acceptance_scope':'Three runtime identities; invoke only native-source warehouse and deterministic public CB research. Risk Regime is guard-tested offline and is not invoked.'}
        s3.put_object(Bucket=bucket,Key='data/cb-research-verification.json',Body=encoded(proof),ContentType='application/json',CacheControl='no-store')
        r.kv(**proof)


if __name__=='__main__':
    try:main()
    except Exception:
        print('CB research acceptance failed; inspect committed runner report.')
        sys.exit(1)
