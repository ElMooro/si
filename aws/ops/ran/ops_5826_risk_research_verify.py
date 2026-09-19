"""Verify original-source Risk Gate research and every affected authority consumer runtime."""
from datetime import datetime,timezone
import gzip
import io
import json
from pathlib import Path
import subprocess
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'scripts'),str(ROOT/'aws/shared')]
from ops_report import report
from replay_risk_gate_research import replay
from risk_gate_research_model import digest,SERIES
from risk_gate_research_catalog import SERIES as RISK_SERIES


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/shared/risk_gate_research_model.py'],text=True).strip()
    assert len(expected)==40,'source revision missing'
    bucket='justhodl-dashboard-live';s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
    events=boto3.client('events',region_name='us-east-1')
    scheduler=boto3.client('scheduler',region_name='us-east-1')
    def raw(key):
        body=s3.get_object(Bucket=bucket,Key=key)['Body'].read(64*1024*1024+1)
        assert len(body)<=64*1024*1024,'source bound exceeded'
        if key.endswith('.gz'):
            with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:body=stream.read(64*1024*1024+1)
            assert len(body)<=64*1024*1024,'decompressed source bound exceeded'
        return body
    def read(key):return json.loads(raw(key))
    def receipt(fn):
        try:return read('data/ops/releases/'+fn+'.json')
        except ClientError as exc:
            if exc.response.get('Error',{}).get('Code') in ('NoSuchKey','404'):return None
            raise
    names=('justhodl-risk-gate','justhodl-best-setups','justhodl-master-ranker','justhodl-opportunity-engine','justhodl-quantum-desk','justhodl-market-machine')
    with report('ops_5826_risk_research_verify') as r:
        runtimes={};configs={};deadline=time.monotonic()+1800
        while len(runtimes)!=len(names):
            for fn in names:
                if fn in runtimes:continue
                row=receipt(fn)
                if row is None or row.get('commit')!=expected:continue
                conf=lam.get_function_configuration(FunctionName=fn)
                assert conf['CodeSha256']==row['code_sha256'],fn+' runtime differs'
                configs[fn]=conf;runtimes[fn]={'commit':expected,'code_sha256':conf['CodeSha256']}
            assert time.monotonic()<deadline,'exact runtime receipt timeout'
            if len(runtimes)!=len(names):time.sleep(15)
        r.kv(runtimes=runtimes)
        conf=configs[names[0]]
        schedules=[]
        for name in events.list_rule_names_by_target(TargetArn=conf['FunctionArn'])['RuleNames']:
            rule=events.describe_rule(Name=name)
            if rule.get('State')=='ENABLED' and rule.get('ScheduleExpression'):
                schedules.append({'name':name,'schedule':rule['ScheduleExpression']})
        # Normal schedules are preserved; acceptance invokes only the deterministic Risk Gate.
        def invoke(fn,event):
            response=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=json.dumps(event).encode())
            result=json.loads(response['Payload'].read())
            assert not response.get('FunctionError') and result.get('statusCode') in (200,409),{'function':fn,'result':result}
            return result
        started=datetime.now(timezone.utc).isoformat()
        invoke(names[0],{'suppress_alerts':True})
        packet=read('data/risk-gate.json')
        assert packet['generated_at']>started and packet['contract']=='risk-gate-research.v1'
        ref=packet['replay'];manifest=read(ref['manifest_key'])
        assert ref['manifest_key']=='data/risk-gate-research/runs/'+digest(manifest)+'.json'
        cache={}
        def retained(key):
            if key not in cache:cache[key]=raw(key)
            return cache[key]
        rebuilt=replay(manifest,read=retained)
        assert rebuilt=={k:v for k,v in packet.items() if k!='replay'}
        assert set(packet['series'])==set(SERIES) and len(SERIES)==25
        assert packet['quality']['fresh_series']>=20,'unexpected source coverage loss'
        assert packet['composite'] is None and packet['posture']=='UNAVAILABLE'
        assert packet['sizing_multiplier'] is None and packet['decision']['verb']=='WAIT'
        assert packet['calls_eligible'] is False and packet['sizing_eligible'] is False
        assert packet['portfolio_consequences']['allows_new_entries'] is False
        assert packet['portfolio_consequences']['forced_liquidation'] is False
        assert all(row['independent_votes']==0 and row['score_adjustment']==0 for row in packet['fleet_context']['inputs'].values())
        from decimal import Decimal
        cp=packet['derived']['cp_a2p2_minus_aa_90d']
        if cp['status']=='descriptive':
            left,right=cp['inputs']
            assert left['latest_date']==right['latest_date']==cp['observation_date']
            assert Decimal(cp['value_decimal'])==(Decimal(left['latest_value_decimal'])-Decimal(right['latest_value_decimal']))*100
        # Run actual consumer function boundaries from these exact reviewed source bytes.
        check=subprocess.run([sys.executable,'aws/lambdas/justhodl-quantum-desk/tests/run_tests.py'],capture_output=True,text=True)
        assert check.returncode==0,check.stdout+check.stderr
        proof={'contract':'risk-gate-research-verification.v1','commit':expected,
            'generated_at':datetime.now(timezone.utc).isoformat(),'runtimes':runtimes,'schedules':schedules,
            'quality':packet['quality'],'commercial_paper':cp,'source_generated_at':packet['source_generated_at'],
            'output_generated_at':packet['generated_at'],'replay':ref,'original_replay_reproduced':True,
            'consumer_authority_boundaries':'Actual ranker readers and Quantum verdict/money-map functions tested offline; no consumer invocation',
            'portfolio_consequences':packet['portfolio_consequences'],'scope':packet['scope'],
            'paid_ai_calls':0,'notifications_sent':0,'account_reads':0,'portfolio_writes':0}
        s3.put_object(Bucket=bucket,Key='data/risk-gate-research-verification.json',Body=json.dumps(proof).encode(),ContentType='application/json',CacheControl='no-cache')
        r.kv(**proof);r.ok('Six exact runtimes; original-source Risk Gate replay and consumer abstention boundaries verified')



if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
