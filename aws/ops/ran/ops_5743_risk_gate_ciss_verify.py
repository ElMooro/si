"""Verify Risk Gate original-source CISS context without notifications or portfolio writes."""
from datetime import datetime, timezone
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
from replay_ciss_research import replay
from ciss_readthrough import context
from ciss_source_model import digest


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/justhodl-risk-gate/source/lambda_function.py'],text=True).strip()
    assert len(expected)==40,'source revision missing'
    bucket='justhodl-dashboard-live';s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=910,retries={'max_attempts':0}))
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
    names={'justhodl-risk-gate':('data/risk-gate.json','ciss_systemic')}
    with report('ops_5743_risk_gate_ciss_verify') as r:
        runtimes={};deadline=time.monotonic()+1800
        while len(runtimes)!=len(names):
            for fn in names:
                if fn in runtimes:continue
                row=receipt(fn)
                if row is None or row.get('commit')!=expected:continue
                conf=lam.get_function_configuration(FunctionName=fn)
                assert conf['CodeSha256']==row['code_sha256'],fn+' runtime differs'
                runtimes[fn]={'commit':expected,'code_sha256':conf['CodeSha256']}
            assert time.monotonic()<deadline,'exact runtime receipt timeout'
            if len(runtimes)!=len(names):time.sleep(15)
        r.kv(runtimes=runtimes)
        started=datetime.now(timezone.utc).isoformat();packets={};samples={};sources={}
        for fn,(key,field) in names.items():
            response=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
            out=json.loads(response['Payload'].read())
            assert not response.get('FunctionError') and out.get('ok') is True,{'function':fn,'result':out}
            packet=read(key);packets[fn]=packet
            assert packet['generated_at']>started
            q=packet[field];ref=q['source_replay'];run_key=ref['manifest_key']
            if run_key not in sources:
                manifest=read(run_key)
                assert run_key=='data/ciss-research/runs/'+digest(manifest)+'.json'
                rebuilt=replay(manifest,read=raw)
                assert digest(rebuilt)==ref['output_sha256']
                sources[run_key]={**rebuilt,'replay':ref}
            expected_context=context(sources[run_key],datetime.fromisoformat(packet['generated_at']))
            assert all(q[k]==v for k,v in expected_context.items()),fn+' readthrough differs from original source'
            assert q['status']=='fresh' and q['regime'] is None and q['score_contribution']==0 and q['independent_votes']==0
            samples[fn]={'generated_at':packet['generated_at'],'context':q}
        packet=packets['justhodl-risk-gate']
        inputs=packet['fleet_context']['inputs']
        legacy=inputs['dollar.ecb_ciss_regime'];measurement=inputs['dollar.ecb_ciss_observation']
        assert legacy['value'] is None and legacy['score_adj']==0
        assert measurement['source_context']==packet['ciss_systemic']
        assert measurement['value']==packet['ciss_systemic']['value'] and measurement['score_adj']==0
        assert measurement['sizing_eligible'] is False and measurement['independent_votes']==0
        assert packet['composite_identity']['check_ok'] is True
        validation=packet['event_study']['validation']
        assert validation['point_in_time'] is False and validation['out_of_sample'] is False
        assert packet['event_study']['gate_adds_value_if'] is None
        proof={'contract':'risk-gate-ciss-verification.v1','commit':expected,
            'generated_at':datetime.now(timezone.utc).isoformat(),'runtimes':runtimes,
            'samples':samples,'source_qualification':packet['source_qualification'],'reconstruction_validation':validation,
            'source_runs_replayed':list(sources),'interpretation_reproduced':True,
            'scope':'Bound ECB context with zero score or sizing effect; remaining Risk Gate inputs, heuristic multipliers and consumers require qualification',
            'paid_ai_calls':0,'notifications_sent':0,'account_reads':0,'portfolio_writes':0}
        s3.put_object(Bucket=bucket,Key='data/risk-gate-ciss-verification.json',Body=json.dumps(proof).encode(),ContentType='application/json',CacheControl='no-cache')
        r.kv(**proof);r.ok('Exact Risk Gate runtime, original-source CISS context, zero CISS adjustment and reconstruction disclosure verified')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
