"""Verify Meta-Regime CISS dependency and missing-evidence semantics."""
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
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/justhodl-regime-composite/source/lambda_function.py'],text=True).strip()
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
    names={'justhodl-regime-composite':('data/regime-composite.json','ciss_systemic')}
    with report('ops_5740_regime_ciss_context_verify') as r:
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
            assert not response.get('FunctionError') and out.get('statusCode')==200,{'function':fn,'result':out}
            packet=read(key);packets[fn]=packet
            assert packet['generated_at']>started
            assert packet['call'] is None and packet['calls_eligible'] is False and packet['sizing_eligible'] is False
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
        packet=packets['justhodl-regime-composite']
        modules=[m for m in packet['modules'] if m.get('evidence_family')=='ecb_ciss']
        assert len(modules)==4 and all(m['vote_eligible'] is False and m['polarity'] is None for m in modules)
        refs={m['source_context']['source_replay']['output_sha256'] for m in modules}
        assert len(refs)==1 and packet['decision']['meaning']=='abstain'
        for module in modules:
            q=module['source_context'];source=sources[q['source_replay']['manifest_key']]
            expected_context=context(source,datetime.fromisoformat(packet['generated_at']),q['series_id'])
            assert q==expected_context,'CISS contribution read differs'
        group=packet['dependency_groups'][0]
        assert group['configured_source_families']==1 and group['independent_votes']==0
        for dim in packet['dimensions'].values():
            assert all(member['label'] not in {m['label'] for m in modules} for member in dim['members'])
            if dim['n']==0:assert dim['score'] is None
        missing=packet['composite_coverage']['missing_dimensions']
        if missing:assert packet['meta_regime']=='UNAVAILABLE'
        proof={'contract':'regime-ciss-context-verification.v1','commit':expected,
            'generated_at':datetime.now(timezone.utc).isoformat(),'runtimes':runtimes,
            'samples':samples,'dependency_groups':packet['dependency_groups'],'composite_coverage':packet['composite_coverage'],
            'source_runs_replayed':list(sources),'interpretation_reproduced':True,
            'scope':'One bound ECB source family, zero CISS votes, explicit missing dimensions; remaining module source/model qualification pending',
            'paid_ai_calls':0,'notifications_sent':0,'account_reads':0,'portfolio_writes':0}
        s3.put_object(Bucket=bucket,Key='data/regime-ciss-context-verification.json',Body=json.dumps(proof).encode(),ContentType='application/json',CacheControl='no-cache')
        r.kv(**proof);r.ok('Exact Meta-Regime runtime, original-source CISS contexts, single family and missing-dimension semantics verified')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
