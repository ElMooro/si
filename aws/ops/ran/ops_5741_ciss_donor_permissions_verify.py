"""Verify CISS donor consumers against original ECB files and denied allocations."""
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
from replay_ciss_research import replay
from ciss_source_model import digest
from macro_donor_inputs import ciss_context,fragmentation_context


def main():
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/shared/macro_donor_inputs.py'],text=True).strip()
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
    names=['justhodl-'+s for s in ('bond-warroom','euro-fragmentation','eurodollar-plumbing',
        'liquidity-capacity','liquidity-credit-engine','liquidity-inflection','repo','treasury-rehypo')]
    with report('ops_5741_ciss_donor_permissions_verify') as r:
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
        samples={};sources={}
        for fn,key in [('justhodl-liquidity-credit-engine','data/liquidity-credit-engine.json'),
                       ('justhodl-euro-fragmentation','data/euro-fragmentation.json')]:
            started=datetime.now(timezone.utc).isoformat()
            response=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
            result=json.loads(response['Payload'].read())
            assert not response.get('FunctionError') and result.get('statusCode')==200,{'function':fn,'result':result}
            packet=read(key);assert packet['generated_at']>started
            assert packet['call'] is None and packet['calls_eligible'] is False and packet['sizing_eligible'] is False
            q=packet['donor_inputs']['ciss_stress'] if fn.endswith('credit-engine') else packet['ciss_context']
            ref=q['source_replay'];run_key=ref['manifest_key']
            if run_key not in sources:
                manifest=read(run_key);assert run_key=='data/ciss-research/runs/'+digest(manifest)+'.json'
                rebuilt=replay(manifest,read=raw);assert digest(rebuilt)==ref['output_sha256']
                sources[run_key]={**rebuilt,'replay':ref}
            clock=datetime.fromisoformat(q['contract']['evaluated_at'])
            expected_context=ciss_context(sources[run_key],clock)
            assert all(q[k]==v for k,v in expected_context.items()),fn+' donor differs from original source replay'
            assert q['contract']['usable'] and q['ea_regime'] is None
            assert q['independent_votes']==0 and q['score_contribution']==0
            assert q['coverage']['current']+q['coverage']['excluded']==q['coverage']['configured']
            assert q['coverage']['current']>0 and q['coverage']['excluded']>0
            if fn.endswith('credit-engine'):
                interp=packet['interpretation']
                assert interp['overall_posture']=='WAIT' and interp['confidence'] is None
                assert interp['target_allocation']==[] and interp['cross_asset']=={} and interp['hedges']==[]
                assert packet['ciss_systemic']==q['source_context']
            else:
                expected_joins=fragmentation_context(sources[run_key],packet['countries'],clock)
                assert q['country_spread_context']==expected_joins['country_spread_context']
            samples[fn]={'generated_at':packet['generated_at'],'evaluated_at':q['contract']['evaluated_at'],
                'coverage':q['coverage'],'headline':q['source_context'],'independent_votes':q['independent_votes']}
        proof={'contract':'ciss-donor-permissions-verification.v1','commit':expected,
            'generated_at':datetime.now(timezone.utc).isoformat(),'runtimes':runtimes,'samples':samples,
            'source_runs_replayed':list(sources),'interpretation_reproduced':True,
            'scope':'Current per-series CISS donors and denied LCE allocations; remaining FRED source/model validation pending',
            'paid_ai_calls':0,'notifications_sent':0,'account_reads':0,'portfolio_writes':0}
        s3.put_object(Bucket=bucket,Key='data/ciss-donor-permissions-verification.json',Body=json.dumps(proof).encode(),ContentType='application/json',CacheControl='no-cache')
        r.kv(**proof);r.ok('Eight exact runtimes; two current donor outputs replayed from original ECB files; no LCE portfolio advice')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
