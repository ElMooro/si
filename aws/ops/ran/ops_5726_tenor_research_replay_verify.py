"""Verify exact producer/consumer releases and replay official Treasury inputs."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'tests')]
from ops_report import report
from evidence_store import read_verified
from tenor_research_model import compile_research,CONTRACT
from tenor_consumer_test_support import run as test_consumer

FUNCTIONS=('tenor-signal-interpreter','allocator','alert-router','daily-report-v3','ai-chat','morning-intelligence')

def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()

def main():
    bucket='justhodl-dashboard-live'
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=920,retries={'max_attempts':0}))
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/shared/tenor_research_model.py'],text=True).strip()
    def raw(key):return s3.get_object(Bucket=bucket,Key=key)['Body'].read()
    def read(key):return json.loads(raw(key))
    with report('ops_5726_tenor_research_replay_verify') as r:
        deadline=time.monotonic()+1800
        for short in FUNCTIONS:
            fn='justhodl-'+short
            while True:
                try:receipt=read('data/ops/releases/'+fn+'.json')
                except ClientError as exc:
                    if exc.response['Error']['Code'] not in ('404','NoSuchKey'):raise
                    receipt={}
                if receipt.get('commit')==expected:break
                assert time.monotonic()<deadline,'missing exact receipt '+fn
                time.sleep(15)
            assert lam.get_function_configuration(FunctionName=fn)['CodeSha256']==receipt['code_sha256']
            r.log(fn+' exact runtime matches '+expected)
            if short!='tenor-signal-interpreter':test_consumer(short)
        response=lam.invoke(FunctionName='justhodl-tenor-signal-interpreter',InvocationType='RequestResponse',
                            Payload=b'{"suppress_alerts":true}')
        result=json.loads(response['Payload'].read())
        assert not response.get('FunctionError') and result.get('statusCode')==200,'producer invoke failed'
        doc=read('data/auction-tenor-signals.json');ref=doc['reproducibility']
        assert doc['schema_version']==CONTRACT and doc['quality']['status']=='fresh',doc.get('quality')
        assert doc['quality']['source_errors']==[] and ref['original_response_replayed'] is True
        body=raw(ref['key']);assert hashlib.sha256(body).hexdigest()==ref['sha256']
        run=json.loads(body)
        for name,compiler in run['compilers'].items():
            local=(ROOT/'aws/shared'/(name+'.py')).read_bytes()
            archived=raw(compiler['key'])
            assert local==archived and hashlib.sha256(archived).hexdigest()==compiler['sha256']
        pages=[json.loads(read_verified(s3,bucket,e)) for e in run['fiscal_pages']]
        fred=json.loads(read_verified(s3,bucket,run['fred'])) if run['fred'] else None
        replay=compile_research(pages,fred,run['as_of'],run['source_complete'])
        replay['quality']['source_errors']=run['source_errors']
        assert canonical(replay)==canonical(run['output'])
        assert hashlib.sha256(canonical(replay)).hexdigest()==ref['output_sha256']
        for key,value in replay.items():assert doc[key]==value,key
        assert doc['sizing_eligible'] is False and doc['alert_eligible'] is False and doc['call'] is None
        assert doc['composite_score'] is None and doc['any_firing'] is None and doc['transitions']==[]
        assert doc['paid_api_calls']==0
        r.kv(commit=expected,generated_at=doc['generated_at'],source_pages=len(pages),
             auction_rows=sum(len(p['data']) for p in pages),eligible_rows=doc['quality']['rows_accepted'],
             channel_states={k:v['state'] for k,v in doc['measurements'].items()},
             dff_observation_date=doc['fed_funds_observation']['as_of'],
             run=ref['key'],output_sha256=ref['output_sha256'],original_response_replay=True,
             consumer_invokes=0,notifications_sent=0,paid_api_calls=0,sizing_authority=False)
        r.ok('Six exact runtimes; original FiscalData/FRED bytes reproduce the public measurements; legacy consumers cannot score, allocate or alert from tenor heuristics')

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
