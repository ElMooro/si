"""Verify private risk replay, recipient boundaries and withheld legacy sizing.

Only contract states and release metadata enter the public ops report. Never
log holdings, account values, archive names derived from account inputs or bodies.
"""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-portfolio-risk/source')]
from ops_report import report
from portfolio_risk_model import ARCHIVE_PREFIX, canonical, replay

BUCKET='justhodl-dashboard-live'
FUNCTIONS=('justhodl-portfolio-risk','justhodl-portfolio-sizer','justhodl-pm-decision')


def denied(url):
    req=urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'})
    try:
        with urllib.request.urlopen(req,timeout=25) as response: status=response.status
    except urllib.error.HTTPError as error: status=error.code
    return status in (401,403)


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=920,retries={'max_attempts':0}))
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/justhodl-portfolio-risk/source/lambda_function.py'],text=True).strip()
    def raw(key): return s3.get_object(Bucket=BUCKET,Key=key)['Body'].read()
    def read(key): return json.loads(raw(key))
    def invoke(fn):
        result=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"suppress_alerts":true}')
        response=json.loads(result['Payload'].read())
        assert not result.get('FunctionError') and response.get('statusCode',200)<400, fn+' invoke failed'
        return response
    with report('ops_5717_private_risk_replay_verify') as r:
        policy=json.loads(s3.get_bucket_policy(Bucket=BUCKET)['Policy'])
        private=next(row for row in policy['Statement'] if row.get('Sid')=='Audit20260909PrivatePersonalArtifacts')
        assert private['Effect']=='Deny' and private['Principal']=='*'
        assert {'s3:GetObject','s3:GetObjectVersion'} <= set(private['Action'])
        assert private['Condition']=={'StringNotEquals':{'aws:PrincipalAccount':'857687956942'}}
        assert 'arn:aws:s3:::'+BUCKET+'/'+ARCHIVE_PREFIX+'*' in private['Resource']
        assert denied('https://'+BUCKET+'.s3.amazonaws.com/portfolio/risk.json')
        deadline=time.monotonic()+1500
        for fn in FUNCTIONS:
            while True:
                try: receipt=read('data/ops/releases/'+fn+'.json')
                except ClientError as exc:
                    if exc.response['Error']['Code'] not in ('404','NoSuchKey'): raise
                    receipt={}
                if receipt.get('commit')==expected: break
                assert time.monotonic()<deadline,fn+' exact receipt unavailable'
                time.sleep(15)
            live=lam.get_function_configuration(FunctionName=fn)
            assert live['CodeSha256']==receipt['code_sha256']
            r.log(fn+' exact receipt and runtime hash: '+expected)
        started=datetime.now(timezone.utc)
        invoke(FUNCTIONS[0]); risk=read('portfolio/risk.json')
        assert datetime.fromisoformat(risk['generated_at'])>=started
        assert risk['schema_version']=='2.0.0' and risk['permissions']['sizing_eligible'] is False
        assert risk['alerts_sent']==0 and risk['risk_contract']['missing_input_defaults'] is False
        ref=risk['replay']; archive=raw(ref['bundle_key'])
        assert hashlib.sha256(archive).hexdigest()==ref['bundle_sha256']
        reproduced=replay(json.loads(archive))
        assert all(canonical(risk.get(key))==canonical(value) for key,value in reproduced.items())
        assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+ref['bundle_key'])
        assert denied('https://justhodl.ai/data/'+ref['bundle_key'])
        invoke(FUNCTIONS[1]); sizing=read('portfolio/sizing.json')
        assert sizing['permissions']['sizing_eligible'] is False and sizing['entry_candidates']==[] and sizing['alerts_sent']==0
        assert all(row['action']=='WAIT' and row['shares_delta'] is None and row['dollar_delta'] is None for row in sizing['positions'])
        invoke(FUNCTIONS[2]); pm=read('data/pm-decision.json')
        assert pm['call_verb']=='WAIT' and pm['permissions']['may_recommend_trades'] is False
        assert all(pm['actions'][key]==[] for key in ('trim','add','hedge'))
        for key in ('portfolio/risk.json','portfolio/sizing.json','data/pm-decision.json'):
            assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
            assert denied('https://justhodl.ai/'+(key if key.startswith('data/') else 'data/'+key))
        r.kv(commit=expected,generated_at=risk['generated_at'],schema_version=risk['schema_version'],
             risk_contract_status=risk['status'],capital_basis_status=risk['capital_basis']['status'],
             private_input_replay='reproduced',original_market_responses='hash and parsed-content checked by replay',
             legacy_sizing='withheld',pm_call='WAIT',automatic_messages_sent=0,anonymous_access='denied')
        r.ok('Three exact releases; private replay, source bytes, NAV separation, abstention and access boundaries verified')


if __name__=='__main__':
    try: main()
    except Exception: sys.exit(1)
