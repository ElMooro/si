"""Register actual prospective observations and verify retained bytes and clocks."""
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
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/shared')]
from ops_report import report
from prospective_journal import read_record


def main():
    bucket='justhodl-dashboard-live';fn='justhodl-signal-harvester'
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=920,retries={'max_attempts':0}))
    events=boto3.client('events',region_name='us-east-1')
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/shared/prospective_journal.py'],text=True).strip()
    def raw(key):return s3.get_object(Bucket=bucket,Key=key)['Body'].read()
    def read(key):return json.loads(raw(key))
    with report('ops_5720_prospective_journal_verify') as r:
        deadline=time.monotonic()+1500
        while True:
            receipt=read('data/ops/releases/'+fn+'.json')
            if receipt['commit']==expected:break
            assert time.monotonic()<deadline,'exact release receipt missing'
            time.sleep(15)
        config=lam.get_function_configuration(FunctionName=fn)
        assert config['CodeSha256']==receipt['code_sha256']
        result=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"capture_only":true,"suppress_alerts":true}')
        payload=json.loads(result['Payload'].read())
        assert not result.get('FunctionError') and payload['statusCode']==200,'capture invocation failed'
        assert payload['legacy_ledger_writes']==0
        summary=read('data/prospective-research.json')
        assert summary['schema_version']=='prospective-research-summary.v1' and summary['sizing_eligible'] is False
        capture_raw=raw(summary['capture']['key'])
        assert hashlib.sha256(capture_raw).hexdigest()==summary['capture']['sha256']
        capture=json.loads(capture_raw)
        assert capture['protocol_ref']==summary['protocol'] and len(capture['records'])==summary['records_in_capture']
        for ref in capture['records']:read_record(s3,bucket,ref)
        assert all(not p['source_key'].startswith('data/ai-brief.') for p in capture['sources'])
        # Keep the existing daily legacy schedule. This separate hourly target
        # registers observations only and performs no quote or DynamoDB writes.
        name='justhodl-prospective-capture-hourly'
        rule=events.put_rule(Name=name,ScheduleExpression='rate(1 hour)',State='ENABLED',
                             Description='Record immutable public directions before future research windows; no trade authority')
        try:
            lam.add_permission(FunctionName=fn,StatementId='prospective-research-capture',Action='lambda:InvokeFunction',
                               Principal='events.amazonaws.com',SourceArn=rule['RuleArn'])
        except ClientError as exc:
            if exc.response['Error']['Code']!='ResourceConflictException':raise
            policy=json.loads(lam.get_policy(FunctionName=fn)['Policy'])
            st=next(row for row in policy['Statement'] if row['Sid']=='prospective-research-capture')
            assert st['Principal']['Service']=='events.amazonaws.com' and st['Condition']['ArnLike']['AWS:SourceArn']==rule['RuleArn']
        target={'Id':'capture-only','Arn':config['FunctionArn'],'Input':'{"capture_only":true,"suppress_alerts":true}'}
        assert events.put_targets(Rule=name,Targets=[target])['FailedEntryCount']==0
        assert events.describe_rule(Name=name)['State']=='ENABLED'
        assert any(t['Arn']==target['Arn'] and json.loads(t['Input']).get('capture_only') is True for t in events.list_targets_by_rule(Rule=name)['Targets'])
        r.kv(commit=expected,generated_at=summary['generated_at'],records=summary['records_in_capture'],
             newly_registered=summary['new_records'],rank_observations=summary['rank_observations'],
             ineligible_sources=summary['ineligible_sources'],coverage=summary['coverage'],
             capture=summary['capture']['key'],protocol=summary['protocol']['key'],schedule='rate(1 hour)',
             legacy_ledger_writes=0,sizing_authority=False)
        r.ok('Exact runtime, retained capture/forecast/protocol bytes and S3 clocks verified; hourly capture installed. No forecast outcomes claimed.')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
