"""Verify the forward evaluator, real market evidence and complete capture traversal."""
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
    bucket='justhodl-dashboard-live'
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=920,retries={'max_attempts':0}))
    events=boto3.client('events',region_name='us-east-1')
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/shared/prospective_journal.py'],text=True).strip()
    def raw(key):return s3.get_object(Bucket=bucket,Key=key)['Body'].read()
    def read(key):return json.loads(raw(key))
    def invoke(fn,event):
        result=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=json.dumps(event).encode())
        payload=json.loads(result['Payload'].read())
        assert not result.get('FunctionError') and payload.get('statusCode',200)<400,fn+' invoke failed'
        return payload
    with report('ops_5723_forward_measurement_reverify') as r:
        deadline=time.monotonic()+1500;configs={}
        for fn in ('justhodl-signal-harvester','justhodl-prospective-evaluator'):
            while True:
                try:receipt=read('data/ops/releases/'+fn+'.json')
                except ClientError as exc:
                    if exc.response['Error']['Code'] not in ('NoSuchKey','404'):raise
                    receipt={}
                if receipt.get('commit')==expected:break
                assert time.monotonic()<deadline,'exact receipt missing for '+fn
                time.sleep(15)
            configs[fn]=lam.get_function_configuration(FunctionName=fn)
            assert configs[fn]['CodeSha256']==receipt['code_sha256']
            r.log(fn+' exact commit and runtime hash '+expected)
        fn='justhodl-prospective-evaluator'
        lam.put_function_concurrency(FunctionName=fn,ReservedConcurrentExecutions=1)
        assert lam.get_function_concurrency(FunctionName=fn)['ReservedConcurrentExecutions']==1
        invoke('justhodl-signal-harvester',{'capture_only':True,'suppress_alerts':True})
        capture_summary=read('data/prospective-research.json')
        coverage=capture_summary['coverage']
        assert coverage['sources_scanned']==coverage['candidate_sources'],'capture stopped at legacy cap'
        capture_doc=read(capture_summary['capture']['key'])
        for ref in capture_doc['records']:read_record(s3,bucket,ref)
        probe=invoke(fn,{'validation_only':True})
        assert probe['kind']=='real_market_source_probe_not_forecast' and probe['observation_count']>=5
        assert probe['archive_checks']['marks_verified']==probe['observation_count']
        assert probe['forecast_writes']==0
        invoke(fn,{'suppress_alerts':True})
        doc=read('data/prospective-outcomes.json')
        assert doc['schema_version']=='prospective-outcome-batch.v1' and doc['sizing_eligible'] is False
        assert doc['evidence_errors']==0 and doc['forecasts_checked']>0
        assert doc['net_return_pct'] is None and doc['portfolio_pnl'] is None
        assert hashlib.sha256(raw(doc['batch']['key'])).hexdigest()==doc['batch']['sha256']
        name='justhodl-prospective-evaluator-hourly'
        rule=events.put_rule(Name=name,ScheduleExpression='rate(1 hour)',State='ENABLED',
                             Description='Verify prospective records and original daily price evidence; no capital or order authority')
        try:
            lam.add_permission(FunctionName=fn,StatementId='forward-measurement-hourly',Action='lambda:InvokeFunction',
                               Principal='events.amazonaws.com',SourceArn=rule['RuleArn'])
        except ClientError as exc:
            if exc.response['Error']['Code']!='ResourceConflictException':raise
            policy=json.loads(lam.get_policy(FunctionName=fn)['Policy'])
            st=next(row for row in policy['Statement'] if row['Sid']=='forward-measurement-hourly')
            assert st['Principal']['Service']=='events.amazonaws.com' and st['Condition']['ArnLike']['AWS:SourceArn']==rule['RuleArn']
        assert events.put_targets(Rule=name,Targets=[{'Id':'forward-measurement','Arn':configs[fn]['FunctionArn'],'Input':'{}'}])['FailedEntryCount']==0
        assert events.describe_rule(Name=name)['State']=='ENABLED'
        assert any(t['Arn']==configs[fn]['FunctionArn'] for t in events.list_targets_by_rule(Rule=name)['Targets'])
        r.kv(commit=expected,generated_at=doc['generated_at'],capture_coverage=coverage,
             registered_in_capture=capture_summary['records_in_capture'],checked_in_batch=doc['forecasts_checked'],
             statuses=doc['status_counts'],price_probe_observations=probe['observation_count'],
             price_probe_as_of=probe['as_of'],price_probe_sha256=probe['evidence']['sha256'],
             batch=doc['batch']['key'],schedule='rate(1 hour)',legacy_ledger_writes=0,sizing_authority=False)
        r.ok('Exact releases, all candidate sources traversed, original market bytes replayed, prospective pending/measurement states verified and hourly evaluator installed')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
