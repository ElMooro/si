"""Verify the isolated daily-report FRED research path and schedule its refresh."""
import gzip
import json
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'scripts'),str(ROOT/'aws/shared')]
from ops_report import report
from replay_report_research import replay


def main():
    bucket='justhodl-dashboard-live';fn='justhodl-daily-report-v3'
    expected=subprocess.check_output(['git','log','-1','--format=%H','--','aws/ops/pending/ops_5731_report_research_reverify.py'],text=True).strip()
    s3=boto3.client('s3',region_name='us-east-1')
    lam=boto3.client('lambda',region_name='us-east-1',config=Config(read_timeout=920,retries={'max_attempts':0}))
    scheduler=boto3.client('scheduler',region_name='us-east-1');iam=boto3.client('iam',region_name='us-east-1')
    def raw(key):
        b=s3.get_object(Bucket=bucket,Key=key)['Body'].read()
        return gzip.decompress(b) if key.endswith('.gz') else b
    def read(key):return json.loads(raw(key))
    with report('ops_5731_report_research_reverify') as r:
        deadline=time.monotonic()+1800
        while True:
            receipt=read('data/ops/releases/'+fn+'.json')
            if receipt.get('commit')==expected:break
            assert time.monotonic()<deadline,'exact release receipt missing'
            time.sleep(15)
        config=lam.get_function_configuration(FunctionName=fn)
        assert config['CodeSha256']==receipt['code_sha256']
        r.log('Exact commit/runtime verified: '+expected)
        before=datetime.now(timezone.utc).isoformat()
        response=lam.invoke(FunctionName=fn,InvocationType='RequestResponse',Payload=b'{"action":"research_measurements","suppress_alerts":true}')
        result=json.loads(response['Payload'].read())
        assert not response.get('FunctionError') and result.get('statusCode')==200,result
        packet=read('data/report-measurements.json')
        assert packet['generated_at']>before and packet['contract']=='report-observations.v1'
        manifest=read(packet['replay']['manifest_key']);rebuilt=replay(manifest,read=raw)
        assert all(packet[k]==v for k,v in rebuilt.items()),'live packet differs from replay'
        assert set(packet['measurements'])|set(packet['errors'])==set(packet['catalog'])
        r.kv(quality=packet['quality'], acquisition_errors=packet['errors'])
        core=('WALCL','WTREGEN','RRPONTSYD','DTWEXBGS','ICSA','DGS10','CPIAUCSL','UNRATE')
        for sid in core:
            row=packet['measurements'].get(sid)
            assert row, (sid, packet['errors'].get(sid, 'unavailable'))
            assert row['quality']['status']=='fresh',(sid,row['quality'])
            assert row['current_decimal'] is not None and set(row['evidence'])=={'definition','observations'}
        claims=packet['measurements']['ICSA']
        assert claims['unit']=='Number' and claims['frequency']=='W'
        assert packet['measurements']['CPIAUCSL']['week_pct'] is None
        assert packet['measurements']['UNRATE']['changes']['month']['change_unit']=='percentage_points'
        growth=packet['measurements'].get('A191RL1Q225SBEA')
        if growth:
            assert growth['changes']['quarter']['change_unit']=='percentage_points'
        policy=packet['measurements'].get('IORB')
        assert policy and policy['date']<=packet['generated_at'][:10], 'current policy rate cannot be future-dated'
        assert all(row['date']>packet['generated_at'][:10] for row in policy['future_dated_rows'])
        assert packet['net_liquidity']['net_decimal'] is not None
        assert packet['net_liquidity']['direction'] is None and packet['call'] is None and packet['sizing_eligible'] is False
        role=iam.get_role(RoleName='justhodl-scheduler-role')['Role']['Arn']
        name='justhodl-report-research-hourly'
        args={'Name':name,'ScheduleExpression':'rate(1 hour)','ScheduleExpressionTimezone':'UTC','State':'ENABLED',
              'Description':'Source-backed macro measurements; deterministic replay; no portfolio or notification authority',
              'FlexibleTimeWindow':{'Mode':'OFF'},
              'Target':{'Arn':config['FunctionArn'],'RoleArn':role,'Input':'{"action":"research_measurements"}',
                        'RetryPolicy':{'MaximumEventAgeInSeconds':1800,'MaximumRetryAttempts':1}}}
        try:old=scheduler.get_schedule(Name=name)
        except scheduler.exceptions.ResourceNotFoundException:old=None
        if old:
            assert old['Target']['Arn']==config['FunctionArn'],'schedule name belongs to another target'
            scheduler.update_schedule(**args)
        else:scheduler.create_schedule(**args)
        check=scheduler.get_schedule(Name=name)
        assert check['State']=='ENABLED' and check['Target']['Arn']==config['FunctionArn']
        assert json.loads(check['Target']['Input'])=={'action':'research_measurements'}
        proof={'contract':'report-research-verification.v1','commit':expected,'generated_at':datetime.now(timezone.utc).isoformat(),
               'quality':packet['quality'],'core':{sid:{'date':packet['measurements'][sid]['date'],'unit':packet['measurements'][sid]['unit'],
                                                    'current_decimal':packet['measurements'][sid]['current_decimal']} for sid in core},
               'replay':packet['replay'],'source_errors':packet['errors'],'schedule':name,'schedule_execution_observed':False,
               'legacy_report_writer_invoked':False,'portfolio_writes':0,'paid_ai_calls':0,'notifications_sent':0,
               'scope':'FRED research feed and The Read; legacy daily-report recommendations and other consumers still require migration'}
        s3.put_object(Bucket=bucket,Key='data/report-research-verification.json',Body=json.dumps(proof).encode(),ContentType='application/json',CacheControl='no-cache')
        r.kv(**proof)
        r.ok('Actual deployed research path, retained-original replay, exact units and core freshness verified; hourly schedule configured')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
