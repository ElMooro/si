"""Read-only operational evidence for holdings acceptance and consumer publication."""
from datetime import datetime, timezone, timedelta
from pathlib import Path
import json
import re
import sys
import boto3

ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'aws/ops'))
from ops_report import report


def main():
    bucket='justhodl-dashboard-live'
    lam=boto3.client('lambda',region_name='us-east-1')
    s3=boto3.client('s3',region_name='us-east-1')
    scheduler=boto3.client('scheduler',region_name='us-east-1')
    logs=boto3.client('logs',region_name='us-east-1')
    with report('ops_5878_holdings_runtime_observation') as r:
        for fn,key in [('justhodl-13f-positions','data/holdings-research.json'),
                       ('justhodl-master-ranker','data/master-ranker.json'),
                       ('justhodl-regime-composite','data/regime-composite.json'),
                       ('justhodl-signal-fabric','data/signal-fabric.json')]:
            conf=lam.get_function_configuration(FunctionName=fn)
            receipt=json.loads(s3.get_object(Bucket=bucket,Key='data/ops/releases/'+fn+'.json')['Body'].read())
            body=json.loads(s3.get_object(Bucket=bucket,Key=key)['Body'].read())
            r.kv(function=fn,code_sha256=conf['CodeSha256'],receipt_commit=receipt.get('commit'),
                 receipt_matches_runtime=receipt.get('code_sha256')==conf['CodeSha256'],state=conf['State'],
                 update_status=conf['LastUpdateStatus'],packet_generated_at=body.get('generated_at') or body.get('as_of'),
                 holdings_context_present=bool(body.get('holdings_context')),collection_request_id=body.get('collection_request_id'))
        schedule=scheduler.get_schedule(Name='justhodl-holdings-originals-research',GroupName='default')
        r.kv(schedule={k:schedule[k] for k in ('Name','State','ScheduleExpression','ScheduleExpressionTimezone')})
        start=int((datetime.now(timezone.utc)-timedelta(minutes=30)).timestamp()*1000)
        args={'logGroupName':'/aws/lambda/justhodl-13f-positions','startTime':start,'limit':1000}
        reports=[]; failures=[]; starts=ends=events=0
        for _ in range(10):
            page=logs.filter_log_events(**args)
            for event in page.get('events',[]):
                events+=1; message=event['message']
                starts+=int(message.startswith('START RequestId:'));ends+=int(message.startswith('END RequestId:'))
                if message.startswith('REPORT RequestId:'):
                    metrics={name:re.search(pattern,message) for name,pattern in {
                        'duration_ms':r'(?<!Billed )Duration: ([0-9.]+) ms',
                        'max_memory_mb':r'Max Memory Used: ([0-9]+) MB',
                        'status':r'Status: ([a-z]+)'}.items()}
                    reports.append({'at':datetime.fromtimestamp(event['timestamp']/1000,timezone.utc).isoformat(),
                                    **{k:(v.group(1) if v else None) for k,v in metrics.items()}})
                if 'holdings_acquisition_failure' in message:
                    # Never expose arbitrary Lambda log contents, URLs or source values.
                    failures.append({'at':event['timestamp'],'kind':'acquisition_failure'})
            token=page.get('nextToken')
            if not token or token==args.get('nextToken'):break
            args['nextToken']=token
        else:raise RuntimeError('Bounded log observation incomplete')
        r.kv(runtime_activity={'events_examined':events,'starts':starts,'ends':ends,'reports':reports,'acquisition_failures':failures},
             lambda_invocations=0,aws_mutations=0,private_account_reads=0)


if __name__=='__main__':
    try:main()
    except Exception:
        print('Read-only holdings observation failed; inspect committed report.')
        sys.exit(1)
