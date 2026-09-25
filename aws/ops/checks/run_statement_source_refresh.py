"""Runner entry point: complete public-source capture, replay, private readiness."""
from pathlib import Path
import json, os, sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops/checks')]
import boto3
from botocore.config import Config
import statement_source_refresh as refresh


def main():
    run=os.environ.get('GITHUB_RUN_ID','')
    if not run.isdigit():raise ValueError('Durable GitHub workflow run identity required')
    if os.environ.get('GITHUB_REF')!='refs/heads/main':raise ValueError('Reviewed main checkout required')
    # Retry attempts keep the same identity. An incomplete attempt is inspected
    # and explicitly recovered, never silently repeated under a new attempt ID.
    request='scheduled-accounting-source:'+run
    cfg=boto3.client('lambda',region_name='us-east-1').get_function_configuration(FunctionName='justhodl-forensic-screen')
    credential=cfg.get('Environment',{}).get('Variables',{}).get('FMP_KEY')
    if not credential:
        credential=boto3.client('ssm',region_name='us-east-1').get_parameter(Name='/justhodl/fmp/api-key',WithDecryption=True)['Parameter']['Value']
    client=boto3.client('s3',region_name='us-east-1',config=Config(max_pool_connections=16,retries={'max_attempts':2}))
    result=refresh.run(client,request,credential,remaining_seconds=3000)
    print(json.dumps({'request_id':request,**result}),flush=True)
    if not result.get('ready_advanced') and result.get('reason') not in ('source_time_rollback','concurrent_ready_publication'):
        raise ValueError('Accounting source readiness did not advance')


if __name__=='__main__':
    try:main()
    except Exception as exc:
        print(json.dumps({'status':'failed','error_type':type(exc).__name__,
            'action':'Inspect the retained source-refresh journal before recovery; no blind retry.'}),flush=True)
        sys.exit(1)
