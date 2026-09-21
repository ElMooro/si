"""Dated ETF desk research with immutable originals and idempotent publication."""
import json,os
import boto3
from botocore.config import Config
from etf_desk_model import CONTRACT
from etf_desk_store import reader,run,missing
PUBLISHED_KEY='data/etf-desk-research.json'


def publish_current(client,bucket,current,raw,condition):
    if current!=PUBLISHED_KEY or set(condition) not in ({'IfMatch'},{'IfNoneMatch'}):
        raise ValueError('Reviewed desk publication and conditional write required')
    client.put_object(Bucket=bucket,Key=PUBLISHED_KEY,Body=raw,ContentType='application/json',CacheControl='no-store',**condition)


def lambda_handler(event=None,context=None):
    event=event if isinstance(event,dict) else {}
    if event.get('validate_only') is True:
        return {'statusCode':200,'body':json.dumps({'validation_only':True,'contract':CONTRACT,'published':False})}
    client=boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=5,read_timeout=20,
        retries={'max_attempts':2},max_pool_connections=24,tcp_keepalive=True))
    bucket='justhodl-dashboard-live'
    if (event.get('requestContext') or {}).get('http') or event.get('httpMethod') or event.get('action')=='current_state':
        try:packet=json.loads(reader(client,bucket)(PUBLISHED_KEY))
        except Exception as exc:
            if not missing(exc):raise
            packet={}
        if packet.get('contract')!=CONTRACT:
            return {'statusCode':503,'body':json.dumps({'reason':'native_desk_evidence_unavailable'})}
        return {'statusCode':200,'headers':{'Content-Type':'application/json','Cache-Control':'no-store'},'body':json.dumps(packet)}
    execution_id=getattr(context,'aws_request_id',None)
    if not execution_id:raise ValueError('AWS execution identity required')
    remaining=context.get_remaining_time_in_millis()/1000 if hasattr(context,'get_remaining_time_in_millis') else 900
    credential=os.environ.get('POLYGON_KEY') or os.environ.get('POLYGON_API_KEY') or os.environ.get('MASSIVE_API_KEY') or ''
    result=run(client,bucket,event.get('request_id') or event.get('id') or execution_id,execution_id,
        credential=credential,remaining_seconds=remaining,recover_run=event.get('recover_run'),publish=publish_current)
    return {'statusCode':200 if result['status']=='complete' else 202 if result['status']=='running' else 500,'body':json.dumps(result)}
