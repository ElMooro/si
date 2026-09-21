"""Original option-chain research; HTTP reads never collect or publish."""
import json
import boto3
from botocore.config import Config
from managed_secret import managed_secret
from option_flow_research import CONTRACT
from option_flow_store import reader,run,missing
PUBLISHED_KEY='data/option-flow-research.json'


def publish_current(client,bucket,key,raw,condition):
    if key!=PUBLISHED_KEY or set(condition) not in ({'IfMatch'},{'IfNoneMatch'}):
        raise ValueError('Reviewed option target and conditional write required')
    client.put_object(Bucket=bucket,Key=PUBLISHED_KEY,Body=raw,ContentType='application/json',CacheControl='no-store',**condition)


def lambda_handler(event=None,context=None):
    event=event if isinstance(event,dict) else {}
    if event.get('validate_only') is True:
        return {'statusCode':200,'body':json.dumps({'validation_only':True,'contract':CONTRACT,'published':False})}
    client=boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=5,read_timeout=20,
        retries={'max_attempts':2},max_pool_connections=24,tcp_keepalive=True));bucket='justhodl-dashboard-live'
    if (event.get('requestContext') or {}).get('http') or event.get('httpMethod') or event.get('action')=='current_state':
        try:packet=json.loads(reader(client,bucket)(PUBLISHED_KEY))
        except Exception as exc:
            if not missing(exc):raise
            packet={}
        if packet.get('contract')!=CONTRACT:
            return {'statusCode':503,'body':json.dumps({'reason':'native_option_research_unavailable'})}
        return {'statusCode':200,'headers':{'Content-Type':'application/json','Cache-Control':'no-store'},'body':json.dumps(packet)}
    execution_id=getattr(context,'aws_request_id',None)
    if not execution_id:raise ValueError('AWS execution identity required')
    recovery=event.get('recover_run')
    credential='' if recovery is not None else managed_secret(('POLYGON_KEY','POLYGON_API_KEY','POLY_KEY'),('/justhodl/polygon/api-key',))
    remaining=context.get_remaining_time_in_millis()/1000 if hasattr(context,'get_remaining_time_in_millis') else 900
    result=run(client,bucket,event.get('request_id') or event.get('id') or execution_id,execution_id,
        credential=credential,remaining_seconds=remaining,recover_run=recovery,publish=publish_current)
    return {'statusCode':200 if result['status']=='complete' else 202 if result['status']=='running' else 500,'body':json.dumps(result)}
