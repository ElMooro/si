"""Native public research synthesis. HTTP reads; scheduled events publish."""
import json
import boto3
from botocore.config import Config
from extremes_native_model import CONTRACT
from extremes_native_store import current,reader,run
ENGINE='market-extremes'

def lambda_handler(event=None,context=None):
    event=event if isinstance(event,dict) else {}
    if event.get('validate_only') is True:
        return {'statusCode':200,'body':json.dumps({'validation_only':True,'contract':CONTRACT,'engine':ENGINE,'published':False})}
    client=boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=3,read_timeout=10,retries={'max_attempts':1},max_pool_connections=8,tcp_keepalive=True))
    bucket='justhodl-dashboard-live'
    if (event.get('requestContext') or {}).get('http') or event.get('httpMethod'):
        packet=json.loads(reader(client,bucket)(current(ENGINE)))
        if packet.get('contract')!=CONTRACT or packet.get('engine')!=ENGINE:
            return {'statusCode':503,'body':json.dumps({'reason':'native_research_publication_unavailable'})}
        return {'statusCode':200,'headers':{'Content-Type':'application/json','Cache-Control':'no-store'},'body':json.dumps(packet)}
    execution_id=getattr(context,'aws_request_id',None)
    if not execution_id:raise ValueError('AWS execution identity required')
    result=run(client,bucket,ENGINE,event.get('request_id',execution_id),execution_id,context.get_remaining_time_in_millis()/1000)
    return {'statusCode':200 if result['status']=='complete' else 202 if result['status']=='running' else 500,'body':json.dumps(result)}
