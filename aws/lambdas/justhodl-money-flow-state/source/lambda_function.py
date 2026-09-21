"""Dated sector research producer; HTTP reads, scheduled events publish."""
import json
import boto3
from botocore.config import Config
from money_volume_model import CONTRACT
from money_volume_store import CURRENT,reader,run
PUBLISHED_KEY='data/money-flow-state.json'
if CURRENT!=PUBLISHED_KEY:raise ValueError('Price-volume publication route drift')


def lambda_handler(event=None,context=None):
    event=event if isinstance(event,dict) else {}
    if event.get('validate_only') is True:
        return {'statusCode':200,'body':json.dumps({'validation_only':True,'contract':CONTRACT,'published':False})}
    client=boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=5,read_timeout=20,
        retries={'max_attempts':2},max_pool_connections=8,tcp_keepalive=True))
    bucket='justhodl-dashboard-live'
    if (event.get('requestContext') or {}).get('http') or event.get('httpMethod') or event.get('action')=='current_state':
        packet=json.loads(reader(client,bucket)(CURRENT))
        if packet.get('contract')!=CONTRACT:
            return {'statusCode':503,'body':json.dumps({'reason':'native_price_volume_publication_unavailable'})}
        return {'statusCode':200,'headers':{'Content-Type':'application/json','Cache-Control':'no-store'},'body':json.dumps(packet)}
    execution_id=getattr(context,'aws_request_id',None)
    if not execution_id:raise ValueError('AWS execution identity required')
    result=run(client,bucket,event.get('request_id',execution_id),execution_id)
    return {'statusCode':200 if result['status']=='complete' else 202 if result['status']=='running' else 500,
            'body':json.dumps(result)}
