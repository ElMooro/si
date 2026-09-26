"""Native source-qualified curve research; public access is read-only."""
import json,os
import boto3
from botocore.config import Config
from yield_curve_store import run,qualified_arithmetic
from yield_curve_model import CONTRACT,CURRENT

BUCKET=os.environ.get('S3_BUCKET','justhodl-dashboard-live')


def lambda_handler(event=None,context=None):
    event=event if isinstance(event,dict) else {}
    if event.get('validate_only') is True:
        qualified_arithmetic()
        return {'statusCode':200,'body':json.dumps({'contract':CONTRACT,'validated':True,'published':False})}
    if event.get('httpMethod') or (event.get('requestContext') or {}).get('http'):
        return {'statusCode':307,'headers':{'Location':'https://justhodl-data-proxy.raafouis.workers.dev/'+CURRENT,
            'Cache-Control':'no-store'},'body':''}
    if not getattr(context,'aws_request_id',None):raise ValueError('Native AWS execution context required')
    client=boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=4,read_timeout=20,
        retries={'max_attempts':2},max_pool_connections=8))
    result=run(client,BUCKET)
    return {'statusCode':200,'headers':{'Content-Type':'application/json'},
        'body':json.dumps({'engine_contract':CONTRACT,'packet_key':CURRENT,**result},allow_nan=False)}
