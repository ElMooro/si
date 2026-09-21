"""Recorded Dollar research; no provider collection, credentials or alerts."""
import json,re
import boto3
from botocore.config import Config
from dollar_research_model import CONTRACT, CURRENT, PRIVATE
from dollar_research_store import run, bounded, missing, artifact
from dollar_research_context import context as research_context
PUBLISHED_KEY='data/dollar-radar.json'
BUCKET='justhodl-dashboard-live'


def publish_current(client,request):
    """The sole head writer, injected into the evidence storage adapter."""
    if request.get('Bucket')!=BUCKET or request.get('Key')!=PUBLISHED_KEY:
        raise ValueError('Reviewed Dollar publication target required')
    if not request.get('IfMatch') or request.get('CacheControl')!='no-store' or request.get('ContentType')!='application/json':
        raise ValueError('Conditional noncached Dollar publication required')
    payload={k:v for k,v in request.items() if k!='Key'}
    return client.put_object(Key=PUBLISHED_KEY,**payload)


class EvidenceStorage:
    """Restrict publication to the one public head and reviewed evidence paths."""
    def __init__(self,client,publisher):self.client=client;self.publisher=publisher
    def get_object(self,**request):return self.client.get_object(**request)
    def put_object(self,**request):
        if request.get('Bucket')!=BUCKET:raise ValueError('Reviewed Dollar bucket required')
        key=request.get('Key')
        if key==PUBLISHED_KEY:
            return self.publisher(self.client,request)
        if not artifact(key) and not (isinstance(key,str) and re.fullmatch(re.escape(PRIVATE)+r'requests/[a-f0-9]{64}\.json',key)):
            raise ValueError('Reviewed Dollar evidence target required')
        return self.client.put_object(**request)


def lambda_handler(event=None,context=None):
    event=event if isinstance(event,dict) else {}
    if event.get('validate_only') is True:
        return {'statusCode':200,'body':json.dumps({'validation_only':True,'contract':CONTRACT,'published':False})}
    client=EvidenceStorage(boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=5,read_timeout=15,
        retries={'max_attempts':2},max_pool_connections=8,tcp_keepalive=True)),publish_current)
    if (event.get('requestContext') or {}).get('http') or event.get('httpMethod') or event.get('action')=='current_state':
        try:packet=json.loads(bounded(client.get_object(Bucket=BUCKET,Key=CURRENT)['Body']))
        except Exception as exc:
            if not missing(exc):raise
            packet={}
        if not research_context(packet)['native_reference_available']:
            return {'statusCode':503,'headers':{'Cache-Control':'no-store'},'body':json.dumps({'reason':'recorded_dollar_research_unavailable'})}
        return {'statusCode':200,'headers':{'Content-Type':'application/json','Cache-Control':'no-store'},'body':json.dumps(packet)}
    execution=getattr(context,'aws_request_id',None)
    if not execution:raise ValueError('AWS execution identity required')
    result=run(client,BUCKET,event.get('request_id') or event.get('id') or execution,execution,recover_run=event.get('recover_run'))
    return {'statusCode':200 if result['status']=='complete' else 202 if result['status']=='running' else 500,'body':json.dumps(result)}
