"""Dated original-source ETF research; no AI, account or notification calls."""
import json,re
import boto3
from botocore.config import Config
from managed_secret import managed_secret
import os
import gold_rotation_model as model
import gold_rotation_store as store
BUCKET='justhodl-dashboard-live'
PUBLISHED_KEY='data/gold-equity-rotation.json'

def publish_current(client,request):
    if request.get('Bucket')!=BUCKET or request.get('Key')!=PUBLISHED_KEY:raise ValueError('Reviewed Gold research target required')
    if not request.get('IfMatch') or request.get('CacheControl')!='no-store' or request.get('ContentType')!='application/json':raise ValueError('Conditional noncached publication required')
    return client.put_object(Key=PUBLISHED_KEY,**{k:v for k,v in request.items() if k!='Key'})

def request_path(key):return isinstance(key,str) and re.fullmatch(re.escape(model.PRIVATE)+r'requests/[a-f0-9]{64}\.json',key)

class EvidenceStorage:
    def __init__(self,client,publisher):self.client=client;self.publisher=publisher
    def get_object(self,**request):
        key=request.get('Key')
        if request.get('Bucket')!=BUCKET or not(key==model.CURRENT or store.artifact_key(key) or request_path(key)):raise ValueError('Reviewed research read required')
        return self.client.get_object(**request)
    def put_object(self,**request):
        if request.get('Bucket')!=BUCKET:raise ValueError('Reviewed research bucket required')
        key=request.get('Key')
        if key==PUBLISHED_KEY:return self.publisher(self.client,request)
        if not (request_path(key) or (store.artifact_key(key) and key.startswith((model.PREFIX,model.PRIVATE)))):raise ValueError('Gold research evidence write required')
        return self.client.put_object(**request)

def native(packet):
    if not isinstance(packet,dict) or packet.get('contract')!=model.CONTRACT or any(packet.get(k) is not False for k in model.FLAGS):return False
    ref=packet.get('replay',{})
    if not isinstance(ref,dict) or not re.fullmatch(re.escape(model.PREFIX)+r'runs/[a-f0-9]{64}\.json',str(ref.get('manifest_key',''))):return False
    return packet.get('call') is None and packet.get('independent_investment_votes')==0 and model.digest({k:v for k,v in packet.items() if k!='replay'})==ref.get('output_sha256')

def lambda_handler(event=None,context=None):
    event=event if isinstance(event,dict) else {}
    if event.get('validate_only') is True:return {'statusCode':200,'body':json.dumps({'contract':model.CONTRACT,'validation_only':True,'published':False})}
    client=EvidenceStorage(boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=4,read_timeout=10,retries={'max_attempts':1},max_pool_connections=8,tcp_keepalive=True)),publish_current)
    if (event.get('requestContext') or {}).get('http') or event.get('httpMethod') or event.get('action')=='current_state':
        try:packet=model.strict(store.bounded(client.get_object(Bucket=BUCKET,Key=PUBLISHED_KEY)['Body']))
        except Exception as exc:
            if not store.missing(exc):raise
            packet={}
        if not native(packet):return {'statusCode':503,'headers':{'Cache-Control':'no-store'},'body':json.dumps({'reason':'recorded_gold_research_unavailable'})}
        return {'statusCode':200,'headers':{'Content-Type':'application/json','Cache-Control':'no-store'},'body':json.dumps(packet)}
    execution=getattr(context,'aws_request_id',None)
    if not execution:raise ValueError('AWS execution identity required')
    credential=os.environ.get('FMP_KEY') or os.environ.get('FMP_API_KEY') or managed_secret((),('/justhodl/fmp/api-key','/justhodl/fmp-api-key'))
    result=store.run(client,BUCKET,event.get('request_id') or event.get('id') or execution,execution,credential)
    return {'statusCode':200,'body':json.dumps(result)}
