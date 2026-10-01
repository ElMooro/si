"""Dated original-source FINRA research. No AI, accounts or notifications.

Deploy retrigger: preflight fixture (squeeze-pretrigger migration entry)
retired after 3/10 rewire; no logic change.
Second retrigger: equity-enrich also retired from migration fixture.

Bloomberg parity 3/10: after the evidence-contract publication below, an
additive consumer-facing layer publishes data/short-interest-tickers.json
(per-ticker descriptive measurements). The research contract is untouched.
"""
import json,re
from datetime import datetime,timezone,date
import boto3
from botocore.config import Config
import short_interest_research_model as model
import short_interest_research_store as store
import short_interest_producer as producer
import short_interest_context
import short_interest_tickers as tickers
BUCKET='justhodl-dashboard-live'
PUBLISHED_KEY='data/short-interest.json'

def publish_current(client,request):
    if request.get('Bucket')!=BUCKET or request.get('Key')!=PUBLISHED_KEY:raise ValueError('Reviewed FINRA research target required')
    if not request.get('IfMatch') or request.get('CacheControl')!='no-store' or request.get('ContentType')!='application/json':raise ValueError('Conditional noncached publication required')
    return client.put_object(Key=PUBLISHED_KEY,**{k:v for k,v in request.items() if k!='Key'})

def request_path(key):return isinstance(key,str) and re.fullmatch(re.escape(model.PRIVATE)+r'requests/[a-f0-9]{64}\.json',key)

class EvidenceStorage:
    def __init__(self,client,publisher):self.client=client;self.publisher=publisher
    def get_object(self,**request):
        key=request.get('Key')
        if request.get('Bucket')!=BUCKET or not(key in (model.CURRENT,PUBLISHED_KEY) or store.artifact_key(key) or request_path(key)):raise ValueError('Reviewed research read required')
        return self.client.get_object(**request)
    def put_object(self,**request):
        if request.get('Bucket')!=BUCKET:raise ValueError('Reviewed research bucket required')
        key=request.get('Key')
        if key==PUBLISHED_KEY:return self.publisher(self.client,request)
        if not (request_path(key) or store.artifact_key(key)):raise ValueError('FINRA research evidence write required')
        return self.client.put_object(**request)

def _publish_tickers_layer(evidence_client,bucket,result):
    """Build and publish the per-ticker artifact from the just-published head.

    Reads the head packet and its record-shard refs through the evidence
    client (both are reviewed evidence keys), builds the consumer view with
    short_interest_tickers, and writes data/short-interest-tickers.json
    with a RAW client: EvidenceStorage only permits evidence-contract
    keys, and the tickers artifact is a separate Khalid-approved consumer
    artifact, not evidence. Fail-soft is the caller's policy; this helper
    raises so the caller can log the skip.
    """
    if not isinstance(result,dict) or result.get('published') is not True:
        return None
    head=store.bounded(evidence_client.get_object(Bucket=bucket,Key=PUBLISHED_KEY)['Body'])
    packet=model.strict(head)
    refs=packet.get('record_shards')
    if not isinstance(refs,dict) or not refs:
        raise ValueError('Published head carries no record shards')
    shards={}
    for prefix,ref in refs.items():
        key=ref.get('key') if isinstance(ref,dict) else None
        if not isinstance(key,str):
            raise ValueError('Shard reference missing key')
        raw=store.bounded(evidence_client.get_object(Bucket=bucket,Key=key)['Body'])
        shards[prefix]=model.strict(raw)
    raw_client=boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=4,read_timeout=10,retries={'max_attempts':1},max_pool_connections=8,tcp_keepalive=True))
    meta=tickers.publish_tickers_artifact(raw_client,bucket,shards,packet.get('settlement_date'),packet.get('generated_at'))
    try:
        age=(date.today()-date.fromisoformat(str(packet.get('settlement_date')))).days
        if age>45:
            print(f"[tickers-layer] WARNING settlement_date {packet.get('settlement_date')} is {age}d old")
    except (ValueError,TypeError):
        pass
    print(f"[tickers-layer] published {tickers.TICKERS_KEY} n_tickers={meta['n_tickers']} settlement={meta['settlement_date']}")
    return meta

def lambda_handler(event=None,context=None):
    event=event if isinstance(event,dict) else {}
    if event.get('validate_only') is True:return {'statusCode':200,'body':json.dumps({'contract':model.CONTRACT,'validation_only':True,'published':False})}
    client=EvidenceStorage(boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=4,read_timeout=10,retries={'max_attempts':1},max_pool_connections=8,tcp_keepalive=True)),publish_current)
    if (event.get('requestContext') or {}).get('http') or event.get('httpMethod') or event.get('action')=='current_state':
        try:packet=model.strict(store.bounded(client.get_object(Bucket=BUCKET,Key=PUBLISHED_KEY)['Body']))
        except Exception as exc:
            if not producer.missing(exc):raise
            packet={}
        if not short_interest_context.context(packet)['native_reference_available']:return {'statusCode':503,'headers':{'Cache-Control':'no-store'},'body':json.dumps({'reason':'recorded_short_interest_research_unavailable'})}
        return {'statusCode':200,'headers':{'Content-Type':'application/json','Cache-Control':'no-store'},'body':json.dumps(packet)}
    execution=getattr(context,'aws_request_id',None)
    if not execution:raise ValueError('AWS execution identity required')
    remaining=context.get_remaining_time_in_millis()/1000
    result=producer.run(client,BUCKET,event.get('request_id') or event.get('id') or 'scheduled-short-interest:'+datetime.now(timezone.utc).date().isoformat(),execution,remaining_seconds=remaining)
    try:
        _publish_tickers_layer(client,BUCKET,result)
    except Exception as exc:
        print(f"[tickers-layer] skipped: {type(exc).__name__}: {exc}")
    return {'statusCode':200,'body':json.dumps(result)}
