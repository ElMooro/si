"""Native replay and conditional publication of qualified accounting research.

Whole previous implementation retained in the statement migration archive.
Provider collection runs on the AWS-authorized Actions runner. No account
access, consumer invocation, signals, notifications or AI.
"""
import json, re
from datetime import datetime, timezone
import boto3
from botocore.config import Config
import statement_research_source as source
import statement_research_v2 as model
import statement_research_store_v2 as store
import statement_producer as producer
import statement_context

BUCKET = 'justhodl-dashboard-live'
PUBLISHED_KEY = 'data/forensic-screen.json'


def publish_current(client, request):
    if (request.get('Bucket') != BUCKET or request.get('Key') != PUBLISHED_KEY
            or not request.get('IfMatch') or request.get('CacheControl') != 'no-store'
            or request.get('ContentType') != 'application/json'):
        raise ValueError('Conditional reviewed accounting publication required')
    return client.put_object(Key=PUBLISHED_KEY, **{k: v for k, v in request.items() if k != 'Key'})


def request_path(key):
    return isinstance(key, str) and re.fullmatch(re.escape(model.PRIVATE) + r'requests/[a-f0-9]{64}\.json', key)


class EvidenceStorage:
    def __init__(self, client, publisher):
        self.client, self.publisher = client, publisher
    def get_object(self, **request):
        key = request.get('Key')
        if request.get('Bucket') != BUCKET or not (key in (PUBLISHED_KEY, producer.READY) or store.artifact_key(key) or request_path(key)):
            raise ValueError('Reviewed accounting research read required')
        return self.client.get_object(**request)
    def head_object(self, **request):
        if request.get('Bucket') != BUCKET or request.get('Key') != producer.READY:
            raise ValueError('Reviewed accounting ready metadata required')
        return self.client.head_object(**request)
    def put_object(self, **request):
        if request.get('Bucket') != BUCKET:
            raise ValueError('Reviewed accounting research bucket required')
        key = request.get('Key')
        if key == PUBLISHED_KEY:
            return self.publisher(self.client, request)
        if not (request_path(key) or (store.artifact_key(key) and key.startswith(model.PRIVATE))):
            raise ValueError('Protected accounting evidence write required')
        return self.client.put_object(**request)


def lambda_handler(event=None, context=None):
    event = event if isinstance(event, dict) else {}
    if event.get('validate_only') is True:
        return {'statusCode': 200, 'body': json.dumps({'contract': model.CONTRACT, 'validation_only': True, 'published': False})}
    client = EvidenceStorage(boto3.client('s3', region_name='us-east-1', config=Config(connect_timeout=4,
        read_timeout=10, retries={'max_attempts': 1}, max_pool_connections=12, tcp_keepalive=True)), publish_current)
    http = bool((event.get('requestContext') or {}).get('http') or event.get('httpMethod'))
    if http or event.get('action') == 'current_state':
        try:
            packet = source.strict(store.bounded(client.get_object(Bucket=BUCKET, Key=PUBLISHED_KEY)['Body']))
        except Exception as exc:
            if not producer.missing(exc): raise
            packet = {}
        if not statement_context.context(packet)['native_reference_available']:
            return {'statusCode': 503, 'headers': {'Cache-Control': 'no-store'},
                'body': json.dumps({'reason': 'recorded_accounting_research_unavailable'})}
        if http:
            return {'statusCode': 307, 'headers': {'Location': 'https://justhodl.ai/' + PUBLISHED_KEY,
                'Cache-Control': 'no-store'}, 'body': ''}
        return {'statusCode': 200, 'body': json.dumps({'canonical_key': PUBLISHED_KEY,
            'generated_at': packet['generated_at'], 'replay': packet['replay'],
            'reported_names': packet['reported_names'], 'provider_rows': packet['provider_rows']})}
    execution = getattr(context, 'aws_request_id', None)
    if not execution:
        raise ValueError('AWS execution identity required')
    result = producer.run(client, BUCKET, event.get('request_id') or event.get('id') or
        'scheduled-accounting-research:' + datetime.now(timezone.utc).date().isoformat(), execution,
        remaining_seconds=context.get_remaining_time_in_millis() / 1000)
    return {'statusCode': 200, 'body': json.dumps(result)}
