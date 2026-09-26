"""Publish the complete independently qualified capital-structure snapshot.

Only scheduled work replays retained original sources and conditionally replaces
the head. HTTP requests are read-only. Acquisition and qualification belong to
the runner; observations grant no forecast, portfolio or execution authority.
"""
import json, re
from datetime import datetime, timezone
import boto3
from botocore.config import Config
import capital_structure_source as source
import capital_structure_store as store
import capital_structure_producer as producer
import capital_structure_context as context_view
import capital_structure_research as model

BUCKET = 'justhodl-dashboard-live'
PUBLISHED_KEY = 'data/share-flows.json'


def request_path(key):
    return isinstance(key, str) and bool(re.fullmatch(re.escape(source.PRIVATE) + r'requests/[a-f0-9]{64}\.json', key))


def publish_current(client, request):
    if (request.get('Bucket') != BUCKET or request.get('Key') != PUBLISHED_KEY
            or not request.get('IfMatch') or request.get('CacheControl') != 'no-store'
            or request.get('ContentType') != 'application/json'):
        raise ValueError('Conditional reviewed capital-structure publication required')
    return client.put_object(Key=PUBLISHED_KEY, **{k: v for k, v in request.items() if k != 'Key'})


class EvidenceStorage:
    def __init__(self, client, publisher):
        self.client, self.publisher = client, publisher

    def get_object(self, **request):
        key = request.get('Key')
        if request.get('Bucket') != BUCKET or not (key in (PUBLISHED_KEY, producer.READY) or store.artifact_key(key) or request_path(key)):
            raise ValueError('Reviewed capital-structure research read required')
        return self.client.get_object(**request)

    def head_object(self, **request):
        if request.get('Bucket') != BUCKET or request.get('Key') != producer.READY:
            raise ValueError('Reviewed capital-structure readiness metadata required')
        return self.client.head_object(**request)

    def put_object(self, **request):
        if request.get('Bucket') != BUCKET:
            raise ValueError('Reviewed capital-structure research bucket required')
        key = request.get('Key')
        if key == PUBLISHED_KEY:
            return self.publisher(self.client, request)
        if not (request_path(key) or (store.artifact_key(key) and key.startswith(source.PRIVATE))):
            raise ValueError('Protected capital-structure evidence write required')
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
            if not producer.missing(exc):
                raise
            packet = {}
        if not context_view.context(packet)['native_reference_available']:
            return {'statusCode': 503, 'headers': {'Cache-Control': 'no-store'},
                'body': json.dumps({'reason': 'recorded_capital_structure_research_unavailable'})}
        if http:
            return {'statusCode': 307, 'headers': {'Location': 'https://justhodl.ai/' + PUBLISHED_KEY,
                'Cache-Control': 'no-store'}, 'body': ''}
        return {'statusCode': 200, 'body': json.dumps({'canonical_key': PUBLISHED_KEY,
            'generated_at': packet['generated_at'], 'replay': packet['replay'],
            'reported_names': packet['reported_names'], 'provider_rows': packet['provider_rows'],
            'calls_eligible': False, 'sizing_eligible': False})}
    execution = getattr(context, 'aws_request_id', None)
    if not execution:
        raise ValueError('Actual AWS execution identity required')
    result = producer.run(client, BUCKET, event.get('request_id') or event.get('id') or
        'scheduled-capital-structure:' + datetime.now(timezone.utc).date().isoformat(), execution,
        remaining_seconds=context.get_remaining_time_in_millis() / 1000)
    return {'statusCode': 200, 'body': json.dumps(result)}
