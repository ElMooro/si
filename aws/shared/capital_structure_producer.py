"""Replay complete qualified capital-structure research before publication.

Only retained source bytes are read. This module never fetches market data,
calls another engine or AI, reads an account, sends a notification or orders.
The runner owns acquisition and independent qualification. An incomplete or
failed snapshot cannot replace the native head.
"""
from datetime import datetime, timezone
import gc, re, time
import capital_structure_source as source
import capital_structure_research as model
import capital_structure_store as store

CURRENT = 'data/share-flows.json'
READY = source.PRIVATE + 'ready.json'
READY_CONTRACT = 'capital-structure-qualified-ready.v1'


def now():
    return datetime.now(timezone.utc).isoformat()


def missing(exc):
    return str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in ('404', 'NoSuchKey')


def conflict(exc):
    return str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed')


def request_key(identity):
    if not isinstance(identity, str) or not 1 <= len(identity) <= 200:
        raise ValueError('Bounded durable capital-structure request identity required')
    return source.PRIVATE + 'requests/' + source.sha(identity.encode()) + '.json'


def raw(client, bucket, key):
    return store.bounded(client.get_object(Bucket=bucket, Key=key)['Body'])


def journal(client, bucket, key, value, claim=False):
    if not re.fullmatch(re.escape(source.PRIVATE) + r'requests/[a-f0-9]{64}\.json', key):
        raise ValueError('Reviewed capital-structure journal required')
    body = source.encoded(value)
    if len(body) > source.MAX:
        raise ValueError('Bounded request journal required')
    client.put_object(Bucket=bucket, Key=key, Body=body, ContentType='application/json', CacheControl='no-store',
        **({'IfNoneMatch': '*'} if claim else {}))
    if raw(client, bucket, key) != body:
        raise ValueError('Capital-structure journal readback differs')


def protect(client, bucket, body):
    if not isinstance(body, bytes) or not 0 < len(body) <= source.MAX:
        raise ValueError('Whole bounded capital-structure predecessor required')
    ref = {'key': source.PRIVATE + source.sha(body) + '.bin', 'sha256': source.sha(body), 'bytes': len(body)}
    try:
        client.put_object(Bucket=bucket, Key=ref['key'], Body=body, ContentType='application/octet-stream',
            CacheControl='no-store', IfNoneMatch='*')
    except Exception as exc:
        if not conflict(exc):
            raise
    if raw(client, bucket, ref['key']) != body:
        raise ValueError('Whole predecessor readback differs')
    return ref


def current_matches(packet, reference):
    return (isinstance(packet, dict) and packet.get('contract') == model.CONTRACT
        and packet.get('replay') == reference
        and source.sha(source.encoded({k: v for k, v in packet.items() if k != 'replay'})) == reference.get('output_sha256'))


def qualification_matches(ready, packet):
    proof = ready.get('qualification', {})
    if not isinstance(proof, dict):
        return False
    counts = {key: packet.get(key) for key in ('reported_names', 'provider_responses', 'provider_rows', 'empty_responses')}
    return (ready.get('contract') == READY_CONTRACT and ready.get('status') == 'qualified'
        and ready.get('source_manifest_sha256') == packet.get('source_manifest_sha256')
        and ready.get('counts') == counts
        and packet.get('contract') == model.CONTRACT
        and all(type(packet.get(key)) is type(value) and packet.get(key) == value for key, value in model.FLAGS.items())
        and proof.get('all_original_rows_conserved') is True
        and proof.get('production_measurement_formulas_imported') is False
        and proof.get('current_sec_pairs_checked') is True
        and proof.get('original_rows_checked') == packet.get('provider_rows')
        and proof.get('identity_metadata_rows_checked') == packet.get('provider_rows')
        and proof.get('metric_comparisons') == sum(packet.get('metric_statuses', {}).values())
        and proof.get('forecast_qualified') is False and proof.get('sizing_qualified') is False)


def run(client, bucket, request_id, execution_id, remaining_seconds=900, clock=now):
    if not isinstance(execution_id, str) or not execution_id:
        raise ValueError('Actual AWS execution identity required')
    deadline = time.monotonic() + remaining_seconds - 60
    status = request_key(request_id)
    try:
        previous_request = source.strict(raw(client, bucket, status))
    except Exception as exc:
        if not missing(exc):
            raise
        previous_request = None
    if previous_request:
        if previous_request.get('status') != 'complete':
            raise ValueError('Request already attempted; inspect its retained failure or ambiguous state')
        return previous_request['result']
    progress = {'request_id': request_id, 'execution_id': execution_id, 'status': 'claimed', 'started_at': clock()}
    journal(client, bucket, status, progress, True)
    try:
        response = client.get_object(Bucket=bucket, Key=READY)
        ready_bytes = store.bounded(response['Body'])
        ready, ready_etag = source.strict(ready_bytes), response['ETag']
        if not isinstance(ready, dict) or ready.get('contract') != READY_CONTRACT or ready.get('status') != 'qualified':
            raise ValueError('Whole independently qualified capital-structure snapshot required')

        def before_read():
            if time.monotonic() >= deadline:
                raise TimeoutError('Source replay budget exhausted; current packet retained')

        reference = ready['replay']
        read = store.reader(client, bucket, before_read=before_read)
        recorded = store.verified_run(reference, read)
        packet = store.checked(recorded['output'], 'outputs', read)
        if not qualification_matches(ready, packet):
            raise ValueError('Independent qualification differs from the recorded population')
        for stamp in (packet['generated_at'], packet['source_acquisition_started_at'],
                packet['source_acquisition_completed_at'], packet['identity_index']['requested_at']):
            if not 0 <= (source.clock(clock()) - source.clock(stamp)).total_seconds() <= 48 * 3600:
                raise ValueError('Underlying capital-structure or SEC acquisition is future-dated or older than 48 hours')
        head = client.get_object(Bucket=bucket, Key=CURRENT)
        old = store.bounded(head['Body'])
        previous = source.strict(old)
        progress.update(replay=reference, ready_sha256=source.sha(ready_bytes))
        if current_matches(previous, reference):
            result = {'published': False, 'reason': 'unchanged_qualified_snapshot', 'replay': reference}
        else:
            if remaining_seconds < 300:
                raise ValueError('Insufficient source replay budget')
            progress['predecessor'] = protect(client, bucket, old)
            journal(client, bucket, status, progress)
            compiled = store.replay(reference, read)
            if compiled['packet'] != packet:
                raise ValueError('Native source reconstruction differs from ready output')
            del compiled
            gc.collect()
            previous_time = source.clock(previous['generated_at']) if previous.get('generated_at') else None
            stamp = source.clock(packet['generated_at'])
            if previous_time and (stamp < previous_time or (stamp == previous_time and previous.get('replay') != reference)):
                result = {'published': False, 'reason': 'observation_rollback_or_equal_time_conflict', 'replay': reference}
            elif client.head_object(Bucket=bucket, Key=READY)['ETag'] != ready_etag:
                result = {'published': False, 'reason': 'new_ready_snapshot_available', 'replay': reference}
            else:
                before_read()
                body = source.encoded({**packet, 'replay': reference})
                if len(body) > source.MAX:
                    raise ValueError('Whole native capital-structure packet exceeds bound')
                try:
                    client.put_object(Bucket=bucket, Key=CURRENT, Body=body, ContentType='application/json',
                        CacheControl='no-store', IfMatch=head['ETag'])
                except Exception as exc:
                    if not conflict(exc):
                        raise
                    result = {'published': False, 'reason': 'concurrent_publication', 'replay': reference}
                else:
                    if raw(client, bucket, CURRENT) != body:
                        raise ValueError('Capital-structure publication readback differs')
                    result = {'published': True, 'generated_at': packet['generated_at'], 'replay': reference,
                        'reported_names': packet['reported_names'], 'provider_rows': packet['provider_rows']}
        journal(client, bucket, status, {**progress, 'status': 'complete', 'finished_at': clock(), 'result': result})
        return result
    except Exception as exc:
        journal(client, bucket, status, {**progress, 'status': 'failed', 'error_type': type(exc).__name__})
        raise
