"""Qualify a complete retained source population before private readiness.

No vendor requests, native/current-head writes, Lambda invocations, account
reads, signals or AI. The independent checker is separate from the frozen
production compiler. Every original remains available for source replay.
"""
from datetime import datetime, timezone
import gc, time
import capital_structure_source as source
import capital_structure_research as model
import capital_structure_store as store
import capital_structure_producer as producer
import capital_structure_arithmetic as arithmetic


def now():
    return datetime.now(timezone.utc).isoformat()


def fingerprint(compiled):
    return {'packet': source.sha(source.encoded(compiled['packet'])),
        'shards': {name: source.sha(source.encoded(value)) for name, value in compiled['shards'].items()}}


def run(client, bucket, request_id, manifest_ref, identity_ref, remaining_seconds=2400, clock=now):
    if not isinstance(remaining_seconds, (int, float)) or isinstance(remaining_seconds, bool) or not 600 <= remaining_seconds <= 5100:
        raise ValueError('Bounded full-population qualification budget required')
    deadline = time.monotonic() + remaining_seconds - 120
    status = producer.request_key('qualification:' + request_id)
    try:
        previous_request = source.strict(producer.raw(client, bucket, status))
    except Exception as exc:
        if not producer.missing(exc):
            raise
        previous_request = None
    if previous_request:
        if previous_request.get('status') != 'complete':
            raise ValueError('Qualification already attempted; inspect retained failure or ambiguous state')
        if previous_request.get('source_manifest') != manifest_ref or previous_request.get('identity_capture') != identity_ref:
            raise ValueError('Request identity cannot be reused for different originals')
        return previous_request['result']
    progress = {'contract': 'capital-structure-qualification.v1', 'request_id': request_id,
        'source_manifest': manifest_ref, 'identity_capture': identity_ref, 'status': 'claimed', 'started_at': clock()}
    producer.journal(client, bucket, status, progress, True)
    try:
        try:
            head = client.get_object(Bucket=bucket, Key=producer.READY)
            previous_bytes = store.bounded(head['Body'])
            previous, etag = source.strict(previous_bytes), head['ETag']
            progress['previous_ready'] = producer.protect(client, bucket, previous_bytes)
        except Exception as exc:
            if not producer.missing(exc):
                raise
            previous, etag = None, None

        def before_read():
            if time.monotonic() >= deadline:
                raise TimeoutError('Qualification budget exhausted; readiness retained')

        read = store.reader(client, bucket, before_read=before_read)
        compiled = model.compile_output(manifest_ref, identity_ref, read)
        for stamp in (compiled['packet']['generated_at'], compiled['packet']['source_acquisition_started_at'],
                compiled['packet']['source_acquisition_completed_at'], compiled['packet']['identity_index']['requested_at']):
            if not 0 <= (source.clock(clock()) - source.clock(stamp)).total_seconds() <= 48 * 3600:
                raise ValueError('Future or stale underlying source acquisition cannot advance readiness')
        before_read()
        proof = arithmetic.verify(manifest_ref, identity_ref, compiled, read)
        expected = fingerprint(compiled)
        before_read()
        reference = store.retain(client, bucket, manifest_ref, identity_ref, compiled)
        progress.update(status='retained', replay=reference, qualification=proof)
        producer.journal(client, bucket, status, progress)
        del compiled, read
        gc.collect()
        # New reader re-fetches the retained original bytes. Warm compiler
        # objects and cached source bodies cannot substitute for this replay.
        read = store.reader(client, bucket, before_read=before_read)
        replayed = store.replay(reference, read)
        if fingerprint(replayed) != expected:
            raise ValueError('Full original-source and issuer-history replay differs')
        packet = replayed['packet']
        ready = {'contract': producer.READY_CONTRACT, 'status': 'qualified', 'request_id': request_id,
            'qualified_at': clock(), 'generated_at': packet['generated_at'], 'source_manifest_sha256': manifest_ref['sha256'],
            'replay': reference, 'qualification': proof,
            'counts': {key: packet[key] for key in ('reported_names', 'provider_responses', 'provider_rows', 'empty_responses')},
            'provider_requests': 0, 'producer_invocations': 0, 'consumer_invocations': 0,
            'private_account_reads': 0, 'current_head_writes': 0, 'signal_writes': 0,
            'paid_ai_calls': 0, 'notifications_sent': 0, 'native_runtime_capacity_verified': False,
            'forecast_qualified': False, 'sizing_qualified': False}
        if not producer.qualification_matches(ready, packet):
            raise ValueError('Independent population qualification does not match native publication contract')
        before_read()
        if previous and source.clock(previous['generated_at']) >= source.clock(ready['generated_at']):
            result = {'ready_advanced': False, 'reason': 'source_time_rollback_or_equal_time', 'replay': reference}
        else:
            body = source.encoded(ready)
            try:
                client.put_object(Bucket=bucket, Key=producer.READY, Body=body,
                    ContentType='application/json', CacheControl='no-store',
                    **({'IfMatch': etag} if etag else {'IfNoneMatch': '*'}))
            except Exception as exc:
                if not producer.conflict(exc):
                    raise
                result = {'ready_advanced': False, 'reason': 'concurrent_ready_publication', 'replay': reference}
            else:
                if producer.raw(client, bucket, producer.READY) != body:
                    raise ValueError('Qualified private readiness readback differs')
                result = {'ready_advanced': True, 'reason': 'qualified', 'replay': reference, 'counts': ready['counts']}
        producer.journal(client, bucket, status, {**progress, 'status': 'complete', 'finished_at': clock(), 'result': result})
        return result
    except Exception as exc:
        producer.journal(client, bucket, status, {**progress, 'status': 'failed', 'error_type': type(exc).__name__})
        raise
