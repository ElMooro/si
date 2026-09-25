"""Publish an exact canonical research mirror for the former pressure endpoint.

The mirror creates no new measurement, forecast or independent evidence vote.
It verifies the retained run and output bytes; it does not repeat source replay.
"""
import short_volume_context as context
import short_volume_research_model as model
import short_volume_research_store as store
import short_volume_producer as producer


def run(client, bucket, request_id, execution_id):
    if not isinstance(execution_id, str) or not execution_id:
        raise ValueError('Actual execution identity required')
    status = producer.request_key(request_id)
    try:
        existing = model.strict(producer.raw_read(client, bucket, status))
    except Exception as exc:
        if not producer.missing(exc):
            raise
        existing = None
    if existing:
        if existing.get('status') != 'complete':
            raise ValueError('Reference request already attempted; inspect retained status')
        return existing['result']
    producer.journal(client, bucket, status, {'request_id': request_id, 'execution_id': execution_id, 'status': 'claimed'}, True)
    progress = {}
    try:
        head = client.get_object(Bucket=bucket, Key=context.ALIAS)
        previous_raw = store.bounded(head['Body'])
        previous = model.strict(previous_raw)
        etag = head['ETag']
        progress['predecessor'] = producer.protect(client, bucket, previous_raw)
        raw = producer.raw_read(client, bucket, model.CURRENT)
        packet = model.strict(raw)
        if not context.context(packet)['native_reference_available']:
            raise ValueError('Qualified native daily research is unavailable')
        read = store.reader(client, bucket)
        run_manifest = store.verified_run(packet['replay'], read)
        retained = store.checked(run_manifest['output'], 'outputs', read)
        if retained != {k: v for k, v in packet.items() if k != 'replay'}:
            raise ValueError('Canonical current differs from its retained output')
        progress['source_publication_sha256'] = model.sha(raw)
        if previous_raw == raw:
            result = {'published': False, 'reason': 'already_current', 'replay': packet['replay']}
        elif not producer.not_older(packet, previous):
            result = {'published': False, 'reason': 'observation_or_publication_rollback', 'replay': packet['replay']}
        else:
            try:
                client.put_object(Bucket=bucket, Key=context.ALIAS, Body=raw, ContentType='application/json',
                                  CacheControl='no-store', IfMatch=etag)
            except Exception as exc:
                if not store.conflict(exc):
                    raise
                result = {'published': False, 'reason': 'concurrent_publication', 'replay': packet['replay']}
            else:
                if producer.raw_read(client, bucket, context.ALIAS) != raw:
                    raise ValueError('Canonical mirror readback differs')
                result = {'published': True, 'generated_at': packet['generated_at'], 'replay': packet['replay'],
                          'source_publication_sha256': model.sha(raw), 'independent_investment_votes': 0}
        producer.journal(client, bucket, status, {'request_id': request_id, 'execution_id': execution_id,
                                                'status': 'complete', 'result': result, **progress})
        return result
    except Exception as exc:
        producer.journal(client, bucket, status, {'request_id': request_id, 'execution_id': execution_id,
                                                'status': 'failed', 'error_type': type(exc).__name__, **progress})
        raise
