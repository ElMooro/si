"""Retain complete public desk evidence before updating its compact-view pointer.

Ordinary producer publication only. Verification operations must remain scoped
to release receipts and isolated fixtures, not fetch these current/derived data.
"""
import auction_delivery as model


def bounded(stream, limit):
    try:
        raw = stream.read(limit+1)
    finally:
        stream.close()
    if len(raw) > limit:
        raise ValueError('Delivery object exceeds its complete-byte bound')
    return raw


def code(exc):
    return str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))


def reader(client, bucket):
    def read(key):
        if not isinstance(key, str) or not key.startswith(model.PREFIX):
            raise ValueError('Only owned delivery artifacts may be read')
        # Paths are checked against exact typed references before this reader
        # is used. It cannot read arbitrary paths by resolving ../ segments.
        import re
        if not re.fullmatch(r'data/auction-desk-delivery/(snapshots|rows|sections|views|manifests)/[a-f0-9]{64}\.json(?:\.gz)?', key):
            raise ValueError('Exact content-addressed delivery path required')
        return bounded(client.get_object(Bucket=bucket, Key=key)['Body'], model.MAX_PACKET)
    return read


def retain(client, bucket, ref, raw):
    model.validate_reference(ref)
    # Validate the entire prospective object before making any write.
    model.verified_bytes(ref, lambda key: raw)
    try:
        # Reconstruct only these owned paths at the actual write site. Both
        # runtime validation and the source ownership graph see the same scope.
        if ref['encoding'] == 'gzip':
            client.put_object(Bucket=bucket, Key=f"data/auction-desk-delivery/snapshots/{ref['sha256']}.json.gz",
                              Body=raw, IfNoneMatch='*', ContentType='application/gzip',
                              CacheControl='public, max-age=31536000, immutable')
        else:
            for category in ('rows', 'sections', 'views', 'manifests'):
                key = f"data/auction-desk-delivery/{category}/{ref['sha256']}.json"
                if key == ref['key']:
                    client.put_object(Bucket=bucket, Key=key, Body=raw, IfNoneMatch='*', ContentType='application/json',
                                      CacheControl='public, max-age=31536000, immutable')
                    break
            else:
                raise ValueError('No owned JSON delivery family matches the reference')
    except Exception as exc:
        if code(exc) not in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
            raise
    model.verified_bytes(ref, reader(client, bucket))


def previous(client, bucket):
    try:
        response = client.get_object(Bucket=bucket, Key=model.CURRENT)
    except Exception as exc:
        if code(exc) in ('404', 'NoSuchKey'):
            return None, {'IfNoneMatch': '*'}
        raise
    raw = bounded(response['Body'], model.MAX_VIEW)
    doc = model.strict(raw)
    if doc.get('contract') != model.CONTRACT or any(doc.get(key) is not False for key in model.FLAGS):
        raise ValueError('Previous delivery pointer is invalid; preserve for review')
    model.clock(doc.get('generated_at'))
    model.validate_reference(doc.get('manifest'), 'manifests')
    model.validate_reference(doc.get('view'), 'views')
    etag = response.get('ETag')
    if type(etag) is not str or not etag:
        raise ValueError('Conditional publication requires an observed ETag')
    return doc, {'IfMatch': etag}


def publish(client, bucket, packet):
    # Only this producer's own publication pointer is inspected. No private,
    # downstream evaluation, account or provider reader is introduced.
    old, condition = previous(client, bucket)
    if old and model.clock(old['generated_at']) > model.clock(packet.get('generated_at')):
        raise ValueError('Newer desk delivery already exists')
    locator, artifacts = model.build(packet)
    if old and model.clock(old['generated_at']) == model.clock(locator['generated_at']) and model.encoded(old) != model.encoded(locator):
        raise ValueError('Conflicting desk publication at the same timestamp')
    manifest = model.strict(artifacts[locator['manifest']['key']])
    refs = [manifest['source_packet'], *manifest['row_chunks'], *manifest['sections'].values(), locator['view'], locator['manifest']]
    if {ref['key'] for ref in refs} != set(artifacts):
        raise ValueError('Complete delivery inventory required')
    # Retain and read back all artifacts before the pointer can become visible.
    for ref in refs:
        retain(client, bucket, ref, artifacts[ref['key']])
    raw = model.encoded(locator)
    # Even an identical retry checks the observed pointer. A concurrent newer
    # publication must not be reported as this attempt's current snapshot.
    client.put_object(Bucket=bucket, Key=model.CURRENT, Body=raw, ContentType='application/json',
                      CacheControl='no-cache', **condition)
    return {'contract': model.CONTRACT, 'view_key': model.CURRENT, 'artifacts': len(artifacts),
            'source_bytes': manifest['source_packet']['bytes'], 'view_bytes': locator['view']['bytes'],
            'generated_at': locator['generated_at']}
