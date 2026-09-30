"""Ordered snapshot transport with private prior-body retention and an outbox.

S3 and the mirror are separate commits. A receipt is an observed successful
delivery, not a perpetual claim that both stores remain current. No authority
to value an account, size a position or qualify upstream research is added.
"""
from datetime import datetime, timezone
import hashlib
import json
import re
import time
import urllib.error
import urllib.request

import portfolio_snapshot_publication as wire

BUCKET = 'justhodl-dashboard-live'
ACCOUNT = '857687956942'
KEY = 'portfolio/snapshot.json'
ARCHIVE = 'history/archive/feed/' + KEY + '/'
PROTOCOL = 'portfolio-snapshot-publication.v1'
MAX_REVISION = 2**53 - 1
TOKEN = re.compile(r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}\Z')
META = 'jh-snapshot-publication-token'
RESERVE_URL = wire.URL + '&action=reserve'


class OrderingUnavailable(wire.SnapshotPublicationUnavailable):
    """Diagnostics contain no account bytes, service credential or response text."""


class MirrorConflict(OrderingUnavailable):
    def __init__(self, code):
        self.code = code
        super().__init__('Snapshot mirror revision is unavailable')


def guard_statements():
    current = 'arn:aws:s3:::' + BUCKET + '/' + KEY
    archive = 'arn:aws:s3:::' + BUCKET + '/' + ARCHIVE + '*'
    return [
        {'Sid':'SnapshotCASv1', 'Effect':'Deny', 'Principal':'*', 'Action':'s3:PutObject',
         'Resource':current, 'Condition':{'Null':{'s3:if-match':'true','s3:if-none-match':'true'}}},
        {'Sid':'SnapshotHistoryCASv1', 'Effect':'Deny', 'Principal':'*', 'Action':'s3:PutObject',
         'Resource':archive, 'Condition':{'Null':{'s3:if-none-match':'true'}}},
        {'Sid':'SnapshotHistoryNoDeletev1', 'Effect':'Deny', 'Principal':'*',
         'Action':['s3:DeleteObject','s3:DeleteObjectVersion','s3:ReplicateObject','s3:ReplicateDelete'],
         'Resource':[current,archive]},
    ]


def strict_document(raw, limit):
    if type(raw) is not bytes or not 0 < len(raw) <= limit:
        raise OrderingUnavailable('Complete bounded snapshot document required')
    def pairs(rows):
        out = {}
        for key, item in rows:
            if key in out:raise ValueError('duplicate')
            out[key] = item
        return out
    def constant(_):raise ValueError('nonfinite')
    try:
        value = json.loads(raw.decode('utf-8', 'strict'), object_pairs_hook=pairs, parse_constant=constant)
        if type(value) is not dict:raise ValueError('object')
        # Enforce all finite values, Unicode and consumer depth/number bounds.
        wire.encode_snapshot(value, max_bytes=limit)
        return value
    except (ValueError, UnicodeError, TypeError, RecursionError, OverflowError):
        raise OrderingUnavailable('Complete snapshot document is invalid') from None


def _response(value):
    if type(value) is not dict or type(value.get('ResponseMetadata')) is not dict or type(value['ResponseMetadata'].get('HTTPStatusCode')) is not int or value['ResponseMetadata']['HTTPStatusCode'] != 200:
        raise OrderingUnavailable('Successful complete storage response required')
    return value


def storage_guard(s3):
    """Observe scoped protections before acquisition and again before a write.

    This is a configuration check, not an atomic policy lock or a cancellation
    of requests admitted before cutover. Only single PutObject is supported.
    """
    args = {'Bucket':BUCKET, 'ExpectedBucketOwner':ACCOUNT}
    try:
        response = _response(s3.get_bucket_policy(**args))
        policy = response.get('Policy')
        if type(policy) is not str:raise OrderingUnavailable('Whole bucket policy required')
        doc = strict_document(policy.encode('utf-8'), 20480)
        rows = doc.get('Statement')
        if type(rows) is not list or any(type(row) is not dict for row in rows):
            raise OrderingUnavailable('Complete bucket statements required')
        for expected in guard_statements():
            if [row for row in rows if row.get('Sid') == expected['Sid']] != [expected]:
                raise OrderingUnavailable('Snapshot conditional storage guard is not installed')
        privacy = [row for row in rows if row.get('Sid') == 'Audit20260909PrivatePersonalArtifacts']
        fields = {'Sid':'Audit20260909PrivatePersonalArtifacts','Effect':'Deny','Principal':'*',
                  'Action':['s3:GetObject','s3:GetObjectVersion'],
                  'Condition':{'StringNotEquals':{'aws:PrincipalAccount':ACCOUNT}}}
        if len(privacy) != 1 or {k:v for k,v in privacy[0].items() if k != 'Resource'} != fields:
            raise OrderingUnavailable('Snapshot private read boundary is unavailable')
        resources = privacy[0].get('Resource')
        required = guard_statements()[-1]['Resource']
        if type(resources) is not list or any(value not in resources for value in required):
            raise OrderingUnavailable('Snapshot history private read boundary is unavailable')
        if _response(s3.get_bucket_versioning(**args)).get('Status') != 'Enabled':
            raise OrderingUnavailable('Snapshot version retention is unavailable')
        lifecycle = _response(s3.get_bucket_lifecycle_configuration(**args))
        rules = lifecycle.get('Rules')
        if type(rules) is not list:raise OrderingUnavailable('Complete lifecycle configuration required')
        for row in rules:
            if type(row) is not dict or row.get('Status') not in ('Enabled','Disabled'):
                raise OrderingUnavailable('Lifecycle rule is invalid')
            if row['Status'] == 'Disabled':continue
            if not any(k in row for k in ('Expiration','NoncurrentVersionExpiration')):continue
            if 'Filter' in row and 'Prefix' in row:raise OrderingUnavailable('Lifecycle filter is ambiguous')
            filt = row.get('Filter', {'Prefix':row.get('Prefix', '')})
            if type(filt) is not dict:raise OrderingUnavailable('Lifecycle filter is invalid')
            # A tag/size constraint may still match a protected object. Require
            # a disjoint explicit prefix; do not assume today's tags stay fixed.
            prefix = filt.get('Prefix', filt.get('And', {}).get('Prefix') if type(filt.get('And', {})) is dict else None)
            if prefix is None and not filt:prefix = ''
            if type(prefix) is not str or KEY.startswith(prefix) or ARCHIVE.startswith(prefix) or prefix.startswith(ARCHIVE):
                raise OrderingUnavailable('Snapshot retention conflicts with lifecycle expiry')
        return hashlib.sha256(policy.encode('utf-8')).hexdigest()
    except OrderingUnavailable:raise
    except Exception:raise OrderingUnavailable('Snapshot storage guard cannot be verified') from None


def _code(error):
    value = getattr(error, 'response', {})
    return value.get('Error', {}).get('Code') if type(value) is dict else None


def _etag(value):
    if type(value) is not str or not re.fullmatch(r'"[^"\r\n\x00-\x1f]{1,200}"', value):
        raise OrderingUnavailable('Opaque storage representation identity required')
    return value


def _rank(value):
    return type(value) is int and 1 <= value <= MAX_REVISION


def publication(value):
    if type(value) is not dict or value.get('schema_version') != PROTOCOL or not _rank(value.get('revision')):
        raise OrderingUnavailable('Complete snapshot publication identity required')
    stamp = value.get('started_at')
    if type(stamp) is not str or not re.fullmatch(r'\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?(?:Z|\+00:00)',stamp):
        raise OrderingUnavailable('UTC snapshot acquisition timestamp required')
    try:datetime.fromisoformat(stamp.replace('Z','+00:00'))
    except ValueError:raise OrderingUnavailable('Valid snapshot acquisition timestamp required') from None
    return value['revision']


def read_current(s3, validate, key=KEY, context=None):
    """Complete bounded read, with no range/sample/default for an unread body."""
    deadline = wire._deadline(context)
    try:
        response = s3.get_object(Bucket=BUCKET,Key=key,ExpectedBucketOwner=ACCOUNT)
    except Exception as error:
        if _code(error) == 'NoSuchKey':return None
        raise OrderingUnavailable('Snapshot storage read unavailable') from None
    body = response.get('Body') if type(response) is dict else None
    try:
        _response(response)
        etag = _etag(response.get('ETag'))
        size = response.get('ContentLength')
        if type(size) is not int or not 0 < size <= wire.MAX_BYTES or response.get('ContentEncoding','identity') not in ('','identity'):
            raise OrderingUnavailable('Complete snapshot storage framing required')
        pieces, received = [], 0
        while True:
            wire._within(deadline)
            part = body.read(min(65536, size + 1 - received))
            wire._within(deadline)
            if type(part) is not bytes:raise OrderingUnavailable('Snapshot storage body is not bytes')
            if not part:break
            received += len(part)
            if received > size:raise OrderingUnavailable('Snapshot storage body exceeds declared size')
            pieces.append(part)
        if received != size:raise OrderingUnavailable('Snapshot storage body is incomplete')
        raw = b''.join(pieces)
        doc = strict_document(raw, wire.MAX_BYTES)
        identity = validate(doc)
        if type(identity) is not int or not 0 < identity <= wire.MAX_IDENTITY_BYTES:
            raise OrderingUnavailable('Snapshot storage typed identity is invalid')
        rank = publication(doc['publication']) if 'publication' in doc else 0
        metadata = response.get('Metadata',{})
        if type(metadata) is not dict:raise OrderingUnavailable('Snapshot storage metadata is invalid')
        token = metadata.get(META)
        if key == KEY and rank and (type(token) is not str or not TOKEN.fullmatch(token)):
            raise OrderingUnavailable('Snapshot recovery token is unavailable')
        return {'raw':raw,'etag':etag,'revision':rank,'identity_bytes':identity,'token':token}
    except OrderingUnavailable:raise
    except Exception:raise OrderingUnavailable('Complete snapshot storage read failed') from None
    finally:
        if body is not None:
            try:body.close()
            except Exception:pass


def request(method, raw, token=None, context=None):
    """Use the reviewed origin and bounded strict acknowledgement transport."""
    if method not in ('POST','PUT') or type(raw) is not bytes or not 0 < len(raw) <= wire.MAX_BYTES:
        raise OrderingUnavailable('Complete snapshot protocol request required')
    if wire.os.environ.get('PRIVATE_ARTIFACT_PROXY',wire.ORIGIN).rstrip('/') != wire.ORIGIN:
        raise OrderingUnavailable('Reviewed snapshot publication origin required')
    if method == 'PUT' and (type(token) is not str or not TOKEN.fullmatch(token)):
        raise OrderingUnavailable('Issued snapshot reservation token required')
    deadline = wire._deadline(context)
    url = RESERVE_URL if method == 'POST' else wire.URL
    try:
        headers = {**wire.service_headers(),'Content-Type':'application/json','Accept-Encoding':'identity'}
        if method == 'PUT':headers.update({'X-JH-Publication-Token':token,'X-JH-Body-SHA256':hashlib.sha256(raw).hexdigest()})
        wire._within(deadline)
        req = urllib.request.Request(url,data=raw,method=method,headers=headers)
        try:response = urllib.request.build_opener(wire.NoRedirect()).open(req,timeout=min(8,deadline-time.monotonic()))
        except urllib.error.HTTPError as error:response = error
        with response:
            value = wire._acknowledgement(response,deadline,expected_url=url,statuses=(200,409))
            if response.status == 409:
                code = value.get('error') if type(value) is dict and value.get('ok') is False else None
                if code in ('superseded_publication','unknown_publication_reservation'):
                    raise MirrorConflict(code)
                raise OrderingUnavailable('Snapshot mirror revision conflict')
        wire._within(deadline)
        return value
    except wire.SnapshotPublicationUnavailable:raise
    except Exception:raise OrderingUnavailable('Snapshot protocol acknowledgement unavailable') from None


def acknowledge(raw, identity, rank, token, context=None):
    ack = request('PUT',raw,token,context)
    if (type(ack) is not dict or ack.get('ok') is not True or ack.get('protocol') != PROTOCOL or
            type(ack.get('revision')) is not int or ack['revision'] != rank or
            type(ack.get('body_bytes')) is not int or ack['body_bytes'] != len(raw) or
            type(ack.get('identity_bytes')) is not int or ack['identity_bytes'] != identity or
            ack.get('body_sha256') != hashlib.sha256(raw).hexdigest() or ack.get('status') not in ('published','unchanged')):
        raise OrderingUnavailable('Exact ordered snapshot was not acknowledged')
    return ack


def begin_snapshot_publication(s3, validate, context=None):
    """Guard, recover the exact pending body, then reserve before collection."""
    storage_guard(s3)
    current = read_current(s3,validate,context=context)
    recovery = 'none'
    if current and current['revision']:
        try:
            acknowledge(current['raw'],current['identity_bytes'],current['revision'],current['token'],context)
            recovery = 'acknowledged'
        except MirrorConflict as error:
            # Expired reservations are never relabeled to make old observations
            # look new. A fresh collection may obtain a genuinely new attempt.
            recovery = error.code
    minimum = current['revision'] if current else 0
    ack = request('POST',wire.encode_snapshot({'minimum_revision':minimum}),context=context)
    if (type(ack) is not dict or ack.get('ok') is not True or ack.get('protocol') != PROTOCOL or
            not _rank(ack.get('revision')) or ack['revision'] <= minimum or
            type(ack.get('token')) is not str or not TOKEN.fullmatch(ack['token'])):
        raise OrderingUnavailable('Snapshot acquisition reservation was not acknowledged')
    return {'frame':{'schema_version':PROTOCOL,'revision':ack['revision'],'started_at':datetime.now(timezone.utc).isoformat()},
            'token':ack['token'],'prior_mirror_recovery':recovery}


def _retain(s3,current,validate,context):
    raw = current['raw']
    key = ARCHIVE + hashlib.sha256(raw).hexdigest() + '.json'
    try:
        s3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType='application/json',CacheControl='private, no-store',
                      ExpectedBucketOwner=ACCOUNT,IfNoneMatch='*')
    except Exception:
        # A lost acknowledgement, conflict, or unexpected service failure can
        # only be resolved by the complete retained bytes, never blind retry.
        pass
    actual = read_current(s3,validate,key,context)
    if actual is None or actual['raw'] != raw:
        raise OrderingUnavailable('Whole previous snapshot was not retained')


def finish_snapshot_publication(s3,validate,attempt,raw,identity,context=None):
    if type(attempt) is not dict:raise OrderingUnavailable('Issued acquisition attempt required')
    rank = publication(attempt.get('frame'))
    token = attempt.get('token')
    if type(token) is not str or not TOKEN.fullmatch(token):raise OrderingUnavailable('Issued acquisition token required')
    doc = strict_document(raw,wire.MAX_BYTES)
    if doc.get('publication') != attempt['frame'] or validate(doc) != identity or type(identity) is not int:
        raise OrderingUnavailable('Candidate differs from issued acquisition attempt')
    del doc
    storage_guard(s3)
    def result(status):
        return {'published':status == 'published','status':status,'revision':rank,
                'prior_mirror_recovery':attempt.get('prior_mirror_recovery','none')}
    for _ in range(4):
        current = read_current(s3,validate,context=context)
        if current and current['revision'] > rank:return result('superseded_before_s3')
        if current and current['revision'] == rank:
            if current['raw'] != raw or current['token'] != token:
                raise OrderingUnavailable('Snapshot revision body conflict')
            break
        if current:_retain(s3,current,validate,context)
        condition = {'IfMatch':current['etag']} if current else {'IfNoneMatch':'*'}
        try:
            wire._deadline(context)
            s3.put_object(Bucket=BUCKET,Key=KEY,Body=raw,Metadata={META:token},
                          ContentType='application/json',CacheControl='private, no-store',ExpectedBucketOwner=ACCOUNT,**condition)
        except Exception as error:
            if _code(error) in ('PreconditionFailed','ConditionalRequestConflict','412','409'):continue
            # Resolve an ambiguous PutObject without changing candidate bytes.
            actual = read_current(s3,validate,context=context)
            if actual is None or actual['raw'] != raw or actual['token'] != token:
                raise OrderingUnavailable('Snapshot conditional write is unconfirmed') from None
        break
    else:raise OrderingUnavailable('Snapshot conditional write contention exceeded bound')
    actual = read_current(s3,validate,context=context)
    if actual is None or actual['raw'] != raw or actual['token'] != token:return result('superseded_after_s3')
    try:acknowledge(raw,identity,rank,token,context)
    except MirrorConflict as error:return result('mirror_' + error.code)
    except wire.SnapshotPublicationUnavailable:
        # S3 holds complete bytes and token for the next original invocation.
        return result('s3_committed_mirror_unconfirmed')
    actual = read_current(s3,validate,context=context)
    if actual is None or actual['raw'] != raw or actual['token'] != token:return result('superseded_after_mirror')
    return result('published')
