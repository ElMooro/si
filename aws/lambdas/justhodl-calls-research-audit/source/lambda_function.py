"""Independently replay the published public brief on a recurring AWS schedule.

No private sources, provider calls, model APIs, notifications or capital changes.
A successful proof verifies this compiler run, not predictive accuracy.
"""
import hashlib
import json
import os
import re
from datetime import datetime, timezone
import boto3
from calls_contract import timestamp
from calls_research_replay import canonical, replay, publish_current
from calls_original_reader import reader as original_reader

BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
s3 = boto3.client('s3', region_name='us-east-1')


def read_document(client, bucket, key):
    """Read complete stored bytes; ambiguous JSON cannot earn an audit proof."""
    response = client.get_object(Bucket=bucket, Key=key)
    body = response['Body']
    try:
        parts = []
        while True:
            chunk = body.read(65536)
            if not isinstance(chunk, bytes): raise ValueError('Stored response must contain bytes')
            if not chunk: break
            parts.append(chunk)
        raw = b''.join(parts)
        length = response.get('ContentLength')
        if length is not None and (type(length) is not int or length < 0 or length != len(raw)):
            raise ValueError('Stored response length mismatch')
        def pairs(rows):
            out = {}
            for key, value in rows:
                if key in out: raise ValueError('Duplicate JSON key')
                out[key] = value
            return out
        def invalid(value): raise ValueError('Nonfinite JSON constant')
        doc = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_constant=invalid)
        if not isinstance(doc, dict): raise ValueError('Stored document must be an object')
        canonical(doc)  # Reject numeric overflow and unencodable Unicode too.
        return raw, doc
    finally:
        body.close()


def verify_current(client, bucket, now=None):
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None or now.utcoffset() is None: raise ValueError('Aware audit clock required')
    public_raw, public = read_document(client, bucket, 'data/ai-brief-public.json')
    if public.get('decision_eligible', False) is not False:
        raise ValueError('Public research cannot acquire allocation authority')
    ref = public.get('research_replay') or {}
    sha = ref.get('payload_sha256')
    if not isinstance(sha, str) or not re.fullmatch('[a-f0-9]{64}', sha):
        raise ValueError('public brief lacks a supported replay record')
    key = 'data/calls-research-runs/'+sha+'.json'
    if ref.get('bundle_key') != key or ref.get('run_id') != 'calls-research-'+sha:
        raise ValueError('replay pointer identity mismatch')
    raw, bundle = read_document(client, bucket, key)
    if hashlib.sha256(raw).hexdigest() != ref.get('bundle_sha256'):
        raise ValueError('retained record bytes differ from the published hash')
    result = replay(bundle,read_original=original_reader(client,bucket))
    if bundle['payload_sha256'] != sha: raise ValueError('retained payload differs from its key')
    output = bundle['payload']['output']
    metadata = {'research_replay', 'snapshot_id', 'decision_status', 'decision_reason', 'decision_eligible'}
    if set(output) - set(public) or set(public) - set(output) - metadata:
        raise ValueError('Public brief contains fields outside retained output and decision metadata')
    if any(canonical(public.get(k)) != canonical(value) for k, value in output.items()):
        raise ValueError('current public brief differs from retained compiler output')
    _, history = read_document(client, bucket, 'data/decisive-call-history.json')
    rows = history.get('snapshots')
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError('Decision history snapshots must be objects')
    matches = [row for row in rows if row.get('snapshot_id') == public.get('snapshot_id')]
    row = matches[0] if len(matches) == 1 else None
    if not row or row.get('research_replay') != ref:
        raise ValueError('decision history does not bind this research run')
    expected_brief_hash = hashlib.sha256(output['brief_md'].encode()).hexdigest()
    if row.get('brief_sha256') != expected_brief_hash or row.get('evidence_ids') != [r['evidence_id'] for r in output['evidence']]:
        raise ValueError('decision history evidence or brief hash mismatch')
    if row.get('call_verb') != 'WAIT' or row.get('sizing_eligible') is not False or row.get('decision_eligible') is not False:
        raise ValueError('public research run improperly received allocation authority')
    if (row.get('decision_status') not in ('ABSTAIN', 'ERROR') or
            row.get('timestamp') != public.get('generated_at') or
            ('decision_reason' in public and row.get('decision_reason') != public['decision_reason']) or
            ('decision_status' in public and row['decision_status'] != public['decision_status'])):
        raise ValueError('decision history status or clock mismatch')
    generated = timestamp(public.get('generated_at'))
    age = (now-generated).total_seconds()/3600 if generated else None
    return {**result, 'schema_version': 'calls-research-replay-proof.v1', 'generated_at': now.isoformat(),
            'payload_sha256': sha, 'bundle_sha256': ref['bundle_sha256'], 'snapshot_id': row['snapshot_id'],
            'brief_sha256': expected_brief_hash, 'publication_age_hours': age,
            'decision_eligible': False, 'publication_generated_at': public.get('generated_at'),
            'public_object': {'key': 'data/ai-brief-public.json', 'sha256': hashlib.sha256(public_raw).hexdigest(), 'bytes': len(public_raw)},
            'publication_overdue': age is None or age < -5/60 or age > 4.5,
            'compiler': bundle['payload']['compiler'], 'private_account_data_read': False,
            'meaning': 'Reproduces public research and its history binding. Included liquidity, FR2004, ECB and TIC originals are replayed only when the frozen run records successful capture; other source families, forecasts and portfolio sizing remain unvalidated.'}


def lambda_handler(event, context):
    try:
        proof = verify_current(s3, BUCKET)
        sha = proof['payload_sha256']
        event_id = hashlib.sha256(canonical(proof)).hexdigest()
        s3.put_object(Bucket=BUCKET, Key='data/calls-research-audit-events/'+sha+'/'+event_id+'.json',
                      Body=canonical(proof), ContentType='application/json', IfNoneMatch='*')
        publish_current(s3, BUCKET, 'data/calls-research-proofs/'+sha+'.json', proof)
        publish_current(s3, BUCKET, 'data/calls-research-audit.json', proof)
        return {'statusCode': 200, 'body': json.dumps(proof)}
    except Exception as exc:
        failure = {'schema_version': 'calls-research-replay-proof.v1', 'status': 'failed',
                   'generated_at': datetime.now(timezone.utc).isoformat(), 'sizing_eligible': False,
                   'reason': type(exc).__name__+': '+str(exc)[:200]}
        # Retire any earlier successful badge for this run when a later check
        # fails. A last-good proof must not hide a changed or corrupted record.
        try:
            _, public = read_document(s3, BUCKET, 'data/ai-brief-public.json')
            ref = public.get('research_replay') or {}
            sha = ref.get('payload_sha256')
            if isinstance(sha, str) and re.fullmatch('[a-f0-9]{64}', sha):
                failure.update(payload_sha256=sha, run_id=ref.get('run_id'), bundle_sha256=ref.get('bundle_sha256'))
                publish_current(s3, BUCKET, 'data/calls-research-proofs/'+sha+'.json', failure)
        except Exception:
            pass
        publish_current(s3, BUCKET, 'data/calls-research-audit.json', failure)
        raise RuntimeError(failure['reason']) from exc
