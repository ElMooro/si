"""Whole-history preservation and conditional publication for Boom Stage.

This protects stored research, not original provider vintages or model accuracy.
Two conditional writes are not an atomic transaction; no rollback overwrites a
concurrent writer. Complete planned bytes and predecessors remain recoverable.
"""
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import re

HISTORY = 'boom/boom-stage-history.json'
HEAD = 'data/boom-stage.json'
PRIVATE = 'audit-private/20260909-originals/boom-history-research/'
LIMIT = 64 * 1024 * 1024
FLAGS = ('forecast_qualified', 'calls_eligible', 'sizing_eligible', 'execution_eligible')


class IntegrityError(RuntimeError):
    pass


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def encode(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')


def decode(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise IntegrityError('Duplicate JSON key')
            result[key] = value
        return result
    try:
        result = json.loads(raw.decode('utf-8'), object_pairs_hook=pairs,
                            parse_constant=lambda _: (_ for _ in ()).throw(IntegrityError('Nonfinite value')))
        encode(result)
        return result
    except Exception:
        raise IntegrityError('Invalid complete JSON') from None


def bounded(body):
    chunks, size = [], 0
    try:
        while True:
            chunk = body.read(min(65536, LIMIT + 1 - size))
            if not chunk:
                return b''.join(chunks)
            size += len(chunk)
            if size > LIMIT:
                raise IntegrityError('Complete object exceeds byte bound')
            chunks.append(chunk)
    finally:
        body.close()


def stamp(value):
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if not parsed.tzinfo or 'T' not in value:
            raise ValueError()
        return parsed.astimezone(timezone.utc)
    except (ValueError, TypeError, AttributeError, OverflowError):
        raise IntegrityError('Aware publication timestamp required') from None


def date(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise IntegrityError('Exact calendar date required')
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except ValueError:
        raise IntegrityError('Invalid calendar date') from None


def retain(client, bucket, raw):
    if not isinstance(raw, bytes) or len(raw) > LIMIT:
        raise IntegrityError('Complete bounded bytes required')
    ref = {'key': PRIVATE + sha(raw) + '.bin', 'sha256': sha(raw), 'bytes': len(raw)}
    try:
        client.put_object(Bucket=bucket, Key=ref['key'], Body=raw, IfNoneMatch='*',
                          ContentType='application/octet-stream', CacheControl='no-store')
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) not in ('409', '412', 'ConditionalRequestConflict', 'PreconditionFailed'):
            raise IntegrityError('Whole original retention failed') from None
    obj = client.get_object(Bucket=bucket, Key=ref['key'])
    back = bounded(obj['Body'])
    if type(obj.get('ContentLength')) is not int or obj['ContentLength'] != len(back) or back != raw:
        raise IntegrityError('Whole retained readback differs')
    return ref


def read(client, bucket, key):
    if key not in (HISTORY, HEAD):
        raise IntegrityError('Only reviewed predecessor objects may be read')
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if str(getattr(exc, 'response', {}).get('Error', {}).get('Code')) in ('NoSuchKey', '404'):
            return None
        raise IntegrityError('Predecessor read failed; cannot initialize empty history') from None
    raw = bounded(obj['Body'])
    if type(obj.get('ContentLength')) is not int or obj['ContentLength'] != len(raw) or not obj.get('ETag'):
        raise IntegrityError('Complete versioned predecessor required')
    # Even malformed predecessors must be retained before interpretation fails.
    original = retain(client, bucket, raw)
    return {'raw': raw, 'etag': obj['ETag'], 'original': original}


class Ledger:
    def __init__(self, client, bucket, generated_at):
        self.client, self.bucket = client, bucket
        self.generated_at = generated_at
        self.at = stamp(generated_at)
        self.old_history = read(client, bucket, HISTORY)
        self.old_head = read(client, bucket, HEAD)
        self.history = decode(self.old_history['raw']) if self.old_history else {'days': {}}
        if not isinstance(self.history, dict) or not isinstance(self.history.get('days'), dict):
            raise IntegrityError('Complete history dictionary required')
        for day, rows in self.history['days'].items():
            if date(day) > self.at.date() or not isinstance(rows, dict):
                raise IntegrityError('Malformed or future history date')
        self.prior_days = deepcopy(self.history['days'])
        self.prior_metadata = deepcopy({k: v for k, v in self.history.items() if k != 'days'})
        if self.old_head:
            head = decode(self.old_head['raw'])
            if not isinstance(head, dict) or stamp(head.get('generated_at')) >= self.at:
                raise IntegrityError('Predecessor head is malformed or newer than this run')
        self.pending = None

    def append(self, pairs):
        if self.pending is not None:
            raise IntegrityError('One complete daily calculation required')
        today = self.at.date().isoformat()
        if today in self.history['days']:
            raise IntegrityError('This research date already exists; preserve it unchanged')
        if not isinstance(pairs, list) or not pairs:
            raise IntegrityError('Complete nonempty pair population required')
        result = {}
        for pair in pairs:
            if not isinstance(pair, dict) or not isinstance(pair.get('id'), str) or not pair['id'] or pair['id'] in result:
                raise IntegrityError('Unique identified pairs required')
            if not isinstance(pair.get('stage'), str) or not isinstance(pair.get('value'), dict) or not isinstance(pair.get('volume'), dict):
                raise IntegrityError('Complete native pair required')
            result[pair['id']] = {'v': pair['value'].get('yoy_pct'),
                                  'vol': pair['volume'].get('vs_baseline_pct'), 'stage': pair['stage']}
        self.history['days'][today] = result
        self.pending = deepcopy(result)
        return self.history

    def prepare(self, packet):
        if self.pending is None or not isinstance(packet, dict) or packet.get('generated_at') != self.generated_at:
            raise IntegrityError('Planned history and output timestamp required')
        today = self.at.date().isoformat()
        if ({k: v for k, v in self.history['days'].items() if k != today} != self.prior_days
                or self.history['days'].get(today) != self.pending
                or {k: v for k, v in self.history.items() if k != 'days'} != self.prior_metadata):
            raise IntegrityError('An original history row was changed or deleted')
        published_pairs = packet.get('pairs')
        if not isinstance(published_pairs, list) or len(published_pairs) != len(self.pending):
            raise IntegrityError('Output and history pair populations differ')
        seen = set()
        for row in published_pairs:
            if (not isinstance(row, dict) or not isinstance(row.get('id'), str)
                    or row['id'] in seen or row['id'] not in self.pending
                    or not isinstance(row.get('value'), dict) or not isinstance(row.get('volume'), dict)):
                raise IntegrityError('Output pair identity differs')
            seen.add(row['id'])
            point = {'v': row['value'].get('yoy_pct'), 'vol': row['volume'].get('vs_baseline_pct'), 'stage': row.get('stage')}
            if point != self.pending[row['id']]:
                raise IntegrityError('Output and history calculation differ')
        output = deepcopy(packet)
        # Protect evidence without granting authority to inherited heuristic labels.
        output.update(**dict.fromkeys(FLAGS, False), portfolio_action='WAIT')
        history_raw = encode(self.history)
        original_calculation = retain(self.client, self.bucket, encode(packet))
        planned_history = retain(self.client, self.bucket, history_raw)
        compilers = {name: sha((Path(__file__).parent/name).read_bytes())
                     for name in ('lambda_function.py', 'boom_history.py', 'boom_measurements.py')}
        shared = Path(__file__).parent/'managed_secret.py'
        if not shared.exists():
            shared = Path(__file__).resolve().parents[3]/'shared/managed_secret.py'
        compilers['managed_secret.py'] = sha(shared.read_bytes())
        output['history_preservation'] = {
            'contract': 'boom-whole-history.v1', 'compiler_sha256': compilers,
            'previous_dates': len(self.prior_days), 'planned_dates': len(self.history['days']),
            'predecessor_history': self.old_history['original'] if self.old_history else None,
            'predecessor_head': self.old_head['original'] if self.old_head else None,
            'complete_native_calculation': original_calculation, 'complete_planned_history': planned_history,
            'publication_atomic': False, 'provider_originals_replayed': False,
            'point_in_time_verified': False, 'forecast_qualified': False,
            'scope': 'Stored predecessors and complete planned research only. Legacy stages, trades, probabilities and source comparisons remain unqualified.'}
        raw = encode(output)
        planned_head = retain(self.client, self.bucket, raw)
        attempt = retain(self.client, self.bucket, encode({'contract': 'boom-history-publication-attempt.v1',
            'status': 'planned_bytes_only', 'history': planned_history, 'head': planned_head,
            'predecessors': {HISTORY: self.old_history['original'] if self.old_history else None,
                             HEAD: self.old_head['original'] if self.old_head else None}}))
        for key, previous in ((HISTORY, self.old_history), (HEAD, self.old_head)):
            current = read(self.client, self.bucket, key)
            if (current is None) != (previous is None) or current and (current['etag'] != previous['etag'] or current['raw'] != previous['raw']):
                raise IntegrityError('Predecessor changed during research calculation')
        return {'history_raw': history_raw, 'head_raw': raw,
                'evidence': {'attempt': attempt, 'head': planned_head, 'history': planned_history}}
