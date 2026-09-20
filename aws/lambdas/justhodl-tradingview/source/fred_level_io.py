"""Source capture and conditional publication for the FRED slice of the vault."""
from datetime import datetime, timezone
from pathlib import Path
import copy
import json
import time
import urllib.parse
import urllib.request
from evidence_store import capture, read_verified
import fred_level_model as model

MAX_BYTES = 32*1024*1024


def body(response):
    stream = response['Body']
    try: raw = stream.read(MAX_BYTES+1)
    finally: stream.close()
    if len(raw) > MAX_BYTES: raise ValueError('complete object exceeds bound')
    return raw


def immutable(s3, bucket, key, raw, private=False):
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=raw, IfNoneMatch='*',
                      ContentType='application/octet-stream' if private else 'application/json',
                      CacheControl='no-store' if private else 'public,max-age=31536000,immutable')
    except Exception as exc:
        if getattr(exc, 'response', {}).get('Error', {}).get('Code') not in ('PreconditionFailed', '412', 'ConditionalRequestConflict', '409'): raise
    if body(s3.get_object(Bucket=bucket, Key=key)) != raw: raise ValueError('retained object readback differs')
    return {'key': key, 'sha256': model.digest(raw), 'bytes': len(raw)}


class Collector:
    def __init__(self, s3, bucket, api_key, now=None, opener=None, sleep=time.sleep):
        self.s3, self.bucket, self.api_key = s3, bucket, api_key
        self.now = now or datetime.now(timezone.utc)
        self.opener, self.sleep = opener or urllib.request.urlopen, sleep
        self.memo, self.failures, self.requests = {}, {}, 0
        self.compiler = None
        self.seeds = {}
        self.reused = set()

    def seed(self, rows):
        for row in rows:
            if row.get('contract_version') == model.CONTRACT and isinstance(row.get('replay'), dict):
                sid = row.get('series_id')
                try:
                    age = (self.now-model.clock(row['fetched_at'])).total_seconds()
                    if not 0 <= age < 20*3600: continue
                except (KeyError, TypeError, ValueError): continue
                old = self.seeds.get(sid)
                if old is None or row['fetched_at'] > old['fetched_at']: self.seeds[sid] = row

    def retained(self, row):
        def verified(ref):
            if not isinstance(ref.get('key'), str) or not ref['key'].startswith(model.PREFIX): raise ValueError('native reference required')
            raw = body(self.s3.get_object(Bucket=self.bucket, Key=ref['key']))
            if len(raw) != ref['bytes'] or model.digest(raw) != ref['sha256']: raise ValueError('reference differs')
            return raw
        manifest = json.loads(verified(row['replay']))
        if manifest.get('contract') != 'fred-native-level-replay.v1' or manifest['series_id'] != row['series_id']: raise ValueError('identity differs')
        if verified(manifest['compiler']) != Path(model.__file__).read_bytes(): raise ValueError('reviewed compiler differs')
        originals = {}
        for kind in ('observations', 'definition'):
            ref = manifest['evidence'][kind]
            if not ref['key'].startswith('data/evidence/fred/') or ref.get('provider') != 'fred': raise ValueError('FRED source required')
            originals[kind] = read_verified(self.s3, self.bucket, ref)
        out = model.compile_level(manifest['series_id'], originals['observations'], originals['definition'], manifest['collected_at'])
        if model.digest(model.encoded(out)) != manifest['output_sha256']: raise ValueError('source replay differs')
        age = (self.now-model.clock(out['fetched_at'])).total_seconds()
        if not 0 <= age < 20*3600: raise ValueError('collection cache expired')
        limit = out['quality']['max_observation_age_days']
        if limit is None or not 0 <= (self.now.date()-datetime.fromisoformat(out['observation_date']).date()).days <= limit:
            raise ValueError('observation age ceiling exceeded')
        return {**out, 'evidence': manifest['evidence'], 'replay': row['replay']}

    def get(self, sid, allowed=True):
        if sid in self.memo: return copy.deepcopy(self.memo[sid])
        self.memo[sid] = None
        if sid in self.seeds:
            try:
                self.memo[sid] = self.retained(self.seeds[sid])
                self.reused.add(sid)
                return copy.deepcopy(self.memo[sid])
            except Exception: pass  # Invalid cache never reaches consumers; fetch originals if budget allows.
        if not allowed:
            self.failures[sid] = 'runtime_budget'; return None
        try:
            refs, originals = {}, {}
            for kind, endpoint in (('observations', 'series/observations'), ('definition', 'series')):
                query = {'series_id': sid, 'api_key': self.api_key, 'file_type': 'json',
                         'realtime_start': self.now.date().isoformat(), 'realtime_end': self.now.date().isoformat()}
                if kind == 'observations': query.update(sort_order='desc', limit=32, offset=0, units='lin', output_type=1)
                url = 'https://api.stlouisfed.org/fred/'+endpoint+'?'+urllib.parse.urlencode(query)
                self.sleep(0.55); self.requests += 1
                req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl-FRED-native/1.0'})
                with self.opener(req, timeout=15) as response: raw = response.read(4*1024*1024+1)
                if len(raw) > 4*1024*1024: raise ValueError('source response exceeds bound')
                refs[kind] = capture(self.s3, self.bucket, 'fred', url, raw)
                if read_verified(self.s3, self.bucket, refs[kind]) != raw: raise ValueError('source readback differs')
                originals[kind] = raw
            observation = model.compile_level(sid, originals['observations'], originals['definition'], self.now.isoformat())
            if self.compiler is None:
                compiler = Path(model.__file__).read_bytes()
                self.compiler = immutable(self.s3, self.bucket, model.PREFIX+'compilers/'+model.digest(compiler)+'.py', compiler)
            manifest = {'contract': 'fred-native-level-replay.v1', 'series_id': sid, 'collected_at': self.now.isoformat(),
                        'evidence': refs, 'compiler': self.compiler, 'output_sha256': model.digest(model.encoded(observation))}
            manifest_ref = immutable(self.s3, self.bucket, model.PREFIX+'runs/'+model.digest(model.encoded(manifest))+'.json', model.encoded(manifest))
            observation['evidence'] = refs
            observation['replay'] = manifest_ref
            self.memo[sid] = observation
            return copy.deepcopy(observation)
        except Exception as exc:
            # URLs/SDK/provider text may contain secrets. Fixed exception type only.
            self.failures[sid] = type(exc).__name__
            return None


def publish(s3, bucket, key, packet, previous_raw, previous_etag):
    """Preserve the complete predecessor, then CAS the mutable vault pointer."""
    if previous_raw is not None:
        immutable(s3, bucket, 'audit-private/20260909-originals/fred-vault/'+model.digest(previous_raw)+'.bin', previous_raw, private=True)
    raw = model.encoded(packet)
    if previous_raw == raw: return
    condition = {'IfMatch': previous_etag} if previous_etag else {'IfNoneMatch': '*'}
    s3.put_object(Bucket=bucket, Key=key, Body=raw, ContentType='application/json', CacheControl='no-store', **condition)


def refresh_public(s3, bucket, key, collector, aliases, context=None, selected=None):
    """Refresh only known public FRED rows; never load Brain or any private inputs."""
    response = s3.get_object(Bucket=bucket, Key=key)
    raw = body(response); packet = json.loads(raw)
    rows = packet['symbols']
    if not isinstance(rows, list) or not rows: raise ValueError('existing public catalog required')
    groups = {}
    for row in rows:
        sid = model.series_for(row, aliases)
        if sid: groups.setdefault(sid, []).append(row)
    if selected is not None:
        if not isinstance(selected, list) or not selected or len(selected) > 64 or any(not isinstance(sid, str) or sid not in groups for sid in selected):
            raise ValueError('select at most 64 existing public FRED series')
        groups = {sid: groups[sid] for sid in sorted(set(selected))}
    else:
        # Rotate by attempt time, including failures, so one retired series cannot starve later roots.
        def last_attempt(sid):
            stamps = []
            for row in groups[sid]:
                try:
                    stamp = model.clock(row['native_fred_attempted_at'])
                    if stamp <= collector.now: stamps.append(stamp.isoformat())
                except (KeyError, ValueError, TypeError): pass
            return max(stamps) if stamps else ''
        groups = {sid: groups[sid] for sid in sorted(groups, key=lambda sid:(last_attempt(sid),sid))[:64]}
    start = time.monotonic(); updated = []
    for sid in sorted(groups):
        allowed = time.monotonic()-start < 420
        if context is not None: allowed = allowed and context.get_remaining_time_in_millis() >= 90000
        observation = collector.get(sid, allowed=allowed)
        for row in groups[sid]:
            if observation: model.merge_observation(row, observation)
            else: model.mark_unavailable(row, sid)
            row['native_fred_attempted_at'] = collector.now.isoformat()
        if observation: updated.append(sid)
    # Partial publication does not renew the overall catalog collection clock.
    packet['fred_level_refresh'] = {'contract': model.CONTRACT, 'generated_at': collector.now.isoformat(),
        'updated_series': updated, 'failures': collector.failures, 'source_requests': collector.requests,
        'scope': 'FRED level rows only; other catalog values and collection dates unchanged'}
    counts = {}
    for row in rows: counts[row['status']] = counts.get(row['status'], 0)+1
    packet.update(status_counts=counts, n_live=counts.get('LIVE', 0), n_pending=counts.get('PENDING_RESOLUTION', 0),
                  n_unresolved=counts.get('NO_FREE_SOURCE', 0), coverage_pct=round(100*counts.get('LIVE', 0)/len(rows), 1))
    publish(s3, bucket, key, packet, raw, response['ETag'])
    return {'ok': not collector.failures, **packet['fred_level_refresh'], 'updated_rows': sum(len(groups[sid]) for sid in updated)}
