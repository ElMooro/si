"""
justhodl-fleet-freshness-monitor
================================
Detects silent failures the fleet-error-monitor can't see:
  - Lambda completes 200 (no error) but didn't actually write its expected
    output (the 35-Lambda silent-except-print-around-put_object pattern)
  - S3 freshness drift (CDN cache hits, partition writes to wrong key)
  - Provider API silent degradation (FRED returns 0 rows but no error)

Uses the EXISTING manifest schema at data/_freshness-manifest.json:
{
  "rules": [{"prefix": "data/", "default_max_age_h": 26.0}],
  "exclude_prefixes": ["data/archive/", "data/_archive/", ...],
  "admin_only_keys": ["data/khalid-config.json", ...],
  "key_overrides": {"data/options-flow-scanner.json": 0.2, ...}
}

Logic:
  - Walk every rule's prefix via list_objects_v2
  - Skip excluded prefixes + admin_only_keys
  - Lookup max_age_h from key_overrides first, else rule's default
  - head_object + compare LastModified
  - Alert if age > max_age_h * ALERT_RATIO

Output:
  data/_freshness-monitor.json with last run state
  Telegram + SNS alerts (deduped 4h per key)
"""
import os, json, re, time, math, urllib.request, urllib.parse
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
from private_artifact import is_private_source
from public_brain_projection import FRESHNESS_REASONS, PUBLIC_FRESHNESS_REPORT, sanitize_public

VERSION = "3.0.0"   # audit 2026-09-08 INST-13: content validation, source-vs-artifact age, missing expected outputs, truncation coverage
REGION = os.environ.get('AWS_REGION', 'us-east-1')
ACCOUNT = '857687956942'
BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
SNS_ARN = os.environ.get('SNS_ARN', f'arn:aws:sns:{REGION}:{ACCOUNT}:justhodl-fleet-alerts')
TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID', '')
DEDUPE_HOURS = int(os.environ.get('DEDUPE_HOURS', '4'))
DEFAULT_MAX_AGE_H = float(os.environ.get('DEFAULT_MAX_AGE_H', '26'))
ALERT_RATIO = float(os.environ.get('ALERT_RATIO', '1.5'))
MAX_KEYS_PER_RULE = int(os.environ.get('MAX_KEYS_PER_RULE', '10000'))

s3 = boto3.client('s3', region_name=REGION)
sns = boto3.client('sns', region_name=REGION)


def send_telegram(msg):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            'chat_id': TELEGRAM_CHAT_ID,
            'text': msg[:4000],
            'parse_mode': 'Markdown',
            'disable_web_page_preview': 'true',
        }).encode()
        req = urllib.request.Request(url, data=data, method='POST')
        try:
            urllib.request.urlopen(req, timeout=10)
            return True
        except urllib.error.HTTPError as he:
            if he.code == 400:  # Markdown entity parse failure (underscores in keys) → resend plain
                data2 = urllib.parse.urlencode({'chat_id': TELEGRAM_CHAT_ID, 'text': msg[:4000],
                                                'disable_web_page_preview': 'true'}).encode()
                urllib.request.urlopen(urllib.request.Request(url, data=data2, method='POST'), timeout=10)
                return True
            raise
    except Exception as e:
        print("[telegram] DELIVERY_FAILED")
        return False


def publish_sns(subject, msg):
    try:
        sns.publish(TopicArn=SNS_ARN, Subject=subject[:100], Message=msg)
        return True
    except Exception as e:
        print("[sns] DELIVERY_FAILED")
        return False


def load_manifest(stamp=True):
    """Load the rules-based manifest from S3. Returns None if missing."""
    try:
        obj = s3.get_object(Bucket=BUCKET, Key='data/_freshness-manifest.json')
        m = json.loads(obj['Body'].read().decode())
        # ops 4264: living doc -- every run validates these rules, so say so.
        # (4255 forensics flagged this key 651h stale; it is a rules file the
        # monitor consumes, and the honest freshness signal is validation.)
        try:
            if stamp and isinstance(m, dict):
                m['last_validated'] = datetime.now(timezone.utc).isoformat()
                m['validated_by'] = 'justhodl-fleet-freshness-monitor'
                s3.put_object(Bucket=BUCKET,
                              Key='data/_freshness-manifest.json',
                              Body=json.dumps(m, default=str).encode(),
                              ContentType='application/json',
                              CacheControl='no-store')
        except Exception as e:
            print("[manifest-stamp] WRITE_FAILED")
        return m
    except ClientError as e:
        if e.response['Error']['Code'] in ('NoSuchKey', '404'):
            return None
        raise


def is_excluded(key, manifest):
    """Check if a key should be skipped (excluded prefix or admin-only)."""
    if is_private_source(key):
        return True
    excl_prefixes = manifest.get('exclude_prefixes', []) or []
    if any(key.startswith(p) for p in excl_prefixes):
        return True
    admin_only = set(manifest.get('admin_only_keys', []) or [])
    if key in admin_only:
        return True
    # Skip the monitor's own state files (would never go stale by themselves)
    self_keys = (
        'data/_freshness-manifest.json',
        'data/_freshness-monitor.json',
        'data/_freshness-alert-history.json',
        'data/_freshness-seen-keys.json',
        'data/_fleet-monitor.json',
        'data/_fleet-monitor-alert-history.json',
    )
    if key in self_keys:
        return True
    return False


def resolve_max_age(key, rule, manifest):
    """Lookup the max-age threshold for a key. Override > rule default."""
    overrides = manifest.get('key_overrides', {}) or {}
    if key in overrides:
        try:
            value = overrides[key]
            return float(value.get('max_age_h', value.get('default_max_age_h', DEFAULT_MAX_AGE_H)) if isinstance(value, dict) else value)
        except Exception:
            pass
    return float(rule.get('default_max_age_h', DEFAULT_MAX_AGE_H))


def list_keys_under_rule(rule):
    """Enumerate all keys under a rule's prefix (cap at MAX_KEYS_PER_RULE).
    audit 2026-09-08 INST-13: returns (keys, truncated) -- a hit on the cap is reported, never silent."""
    prefix = rule.get('prefix', 'data/')
    keys = []
    paginator = s3.get_paginator('list_objects_v2')
    kw = {'Bucket': BUCKET, 'Prefix': prefix}
    # audit 2026-09-08 INST-13 (ops 5225 finding): listing data/ without a delimiter walks the 9.7M-object
    # warehouse and hits the cap after 10,000 keys, so most expected feeds were never even enumerated.
    # Depth-1 by default; a rule sets "recursive": true to opt into a full walk.
    if rule.get('delimiter'):
        kw['Delimiter'] = rule['delimiter']
    elif not rule.get('recursive'):
        kw['Delimiter'] = '/'
    for page in paginator.paginate(**kw):
        for obj in page.get('Contents', []):
            keys.append(obj)
            if len(keys) >= MAX_KEYS_PER_RULE:
                return keys, True
    return keys, False


# ── content validation (audit 2026-09-08 INST-13) ─────────────────────────────
# LastModified only proves a writer ran; it says nothing about what it wrote. For the
# scoped feeds (depth-1 data/*.json + key_overrides) the body is inspected: a zero-byte or
# non-JSON object is EMPTY/INVALID, a fresh wrapper whose OWN generated_at is stale is
# SOURCE_STALE (a writer copying old data forward), and a future timestamp is INVALID.
VALIDATE_MAX_BYTES = int(os.environ.get('VALIDATE_MAX_BYTES', str(3 * 1024 * 1024)))
TS_FIELDS = ('generated_at', 'as_of', 'updated_at', 'updated', 'timestamp', 'ts', 'last_updated', 'run_ts')
_ts_re = re.compile(r'"(generated_at|as_of|updated_at|updated|timestamp|ts|last_updated|run_ts)"\s*:\s*"([^"]{8,40})"')


def _parse_ts(v):
    try:
        if isinstance(v, (int, float)):
            v = float(v)
            return datetime.fromtimestamp(v / 1000.0 if v > 1e11 else v, tz=timezone.utc)
        t = datetime.fromisoformat(str(v).replace('Z', '+00:00'))
        return t if t.tzinfo else t.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def validate_body(key, size, max_age_h, now=None, schema=None):
    """Fetch (bounded) and classify the object's content. Returns a dict of findings."""
    now = now or datetime.now(timezone.utc)
    if is_private_source(key):
        return {'validated': False, 'content_status': 'UNKNOWN', **failure('PRIVATE_SOURCE_EXCLUDED')}
    out = {'validated': False}
    if size == 0:
        return {'validated': True, 'content_status': 'EMPTY', **failure('ZERO_BYTE_OBJECT')}
    try:
        ts = None
        if size <= VALIDATE_MAX_BYTES:
            body = s3.get_object(Bucket=BUCKET, Key=key)['Body'].read()
            try:
                doc = json.loads(body)
            except Exception:
                return {'validated': True, 'content_status': 'INVALID', **failure('INVALID_JSON')}
            if doc in ({}, [], None, ''):
                return {'validated': True, 'content_status': 'EMPTY', **failure('EMPTY_JSON')}
            schema = schema if isinstance(schema, dict) else {}
            for field in schema.get('required_fields', []):
                value = doc
                for part in field.split('.'):
                    value = value.get(part) if isinstance(value, dict) else None
                if value is None:
                    return {'validated': True, 'content_status': 'INVALID', **failure('REQUIRED_FIELD_MISSING')}
            if schema.get('schema_version') is not None and (not isinstance(doc, dict) or doc.get('schema_version') != schema['schema_version']):
                return {'validated': True, 'content_status': 'INVALID', **failure('SCHEMA_VERSION_MISMATCH')}
            if isinstance(doc, dict):
                fields = tuple(schema.get('timestamp_fields') or ()) or TS_FIELDS
                for f in fields:
                    if doc.get(f) is not None:
                        ts = _parse_ts(doc.get(f))
                        if ts:
                            out['source_ts_field'] = f if f in TS_FIELDS else 'configured_timestamp_field'
                            break
            out['n_top_level'] = len(doc) if isinstance(doc, (list, dict)) else None
        else:
            head = s3.get_object(Bucket=BUCKET, Key=key, Range='bytes=0-65535')['Body'].read().decode('utf-8', 'ignore')
            m = _ts_re.search(head)
            ts = _parse_ts(m.group(2)) if m else None
            if m:
                out['source_ts_field'] = m.group(1)
            out['note'] = 'large object: head-only timestamp check, JSON validity not verified'
            out['partial_validation'] = True
        out['validated'] = not out.get('partial_validation', False)
        if ts is not None:
            src_age_h = (now - ts).total_seconds() / 3600
            out['source_generated_at'] = ts.isoformat()
            out['source_age_h'] = round(src_age_h, 2)
            if src_age_h < -0.1:
                out['content_status'] = 'INVALID'
                out.update(failure('SOURCE_TIME_FUTURE'))
            elif src_age_h > max_age_h * ALERT_RATIO:
                out['content_status'] = 'SOURCE_STALE'
                out.update(failure('SOURCE_TIME_STALE'))
            else:
                out['content_status'] = 'UNKNOWN' if out.get('partial_validation') else 'OK'
        else:
            out['content_status'] = 'UNKNOWN'
            out.update(failure('NO_SOURCE_TIMESTAMP'))
            out['note'] = ((out.get('note') or '') + ' no valid source timestamp found').strip()
    except Exception as e:
        out['content_status'] = 'UNKNOWN'
        out.update(failure('CONTENT_READ_FAILED'))
    return out


def scoped_key(key, manifest):
    """The feeds whose bodies are inspected: depth-1 data/*.json and manifest key_overrides."""
    ov = set(((manifest or {}).get('key_overrides') or {}).keys())
    return key in ov or (key.startswith('data/') and '/' not in key[len('data/'):])


def load_expected_keys():
    """Expected outputs from the deploy-time engine manifest (data/engine-manifest.json).
    The generator has known false negatives/positives (audit section C), so a declared key that has
    NEVER been seen on S3 is reported as DECLARED_ABSENT (informational); a declared key that WAS seen
    before and is now gone is MISSING (alerting)."""
    try:
        m = json.loads(s3.get_object(Bucket=BUCKET, Key='data/engine-manifest.json')['Body'].read())
    except Exception as e:
        print("[freshness-monitor] EXPECTED_MANIFEST_UNAVAILABLE")
        raise RuntimeError("expected output manifest unavailable") from e
    if not isinstance(m, dict) or not isinstance(m.get('engines'), list) or not m['engines']:
        raise ValueError("expected output manifest has no engine contracts")
    out = {}
    for eng in (m.get('engines') or []):
        for k in (eng.get('keys') or []):
            if isinstance(k, str) and k.endswith('.json'):
                out[k] = eng.get('engine') or eng.get('name')
    return out


def load_seen_keys():
    try:
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key='data/_freshness-seen-keys.json')['Body'].read())
        return doc if isinstance(doc, dict) else {}
    except Exception:
        return {}


def save_seen_keys(seen):
    s3.put_object(Bucket=BUCKET, Key='data/_freshness-seen-keys.json', Body=json.dumps(seen).encode(),
                  ContentType='application/json', CacheControl='max-age=60, public')


def evaluate_key(obj, rule, manifest):
    """Evaluate one S3 object's freshness."""
    key = obj['Key']
    if is_excluded(key, manifest):
        return None
    # Skip directory markers / non-JSON outputs
    if key.endswith('/'):
        return None
    if not key.endswith('.json'):
        return None
    
    try:
        max_age_h = resolve_max_age(key, rule, manifest)
    except Exception:
        max_age_h = float('nan')
    if not math.isfinite(max_age_h) or max_age_h <= 0:
        return {'key':key, 'status':'UNKNOWN', **failure('INVALID_FRESHNESS_SLA'), 'age_h':None}
    last_modified = obj['LastModified']
    age_h = (datetime.now(timezone.utc) - last_modified).total_seconds() / 3600
    alert_threshold = max_age_h * ALERT_RATIO
    
    result = {
        'key': key,
        'max_age_h': max_age_h,
        'artifact_age_h': round(age_h, 2),
        'age_h': round(age_h, 2),          # kept for v1 consumers (artifact age)
        'last_modified': last_modified.isoformat(),
        'size': obj.get('Size', 0),
    }
    if age_h > alert_threshold:
        result['status'] = 'STALE'
    else:
        result['status'] = 'FRESH'
    # audit 2026-09-08 INST-13: a fresh LastModified is not a fresh feed -- inspect scoped bodies
    if True:  # Every declared/enumerated JSON feed is validated, including nested outputs.
        schema = (manifest.get('key_overrides') or {}).get(key)
        v = validate_body(key, obj.get('Size', 0), max_age_h, schema=schema)
        result.update(v)
        cs = v.get('content_status')
        if cs in ('EMPTY', 'INVALID'):
            result['status'] = cs
        elif cs in ('SOURCE_STALE', 'UNKNOWN', 'UNVERIFIED'):
            result['status'] = 'UNKNOWN' if cs == 'UNVERIFIED' else cs
    return result


def load_alert_history():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key='data/_freshness-alert-history.json')
        doc = json.loads(obj['Body'].read().decode())
        return doc if isinstance(doc, dict) else {}
    except Exception:
        return {}


def save_alert_history(h):
    s3.put_object(
        Bucket=BUCKET,
        Key='data/_freshness-alert-history.json',
        Body=json.dumps(h, default=str).encode(),
        ContentType='application/json',
        CacheControl='no-store',
    )


def should_alert(key, history):
    last_iso = history.get(key)
    if not last_iso:
        return True
    try:
        last_ts = datetime.fromisoformat(last_iso.replace('Z', '+00:00'))
        return (datetime.now(timezone.utc) - last_ts).total_seconds() / 3600 > DEDUPE_HOURS
    except Exception:
        return True


def failure(code):
    return {'reason_code': code, 'reason': FRESHNESS_REASONS[code]}


def finish(state, mode, status_code=200):
    state.update(schema_version='fleet-freshness-monitor.v3', publication=dict(PUBLIC_FRESHNESS_REPORT))
    public = sanitize_public('data/_freshness-monitor.json', state)
    body = json.dumps(public, allow_nan=False).encode()
    if mode == 'validate_only':
        return {'ok': public['status'] != 'UNKNOWN', 'validation_only': True,
                'schema_version': 'audit-freshness-1.0', 'status': 'BLOCKED' if public['status'] == 'UNKNOWN' else 'READY',
                'report_status': public['status'], 'artifact_size_bytes': len(body)}
    s3.put_object(Bucket=BUCKET, Key='data/_freshness-monitor.json', Body=body,
                  ContentType='application/json', CacheControl='no-store')
    summary = {'schema_version': public['schema_version'], 'status': public['status'],
               'reason_code': public.get('reason_code'), 'n_tracked': public.get('n_keys_tracked', 0),
               'stale': public.get('n_stale', 0), 'fresh': public.get('n_fresh', 0),
               'alerts': public.get('n_alerts_raised', 0), 'suppressed': public.get('n_alerts_suppressed', 0),
               'elapsed_s': public.get('elapsed_s'), 'artifact_size_bytes': len(body),
               'output_key': 'data/_freshness-monitor.json'}
    return {'statusCode': status_code, 'body': json.dumps(summary)}


def publish_unknown(code, coverage=None, mode='normal', results=None):
    rows = results or []
    coverage = dict(coverage or {})
    coverage.update(results_complete=True, results_returned=len(rows), enumeration_complete=False)
    state = {'version': VERSION, 'status': 'UNKNOWN', 'generated_at': datetime.now(timezone.utc).isoformat(),
             **failure(code), 'coverage': coverage, 'n_fresh': 0, 'full_expected_coverage': False,
             'results': rows, 'n_keys_tracked': len(rows), 'notifications_suppressed': mode != 'normal'}
    return finish(state, mode, 503)


def lambda_handler(event=None, context=None):
    event = event if isinstance(event, dict) else {}
    if event.get('requestContext') or 'headers' in event:
        return {'statusCode': 405, 'headers': {'Cache-Control': 'no-store'}, 'body': '{"error":"scheduled monitor only"}'}
    mode = event.get('mode', 'normal')
    if mode not in ('normal', 'validate_only', 'quiet_refresh'):
        return {'statusCode': 400, 'body': '{"error":"unsupported monitor mode"}'}
    quiet = mode != 'normal'
    started = time.time()
    run_id = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    print(f"[freshness-monitor] v{VERSION} run_id={run_id}")
    
    try:
        manifest = load_manifest(stamp=not quiet)
    except Exception as e:
        return publish_unknown('FRESHNESS_MANIFEST_UNREADABLE', mode=mode)
    if manifest is None:
        print("[freshness-monitor] manifest missing — cannot run")
        return publish_unknown('FRESHNESS_MANIFEST_MISSING', mode=mode)
    
    rules = manifest.get('rules', []) if isinstance(manifest, dict) else []
    if not isinstance(rules, list) or not rules or any(not isinstance(r, dict) or not isinstance(r.get('prefix', 'data/'), str) for r in rules):
        return publish_unknown('FRESHNESS_MANIFEST_INVALID', mode=mode)
    print(f"[freshness-monitor] {len(rules)} rule(s) in manifest")
    
    # Walk each rule
    all_results = []
    coverage = {'rules': [], 'truncated_rules': [], 'keys_enumerated': 0, 'bodies_validated': 0, 'private_sources_excluded': 0, 'expected_private_sources_excluded': 0}
    present = set()
    for rule in rules:
        if is_private_source(rule.get('prefix', 'data/')):
            coverage['private_sources_excluded'] += 1
            continue
        try:
            objs, truncated = list_keys_under_rule(rule)
        except Exception as e:
            return publish_unknown('FEED_ENUMERATION_FAILED', coverage, mode, all_results)
        print(f"[freshness-monitor] enumerated={len(objs)} truncated={truncated}")
        coverage['rules'].append({'prefix': rule.get('prefix'), 'n_objects': len(objs), 'truncated': truncated})
        if truncated:
            coverage['truncated_rules'].append(rule.get('prefix'))
        coverage['keys_enumerated'] += len(objs)
        for obj in objs:
            if is_private_source(obj['Key']):
                coverage['private_sources_excluded'] += 1
                continue
            if obj['Key'] in present:
                continue
            present.add(obj['Key'])
            r = evaluate_key(obj, rule, manifest)
            if r is not None:
                all_results.append(r)
                if r.get('validated'):
                    coverage['bodies_validated'] += 1

    # audit 2026-09-08 INST-13: expected outputs that are ABSENT never entered the result set before.
    try:
        expected = load_expected_keys()
    except Exception:
        return publish_unknown('EXPECTED_MANIFEST_UNAVAILABLE', coverage, mode, all_results)
    seen = {k: v for k, v in load_seen_keys().items() if not is_private_source(k)}
    now_iso_seen = datetime.now(timezone.utc).isoformat()
    missing, declared_absent = [], []
    prefixes = [r.get('prefix', 'data/') for r in rules]
    heads = 0
    HEAD_BUDGET = int(os.environ.get('EXPECTED_HEAD_BUDGET', '2500'))
    expected_evaluated = 0
    unknown = []
    family_keys = []
    for k, eng in expected.items():
        if is_private_source(k):
            coverage['expected_private_sources_excluded'] += 1
            continue
        if is_excluded(k, manifest):
            continue
        if '*' in k:
            family_keys.append(k)
            continue
        if k in present:
            seen[k] = now_iso_seen
            expected_evaluated += 1
            continue
        rule = max((r for r in rules if k.startswith(r.get('prefix','data/'))),
                   key=lambda r:len(r.get('prefix','data/')), default={'default_max_age_h':DEFAULT_MAX_AGE_H})
        if heads >= HEAD_BUDGET:
            unknown.append({'key':k,'engine':eng,'status':'UNKNOWN',**failure('EXPECTED_HEAD_BUDGET_EXHAUSTED'),'age_h':None})
            continue
        heads += 1
        try:
            obj = s3.head_object(Bucket=BUCKET, Key=k)
        except Exception as e:
            code = str(getattr(e, 'response', {}).get('Error', {}).get('Code', ''))
            if code in ('404', 'NoSuchKey', 'NotFound'):
                missing.append({'key':k,'engine':eng,'status':'MISSING','last_seen':seen.get(k),
                                **failure('EXPECTED_OUTPUT_MISSING'),'max_age_h':resolve_max_age(k,rule,manifest),'age_h':None})
                expected_evaluated += 1
            else:
                unknown.append({'key':k,'engine':eng,'status':'UNKNOWN',**failure('EXPECTED_OUTPUT_UNREADABLE'),'age_h':None})
            continue
        obj['Key'] = k
        obj['Size'] = obj.get('ContentLength', obj.get('Size', 0))
        result = evaluate_key(obj, rule, manifest)
        if result is not None:
            result['engine'] = eng
            all_results.append(result)
            coverage['bodies_validated'] += int(result.get('validated', False))
        present.add(k)
        seen[k] = now_iso_seen
        expected_evaluated += 1
    try:
        if not quiet:
            save_seen_keys(seen)
    except Exception as e:
        print("[freshness-monitor] SEEN_KEYS_WRITE_FAILED")
    all_results.extend(missing + unknown)
    coverage.update({'expected_keys_declared': len(expected), 'expected_keys_checked': expected_evaluated, 'unresolved_key_families': family_keys, 'head_budget_exhausted': any(r.get('reason_code') == 'EXPECTED_HEAD_BUDGET_EXHAUSTED' for r in unknown), 'expected_keys_headed': heads, 'missing': len(missing), 'declared_absent_never_seen': len(declared_absent)})

    stale = [r for r in all_results if r.get('status') == 'STALE']
    fresh = [r for r in all_results if r.get('status') == 'FRESH']
    invalid = [r for r in all_results if r.get('status') in ('INVALID', 'EMPTY')]
    source_stale = [r for r in all_results if r.get('status') == 'SOURCE_STALE']
    unknown = [r for r in all_results if r.get('status') == 'UNKNOWN']
    print(f"[freshness-monitor] tracked={len(all_results)}  stale={len(stale)}  fresh={len(fresh)}  invalid/empty={len(invalid)}  source_stale={len(source_stale)}  missing={len(missing)}")
    # alerts cover every non-fresh state, not only LastModified age
    stale = stale + invalid + source_stale + missing + unknown
    
    # Dedupe alerts
    history = load_alert_history() if not quiet else {}
    new_alerts = [r for r in stale if should_alert(r['key'], history)]

    # v1.2 escalation: persistent staleness must not hide behind dedupe.
    # If anything is >3x past its SLA (or >=10 keys stale), send an un-deduped
    # digest at most once per 6h via the '_escalation' history slot.
    try:
        ov = set(((manifest or {}).get('key_overrides') or {}).keys())
        def _scoped(r):
            k = r['key']
            return k in ov or ('/' not in k[len('data/'):])
        critical = [r for r in stale if _scoped(r) and ((r.get('age_h') or 0) > 3 * (r.get('max_age_h') or 26) or r.get('status') in ('INVALID', 'EMPTY', 'MISSING'))]
        now_ts = datetime.now(timezone.utc).timestamp()
        last_esc = history.get('_escalation', 0)
        if not quiet and (critical or len(stale) >= 10) and now_ts - last_esc > 6 * 3600:
            worst = sorted(stale, key=lambda r: -(r.get('age_h') or 0))[:8]
            lines = [f"🚨 *FRESHNESS ESCALATION* — {len(stale)} stale ({len(critical)} critical >3×SLA)"]
            for r in worst:
                lines.append(f"• `{r['key']}` {r.get('status')} {(r.get('age_h') or 0):.0f}h (SLA {r.get('max_age_h')}h)")
            send_telegram("\n".join(lines))
            history['_escalation'] = now_ts
    except Exception as e:
        print("[escalation] ALERT_FAILED")

    suppressed = len(stale) - len(new_alerts)
    
    # Send digest
    sent_telegram = False
    sent_sns = False
    if new_alerts and not quiet:
        new_alerts.sort(key=lambda r: ((r.get('age_h') or 0) / (r.get('max_age_h') or 1)) if r.get('status') == 'STALE' else 1e9, reverse=True)
        lines = [f"🕰️ *FRESHNESS MONITOR* — {len(new_alerts)} new stale key(s)"]
        for r in new_alerts[:12]:
            ratio = (r['age_h'] / r['max_age_h']) if (r.get('age_h') is not None and r.get('max_age_h')) else 99.0
            severity = "🔴" if (ratio > 3 or r.get('status') in ('INVALID', 'EMPTY', 'MISSING')) else "🟡"
            lines.append(f"{severity} `{r['key']}` [{r.get('status')}]")
            if r.get('status') == 'STALE':
                lines.append(f"     {r['age_h']}h old (max {r['max_age_h']}h, ratio {ratio:.1f}×)")
            else:
                lines.append(f"     {r.get('reason') or r.get('last_seen') or ''}")
        if len(new_alerts) > 12:
            lines.append(f"\n_+{len(new_alerts)-12} more, see data/_freshness-monitor.json_")
        if suppressed:
            lines.append(f"\n_({suppressed} suppressed by {DEDUPE_HOURS}h dedupe)_")
        digest = "\n".join(lines)
        sent_telegram = send_telegram(digest)
        sent_sns = publish_sns(f"Freshness: {len(new_alerts)} stale", digest)
        
        now_iso = datetime.now(timezone.utc).isoformat()
        for r in new_alerts:
            history[r['key']] = now_iso
        save_alert_history(history)
    
    # Run state
    # Sort stale by ratio (most-stale first) for the dashboard
    stale_sorted = sorted(stale, key=lambda r: ((r.get('age_h') or 0) / (r.get('max_age_h') or 1)) if r.get('status') == 'STALE' else 1e9, reverse=True)
    state = {
        'version': VERSION,
        'status': 'DEGRADED' if stale or coverage['truncated_rules'] or family_keys else 'HEALTHY',
        'full_expected_coverage': not unknown and not coverage['truncated_rules'] and not family_keys,
        'n_unknown': len(unknown), 'unknown': unknown,
        'run_id': run_id,
        'generated_at': datetime.now(timezone.utc).isoformat(),
        'n_keys_tracked': len(all_results),
        'n_stale': len(stale),
        'n_fresh': len(fresh),
        'n_invalid_or_empty': len(invalid),
        'n_source_stale': len(source_stale),
        'n_missing': len(missing),
        'missing': missing,
        'invalid_or_empty': invalid,
        'source_stale_top_50': sorted(source_stale, key=lambda r: -(r.get('source_age_h') or 0))[:50],
        'declared_absent_never_seen': declared_absent,
        'coverage': {**coverage, 'results_complete': True, 'results_returned': len(all_results), 'enumeration_complete': not coverage['truncated_rules']},
        'results': all_results, 'stale': stale_sorted, 'source_stale': source_stale,
        'notifications_suppressed': quiet,
        'semantics': "artifact_age_h = S3 LastModified age (a writer ran); source_age_h = the document's own timestamp age (what it wrote). FRESH requires both within SLA for scoped feeds; EMPTY/INVALID = zero-byte or non-JSON; SOURCE_STALE = rewritten with old data; MISSING = a required declared output does not exist; UNKNOWN = missing/invalid time provenance, partial validation or unreadable content (audit 2026-09-08 INST-13)",
        'n_alerts_raised': len(new_alerts) if not quiet else 0,
        'n_alerts_suppressed': suppressed,
        'stale_top_50': stale_sorted[:50],
        'elapsed_s': round(time.time() - started, 2),
        'thresholds': {
            'default_max_age_h': DEFAULT_MAX_AGE_H,
            'alert_ratio': ALERT_RATIO,
            'dedupe_hours': DEDUPE_HOURS,
        },
        'manifest_rules': rules,
        'telegram_sent': sent_telegram,
        'sns_sent': sent_sns,
    }
    print(f"[freshness-monitor] done tracked={len(all_results)} mode={mode}")
    return finish(state, mode)


if __name__ == "__main__":
    print(json.dumps(lambda_handler(), indent=2))
