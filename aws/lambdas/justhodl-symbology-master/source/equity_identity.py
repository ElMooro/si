"""SEC symbol identities and optional FIGI resolution, with conservative recovery.

Pure/injected operations: importing this module never contacts storage/providers.
A symbol lookup is not a historically qualified issuer-security relationship.
"""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import json
import math
import re
import time

IDENTIFIERS = ('figi', 'figi_name', 'figi_status', 'cusip', 'isin', 'sedol', 'lei')
MASTER_KEY = 'data/symbology/master.json'
MAX_MASTER_BYTES = 64 * 1024 * 1024


class IdentityError(ValueError):
    pass


def cik(value):
    if type(value) is int:
        text = str(value)
    elif isinstance(value, str):
        text = value.strip()
    else:
        raise IdentityError('Positive SEC issuer identity required')
    if not re.fullmatch(r'[0-9]{1,10}', text) or int(text) == 0:
        raise IdentityError('Positive SEC issuer identity required')
    return text.zfill(10)


def ticker(value):
    if not isinstance(value, str) or not value.strip():
        raise IdentityError('SEC ticker required')
    value = value.strip().upper()
    if any(c.isspace() or ord(c) < 32 for c in value):
        raise IdentityError('Unambiguous SEC ticker required')
    return value


def read_prior(client, bucket):
    from bond_symbology import _read
    doc, etag = _read(client, bucket, MASTER_KEY, max_bytes=MAX_MASTER_BYTES)
    if doc is None and etag is None:
        return {}, None
    if not isinstance(doc, dict) or not isinstance(doc.get('by_ticker'), dict):
        raise IdentityError('Whole prior equity master required')
    for key, row in doc['by_ticker'].items():
        if not isinstance(key, str) or not isinstance(row, dict) or ticker(key) != key:
            raise IdentityError('Whole prior equity rows required')
    if 'retained_prior_tickers' in doc and not isinstance(doc['retained_prior_tickers'], dict):
        raise IdentityError('Whole retained prior population required')
    for key, row in doc.get('retained_prior_tickers', {}).items():
        if not isinstance(key, str) or not isinstance(row, dict) or ticker(key) != key:
            raise IdentityError('Whole retained prior rows required')
    return doc, etag


def spine(data, source):
    if not isinstance(data, dict) or not data:
        raise IdentityError('Complete nonempty SEC ticker source required')
    by_ticker, by_cik = {}, {}
    for position, record in data.items():
        if not isinstance(record, dict):
            raise IdentityError('Whole SEC source row required')
        t, issuer = ticker(record.get('ticker')), cik(record.get('cik_str'))
        name = record.get('title')
        if name is not None and not isinstance(name, str):
            raise IdentityError('SEC name must be text or unavailable')
        if t in by_ticker:
            old = by_ticker[t]
            if old['cik'] != issuer:
                raise IdentityError('Conflicting issuers for one SEC ticker')
            old['source_records'].append({'position': str(position), 'record': deepcopy(record)})
            if old['name'] != name:
                old['name'] = None
                old['name_status'] = 'conflicting_source_labels'
            continue
        row = {'ticker': t, 'cik': issuer, 'name': name, 'cusip': None, 'isin': None,
               'figi': None, 'sedol': None, 'lei': None, 'source': deepcopy(source),
               'source_records': [{'position': str(position), 'record': deepcopy(record)}]}
        by_ticker[t] = row
        by_cik.setdefault(issuer, []).append(t)
    return by_ticker, by_cik


def carry_previous(by_ticker, prior):
    """Carry only the same issuer; retain incompatible predecessors explicitly."""
    old_rows = prior.get('by_ticker', {})
    retained = deepcopy(prior.get('retained_prior_tickers', {}))
    for t, old in old_rows.items():
        if t not in by_ticker:
            retained[t] = deepcopy(old)
    for t, row in list(by_ticker.items()):
        old = old_rows[t] if t in old_rows else retained.get(t)
        if old is None:
            continue
        try:
            same_issuer = cik(old.get('cik')) == row['cik']
        except IdentityError:
            same_issuer = False
        if same_issuer:
            # Preserve unknown prior fields without recursively nesting the whole
            # previous row on each normal daily refresh.
            merged = {**deepcopy(old), **row}
            for name in IDENTIFIERS:
                if old.get(name) is not None:
                    merged[name] = deepcopy(old[name])
            if 'legacy_identifier_origin' not in merged:
                merged['legacy_identifier_origin'] = {
                    'status': 'unqualified_prior_input', 'prior_as_of': prior.get('as_of'),
                    'cik': old.get('cik'), 'source': deepcopy(old.get('source')),
                    'identifiers': {k: deepcopy(old.get(k)) for k in IDENTIFIERS if old.get(k) is not None}}
            merged['figi_resolution_eligible'] = False
            merged['cusip_match_qualified'] = False
            merged['isin_match_qualified'] = False
            merged['lei_match_qualified'] = False
            by_ticker[t] = merged
        else:
            row['quarantined_prior_identity'] = deepcopy(old)
            row['identity_reconciliation'] = 'issuer_changed_or_unverified; identifiers_not_carried'
    return retained


def _stamp(value):
    if not isinstance(value, str):
        return None
    try:
        result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    return result.astimezone(timezone.utc) if result.tzinfo else None


def _qualified_figi(t, row, now):
    result = row.get('figi_resolution')
    if not isinstance(result, dict) or result.get('schema') != 'openfigi-resolution.v1':
        return False
    if result.get('query') != {'idType': 'TICKER', 'idValue': t, 'exchCode': 'US'} or result.get('sec_cik') != row['cik']:
        return False
    stamp = _stamp(row.get('figi_attempted_at'))
    if stamp is None or stamp > now or now-stamp >= timedelta(days=7):
        return False
    from openfigi import resolution
    computed = resolution(result.get('response'))
    if any(result.get(k) != v for k,v in computed.items()):
        return False
    if computed['status'] == 'no_match':
        return row.get('figi') is None
    if computed['status'] != 'resolved':
        return False
    security = computed['security']
    return (row.get('figi') == security['figi'] and security.get('marketSector') == 'Equity'
            and security.get('ticker') == t and security.get('exchCode') == 'US')


def enrich_figi(by_ticker, context, *, limit=2500, now=None):
    import openfigi
    now = now or datetime.now(timezone.utc)
    stats = {'enriched': 0, 'no_match': 0, 'errors': 0, 'ambiguous': 0, 'attempted': 0,
             'remaining_null': sum(row.get('figi') is None for row in by_ticker.values())}
    if not isinstance(now, datetime) or now.tzinfo is None:
        return {**stats, 'errors': 1, 'reason': 'invalid_clock'}
    for t, row in by_ticker.items():
        row['figi_resolution_eligible'] = _qualified_figi(t,row,now) and row['figi_resolution'].get('status') == 'resolved'
    if type(limit) is not int or not 1 <= limit <= 2500:
        return {**stats, 'errors': 1, 'reason': 'invalid_budget'}
    if context is None or not callable(getattr(context, 'get_remaining_time_in_millis', None)):
        return {**stats, 'reason': 'remaining_time_unavailable'}
    remaining = context.get_remaining_time_in_millis()
    if type(remaining) not in (int, float) or not math.isfinite(remaining) or not 40000 < remaining <= 900000:
        return {**stats, 'reason': 'insufficient_time'}
    deadline = time.monotonic() + min(40, remaining/1000-30)
    key = openfigi.get_api_key()
    if not key:
        return {**stats, 'note': 'no key in SSM'}
    todo = [t for t,row in by_ticker.items() if not _qualified_figi(t,row,now)]
    todo.sort(key=lambda t: _stamp(by_ticker[t].get('figi_attempted_at')) or datetime.min.replace(tzinfo=timezone.utc))
    for i in range(0, min(len(todo),limit), 100):
        if deadline-time.monotonic() < 5:
            stats['reason'] = 'time_budget_reached'
            break
        batch=todo[i:min(i+100,limit)]
        jobs=[{'idType':'TICKER','idValue':t,'exchCode':'US'} for t in batch]
        try:
            results=openfigi.mapping(jobs,api_key=key,deadline=deadline)
        except Exception:
            results=None
        if not isinstance(results,list) or len(results)!=len(jobs):
            results=[{'error':'request failed or cardinality differs'} for _ in jobs]
        for t,job,item in zip(batch,jobs,results):
            row=by_ticker[t];out=openfigi.resolution(item)
            if out['status']=='resolved':
                security=out['security']
                if security.get('marketSector')!='Equity' or security.get('ticker')!=t or security.get('exchCode')!='US':
                    out={**out,'status':'ambiguous','reason':'returned security does not establish requested US equity identity'}
            row['figi_resolution']={**out,'query':job,'sec_cik':row['cik'],'issuer_security_relationship_qualified':False}
            row['figi_attempted_at']=now.isoformat();row['figi_status']=out['status']
            stats['attempted']+=1
            if out['status']=='resolved':
                row['figi']=out['security']['figi'];row['figi_name']=out['security'].get('name');stats['enriched']+=1
            elif out['status']=='no_match':
                stats['no_match']+=1
            elif out['status']=='ambiguous':
                stats['ambiguous']+=1
            else:
                stats['errors']+=1
            # A last known identifier remains retained but is not relabeled as
            # newly verified when this lookup fails or becomes ambiguous.
            row['figi_resolution_eligible']=out['status']=='resolved'
    stats['remaining_null']=sum(row.get('figi') is None for row in by_ticker.values())
    stats['remaining_unqualified']=sum(not _qualified_figi(t,row,now) for t,row in by_ticker.items())
    return stats


def publish(client, bucket, doc, etag):
    if etag is not None and (not isinstance(etag,str) or not etag):
        raise IdentityError('Observed version required')
    raw=json.dumps(doc,allow_nan=False,separators=(',',':')).encode('utf-8')
    if len(raw)>MAX_MASTER_BYTES:
        raise IdentityError('Complete master exceeds storage bound; prior retained')
    client.put_object(Bucket=bucket,Key=MASTER_KEY,Body=raw,ContentType='application/json',CacheControl='no-cache',
                      **({'IfMatch':etag} if etag is not None else {'IfNoneMatch':'*'}))
