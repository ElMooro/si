"""Reconstruct every returned ETF holding without inferring trades or weight units.

An exact processed-date snapshot is a provider observation, not proof of the
fund's present portfolio, historical availability, corporate actions or trades.
"""
from collections import Counter, defaultdict
from datetime import datetime, timezone, date, timedelta
from decimal import Decimal, localcontext
from urllib.parse import urlsplit, parse_qsl, urlencode
import hashlib, json, re

PRIVATE = 'audit-private/20260909-originals/etf-constituent-research/'
ENDPOINT = 'https://api.polygon.io/etf-global/v1/constituents'
PATH = '/etf-global/v1/constituents'
MAX_SOURCE_BYTES = 8 * 1024 * 1024
MAX_PAGES = 20
TEXT_FIELDS = ('constituent_ticker', 'constituent_name', 'figi', 'isin', 'us_code',
    'sedol', 'exchange', 'country_of_exchange', 'currency_traded', 'asset_class', 'security_type')
IDENTITY_FIELDS = ('figi', 'isin', 'us_code', 'sedol', 'exchange', 'currency_traded', 'asset_class', 'security_type')
NUMERIC_FIELDS = ('weight', 'market_value', 'shares_held')
ERRORS = frozenset(('credential_unavailable', 'provider_http_error', 'provider_request_failed',
    'response_rejected', 'pagination_bound', 'acquisition_deadline', 'unavailable', 'incomplete'))


class SourceRejected(ValueError):
    """Retained provider content cannot be used; other funds may still reconstruct."""


def sha(raw): return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def clock(value):
    if not isinstance(value, str): raise ValueError('Aware observation clock required')
    out = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if out.tzinfo is None: raise ValueError('Aware observation clock required')
    return out.astimezone(timezone.utc)


def day(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise ValueError('ISO observation date required')
    return date.fromisoformat(value)


def ds(value): return None if value is None else '0' if not value else format(value, 'f')


def decimal(value):
    if value is None: return None
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError('Original numeric measurement required')
    value = Decimal(str(value))
    if not value.is_finite() or abs(value) > Decimal('1e30'): raise ValueError('Bounded finite measurement required')
    return value


def pairs(items):
    out = {}
    for key, value in items:
        if key in out: raise ValueError('Duplicate original JSON field')
        out[key] = value
    return out


def strict(raw):
    return json.loads(raw, parse_float=Decimal, object_pairs_hook=pairs,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Nonfinite JSON constant')))


def ticker(value):
    if not isinstance(value, str) or not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,14}', value):
        raise ValueError('Configured fund identity required')
    return value


def selection_url(fund, cutoff):
    ticker(fund); day(cutoff)
    return ENDPOINT + '?' + urlencode({'composite_ticker': fund, 'processed_date.lte': cutoff,
                                      'sort': 'processed_date.desc', 'limit': 1})


def snapshot_url(fund, processed_date):
    ticker(fund); day(processed_date)
    # Default provider sort is intentional: constituent_rank.asc was rejected.
    return ENDPOINT + '?' + urlencode({'composite_ticker': fund, 'processed_date': processed_date, 'limit': 5000})


def next_url(value):
    if not isinstance(value, str) or len(value) > 8192: raise ValueError('Bounded source URL required')
    u = urlsplit(value)
    if (u.scheme != 'https' or u.hostname not in ('api.polygon.io', 'api.massive.com') or u.path != PATH
            or u.username or u.password or u.port not in (None, 443) or u.fragment):
        raise ValueError('Unreviewed provider destination')
    query = parse_qsl(u.query, keep_blank_values=True)
    allowed = {'composite_ticker', 'processed_date', 'processed_date.lte', 'sort', 'limit', 'cursor'}
    if not query or len({k for k, _ in query}) != len(query) or any(k not in allowed or not v for k, v in query):
        raise ValueError('Unreviewed provider query')
    return value


def original(ref, read):
    digest = ref.get('sha256', '')
    if (not re.fullmatch('[a-f0-9]{64}', digest) or ref.get('key') != PRIVATE + digest + '.bin'
            or type(ref.get('bytes')) is not int or not 0 < ref['bytes'] <= MAX_SOURCE_BYTES):
        raise ValueError('Bounded protected original identity required')
    raw = read(ref['key'])
    if len(raw) != ref['bytes'] or sha(raw) != digest: raise ValueError('Original bytes differ')
    try: doc = strict(raw)
    except (ValueError, TypeError): raise SourceRejected('Original provider JSON rejected') from None
    if not isinstance(doc, dict) or doc.get('status') != 'OK' or not isinstance(doc.get('results'), list):
        raise SourceRejected('Successful original holdings response required')
    if len(doc['results']) > 5000: raise SourceRejected('Provider row bound exceeded')
    if 'count' in doc and (type(doc['count']) is not int or doc['count'] != len(doc['results'])):
        raise SourceRejected('Provider count differs from returned rows')
    return doc


def source_page(ref, read, generated_at):
    stamp = clock(ref['acquired_at'])
    if stamp > clock(generated_at): raise ValueError('Source acquired after compilation')
    next_url(ref['url'])
    return original(ref['original'], read), stamp


def normalize(row, fund, processed, source):
    if not isinstance(row, dict) or row.get('composite_ticker') != fund or row.get('processed_date') != processed:
        raise SourceRejected('Snapshot identity or processing date differs')
    try: effective = day(row.get('effective_date'))
    except ValueError: raise SourceRejected('Source effective date rejected') from None
    if effective > day(processed): raise SourceRejected('Effective date after source processing date')
    out = {'effective_date': effective.isoformat(), 'processed_date': processed,
           'source': source, 'field_errors': [], 'identity_key': None}
    for field in TEXT_FIELDS:
        value = row.get(field)
        if value is None: out[field] = None
        elif isinstance(value, str) and len(value) <= 512: out[field] = value.strip() or None
        else: out[field] = None; out['field_errors'].append(field)
    for field in NUMERIC_FIELDS:
        try: out[field + '_raw_decimal'] = ds(decimal(row.get(field)))
        except ValueError: out[field + '_raw_decimal'] = None; out['field_errors'].append(field)
    rank = row.get('constituent_rank')
    if rank is None or type(rank) is int and 0 < rank <= 10000000: out['constituent_rank'] = rank
    else: out['constituent_rank'] = None; out['field_errors'].append('constituent_rank')
    if any(out[field] for field in ('figi', 'isin', 'us_code', 'sedol')) and not set(out['field_errors']) & set(IDENTITY_FIELDS):
        out['identity_key'] = sha(encoded({field: out[field] for field in IDENTITY_FIELDS}))
    out['row_id'] = sha(encoded(source))
    return out


def reconstruct(collection, read, generated_at):
    fund = ticker(collection['ticker']); cutoff = day(collection['cutoff'])
    generated = clock(generated_at)
    if cutoff > generated.date(): raise ValueError('Query cutoff after compilation')
    result = {'ticker': fund, 'cutoff': cutoff.isoformat(), 'acquisition_status': collection['status'],
        'processed_date': None, 'effective_dates': {}, 'source_acquired_at': None, 'source_valid_until': None,
        'rows': [], 'originals': [], 'quality': {'status': 'unavailable', 'returned_rows': 0,
            'pagination_complete': False, 'current_holdings_confirmed': False,
            'weight_unit_certified': False, 'market_value_currency_certified': False,
            'corporate_actions_verified': False, 'historical_first_availability_verified': False}}
    selection = collection.get('selection')
    if not selection or not selection.get('original'):
        if collection['status'] not in ERRORS: raise ValueError('Selection original required')
        return result
    if selection['url'] != selection_url(fund, collection['cutoff']): raise ValueError('Exact date-selection request required')
    selected, acquired = source_page(selection, read, generated_at)
    if len(selected['results']) > 1: raise SourceRejected('One date-selection row required')
    result['selection'] = selection
    if not selected['results']:
        if collection.get('pages'): raise ValueError('Unexpected pages without a selected snapshot')
        return result
    selected_row = selected['results'][0]
    if not isinstance(selected_row, dict) or selected_row.get('composite_ticker') != fund:
        raise SourceRejected('Selected fund identity differs')
    processed = selected_row.get('processed_date')
    try: processed_day = day(processed)
    except ValueError: raise SourceRejected('Source processing date rejected') from None
    if processed_day > cutoff or processed_day > acquired.date(): raise SourceRejected('Source processing date after request')
    result['processed_date'] = processed
    if len(collection.get('pages', [])) > MAX_PAGES: raise ValueError('Source page bound exceeded')
    expected = snapshot_url(fund, processed); seen = set(); stamps = [acquired]
    for index, page in enumerate(collection.get('pages', [])):
        if expected is None or page['url'] != expected or page['url'] in seen: raise ValueError('Exact complete pagination chain required')
        seen.add(page['url']); body, stamp = source_page(page, read, generated_at); stamps.append(stamp)
        if stamp < acquired: raise ValueError('Snapshot acquired before date selection')
        source = {'page': index, **page['original'], 'acquired_at': page['acquired_at']}
        result['originals'].append(source)
        for ordinal, row in enumerate(body['results']):
            result['rows'].append(normalize(row, fund, processed,
                {'page': index, 'row_index': ordinal, 'sha256': page['original']['sha256']}))
        expected = next_url(body['next_url']) if body.get('next_url') else None
    complete = bool(collection.get('pages')) and expected is None and collection['status'] == 'complete_returned_snapshot'
    if collection['status'] == 'complete_returned_snapshot' and not complete: raise ValueError('Incomplete pagination presented as complete')
    if not complete and collection['status'] not in ERRORS: raise ValueError('Unreviewed acquisition status')
    rows = result['rows']; ids = Counter(r['identity_key'] for r in rows if r['identity_key'])
    dates = dict(sorted(Counter(r['effective_date'] for r in rows).items()))
    result.update(effective_dates=dates, source_acquired_at=min(stamps).isoformat(),
                  source_valid_until=(min(stamps) + timedelta(hours=26)).isoformat())
    result['quality'].update(status='complete_returned_snapshot' if complete and rows else 'incomplete' if rows else 'unavailable',
        returned_rows=len(rows), pagination_complete=complete, rows_with_field_errors=sum(bool(r['field_errors']) for r in rows),
        missing_ticker_rows=sum(r['constituent_ticker'] is None for r in rows),
        missing_identity_rows=sum(r['identity_key'] is None for r in rows),
        duplicate_identity_rows=sum(n - 1 for n in ids.values() if n > 1),
        source_check_overdue=generated >= min(stamps) + timedelta(hours=26),
        effective_age_days={d: (generated.date() - day(d)).days for d in dates},
        mixed_effective_dates=len(dates) > 1)
    with localcontext() as ctx:
        ctx.prec = 50
        values = [Decimal(r['weight_raw_decimal']) for r in rows if r['weight_raw_decimal'] is not None]
        result['weight_audit'] = {'raw_observed_sum_decimal': ds(sum(values, Decimal(0))) if values else None,
            'rows_with_numeric_weight': len(values), 'rows_without_numeric_weight': len(rows) - len(values),
            'unit': 'provider_raw_weight_unverified_scale', 'normalized': False, 'portfolio_weight_eligible': False}
    result['reported_classifications'] = {field: dict(sorted(Counter(r[field] or '<missing>' for r in rows).items()))
        for field in ('asset_class', 'security_type', 'currency_traded')}
    return result


def reconstruct_or_reject(collection, read, generated_at):
    """Quarantine malformed source content, never hash or trusted-manifest errors."""
    try: return reconstruct(collection, read, generated_at)
    except SourceRejected:
        return {'ticker': collection['ticker'], 'cutoff': collection['cutoff'],
            'acquisition_status': collection['status'], 'processed_date': None, 'effective_dates': {},
            'source_acquired_at': None, 'source_valid_until': None, 'rows': [], 'originals': [],
            'retained_attempt': collection, 'quality': {'status': 'source_rejected', 'returned_rows': 0,
                'source_row_count': None, 'pagination_complete': False, 'current_holdings_confirmed': False,
                'weight_unit_certified': False, 'market_value_currency_certified': False,
                'corporate_actions_verified': False, 'historical_first_availability_verified': False}}


def compare(current, prior):
    """Only exact unambiguous provider identities; no implied buying or selling."""
    if current['ticker'] != prior['ticker']: raise ValueError('Comparison fund differs')
    def indexed(snapshot):
        out = defaultdict(list)
        for row in snapshot['rows']:
            if row['identity_key']: out[row['identity_key']].append(row)
        return out
    now, before = indexed(current), indexed(prior)
    dates = (list(current['effective_dates']), list(prior['effective_dates']))
    valid = (all(p['quality']['status'] == 'complete_returned_snapshot' for p in (current, prior))
        and all(len(d) == 1 for d in dates) and dates[0][0] > dates[1][0])
    rows = []
    with localcontext() as ctx:
        ctx.prec = 50
        for identity in sorted(set(now) | set(before)):
            a, b = now.get(identity, []), before.get(identity, [])
            status = ('not_comparable_snapshots' if not valid else 'ambiguous_identity' if len(a) > 1 or len(b) > 1
                else 'observed_in_both' if a and b else 'observed_only_in_current' if a else 'observed_only_in_prior')
            rec = {'identity_key': identity, 'status': status, 'current_rows': [r['row_id'] for r in a],
                   'prior_rows': [r['row_id'] for r in b], 'shares_held_change_raw_decimal': None,
                   'weight_change_raw_decimal': None, 'inferred_trade_usd': None}
            if status == 'observed_in_both':
                for source, target in [('shares_held_raw_decimal', 'shares_held_change_raw_decimal'),
                                       ('weight_raw_decimal', 'weight_change_raw_decimal')]:
                    if a[0][source] is not None and b[0][source] is not None:
                        rec[target] = ds(Decimal(a[0][source]) - Decimal(b[0][source]))
            rows.append(rec)
    return {'ticker': current['ticker'], 'current_effective_dates': dates[0], 'prior_effective_dates': dates[1],
        'comparable_snapshots': valid, 'rows': rows, 'identity_status_counts': dict(Counter(r['status'] for r in rows)),
        'current_unidentified_rows': [r['row_id'] for r in current['rows'] if not r['identity_key']],
        'prior_unidentified_rows': [r['row_id'] for r in prior['rows'] if not r['identity_key']],
        'scope': 'Changes in reported unadjusted positions, not trades, flow attribution, index events or corporate-action-adjusted accumulation.'}
