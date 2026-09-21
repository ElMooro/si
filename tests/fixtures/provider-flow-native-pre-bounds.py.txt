"""Pure reconstruction of dated provider fund flows from complete response bytes.

Effective and processing dates, latest acquired vintages and original row positions
stay separate. SPY reporting dates are a comparison grid, not an exchange calendar.
"""
from datetime import datetime, timezone, timedelta, date
from decimal import Decimal, localcontext
from urllib.parse import urlsplit, parse_qsl, urlencode
import hashlib, json, re

PRIVATE = 'audit-private/20260909-originals/provider-fund-flow-research/'
ENDPOINT = 'https://api.polygon.io/etf-global/v1/fund-flows'
PATH = '/etf-global/v1/fund-flows'
MAX_SOURCE_BYTES = 2 * 1024 * 1024
MAX_PAGES = 4
ERRORS = frozenset(('provider_http_error', 'provider_request_failed', 'credential_unavailable',
                    'response_rejected', 'acquisition_deadline', 'pagination_bound'))


def sha(raw): return hashlib.sha256(raw).hexdigest()


def encoded(doc):
    return json.dumps(doc, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def clock(value):
    if not isinstance(value, str): raise ValueError('Aware source clock required')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None: raise ValueError('Aware source clock required')
    return result.astimezone(timezone.utc)


def day(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value):
        raise ValueError('ISO observation date required')
    return date.fromisoformat(value)


def dec(value, positive=False):
    if value is None: return None
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise ValueError('Provider numeric field required')
    result = Decimal(str(value))
    if not result.is_finite() or abs(result) > Decimal('1e30') or (positive and result <= 0):
        raise ValueError('Finite bounded provider measurement required')
    return result


def ds(value):
    if value is None: return None
    if not value: return '0'
    return format(value, 'f')


def pairs(items):
    result = {}
    for key, value in items:
        if key in result: raise ValueError('Duplicate JSON field')
        result[key] = value
    return result


def initial_url(ticker, query_date):
    if not isinstance(ticker, str) or not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,14}', ticker):
        raise ValueError('Configured fund ticker required')
    end = day(query_date)
    return ENDPOINT + '?' + urlencode({'composite_ticker': ticker,
        'processed_date.gte': (end - timedelta(days=110)).isoformat(),
        'processed_date.lte': end.isoformat(), 'sort': 'processed_date.desc', 'limit': 5000})


def next_url(value):
    if not isinstance(value, str) or len(value) > 8192: raise ValueError('Bounded provider page URL required')
    u = urlsplit(value)
    if (u.scheme != 'https' or u.hostname not in ('api.polygon.io', 'api.massive.com')
            or u.path != PATH or u.username or u.password or u.port not in (None, 443) or u.fragment):
        raise ValueError('Unreviewed provider page destination')
    query = parse_qsl(u.query, keep_blank_values=True)
    allowed = {'cursor', 'composite_ticker', 'processed_date.gte', 'processed_date.lte', 'sort', 'limit'}
    if not query or len({k for k, _ in query}) != len(query) or any(k not in allowed or not v for k, v in query):
        raise ValueError('Unreviewed provider page query')
    return value


def original(ref, read):
    digest = ref.get('sha256', '')
    if (not re.fullmatch('[a-f0-9]{64}', digest) or ref.get('key') != PRIVATE + digest + '.bin'
            or type(ref.get('bytes')) is not int or not 0 < ref['bytes'] <= MAX_SOURCE_BYTES):
        raise ValueError('Exact bounded original reference required')
    raw = read(ref['key'])
    if len(raw) != ref['bytes'] or sha(raw) != digest: raise ValueError('Original provider bytes differ')
    doc = json.loads(raw, parse_float=Decimal, object_pairs_hook=pairs,
                     parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Nonfinite JSON constant')))
    if not isinstance(doc, dict) or doc.get('status') != 'OK' or not isinstance(doc.get('results'), list):
        raise ValueError('Successful provider response required')
    if 'count' in doc and (type(doc['count']) is not int or doc['count'] != len(doc['results'])):
        raise ValueError('Provider response count differs')
    if len(doc['results']) > 5000: raise ValueError('Provider row bound exceeded')
    return doc


def reconstruct(ticker, collection, read, generated_at):
    at = clock(generated_at)
    if not isinstance(collection, dict) or collection.get('ticker') != ticker:
        raise ValueError('Exact fund collection identity required')
    acquired = clock(collection['completed_at'])
    query_day = day(collection['query_date'])
    if query_day > acquired.date() or acquired > at: raise ValueError('Provider acquisition clock differs')
    pages = collection.get('pages')
    if not isinstance(pages, list) or len(pages) > MAX_PAGES: raise ValueError('Provider pagination bound')
    if collection.get('status') not in ({'retained'} | ERRORS): raise ValueError('Fixed acquisition status required')
    expected = initial_url(ticker, collection['query_date']); seen = set(); versions = {}; rejected = []
    identities = []; first = query_day - timedelta(days=110)
    for page_index, page in enumerate(pages):
        url = page.get('url')
        if url != expected or url in seen: raise ValueError('Provider page chain differs')
        seen.add(url)
        acquired_page = clock(page['acquired_at'])
        if not query_day <= acquired_page.date() or acquired_page > acquired:
            raise ValueError('Provider page acquisition clock differs')
        if page_index and acquired_page < clock(pages[page_index - 1]['acquired_at']):
            raise ValueError('Provider pages acquired out of order')
        doc = original(page['original'], read)
        identities.append({'page': page_index, 'acquired_at': page['acquired_at'], **page['original']})
        for row_index, row in enumerate(doc['results']):
            loc = {'page': page_index, 'row_index': row_index, 'sha256': page['original']['sha256']}
            if not isinstance(row, dict) or row.get('composite_ticker') != ticker:
                raise ValueError('Provider fund identity differs')
            try:
                effective = day(row.get('effective_date')); processed = day(row.get('processed_date'))
                if not first <= processed <= query_day or effective > processed:
                    raise ValueError('Provider date order or query bounds differ')
            except (ValueError, TypeError):
                rejected.append({**loc, 'reason': 'invalid_or_missing_dates'}); continue
            values = {}; invalid = []
            for key in ('fund_flow', 'nav', 'shares_outstanding'):
                try: values[key] = ds(dec(row.get(key), key != 'fund_flow'))
                except (ValueError, ArithmeticError): values[key] = None; invalid.append(key)
            item = {'date': effective.isoformat(), 'processed_date': processed.isoformat(),
                    'flow_decimal': values['fund_flow'], 'nav_decimal': values['nav'],
                    'shares_decimal': values['shares_outstanding'], 'invalid_fields': invalid, 'source_rows': [loc]}
            key = (item['date'], item['processed_date'])
            prior = versions.get(key)
            if prior is None: versions[key] = item
            else:
                numeric = ('flow_decimal', 'nav_decimal', 'shares_decimal')
                comparable = lambda v: Decimal(v) if v is not None else None
                if (any(comparable(prior[k]) != comparable(item[k]) for k in numeric)
                        or prior['invalid_fields'] != item['invalid_fields']): prior['conflicting_version'] = True
                prior['source_rows'].append(loc)
        expected = next_url(doc['next_url']) if doc.get('next_url') else None
    complete = collection['status'] == 'retained'
    if complete and (not pages or expected is not None): raise ValueError('Complete pagination required')
    rows = {}; revision_count = 0
    for (effective, processed), row in sorted(versions.items()):
        if effective in rows: revision_count += 1
        if row.get('conflicting_version'):
            row.update(flow_decimal=None, nav_decimal=None, shares_decimal=None)
        rows[effective] = row
    history = [rows[key] for key in sorted(rows)]
    latest = history[-1] if history else None
    date_due = clock(latest['date'] + 'T00:00:00Z') + timedelta(days=6) if latest else None
    due = min(acquired + timedelta(hours=26), date_due) if date_due else acquired
    invalid_rows = len(rejected) + sum(bool(r.get('conflicting_version') or r['invalid_fields']) for r in history)
    status = ('unavailable' if not complete or not history else 'invalid' if invalid_rows else
              'stale' if at >= due else 'complete_acquired_history')
    return {'ticker': ticker, 'acquisition_status': collection['status'], 'source_acquired_at': collection['completed_at'],
            'source_valid_until': due.isoformat(), 'latest_effective_date': latest['date'] if latest else None,
            'latest_processed_date': latest['processed_date'] if latest else None, 'history': history,
            'originals': identities, 'rejected_rows': rejected, 'revision_versions_selected': revision_count,
            'quality': {'status': status, 'pagination_complete': complete, 'invalid_rows': invalid_rows,
                        'observed_dates': len(history), 'source_age_hours': (at - acquired).total_seconds() / 3600,
                        'exchange_calendar_verified': False, 'first_historical_availability_verified': False}}


def window(fund, reference_dates, end, observations):
    if observations not in (1, 5, 21): raise ValueError('Reviewed fund-flow window required')
    dates = [d for d in reference_dates if d <= end][-observations:] if end else []
    by_date = {r['date']: r for r in fund['history']}
    rows = [by_date[d] for d in dates if d in by_date]
    missing = [d for d in dates if d not in by_date]
    ready = (fund['quality']['status'] == 'complete_acquired_history' and len(dates) == observations
             and dates[-1] == end and not missing and all(r['flow_decimal'] is not None for r in rows))
    reasons = []
    if fund['quality']['status'] != 'complete_acquired_history': reasons.append('source_' + fund['quality']['status'])
    if len(dates) != observations or (dates and dates[-1] != end): reasons.append('reference_window_incomplete')
    if missing: reasons.append('missing_reference_observations')
    if any(r['flow_decimal'] is None for r in rows): reasons.append('missing_reported_flow')
    with localcontext() as ctx:
        ctx.prec = 50
        value = sum((Decimal(r['flow_decimal']) for r in rows), Decimal(0)) if ready else None
        end_row = by_date.get(end)
        aum = (Decimal(end_row['nav_decimal']) * Decimal(end_row['shares_decimal'])
               if ready and end_row and end_row['nav_decimal'] and end_row['shares_decimal'] else None)
        ratio = value / aum * 100 if value is not None and aum else None
    return {'status': 'matched_reporting_window' if ready else 'incomplete', 'requested_observations': observations,
            'dates': dates, 'missing_dates': missing, 'available_observations': len(rows), 'reasons': reasons,
            'start_date': dates[0] if dates else None, 'end_date': end, 'flow_usd_decimal': ds(value),
            'end_reported_assets_usd_decimal': ds(aum), 'flow_to_end_assets_pct_decimal': ds(ratio),
            'source_rows': [v for row in rows for v in row['source_rows']],
            'method': 'Sum provider fund_flow on each listed effective date. Denominator is same-end-date NAV times shares. SPY reporting dates are not a verified exchange calendar.'}


def reconcile(fund, reference_dates):
    positions = {d: i for i, d in enumerate(reference_dates)}; out = []
    with localcontext() as ctx:
        ctx.prec = 50
        for previous, current in zip(fund['history'], fund['history'][1:]):
            if (current['date'] not in positions or previous['date'] not in positions
                    or positions[current['date']] != positions[previous['date']] + 1): continue
            if any(row.get('conflicting_version') or row['invalid_fields'] for row in (previous, current)): continue
            if any(row[key] is None for row in (previous, current) for key in ('nav_decimal', 'shares_decimal')): continue
            if current['flow_decimal'] is None: continue
            change = Decimal(current['shares_decimal']) - Decimal(previous['shares_decimal'])
            prior_value = change * Decimal(previous['nav_decimal']); current_value = change * Decimal(current['nav_decimal'])
            reported = Decimal(current['flow_decimal'])
            out.append({'date': current['date'], 'previous_date': previous['date'], 'reported_flow_usd_decimal': ds(reported),
                'share_change_decimal': ds(change), 'prior_nav_valued_change_usd_decimal': ds(prior_value),
                'current_nav_valued_change_usd_decimal': ds(current_value),
                'reported_minus_prior_nav_change_usd_decimal': ds(reported - prior_value),
                'reported_minus_current_nav_change_usd_decimal': ds(reported - current_value),
                'source_rows': previous['source_rows'] + current['source_rows'], 'corporate_actions_verified': False,
                'scope': 'Arithmetic comparison only. No provider methodology, cash settlement, split adjustment or independent confirmation is inferred.'})
    return out
