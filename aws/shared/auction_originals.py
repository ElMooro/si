"""Complete Treasury auction originals and independently calculated measurements.

Pure: no network, storage, current consumers, clocks or portfolio decisions.
The retained page set is a current acquisition vintage, not historical availability.
"""
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import math
import re
from urllib.parse import urlencode

from treasury_instruments import instrument_fields

CONTRACT = 'auction-original-measurements.v1'
PREFIX = 'data/auction-observation-originals/'
ENDPOINT = 'https://api.fiscaldata.treasury.gov/services/api/fiscal_service/v1/accounting/od/auctions_query'
MAX_PAGE_BYTES = 4 * 1024 * 1024
MAX_PAGES = 30
QUOTE_FIELDS = {
    'BILL': ('high_discnt_rate', 'low_discnt_rate', 'median_discnt_rate', 'discount_rate_pct'),
    'NOMINAL_COUPON': ('high_yield', 'low_yield', 'median_yield', 'nominal_yield_pct'),
    'TIPS': ('high_yield', 'low_yield', 'median_yield', 'real_yield_pct'),
    'FRN': ('high_discount_margin', 'low_discount_margin', 'median_discount_margin', 'discount_margin_pct'),
}
BIDDERS = ('primary_dealer_accepted', 'direct_bidder_accepted', 'indirect_bidder_accepted')


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode('utf-8')


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def clock(value):
    if not isinstance(value, str):
        raise ValueError('Acquisition clock must be a timestamp')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError('Acquisition clock requires a timezone')
    return result.astimezone(timezone.utc)


def strict_json(raw):
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise ValueError('Duplicate JSON key')
            out[key] = value
        return out

    def bad(_):
        raise ValueError('Nonfinite JSON constant')

    return json.loads(raw, object_pairs_hook=pairs, parse_constant=bad)


def url(start, end, size, page):
    return ENDPOINT + '?' + urlencode({
        'filter': f'auction_date:gte:{start},auction_date:lte:{end}',
        'sort': '-auction_date,cusip', 'format': 'json',
        'page[size]': size, 'page[number]': page,
    }, safe=':,')


def count(value):
    if isinstance(value, str) and value.isascii() and value.isdigit() and len(value) < 10:
        value = int(value)
    if type(value) is not int or value < 0:
        raise ValueError('Invalid pagination count')
    return value


def complete_pages(pages, start, end, size, generated_at):
    """Bind every byte/page/request/clock/row to one complete bounded window."""
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    generated = clock(generated_at)
    if first.isoformat() != start or last.isoformat() != end or not 0 <= (last-first).days <= 400:
        raise ValueError('Invalid acquisition date window')
    if last > generated.date() or type(size) is not int or not 1 <= size <= 200:
        raise ValueError('Invalid acquisition size or future window')
    if not isinstance(pages, list) or not 1 <= len(pages) <= MAX_PAGES:
        raise ValueError('Complete nonempty page manifest required')
    rows, identities, totals = [], set(), None
    previous_clock = None
    for number, page in enumerate(pages, 1):
        if not isinstance(page, dict) or page.get('request_url') != url(start, end, size, number):
            raise ValueError('Page request identity differs')
        if type(page.get('http_status')) is not int or page['http_status'] != 200:
            raise ValueError('Successful HTTP response required')
        acquired = clock(page['acquired_at'])
        if acquired > generated or (previous_clock is not None and acquired < previous_clock):
            raise ValueError('Page acquisition order or cutoff differs')
        previous_clock = acquired
        raw = page.get('raw')
        if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_PAGE_BYTES:
            raise ValueError('Original page size invalid')
        document = strict_json(raw)
        if not isinstance(document, dict) or not isinstance(document.get('meta'), dict) or not isinstance(document.get('data'), list):
            raise ValueError('Original page schema differs')
        total_pages = count(document['meta'].get('total-pages'))
        total_rows = count(document['meta'].get('total-count'))
        if total_pages != max(1, (total_rows+size-1)//size) or total_pages != len(pages):
            raise ValueError('Page set incomplete or cardinality inconsistent')
        if totals is None:
            totals = (total_pages, total_rows)
        if totals != (total_pages, total_rows):
            raise ValueError('Pagination changed during acquisition')
        if len(document['data']) != min(size, total_rows-(number-1)*size):
            raise ValueError('Page row count differs')
        for index, row in enumerate(document['data']):
            if not isinstance(row, dict) or not isinstance(row.get('cusip'), str) or not row['cusip'].strip():
                raise ValueError('Auction identity missing')
            day = row.get('auction_date')
            if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day or not start <= day <= end:
                raise ValueError('Auction date outside request')
            identity = (day, row['cusip'])
            if identity in identities:
                raise ValueError('Repeated auction identity')
            identities.add(identity)
            rows.append({'row': row, 'page_number': number, 'row_index': index, 'page_sha256': sha(raw)})
    if len(rows) != totals[1]:
        raise ValueError('Whole row count differs')
    return rows


def number(value, nonnegative=False, maximum=None):
    """Decimal reference calculation with the native finite numeric domain."""
    if type(value) not in (int, float, str):
        return None
    text = str(value).strip()
    if not text or len(text) > 128 or not re.fullmatch(r'[-+]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][-+]?[0-9]+)?', text):
        return None
    try:
        result = Decimal(text)
        if not result.is_finite() or not math.isfinite(float(result)):
            return None
        if nonnegative and result < 0 or maximum is not None and result > maximum:
            return None
        return result
    except (InvalidOperation, ValueError, OverflowError):
        return None


def decimal_text(value):
    return str(value) if value is not None else None


def measurements(row):
    """Independent Decimal arithmetic for the reported native direct measurements."""
    identity = instrument_fields(row, 'fiscaldata')
    fields = QUOTE_FIELDS.get(identity['instrument_kind'])
    rates = [number(row.get(field)) for field in fields[:3]] if fields else [None, None, None]
    amounts = [number(row.get(field), nonnegative=True) for field in BIDDERS]
    missing = [field for field, value in zip(BIDDERS, amounts) if value is None]
    with localcontext() as context:
        context.prec = 50
        total = sum(amounts, Decimal(0)) if not missing else None
        if total is not None and not math.isfinite(float(total)):
            total = None
        shares = [value/total*100 for value in amounts] if total is not None and total > 0 else [None]*3
        accepted = number(row.get('total_accepted'), nonnegative=True)
        spread = (rates[0]-rates[2])*100 if rates[0] is not None and rates[2] is not None else None
        if spread is not None and not math.isfinite(float(spread)):
            spread = None
        values = dict(zip(('high_rate', 'low_rate', 'median_rate'), rates))
        values.update(btc=number(row.get('bid_to_cover_ratio'), nonnegative=True),
                      allocated_at_high_pct=number(row.get('allocation_pctage'), nonnegative=True, maximum=100),
                      primary_dealer_pct=shares[0], direct_pct=shares[1], indirect_pct=shares[2],
                      bidder_denominator_usd=total, accepted_billions=accepted/Decimal(10**9) if accepted is not None else None,
                      high_minus_median_bp=spread, tail_bp=None, wi_tail_bp=None)
    return {
        **identity, 'quote_basis': fields[3] if fields else None,
        'auction_date': row['auction_date'], 'cusip': row['cusip'],
        'issue_date': row.get('issue_date'), 'security_type': row.get('security_type'), 'security_term': row.get('security_term'),
        'values_decimal': {name: decimal_text(value) for name, value in values.items()},
        'bidder_missing_fields': missing,
        'bidder_share_status': 'complete' if total is not None and total > 0 else 'missing_or_invalid' if missing or total is None else 'zero_denominator',
        'bidder_denominator_scope': 'Sum of primary dealer, direct and indirect accepted amounts; not all auction allotments.',
        'wi_tail_status': 'unavailable_no_timestamped_same_security_when_issued_quote',
    }


def assert_native(rows, compute):
    """Check all acquired rows, including those excluded by the legacy scorer."""
    for row in rows:
        reference, native = measurements(row), compute(row)
        for name in ('instrument_kind', 'instrument_classification_status', 'quote_basis', 'bidder_share_status', 'bidder_missing_fields'):
            if native[name] != reference[name]:
                raise ValueError('Native auction identity/status differs: '+name)
        for name, value in reference['values_decimal'].items():
            observed = native[name]
            if value is None:
                if observed is not None:
                    raise ValueError('Native missing observation differs: '+name)
            elif type(observed) not in (int, float) or not math.isfinite(observed) or not math.isclose(observed, float(value), rel_tol=1e-12, abs_tol=1e-10):
                raise ValueError('Native auction arithmetic differs: '+name)


def build(pages, start, end, size, generated_at):
    rows = complete_pages(pages, start, end, size, generated_at)
    observations = [{**measurements(item['row']), 'source': {k:v for k,v in item.items() if k != 'row'}} for item in rows]
    return {
        'contract': CONTRACT, 'generated_at': generated_at,
        'request_window': {'start': start, 'end': end, 'page_size': size},
        'coverage': {'pages': len(pages), 'observations': len(rows), 'complete_requested_window': True,
                     'current_acquisition_vintage': True, 'provider_snapshot_atomicity_verified': False,
                     'historical_publication_vintages_verified': False},
        'observations': observations,
        'units': {'high_rate': 'named quote_basis percent', 'low_rate': 'named quote_basis percent', 'median_rate': 'named quote_basis percent',
                  'btc': 'ratio', 'allocated_at_high_pct': 'pct', 'primary_dealer_pct': 'pct', 'direct_pct': 'pct', 'indirect_pct': 'pct',
                  'bidder_denominator_usd': 'usd', 'accepted_billions': 'usd_bn', 'high_minus_median_bp': 'bp', 'tail_bp': 'bp', 'wi_tail_bp': 'bp'},
        'source_fields': {'bidder_amounts': list(BIDDERS), 'accepted_billions': 'total_accepted',
                          'btc': 'bid_to_cover_ratio', 'allocated_at_high_pct': 'allocation_pctage',
                          'quote_fields_by_kind': {kind:list(fields) for kind,fields in QUOTE_FIELDS.items()}},
        'calls_eligible': False, 'forecast_eligible': False, 'sizing_eligible': False, 'execution_eligible': False,
        'limitations': ['Only the direct Treasury auction measurements are reproduced. Legacy scores, FRED context, forecasts and analogs are outside this contract.',
                       'Observation date is not acquisition time or proven first-publication time. A full current page set cannot prove historical availability.'],
    }
