"""Reconstruct dated ETF profiles without guessing units or portfolio trades.

The profile's effective date belongs to this profile, not to its constituent
snapshot. Whole source bodies remain protected; public measurements name their
exact source row and field. Unspecified financial units remain unqualified.
"""
from datetime import timedelta
from decimal import Decimal, DecimalException, localcontext
from urllib.parse import urlsplit, parse_qsl, urlencode
import re
import etf_holdings_native as base

PRIVATE = 'audit-private/20260909-originals/etf-desk-research/'
ENDPOINT = 'https://api.polygon.io/etf-global/v1/profiles'
PATH = '/etf-global/v1/profiles'
DOCUMENTATION = 'https://massive.com/docs/rest/partners/etf-global/profiles'
MAX_SOURCE_BYTES = 8 * 1024 * 1024
MAX_PAGES = 8
ERRORS = base.ERRORS
sha, encoded, clock, day, ds, decimal = base.sha, base.encoded, base.clock, base.day, base.ds, base.decimal
SourceRejected = base.SourceRejected
TEXT_FIELDS = ('administrator', 'advisor', 'asset_class', 'category', 'custodian',
    'description', 'development_class', 'distribution_frequency', 'distributor',
    'fiscal_year_end', 'focus', 'futures_commission_merchant', 'inception_date',
    'issuer', 'lead_market_maker', 'leverage_style', 'listing_exchange',
    'management_classification', 'portfolio_manager', 'primary_benchmark',
    'product_type', 'region', 'subadvisor', 'tax_classification', 'transfer_agent', 'trustee')
EXPOSURES = ('sector_exposure', 'industry_exposure', 'industry_group_exposure',
    'subindustry_exposure', 'geographic_exposure', 'currency_exposure',
    'maturity_exposure', 'coupon_exposure')
# Only units explicitly stated for this endpoint are qualified. In particular,
# another distributor's fee/exposure scale does not certify this response.
UNITS = {
    'aum': ('provider_reported_assets_currency_unqualified', False),
    'net_expenses': ('provider_reported_net_expenses_scale_unqualified', False),
    'total_expenses': ('provider_reported_total_expenses_scale_unqualified', False),
    'management_fee': ('provider_reported_management_fee_scale_unqualified', False),
    'fee_waivers': ('provider_reported_fee_waivers_scale_unqualified', False),
    'other_expenses': ('provider_reported_other_expenses_scale_unqualified', False),
    'creation_fee': ('provider_reported_creation_fee_currency_unqualified', False),
    'creation_unit_size': ('provider_reported_creation_unit_size', False),
    'bid_ask_spread': ('provider_reported_spread_scale_unqualified', False),
    'discount_premium': ('provider_reported_premium_discount_scale_unqualified', False),
    'avg_daily_trading_volume': ('shares_per_day_past_month', True),
    'num_holdings': ('reported_holdings_count', True),
    'levered_amount': ('reported_leverage_multiplier', True),
    'options_available': ('provider_reported_options_availability_code', False),
    'options_volume': ('provider_reported_options_volume_counting_unit_unqualified', False),
    'call_volume': ('provider_reported_call_volume_counting_unit_unqualified', False),
    'put_volume': ('provider_reported_put_volume_counting_unit_unqualified', False),
    'put_call_ratio': ('reported_put_call_ratio', True),
    'short_interest': ('provider_reported_short_interest_unit_unqualified', False),
}
NONNEGATIVE = frozenset(('aum', 'avg_daily_trading_volume', 'num_holdings',
    'creation_unit_size', 'options_volume', 'call_volume', 'put_volume', 'short_interest', 'put_call_ratio'))


def next_url(value):
    if not isinstance(value, str) or len(value) > 8192: raise ValueError('Bounded profile URL required')
    u = urlsplit(value)
    if (u.scheme != 'https' or u.hostname not in ('api.polygon.io', 'api.massive.com') or u.path != PATH
            or u.username or u.password or u.port not in (None, 443) or u.fragment):
        raise ValueError('Unreviewed profile destination')
    items = parse_qsl(u.query, keep_blank_values=True)
    allowed = {'composite_ticker', 'processed_date', 'processed_date.lte', 'sort', 'limit', 'cursor'}
    if not items or len(dict(items)) != len(items) or any(k not in allowed or not v for k, v in items):
        raise ValueError('Unreviewed profile query')
    return value


def selection_url(ticker, cutoff):
    base.ticker(ticker); day(cutoff)
    return ENDPOINT + '?' + urlencode({'composite_ticker': ticker, 'processed_date.lte': cutoff,
                                       'sort': 'processed_date.desc', 'limit': 1})


def snapshot_url(ticker, processed):
    base.ticker(ticker); day(processed)
    return ENDPOINT + '?' + urlencode({'composite_ticker': ticker, 'processed_date': processed, 'limit': 5000})


def original(ref, read):
    digest = ref.get('sha256', '')
    if (not re.fullmatch('[a-f0-9]{64}', digest) or ref.get('key') != PRIVATE + digest + '.bin'
            or type(ref.get('bytes')) is not int or not 0 < ref['bytes'] <= MAX_SOURCE_BYTES):
        raise ValueError('Bounded protected profile identity required')
    raw = read(ref['key'])
    if len(raw) != ref['bytes'] or sha(raw) != digest: raise ValueError('Original profile bytes differ')
    try: doc = base.strict(raw)
    except (ValueError, TypeError, DecimalException, RecursionError): raise SourceRejected('Original profile JSON rejected') from None
    if (not isinstance(doc, dict) or doc.get('status') != 'OK' or not isinstance(doc.get('results'), list)
            or len(doc['results']) > 5000): raise SourceRejected('Successful bounded profile response required')
    if 'count' in doc and (type(doc['count']) is not int or doc['count'] != len(doc['results'])):
        raise SourceRejected('Profile count differs from returned rows')
    return doc


def raw_type(value):
    return 'null' if value is None else 'boolean' if isinstance(value, bool) else 'number' if isinstance(value, (int, float, Decimal)) else 'string' if isinstance(value, str) else 'array' if isinstance(value, list) else 'object' if isinstance(value, dict) else 'unsupported'


def tree(value, budget=None, depth=0):
    """Preserve typed provider structures, including arrays, without unit inference."""
    if budget is None: budget = [0]
    budget[0] += 1
    if budget[0] > 20000 or depth > 12: raise SourceRejected('Profile field structure exceeds reviewed bound')
    kind = raw_type(value)
    if kind == 'number':
        try: return {'type': kind, 'decimal': ds(decimal(value))}
        except ValueError: return {'type': kind, 'decimal': None, 'status': 'invalid_numeric_bounds'}
    if kind == 'string':
        if len(value) > 8192: raise SourceRejected('Profile text exceeds reviewed bound')
        return {'type': kind, 'value': value}
    if kind in ('null', 'boolean'): return {'type': kind, 'value': value}
    if kind == 'array': return {'type': kind, 'items': [tree(v, budget, depth + 1) for v in value]}
    if kind == 'object':
        if any(not isinstance(k, str) or len(k) > 512 for k in value): raise SourceRejected('Profile field name rejected')
        return {'type': kind, 'fields': {k: tree(v, budget, depth + 1) for k, v in value.items()}}
    raise SourceRejected('Profile field type rejected')


def numeric(row, field, source):
    value = row.get(field); unit, certified = UNITS[field]
    result = {'present': field in row, 'raw_type': raw_type(value), 'raw_decimal': None,
        'value_decimal': None, 'status': 'missing' if field not in row else 'null' if value is None else 'invalid',
        'unit': unit, 'unit_certified': certified, 'source': {**source, 'field': field}}
    if value is None: return result
    try:
        val = decimal(value); result['raw_decimal'] = ds(val)
        if field in NONNEGATIVE and val < 0: return result
        if field in ('num_holdings', 'creation_unit_size') and val != val.to_integral_value(): return result
        if field == 'num_holdings' and val > 10000000: return result
        if field == 'options_available' and val not in (0, 1): return result
        result.update(value_decimal=ds(val), status='reported')
    except ValueError: pass
    return result


def exposure(row, field, source):
    value = row.get(field)
    result = {'present': field in row, 'raw_type': raw_type(value), 'source': {**source, 'field': field},
        'status': 'missing' if field not in row else 'null' if value is None else 'unqualified_structure',
        'unit': 'provider_reported_exposure_scale_unqualified', 'unit_certified': False,
        'normalized': False, 'portfolio_weight_eligible': False, 'raw_structure': tree(value),
        'entries': [], 'raw_observed_sum_decimal': None, 'invalid_entries': 0}
    if isinstance(value, dict):
        vals = []
        for label, raw in value.items():
            entry = {'label': label, 'raw_decimal': None, 'status': 'invalid',
                'source': {**source, 'field': field, 'member_key': label}}
            try:
                val = decimal(raw)
                if val is not None: entry.update(raw_decimal=ds(val), status='reported'); vals.append(val)
            except ValueError: pass
            result['entries'].append(entry)
        result['invalid_entries'] = sum(e['status'] != 'reported' for e in result['entries'])
        result['status'] = 'reported_mapping' if not result['invalid_entries'] else 'mapping_with_invalid_entries'
        # Precision covers the entire bounded exponent range plus <=20,000 terms.
        with localcontext() as ctx:
            ctx.prec = 180
            result['raw_observed_sum_decimal'] = ds(sum(vals, Decimal(0))) if vals else None
    elif isinstance(value, list):
        result['status'] = 'reported_array_unqualified_schema'
        # Array contents survive exactly as typed fields; a label/weight pair is
        # never guessed from field names or positions.
    elif value is not None: result['status'] = 'invalid_structure'
    return result


def normalize(row, ticker, processed, source):
    if not isinstance(row, dict) or row.get('composite_ticker') != ticker or row.get('processed_date') != processed:
        raise SourceRejected('Profile identity or processing date differs')
    try: effective = day(row.get('effective_date'))
    except ValueError: raise SourceRejected('Profile effective date rejected') from None
    if effective > day(processed): raise SourceRejected('Profile effective date after processing')
    result = {'ticker': ticker, 'effective_date': effective.isoformat(), 'processed_date': processed,
        'source': source, 'text': {}, 'numeric': {}, 'exposures': {}, 'field_errors': [],
        'dates_apply_to': 'profile_only', 'holdings_effective_date_inferred': False}
    for field in TEXT_FIELDS:
        value = row.get(field)
        status = 'missing' if field not in row else 'null' if value is None else 'reported' if isinstance(value, str) and len(value) <= 8192 else 'invalid'
        result['text'][field] = {'value': value if status == 'reported' else None, 'status': status, 'source': {**source, 'field': field}}
        if status == 'invalid': result['field_errors'].append(field)
    for field in UNITS:
        result['numeric'][field] = numeric(row, field, source)
        if result['numeric'][field]['status'] == 'invalid': result['field_errors'].append(field)
    for field in EXPOSURES:
        result['exposures'][field] = exposure(row, field, source)
        if result['exposures'][field]['invalid_entries'] or result['exposures'][field]['status'] == 'invalid_structure': result['field_errors'].append(field)
    known = set(TEXT_FIELDS) | set(UNITS) | set(EXPOSURES) | {'composite_ticker', 'processed_date', 'effective_date'}
    result['additional_field_types'] = {k: raw_type(v) for k, v in row.items() if k not in known}
    result['original_field_count'] = len(row)
    result['row_id'] = sha(encoded(source))
    return result


def page(entry, read, generated_at):
    at = clock(entry['acquired_at'])
    if at > clock(generated_at): raise ValueError('Profile acquired after compilation')
    next_url(entry['url'])
    return original(entry['original'], read), at


def reconstruct(collection, read, generated_at):
    ticker = base.ticker(collection['ticker']); cutoff = day(collection['cutoff']); generated = clock(generated_at)
    if cutoff > generated.date(): raise ValueError('Profile request cutoff after compilation')
    result = {'ticker': ticker, 'cutoff': cutoff.isoformat(), 'acquisition_status': collection['status'],
        'processed_date': None, 'effective_date': None, 'source_acquired_at': None, 'source_valid_until': None,
        'profiles': [], 'selected_profile': None, 'originals': [],
        'quality': {'status': 'unavailable', 'pagination_complete': False, 'single_profile_unambiguous': False,
            'current_holdings_confirmed': False, 'historical_first_availability_verified': False,
            'independent_investment_votes': 0, 'current_profile_eligible': False}}
    selection = collection.get('selection')
    if not selection or not selection.get('original'):
        if collection['status'] not in ERRORS: raise ValueError('Profile selection original required')
        if collection.get('pages'): raise ValueError('Profile pages require a selection')
        return result
    if selection['url'] != selection_url(ticker, collection['cutoff']): raise ValueError('Exact profile selection query required')
    doc, acquired = page(selection, read, generated_at); result['selection'] = selection
    if len(doc['results']) > 1: raise SourceRejected('One profile date-selection row required')
    if not doc['results']:
        if collection.get('pages') or collection['status'] not in ERRORS: raise ValueError('Empty profile selection cannot be complete')
        return result
    selected = doc['results'][0]
    if not isinstance(selected, dict) or selected.get('composite_ticker') != ticker: raise SourceRejected('Selected profile identity differs')
    processed = selected.get('processed_date')
    try: processed_day = day(processed)
    except ValueError: raise SourceRejected('Profile processing date rejected') from None
    if processed_day > cutoff or processed_day > acquired.date(): raise SourceRejected('Profile processing date after request')
    if len(collection.get('pages', [])) > MAX_PAGES: raise ValueError('Profile page bound exceeded')
    selected_tree = tree(selected); matched = False
    expected = snapshot_url(ticker, processed); seen = set(); stamps = [acquired]
    for index, entry in enumerate(collection.get('pages', [])):
        if expected is None or entry['url'] != expected or entry['url'] in seen: raise ValueError('Exact profile pagination chain required')
        seen.add(entry['url']); body, at = page(entry, read, generated_at); stamps.append(at)
        if at < acquired: raise ValueError('Profile page acquired before selection')
        result['originals'].append({'page': index, **entry['original'], 'acquired_at': entry['acquired_at']})
        for ordinal, row in enumerate(body['results']):
            matched |= tree(row) == selected_tree
            result['profiles'].append(normalize(row, ticker, processed,
                {'page': index, 'row_index': ordinal, 'sha256': entry['original']['sha256']}))
        expected = next_url(body['next_url']) if body.get('next_url') else None
    complete = bool(collection.get('pages')) and expected is None and collection['status'] == 'complete_returned_profile_snapshot'
    if collection['status'] == 'complete_returned_profile_snapshot' and not complete: raise ValueError('Incomplete profile pagination presented as complete')
    if not complete and collection['status'] not in ERRORS: raise ValueError('Unreviewed profile acquisition status')
    if complete and not matched: raise SourceRejected('Selected profile changed before complete snapshot')
    rows = result['profiles']; unambiguous = complete and len(rows) == 1
    due = min(min(stamps) + timedelta(hours=26), clock(processed + 'T00:00:00Z') + timedelta(days=6))
    status = 'complete_returned_profile_snapshot' if unambiguous else 'ambiguous_profiles' if complete and rows else 'incomplete' if rows else 'unavailable'
    result.update(processed_date=processed, effective_date=rows[0]['effective_date'] if unambiguous else None,
        source_acquired_at=min(stamps).isoformat(), source_valid_until=due.isoformat(),
        selected_profile=rows[0]['row_id'] if unambiguous else None)
    result['quality'].update(status=status, pagination_complete=complete, single_profile_unambiguous=unambiguous,
        returned_profiles=len(rows), rows_with_field_errors=sum(bool(r['field_errors']) for r in rows),
        source_check_overdue=generated >= min(stamps) + timedelta(hours=26),
        processed_age_days=(generated.date() - processed_day).days,
        effective_age_days={d: (generated.date() - day(d)).days for d in sorted({r['effective_date'] for r in rows})},
        current_profile_eligible=unambiguous and generated < due and not rows[0]['field_errors'],
        freshness_policy='26 hours since acquisition and six calendar days since processing; profile-only, not a verified exchange calendar')
    return result



def reconstruct_or_reject(collection, read, generated_at):
    """Quarantine provider content errors; manifest/hash/storage errors still fail."""
    try: return reconstruct(collection, read, generated_at)
    except SourceRejected:
        return {'ticker': collection['ticker'], 'cutoff': collection['cutoff'],
            'acquisition_status': collection['status'], 'processed_date': None, 'effective_date': None,
            'source_acquired_at': None, 'source_valid_until': None, 'profiles': [],
            'selected_profile': None, 'originals': [], 'retained_attempt': collection,
            'quality': {'status': 'source_rejected', 'pagination_complete': False,
                'single_profile_unambiguous': False, 'current_holdings_confirmed': False,
                'historical_first_availability_verified': False, 'independent_investment_votes': 0,
                'current_profile_eligible': False}}
