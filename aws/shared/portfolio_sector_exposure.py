"""Reported sector labels and unknown exposure, never verified classification.

The caller supplies marked lot values in the exact input order. Unknown labels
are not an economic sector and cannot contribute a made-up HHI component.
"""
import math

SCHEMA = 'reported-sector-exposure.v1'
UNCLASSIFIED = frozenset(('', 'unknown', 'unclassified', 'unavailable', 'not available',
    'not classified', 'n/a', 'na', 'none', 'null', '-', '—', 'other', 'etf', 'fund'))


def number(value):
    if type(value) not in (int, float):
        return None
    try:
        result = float(value)
        return result if math.isfinite(result) and (not result.is_integer() or abs(result) <= 9007199254740991) else None
    except (OverflowError, ValueError):
        return None


def total(values):
    try:
        return number(math.fsum(values))
    except (OverflowError, ValueError):
        return None


def label(value):
    if type(value) is not str:
        return None, 'MISSING_LABEL' if value is None else 'INVALID_LABEL_TYPE'
    normalized = value.strip()
    if normalized.casefold() in UNCLASSIFIED:
        return None, 'UNCLASSIFIED_LABEL'
    return normalized, None


def marked_lot_value(position):
    """Recompute from the retained operands; a displayed amount is not an operand."""
    if type(position) is not dict:
        return None
    qty, price = number(position.get('qty')), number(position.get('current_price'))
    return number(qty * price) if qty is not None and price is not None and price > 0 else None


def build_sector_exposure(positions, marked_values, *, valuation_complete=True):
    """Keep every row; full-book percentages require every mark to be eligible."""
    if type(positions) is not list or type(marked_values) is not list or len(positions) != len(marked_values) or type(valuation_complete) is not bool:
        raise ValueError('Complete ordered position and marked-value arrays are required')
    records, by_symbol = [], {}
    for index, (position, value) in enumerate(zip(positions, marked_values)):
        source = position if type(position) is dict else {}
        reported = source.get('sector')
        sector, reason = label(reported)
        symbol = source.get('symbol') if type(source.get('symbol')) is str else None
        signed = number(value)
        row = {'input_index': index, 'symbol': symbol, 'reported_sector': reported,
               'reported_market_value': source.get('market_value'),
               'sector': sector, 'classification_reason': reason,
               'signed_marked_value': signed, 'gross_marked_value': abs(signed) if signed is not None else None}
        records.append(row)
        if symbol and sector is not None:
            by_symbol.setdefault(symbol, set()).add(sector)
    conflicts = {symbol for symbol, sectors in by_symbol.items() if len(sectors) > 1}
    groups, unknown = {}, []
    for row in records:
        if row['symbol'] in conflicts:
            row['sector'], row['classification_reason'] = None, 'CONFLICTING_INSTRUMENT_LABELS'
        if row['sector'] is None:
            unknown.append(row)
        else:
            groups.setdefault(row['sector'], []).append(row)
    priced = [r for r in records if r['gross_marked_value'] is not None]
    full_values = valuation_complete and len(priced) == len(records)
    priced_gross = total(r['gross_marked_value'] for r in priced)
    gross = priced_gross if full_values else None
    unknown_priced = total(r['gross_marked_value'] for r in unknown if r['gross_marked_value'] is not None)
    unknown_gross = unknown_priced if full_values else None
    known_priced = total(r['gross_marked_value'] for r in priced if r['sector'] is not None)
    positive_gross = gross is not None and gross > 0
    shares = []
    for sector, rows in groups.items():
        group_gross = total(r['gross_marked_value'] for r in rows if r['gross_marked_value'] is not None)
        group_complete = all(r['gross_marked_value'] is not None for r in rows)
        shares.append({'sector': sector, 'gross_marked_value': group_gross if group_complete else None,
            'signed_marked_value': total(r['signed_marked_value'] for r in rows) if group_complete else None,
            'weight_pct': group_gross / gross * 100 if positive_gross and group_gross is not None and group_complete else None,
            'input_indices': [r['input_index'] for r in rows]})
    shares.sort(key=lambda r: (r['gross_marked_value'] is None, -(r['gross_marked_value'] or 0), r['sector']))
    fractions = [r['weight_pct'] for r in shares]
    complete_classification = positive_gross and unknown_gross == 0 and all(v is not None for v in fractions)
    hhi = total(v*v for v in fractions) if complete_classification else None
    maximum_known = max(fractions, default=None) if positive_gross and all(v is not None for v in fractions) else None
    breach = True if maximum_known is not None and maximum_known > 40 else False if complete_classification else None
    status = ('VALUATION_INCOMPLETE' if not full_values or gross is None else 'NO_GROSS_EXPOSURE' if not positive_gross else
              'COMPLETE_REPORTED_CLASSIFICATION' if complete_classification else 'PARTIAL_CLASSIFICATION')
    return {'schema_version': SCHEMA, 'status': status,
        'basis': 'Unrounded absolute quantity-times-price lot exposure / full gross marked lot exposure; not NAV or ETF look-through',
        'classification_basis': 'Reported text labels only; provider taxonomy, vintage and instrument classification are unverified',
        'classification_verified': False, 'sizing_eligible': False,
        'position_count': len(records), 'priced_position_count': len(priced),
        'classified_position_count': len(records)-len(unknown), 'unclassified_position_count': len(unknown),
        'gross_marked_value': gross, 'priced_gross_marked_value': priced_gross,
        'classified_priced_gross_marked_value': known_priced, 'unclassified_priced_gross_marked_value': unknown_priced,
        'unclassified_gross_marked_value': unknown_gross,
        'unclassified_weight_pct': unknown_gross/gross*100 if positive_gross and unknown_gross is not None else None,
        'classification_coverage_pct': known_priced/gross*100 if positive_gross and known_priced is not None else None,
        'known_sectors': shares, 'records': records,
        'concentration_hhi': hhi, 'maximum_known_sector_weight_pct': maximum_known,
        'maximum_sector_weight_pct': maximum_known if complete_classification else None,
        'known_sector_above_40pct': breach,
        'threshold_basis': 'Strictly above 40% of full gross marked exposure, evaluated before display rounding'}
