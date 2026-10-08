"""Treasury auction identity and quote comparability; no inferred TIPS flags."""
CONTRACT_VERSION = 'treasury-instrument.v1'


def _flag(value):
    text = str(value).strip().lower()
    return True if text in ('yes', 'true', '1') else False if text in ('no', 'false', '0') else None


def instrument_fields(row, provider):
    """Preserve original security type while declaring its economic quote basis."""
    td = provider == 'treasurydirect'
    kind = str(row.get('securityType' if td else 'security_type') or '').strip()
    tips = _flag(row.get('tips' if td else 'inflation_index_security'))
    floating = _flag(row.get('floatingRate' if td else 'floating_rate'))
    conflict = (tips is True and floating is True) or (kind.upper() == 'TIPS' and (tips is False or floating is True)) or (kind.upper() == 'FRN' and (floating is False or tips is True))
    if conflict:
        instrument = 'UNKNOWN'
    elif tips is True or kind.upper() == 'TIPS':
        instrument = 'TIPS'
    elif floating is True or kind.upper() == 'FRN':
        instrument = 'FRN'
    elif kind == 'Bill' and tips is not True and floating is not True:
        instrument = 'BILL'
    elif kind in ('Note', 'Bond') and tips is False and floating is False:
        instrument = 'NOMINAL_COUPON'
    else:
        instrument = 'UNKNOWN'
    return {
        'instrument_contract': CONTRACT_VERSION,
        'instrument_kind': instrument,
        'instrument_classification_status': 'verified' if instrument != 'UNKNOWN' else 'unverified',
        'tips': tips, 'floating_rate': floating,
        'original_term': row.get('originalSecurityTerm' if td else 'original_security_term'),
        'quote_basis': {'TIPS': 'real_yield_pct', 'FRN': 'discount_margin_pct',
                        'BILL': 'investment_rate_pct', 'NOMINAL_COUPON': 'nominal_yield_pct'}.get(instrument),
    }


def cohort_term(row):
    """Comparable-term bucket.

    Bills compare on the auctioned (remaining) term: a 4-week reopening of a
    26-week CUSIP trades like a 4-week bill. Coupons, TIPS and FRNs compare on
    the original security term: a 29-year-10-month reopening is a 30-year
    auction and a 9-year-11-month reopening is a 10-year auction. Remaining
    terms vary month to month and would otherwise split one 30-year programme
    into cohorts too small to grade. Rows without a retained original term
    fall back to the auctioned term.
    """
    term = row.get('term')
    if row.get('instrument_kind') == 'BILL':
        return term
    original = row.get('original_term')
    return original if type(original) is str and original.strip() else term


def comparable_cohort(row):
    """Unknown legacy rows cannot establish a comparable demand distribution."""
    kind = row.get('instrument_kind')
    if row.get('instrument_contract') != CONTRACT_VERSION or kind not in ('TIPS', 'FRN', 'BILL', 'NOMINAL_COUPON'):
        return None
    return kind, cohort_term(row), bool(row.get('reopening'))


def nominal_par_eligible(row):
    return comparable_cohort(row) is not None and row.get('instrument_kind') in ('BILL', 'NOMINAL_COUPON')
