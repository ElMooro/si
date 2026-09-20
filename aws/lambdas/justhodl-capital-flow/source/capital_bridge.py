"""Exact accounting bridges between public holdings disclosures, never cash flows."""
from collections import Counter
from fractions import Fraction
import hashlib
import json
import re

CONTRACT = 'capital-holdings-value-bridge.v1'
COMPLETE = 'complete_selected_public_chain'
PERMISSION = {'call': None, 'calls_eligible': False, 'sizing_eligible': False,
              'execution_eligible': False, 'additional_independent_votes': 0}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def exact(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{1,30}(?:\.\d{1,12})?', value):
        raise ValueError('Exact nonnegative disclosure amount required')
    return Fraction(value)


def rounded(value, places=2):
    """Decimal half-even rounding performed with integers, without float drift."""
    scale = 10**places; negative = value < 0
    numerator = abs(value.numerator)*scale; quotient, remainder = divmod(numerator, value.denominator)
    if 2*remainder > value.denominator or (2*remainder == value.denominator and quotient % 2): quotient += 1
    digits = str(quotient).zfill(places+1)
    return ('-' if negative and quotient else '') + (digits[:-places]+'.'+digits[-places:] if places else digits)


def rational(value, places=2):
    return {'numerator': str(value.numerator), 'denominator': str(value.denominator),
            'display_decimal': rounded(value, places)}


def positions(period):
    records = period.get('positions', {})
    if period.get('status') != COMPLETE and records:
        raise ValueError('Incomplete public chain cannot contribute partial holdings')
    for key, row in records.items():
        identity = row['identity']
        if set(identity) != {'cusip', 'class', 'quantity_type', 'put_call'} or hashlib.sha256(encoded(identity)).hexdigest() != key:
            raise ValueError('Native identity differs')
        if identity['quantity_type'] not in ('SH', 'PRN') or identity['put_call'] not in (None, 'PUT', 'CALL'):
            raise ValueError('Reported instrument requires review')
        exact(row['reported_quantity']); exact(row['reported_value_usd'])
    if period.get('status') == COMPLETE:
        total = sum((exact(v['reported_value_usd']) for v in records.values()), Fraction())
        if total != exact(period['reported_value_usd']):
            raise ValueError('Complete reported table value differs')
    return records


def bridge(before, current, allowed, reviewed):
    if not allowed: return {'status': 'comparison_unavailable'}
    if before is None or current is None: return {'status': 'identity_not_reported_in_both_periods'}
    identity = current['identity']
    if identity['quantity_type'] != 'SH' or identity['put_call'] is not None:
        return {'status': 'option_or_principal_scope_not_decomposed'}
    if not reviewed: return {'status': 'reported_value_requires_review'}
    q0, q1 = exact(before['reported_quantity']), exact(current['reported_quantity'])
    if not q0 or not q1: return {'status': 'zero_quantity_unit_value_unavailable'}
    v0, v1 = exact(before['reported_value_usd']), exact(current['reported_value_usd'])
    p0, p1 = v0/q0, v1/q1
    quantity = (q1-q0)*(p1+p0)/2
    unit_value = (p1-p0)*(q1+q0)/2
    assert quantity+unit_value == v1-v0
    rounded_sum = Fraction(rounded(quantity))+Fraction(rounded(unit_value))
    return {'status': 'exact_reported_value_identity',
            'prior_implied_reported_value_per_share_usd': rational(p0, 8),
            'current_implied_reported_value_per_share_usd': rational(p1, 8),
            'quantity_term_usd': rational(quantity), 'unit_value_term_usd': rational(unit_value),
            'reported_value_change_usd': rational(v1-v0),
            'display_rounding_residual_usd': rational(v1-v0-rounded_sum)}


def build_manager(detail):
    """Retain every exact identity and reconcile each instrument scope separately."""
    if detail.get('contract') != 'holdings-native-fund.v1': raise ValueError('Native manager artifact required')
    current_date, prior_date = detail['current_holdings_period'], detail['prior_holdings_period']
    current = detail['periods'].get(current_date, {}); prior = detail['periods'].get(prior_date, {})
    now, before = positions(current), positions(prior)
    complete = current.get('status') == prior.get('status') == COMPLETE
    reviewed = not current.get('valuation_reviews') and not prior.get('valuation_reviews')
    records, scopes = [], {}
    for key in sorted(set(now) | set(before)):
        a, b = now.get(key), before.get(key); identity = (a or b)['identity']
        scope = identity['quantity_type']+'|'+(identity['put_call'] or 'NONE')
        group = scopes.setdefault(scope, {'current': Fraction(), 'prior': Fraction(), 'matched': Fraction(),
                 'newly_disclosed': Fraction(), 'absent': Fraction(), 'quantity': Fraction(),
                 'unit_value': Fraction(), 'rounding': Fraction(), 'not_decomposed': Fraction(), 'rows': 0, 'decomposed_rows': 0})
        group['rows'] += 1
        va, vb = exact(a['reported_value_usd']) if a else Fraction(), exact(b['reported_value_usd']) if b else Fraction()
        group['current'] += va; group['prior'] += vb
        status = ('comparison_unavailable' if not complete else
                  'newly_present_in_public_disclosure' if b is None else
                  'not_present_in_current_public_disclosure' if a is None else 'matched_public_identity')
        calculation = bridge(b, a, complete, reviewed)
        if complete:
            if a is not None and b is not None: group['matched'] += va-vb
            elif b is None: group['newly_disclosed'] += va
            else: group['absent'] += vb
            if calculation['status'] == 'exact_reported_value_identity':
                group['decomposed_rows'] += 1
                # Exact fractions are retained per security. Summing their unrelated
                # denominators creates enormous integers; sum cent-displays instead
                # and retain the explicit adjustment that closes the table bridge.
                quantity_display = Fraction(calculation['quantity_term_usd']['display_decimal'])
                value_display = Fraction(calculation['unit_value_term_usd']['display_decimal'])
                group['quantity'] += quantity_display
                group['unit_value'] += value_display
                group['rounding'] += va-vb-quantity_display-value_display
            elif a is not None and b is not None: group['not_decomposed'] += va-vb
        def observation(row, date):
            if row is None: return None
            return {'quantity': row['reported_quantity'], 'reported_value_usd': row['reported_value_usd'],
                    'native_rows': row['native_rows'], 'detail_pointer': '/periods/'+date+'/positions/'+key}
        records.append({'position_id': key, 'identity': identity,
            'issuer_names': sorted(set((a or {}).get('issuer_names', [])+(b or {}).get('issuer_names', []))),
            'prior': observation(b, prior_date), 'current': observation(a, current_date), 'status': status,
            'reported_quantity_change': rational(exact(a['reported_quantity'])-exact(b['reported_quantity']), 6) if complete and a and b else None,
            'reported_value_change_usd': rational(va-vb) if complete and a and b else None,
            'bridge': calculation})
    summaries = {}
    for scope, group in sorted(scopes.items()):
        if complete:
            delta = group['current']-group['prior']
            assert delta == group['matched']+group['newly_disclosed']-group['absent']
            assert group['matched'] == group['quantity']+group['unit_value']+group['rounding']+group['not_decomposed']
        summaries[scope] = {'identity_count': group['rows'], 'decomposed_identity_count': group['decomposed_rows'],
            'current_reported_value_usd': rational(group['current']) if current.get('status') == COMPLETE else None,
            'prior_reported_value_usd': rational(group['prior']) if prior.get('status') == COMPLETE else None,
            'change_in_reported_table_value_usd': rational(group['current']-group['prior']) if complete else None,
            'matched_reported_value_change_usd': rational(group['matched']) if complete else None,
            'newly_disclosed_reported_value_usd': rational(group['newly_disclosed']) if complete else None,
            'absent_prior_reported_value_usd': rational(group['absent']) if complete else None,
            'sum_display_quantity_terms_usd': rational(group['quantity']) if complete and group['decomposed_rows'] else None,
            'sum_display_unit_value_terms_usd': rational(group['unit_value']) if complete and group['decomposed_rows'] else None,
            'display_rounding_adjustment_usd': rational(group['rounding']) if complete and group['decomposed_rows'] else None,
            'matched_value_change_not_decomposed_usd': rational(group['not_decomposed']) if complete else None}
    return {'contract': CONTRACT, 'fund': detail['fund'], 'cik': detail['cik'], 'official_name': detail['official_name'],
        'current_report_period': current_date, 'prior_report_period': prior_date,
        'current_chain_status': current.get('status', 'not_acquired'), 'prior_chain_status': prior.get('status', 'not_acquired'),
        'comparison_available': complete, 'value_reviews': {'current': current.get('valuation_reviews', []), 'prior': prior.get('valuation_reviews', [])},
        'rows': records, 'record_count': len(records), 'bridge_status_counts': dict(sorted(Counter(v['bridge']['status'] for v in records).items())),
        'scopes': summaries, 'method': 'Exact symmetric arithmetic identity: change in reported quantity times mean implied reported value per share, plus change in implied reported value per share times mean reported quantity.',
        'interpretation': 'An accounting identity, not causal price attribution or transactions. Corporate actions, discretion changes, public omissions and interim trades are unresolved. No cross-manager dollar total.',
        'rounding': 'Exact rational terms are retained per security. Scope totals sum cent-rounded security terms with an explicit adjustment; adding the adjustment, unmatched disclosures and undecomposed changes reconciles the full reported table. Decimal displays use half-even rounding.',
        'corporate_actions_adjusted': False, 'execution_inferred': False, **PERMISSION}
