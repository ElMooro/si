"""Calendar and currency arithmetic for a declared three-central-bank subtotal.

Current-retrieved observations only, never a historical publication-time signal.
"""
import calendar
from datetime import date, timedelta
from decimal import Decimal, localcontext

POLICY = {
    'WALCL': ('Millions of U.S. Dollars', 'W', '1', None, 14),
    'ECBASSETSW': ('Millions of Euros', 'W', '1', 'DEXUSEU', 14),
    'JPNASSETS': ('100 Million Yen', 'M', '100', 'DEXJPUS', 62),
    'DEXUSEU': ('U.S. Dollars to One Euro', 'D', None, None, 10),
    'DEXJPUS': ('Japanese Yen to One U.S. Dollar', 'D', None, None, 10),
}


def effective_date(sid, label):
    result = date.fromisoformat(label)
    if result.isoformat() != label:
        raise ValueError('canonical observation label required')
    if sid == 'JPNASSETS':
        if result.day != 1:
            raise ValueError('monthly period label must begin on day one')
        result = result.replace(day=calendar.monthrange(result.year, result.month)[1])
    return result


def series_rows(sid, original, measurement):
    expected_unit, frequency, *_ = POLICY[sid]
    if measurement['unit'] != expected_unit or measurement['frequency'] != frequency:
        return {'status': 'unreviewed_definition', 'rows': [], 'reason': 'Native unit/frequency differs from the reviewed conversion'}
    if sid == 'JPNASSETS' and measurement['definition'].get('frequency') != 'Monthly, End of Period':
        return {'status': 'unreviewed_period_basis', 'rows': []}
    rows = []; seen = set()
    for index, row in enumerate(original['observations']['observations']):
        day = effective_date(sid, row['date'])
        if day in seen:
            raise ValueError('duplicate effective observation date')
        seen.add(day)
        raw = row.get('value')
        if isinstance(raw, bool): raise ValueError('boolean is not a native observation')
        value = None if raw in (None, '.', '') else Decimal(str(raw))
        if value is not None and not value.is_finite():
            raise ValueError('finite native value required')
        rows.append({'observation_label': row['date'], 'effective_observation_date': day.isoformat(),
                     'native_decimal': str(value) if value is not None else None, 'source_row': index})
    return {'status': 'reviewed_definition', 'rows': sorted(rows, key=lambda r: r['effective_observation_date'], reverse=True)}


def select(sid, series, target):
    target_day = date.fromisoformat(target)
    result = {'status': 'unavailable', 'valuation_date': target, 'selected': None,
              'max_carry_age_days': POLICY[sid][4], 'period_policy': 'month_end' if sid == 'JPNASSETS' else 'provider_date'}
    if series.get('status') != 'reviewed_definition':
        result['reason'] = series.get('status', 'source_missing'); return result
    eligible = [row for row in series['rows'] if row['effective_observation_date'] <= target]
    if not eligible:
        result['reason'] = 'no_observation_on_or_before_valuation'; return result
    row = max(eligible, key=lambda r: r['effective_observation_date'])
    age = (target_day-date.fromisoformat(row['effective_observation_date'])).days
    result.update(selected=row, carry_age_days=age)
    if row['native_decimal'] is None:
        result['reason'] = 'latest_observation_missing'; return result
    if age > POLICY[sid][4]:
        result['reason'] = 'observation_exceeds_carry_limit'; return result
    value = Decimal(row['native_decimal'])
    if value < 0 or (sid.startswith('DEX') and value == 0):
        result['reason'] = 'invalid_balance_or_exchange_rate'; return result
    result.update(status='descriptive', reason='Dated current-retrieved observation; historical publication time unverified')
    return result


def subtotal(series, target):
    components = {}; missing = []
    with localcontext() as context:
        context.prec = 28
        for sid in ('WALCL', 'ECBASSETSW', 'JPNASSETS'):
            unit, _, factor, fx_sid, _ = POLICY[sid]
            row = select(sid, series.get(sid, {}), target)
            fx = select(fx_sid, series.get(fx_sid, {}), target) if fx_sid else None
            component = {'series_id': sid, 'native_unit': unit, 'native_multiplier_to_millions': factor,
                         'balance': row, 'fx_series_id': fx_sid, 'fx': fx,
                         'local_millions_decimal': None, 'usd_per_native_currency_decimal': None,
                         'usd_millions_decimal': None}
            if row['status'] != 'descriptive' or (fx and fx['status'] != 'descriptive'):
                missing.append(sid)
            else:
                local = Decimal(row['selected']['native_decimal'])*Decimal(factor)
                rate = Decimal(fx['selected']['native_decimal']) if fx else Decimal(1)
                if fx_sid == 'DEXJPUS': rate = Decimal(1)/rate
                component.update(local_millions_decimal=str(local), usd_per_native_currency_decimal=str(rate),
                                 usd_millions_decimal=str(local*rate))
            components[sid] = component
        total = None if missing else sum(Decimal(c['usd_millions_decimal']) for c in components.values())
    return {'status': 'incomplete' if missing else 'descriptive', 'valuation_date': target,
            'total_usd_millions_decimal': str(total) if total is not None else None,
            'components': components, 'missing_components': missing,
            'scope': 'Fed, Eurosystem and Bank of Japan balance-sheet subtotal. No other central banks, bank credit or market liquidity included.'}


def endpoint_change(history, end, weeks):
    if weeks not in (13, 52): raise ValueError('reviewed horizon required')
    start = (date.fromisoformat(end)-timedelta(weeks=weeks)).isoformat()
    out = {'status': 'missing_endpoint', 'start': start, 'end': end, 'calendar_days': 7*weeks,
           'change_usd_millions_decimal': None, 'relative_percent_decimal': None,
           'balance_effect_usd_millions_decimal': None, 'fx_effect_usd_millions_decimal': None,
           'rounding_residual_usd_millions_decimal': None, 'components': {},
           'formula': 'Balance effect=(local1-local0)*FX0; FX effect=local1*(FX1-FX0), with FX in USD per native currency unit. Decimal rounding residual is reported.',
           'scope': 'Arithmetic USD valuation decomposition, not causal flows; endpoints exactly 13 or 52 calendar weeks apart.'}
    a, b = history.get(start), history.get(end)
    if not a or not b or a['status'] != 'descriptive' or b['status'] != 'descriptive': return out
    with localcontext() as ctx:
        ctx.prec = 28
        balance = Decimal(0); fx = Decimal(0)
        for sid in ('WALCL', 'ECBASSETSW', 'JPNASSETS'):
            old, new = a['components'][sid], b['components'][sid]
            local0, local1 = (Decimal(r['local_millions_decimal']) for r in (old, new))
            fx0, fx1 = (Decimal(r['usd_per_native_currency_decimal']) for r in (old, new))
            change = Decimal(new['usd_millions_decimal'])-Decimal(old['usd_millions_decimal'])
            balance_leg = (local1-local0)*fx0; fx_leg = local1*(fx1-fx0)
            balance += balance_leg; fx += fx_leg
            out['components'][sid] = {'change_usd_millions_decimal': str(change),
                'balance_effect_usd_millions_decimal': str(balance_leg), 'fx_effect_usd_millions_decimal': str(fx_leg),
                'rounding_residual_usd_millions_decimal': str(change-balance_leg-fx_leg)}
        baseline = Decimal(a['total_usd_millions_decimal'])
        change = Decimal(b['total_usd_millions_decimal'])-baseline
        out.update(status='descriptive', change_usd_millions_decimal=str(change),
                   relative_percent_decimal=str(100*change/baseline) if baseline>0 else None,
                   balance_effect_usd_millions_decimal=str(balance), fx_effect_usd_millions_decimal=str(fx),
                   rounding_residual_usd_millions_decimal=str(change-balance-fx))
    return out
