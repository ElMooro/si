"""Deterministic Taiwan exchange research, original rows and observed-date windows."""
from datetime import timedelta
from decimal import Decimal, localcontext
import hashlib
import json
import hot_native as native
import pd_fails_context

CONTRACT = 'hot-money-research.v1'
METHOD = 'exchange-originals.v3'
BOARDS = ('twse', 'tpex')


def encoded(value): return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
def digest(value): return hashlib.sha256(encoded(value)).hexdigest()
def bn(value): return float(Decimal(value) / Decimal(10**9))


def quality(row, stamp, failed=False):
    now = native.clock(stamp); observed = native.day(row['date']) if row else None
    age = (now.astimezone(native.TZ).date() - observed).days if observed else None
    acquired_age = (now - native.clock(row['acquired_at'])).total_seconds() if row else None
    state = 'unavailable' if not row else 'invalid' if age < 0 or acquired_age < 0 else 'stale' if age > 5 or acquired_age > 26*3600 else 'degraded' if failed else 'fresh'
    return {'status': state, 'observation_date': observed.isoformat() if observed else None,
            'acquired_at': row['acquired_at'] if row else None, 'age_days': age, 'acquisition_age_seconds': acquired_age,
            'max_age_days': 5, 'max_acquisition_age_hours': 26, 'clock': 'Asia/Taipei',
            'exchange_calendar_complete': False, 'note': 'Five calendar days is a bounded age policy, not proof of the latest exchange session.'}


def board_view(board, points, legacy_rows, stamp, acquisition_failed):
    days = sorted(points); latest = points[days[-1]] if days else None
    q = quality(latest, stamp, acquisition_failed); live = q['status'] == 'fresh'
    out = {'status': 'LIVE' if live else q['status'].upper(), 'quality': q, 'unit': 'TWD bn',
           'scope': native.SCOPES[board], 'source': board.upper(), 'latest_day': days[-1] if days else None,
           'latest_bn': bn(latest['net_twd']) if live else None, 'last_observed_bn': bn(latest['net_twd']) if latest else None,
           'latest': latest, 'ledger_days': len(set(days) | set(legacy_rows)), 'verified_observations': len(days),
           'legacy_unverified_observations': len(set(legacy_rows) - set(days)),
           'window_basis': 'last N original-verified reported dates; no exchange-calendar completeness claim',
           'windows': {}, 'z_60d': None, 'z_60_observations': None, 'z_definition': None,
           'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False}
    for n in (5, 20, 60):
        chosen = days[-n:]; start = chosen[0] if chosen else None; end = chosen[-1] if chosen else None
        span = (native.day(end) - native.day(start)).days if chosen else None
        holes = sorted(d for d in legacy_rows if chosen and start <= d <= end and d not in points)
        sufficient = live and len(chosen) == n and span <= 2*n+14 and not holes
        value = sum(int(points[d]['net_twd']) for d in chosen) if sufficient else None
        out['sum_%dd_bn' % n] = None
        out['sum_%dobs_bn' % n] = bn(value) if value is not None else None
        out['windows'][str(n)] = {'status': 'observed_window' if sufficient else 'insufficient_or_stale',
            'observations': len(chosen), 'start': start, 'end': end, 'calendar_span_days': span,
            'known_unverified_dates': holes, 'dates': chosen, 'sum_twd': str(value) if value is not None else None,
            'sum_bn': out['sum_%dobs_bn' % n], 'exchange_calendar_complete': False}
    chosen = days[-61:]
    holes = [d for d in legacy_rows if chosen and chosen[0] <= d <= chosen[-1] and d not in points]
    if live and len(chosen) == 61 and not holes and (native.day(chosen[-1])-native.day(chosen[0])).days <= 136:
        with localcontext() as ctx:
            ctx.prec = 34
            values = [Decimal(points[d]['net_twd']) for d in chosen]
            mean = sum(values[:-1]) / 60
            variance = sum((v-mean)**2 for v in values[:-1]) / 59
            std = variance.sqrt()
            z = (values[-1]-mean)/std if std else None
            out['z_60_observations'] = float(z) if z is not None else None
            out['z_definition'] = {'status': 'descriptive' if z is not None else 'constant_history',
                'baseline_dates': chosen[:-1], 'current_date': chosen[-1], 'sample_size': 60, 'ddof': 1,
                'mean_twd_decimal': str(mean), 'sample_std_twd_decimal': str(std),
                'z_decimal': str(z) if z is not None else None, 'clipped': False,
                'note': 'Current observation excluded from baseline. A descriptive z-score is not a return forecast.'}
    out['history'] = [points[d] for d in days]
    return out


def combined_view(tw, otc):
    combined = {'status': 'UNAVAILABLE', 'latest_bn': None, 'latest_day': None, 'unit': 'TWD bn',
                'windows': {}, 'scope': 'sum of the two separately defined board totals; not all Taiwan instruments',
                'note': 'Same-date exchange net purchases; not cross-border cash settlement or TIC/BOP.',
                'calls_eligible': False, 'sizing_eligible': False}
    if tw['status'] == otc['status'] == 'LIVE' and tw['latest_day'] == otc['latest_day']:
        total = int(tw['latest']['net_twd']) + int(otc['latest']['net_twd'])
        combined.update(status='LIVE', latest_day=tw['latest_day'], latest_bn=bn(total), latest_twd=str(total))
    elif tw['latest_day'] and otc['latest_day'] and tw['latest_day'] != otc['latest_day']:
        combined.update(status='MISALIGNED', why='Exchange observation dates differ')
    for n in (5, 20, 60):
        a, b = tw['windows'][str(n)], otc['windows'][str(n)]
        good = (combined['status'] == 'LIVE' and min(tw['verified_observations'], otc['verified_observations']) >= 60
                and a['status'] == b['status'] == 'observed_window' and a['dates'] == b['dates'])
        total = int(a['sum_twd']) + int(b['sum_twd']) if good else None
        combined['windows'][str(n)] = {'status': 'observed_window' if good else 'insufficient_or_misaligned',
            'sum_twd': str(total) if total is not None else None, 'sum_bn': bn(total) if total is not None else None,
            'dates': a['dates'] if good else [], 'minimum_verified_per_board': 60, 'exchange_calendar_complete': False}
    return combined


def fails_context(document, stamp):
    result = pd_fails_context.project(document, native.clock(stamp))
    try: age = (native.clock(stamp)-native.clock(document['generated_at'])).total_seconds()
    except (KeyError, ValueError, TypeError, AttributeError): age = None
    for row, source, total_key in ((result, document.get('treasury', {}), 'gross_bn'),
                                   (result['ust_ex_tips'], document.get('headline', {}), 'combined_bn')):
        units = source.get('field_units') or {}
        valid_units = all(units.get(k) == 'usd_bn' for k in ('ftd_bn', 'ftr_bn', total_key))
        values = [row[k] for k in ('ftd_bn', 'ftr_bn', 'combined_bn')]
        valid_sum = all(v is not None for v in values) and abs(Decimal(str(values[0]))+Decimal(str(values[1]))-Decimal(str(values[2]))) <= Decimal('.02')
        row['reconciliation'] = {'consistent': valid_sum, 'tolerance_usd_bn': '.02'}
        if not valid_sum: row['quality']['status'] = 'inconsistent_or_missing_scope'
        if not valid_units:
            row['retained_unqualified_values'] = {k: row[k] for k in ('ftd_bn', 'ftr_bn', 'combined_bn')}
            row.update(ftd_bn=None, ftr_bn=None, combined_bn=None); row['quality']['status'] = 'unverified_units'
        if row['quality']['status'] == 'fresh' and (age is None or not 0 <= age <= 26*3600): row['quality']['status'] = 'stale_source'
        row['original_provider_verified'] = False; row['role'] = 'retained_engine_context'
    return result


def build(inputs, originals):
    state = inputs['state']; stamp = inputs['generated_at']; points = {b: {} for b in BOARDS}
    if native.clock(state['generated_at']) > native.clock(stamp): raise ValueError('source clock after generation')
    for board in BOARDS:
        for date, stored in state['boards'][board].items():
            descriptor = stored['descriptor']; row = native.parse(board, originals[descriptor['evidence']['key']], descriptor)
            if row['date'] != date: raise ValueError('ledger date differs from original')
            revisions = []
            for previous in stored.get('revisions', []):
                old = native.parse(board, originals[previous['evidence']['key']], previous)
                if old['date'] != date: raise ValueError('revision date differs')
                revisions.append({'acquired_at': old['acquired_at'], 'net_twd': old['net_twd'], 'original': old['original']})
            row['prior_captures'] = revisions
            old_value = inputs['legacy_rows'].get(board, {}).get(date)
            row['legacy_comparison'] = {'status': 'no_legacy_value' if old_value is None else 'matches' if old_value == row['net_twd'] else 'differs',
                'legacy_value_twd': old_value, 'difference_twd': str(int(row['net_twd'])-int(old_value)) if old_value is not None else None,
                'legacy_original_verified': False}
            points[board][date] = row
    comparison = {'status': 'unavailable', 'independent_evidence': False}
    probe = inputs.get('openapi')
    if probe:
        api = native.parse('tpex_openapi', originals[probe['evidence']['key']], probe)
        matching = points['tpex'].get(api['date'])
        if matching:
            same = all(api[k] == matching[k] for k in ('buy_twd', 'sell_twd', 'net_twd'))
            comparison = {'status': 'agree' if same else 'disagree', 'date': api['date'],
                'openapi_original': api['original'], 'daily_report_original': matching['original'],
                'independent_evidence': False, 'note': 'Two copies of the same TPEx report, one evidence family.'}
    tw, otc = [board_view(b, points[b], inputs['legacy_rows'].get(b, {}), stamp,
                bool(inputs['current_errors'].get(b)) or (b == 'tpex' and comparison['status'] == 'disagree')) for b in BOARDS]
    tw.update(specialty='semiconductors', otc=otc, combined=combined_view(tw, otc))
    fails = fails_context(inputs.get('fails_context') or {}, stamp)
    fresh = sum(v['quality']['status'] == 'fresh' for v in (tw, otc))
    return {'contract': CONTRACT, 'v': '3.0.0', 'engine': 'justhodl-hot-money', 'methodology_version': METHOD,
        'generated_at': stamp, 'source_generated_at': state['generated_at'],
        'as_of': native.clock(stamp).astimezone(native.TZ).date().isoformat(), 'units': 'TWD_bn',
        'status': 'LIVE' if fresh == 2 else 'PARTIAL' if fresh else 'INSUFFICIENT_DATA', 'countries': {'taiwan': tw},
        'deferred': {'korea': {'status': 'DEFERRED', 'why': 'No validated provider feed; excluded from measured coverage.'}},
        'quality': {'status': 'fresh' if fresh == 2 else 'degraded' if fresh else 'unavailable',
            'fresh_boards': fresh, 'expected_boards': 2, 'calendar_completeness_verified': False,
            'original_verified_rows': {b: len(points[b]) for b in BOARDS}},
        'doctrine': 'Exchange foreign net purchases in TWD, separate from cross-border cash settlement and monthly or quarterly TIC/BOP.',
        'source_comparisons': {'tpex': comparison}, 'dependency_groups': ['TWSE.foreign-trading', 'TPEx.foreign-trading'],
        'legacy_context': inputs['legacy_context'], 'acquisition_errors': inputs['current_errors'],
        'backfill_errors': inputs['backfill_errors'], 'pd_settlement_fails': fails,
        'call': None, 'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False,
        'decision': {'verb': 'WAIT', 'meaning': 'abstain', 'portfolio_change': None,
            'reason': 'Exchange-trading measurements have no validated return forecast or position-size model. WAIT does not mean liquidate.'}}
