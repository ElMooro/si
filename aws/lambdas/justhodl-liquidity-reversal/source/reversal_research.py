"""Original native measurements and descriptive turning points, without policy votes."""
import calendar
from datetime import date, datetime, timezone
from decimal import Decimal, localcontext
import re
from report_observations import encoded, digest, decimal, measurement, months_before, baseline_for
from reversal_research_catalog import SERIES

CONTRACT = 'liquidity-reversal-research.v1'
METHOD = 'dated-native-differences.v1'
CURRENT = 'data/liquidity-reversal.json'
PREFIX = 'data/reversal-research/'
WINDOWS = {'D': (20, 60), 'W': (8, 26), 'BW': (4, 13), 'M': (3, 12), 'Q': (3, 6)}
FAMILIES = {
    'fed_balance_sheet': ('WALCL', 'WLCFLL', 'WLCFLPCL', 'SWPT', 'WRESBAL', 'RESPPANWW',
                          'WRBWFRBL', 'WREPO', 'WORAL', 'WLODL', 'WLODLL', 'WSRLL'),
    'treasury_deposits': ('TREASURY', 'WTREGEN', 'WDTGAL'),
    'money_aggregates': ('M2SL', 'BOGMBASE', 'BOGMBBM', 'TOTRESNS', 'RMFSL', 'MMMFFAQ027S', 'MMMFTAQ027S'),
    'h8_bank_balance_sheets': ('CASACBW027SBOG', 'DPSACBW027SBOG', 'TOTBKCR', 'TASACBW027SBOG', 'TMBACBW027SBOG', 'BUSLOANS', 'TOTLL', 'REALLN'),
    'chicago_financial_conditions': ('NFCI', 'NFCILEVERAGE', 'NFCICREDIT', 'NFCIRISK'),
    'other_financial_conditions': ('STLFSI4', 'KCFSI'),
    'nyfed_money_markets': ('SOFR', 'SOFR30DAYAVG', 'SOFRVOL', 'EFFR', 'OBFRVOL', 'RRPONTSYD', 'RPONTSYD', 'RRPONTTLD', 'RPONTTLD'),
    'fed_rates_and_targets': ('DFF', 'FEDFUNDS', 'DFEDTARU', 'IORB', 'DPCREDIT'),
    'fed_broad_dollar': ('DTWEXBGS', 'DTWEXAFEGS', 'DTWEXEMEGS', 'RTWEXBGS'),
}
LEGACY_NUMBERS = ('last', 'dod_pct', 'move_z', 'range_pos_pct', 'n_obs', 'data_age_days',
                  'slope_now_pct', 'slope_prev_pct', 'polarity')
LEGACY_STATES = ('resolved', 'stale', 'basis', 'move_state', 'range_state', 'trend_state',
                 'reversal_state', 'reversal', 'reversal_conf', 'liquidity_reversal_dir')


def clock(stamp):
    value = datetime.fromisoformat(stamp.replace('Z', '+00:00'))
    if value.tzinfo is None: raise ValueError('timezone required')
    return value


def project_inventory(packet):
    """Retain every entry and numeric legacy context, excluding owner list names/prose."""
    rows = packet.get('rows') or []
    if not isinstance(rows, list) or len(rows) > 2000: raise ValueError('inventory bound')
    out = []; seen = set()
    for row in rows:
        symbol = row.get('symbol')
        if not isinstance(symbol, str) or not re.fullmatch(r'[A-Za-z0-9_:.!+*/()=^$&% -]{1,160}', symbol):
            raise ValueError('inventory identity rejected')
        if symbol in seen: raise ValueError('duplicate inventory identity')
        seen.add(symbol); old = {}
        for key in LEGACY_NUMBERS:
            value = decimal(row.get(key)); old[key] = float(value) if value is not None else None
        for key in LEGACY_STATES:
            value = row.get(key)
            old[key] = value if isinstance(value, bool) or (isinstance(value, str) and re.fullmatch(r'[A-Za-z0-9_ -]{1,50}', value)) else None
        out.append({'symbol': symbol, 'legacy': old})
    stamp = packet.get('generated_at') or packet.get('as_of')
    if stamp is not None: clock(stamp)
    return {'contract': 'reversal-inventory.v1', 'legacy_generated_at': stamp, 'rows': out,
            'scope': 'Complete legacy membership and typed numeric context; owner list names and free text excluded'}


def validate_inventory(value):
    rebuilt = project_inventory({'generated_at': value.get('legacy_generated_at'),
        'rows': [{'symbol': r['symbol'], **r['legacy']} for r in value['rows']]})
    if rebuilt != value: raise ValueError('inventory projection differs')


def related_family(sid):
    if sid.startswith('BAML'): return 'ice_bofa_bond_indices'
    for name, ids in FAMILIES.items():
        if sid in ids: return name
    return 'dependence_not_reviewed'


def compatible_pair(new, old, frequency):
    a, b = date.fromisoformat(new['date']), date.fromisoformat(old['date'])
    if frequency == 'D': return 0 < (a-b).days <= 4
    if frequency in ('W', 'BW'): return (a-b).days == (7 if frequency == 'W' else 14)
    return frequency in ('M', 'Q') and months_before(a, 1 if frequency == 'M' else 3) == b


def number(value): return float(value) if value is not None else None
def direction(value): return 'UP' if value > 0 else 'DOWN' if value < 0 else 'FLAT'


def slope(rows):
    """Native units per elapsed calendar day; no division by level or return claim."""
    if len(rows) < 3 or any(decimal(r['value']) is None for r in rows): return None
    first = date.fromisoformat(rows[0]['date'])
    x = [Decimal((date.fromisoformat(r['date'])-first).days) for r in rows]
    y = [decimal(r['value']) for r in rows]; n = Decimal(len(rows))
    mx, my = sum(x)/n, sum(y)/n
    den = sum((v-mx)**2 for v in x)
    return sum((a-mx)*(b-my) for a,b in zip(x,y))/den if den else None


def technical(rows, frequency, unit):
    """Rows remain in descending provider-date order, including null observations."""
    result = {'method': METHOD, 'change_unit': 'percentage_points' if unit == 'Percent' else unit,
        'latest_change': None, 'change_direction': None, 'z_signed': None, 'z_absolute': None,
        'z_n_prior': 0, 'z_mean_decimal': None, 'z_sd_decimal': None, 'z_reason': 'insufficient_prior_changes',
        'z_basis': 'native level differences; at most 90 prior compatible observed intervals, current excluded; sample standard deviation',
        'range_1y': None, 'range_reason': 'calendar_coverage_missing', 'trend': None,
        'publication_time_verified': False, 'strategy_validated': False}
    changes = []
    for i in range(len(rows)-1):
        a, b = rows[i], rows[i+1]; av, bv = decimal(a['value']), decimal(b['value'])
        if av is not None and bv is not None and compatible_pair(a,b,frequency):
            changes.append({'index': i, 'value': av-bv, 'date': a['date'], 'baseline_date': b['date'],
                            'row_index': a['row_index'], 'baseline_row_index': b['row_index']})
    current = changes[0] if changes and changes[0]['index'] == 0 else None
    if current:
        result['latest_change'] = {k:v for k,v in current.items() if k not in ('index','value')}
        result['latest_change'].update(value=number(current['value']), value_decimal=str(current['value']))
        result['change_direction'] = direction(current['value'])
        prior = changes[1:91]; result['z_n_prior'] = len(prior)
        if len(prior) >= 20:
            mean = sum(r['value'] for r in prior)/len(prior)
            sd = (sum((r['value']-mean)**2 for r in prior)/(len(prior)-1)).sqrt()
            result.update(z_mean_decimal=str(mean), z_sd_decimal=str(sd),
                          z_reference_start=prior[-1]['baseline_date'], z_reference_end=prior[0]['date'])
            if sd:
                z = (current['value']-mean)/sd
                result.update(z_signed=number(z), z_absolute=number(abs(z)), z_reason=None)
            else: result['z_reason'] = 'constant_prior_changes'
    else: result['z_reason'] = 'latest_pair_missing_or_incompatible_interval'
    if rows and decimal(rows[0]['value']) is not None:
        base, target, _ = baseline_for(rows, frequency, 12)
        window = [r for r in rows if r['date'] >= target.isoformat()]
        values = [decimal(r['value']) for r in window if decimal(r['value']) is not None]
        if base and len(values) >= 2:
            lo, hi = min(values), max(values)
            result['range_1y'] = {'start_cutoff':target.isoformat(),'end':rows[0]['date'],
                'first_observation':window[-1]['date'],'numeric_rows':len(values),
                'missing_rows':len(window)-len(values),'min_decimal':str(lo),'max_decimal':str(hi),
                'position_pct':number(100*(decimal(rows[0]['value'])-lo)/(hi-lo)) if hi>lo else None,
                'complete_exchange_calendar_verified':False,'basis':'returned observations inside trailing 12 calendar months'}
            result['range_reason'] = None if hi>lo else 'constant_range'
    if frequency not in WINDOWS: return result
    short, long = WINDOWS[frequency]; needed = max(long+7, 2*short)
    segment = rows[:needed]
    if len(segment) < needed or any(decimal(r['value']) is None for r in segment): return result
    if any(not compatible_pair(segment[i],segment[i+1],frequency) for i in range(len(segment)-1)): return result
    now_slope = slope(list(reversed(segment[:short])))
    previous_slope = slope(list(reversed(segment[short:2*short])))
    if now_slope is None or previous_slope is None: return result
    reversal = 'UP' if previous_slope<0<now_slope else 'DOWN' if previous_slope>0>now_slope else None
    spreads = [sum(decimal(r['value']) for r in segment[back:back+short])/short -
               sum(decimal(r['value']) for r in segment[back:back+long])/long for back in range(7)]
    cross = None
    for back in range(6):
        a,b = spreads[back],spreads[back+1]
        crossed = 'UP' if b<=0<a else 'DOWN' if b>=0>a else None
        if crossed:
            cross={'direction':crossed,'observed_intervals_ago':back,'date':segment[back]['date']};break
    result['trend'] = {'short_observations':short,'long_observations':long,
        'slope_now':number(now_slope),'slope_previous':number(previous_slope),
        'slope_unit':result['change_unit']+' per calendar day',
        'current_window':{'start':segment[short-1]['date'],'end':segment[0]['date']},
        'previous_window':{'start':segment[2*short-1]['date'],'end':segment[short]['date']},
        'direction':direction(spreads[0]),'slope_direction_change':reversal,'most_recent_ma_cross':cross,
        'matching_ma_cross':bool(reversal and cross and cross['direction']==reversal),
        'interpretation':'Descriptive slope sign change and moving-average relation; not a forecast or confirmed policy turn'}
    return result


def compile_native(sid, original, source, generated_at):
    observed = source['measurements'][sid]
    rebuilt = measurement(sid, original['definition'], original['observations'], original['evidence'],
                          source['generated_at'], original['acquired_at'])
    if rebuilt != observed: raise ValueError('canonical native reconstruction differs: '+sid)
    current = measurement(sid, original['definition'], original['observations'], original['evidence'],
                          generated_at, original['acquired_at'])
    rows = [{'date':r['date'],'value':str(decimal(r.get('value'))) if decimal(r.get('value')) is not None else None,'row_index':i}
            for i,r in enumerate(original['observations']['observations']) if r['date']<=generated_at[:10]]
    rows.sort(key=lambda r:r['date'],reverse=True)
    stats = technical(rows,current['frequency'],current['unit'])
    valid = current['quality']['status']=='fresh'
    period = date.fromisoformat(current['date'])
    end_month = ((period.month-1)//3+1)*3 if current['frequency']=='Q' else period.month
    period_end = date(period.year,end_month,calendar.monthrange(period.year,end_month)[1]) if current['frequency'] in ('M','Q') else period
    current['period_end'] = period_end.isoformat()
    if period_end>clock(generated_at).date():
        valid=False;current['quality']={**current['quality'],'status':'incomplete_measurement_period'}
    reason = 'discontinued_monthly_treasury_deposits' if sid=='TREASURY' else None
    if reason: valid=False;current['quality']={**current['quality'],'status':'historical_discontinued'}
    return {'measurement':current,'technical':stats,'measurement_eligible':valid,
        'original_provider_verified':True,'scope_note':reason,'evidence_family':related_family(sid),
        'family_independence_verified':False,'call':None,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False}


def build(source, originals, inventory, generated_at, fails_context=None, legacy_ref=None):
    clock(generated_at); validate_inventory(inventory)
    if source.get('contract')!='report-observations.v1' or clock(source['generated_at'])>clock(generated_at):
        raise ValueError('canonical source contract or clock differs')
    rows = []; members={r['symbol']:r for r in inventory['rows']}
    symbols=list(members)+['FRED:'+sid for sid in SERIES if 'FRED:'+sid not in members]
    with localcontext() as ctx:
        ctx.prec=36
        for symbol in symbols:
            row={'symbol':symbol,'name':symbol,'inventory_entry':symbol in members,
                 'legacy':members.get(symbol,{}).get('legacy'),'legacy_generated_at':inventory['legacy_generated_at'] if symbol in members else None,
                 'status':'unqualified_legacy' if symbol in members else 'native_original_unavailable','last':None,'unit':None,'date':None,'resolved':False,
                 'move_z':None,'move_z_signed':None,'dod_pct':None,'trend_state':None,'reversal_state':None,
                 'reversal_conf':None,'range_pos_pct':None,'polarity':None,'liquidity_reversal_dir':None,
                 'measurement_eligible':False,'original_provider_verified':False,'call':None,
                 'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False}
            sid=symbol[5:] if re.fullmatch(r'FRED:[A-Z0-9_]+',symbol) else None
            if sid in SERIES and sid in source['measurements']:
                original=originals.get(sid)
                if not original: raise ValueError('native source not retained: '+sid)
                native=compile_native(sid,original,source,generated_at);m=native['measurement'];t=native['technical']
                row.update(native); row.update(name=m['name'],unit=m['unit'],date=m['date'],
                    status=m['quality']['status'],resolved=True,last=m['current'],data_age_days=m['quality']['observation_age_days'],
                    stale=not native['measurement_eligible'])
                if native['measurement_eligible']:
                    row.update(move_z=t['z_absolute'],move_z_signed=t['z_signed'],
                        trend_state=(t['trend'] or {}).get('direction'),
                        reversal_state=(t['trend'] or {}).get('slope_direction_change'),
                        range_pos_pct=(t['range_1y'] or {}).get('position_pct'))
                row['decision_id']='reversal-'+digest({'generated_at':generated_at,'row':row})
            rows.append(row)
    native_rows=[r for r in rows if r['original_provider_verified']]
    eligible=[r for r in native_rows if r['measurement_eligible']]
    families={}
    for row in native_rows: families.setdefault(row['evidence_family'],[]).append(row['symbol'])
    return {'contract':CONTRACT,'method':METHOD,'engine':'justhodl-liquidity-reversal','version':'2.0.0',
        'generated_at':generated_at,'as_of':generated_at,'source_generated_at':source['generated_at'],
        'source_replay':source['replay'],'inventory':{'entries':len(members),'legacy_generated_at':inventory['legacy_generated_at'],
            'legacy_ref':legacy_ref,'all_entries_retained':True,'additional_registered_native_identities':len(symbols)-len(members)},
        'rows':rows,'n_members':len(members),'n_resolved':len(native_rows),'n_with_history':len(native_rows),
        'quality':{'status':'partial_verified_coverage' if eligible else 'unavailable',
            'original_verified_series':len(native_rows),'current_measurements':len(eligible),
            'unqualified_inventory_entries':sum(r['inventory_entry'] and not r['original_provider_verified'] for r in rows),
            'registered_native_identities':len(SERIES),'missing_native_identities':sorted(set(SERIES)-{r['symbol'][5:] for r in native_rows}),
            'release_calendar_verified':False,'publication_time_verified':False},
        'dependency_map':{'groups':[{'family':k,'members':v,'independence_verified':False} for k,v in sorted(families.items())],
            'effective_independent_inputs':None,'scope':'Reviewed overlapping families, not a complete causal dependency graph'},
        'liquidity':{'trend_score':None,'trend_label':'NOT_ATTRIBUTED','reversal_score':None,
            'reversal_label':'NOT_CALIBRATED','top_reversals':[],
            'reason':'No validated mapping from these overlapping stock/rate changes to monetary policy or portfolio action'},
        'barometer':{'value':None,'label':'NOT_CALIBRATED'},'strip':{'alarm':'ABSTAIN','top_movers':[]},
        'pd_settlement_fails':fails_context,'call':None,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'decision':{'verb':'WAIT','meaning':'abstain','portfolio_change':None},
        'portfolio_consequences':{'status':'not_identified','expected_return':None,'allocation':None,
            'required':['validated_strategy','point_in_time_history','costs_and_exposure_model','authorized_portfolio_snapshot']},
        'limitations':['Current-vintage observations cannot establish historical publication-time knowledge',
            'Descriptive z scores and slope changes are not crisis probabilities or policy actions',
            'No daily SOFR minus monthly FEDFUNDS synthetic spread; source identities remain separate',
            'Unqualified legacy measurements remain visible with their original engine date; no new owner-context reads'],
        'paid_ai_calls':0,'notifications_sent':0,'private_account_reads':0,'portfolio_writes':0}
