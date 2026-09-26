"""Pure, complete original-source curve candidate; no IO or investment authority."""
from copy import deepcopy
from datetime import date
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
import math, re
import report_observations as observations
from research_brief_model import clock
from yield_curve_catalog import (SERIES,SPECS,NOMINAL,REAL,METRICS,LAGS,
    MAX_OBSERVATION_DAYS,MAX_ACQUISITION_SECONDS,METHOD_SOURCES,DEFINITION_NOTES)

CONTRACT = 'yield-curve-candidate.v1'
PRIVATE = 'audit-private/20260909-originals/yield-curve-research/'
AUTHORITY = {k:False for k in ('calls_eligible','sizing_eligible','execution_eligible',
    'forecast_qualified','point_in_time_backtest_qualified','publication_eligible')}


def scalar(number):
    display = float(number) if number is not None else None
    if display is not None and not math.isfinite(display):
        raise ValueError('Nonfinite curve display')
    return {'value':display,'exact_decimal':str(number) if number is not None else None}


def original_ref(ref):
    return (isinstance(ref,dict) and bool(re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256'))))
        and ref.get('key') == PRIVATE+ref['sha256']+'.bin' and type(ref.get('bytes')) is int
        and 0 < ref['bytes'] <= 64*1024*1024)


def history(original):
    result = []; seen = set()
    for index,row in enumerate(original['observations']['observations']):
        day = date.fromisoformat(row['date']); value = observations.decimal(row.get('value'))
        if str(day) != row['date'] or day in seen:
            raise ValueError('Noncanonical or duplicate observation date')
        if row.get('value') not in (None,'.','') and value is None:
            raise ValueError('Invalid original numeric value')
        if value is not None:
            scalar(value)
            if len(value.as_tuple().digits)>28 or not -28 <= value.as_tuple().exponent <= 12:
                raise ValueError('Original precision outside reviewed decimal boundary')
        seen.add(day)
        result.append({'original_row':index,'observation_date':row['date'],'native_value':row.get('value'),
            'realtime_start':row.get('realtime_start'),'realtime_end':row.get('realtime_end')})
    return result


def measure(source, originals, generated_at):
    if (source.get('contract') != observations.CONTRACT or source.get('calls_eligible') is not False
        or source.get('sizing_eligible') is not False or set(originals) != set(SERIES)):
        raise ValueError('Complete descriptive canonical input inventory required')
    ref = source.get('replay') or {}
    if (not re.fullmatch(r'data/report-research/runs/[a-f0-9]{64}\.json',ref.get('manifest_key',''))
        or ref.get('output_sha256') != observations.digest({k:v for k,v in source.items() if k!='replay'})):
        raise ValueError('Exact canonical output identity required')
    now = clock(generated_at); source_time = clock(source['generated_at'])
    source_age = (now-source_time).total_seconds()
    if source_age < 0: raise ValueError('Future source publication')
    result = {}
    for sid,spec in SPECS.items():
        original = originals[sid]; m = source.get('measurements',{}).get(sid)
        if (original is None) != (m is None): raise ValueError('Original availability differs: '+sid)
        rows = []; definition = {}; state = 'unavailable'; value = None
        reviewed = False; acquired = None; latest_date = None; index = None
        observation_age = acquisition_age = None
        if original is not None:
            # Reconstruct all canonical fields, not just a convenient current value.
            with localcontext() as canonical_precision:
                canonical_precision.prec=28; canonical_precision.rounding=ROUND_HALF_EVEN
                rebuilt=observations.measurement(sid,original['definition'],original['observations'],
                    original['evidence'],source['generated_at'],original['acquired_at'])
            if rebuilt != m: raise ValueError('Whole canonical reconstruction differs: '+sid)
            rows = history(original); definition = original['definition']['seriess'][0]
            reviewed = (definition.get('units'),definition.get('frequency_short'),definition.get('frequency'),
                definition.get('seasonal_adjustment')) == spec['reviewed_definition']
            acquired = original['acquired_at']; acquisition_age = (now-clock(acquired)).total_seconds()
            eligible = sorted((r for r in rows if r['observation_date'] <= str(source_time.date())),
                key=lambda r:r['observation_date'])
            latest = eligible[-1] if eligible else None
            if latest:
                latest_date=latest['observation_date']; index=latest['original_row']
                value=observations.decimal(latest['native_value'])
                observation_age=(now.date()-date.fromisoformat(latest_date)).days
            state = ('definition_mismatch' if not reviewed else 'unavailable' if value is None else
                'stale_observation' if not 0 <= observation_age <= MAX_OBSERVATION_DAYS else
                'stale_source' if not 0 <= acquisition_age <= MAX_ACQUISITION_SECONDS or source_age>MAX_ACQUISITION_SECONDS else
                'source_unusable' if m['quality']['status']!='fresh' else 'within_age_ceiling')
        result[sid] = {'series_id':sid,**deepcopy(spec),'source_definition':deepcopy(definition) or None,
            'definition_reviewed':reviewed,'source_url':'https://fred.stlouisfed.org/series/'+sid,
            'unit':definition.get('units'),'frequency':definition.get('frequency_short'),
            'seasonal_adjustment':definition.get('seasonal_adjustment'),
            'current':scalar(value if state=='within_age_ceiling' else None),'last_observed':scalar(value),
            'latest_date':latest_date,'original_row':index,'acquired_at':acquired,
            'provider_updated_at':definition.get('last_updated'),'published_at':None,
            'quality':{'status':state,'evaluated_at':generated_at,'observation_age_days':observation_age,
                'acquisition_age_seconds':acquisition_age,'max_observation_age_days':MAX_OBSERVATION_DAYS,
                'max_acquisition_age_seconds':MAX_ACQUISITION_SECONDS,'release_calendar_verified':False},
            'history':rows,'coverage':deepcopy((m or {}).get('coverage')),
            'history_scope':'Every returned original row, including missing and future records; bounded current-vintage history.',
            'evidence':deepcopy((original or {}).get('evidence',{})),**AUTHORITY}
    return result


def metric(series, coefficients, divisor, unit, source_generated_at):
    """An exact linear statistic on a common observation date, never forward-filled."""
    cutoff=str(clock(source_generated_at).date()); maps={}; all_dates=set()
    for sid in coefficients:
        maps[sid]={r['observation_date']:r for r in series[sid]['history'] if r['observation_date']<=cutoff}
        all_dates.update(maps[sid])
    reviewed=all(series[s]['definition_reviewed'] for s in coefficients)
    matched=sorted(d for d in all_dates if all(d in maps[s] and observations.decimal(maps[s][d]['native_value']) is not None for s in coefficients))
    def point(day):
        if day is None: return None
        legs={s:{'original_row':maps[s][day]['original_row'],'native_decimal':str(observations.decimal(maps[s][day]['native_value']))}
            for s in coefficients}
        value=sum(Decimal(coefficients[s])*observations.decimal(maps[s][day]['native_value']) for s in coefficients)/Decimal(divisor)
        return {'observation_date':day,'legs':legs,'measurement':scalar(value if reviewed else None)}
    matched_set=set(matched)
    end=matched[-1] if matched else None; latest=point(end)
    latest_dates={s:series[s]['latest_date'] for s in coefficients}
    usable=bool(reviewed and latest and all(series[s]['quality']['status']=='within_age_ceiling' for s in coefficients)
        and end==min(latest_dates.values()))
    comparisons={}
    for lag in LAGS:
        start=matched[-lag-1] if len(matched)>lag else None; baseline=point(start)
        change=(observations.decimal(latest['measurement']['exact_decimal'])-observations.decimal(baseline['measurement']['exact_decimal'])) if reviewed and latest and baseline else None
        comparisons[str(lag)]={'matched_observation_steps':lag,'current_date':end,'baseline':baseline,
            'elapsed_calendar_days':(date.fromisoformat(end)-date.fromisoformat(start)).days if start else None,
            'unmatched_or_missing_dates':sum(start<d<=end and d not in matched_set for d in all_dates) if start else None,
            'change':scalar(change),'change_unit':'percentage_points' if unit=='percent' else 'basis_points'}
    return {'coefficients':dict(coefficients),'divisor':divisor,'unit':unit,
        'formula':'sum(coefficient * native percent yield) / divisor; same observation date for every leg',
        'latest_source_dates':latest_dates,'matched_numeric_dates':len(matched),'current':deepcopy(latest) if usable else None,
        'last_matched':latest,'historical_comparisons':comparisons,
        'current_comparisons':deepcopy(comparisons) if usable else None,
        'quality':{'status':'within_age_ceiling' if usable else 'unavailable',
            'reason':None if usable else 'unreviewed_missing_expired_or_unaligned_leg'},
        'change_basis':DEFINITION_NOTES['comparisons'],**AUTHORITY}


def shape(spread):
    latest=spread['current']; baseline=(spread['current_comparisons'] or {}).get('5',{}).get('baseline')
    result={'status':'unavailable','window_matched_observations':5,'current_date':None,'baseline_date':None,
        'label':None,'inverted_2s10s':None,'short_change_bps':scalar(None),'long_change_bps':scalar(None),
        'slope_change_bps':scalar(None),'mean_endpoint_change_bps':scalar(None),**AUTHORITY}
    if latest:
        result.update(current_date=latest['observation_date'],inverted_2s10s=Decimal(latest['measurement']['exact_decimal'])<0)
    if not latest or not baseline: return result
    changes={s:100*(Decimal(latest['legs'][s]['native_decimal'])-Decimal(baseline['legs'][s]['native_decimal'])) for s in ('DGS2','DGS10')}
    short,long=changes['DGS2'],changes['DGS10']; slope=long-short; mean=(short+long)/2
    movement='RISING' if mean>0 else 'FALLING' if mean<0 else 'UNCHANGED_MEAN'
    direction='STEEPENING' if slope>0 else 'FLATTENING' if slope<0 else 'PARALLEL' if mean else 'UNCHANGED'
    result.update(status='descriptive',baseline_date=baseline['observation_date'],label=movement+'_'+direction,
        short_change_bps=scalar(short),long_change_bps=scalar(long),slope_change_bps=scalar(slope),mean_endpoint_change_bps=scalar(mean))
    return result


def build(source,originals,generated_at,term_premium_context,predecessor):
    if not original_ref(predecessor): raise ValueError('Complete preserved predecessor required')
    context=term_premium_context
    if (context.get('source_key')!='data/term-premium.json' or context.get('independent_votes')!=0
        or context.get('status') not in ('retained_unqualified_context','missing')
        or (not original_ref(context.get('original')) if context['status']!='missing' else context.get('original') is not None)):
        raise ValueError('Whole separate ACM context required')
    if context.get('captured_at') and clock(context['captured_at'])>clock(generated_at): raise ValueError('Future context capture')
    with localcontext() as arithmetic:
        # Covers the full reviewed input exponent/significant-digit range even
        # when large legs cancel; actual original decimal strings stay intact.
        arithmetic.prec=80; arithmetic.rounding=ROUND_HALF_EVEN
        series=measure(source,originals,generated_at)
        derived={name:metric(series,**spec,source_generated_at=source['generated_at']) for name,spec in METRICS.items()}
        for sid,row in series.items():
            single=metric(series,{sid:100},1,'basis_points',source['generated_at'])
            row['historical_observation_comparisons']=single['historical_comparisons']
            row['current_observation_comparisons']=single['current_comparisons']
        curves={}
        for group,members in (('nominal',NOMINAL),('real',REAL)):
            aligned=metric(series,{sid:1 for sid in members},len(members),'percent',source['generated_at'])
            point=aligned['current']
            curves[group]={'observation_date':point['observation_date'] if point else None,
                'points':[{'series_id':sid,'tenor_months':SPECS[sid]['tenor_months'],
                    'original_row':point['legs'][sid]['original_row'],
                    **scalar(Decimal(point['legs'][sid]['native_decimal']))} for sid in members] if point else [],
                'unit':'percent','requested_series':list(members),'complete':point is not None,'interpolated':False}
        fresh=sum(row['quality']['status']=='within_age_ceiling' for row in series.values())
        return {'contract':CONTRACT,'candidate_only':True,'engine':'justhodl-yield-curve','generated_at':generated_at,
            'source_generated_at':source['generated_at'],'source_replay':deepcopy(source['replay']),
            'series':series,'curves':curves,'derived':derived,'shape':shape(derived['2s10s']),
            'quality':{'status':'within_age_ceiling' if fresh==len(SERIES) else 'degraded' if fresh else 'unavailable',
                'current_series':fresh,'requested_series':len(SERIES),'missing_series':[s for s,r in series.items() if not r['history']],
                'release_calendar_verified':False},
            'dependency_graph':{'series_to_engine':list(SERIES),'independent_votes':0,
                'known_derived_roots':{'T5YIE':['DGS5','DFII5'],'T10YIE':['DGS10','DFII10'],
                    'T5YIFR':['DGS5','DFII5','DGS10','DFII10']},'complete_component_lineage_verified':False},
            'term_premium_context':deepcopy(context),'qualified_term_premium_bps':None,'predecessor':deepcopy(predecessor),
            'definition_notes':deepcopy(DEFINITION_NOTES),'method_sources':METHOD_SOURCES.copy(),
            'signals':[],'call':None,'decision':{'verb':'WAIT','meaning':'abstain'},
            'portfolio_consequences':{'status':'UNAVAILABLE','target_weights':None,'reason':'No out-of-sample portfolio qualification.'},
            **AUTHORITY}
