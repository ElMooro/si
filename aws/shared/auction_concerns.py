"""Dated, complete-input legacy concern arithmetic; no forecasting authority.

Retains the existing public score keys and coefficients. Missing inputs never
become zero. Historical anchor comparability has not been established, so the
anchor-dependent concern remains unavailable rather than inventing an amplifier.
These traces explain supplied model inputs, not original-provider verification.
"""
from datetime import date, timedelta
import math
from auction_benchmarks import day
from auction_weighting import weighted_summary
import auction_cross_observations

CONTRACT = 'auction-concern-inputs.v1'
PERMISSIONS = {key:False for key in ('source_capture_verified','historical_point_in_time_verified',
    'forecast_eligible','calls_eligible','sizing_eligible','execution_eligible')}


def numeric(value):
    if type(value) not in (int,float):return None
    try:return float(value) if math.isfinite(value) else None
    except (ValueError,OverflowError):return None


def score(value):
    n=numeric(value)
    return n if n is not None and 0<=n<=100 else None


def mapping(value):return value if isinstance(value,dict) else {}


def momentum(history,today,scored):
    history=mapping(history);current=mapping(history.get('current'));rows=history.get('series')
    start=today-timedelta(days=7);seen={};trace=[];issues=[]
    if not isinstance(rows,list):rows=[];issues.append('history_series_unavailable')
    for index,row in enumerate(rows):
        row=mapping(row);at=day(row.get('date'));value=score(row.get('composite'))
        reason=('invalid_date' if at is None else 'future_date' if at>today else
                'duplicate_date' if at in seen else 'missing_or_invalid_score' if value is None else None)
        trace.append({'source_row_index':index,'date':at.isoformat() if at else None,'composite':value,'reason':reason})
        if reason in ('invalid_date','future_date','duplicate_date'):issues.append('invalid_history_date_population')
        if at is not None:seen[at]=value
    latest=score(current.get('composite'));at=day(current.get('date'))
    if at!=today or latest is None or today not in seen or seen[today]!=latest:issues.append('matching_current_composite_unavailable')
    prior=seen.get(start)
    if prior is None:issues.append('exact_seven_calendar_day_comparison_unavailable')
    # Reconstruct both endpoint means from every supplied scored auction. A
    # plausible dated history alone cannot substantiate its own arithmetic.
    windows=[];supplied=scored if isinstance(scored,list) else []
    for end in (start,today):
        begin=end-timedelta(days=14);members=[];selected=[];identities=set();invalid=False
        for index,item in enumerate(supplied):
            row=mapping(item);observed=day(row.get('auction_date'))
            if observed is None or observed>today:invalid=True;continue
            if not begin<=observed<=end:continue
            cusip=row.get('cusip')
            if not isinstance(cusip,str) or not cusip or (cusip,observed) in identities:invalid=True
            else:identities.add((cusip,observed))
            selected.append(row)
            members.append({'source_row_index':index,'auction_date':observed.isoformat(),
                            'cusip':cusip if isinstance(cusip,str) else None,
                            'accepted_usd_bn':numeric(row.get('accepted_billions')),
                            'composite_score':score(row.get('composite_score'))})
        weighting=weighted_summary(selected)
        expected=round(weighting['composite'],1) if weighting['status']=='complete' and not invalid else None
        matched=expected is not None and expected==seen.get(end)
        if not matched:issues.append('history_endpoint_weighted_inputs_do_not_match')
        windows.append({'window_start':begin.isoformat(),'window_end':end.isoformat(),
                        'weighting':weighting,'observations':members,'recomputed_composite':expected,'matches_history':matched})
    delta=latest-prior if not issues else None
    return {'value':delta,'unit':'score_points','start_date':start.isoformat(),'end_date':today.isoformat(),
            'start_composite':prior,'end_composite':latest,'elapsed_calendar_days':7,
            'formula':'end composite - composite exactly seven calendar days earlier',
            'status':'complete' if not issues else 'unavailable','missing_inputs':sorted(set(issues)),
            'observations':trace,'endpoint_windows':windows,'historical_point_in_time_verified':False,
            'scope':'Reconstructed daily heuristic history in the current source vintage; not an as-published historical signal.'}


def participation(scored,today):
    start=today-timedelta(days=14);rows=[];recent=[];coupons=[];issues=[]
    if not isinstance(scored,list):scored=[];issues.append('scored_auction_population_unavailable')
    identities=set()
    for index,item in enumerate(scored):
        row=mapping(item);at=day(row.get('auction_date'));kind=row.get('instrument_kind');signals=mapping(row.get('indicator_scores'))
        reason=None
        if at is None:reason='invalid_auction_date';issues.append('invalid_auction_date_population')
        elif at>today:reason='future_auction_date';issues.append('invalid_auction_date_population')
        elif at<start:reason='outside_inclusive_fourteen_day_window'
        elif row.get('instrument_contract')!='treasury-instrument.v1' or row.get('instrument_classification_status')!='verified' or kind not in ('BILL','NOMINAL_COUPON','TIPS','FRN'):
            reason='unverified_instrument_identity';issues.append('unverified_recent_instrument_identity')
        else:
            recent.append(row)
            identity=(row.get('cusip'),at)
            if not isinstance(identity[0],str) or not identity[0]:
                reason='missing_auction_identity';issues.append('missing_recent_auction_identity')
            elif identity in identities:
                reason='duplicate_auction_identity';issues.append('duplicate_recent_auction_identity')
            if isinstance(identity[0],str) and identity[0]:identities.add(identity)
            if kind in ('NOMINAL_COUPON','TIPS'):coupons.append(row)
            elif reason is None:reason='not_a_coupon_participation_observation'
        rows.append({'source_row_index':index,'auction_date':at.isoformat() if at else None,
            'cusip':row.get('cusip') if isinstance(row.get('cusip'),str) else None,
            'instrument_kind':kind if isinstance(kind,str) else None,
            'pd_absorption':score(signals.get('pd_absorption')),'indirect_collapse':score(signals.get('indirect_collapse')),
            'selection_reason':reason})
    if not coupons:issues.append('coupon_participation_population_unavailable')
    flags={};coverage={}
    for key in ('pd_absorption','indirect_collapse'):
        values=[score(mapping(row.get('indicator_scores')).get(key)) for row in coupons]
        missing=sum(value is None for value in values)
        coverage[key]={'n_observations':len(values),'n_missing':missing,'threshold':70}
        if missing:issues.append(key+'_incomplete')
        flags[key]=any(value>=70 for value in values) if values and not missing and not issues else None
    # A later missing dimension also withholds the complete combined population.
    if issues:flags={key:None for key in flags}
    return {'flags':flags,'coverage':coverage,'status':'complete' if not issues else 'unavailable',
            'missing_inputs':sorted(set(issues)),'observations':rows,
            'window_start':start.isoformat(),'window_end':today.isoformat(),'n_supplied':len(scored),
            'n_recent':len(recent),'n_coupon_observations':len(coupons),
            'scope':'All supplied scored nominal-coupon and TIPS auctions in the inclusive fourteen-calendar-day window; no first-twenty-row cutoff. Bills and FRNs are excluded from these coupon participation flags.'},recent


def long_coupon(tenors,recent,today):
    declared=mapping(mapping(tenors).get('coupons_gt_3y'));rows=[row for row in recent if row.get('tenor_bucket')=='coupons_gt_3y']
    # The native bucket is nominal only. An inconsistent identity must not enter its mean.
    identity_ok=all(row.get('instrument_kind')=='NOMINAL_COUPON' for row in rows)
    calculated=weighted_summary(rows);value=score(declared.get('composite'));n=declared.get('n_auctions')
    latest=day(declared.get('latest_date'));expected=round(calculated['composite'],1) if calculated['status']=='complete' else None
    complete=(bool(rows) and identity_ok and type(n) is int and n==len(rows) and latest is not None and
              latest==max(day(row['auction_date']) for row in rows) and value is not None and value==expected)
    return {'value':value if complete else None,'status':'complete' if complete else 'unavailable',
            'declared_composite':value,'declared_count':n if type(n) is int else None,
            'expected_composite':expected,'weighting':calculated,
            'observations':[{'cusip':row.get('cusip') if isinstance(row.get('cusip'),str) else None,'auction_date':row.get('auction_date'),
                             'composite_score':score(row.get('composite_score')),
                             'accepted_usd_bn':numeric(row.get('accepted_billions'))} for row in rows],
            'scope':'Accepted-amount-weighted supplied nominal-coupon auctions in the existing greater-than-three-year bucket, within the inclusive fourteen-day window.'}


def cross_context(cross,today):
    cross=mapping(cross);valid=False
    try:
        # Bind derived source values to the existing reviewed pure compiler.
        expected=auction_cross_observations.build(cross.get('source_frames'),today)
        valid=cross==expected
    except (ValueError,TypeError,OverflowError,KeyError,AttributeError):pass
    repo=mapping(cross.get('repo_stress'));dollar=mapping(cross.get('dollar_strength'))
    spread=numeric(repo.get('spread_bp'));change=numeric(dollar.get('change_30d_target_pct'));band=repo.get('legacy_threshold_band')
    complete=(valid and repo.get('measurement_status')=='complete' and dollar.get('measurement_status')=='complete' and
              spread is not None and change is not None and band in ('CALM','WATCH','ELEVATED','ACUTE'))
    return {'status':'complete' if complete else 'unavailable','reviewed_cross_arithmetic_matches':valid,
            'repo_band':band if isinstance(band,str) else None,'repo_spread_bp':spread,
            'repo_observation_date':repo.get('observation_date') if isinstance(repo.get('observation_date'),str) else None,
            'dollar_target_change_pct':change,
            'dollar_change_30d':numeric(dollar.get('change_30d_pct')),
            'dollar_comparison':dollar.get('comparison') if valid else None,
            'calculation_as_of':today.isoformat(),'source_capture_verified':False}


def concern(value,drivers,trace,missing,note,today,calculation=None):
    return {'probability':None,'heuristic_score':round(value,1) if value is not None and not missing else None,
            'unit':'score_0_100','calibrated':False,'forecast_horizon_days':None,
            'status':'unavailable' if missing or value is None else 'available','drivers':drivers,
            'measurement_contract':CONTRACT,'calculation_as_of':today.isoformat(),
            'missing_inputs':sorted(set(missing)),'input_trace':trace,'calculation':calculation,
            'interpretation':note+' Uncalibrated concern arithmetic; no event probability, validated horizon or position-size authority.',**PERMISSIONS}


def calculate(scored,history,tenors,analog,cross,today):
    if type(today) is not date:raise ValueError('An explicit calculation date is required')
    delta=momentum(history,today,scored);population,recent=participation(scored,today)
    tenor=long_coupon(tenors,recent,today);context=cross_context(cross,today)
    shared=list(delta['missing_inputs'])
    if not recent:shared.append('recent_scored_auction_population_unavailable')
    # Invalid/future/duplicate identities cannot silently enter any concern.
    shared.extend(reason for reason in population['missing_inputs'] if reason not in
                  ('coupon_participation_population_unavailable','pd_absorption_incomplete','indirect_collapse_incomplete'))
    change=delta['value'];positive=max(0,change) if change is not None else None
    soft_missing=shared+population['missing_inputs']+([] if tenor['status']=='complete' else ['complete_long_coupon_composite_unavailable'])
    flags=population['flags'];soft_value=None;soft_formula=None
    if not soft_missing:
        terms={'base':5,'long_coupon':tenor['value']*.5,'primary_dealers':20 if flags['pd_absorption'] else 0,
               'indirect_bidders':15 if flags['indirect_collapse'] else 0,'positive_momentum':positive*.5}
        soft_value=min(85,sum(terms.values()));soft_formula={'terms':terms,'cap':85,'sum_before_cap':sum(terms.values())}
    soft=concern(soft_value,{'coupons_long_stress':tenor['value'],'pd_concern':flags['pd_absorption'],
                          'indirect_concern':flags['indirect_collapse'],'momentum':change},
                 {'momentum':delta,'participation':population,'long_coupon':tenor},soft_missing,
                 'Fixed coupon-participation and tenor thresholds. AAH is marginal-bid proration, not dealer absorption.',today,soft_formula)
    current=delta['end_composite'];distance=(25-current if current<25 else 50-current if current<50 else 75-current if current<75 else 0) if current is not None else None
    matches=mapping(analog).get('top_matches');candidate=mapping(matches[0]) if isinstance(matches,list) and matches else {}
    analog_trace={'candidate_date':candidate.get('date') if isinstance(candidate.get('date'),str) else None,
                 'candidate_regime':candidate.get('regime') if isinstance(candidate.get('regime'),str) else None,
                 'reported_similarity':numeric(candidate.get('similarity')),
                 'comparability_verified':False,'amplifier':None,
                 'reason':'Legacy anchors use different simplified feature mappings and lack a verified comparable instrument/source basis. Ranked or zero-similarity labels cannot supply concern points.'}
    escalation=concern(None,{'current_composite':current,'momentum_7d':change,
                            'distance_to_next_threshold':distance,'top_analog_regime':None},
                       {'momentum':delta,'historical_anchor':analog_trace},shared+['historical_anchor_comparability_unverified'],
                       'The legacy anchor-dependent formula is unavailable until its historical comparison is qualified. Threshold distance remains descriptive context.',today)
    supply_missing=shared+([] if context['status']=='complete' else ['matching_dated_cross_source_context_unavailable'])
    supply_value=None;supply_formula=None
    if not supply_missing:
        terms={'base':10,'repo_band':{'CALM':0,'WATCH':15,'ELEVATED':30,'ACUTE':45}[context['repo_band']],
               'dollar_target_change':min(20,abs(context['dollar_target_change_pct'])*4),'positive_momentum':positive*.4}
        supply_value=min(75,sum(terms.values()));supply_formula={'terms':terms,'cap':75,'sum_before_cap':sum(terms.values())}
    supply=concern(supply_value,{'repo_stress':context['repo_band'],'repo_observation_date':context['repo_observation_date'],
                               'dollar_change_30d':context['dollar_change_30d'],'dollar_target_change_pct':context['dollar_target_change_pct'],
                               'dollar_comparison':context['dollar_comparison'],'momentum':change},
                   {'momentum':delta,'cross_source':context},supply_missing,
                   'Dated funding and dollar measurements in a fixed legacy formula; no demonstrated volatility causation.',today,supply_formula)
    return {'p_soft_demand_30d':soft,'p_failed_auction_30d':dict(soft,deprecated=True,alias_of='p_soft_demand_30d'),
            'p_regime_escalation_14d':escalation,'p_supply_volatility_30d':supply}
