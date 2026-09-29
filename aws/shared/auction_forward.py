"""Complete-input legacy auction heuristic; no calibrated forecast confidence.

Pure, dated inputs only. Existing output names are compatibility fields, not
probabilities, evidence of contagion, expected auction outcomes or trade advice.
"""
from datetime import timedelta
import math
from auction_benchmarks import day, number, term_days

CONTRACT='auction-forward-heuristic.v1'
TENORS=('bills_lt_90d','bills_gte_90d','coupons_lt_3y','coupons_gt_3y','tips','frn')


def score(value):
    measured=number(value) if type(value) in (int,float) else None
    return measured if measured is not None and 0<=measured<=100 else None


def identity(row):
    if (row.get('instrument_contract')!='treasury-instrument.v1' or row.get('instrument_classification_status')!='verified' or
        row.get('instrument_kind') not in ('BILL','NOMINAL_COUPON','TIPS','FRN')):return None
    # Exact approximate remaining-term cohort; never silently pool 5y with 30y.
    term=term_days(row.get('security_term'))
    return (row['instrument_kind'],term) if term is not None else None


def calculate(upcoming,tenor_decomp,measurements,as_of):
    missing=[];tenor=upcoming.get('tenor_bucket');own=None;cross=None
    coverage=[];active=[]
    for name in TENORS:
        row=tenor_decomp.get(name);count=row.get('n_auctions') if isinstance(row,dict) else None
        valid_count=type(count) is int and count>=0
        value=score(row.get('composite')) if isinstance(row,dict) else None
        status='empty' if valid_count and count==0 else 'complete' if valid_count and value is not None else 'unavailable'
        coverage.append({'tenor_bucket':name,'n_auctions':count,'composite':value,'status':status})
        if status=='complete':active.append(value)
        if name==tenor and status=='complete':own=value
    if own is None:missing.append('own_tenor_composite')
    extra=sorted(set(tenor_decomp)-set(TENORS))
    if active and not extra and all(row['status']!='unavailable' for row in coverage):cross=math.fsum(active)/len(active)
    else:missing.append('complete_cross_tenor_population')

    cohort=identity(upcoming);target=day(upcoming.get('auction_date'));days=upcoming.get('days_ahead')
    expected_tenor=(('tips' if cohort[0]=='TIPS' else 'frn' if cohort[0]=='FRN' else
                    ('bills_lt_90d' if cohort[1]<90 else 'bills_gte_90d') if cohort[0]=='BILL' else
                    ('coupons_lt_3y' if cohort[1]<=1095 else 'coupons_gt_3y')) if cohort else None)
    if cohort is None or tenor!=expected_tenor:missing.append('verified_upcoming_identity')
    if type(days) is not int or not 0<=days<=30 or target is None or target!=as_of+timedelta(days=days):
        missing.append('dated_auction_horizon');days=None
    time_factor=(1.0 if days<=3 else .85 if days<=10 else .7 if days<=21 else .55) if days is not None else None
    offering=number(upcoming.get('offering_amount_billions'))
    if offering is None or offering<0:missing.append('offering_amount');offering=None

    amounts=[];members=[];excluded_identity=outside=invalid_dates=invalid_sizes=0
    for row in measurements:
        if cohort is None or identity(row)!=cohort:excluded_identity+=1;continue
        observed=day(row.get('auction_date'))
        if observed is None:invalid_dates+=1;continue
        if not as_of-timedelta(days=90)<=observed<=as_of:outside+=1;continue
        amount=number(row.get('accepted_billions'))
        members.append({'auction_date':row['auction_date'],'cusip':row.get('cusip'),'accepted_usd_bn':amount})
        if amount is None or amount<0:invalid_sizes+=1;continue
        amounts.append(amount)
    average=None
    if amounts and not invalid_dates and not invalid_sizes:
        try:average=math.fsum(amounts)/len(amounts)
        except (ValueError,OverflowError):pass
        if average is not None and (not math.isfinite(average) or average<=0):average=None
    if average is None:missing.append('complete_positive_size_baseline')
    shock=None;size_score=None
    if offering is not None and average is not None:
        shock=(offering/average-1)*100
        if math.isfinite(shock):size_score=50 if shock>30 else 25 if shock>15 else 10 if shock>0 else 0
        else:shock=None;missing.append('finite_size_comparison')
    value=None
    if not missing:value=round(min(100,max(0,(.5*own+.25*cross+.25*size_score)*time_factor)),1)
    label=('UNAVAILABLE' if value is None else 'ACUTE' if value>=70 else 'ELEVATED' if value>=45 else 'WATCH' if value>=22 else 'CALM')
    components={'own_tenor_stress':own,'cross_tenor_contagion':cross,'size_shock_pct':round(shock,1) if shock is not None else None,
                'size_shock_score':size_score,'time_decay_factor':time_factor}
    return {'forecast_score':value,'forecast_label':label,'confidence':'UNVALIDATED','components':components,
            'narrative':('Legacy heuristic unavailable: '+', '.join(missing)+'.' if missing else
                         'Complete-input legacy heuristic; its arithmetic does not establish auction demand, contagion, clearing results or a market return.'),
            'measurement_contract':CONTRACT,'calculation_as_of':as_of.isoformat(),'status':'unavailable' if missing else 'complete_unvalidated_heuristic',
            'missing_inputs':missing,'calibrated':False,'forecast_horizon_days':None,
            'calls_eligible':False,'forecast_eligible':False,'sizing_eligible':False,'execution_eligible':False,
            'historical_point_in_time_verified':False,'source_capture_verified':False,
            'cross_tenor_population':coverage,'unexpected_tenor_buckets':extra,
            'size_baseline':{'window_start':(as_of-timedelta(days=90)).isoformat(),'window_end':as_of.isoformat(),
                'cohort':{'instrument_kind':cohort[0],'remaining_term_days_approximate':cohort[1]} if cohort else None,
                'n_supplied_observations':len(measurements),'n_cohort_members':len(members),'n_valid_amounts':len(amounts),
                'n_invalid_amounts':invalid_sizes,'n_invalid_dates':invalid_dates,'n_outside_window':outside,
                'n_incomparable_identity':excluded_identity,'mean_accepted_usd_bn':average,'offering_usd_bn':offering,
                'observations':members,'reopening_strata_verified':False},
            'methodology':{'weights':{'own_tenor':.5,'cross_tenor':.25,'size_comparison':.25},
                'cross_tenor_contagion':'Legacy field name for the mean of all available populated tenor buckets; not independent evidence or demonstrated contagion.',
                'size_comparison':'All supplied direct observations in the same verified instrument-kind and approximate remaining-term cohort, within the inclusive 90-day window. No missing-size imputation.',
                'time_factor':'Fixed legacy heuristic parameter, not measured confidence or calibrated forecast decay.',
                'limitations':'Current source vintages and uncalibrated inputs. Reopening, settlement cash, independent validation and portfolio consequences remain unverified.'}}
