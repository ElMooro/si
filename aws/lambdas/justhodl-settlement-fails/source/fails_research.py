"""Deterministic, exact FR2004C measurement research; no trade emission."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal, localcontext
from zoneinfo import ZoneInfo
import fails_native as native

CONTRACT='fr2004-fails-research.v1'
METHOD='exact-reported-millions.same-period-prior104.v1'
PREFIX='data/fails-research/'
CURRENT='data/settlement-fails.json'
AUTHORITY={'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
           'call':None,'portfolio_action':'WAIT','allocation_pct':None,
           'reason':'Descriptive settlement evidence; no validated directional strategy or authorized portfolio impact.'}


def bn(value): return float(Decimal(value)/1000) if value is not None else None
def exact_bn(value): return format(Decimal(value)/1000,'f') if value is not None else None
def decimal_text(value): return format(value,'f') if value is not None else None


def quality(observed, acquired, at, complete=True):
    now=native.clock(at);receipt=native.clock(acquired);d=native.day(observed) if observed else None
    next_release=(datetime.combine(d+timedelta(days=(3-d.weekday())%7+14),datetime.min.time(),ZoneInfo('America/New_York'))
                  .replace(hour=16,minute=15).astimezone(timezone.utc)) if d else None
    reason=[]
    if not complete or d is None:reason.append('incomplete_latest_reporting_date')
    if d and d>now.date() or receipt>now:reason.append('future_source_clock')
    if (now-receipt).total_seconds()>36*3600:reason.append('acquisition_older_than_36h')
    if next_release and now>next_release+timedelta(hours=24):reason.append('normal_next_release_plus_24h_exceeded')
    status='incomplete' if not complete or not d else 'invalid' if 'future_source_clock' in reason else 'stale' if reason else 'fresh'
    return {'status':status,'observation_date':observed,'publication_date':None,'acquired_at':acquired,
            'frequency':'weekly','period_measure':'cumulative_reported_fails','freshness_basis':'original_acquisition_and_normal_release_policy',
            'next_expected_publication_date':next_release.isoformat() if next_release else None,
            'actual_publication_time_verified':False,'holiday_adjustment_verified':False,
            'max_acquisition_age_hours':36,'normal_release_grace_hours':24,'missing':reason}


def statistics(history):
    current=history[-1] if history else None
    out={'latest':bn(current['gross_usd_mn']) if current else None,'z':None,'pctile':None,
         'spike':None,'n_obs':len(history),'as_of':current['date'] if current else None,
         'start':history[0]['date'] if history else None,'mean':None,'min':None,'max':None,'avg_52w':None,
         'baseline':{'method':'104 immediately preceding weekly reports within the same source period; current excluded; sample SD; percentile uses midrank ties',
                     'minimum_prior_reports':26,'current_excluded':True,'sample_n':0},
         'status':'insufficient_comparable_prior_reports','z_decimal':None,'percentile_decimal':None,
         'standard_deviation_usd_mn_decimal':None,'mean_usd_mn_decimal':None,
         'probability':None,'forecast_eligible':False}
    if not current or current['gross_usd_mn'] is None:return out
    # Do not skip suppressed reports or silently bridge missing weekly periods.
    prior=[];last=native.day(current['date'])
    for row in reversed(history[:-1]):
        if row['seriesbreak']!=current['seriesbreak'] or row['gross_usd_mn'] is None or (last-native.day(row['date'])).days!=7:break
        prior.append(row);last=native.day(row['date'])
        if len(prior)==104:break
    out['baseline'].update(sample_n=len(prior),from_date=prior[-1]['date'] if prior else None,to_date=prior[0]['date'] if prior else None,
                           source_period=current['seriesbreak'])
    if len(prior)<26:return out
    with localcontext() as ctx:
        ctx.prec=36
        values=[Decimal(row['gross_usd_mn']) for row in prior];value=Decimal(current['gross_usd_mn'])
        mean=sum(values)/len(values);sd=(sum((x-mean)**2 for x in values)/(len(values)-1)).sqrt()
        z=(value-mean)/sd if sd else None
        percentile=(sum(x<value for x in values)+Decimal('0.5')*sum(x==value for x in values))*100/len(values)
        out.update(status='available' if sd else 'constant_prior_sample',z=float(z) if z is not None else None,
                   pctile=float(percentile),mean=float(mean/1000),min=bn(min(values)),max=bn(max(values)),
                   z_decimal=decimal_text(z),percentile_decimal=decimal_text(percentile),
                   mean_usd_mn_decimal=decimal_text(mean),standard_deviation_usd_mn_decimal=decimal_text(sd))
        if len(prior)>=52:out['avg_52w']=float(sum(values[:52])/52/1000)
    return out


def scope(identity,label,members,series,acquired,at):
    keys=[key for pair in members for key in pair]
    dates=sorted(set().union(*(set(series[key]) for key in keys)))
    history=[]
    for d in dates:
        legs={key:series[key].get(d) for key in keys}
        def total(side):
            rows=[legs[pair[side]] for pair in members]
            return sum(row['usd_mn'] for row in rows) if all(row and row['usd_mn'] is not None for row in rows) else None
        deliver,receive=total(0),total(1)
        combined=deliver+receive if deliver is not None and receive is not None else None
        history.append({'date':d,'seriesbreak':native.period(d),'ftd_usd_mn':deliver,'ftr_usd_mn':receive,'gross_usd_mn':combined,
                        'complete':combined is not None,'components':{key:{'row_index':row['row_index'],'value_usd_mn':row['usd_mn'],
                        'status':row['status']} if row else {'row_index':None,'value_usd_mn':None,'status':'missing_observation'} for key,row in legs.items()}})
    latest=history[-1] if history else None
    q=quality(latest['date'] if latest else None,acquired,at,bool(latest and latest['complete']))
    stats=statistics(history)
    # Latest historical values remain inspectable, with no renewal of currency.
    values={side:latest[key] if latest else None for side,key in [('ftd','ftd_usd_mn'),('ftr','ftr_usd_mn'),('gross','gross_usd_mn')]}
    previous=history[-2] if len(history)>1 else None
    change=None
    if latest and previous and latest['complete'] and previous['complete'] and latest['seriesbreak']==previous['seriesbreak']:
        change={'from':previous['date'],'to':latest['date'],'elapsed_days':(native.day(latest['date'])-native.day(previous['date'])).days,
                'difference_usd_mn':latest['gross_usd_mn']-previous['gross_usd_mn'],
                'difference_usd_bn_decimal':exact_bn(latest['gross_usd_mn']-previous['gross_usd_mn'])}
    return {'scope_id':identity,'scope':identity,'label':label,'as_of':latest['date'] if latest else None,
            'unit':'usd_bn','native_unit':'usd_mn','valuation_basis':'cash_principal_ex_accrued_interest; financing_amount_due',
            'period_measure':'cumulative_reported_fails','complete':bool(latest and latest['complete']),
            'ftd_bn':bn(values['ftd']),'ftr_bn':bn(values['ftr']),'gross_bn':bn(values['gross']),'combined_bn':bn(values['gross']),
            'ftd_usd_mn':values['ftd'],'ftr_usd_mn':values['ftr'],'gross_usd_mn':values['gross'],
            'exact_usd_bn':{key:exact_bn(value) for key,value in values.items()},
            'field_units':{'ftd_bn':'usd_bn','ftr_bn':'usd_bn','gross_bn':'usd_bn','combined_bn':'usd_bn'},
            'source_series':keys,'quality':q,'history':history,'statistics':stats,'previous_reported_change':change,
            'ftd':[[row['date'],bn(row['ftd_usd_mn'])] for row in history],
            'ftr':[[row['date'],bn(row['ftr_usd_mn'])] for row in history],
            'gross':[[row['date'],bn(row['gross_usd_mn'])] for row in history],
            'combined':[[row['date'],bn(row['gross_usd_mn'])] for row in history],
            'regime':'UNQUALIFIED','score':None,'score_0_100':None,'measurement_note':native.MEASUREMENT_NOTE,
            **AUTHORITY}


def compile_research(inputs, read):
    series,definitions=native.load(inputs,read)
    at=inputs['generated_at'];acquired=inputs['sources']['observations']['acquired_at']
    pairs={key:('PDFTD-'+suffix,'PDFTR-'+suffix) for key,suffix,_,_ in native.CLASSES}
    classes=[]
    for key,_,label,_ in native.CLASSES:
        row=scope(key,label,[pairs[key]],series,acquired,at)
        # Compatibility fields retain measurements while withholding legacy spike/regime authority.
        row.update(key=key,ftd_latest=row['ftd_bn'],ftr_latest=row['ftr_bn'],
                   stats={**row['statistics'],'z':None,'pctile':None,'spike':None,'compatibility_note':'Use statistics for descriptive evidence; legacy vote inputs withheld'},
                   deep=row['combined'],deep_start=row['history'][0]['date'] if row['history'] else None,
                   deep_sampling='full reported weekly observations; no arbitrary monthly selection')
        classes.append(row)
    treasury=scope('treasury_incl_tips','U.S. Treasury (including TIPS)',[pairs['ust_ex_tips'],pairs['tips']],series,acquired,at)
    treasury.update(stats={'gross':{**treasury['statistics'],'z':None,'pctile':None,'spike':None}},
                    narrative='Exact cumulative two-sided Treasury fails; descriptive context only.')
    headline=dict(classes[0]);headline.update(z=None,pctile=None,max_bn=None)
    # Legacy headline scalar statistics drove unqualified downstream risk scores.
    # Full dated descriptive statistics remain at headline.statistics, with no vote permission.
    total=scope('all_asset','All six reported FR2004C fails classes',list(pairs.values()),series,acquired,at)
    counts={key:{'observations':len(rows),'suppressed':sum(row['status']=='suppressed' for row in rows.values()),
                 'missing':sum(row['status']=='missing' for row in rows.values())} for key,rows in series.items()}
    output={'contract':CONTRACT,'engine':'settlement-fails','version':'2.0.0','methodology_version':METHOD,
            'generated_at':at,'source_generated_at':acquired,'as_of':treasury['as_of'],
            'quality':treasury['quality'],'headline':headline,'treasury':treasury,'classes':classes,'totals':total,
            'signal':{'regime':'UNQUALIFIED','score':None,'score_0_100':None,'scope':'ust_ex_tips','quality':headline['quality'],
                      'drivers':[native.MEASUREMENT_NOTE],'emission_enabled':False},
            'source':'NY Fed FR2004C original API; cumulative reported amounts, USD millions converted exactly to billions',
            'sources':inputs['sources'],'series_definitions':definitions,'series_coverage':counts,
            'reporting_periods':native.DEFINITIONS,'measurement_note':native.MEASUREMENT_NOTE,
            'legacy_context':inputs.get('legacy_context'),
            'limitations':{'current_provider_vintage':True,'historical_publication_time_verified':False,
                          'unique_failed_securities_measured':False,'daily_average_not_computed':True,
                          'holiday_reporting_calendar_verified':False,'cross_period_statistics_allowed':False,
                          'provider_revision_history_complete':False,'backtest_eligible':False},
            **AUTHORITY}
    prior=inputs.get('previous_output');revisions=[]
    if prior is not None:
        raw=read(prior['key'])
        if prior['key']!=PREFIX+'outputs/'+prior['sha256']+'.json' or len(raw)!=prior['bytes'] or __import__('hashlib').sha256(raw).hexdigest()!=prior['sha256']:
            raise ValueError('previous measurement snapshot differs')
        old=native.strict_json(raw)
        if old.get('contract')!=CONTRACT or native.clock(old['generated_at'])>native.clock(at):raise ValueError('previous snapshot identity differs')
        oldclasses={row['key']:row for row in old['classes']}
        for row in classes:
            earlier={item['date']:item for item in oldclasses[row['key']]['history']}
            current={item['date']:item for item in row['history']}
            if set(earlier)-set(current):raise ValueError('provider dropped previously retained report dates')
            for d,before in earlier.items():
                after=current[d]
                for side in ('ftd_usd_mn','ftr_usd_mn'):
                    if before[side]!=after[side]:revisions.append({'class':row['key'],'date':d,'field':side,'previous':before[side],'current':after[side]})
    output['revisions']={'previous_output':prior,'changes':revisions,'known_changed_values':len(revisions),
                         'scope':'Changes between retained acquisitions; not the provider full revision history.'}
    output['decision_id']='fails-'+native.digest({'sources':inputs['sources'],'method':METHOD,'at':at})
    return output
