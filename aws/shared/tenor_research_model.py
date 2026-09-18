"""Reproducible Treasury observations, with no policy, funding or allocation inference.

Inputs are retained FiscalData pages and a retained FRED DFF response. Comparisons
use declared instrument flags, exact remaining term and known reopening status.
Original observation dates are not asserted to be original publication times.
"""
import math
from collections import Counter
from datetime import date, datetime, timezone, timedelta
from statistics import mean
from treasury_instruments import instrument_fields

CONTRACT = 'treasury-tenor-research.v2'
VERSION = '2.0.0'
BILL_TERMS = ('4-Week','6-Week','8-Week','13-Week','17-Week','26-Week','52-Week')
LIMITS = {'nominal_2y':45, 'nominal_30y':130, 'bill_participation':14}
LABELS = {'nominal_2y':'2-year nominal auction yields',
          'nominal_30y':'30-year nominal auction yields',
          'bill_participation':'Bill auction participation'}
ALIASES = {'fed_path':'nominal_2y','eurodollar':'bill_participation','qe_imminence':'nominal_30y'}


def number(value):
    if isinstance(value,bool):return None
    try:result=float(value)
    except (TypeError,ValueError):return None
    return result if math.isfinite(result) else None


def flag(value):
    v=str(value).strip().lower()
    return True if v in ('yes','true','1') else False if v in ('no','false','0') else None


def day(value):
    try:return date.fromisoformat(str(value))
    except (ValueError,TypeError):return None


def normalize(pages, as_of):
    """Every accepted row names its exact original page and row. Reject duplicates."""
    today=day(as_of)
    if today is None:raise ValueError('valid as_of date required')
    rows=[];issues=Counter();seen=set()
    for pi,page in enumerate(pages):
        if not isinstance(page,dict) or not isinstance(page.get('data'),list):raise ValueError('invalid FiscalData page')
        for ri,raw in enumerate(page['data']):
            if not isinstance(raw,dict):raise ValueError('invalid FiscalData row')
            d=day(raw.get('auction_date'));cusip=raw.get('cusip')
            if d is None or d>today or d<today-timedelta(days=180):issues['outside_dated_window']+=1;continue
            accepted=number(raw.get('total_accepted'))
            if accepted is None or accepted<=0:issues['no_completed_award']+=1;continue
            if not isinstance(cusip,str) or len(cusip)!=9 or not cusip.isalnum():issues['invalid_identity']+=1;continue
            identity=(cusip,d.isoformat())
            if identity in seen:raise ValueError('duplicate auction identity across source pages')
            seen.add(identity)
            inst=instrument_fields(raw,'fiscaldata')
            reopening=flag(raw.get('reopening'))
            term=raw.get('security_term')
            if inst['instrument_kind']=='UNKNOWN' or reopening is None or not isinstance(term,str) or not term:
                issues['unverified_instrument_or_cohort']+=1;continue
            cmb=flag(raw.get('cash_management_bill_cmb'))
            if inst['instrument_kind']=='BILL' and cmb is not False:
                issues['cash_management_or_unverified_bill']+=1;continue
            amounts=[number(raw.get(k)) for k in ('primary_dealer_accepted','direct_bidder_accepted','indirect_bidder_accepted')]
            total=sum(amounts) if all(x is not None and x>=0 for x in amounts) else None
            shares=[round(100*x/total,6) for x in amounts] if total is not None and total>0 else [None]*3
            quote_field={'BILL':'high_investment_rate','NOMINAL_COUPON':'high_yield',
                         'TIPS':'high_yield','FRN':'high_discnt_margin'}[inst['instrument_kind']]
            rows.append({**inst,'cusip':cusip,'auction_date':d.isoformat(),'term':term,
                         'reopening':reopening,'quote_value':number(raw.get(quote_field)),
                         'quote_field':quote_field,'btc':number(raw.get('bid_to_cover_ratio')),
                         'units':{'quote_value':inst['quote_basis'],'btc':'bid_to_cover_ratio',
                                  'primary_dealer_pct':'percent_of_three_reported_bidder_awards',
                                  'direct_pct':'percent_of_three_reported_bidder_awards',
                                  'indirect_pct':'percent_of_three_reported_bidder_awards',
                                  'participation_denominator_usd':'usd'},
                         'primary_dealer_pct':shares[0],'direct_pct':shares[1],'indirect_pct':shares[2],
                         'participation_denominator_usd':total,
                         'participation_basis':'Sum of the three reported bidder-category award amounts; all three required',
                         'source_ref':{'page':pi,'row':ri,'cusip':cusip,'auction_date':d.isoformat()},
                         'field_map':{'btc':'bid_to_cover_ratio','quote_value':quote_field,
                                      'participation':'primary_dealer_accepted + direct_bidder_accepted + indirect_bidder_accepted'}})
    rows.sort(key=lambda r:(r['auction_date'],r['cusip']),reverse=True)
    return rows,dict(issues)


def cohort(row):
    return row['instrument_kind'],row['term'],row['reopening'],row['quote_basis']


def fed_observations(doc, as_of):
    result={}
    if doc is None:return result
    if not isinstance(doc,dict) or not isinstance(doc.get('observations'),list):raise ValueError('invalid DFF response')
    for i,row in enumerate(doc['observations']):
        d=day(row.get('date'));value=number(row.get('value'))
        if d is None or value is None or d>day(as_of):continue
        if d.isoformat() in result:raise ValueError('duplicate DFF observation')
        result[d.isoformat()]={'value':value,'unit':'percent_per_annum','series_id':'DFF',
                               'as_of':d.isoformat(),'source_ref':{'response':'fred','observation':i}}
    return result


def base(channel):
    return {'channel':channel,'label':LABELS[channel],'state':'UNAVAILABLE','direction':None,
            'interpretation':'No eligible comparable observations available.',
            'call':None,'decision_eligible':False,'sizing_eligible':False,'alert_eligible':False,
            'metrics':{},'metric_definitions':{},'evidence':[],'latest_auction':None,
            'validation_scope':'SOURCE_BACKED_DESCRIPTIVE_MEASUREMENTS_ONLY'}


def nominal(rows, fed, channel, as_of):
    result=base(channel)
    original='2-Year' if channel=='nominal_2y' else '30-Year'
    candidates=[r for r in rows if r['instrument_kind']=='NOMINAL_COUPON' and
                (r['original_term']==original or r['term']==original)]
    if not candidates:return result
    latest=candidates[0]
    prior=next((r for r in candidates[1:] if cohort(r)==cohort(latest) and r['auction_date']<latest['auction_date']),None)
    result['latest_auction']=latest
    result['prior_auction']=prior
    age=(day(as_of)-day(latest['auction_date'])).days
    result['observation_age_days']=age;result['observation_max_age_days']=LIMITS[channel]
    if age>LIMITS[channel]:result.update(state='STALE',interpretation='Latest comparable auction observation exceeds the declared age limit.');return result
    if prior is None or latest['quote_value'] is None or prior['quote_value'] is None:return result
    change=round((latest['quote_value']-prior['quote_value'])*100,6)
    rate_dates=[d for d in fed if d<=latest['auction_date'] and (day(latest['auction_date'])-day(d)).days<=7]
    ff=fed[max(rate_dates)] if rate_dates else None
    spread=round((latest['quote_value']-ff['value'])*100,6) if ff else None
    result.update(state='AVAILABLE',observed_direction='LOWER_YIELD' if change<0 else 'HIGHER_YIELD' if change>0 else 'UNCHANGED_YIELD',
                  fed_funds_observation=ff,
                  interpretation='Nominal auction yield comparison across dated auctions. This includes intervening market moves and does not identify an auction surprise, expected policy path or QE probability.')
    result['metrics']={'yield_change_bp':change,'yield_drop_bp':-change,'spread_to_ff_bp':spread,
                       'fed_funds_pct':ff['value'] if ff else None,'indirect_pct':latest['indirect_pct']}
    result['metric_definitions']={
        'yield_change_bp':{'unit':'basis_points','formula':'100 * (latest.quote_value - prior.quote_value)','inputs':['latest_auction','prior_auction']},
        'yield_drop_bp':{'unit':'basis_points','formula':'-yield_change_bp','inputs':['latest_auction','prior_auction']},
        'spread_to_ff_bp':{'unit':'basis_points','formula':'100 * (latest.quote_value - dated DFF.value)','inputs':['latest_auction','fed_funds_observation']},
        'fed_funds_pct':{'unit':'percent_per_annum','formula':'dated DFF.value','inputs':['fed_funds_observation']},
        'indirect_pct':{'unit':'percent_of_three_reported_bidder_awards','formula':'100 * indirect / sum(three bidder awards)','inputs':['latest_auction']}}
    result['evidence']=[{'label':'Auction yield change','value':f'{change:+.2f} bp'},
                        {'label':'Latest / prior auction','value':latest['auction_date']+' / '+prior['auction_date']},
                        {'label':'Quote basis','value':latest['quote_basis']},
                        {'label':'DFF comparison date','value':ff['as_of'] if ff else 'Unavailable'}]
    return result


def bills(rows, as_of):
    result=base('bill_participation');breakdown=[]
    for term in BILL_TERMS:
        candidates=[r for r in rows if r['instrument_kind']=='BILL' and r['term']==term]
        if not candidates:continue
        latest=candidates[0]
        prior=[r for r in candidates[1:] if cohort(r)==cohort(latest) and r['auction_date']<latest['auction_date']][:4]
        age=(day(as_of)-day(latest['auction_date'])).days
        max_age=45 if term=='52-Week' else LIMITS['bill_participation']
        item={'tenor':term,'latest_auction':latest,'prior_auctions':prior,'state':'UNAVAILABLE',
              'indirect_drop_pts':None,'btc_spike':None,'avg_prior_indirect_pct':None,'avg_prior_btc':None,
              'observation_age_days':age,'observation_max_age_days':max_age}
        if age>max_age:item['state']='STALE'
        elif len(prior)==4:
            if latest['indirect_pct'] is not None and all(r['indirect_pct'] is not None for r in prior):
                item['avg_prior_indirect_pct']=round(mean(r['indirect_pct'] for r in prior),6)
                item['indirect_drop_pts']=round(item['avg_prior_indirect_pct']-latest['indirect_pct'],6)
            if latest['btc'] is not None and all(r['btc'] is not None for r in prior):
                item['avg_prior_btc']=round(mean(r['btc'] for r in prior),6)
                item['btc_spike']=round(latest['btc']-item['avg_prior_btc'],6)
            if item['indirect_drop_pts'] is not None or item['btc_spike'] is not None:item['state']='AVAILABLE'
        breakdown.append(item)
    result['metrics']={'tenor_breakdown':breakdown}
    result['metric_definitions']={
        'indirect_drop_pts':{'unit':'percentage_points','formula':'mean(prior four indirect_pct) - latest.indirect_pct','inputs':['latest_auction','prior_auctions']},
        'btc_spike':{'unit':'bid_to_cover_ratio_points','formula':'latest.btc - mean(prior four btc)','inputs':['latest_auction','prior_auctions']}}
    available=[t for t in breakdown if t['state']=='AVAILABLE']
    if available:result['state']='AVAILABLE'
    elif breakdown and all(t['state']=='STALE' for t in breakdown):result['state']='STALE'
    result['interpretation']='Exact-term bill participation comparisons. Indirect awards include domestic and foreign customers; offshore dollar funding conditions, cross-currency basis and panic are not measured here.'
    result['tenors_firing']=[]
    result['evidence']=[{'label':t['tenor']+' observation','value':t['latest_auction']['auction_date']+' · '+t['state']} for t in breakdown]
    return result


def compile_research(pages, fred, as_of, source_complete=True):
    rows,issues=normalize(pages,as_of);ff=fed_observations(fred,as_of)
    measurements={'nominal_2y':nominal(rows,ff,'nominal_2y',as_of),
                  'bill_participation':bills(rows,as_of),
                  'nominal_30y':nominal(rows,ff,'nominal_30y',as_of)}
    available=sum(m['state']=='AVAILABLE' for m in measurements.values())
    latest_ff=ff[max(ff)] if ff else None
    if latest_ff and (day(as_of)-day(latest_ff['as_of'])).days>7:latest_ff=None
    return {'schema_version':CONTRACT,'version':VERSION,'as_of':as_of,'role':'research_measurements',
            'quality':{'status':'fresh' if source_complete and available==3 and latest_ff else 'degraded',
                       'source_traversal_complete':bool(source_complete),'channels_available':available,
                       'rows_accepted':len(rows),'excluded_rows':issues,'publication_time_is_observation_time':False},
            'measurements':measurements,
            'signals':{old:{**measurements[new],'channel':old,'alias_of':new} for old,new in ALIASES.items()},
            'legacy_channel_aliases':ALIASES,'composite_score':None,'any_firing':None,'any_watch':None,'transitions':[],
            'fed_funds_rate':latest_ff['value'] if latest_ff else None,'fed_funds_observation':latest_ff,
            'n_auctions_in_window':len(rows),'call':None,'decision_eligible':False,'sizing_eligible':False,'alert_eligible':False,
            'methodology':{'window_days':180,'comparison':'exact instrument, remaining term, known reopening and quote basis',
                           'price_surprise_measured':False,'forecast_validated':False,
                           'scope':'Reproducible source observations; no Fed-path, QE, offshore-dollar or portfolio inference'}}


def public_summary(packet, now=None):
    """Bounded narrative context. No packet, including old schemas, grants authority."""
    unavailable={'status':'UNAVAILABLE','role':'research_measurements','call':None,'sizing_eligible':False,
                 'note':'Treasury auction participation cannot establish policy, QE or offshore funding conditions.'}
    if not isinstance(packet,dict) or packet.get('schema_version')!=CONTRACT:return unavailable
    try:
        stamp=datetime.fromisoformat(packet['generated_at'].replace('Z','+00:00'))
        current=now or datetime.now(timezone.utc)
        age=(current-stamp).total_seconds()
    except (KeyError,TypeError,ValueError):return unavailable
    if age < -300 or age>48*3600:return {**unavailable,'status':'STALE'}
    measurements=packet.get('measurements') or {}
    summaries={}
    for key in LABELS:
        m=measurements.get(key) or {}
        if m.get('state')!='AVAILABLE':summaries[key]={'state':'UNAVAILABLE'};continue
        metric=m.get('metrics') or {};latest=m.get('latest_auction') or {}
        summaries[key]={'state':'AVAILABLE','auction_date':latest.get('auction_date'),
                        'yield_change_bp':number(metric.get('yield_change_bp')),
                        'quote_basis':latest.get('quote_basis'),
                        'bill_cohorts_available':sum(t.get('state')=='AVAILABLE' for t in metric.get('tenor_breakdown',[])) if key=='bill_participation' else None}
    return {**unavailable,'status':'RESEARCH_ONLY','generated_at':packet['generated_at'],
            'source_quality':(packet.get('quality') or {}).get('status'),'measurements':summaries,
            'scope':'Dated descriptive observations, not policy probabilities, funding diagnoses or allocation signals.'}
