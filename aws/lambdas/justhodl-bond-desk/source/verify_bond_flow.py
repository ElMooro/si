"""Rational arithmetic and complete membership check, independent of the compiler.

This checks a descriptive candidate against a separately replayed ETF packet.
It does not establish past information availability or investment performance.
"""
from datetime import date,datetime,timezone
from fractions import Fraction
import hashlib,json


def stamp(value):
    dt=datetime.fromisoformat(value.replace('Z','+00:00'))
    if dt.tzinfo is None:raise ValueError('Timezone required')
    return dt.astimezone(timezone.utc)


def same(a,b):
    def encode(x):return json.dumps(x,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)
    if encode(a)!=encode(b):raise ValueError('Typed source binding differs')


def verify(raw,view,scope,at):
    packet=json.loads(raw);evaluated=stamp(at);generated=stamp(packet['generated_at'])
    if packet['contract']!='etf-original-research.v1' or not 0<=(evaluated-generated).total_seconds()<=172800:raise ValueError('Source contract or publication differs')
    same(view['contract'],'bond-flow-cohorts.v1');same(view['evaluated_at'],at)
    same(view['source'],{'key':'data/etf-true-flows.json','bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),
        'generated_at':packet['generated_at'],'replay':packet.get('replay'),'original_replay_verified_here':False})
    same(sorted(view['cohorts']),sorted(scope));same(view['configured_funds'],sum(map(len,scope.values())))
    same(view['source_funds'],len(packet['by_etf']))
    for key in ('calls_eligible','sizing_eligible','execution_eligible'):
        same(packet.get(key),False);same(view.get(key),False)
    same(view['forecast_qualified'],False);same(view['independent_votes'],0)
    same(view['decision'],{'verb':'WAIT','meaning':'abstain'})
    for key in ('duration_tilt','credit_appetite_signal','equity_to_bond_transfer'):same(view[key],None)
    counts={'cohorts':len(scope),'windows_checked':0,'members_checked':0,'rational_sums':0,'complete_windows':0,'partial_windows':0,'unavailable_windows':0}
    for name,tickers in scope.items():
        cohort=view['cohorts'][name];same(cohort['configured_tickers'],tickers)
        same(cohort['flow_21d_usd'],None);same(cohort['avg_z90'],None)
        same(sorted(cohort['windows']),sorted(str(n)+'_observations' for n in (1,5,20)))
        for n in (1,5,20):
            output=cohort['windows'][str(n)+'_observations'];members={};missing=[]
            for ticker in tickers:
                row=packet['by_etf'].get(ticker,{})
                try:
                    w=row['flow_windows'][str(n)+'d'];identity=row['identity']
                    age=(evaluated-stamp(row['source']['acquired_at'])).total_seconds()
                    recent=0<=age<=172800 and 0<=(evaluated.date()-date.fromisoformat(row['observation_date'])).days<=4
                    recent=recent and stamp(row['source']['acquired_at'])<=generated and row['observation_date']<=generated.date().isoformat()
                    valid=recent and row['ticker']==ticker and identity['ticker']==ticker and identity['currency']=='USD'
                    valid=valid and all(row.get(k) is False for k in ('calls_eligible','sizing_eligible','execution_eligible'))
                    valid=valid and row['source_status']=='retained_native_history' and row['evidence_tier']=='issuer_nav_valued_share_change_estimate' and row['quality']['status']=='recent_source_check'
                    valid=valid and w['status']=='complete_descriptive_estimate' and w['observations_required']==n and w['observations_available']==n and w['excluded_dates']==[]
                    valid=valid and w['start_date']<w['end_date']<=row['observation_date'] and w['end_date']==packet['aggregation_period']['end_date']
                    if valid:members[ticker]=(row,w)
                    else:missing.append(ticker)
                except (KeyError,TypeError,ValueError):missing.append(ticker)
            same([r['ticker'] for r in output['members']],list(members));same([r['ticker'] for r in output['excluded']],missing)
            if any(not isinstance(r.get('reason'),str) or not r['reason'] for r in output['excluded']):raise ValueError('Missing exclusion reason')
            same(output['configured_tickers'],tickers);same(output['included_count'],len(members));same(output['configured_count'],len(tickers))
            same(output['observations'],n);same(output['unit'],'USD');same(output['whole_market_total'],False);same(output['predictive_probability'],False)
            endpoints={(w['start_date'],w['end_date']) for row,w in members.values()};aligned=len(endpoints)==1
            complete=aligned and not missing
            for m in output['members']:
                row,w=members[m['ticker']]
                same(m,{'ticker':row['ticker'],'source_path':'/by_etf/'+row['ticker']+'/flow_windows/'+str(n)+'d','unit':'USD',
                    'start_date':w['start_date'],'end_date':w['end_date'],'observations':n,'value_decimal':w['value_decimal'],
                    'precision_sensitivity_decimal':w['precision_sensitivity_decimal'],'acquired_at':row['source']['acquired_at'],
                    'latest_observation':row['observation_date'],'history':row['history'],'original_evidence':row['source']['evidence'],
                    'calendar_independently_complete':False})
            if aligned:
                for field,source in (('coverage_subtotal_decimal','value_decimal'),('precision_sensitivity_decimal','precision_sensitivity_decimal')):
                    value=output[field]
                    if not isinstance(value,str) or Fraction(value)!=sum((Fraction(w[source]) for row,w in members.values()),Fraction(0)):raise ValueError('Rational sum differs')
                    counts['rational_sums']+=1
                same((output['start_date'],output['end_date']),next(iter(endpoints)))
            else:
                for field in ('coverage_subtotal_decimal','precision_sensitivity_decimal','start_date','end_date'):same(output[field],None)
            same(output['complete_cohort_decimal'],output['coverage_subtotal_decimal'] if complete else None)
            same(output['status'],'complete_configured_cohort' if complete else 'partial_coverage_subtotal' if aligned else 'unaligned_windows' if members else 'unavailable')
            counts['windows_checked']+=1;counts['members_checked']+=len(tickers)
            counts['complete_windows' if complete else 'partial_windows' if aligned else 'unavailable_windows']+=1
    return {'contract':'bond-flow-independent-check.v1',**counts,'all_checks_passed':True,
        'source_packet_sha256':hashlib.sha256(raw).hexdigest(),'source_original_replay_required_separately':True,
        'historical_point_in_time_verified':False,'forecast_qualified':False,'sizing_eligible':False}
