"""Replayable weekly balance research with explicit scope and no trade authority."""
from datetime import date,timedelta
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import json,re
import canonical_macro_sources
import official_original as native

CONTRACT='official-original-research.v1'
PREFIX='data/official-research/'
CURRENT='data/official-pulse.json'
encoded=native.encoded
digest=native.digest
clock=native.clock
QUALIFICATION={'status':'MONITOR_ONLY','call':None,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
 'reason':'Custody and reverse-repo balances are descriptive stocks. No validated price impact, investor-intent inference or return model.'}

def decimal_text(value):return format(value,'f') if value is not None else None
def bn(value):return float(Decimal(value)/1000) if value is not None else None

def change(by_day,day,weeks):
    ending=date.fromisoformat(day);required=[(ending-timedelta(weeks=k)).isoformat() for k in range(weeks+1)]
    invalid=[d for d in required if not by_day.get(d,{}).get('analysis_eligible')]
    return {'weeks':weeks,'start_date':required[-1],'end_date':day,'required_observations':weeks+1,
      'unavailable_dates':invalid,'status':'unavailable' if invalid else 'complete',
      'change_usd_million_decimal':None if invalid else decimal_text(Decimal(by_day[day]['value_decimal'])-Decimal(by_day[required[-1]]['value_decimal']))}

def statistics(by_day,day):
    current=change(by_day,day,13);prior=[];end=date.fromisoformat(day)
    for k in range(1,521):prior.append(change(by_day,(end-timedelta(weeks=k)).isoformat(),13))
    complete=[Decimal(v['change_usd_million_decimal']) for v in prior if v['status']=='complete']
    out={'statistic':'13-week balance change vs 520 preceding weekly 13-week changes','prior_window_start':prior[-1]['end_date'],
      'prior_window_end':prior[0]['end_date'],'required_prior_changes':520,'available_prior_changes':len(complete),
      'current_excluded_from_reference':True,'overlapping_windows':True,'z_decimal':None,'percentile_midrank_decimal':None,
      'status':'unavailable','predictive_interpretation':False,'rounding':'Decimal precision 42, round-half-even to 8 decimal places'}
    if current['status']!='complete' or len(complete)!=520:return out
    with localcontext() as ctx:
        ctx.prec=42;ctx.rounding=ROUND_HALF_EVEN
        x=Decimal(current['change_usd_million_decimal']);mean=sum(complete)/Decimal(520)
        sd=(sum((v-mean)**2 for v in complete)/Decimal(519)).sqrt();rounding=Decimal('.00000001')
        out.update(mean_usd_million_decimal=str(mean.quantize(rounding)),sample_sd_usd_million_decimal=str(sd.quantize(rounding)),
          z_decimal=str(((x-mean)/sd).quantize(rounding)) if sd else None,
          percentile_midrank_decimal=str(((Decimal(sum(v<x for v in complete))+Decimal(sum(v==x for v in complete))/2)/Decimal(520)*100).quantize(rounding)),
          status='complete' if sd else 'zero_reference_variance')
    return out

def quality(day,acquired,calendar,at,disagreement=False):
    age=(clock(at).date()-date.fromisoformat(day)).days;acquisition_age=(clock(at)-clock(acquired)).total_seconds()/3600
    expected=calendar.get('nominal_expected_wednesday');expires=calendar.get('calendar_expires_at')
    status='source_disagreement' if disagreement else 'stale' if age>14 or acquisition_age>48 or expected and day<expected else 'fresh'
    if not expected or not expires or clock(at)>clock(expires):status='calendar_unverified' if status=='fresh' else status
    return {'status':status,'observation_date':day,'observation_age_days':age,'acquired_at':acquired,'acquisition_age_hours':round(acquisition_age,3),
      'frequency':'weekly','observation_sla_days':14,'acquisition_sla_hours':48,'nominal_expected_wednesday':expected,'calendar_expires_at':expires,
      'note':'Generated time does not reset observation age. Fresh means current within this declared source policy, not qualified for trading.'}

def reconciliation(series,total,parts):
    maps={k:{r['date']:r for r in series[k]['rows']} for k in (total,*parts)};days=sorted(set().union(*(set(m) for m in maps.values())));rows=[]
    bound=Decimal(len(parts)+1)/2  # Integer-million rounding of total and each leg.
    for day in days:
        valid=all(maps[k].get(day,{}).get('analysis_eligible') for k in maps)
        values={k:maps[k].get(day,{}).get('value_decimal') for k in maps}
        residual=Decimal(values[total])-sum(Decimal(values[k]) for k in parts) if valid else None
        rows.append({'date':day,'components_usd_million_decimal':values,'residual_usd_million_decimal':decimal_text(residual),
          'rounding_bound_usd_million_decimal':decimal_text(bound),
          'status':'unavailable' if residual is None else 'within_reporting_rounding' if abs(residual)<=bound else 'outside_reporting_rounding'})
    return {'total':total,'components':parts,'unit':'usd_million','rows':rows,'rule':'Total minus separately rounded components; no cross-scope cash-plus-custody sum.'}

def tic_context(packet,read,at):
    if not packet:return {'status':'unavailable','additional_independent_votes':0,**QUALIFICATION}
    key=(packet.get('replay') or {}).get('manifest_key','')
    if not re.fullmatch(r'data/foreign-research/runs/[a-f0-9]{64}\.json',key):return {'status':'unverified_context','generated_at':packet.get('generated_at'),'additional_independent_votes':0,**QUALIFICATION}
    manifest=json.loads(read(key));ref=manifest['output']
    if ref['key']!='data/foreign-research/outputs/'+ref['sha256']+'.json' or ref['sha256']!=manifest['output_sha256'] or packet['replay'].get('output_sha256')!=ref['sha256']:raise ValueError('TIC context output binding differs')
    body=read(ref['key'])
    import hashlib
    if key!='data/foreign-research/runs/'+digest(manifest)+'.json' or len(body)!=ref['bytes'] or hashlib.sha256(body).hexdigest()!=manifest['output_sha256'] or digest({k:v for k,v in packet.items() if k!='replay'})!=manifest['output_sha256']:raise ValueError('TIC context snapshot differs')
    if clock(packet['generated_at'])>clock(at):raise ValueError('future TIC context')
    return {'status':'immutable_descriptive_context','generated_at':packet['generated_at'],'observation_date':packet.get('latest_month'),
      'quality':packet.get('quality'),'holder_splits':packet.get('holder_splits'),'replay':packet['replay'],
      'independent_evidence_root':'US_TREASURY:TIC:CSLT','additional_independent_votes':0,
      'interpretation':'Monthly securities transactions and weekly custody levels have different scope and dates; neither validates a stock-change-as-buying inference.',**QUALIFICATION}

def build(inputs,read,stamp):
    if inputs.get('contract')!='official-original-inputs.v1':raise ValueError('official input contract differs')
    refs=inputs['originals'];series,archive_info=native.archive(refs['native_archive'],read,stamp)
    calendar=native.calendar(refs,read,stamp);printed=native.release(refs['release_html'],read,stamp,series)
    faq=native.original(refs['scope_definition'],read,native.FAQ_URL,stamp).decode('utf-8')
    if 'July 2007' not in faq or 'current face' not in faq:raise ValueError('custody back-history definition differs')
    canonical=inputs['canonical'];originals=canonical_macro_sources.originals(canonical,read,('WLRRAFOIAL','WMTSECL1'))
    if clock(canonical['generated_at'])>clock(stamp):raise ValueError('future canonical source')
    histories={};measurements={};comparisons={};reconciliations={}
    def retain(doc):
        raw=encoded(doc);sha=digest(doc);key=PREFIX+'histories/'+sha+'.json';histories[key]=raw
        return {'key':key,'sha256':sha,'bytes':len(raw),'rows':len(doc['rows'])}
    for name,part in series.items():
        rows=part['rows'];by_day={r['date']:r for r in rows};latest=rows[-1];windows={str(w):change(by_day,latest['date'],w) for w in (1,4,13,26,52)}
        stats=statistics(by_day,latest['date']);comparison=None
        if name in ('foreign_rrp','custody') and part['fred_id'] in originals:
            comparison=native.distribution(originals[part['fred_id']],part)
            comparison['history']=retain({'contract':'official-distribution-history.v1',**comparison})
            comparisons[name]={k:v for k,v in comparison.items() if k!='rows'}
        elif name in ('foreign_rrp','custody'):comparisons[name]={'status':'canonical_unavailable','fred_id':part['fred_id'],'additional_independent_votes':0}
        for row in rows:
            row['changes']={str(w):change(by_day,row['date'],w) for w in (4,13,26)}
        ref=retain({'contract':'official-native-history.v1',**part})
        disagreement=bool(comparison and comparison['different_values']) or any(c['measurement']==name and not c['matches'] for c in printed['checks'])
        q=quality(latest['date'],refs['native_archive']['acquired_at'],calendar,stamp,disagreement)
        if not latest['analysis_eligible']:q['status']='unavailable'
        measurements[name]={k:v for k,v in part.items() if k not in ('rows','original')}
        measurements[name].update(id=part['fred_id'] or part['native_id'],status='LIVE' if q['status']=='fresh' else q['status'].upper(),
            latest_date=latest['date'],latest_bn=bn(latest['value_decimal']) if latest['analysis_eligible'] else None,
            latest_usd_million_decimal=latest['value_decimal'],n_obs=len(rows),first=rows[0]['date'],history=ref,changes=windows,
            quality=q,statistics=stats,z_13wchg_10y=None,descriptive_z_13wchg_prior520=stats['z_decimal'],
            source=refs['native_archive'],scope_definition=refs['scope_definition'],call=None,calls_eligible=False,sizing_eligible=False)
        for weeks in (4,13,26):measurements[name]['chg_%dw_bn'%weeks]=bn(windows[str(weeks)]['change_usd_million_decimal'])
    for name,total,parts in [('reverse_repo','rrp_total',['foreign_rrp','rrp_other']),('custody','custody_total',['custody','custody_agency','custody_other'])]:
        doc=reconciliation(series,total,parts);ref=retain({'contract':'official-reconciliation-history.v1',**doc})
        reconciliations[name]={'latest':doc['rows'][-1],'history':ref,'total':total,'components':parts,
           'outside_reporting_rounding':sum(r['status']=='outside_reporting_rounding' for r in doc['rows']),
           'unavailable':sum(r['status']=='unavailable' for r in doc['rows']),'rule':doc['rule']}
    context=tic_context(inputs.get('tic'),read,stamp)
    all_fresh=all(v['quality']['status']=='fresh' for v in measurements.values())
    errors=inputs.get('acquisition_errors',{})
    current_recon=all(v['latest']['status']=='within_reporting_rounding' for v in reconciliations.values())
    out={'contract':CONTRACT,'schema_version':CONTRACT,'v':'2.0.0','engine':'justhodl-official-pulse','generated_at':stamp,
      'as_of':min(v['latest_date'] for v in measurements.values()),'status':'MONITOR_ONLY','call':None,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
      'doctrine':'Weekly balances and changes with reproducible evidence; no purchases, reserve-management intent, funding crisis or asset-return inference.',
      'quality':{'status':'fresh' if all_fresh and current_recon and not errors else 'partial','measurement_count':len(measurements),
        'fresh_measurements':sum(v['quality']['status']=='fresh' for v in measurements.values()),'current_reconciliations_pass':current_recon,
        'calendar_expires_at':calendar['calendar_expires_at'],'calls_eligible':False,'sizing_eligible':False},
      'measurements':measurements,'foreign_rrp':measurements['foreign_rrp'],'custody':measurements['custody'],
      'distribution_checks':comparisons,'reconciliations':reconciliations,'release':printed,'calendar':calendar,'archive':archive_info,
      'source_clocks':{k:v['acquired_at'] for k,v in refs.items()},'source_generated_at':canonical['generated_at'],
      'canonical_replay':canonical['replay'],'source_status_codes':errors,'qualification':QUALIFICATION,'legacy':inputs['legacy'],
      'dollar_leg':{'status':'UNKNOWN','available':0,'legs_firing':0,'firing':[],
        'legs':{name:{'z':None,'fires':False,'available':False,'qualification':'MONITOR_ONLY'} for name in ('official_flows_monthly','safe_haven_monthly','custody_weekly')},
        'tic_context':context,'doctrine':'No qualified stress-vote model; UNKNOWN is abstention, not CALM.'},
      'ff_generated_at':context.get('generated_at'),'independent_evidence_roots':[native.ROOT],
      'lineage':{'native_archive_and_FRED_are_same_evidence_root':True,'duplicated_votes':0,'TIC_monthly_context_is_not_a_weekly_balance_input':True},
      'scope_notes':['Custody Treasury securities are current face value, include STRIPS and TIPS inflation compensation, and exclude collateral pledged against Fed reverse repos.',
        'Foreign official reverse repos are Fed cash liabilities; FIMA repo assets are a different facility and side of the balance sheet.',
        'Custody transfers, valuation conventions, redemptions and transactions have different meanings. A custody change does not identify purchases or intent.',
        'All native rows and statuses are retained. Custody before July 2007 is outside documented current-face back-history; the first July week-average is conservatively excluded because its full daily window begins before July.',
        'Current-vintage source history is not evidence of historical publication-time knowledge. Revisions are preserved in new immutable runs, never merged into an old run.',
        'The 520 prior weekly 13-week changes overlap. Their z-score and percentile are descriptive and do not provide independent observations or a return forecast.'],
      'portfolio_consequences':{'automatic_allocation':False,'estimated_price_impact':None,'reason':'Use separately entered duration, yield and currency scenarios; these balances do not estimate those shocks.'}}
    return out,histories
