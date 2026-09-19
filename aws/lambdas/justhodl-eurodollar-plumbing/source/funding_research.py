"""Dated funding measurements and declared comparisons, without crisis votes."""
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal
import hashlib
import json
import re

import canonical_macro_sources
import funding_original as native
from funding_research_catalog import SERIES
import report_observations

CONTRACT='funding-original-research.v1'
CURRENT='data/eurodollar-plumbing.json'
PREFIX='data/funding-research/'
AUTHORITY={'call':None,'portfolio_action':'WAIT','calls_eligible':False,'forecast_eligible':False,
           'sizing_eligible':False,'execution_eligible':False,'allocation_pct':None}
LAYERS={'us_core':'US money markets','bank_funding':'Bank and term funding','credit':'Credit context',
        'backstops':'Central-bank balance-sheet observations','settlement':'Reported settlement fails',
        'fx':'Dollar and cross-border balance-sheet context','hubs':'Currency and country funding context'}


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def digest(value):return hashlib.sha256(encoded(value)).hexdigest()


def fred(source,read,at):
    originals=canonical_macro_sources.originals(source,read,SERIES);out={};errors={}
    for sid,(layer,unit,frequency,max_age,note) in SERIES.items():
        value=originals.get(sid)
        if value is None:errors[sid]='original_source_unavailable';continue
        try:
            measured=report_observations.measurement(sid,value['definition'],value['observations'],value['evidence'],at,value['acquired_at'])
            if measured['unit']!=unit or measured['frequency']!=frequency:raise ValueError('reviewed native unit or frequency differs')
            rows=[];future=[]
            for index,r in enumerate(value['observations']['observations']):
                number=native.amount(r['value']);item={'date':r['date'],'value_decimal':number,'row_index':index,
                    'status':'observed' if number is not None else 'missing',
                    'realtime_start':r.get('realtime_start'),'realtime_end':r.get('realtime_end')}
                (future if r['date']>native.clock(at).date().isoformat() else rows).append(item)
            item=native.entry(sid,measured['name'],unit,frequency,rows,measured['definition'],value['evidence'],value['acquired_at'],at,max_age)
            item.update(layer=layer,limitation=note,coverage=measured['coverage'],future_dated_rows=future,
                        source_vintage='current_provider_response',published_at=None)
            out[sid]=item
        except (ValueError,KeyError,TypeError,ArithmeticError) as exc:errors[sid]=type(exc).__name__
    return out,errors


def statistics(rows,frequency):
    """Prior observations only; no probability interpretation or observation-count year."""
    n=252 if frequency=='D' else 52 if frequency=='W' else 12 if frequency=='M' else 0
    prior=[];current=rows[-1] if rows else {};result={'method':'prior_observations_excluding_current.v1','requested_prior':n,
        'prior_n':0,'excluded_missing_observations':0,'z':None,'percentile':None,'status':'insufficient_comparable_history','from':None,'to':None}
    if not n or current.get('value_decimal') is None:return result
    later=current
    for row in reversed(rows[:-1]):
        if frequency=='D' and row['value_decimal'] is None:
            result['excluded_missing_observations']+=1
            continue
        gap=(date.fromisoformat(later['date'])-date.fromisoformat(row['date'])).days
        compatible=(0<gap<=6 if frequency=='D' else gap==7 if frequency=='W' else
                    date.fromisoformat(later['date']).year*12+date.fromisoformat(later['date']).month-
                    (date.fromisoformat(row['date']).year*12+date.fromisoformat(row['date']).month)==1)
        if not compatible or row['value_decimal'] is None:break
        prior.append(row);later=row
        if len(prior)==n:break
    result['prior_n']=len(prior)
    if prior:result.update({'from':prior[-1]['date'],'to':prior[0]['date']})
    if len(prior)<n:return result
    values=[Decimal(r['value_decimal']) for r in prior];latest=Decimal(current['value_decimal']);mean=sum(values)/len(values)
    sd=(sum((x-mean)**2 for x in values)/(len(values)-1)).sqrt()
    result.update(mean_decimal=str(mean),sample_sd_decimal=str(sd),
        percentile=float(100*(sum(v<latest for v in values)+Decimal('0.5')*sum(v==latest for v in values))/len(values)),
        z=float((latest-mean)/sd) if sd else None,status='available' if sd else 'constant_prior_sample')
    return result


def changes(rows):
    if not rows:return {}
    latest=rows[-1];indexed={r['date']:r for r in rows};out={}
    for label,days in (('1w',7),('13w',91),('52w',364)):
        target=(date.fromisoformat(latest['date'])-timedelta(days=days)).isoformat();baseline=indexed.get(target)
        valid=baseline is not None and baseline['value_decimal'] is not None and latest['value_decimal'] is not None
        change=Decimal(latest['value_decimal'])-Decimal(baseline['value_decimal']) if valid else None
        out[label]={'from':target,'to':latest['date'],'difference_decimal':str(change) if valid else None,
                    'difference':float(change) if valid else None,'status':'available' if valid else 'exact_calendar_endpoint_unavailable',
                    'baseline_row_index':baseline['row_index'] if baseline else None}
    return out


def pair(measured,identifier,left,right,label,anchor,limitation):
    """Use one explicitly named latest observation date, never a hidden old common date."""
    a=measured.get(left);b=measured.get(right);ref=measured.get(anchor);rows=[]
    out={'id':identifier,'label':label,'unit':'basis_points','value':None,'value_decimal':None,'as_of':ref['as_of'] if ref else None,
        'left':left,'right':right,'anchor_series':anchor,'formula':'100 * (left percent quote - right percent quote)',
        'limitation':limitation,'status':'unavailable','components':{},'rows':[],**AUTHORITY}
    if not a or not b or not ref or a['unit']!='Percent' or b['unit']!='Percent':return out
    ai={r['date']:r for r in a['rows']};bi={r['date']:r for r in b['rows']}
    for base in ref['rows']:
        day=base['date'];ra=ai.get(day);rb=bi.get(day)
        valid=ra and rb and ra['value_decimal'] is not None and rb['value_decimal'] is not None
        val=100*(Decimal(ra['value_decimal'])-Decimal(rb['value_decimal'])) if valid else None
        rows.append({'date':day,'value_decimal':str(val) if val is not None else None,'row_index':base['row_index'],
                     'left_row_index':ra['row_index'] if ra else None,'right_row_index':rb['row_index'] if rb else None,
                     'status':'matched_date' if val is not None else 'same_date_leg_missing'})
    day=ref['as_of'];last=rows[-1]
    out.update(rows=rows,components={left:ai.get(day),right:bi.get(day)})
    if a['quality']['status']!='fresh' or b['quality']['status']!='fresh':out['status']='source_not_current'
    elif last['value_decimal'] is None:out['status']='same_date_leg_missing'
    else:out.update(status='dated_comparison',value_decimal=last['value_decimal'],value=float(last['value_decimal']))
    out['quality']={'status':'fresh' if out['status']=='dated_comparison' else 'unavailable',
        'acquired_at':min(a['quality']['acquired_at'],b['quality']['acquired_at']),
        'max_acquisition_age_hours':26,'observation_date':day,
        'max_observation_age_days':min(a['quality']['max_observation_age_days'],b['quality']['max_observation_age_days'])}
    return out


PAIRS=(
 ('sofr_iorb','SOFR','IORB','SOFR minus IORB','SOFR','Secured market median versus an administered rate; not an executable trade or reserve-scarcity test.'),
 ('sofr99_iorb','SOFR99','IORB','SOFR 99th percentile minus IORB','SOFR99','A distribution tail versus an administered rate; percentile is not the probability of funding stress.'),
 ('effr_iorb','EFFR','IORB','EFFR minus IORB','EFFR','Unsecured overnight transactions versus an administered rate.'),
 ('obfr_iorb','OBFR','IORB','OBFR minus IORB','OBFR','Defined bank funding transactions versus an administered rate; not the whole offshore dollar market.'),
 ('cp_ois','DCPF3M','SOFR','Three-month financial CP minus overnight SOFR','DCPF3M','Mixed tenors and instruments. This is a dated quote difference, not a CP-OIS spread or isolated credit premium.'),
 ('cpn_ois','DCPN3M','SOFR','Three-month nonfinancial CP minus overnight SOFR','DCPN3M','Mixed tenors and instruments. A matched-tenor OIS quote is unavailable.'),
 ('bill_ois','SOFR','DTB3','Overnight SOFR minus three-month bill quote','DTB3','Mixed tenor and quotation bases: simple overnight versus discount-basis bill yield. Not a bill-OIS spread.'),
 ('gcf_tri','REPO-GCF_AR_TOT-P','REPO-TRI_AR_TOT-P','OFR preliminary GCF minus tri-party average rate','REPO-TRI_AR_TOT-P','Same report date and preliminary vintage, but different collateral/tenor populations. Not an identified balance-sheet scarcity premium.'),
)


def compact_histories(output):
    """Keep initial page downloads small; every full history has a verified immutable object."""
    artifacts={}
    for row in [*output['measurements'].values(),*output['comparisons'].values()]:
        records=row.pop('rows',[])
        if not records:row['history']=None;continue
        fields=sorted(set().union(*(set(r) for r in records)) - {'date','value_decimal','row_index','status'})
        columns=['date','value_decimal','row_index','status',*fields]
        history={'contract':'funding-history.v1','id':row['id'],'unit':row['unit'],'frequency':row.get('frequency','quote' if row['id']=='cnh_cny' else 'D'),'columns':columns,
                 'rows':[[r.get(k) for k in columns] for r in records]}
        raw=encoded(history);sha=hashlib.sha256(raw).hexdigest();key=PREFIX+'histories/'+sha+'.json';artifacts[key]=raw
        row['history']={'key':key,'sha256':sha,'bytes':len(raw),'observations':len(records),
                        'first':records[0]['date'],'last':records[-1]['date']}
    return artifacts


def spot_gap(measured):
    """A simultaneous indicative spot difference; never a currency funding basis."""
    left=measured.get('USDCNH');right=measured.get('USDCNY')
    out={'id':'cnh_cny','label':'Offshore minus onshore indicative renminbi spot',
         'unit':'yuan_per_USD','value':None,'value_decimal':None,'as_of':None,
         'left':'USDCNH','right':'USDCNY','components':{},'rows':[],
         'formula':'USDCNH indicative last price minus USDCNY indicative last price',
         'limitation':'Different onshore/offshore markets. Quotes must be within 60 seconds; this is not a forward-implied cross-currency funding basis.',
         'status':'unavailable',**AUTHORITY}
    if not left or not right:return out
    out.update(as_of=left['as_of'],components={r['id']:{'quoted_at':r['quoted_at'],'value_decimal':r['value_decimal']} for r in (left,right)})
    if any(r['quality']['status']!='fresh' for r in (left,right)):out['status']='source_not_current';return out
    difference=abs((native.clock(left['quoted_at'])-native.clock(right['quoted_at'])).total_seconds())
    out['timestamp_difference_seconds']=difference
    if difference>60:out['status']='quote_times_not_aligned';return out
    value=Decimal(left['value_decimal'])-Decimal(right['value_decimal'])
    out.update(value=float(value),value_decimal=str(value),status='dated_comparison',
        rows=[{'date':left['as_of'],'value_decimal':str(value),'row_index':0,'status':'matched_quote_times',
               'left_quoted_at':left['quoted_at'],'right_quoted_at':right['quoted_at']}],
        quality={**left['quality'],'acquired_at':min(left['quality']['acquired_at'],right['quality']['acquired_at'])})
    return out


def fails_context(packet,read,at):
    empty={'status':'unavailable','scopes':{},'original_calculations_reexecuted_here':False,**AUTHORITY}
    if not packet:return empty
    if packet.get('contract')!='fr2004-fails-research.v1':return {**empty,'reason':'original_fails_contract_missing'}
    ref=packet.get('replay') or {};key=ref.get('manifest_key','')
    if not re.fullmatch(r'data/fails-research/runs/[a-f0-9]{64}\.json',key):raise ValueError('fails run path differs')
    manifest=json.loads(read(key));target=manifest['output'];body=read(target['key'])
    if key!='data/fails-research/runs/'+digest(manifest)+'.json' or target['key']!='data/fails-research/outputs/'+target['sha256']+'.json':
        raise ValueError('fails manifest identity differs')
    if len(body)!=target['bytes'] or hashlib.sha256(body).hexdigest()!=target['sha256'] or ref.get('output_sha256')!=target['sha256'] or manifest.get('output_sha256')!=target['sha256']:
        raise ValueError('fails retained output differs')
    if json.loads(body)!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('fails pointer differs from retained snapshot')
    scopes={}
    for name,scope in (('treasury','treasury_incl_tips'),('headline','ust_ex_tips')):
        row=packet[name];q=dict(row['quality']);day=row['as_of']
        if row['scope_id']!=scope or row['unit']!='usd_bn':raise ValueError('fails scope or unit differs')
        amounts={key:row[key] for key in ('ftd_usd_mn','ftr_usd_mn','gross_usd_mn')}
        if row['complete'] and (any(type(v) is not int or v<0 for v in amounts.values()) or amounts['ftd_usd_mn']+amounts['ftr_usd_mn']!=amounts['gross_usd_mn']):
            raise ValueError('fails integer sum differs')
        for short,full in (('ftd','ftd_usd_mn'),('ftr','ftr_usd_mn'),('gross','gross_usd_mn')):
            exact=row['exact_usd_bn'][short]
            if (exact is None)!=(amounts[full] is None) or (exact is not None and Decimal(exact)*1000!=amounts[full]):
                raise ValueError('fails exact unit conversion differs')
        age=(native.clock(at)-native.clock(q['acquired_at'])).total_seconds()
        if q['status']=='fresh' and (not 0<=age<=36*3600 or native.clock(at)>native.clock(q['next_expected_publication_date'])+timedelta(hours=24)):q['status']='stale'
        scopes[name]={'scope_id':scope,'as_of':day,'unit':'usd_bn',**amounts,'exact_usd_bn':row['exact_usd_bn'],'quality':q}
    return {**empty,'status':'retained_output_verified','scopes':scopes,'source_replay':ref,
            'note':'Two-sided gross reported fails, not unique securities. Including-TIPS and excluding-TIPS scopes overlap; never add them.'}


def build(inputs,read,at):
    measured,errors=fred(inputs['source'],read,at)
    errors.update(inputs.get('acquisition_errors') or {})
    for name in native.URLS:
        descriptor=inputs['originals'].get(name)
        if not descriptor:errors.setdefault(name,'original_source_unavailable');continue
        try:measured.update(native.load(name,descriptor,read,at))
        except (ValueError,KeyError,TypeError,ArithmeticError) as exc:errors[name]=type(exc).__name__
    comparisons={r[0]:pair(measured,*r) for r in PAIRS}
    comparisons['cnh_cny']=spot_gap(measured)
    for row in measured.values():
        row['statistics']=statistics(row['rows'],row['frequency']);row['changes']=changes(row['rows'])
        row.setdefault('layer','us_core' if row['id'] in native.OFR_IDS else 'bank_funding' if row['id'].startswith('ofr_fsi:') else
                       'backstops' if row['id']==native.ECB_KEY else 'hubs')
    for row in comparisons.values():
        if row['rows']:row['statistics']=statistics(row['rows'],'quote' if row['id']=='cnh_cny' else 'D');row['changes']=changes(row['rows'])
    revisions={}
    for sid in native.OFR_IDS:
        if not sid.endswith('-F') or sid not in measured or sid[:-1]+'P' not in measured:continue
        final=measured[sid];preliminary=measured[sid[:-1]+'P'];pi={r['date']:r for r in preliminary['rows']};differences=[]
        for row in final['rows']:
            other=pi.get(row['date'])
            if other and row['value_decimal'] is not None and other['value_decimal'] is not None:
                delta=Decimal(row['value_decimal'])-Decimal(other['value_decimal'])
                if delta:differences.append({'date':row['date'],'final_minus_preliminary_decimal':str(delta),'final_row':row['row_index'],'preliminary_row':other['row_index']})
        revisions[sid]={'final_as_of':final['as_of'],'preliminary_as_of':preliminary['as_of'],
                        'different_dates':len(differences),'latest_differences':differences[-12:],
                        'basis':'Cross-vintage comparison of the two currently retained originals; not a reconstruction of all past publication-time vintages.'}
    counts=dict(Counter(row['quality']['status'] for row in measured.values()))
    fails=fails_context(inputs.get('fails'),read,at)
    pd={'source':'data/settlement-fails.json','source_replay':fails.get('source_replay'),'role':'context_only',**AUTHORITY}
    for key,target in (('treasury',pd),('headline',{})):
        row=fails['scopes'].get(key) or {};values=row.get('exact_usd_bn') or {}
        target.update(scope_id=row.get('scope_id'),as_of=row.get('as_of'),unit='usd_bn',
            ftd_bn=float(values['ftd']) if values.get('ftd') is not None else None,
            ftr_bn=float(values['ftr']) if values.get('ftr') is not None else None,
            combined_bn=float(values['gross']) if values.get('gross') is not None else None,
            quality=row.get('quality') or {'status':'unavailable'})
        if key=='headline':pd['ust_ex_tips']=target
    output={'engine':'justhodl-eurodollar-plumbing','contract':CONTRACT,'version':'2.0.0','generated_at':at,'source_generated_at':inputs['source']['generated_at'],
        'source_clocks':{'canonical_macro':inputs['source']['generated_at'],**{k:v['acquired_at'] for k,v in inputs['originals'].items()}},
        'measurements':measured,'comparisons':comparisons,'ofr_vintage_comparisons':revisions,'errors':errors,
        'quality':{'status':'degraded' if errors or any(k not in ('fresh','historical_final') for k in counts) else 'fresh','counts':counts,
                   'original_measurements':len(measured),'original_fred_requested':len(SERIES)},
        'plumbing_health':None,'composite_score':None,'score':None,'stress_score':None,'verdict':'UNQUALIFIED',
        'severity':'UNQUALIFIED','stress_regime':'UNQUALIFIED','red_flags':[],'yellow_flags':[],
        'ai':{'state':'WAIT','summary':'Dated source measurements are available. No validated funding-crisis classifier or directional forecast is assigned.',
              'short_term':None,'key_drivers':[],'method':'deterministic_source_description'},
        'missing_capabilities':{'cross_currency_basis':'Requires matched forward points, both currency money-market curves, tenor, day counts and conventions.',
            'executable_carry':'Indicative spot and benchmark rates are not borrow/lend quotes, collateral terms or an executable hedged return.',
            'crisis_probability':'No out-of-sample calibrated classifier or current strategy permission.'},
        'portfolio_consequences':{'status':'requires_explicit_user_assumptions','formula':'incremental simple funding cost = USD liability * rate_shock_bp / 10000 * days / selected_day_basis',
            'scope':'Hypothetical floating-rate USD liability, constant entered shock; excludes hedges, fees, compounding, collateral changes and asset returns.',
            'position_size':None,'forecast_eligible':False},
        'legacy_retention':inputs['legacy_ref'],'legacy_inventory':inputs.get('legacy_inventory') or [],
        'context_snapshots':inputs.get('contexts') or {},'settlement_fails':fails,'pd_settlement_fails':pd,
        'methodology':'Exact retained originals, declared units and observation dates. Current-vintage histories are not publication-time backtests. Reported balances, transactions and policy operations have different economic meanings.',
        **AUTHORITY}
    layers={key:{'title':title,'metrics':[]} for key,title in LAYERS.items()}
    for row in measured.values():
        layers[row['layer']]['metrics'].append({'id':row['id'],'label':row['label'],'value':row['value'] if row['quality']['status']=='fresh' else None,
            'unit':row['unit'],'asof':row['as_of'],'status':'info' if row['quality']['status']=='fresh' else 'unknown',
            'detail':row.get('limitation'),'source_measurement':row['id'],'pctile':None})
    for identifier,row in comparisons.items():
        layer='hubs' if identifier=='cnh_cny' else 'bank_funding' if identifier in ('cp_ois','cpn_ois','bill_ois') else 'us_core'
        layers[layer]['metrics'].append({'id':identifier,'label':row['label'],'value':row['value'],'unit':row['unit'],'asof':row['as_of'],
            'status':'info' if row['value'] is not None else 'unknown','detail':row['limitation'],'source_comparison':identifier,'pctile':None})
    for scope,row in fails['scopes'].items():
        for field,label in (('ftd','Fails to deliver'),('ftr','Fails to receive'),('gross','Two-sided gross fails')):
            value=row['exact_usd_bn'][field]
            layers['settlement']['metrics'].append({'id':scope+'_'+field,'label':row['scope_id']+' · '+label,
                'value':float(value) if value is not None and row['quality']['status']=='fresh' else None,
                'unit':'usd_bn','asof':row['as_of'],'status':'info' if row['quality']['status']=='fresh' else 'unknown',
                'detail':fails['note'],'source_replay':fails.get('source_replay'),'pctile':None})
    output['layers']=layers
    histories=compact_histories(output)
    return output,histories
