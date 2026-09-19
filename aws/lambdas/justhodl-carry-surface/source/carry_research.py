"""Dated income and rate comparisons with retained originals and explicit limits."""
import calendar
from collections import Counter
from datetime import date
from decimal import Decimal
import carry_catalog as catalog
import carry_original as native

CONTRACT='carry-original-research.v1'
PREFIX='data/carry-research/'
CURRENT='data/carry-surface.json'
AUTHORITY={'call':None,'decisive_call':None,'portfolio_action':'WAIT','calls_eligible':False,
 'sizing_eligible':False,'execution_eligible':False,'allocation_pct':None,'forecast_eligible':False}
encoded=native.encoded
digest=native.digest

def ref(row):return {k:row[k] for k in ('date','row_index','segment') if k in row}

def monthly_gap(foreign,funding,at):
    result={'id':foreign['id']+':DFF_MONTHLY_GAP','name':'Reported foreign monthly rate minus same-month mean DFF',
      'unit':'percentage_points','frequency':'M','left':foreign['id'],'right':'DFF','rows':[],
      'value_decimal':None,'as_of':foreign['as_of'],'status':'unavailable',
      'limitation':'Different instruments and tenors. Same-month comparison is not forward-implied carry, a borrowing quote, or a tradable expected return.'}
    if not funding:return result
    groups={}
    for row in funding['rows']:groups.setdefault(row['date'][:7],[]).append(row)
    for row in foreign['rows']:
        y,m=map(int,row['date'][:7].split('-'));required=calendar.monthrange(y,m)[1];rows=groups.get(row['date'][:7],[])
        expected={date(y,m,d).isoformat() for d in range(1,required+1)}
        valid=(len(rows)==required and {r['date'] for r in rows}==expected and all(r['value_decimal'] is not None for r in rows)
          and row['value_decimal'] is not None and date(y,m,required)<native.clock(at).date() and row.get('status') in ('observed','A'))
        mean=sum(Decimal(r['value_decimal']) for r in rows)/required if valid else None
        gap=Decimal(row['value_decimal'])-mean if valid else None
        result['rows'].append({'date':row['date'],'value_decimal':str(gap) if gap is not None else None,
          'foreign_rate_decimal':row['value_decimal'],'usd_overnight_monthly_mean_decimal':str(mean) if mean is not None else None,
          'foreign_ref':ref(row),'funding_refs':[ref(r) for r in rows],'expected_calendar_days':required,'observed_calendar_days':len(rows),
          'status':'dated_comparison' if valid else 'incomplete_month'})
    latest=result['rows'][-1]
    if foreign['quality']['status']!='fresh' or funding['quality']['status']!='fresh':result['status']='source_not_current'
    elif latest['value_decimal'] is not None:result.update(status='dated_comparison',value_decimal=latest['value_decimal'])
    else:result['status']='latest_month_incomplete'
    return result

def daily_gap(item,funding):
    result={'id':item['id']+':DFF_DAILY_GAP','name':'Same-date nominal yield minus overnight DFF','unit':'percentage_points','frequency':'D',
      'left':item['id'],'right':'DFF','rows':[],'as_of':item['as_of'],'value_decimal':None,'status':'unavailable',
      'limitation':'Yield spread only. Does not include bond price risk, rolldown, defaults, financing spread, transaction costs, or reinvestment. Not expected or realized carry.'}
    if item['kind']=='real_yield':result['status']='real_nominal_comparison_prohibited';return result
    if not funding:return result
    bydate={r['date']:r for r in funding['rows']}
    for row in item['rows']:
        other=bydate.get(row['date']);value=row['value_decimal'];base=other['value_decimal'] if other else None
        gap=Decimal(value)-Decimal(base) if value is not None and base is not None else None
        result['rows'].append({'date':row['date'],'value_decimal':str(gap) if gap is not None else None,
          'yield_decimal':value,'funding_decimal':base,'yield_ref':ref(row),'funding_ref':ref(other) if other else None,
          'status':'dated_comparison' if gap is not None else 'same_date_input_missing'})
    latest=result['rows'][-1]
    if item['quality']['status']!='fresh' or funding['quality']['status']!='fresh':result['status']='source_not_current'
    elif latest['value_decimal'] is not None:result.update(status='dated_comparison',value_decimal=latest['value_decimal'])
    else:result['status']='latest_date_incomplete'
    return result

def shard(identifier,unit,frequency,rows,artifacts,kind='observations'):
    if not rows:return None
    value={'contract':'carry-history.v1','id':identifier,'unit':unit,'frequency':frequency,'kind':kind,'rows':rows}
    body=encoded(value);sha=digest(value);key=PREFIX+'histories/'+sha+'.json';artifacts[key]=body
    return {'key':key,'sha256':sha,'bytes':len(body),'observations':len(rows),'from':rows[0]['date'],'to':rows[-1]['date'],'kind':kind,'unit':unit,'frequency':frequency}

def build(inputs,read,at):
    if inputs.get('contract')!='carry-original-inputs.v1':raise ValueError('carry input contract differs')
    native.clock(at);errors=dict(inputs.get('acquisition_errors',{}));measured={};equities={};clocks={};artifacts={}
    for sid in catalog.FRED:
        refs=inputs.get('fred',{}).get(sid)
        if not refs:errors[sid]='original_source_unavailable';continue
        try:measured[sid]=native.fred(sid,refs,read,at)
        except (ValueError,TypeError,KeyError,ArithmeticError):errors[sid]='original_validation_failed'
    if inputs.get('ecb'):
        try:measured[catalog.ECB_EURIBOR_ID]=native.ecb(inputs['ecb'],read,at)
        except (ValueError,TypeError,KeyError,ArithmeticError):errors['ECB_EURIBOR']='original_validation_failed'
    else:errors['ECB_EURIBOR']='original_source_unavailable'
    for symbol in catalog.EQUITIES:
        refs=inputs.get('equities',{}).get(symbol)
        if not refs:errors[symbol]='original_source_unavailable';continue
        try:equities[symbol]=native.equity(symbol,refs,read,at)
        except (ValueError,TypeError,KeyError,ArithmeticError):errors[symbol]='original_validation_failed'
    funding=measured.get('DFF');comparisons={};by_class={k:[] for k in ('equity','fx','fixed_income','commodity')}
    for symbol,definition in catalog.EQUITIES.items():
        item=equities.get(symbol);summary={'symbol':symbol,'asset_class':'equity','security_type':definition['expected_type'],'research_id':'FMP:'+symbol,
          'carry_pct':None,'carry_per_vol':None,'cash_income_yield_pct':None,'quality':item['quality'] if item else {'status':'unavailable'},**AUTHORITY}
        if item:
            income=item['trailing_distribution'];value=income['yield_pct_decimal']
            summary.update(as_of=item['as_of'],price_decimal=item['price_decimal'],trailing_distribution_yield_pct_decimal=value,
              cash_income_yield_pct=float(value) if value is not None else None,income_status=income['status'])
            # Same-date funding comparison is clearly distinct from actual financing economics.
            fund=next((r for r in funding['rows'] if r['date']==item['as_of']),None) if funding else None
            gap=Decimal(value)-Decimal(fund['value_decimal']) if value is not None and fund and fund['value_decimal'] is not None else None
            item['funding_reference_comparison']={'yield_minus_dff_pp_decimal':str(gap) if gap is not None else None,
              'as_of':item['as_of'],'funding_ref':ref(fund) if fund else None,'funding_decimal':fund['value_decimal'] if fund else None,
              'status':'dated_comparison' if gap is not None else 'same_date_input_missing',
              'limitation':'Historical trailing distributions minus a same-date overnight reference. Neither is promised income or the investor borrowing rate. Not expected total return.'}
            for key,unit,frequency in (('prices','USD_per_current_share','D'),('distributions','USD_per_current_share','event'),('splits','ratio','event')):
                rows=item.pop(key);item[key+'_history']=shard(item['id'],unit,frequency,rows,artifacts,key)
            clocks[item['id']]=item['quality']['acquired_at']
        by_class['equity'].append(summary)
    for symbol,definition in catalog.FX.items():
        sid=definition['series'];item=measured.get(sid)
        row={'symbol':symbol,'legacy_alias':definition['legacy_alias'],'asset_class':'fx','research_id':sid,'rate_kind':definition['rate_kind'],
          'carry_pct':None,'carry_per_vol':None,'quality':item['quality'] if item else {'status':'unavailable'},**AUTHORITY}
        if item:
            row.update(as_of=item['as_of'],rate_pct_decimal=item['value_decimal'])
            if sid!='DFF':
                pair=monthly_gap(item,funding,at);comparisons[pair['id']]=pair;row['comparison_id']=pair['id']
        if symbol=='EUR' and catalog.ECB_EURIBOR_ID in measured:
            alternate=measured[catalog.ECB_EURIBOR_ID];pair=monthly_gap(alternate,funding,at);comparisons[pair['id']]=pair
            row['explicit_alternative']={'research_id':alternate['id'],'comparison_id':pair['id'],'as_of':alternate['as_of'],
              'rate_pct_decimal':alternate['value_decimal'],'quality':alternate['quality'],
              'note':'ECB Euribor 3M monthly average, separate series and history. Does not overwrite the older OECD definition.'}
        by_class['fx'].append(row)
    for symbol,definition in catalog.FIXED_INCOME.items():
        sid=definition['series'];item=measured.get(sid)
        row={'symbol':symbol,'asset_class':'fixed_income','research_id':sid,'yield_kind':definition['kind'],'carry_pct':None,'carry_per_vol':None,
          'quality':item['quality'] if item else {'status':'unavailable'},**AUTHORITY}
        if item:
            row.update(as_of=item['as_of'],yield_pct_decimal=item['value_decimal'])
            pair=daily_gap(item,funding);comparisons[pair['id']]=pair;row['comparison_id']=pair['id']
        by_class['fixed_income'].append(row)
    for symbol in catalog.COMMODITIES:
        by_class['commodity'].append({'symbol':symbol,'asset_class':'commodity','carry_pct':None,'carry_per_vol':None,'quality':{'status':'unavailable'},
          'missing':['dated futures curve','contract identities and expiries','roll rule','collateral return','fees'],
          'note':'ETF spot performance is not an observed futures roll yield.',**AUTHORITY})
    for identifier,item in measured.items():
        clocks[identifier]=item['quality']['acquired_at'];rows=item.pop('rows');item['history']=shard(identifier,item['unit'],item['frequency'],rows,artifacts)
        item.update(AUTHORITY)
    for identifier,item in comparisons.items():
        rows=item.pop('rows');item['history']=shard(identifier,item['unit'],item['frequency'],rows,artifacts,'matched_rate_comparison');item.update(AUTHORITY)
    statuses=Counter(r['quality']['status'] for r in [*measured.values(),*equities.values()]);missing=len(catalog.FRED)+1+len(catalog.EQUITIES)-len(measured)-len(equities)
    out={'contract':CONTRACT,'version':'2.0.1','methodology_version':'original_income_and_matched_rates.v2','generated_at':at,
      'headline':'Dated distribution income and funding comparisons; no calibrated carry allocation or unwind forecast.',
      'quality':{'status':'partial' if measured or equities else 'unavailable','source_status_counts':dict(statuses),'unavailable_source_families':missing,
        'basis':'Each observation has its own date and acquisition limit. Missing commodity curves and unqualified signals remain unavailable.'},
      'ok':bool(measured or equities),'measurements':measured,'equities':equities,'comparisons':comparisons,'by_class':by_class,
      'all_assets':[r for rows in by_class.values() for r in rows],'n_assets':sum(map(len,by_class.values())),
      'n_dormant':sum(r['quality']['status']=='unavailable' for rows in by_class.values() for r in rows),
      'source_clocks':clocks,'errors':len(errors),'source_status_codes':errors,'legacy_preservation':inputs.get('legacy'),'cross_asset_top':[],'cross_asset_bottom':[],
      'risk_adjusted_leaders':[],'dislocation_leaders':[],'unwind_overlay':{'cohort_fragility':None,'status':'NOT_CALIBRATED','calls_eligible':False},
      'regime_summary':{'status':'NOT_CALIBRATED'},'financing_rate_pct':None,'financing_source':{'series':'DFF','role':'Descriptive overnight reference, not investor financing'},
      'decision_qualification':{'status':'research_only','reason':'No point-in-time, net-of-cost out-of-sample qualification for these carry proxies.',
        'missing':['executable funding and hedging quotes','instrument-level total returns and costs','independent economic hypotheses','out-of-sample calibration','portfolio risk and constraints']},
      'history_basis':'Native current-vintage source records with original row references. First-received timestamps do not reconstruct what investors knew historically.',
      'scenario_basis':'User-entered hypothetical positions and costs only; no private accounts or automatic portfolio actions.',**AUTHORITY}
    return out,artifacts
