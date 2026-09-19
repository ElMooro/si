"""Replayed central-bank stock reconciliation, with no inferred policy-flow authority."""
from datetime import date
from decimal import Decimal
from cb_native import POLICY, ECB, fred_inputs, ecb_input, measure, previous, quality
from report_observations import decimal
from research_brief_model import clock, digest, encoded
from pd_fails_context import project as project_fails

CONTRACT='cb-research.v1'
METHOD='cb-component-measurements.v3'
REASON='Native-currency stock changes do not identify purchases, maturities, FX/gold valuation or net policy injections. These measurements have no calibrated return forecast or position-size authority.'
FED={'total_assets':'WALCL','securities_outright':'WSHOSHO','primary_credit':'WLCFLPCL','central_bank_swaps':'SWPT'}


def decompose(series, unit, sources, stamp):
    components={name:measure(series.get(sid,{'source_id':sid,'unit':unit}),stamp) for name,sid in sources.items()}
    out={'status':'incomplete','unit':unit,'observation_date':None,'components':components,
         'stock_change_1m':None,'stock_change_1m_decimal':None,'other_assets_and_adjustments_level':None,
         'other_assets_and_adjustments_change_1m':None,'fx_valuation_change_1m':None,
         'policy_purchase_transactions_1m':None,'net_injection_estimate':None,'reconciliation_residual':None,
         'interpretation':REASON,'calls_eligible':False,'sizing_eligible':False}
    if any(m['quality']['status']!='fresh' for m in components.values()):return out
    lookups={name:{row['date']:row for row in series[sid]['rows']} for name,sid in sources.items()}
    common=set.intersection(*(set(rows) for rows in lookups.values()))
    if not common:return out
    ending=max(common); out['observation_date']=ending
    if not 0<=(clock(stamp).date()-date.fromisoformat(ending)).days<=21:return out
    total_rows=[lookups['total_assets'][day] for day in sorted(common,reverse=True)]
    old,target=previous(total_rows,1,'W');start=old['date'] if old else None
    out.update(start_date=start,calendar_target_date=target)
    now_values={name:decimal(rows[ending]['value_decimal']) for name,rows in lookups.items()}
    before={name:decimal(rows[start]['value_decimal']) for name,rows in lookups.items()} if start else {}
    for name,m in components.items():
        val=now_values[name];base=before.get(name);delta=val-base if val is not None and base is not None else None
        m.update(aligned_value=float(val) if val is not None else None,
                 aligned_value_decimal=str(val) if val is not None else None,
                 aligned_change_1m=float(delta) if delta is not None else None,
                 aligned_change_1m_decimal=str(delta) if delta is not None else None,
                 aligned_current=lookups[name][ending],aligned_baseline=lookups[name][start] if start else None)
    if any(v is None or v<0 for v in now_values.values()):return out
    other=now_values['total_assets']-sum(v for k,v in now_values.items() if k!='total_assets')
    out.update(other_assets_and_adjustments_level=float(other),other_assets_and_adjustments_level_decimal=str(other))
    if other<0:out['status']='invalid_components_exceed_total';return out
    if not before or any(v is None or v<0 for v in before.values()):return out
    old_other=before['total_assets']-sum(v for k,v in before.items() if k!='total_assets')
    if old_other<0:out['status']='invalid_components_exceed_total';return out
    delta=now_values['total_assets']-before['total_assets'];residual_change=other-old_other
    attributed=sum(now_values[k]-before[k] for k in now_values if k!='total_assets')
    residual=delta-attributed-residual_change
    out.update(status='partial_attribution',stock_change_1m=float(delta),stock_change_1m_decimal=str(delta),
        other_assets_and_adjustments_change_1m=float(residual_change),other_assets_and_adjustments_change_1m_decimal=str(residual_change),
        reconciliation_residual=float(residual),reconciliation_residual_decimal=str(residual))
    return out


def fails_context(document, stamp):
    result=project_fails(document,clock(stamp))
    try:source_age=(clock(stamp)-clock(document['generated_at'])).total_seconds()
    except (KeyError,TypeError,ValueError,AttributeError):source_age=None
    result['source_age_seconds']=source_age
    result['max_source_age_seconds']=26*3600
    # A captured engine packet is reproducible context, not original FR2004 proof.
    for row,source_key,total_key in ((result,'treasury','gross_bn'),(result['ust_ex_tips'],'headline','combined_bn')):
        original=document.get(source_key,{}) if isinstance(document,dict) else {}
        units=original.get('field_units') or {}
        units_ok=all(units.get(k)=='usd_bn' for k in ('ftd_bn','ftr_bn',total_key))
        vals=[decimal(row.get(k)) for k in ('ftd_bn','ftr_bn','combined_bn')]
        reconciles=all(v is not None for v in vals) and abs(vals[0]+vals[1]-vals[2])<=Decimal('0.02')
        row['reconciliation']={'consistent':reconciles,'tolerance_usd_bn':'0.02'}
        if not reconciles:row['quality']['status']='inconsistent_or_missing_scope'
        if not units_ok:
            row['quality']['status']='unverified_units'
            row['retained_unqualified_values']={key:row[key] for key in ('ftd_bn','ftr_bn','combined_bn')}
            row.update(ftd_bn=None,ftr_bn=None,combined_bn=None)
        if row['quality']['status']=='fresh' and (source_age is None or source_age<0 or source_age>26*3600):
            row['quality']['status']='stale_or_invalid_source_clock'
        row['original_provider_verified']=False
        row['provenance_status']='retained_engine_context_only'
    return result


def build(source, originals, ecb_originals, fails, stamp, legacy_context=None, acquisition_errors=None):
    clock(stamp);native=fred_inputs(source,originals);ecb={};errors=dict(acquisition_errors or {})
    for name,item in ecb_originals.items():
        if name not in ECB:raise ValueError('unexpected ECB identity')
        ecb[name]=ecb_input(name,item['raw'],item['evidence'],item['acquired_at'])
    all_series={**native,**{ECB[name].replace('/','.',1):row for name,row in ecb.items()}}
    observed={sid:measure(native.get(sid,{'source_id':sid,'unit':policy[2]}),stamp) for sid,policy in POLICY.items()}
    banks=[]
    specs=(('Fed','USD','WALCL','DFEDTARU','Fed target upper bound',True),
           ('ECB','EUR','ECBASSETSW','ECBDFR','ECB deposit facility rate',True),
           ('BOJ','JPY','JPNASSETS','IR3TIB01JPM156N','OECD monthly 3M interbank rate; not central-bank policy rate',False),
           ('SNB','CHF',None,'IR3TIB01CHM156N','OECD monthly 3M interbank rate; not central-bank policy rate',False))
    for name,ccy,bs,rate,definition,policy in specs:
        assets=observed[bs] if bs else measure({'source_id':'not configured','unit':'CHF_bn'},stamp)
        interest=observed[rate]
        reconciliation=(decompose(native,'USD_bn',FED,stamp) if name=='Fed' else
            decompose(all_series,'EUR_bn',{k:v.replace('/','.',1) for k,v in ECB.items()},stamp) if name=='ECB' else
            {'status':'unavailable','net_injection_estimate':None,'missing':['purchases and maturities','lending','FX and gold valuation accounting']})
        banks.append({'cb':name,'currency':ccy,'balance_sheet':assets,'rate':interest,'rate_definition':definition,
            'policy_rate_pct':interest['latest'] if policy else None,'interbank_proxy_pct':None if policy else interest['latest'],
            'decomposition':reconciliation,'injection_stance':None,'stance_label':'NOT_ATTRIBUTED',
            'quality':{'status':'partial' if assets['latest'] is not None or interest['latest'] is not None else 'unavailable'},
            'calls_eligible':False,'sizing_eligible':False})
    ecb_total=measure(ecb.get('total_assets',{}),stamp);fred_total=observed['ECBASSETSW']
    matching=(ecb_total['latest_decimal'] is not None and fred_total['latest_decimal'] is not None
              and ecb_total['quality']['observation_date']==fred_total['quality']['observation_date'])
    difference=decimal(fred_total['latest_decimal'])-decimal(ecb_total['latest_decimal']) if matching else None
    fresh=sum(m['quality']['status']=='fresh' for m in observed.values())
    return {'contract':CONTRACT,'schema_version':'3.0','version':'3.0.0','methodology_version':METHOD,'generated_at':stamp,
        'source_generated_at':source['generated_at'],'source_replay':source['replay'],'ok':fresh>0,
        'quality':{'status':'partial' if fresh else 'unavailable','fresh_native_series':fresh,'expected_native_series':len(POLICY),
                   'fresh_ecb_series':sum(quality(row,stamp)['status']=='fresh' for row in ecb.values()),'expected_ecb_series':len(ECB),
                   'reason':'Transaction and valuation attribution incomplete; source availability is measured separately.'},
        'headline':'Original-bound central-bank stocks, rates and matched-date component reconciliation.',
        'central_banks':banks,'measurements':observed,'ecb_components':{k:measure(v,stamp) for k,v in ecb.items()},
        'source_comparisons':{'ecb_total_assets':{'matching_observation_dates':matching,'difference_eur_bn_decimal':str(difference) if difference is not None else None,
            'status':'agree' if difference==0 else 'disagree' if difference is not None else 'not_comparable',
            'meaning':'FRED and ECB copies of the same economic series are one evidence family, not independent signals.'}},
        'fx_context':{name:observed[sid] for name,sid in [('JPY','DEXJPUS'),('CHF','DEXSZUS'),('EUR','DEXUSEU')]},
        'pd_settlement_fails':fails_context(fails,stamp),'global_injection_impulse':{'score':None,'label':'NOT_ATTRIBUTED','reason':REASON},
        'carry_trade':{'carry_conditions':'NOT_CALIBRATED','unwind_risk_score':None,'unwind_risk_label':'NOT_CALIBRATED'},
        'call':None,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'eurodollar_read':None,'cross_reference':{},
        'decision':{'verb':'WAIT','meaning':'abstain','reason':REASON},
        'portfolio_consequences':{'status':'UNAVAILABLE','target_weights':None,'expected_return':None,'forced_liquidation':False,'reason':REASON},
        'dependency_groups':{'fed_h41':list(FED.values()),'eurosystem_weekly_statement':['ECBASSETSW',*ECB.values()],
            'boj_accounts':['JPNASSETS'],'policy_rates':['ECBDFR','DFEDTARU'],'monthly_interbank':['IR3TIB01JPM156N','IR3TIB01CHM156N'],
            'fed_h10_fx':['DEXJPUS','DEXSZUS','DEXUSEU'],'fr2004':['pd_settlement_fails']},
        'legacy_context':legacy_context,'errors':errors,'paid_ai_calls':0,'notifications_sent':0,
        'history_basis':'Current-vintage source histories, not a reconstruction of historical release-time availability.',
        'note':REASON+' Rate changes are percentage points. Settlement fails are separate two-sided gross context, never an injection term.'}
