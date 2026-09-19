"""Reproducible transaction amounts; no causal market forecast or allocation authority."""
from datetime import date
from decimal import Decimal
import tic_original as native

CONTRACT='tic-original-research.v1'
PREFIX='data/tic-research/'
CURRENT='data/capital-inflows.json'
encoded=native.encoded
digest=native.digest
SID={v[1]:k for k,v in native.SERIES.items()}
ASSETS=('treasuries','agency_bonds','corporate_bonds','equities')


def month_before(day,n):
    d=date.fromisoformat(day);year,month=divmod(d.year*12+d.month-1-n,12)
    return f'{year:04d}-{month+1:02d}-01'


def bn(value):return str(Decimal(value)/1000) if value is not None else None
def number(value):return float(value) if value is not None else None


def window(rows,at,n):
    mapping={r['date']:r for r in rows};months=[month_before(at,i) for i in range(n)]
    missing=[d for d in months if d not in mapping or mapping[d]['value_decimal'] is None]
    total=sum(Decimal(mapping[d]['value_decimal']) for d in months) if not missing else None
    return {'status':'incomplete' if missing else 'complete','months':list(reversed(months)),'missing_months':missing,
        'usd_million_decimal':str(total) if total is not None else None,'usd_bn_decimal':bn(total),
        'source_rows':[mapping[d]['row_index'] for d in months if d in mapping]}


def reconcile(total,children,day):
    refs={};values=[];missing=[]
    for name,rows in [('total',total),*children.items()]:
        row=next((r for r in rows if r['date']==day),None)
        refs[name]={'date':day,'row_index':row['row_index'] if row else None,'usd_million_decimal':row['value_decimal'] if row else None}
        if not row or row['value_decimal'] is None:missing.append(name)
        else:values.append(Decimal(row['value_decimal']))
    gap=values[0]-sum(values[1:]) if not missing else None
    tolerance=Decimal(len(children)+1)/2
    status='incomplete' if missing else 'exact' if gap==0 else 'within_reporting_rounding' if abs(gap)<=tolerance else 'unreconciled'
    return {'date':day,'status':status,'gap_usd_million_decimal':str(gap) if gap is not None else None,
        'rounding_bound_usd_million_decimal':str(tolerance),'missing_components':missing,'components':refs,
        'rule':'Rounded integer USD millions: absolute total-minus-components gap <= 0.5 million times the number of reported amounts; this checks arithmetic, not independent economic accuracy.'}


def build(inputs,read,at):
    if inputs.get('contract')!='tic-original-inputs.v1':raise ValueError('TIC input contract differs')
    originals=inputs['originals'];vintage=inputs['requested_fred_vintage'];date.fromisoformat(vintage)
    if vintage>native.clock(at).date().isoformat():raise ValueError('future requested vintage')
    measurements,archive=native.cslt(originals['bulk'],read,at);errors=dict(inputs.get('acquisition_errors',{}));calendar=None
    try:calendar=native.release_calendar(originals,read,at)
    except Exception as exc:errors['release_calendar']='validation_'+type(exc).__name__
    histories={};parity={};all_rows={};source_clocks={}
    def shard(kind,rows,**metadata):
        doc={'contract':'tic-history.v1','kind':kind,'rows':rows,**metadata};body=encoded(doc);sha=digest(doc);key=PREFIX+'histories/'+sha+'.json'
        histories[key]=body
        return {'key':key,'sha256':sha,'bytes':len(body),'observations':len(rows),'kind':kind}
    for sid,m in measurements.items():
        rows=m.pop('rows');all_rows[m['role']]=rows;acquired=m['original']['acquired_at'];source_clocks['CSLT:'+sid]=acquired
        state={'status':'unavailable','independent_evidence_roots':1,'reason':'FRED republishes the same CSLT root; agreement is a distribution-integrity check.'}
        if sid+':definition' in originals and sid+':observations' in originals:
            try:
                checked=native.fred(sid,originals,read,at,vintage)
                left={r['date']:r['value_decimal'] for r in rows};right={r['date']:r['value_decimal'] for r in checked['rows']}
                differing=[d for d in sorted(set(left)|set(right)) if d not in left or d not in right or left[d]!=right[d]]
                state.update(status='matching' if not differing else 'source_disagreement',compared_periods=len(set(left)&set(right)),
                    different_periods=differing,fred_coverage=checked['coverage'],fred_definition=checked['definition'],originals=checked['originals'],
                    history=shard('fred_comparison',checked['rows'],series_id=sid,unit='usd_million',requested_vintage=vintage))
                for part,ref in checked['originals'].items():source_clocks['FRED:'+sid+':'+part]=ref['acquired_at']
            except Exception as exc:errors[sid+':fred_comparison']='validation_'+type(exc).__name__
        parity[sid]=state
        m['history']=shard('native_monthly_transactions',rows,series_id=sid,native_source_id=m['native_source_id'],unit='usd_million',original=m['original'])
        m['value']=number(m['value_decimal']);m['value_usd_bn_decimal']=bn(m['value_decimal'])
        m['quality']=native.quality(m['as_of'],acquired,calendar,at,m['value_decimal'] is None,state['status']=='source_disagreement')
        m['current_vintage_totals']={str(n):window(rows,m['as_of'],n) for n in (1,3,12)}
        m.update(call=None,calls_eligible=False,sizing_eligible=False)
    if calendar:
        for key,ref in calendar['originals'].items():source_clocks['calendar:'+key]=ref['acquired_at']
    total=all_rows['total'];asof=measurements[SID['total']]['as_of'];latest={name:window(rows,asof,1) for name,rows in all_rows.items()}
    sums={name:window(rows,asof,12) for name,rows in all_rows.items()};three=window(total,asof,3);prior=window(total,month_before(asof,12),12)
    def val(item):return Decimal(item['usd_million_decimal']) if item['usd_million_decimal'] is not None else None
    into=val(sums['total']);abroad=val(sums['us_abroad']);recent=val(three);before=val(prior)
    net=into-abroad if into is not None and abroad is not None else None
    by_asset={name:{'latest_month_b':number(latest[name]['usd_bn_decimal']),'rolling_12mo_b':number(sums[name]['usd_bn_decimal']),
        'data_asof':asof,'unit':'usd_bn','series_id':SID[name],'latest':latest[name],'rolling_12mo':sums[name]} for name in ASSETS}
    reconciliations=[];rolling=[]
    for row in total:
        day=row['date'];assets=reconcile(total,{k:all_rows[k] for k in ASSETS},day);holders=reconcile(total,{k:all_rows[k] for k in ('official','private')},day)
        reconciliations.append({'date':day,'asset_classes':assets,'official_private':holders})
        totals={name:window(rows,day,12) for name,rows in all_rows.items()}
        a,b=val(totals['total']),val(totals['us_abroad'])
        rolling.append({'date':day,'rolling_12mo_b':number(totals['total']['usd_bn_decimal']),
            'net_cross_border_lt_12mo_b':number(bn(a-b)) if a is not None and b is not None else None,
            'totals':totals})
    latest_recon=reconciliations[-1];months=set(sums['total']['months']);window_recon=[r for r in reconciliations if r['date'] in months]
    split_ok=latest_recon['official_private']['status'] in ('exact','within_reporting_rounding')
    split12_ok=len(window_recon)==12 and all(r['official_private']['status'] in ('exact','within_reporting_rounding') for r in window_recon)
    qstates={sid:m['quality']['status'] for sid,m in measurements.items()}
    incomplete=[]
    if any(v['status']!='complete' for v in sums.values()):incomplete.append('complete_latest_twelve_months')
    if len(window_recon)!=12 or any(r[k]['status'] not in ('exact','within_reporting_rounding') for r in window_recon for k in ('asset_classes','official_private')):
        incomplete.append('reconciled_latest_twelve_months')
    all_fresh=all(s=='fresh' for s in qstates.values()) and all(p['status']=='matching' for p in parity.values()) and not errors and not incomplete
    quality={'status':'fresh' if all_fresh else 'partial','observation_date':asof,'input_status':qstates,
        'missing':sorted([*errors,*incomplete]),'frequency':'monthly','publication_date':None,'generated_at':at,
        'vintage_date':vintage,'vintage_basis':'Explicit FRED request day; Treasury source acquired separately. Neither date is verified initial publication.',
        'original_archive_acquired_at':originals['bulk']['acquired_at'],'calendar_expires_at':(calendar or {}).get('calendar_expires_at'),
        'max_acquisition_age_hours':26,'calendar_acquired_at':(calendar or {}).get('acquired_at')}
    out={'contract':CONTRACT,'engine':'capital-inflows','version':'2.0.0','methodology_version':'original_cslt_transactions.v1','generated_at':at,
        'data_asof':asof,'ok':True,'quality':quality,'call':None,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'regime':'MONITOR_ONLY','regime_interpretation':'Dated monthly securities transactions. No validated sudden-stop probability, return forecast or allocation signal.',
        'flags':[],'units':'usd_bn','source_unit':'usd_million','duration_s':None,'measurements':measurements,'cross_source_checks':parity,
        'archive':archive,'release_calendar':calendar,'source_clocks':source_clocks,'errors':len(errors),'source_status_codes':errors,
        'headline':{'foreign_net_into_us_lt_12mo_b':number(bn(into)),'net_cross_border_lt_12mo_b':number(bn(net)),
            'latest_month_b':number(latest['total']['usd_bn_decimal']),'run_rate_3mo_annualized_b':number(bn(recent*4)) if recent is not None else None,
            'yoy_change_12mo_b':number(bn(into-before)) if into is not None and before is not None else None,
            'short_term_treasury_12mo_b':number(sums['short_treasury']['usd_bn_decimal'])},
        'exact_headline':{'foreign_net_into_us_lt_12mo':sums['total'],'us_net_purchases_of_foreign_lt_12mo':sums['us_abroad'],
            'net_cross_border_lt_12mo_usd_million_decimal':str(net) if net is not None else None,'latest_three_months':three,'prior_nonoverlapping_twelve_months':prior,
            'net_formula':'Foreign net purchases of U.S. long-term securities minus U.S. net purchases of foreign long-term securities, same twelve calendar months.',
            'annualization_note':'Four times three reported monthly flows is an arithmetic extrapolation, not a forecast or additional observed period.'},
        'by_asset_class':by_asset,'holder_splits':{'lt_total':{'status':'OK' if split_ok else 'UNAVAILABLE','month':asof,
            'recon_gap_bn':number(bn(latest_recon['official_private']['gap_usd_million_decimal'])),
            'official':{'latest':number(latest['official']['usd_bn_decimal']) if split_ok else None,'sum_12m':number(sums['official']['usd_bn_decimal']) if split12_ok else None},
            'private':{'latest':number(latest['private']['usd_bn_decimal']) if split_ok else None,'sum_12m':number(sums['private']['usd_bn_decimal']) if split12_ok else None},
            'rolling_twelve_months_reconciled':split12_ok,'definition':'Foreign official and non-official CSLT classifications; residence/coverage conventions and staff estimates apply.'}},
        'reconciliation':{'latest':latest_recon,'history':shard('component_reconciliation',reconciliations,unit='usd_million'),
            'rounding_note':'Each integer-million source amount carries up to 0.5 million rounding uncertainty; all dates are checked before qualifying a composed total.'},
        'rolling_history':shard('rolling_twelve_months',rolling,unit='mixed_explicit',field_units={
            'rows.*.rolling_12mo_b':'usd_bn','rows.*.net_cross_border_lt_12mo_b':'usd_bn',
            'rows.*.totals.*.usd_million_decimal':'usd_million','rows.*.totals.*.usd_bn_decimal':'usd_bn'}),
        'history_12mo_rolling_b':[{'asof':r['date'],'rolling_12mo_b':r['rolling_12mo_b']} for r in reversed(rolling)],
        'thesis':'Trace actual reported and estimated cross-border securities transactions by security type and holder classification.',
        'sources':{'release':'Treasury International Capital: Continuous Securities Long Term (CSLT), Treasury inputs plus Federal Reserve staff estimates',
            'release_id':3,'canonical_feed':CURRENT,'total_series':SID['total'],'asset_series':{k:SID[k] for k in ASSETS},
            'short_treasury':SID['short_treasury'],'us_abroad':SID['us_abroad'],'official_series':SID['official'],'private_series':SID['private'],
            'native_archive':originals['bulk'],'vintage_date':vintage,'retrieval_date':originals['bulk']['acquired_at'],
            'definition':'Net purchases at market value. Excludes valuation and residual other position changes, banking flows, direct investment and most derivatives.'},
        'vintage_note':'Complete currently distributed series retained. Historical observations reconstructed today do not establish what an investor knew then.',
        'field_units':{'headline.*_b':'usd_bn','measurements.*.value_decimal':'usd_million','holder_splits.lt_total.*.latest':'usd_bn'},
        'decision_qualification':{'status':'monitor_only','independent_root':'US_TREASURY:TIC:CSLT','return_model_qualified':False,'reason':'Transaction evidence has no demonstrated net-of-cost forecasting or sizing authority.'},
        'legacy_preservation':inputs['legacy'],'disclaimer':'Research measurements and explicit hypothetical scenarios; no automatic trading recommendation.'}
    return out,histories
