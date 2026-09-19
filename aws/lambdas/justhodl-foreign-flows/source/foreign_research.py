"""Recompile dated foreign securities research from original Treasury reports."""
from datetime import date
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import foreign_original as native
import foreign_supplement as supplement

CONTRACT='foreign-original-research.v1'
PREFIX='data/foreign-research/'
CURRENT='data/foreign-flows.json'
encoded=native.encoded
digest=native.digest
FLOW_FAMILIES={'total':'lt_total','treas':'treas','equity':'lt_eqty','corp':'lt_corp','agency':'lt_agcy','tbills':'st_treas'}
COUNTRY_ALIASES={'china':'41408','japan':'42609','united_kingdom':'13005','belgium':'10251','luxembourg':'11703',
 'cayman':'36137','ireland':'11401','france':'10804','switzerland':'12688','canada':'29998','taiwan':'46302','hong_kong':'42005',
 'singapore':'46019','korea':'43001','india':'42102','brazil':'30309','norway':'12203','saudi_arabia':'45608','uae':'46604',
 'mexico':'31704','germany':'11002','australia':'60089','bahamas':'35319','bermuda':'35602'}


def month_before(day,n):
    value=date.fromisoformat(day);year,month=divmod(value.year*12+value.month-1-n,12)
    return f'{year:04d}-{month+1:02d}-01'


def dec(value):return Decimal(value) if value is not None else None
def text(value):return str(value) if value is not None else None
def bn(value):return float(Decimal(value)/1000) if value is not None else None


def windows(mapping,at,n):
    months=[month_before(at,i) for i in reversed(range(n))]
    missing=[day for day in months if mapping.get(day,{}).get('value_decimal') is None]
    value=sum((Decimal(mapping[day]['value_decimal']) for day in months),Decimal(0)) if not missing else None
    return {'status':'incomplete' if missing else 'complete','ending_month':at,'calendar_months':n,'missing_months':missing,
        'usd_million_decimal':text(value),'source_rows':[mapping[d]['row_index'] for d in months if d in mapping]}


def zscore(mapping,at):
    current=dec(mapping.get(at,{}).get('value_decimal'));prior=[dec(mapping.get(month_before(at,n),{}).get('value_decimal')) for n in range(1,121)]
    if current is None or any(v is None for v in prior):return None
    with localcontext() as ctx:
        ctx.prec=42;mean=sum(prior)/120;variance=sum((v-mean)**2 for v in prior)/119
        if variance==0:return None
        return float(((current-mean)/variance.sqrt()).quantize(Decimal('0.00000001'),rounding=ROUND_HALF_EVEN))


def decompose(maps,family,at,n):
    pos=maps.get(family+':pos',{});current=dec(pos.get(at,{}).get('value_decimal'));prior=dec(pos.get(month_before(at,n),{}).get('value_decimal'))
    tx=windows(maps.get(family+':net',{}),at,n);valuation=windows(maps.get(family+':valchg',{}),at,n)
    delta=current-prior if current is not None and prior is not None else None
    net=dec(tx['usd_million_decimal']);value=dec(valuation['usd_million_decimal'])
    other=delta-net-value if all(v is not None for v in (delta,net,value)) else None
    return {'ending_month':at,'calendar_months':n,'status':'complete' if other is not None else 'incomplete',
        'holdings_usd_million_decimal':text(current),'prior_holdings_usd_million_decimal':text(prior),
        'holdings_change_usd_million_decimal':text(delta),'transactions_usd_million_decimal':text(net),
        'valuation_change_usd_million_decimal':text(value),'other_change_residual_usd_million_decimal':text(other),
        'position_rows':[pos.get(at,{}).get('row_index'),pos.get(month_before(at,n),{}).get('row_index')],
        'transaction_rows':tx['source_rows'],'valuation_rows':valuation['source_rows'],
        'missing_transaction_months':tx['missing_months'],'missing_valuation_months':valuation['missing_months']}


def linear_series(maps,components):
    dates=sorted(set().union(*(set(maps.get(code,{}).get(key,{})) for code,key,weight in components)))
    out=[]
    for index,day in enumerate(dates):
        source=[];value=Decimal(0);missing=[]
        for code,key,weight in components:
            row=maps.get(code,{}).get(key,{}).get(day);number=dec((row or {}).get('value_decimal'))
            source.append({'group':code,'series':key,'weight':weight,'row_index':row.get('row_index') if row else None})
            if number is None:missing.append(code+':'+key)
            else:value+=number*weight
        out.append({'date':day,'value_decimal':None if missing else str(value),'row_index':index,'source_rows':source,'missing_components':missing})
    return out


def holder_splits(maps,at):
    result={};histories={}
    for family in ('lt_total','lt_treas','st_treas','lt_corp','lt_eqty','lt_agcy'):
        key=family+':net';combined=linear_series(maps,[('99996',key,1),('99990',key,-1),('99991',key,-1)])
        by_day={r['date']:r for r in combined};months=[month_before(at,i) for i in range(12)]
        def okay(day):
            value=dec(by_day.get(day,{}).get('value_decimal'))
            return value is not None and abs(value)<=Decimal('1.5')
        latest_ok=okay(at);full_ok=all(okay(day) for day in months);row={'status':'OK' if latest_ok else 'UNRECONCILED','month':at,
            'recon_gap_bn':bn(by_day.get(at,{}).get('value_decimal')),'rolling_twelve_months_reconciled':full_ok,
            'rounding_bound_usd_million_decimal':'1.5','calls_eligible':False,'sizing_eligible':False}
        for name,code in (('official','99990'),('private','99991')):
            mapping=maps.get(code,{}).get(key,{})
            row[name]={'latest':bn(mapping.get(at,{}).get('value_decimal')) if latest_ok else None,
                'sum_12m':bn(windows(mapping,at,12)['usd_million_decimal']) if full_ok else None,
                'z_10y':zscore(mapping,at) if latest_ok else None,'series_id':'FOR'+family.replace('_','').upper()+'NET'+code}
        result['lt_equity' if family=='lt_eqty' else 'lt_agency' if family=='lt_agcy' else family]=row
        histories[family]=combined
    return result,histories


def table_check(table,areas,maps):
    names={v['name']:k for k,v in areas.items()};result=[]
    direct=[name for name in table['rows'] if name in names and areas[names[name]]['scope']=='reported_country_or_territory']
    aliases={'Of Which: Foreign Official':('99990','treas:pos'),'Of Which: Foreign Official Treasury Bills':('99990','st_treas:pos'),
        'Of Which: Foreign Official T-Bonds & Notes':('99990','lt_treas:pos')}
    for name,item in table['rows'].items():
        code,key=aliases.get(name,(names.get(name),'treas:pos'))
        for day,raw in item['values_usd_million_decimal'].items():
            reported=dec(raw);value=None;components=[]
            if name=='All Other':
                codes=['99996',*[names[n] for n in direct]];values=[dec(maps[c].get('treas:pos',{}).get(day,{}).get('value_decimal')) for c in codes]
                if all(v is not None for v in values):value=values[0]-sum(values[1:])
                bound=Decimal(50)+Decimal(len(codes))/2;components=codes
            else:
                value=dec(maps.get(code,{}).get(key,{}).get(day,{}).get('value_decimal'));bound=Decimal('50.5');components=[code] if code else []
            gap=value-reported if value is not None and reported is not None else None
            result.append({'name':name,'date':day,'table_value_usd_million_decimal':text(reported),'native_value_usd_million_decimal':text(value),
                'gap_usd_million_decimal':text(gap),'rounding_bound_usd_million_decimal':str(bound),
                'status':'incomplete' if gap is None else 'exact' if gap==0 else 'within_reporting_rounding' if abs(gap)<=bound else 'source_disagreement',
                'table_row_index':item['source_row_index'],'native_group_codes':components,'native_family':key})
    return result


def build(inputs,read,at):
    if inputs.get('contract')!='foreign-original-inputs.v1':raise ValueError('Foreign research input contract')
    refs=inputs['originals'];areas,archive=native.archive(refs['bulk'],read,at)
    table=native.holdings_table(refs['table5'],read,at);errors=dict(inputs.get('acquisition_errors',{}));calendar=None
    try:calendar=native.release_calendar(refs,read,at)
    except Exception as exc:errors['release_calendar']='validation_'+type(exc).__name__
    asof=areas['99996']['series']['lt_total:net']['rows'][-1]['date'];maps={code:{key:{r['date']:r for r in series['rows']} for key,series in area['series'].items()} for code,area in areas.items()}
    shards={};groups={};historical_rows=0
    def shard(kind,doc):
        body=encoded({'contract':'foreign-history.v1','kind':kind,**doc});sha=native.hashlib.sha256(body).hexdigest();key=PREFIX+'histories/'+sha+'.json';shards[key]=body
        return {'key':key,'sha256':sha,'bytes':len(body),'kind':kind}
    for code,area in areas.items():
        series_summary={};decompositions={};current_decompositions={};mapping=maps[code]
        for key,series in area['series'].items():
            rows=series['rows'];latest=rows[-1];window={str(n):windows(mapping[key],asof,n) for n in (1,3,12)}
            quality=native.quality(latest['date'],refs['bulk']['acquired_at'],calendar,at,latest['value_decimal'] is None)
            if series['source_status']=='D':quality['status']='discontinued'
            series_summary[key]={'id':series['id'],'source_status':series['source_status'],'quality':{k:quality[k] for k in ('status','observation_age_days','missing_value','source_disagreement')},'coverage':series['coverage'],
                'as_of':latest['date'],'latest_reported_usd_million_decimal':latest['value_decimal'],
                'aligned_month':asof,'aligned_value_usd_million_decimal':mapping[key].get(asof,{}).get('value_decimal'),
                'current_windows':window if series['measure']!='pos' else None,
                'window_note':'Monthly transactions/valuation changes are summed; holdings stocks are never summed across months.'}
            historical_rows+=len(rows)
        for family in ('lt_treas','lt_eqty','lt_total','lt_agcy','lt_corp'):
            if not any(family+':'+part in mapping for part in ('pos','net','valchg')):continue
            dates=sorted(set().union(*(set(mapping.get(family+':'+part,{})) for part in ('pos','net','valchg'))))
            decompositions[family]=[decompose(mapping,family,day,1) for day in dates]
            current_decompositions[family]={str(n):decompose(mapping,family,asof,n) for n in (1,3,12)}
        # Acquisition clocks belong to this run's inputs. Stable source bytes
        # and identities let unchanged full histories reuse their retained key.
        retained_series={key:{**series,'original':{'url':series['original']['url'],'evidence':series['original']['evidence']}} for key,series in area['series'].items()}
        ref=shard('reporting_group',{'code':code,'name':area['name'],'scope':area['scope'],'unit':'usd_million','series':retained_series,'decompositions':decompositions,'current_decompositions':current_decompositions,
            'decomposition_note':'Change in reported holdings = net transactions + valuation change + unexplained residual other changes. Residual is not attributed to an investor or assumed erroneous.'})
        compact={family:{n:{k:v for k,v in row.items() if not k.endswith('_rows') and not k.startswith('missing_')} for n,row in periods.items()} for family,periods in current_decompositions.items()}
        groups[code]={'code':code,'name':area['name'],'scope':area['scope'],'series':series_summary,'current_decompositions':compact,
            'history':ref,'call':None,'calls_eligible':False,'sizing_eligible':False,'ultimate_ownership_identified':False}
    checks=table_check(table,areas,maps);table_ref=shard('major_holders_table',{'table':table,'distribution_checks':checks,'independent_evidence_roots':1})
    flows={};history={}
    for name,family in FLOW_FAMILIES.items():
        key=family+':net';mapping=maps['99996'].get(key,{});series=groups['99996']['series'].get(key,{})
        windows_now={n:windows(mapping,asof,n) for n in (1,3,12)}
        flows[name]={'latest':bn(windows_now[1]['usd_million_decimal']),'sum_3m':bn(windows_now[3]['usd_million_decimal']),
            'sum_12m':bn(windows_now[12]['usd_million_decimal']),'z_10y':zscore(mapping,asof),'series_id':series.get('id'),
            'date':asof,'unit':'usd_bn','scope':'Grand Total including international and regional organizations',
            'definition':'Current monthly net transactions; z-score compares with 120 prior complete calendar months and is descriptive only.'}
        days=sorted(mapping)[-120:];history[name]={'dates':days,'vals':[bn(mapping[d]['value_decimal']) for d in days]}
    split_rows,split_history=holder_splits(maps,asof)
    specs={'risk_appetite':('Non-Treasury long-term net transactions',[('99996','lt_eqty:net',1),('99996','lt_corp:net',1),('99996','lt_agcy:net',1)]),
        'safe_haven':('All-maturity Treasury net transactions minus equity net transactions',[('99996','treas:net',1),('99996','lt_eqty:net',-1)]),
        'total_demand':('Long-term securities plus Treasury bill net transactions',[('99996','lt_total:net',1),('99996','st_treas:net',1)]),
        'official_private':('Non-official minus official long-term net transactions',[('99991','lt_total:net',1),('99990','lt_total:net',-1)])}
    signals={};composed={}
    for name,(label,components) in specs.items():
        rows=linear_series(maps,components);composed[name]={'label':label,'components':components,'rows':rows,'unit':'usd_million'};mapping={r['date']:r for r in rows}
        value=windows(mapping,asof,1);signals[name]={'label':label,'latest_bn':bn(value['usd_million_decimal']),
            'sum_12m_bn':bn(windows(mapping,asof,12)['usd_million_decimal']),'z_10y':zscore(mapping,asof),'date':asof,
            'calls_eligible':False,'sizing_eligible':False,'interpretation':'Arithmetic composition only; the legacy key does not establish investor intent, safe-haven rotation or a market-return signal.'}
        last=rows[-120:];history['sig_'+name]={'dates':[r['date'] for r in last],'vals':[bn(r['value_decimal']) for r in last]}
    checks_ref=shard('flow_compositions',{'compositions':composed,'official_private_reconciliation':split_history,'unit':'usd_million',
        'zscore_method':'120 prior complete calendar months, current excluded; sample standard deviation, Decimal42 digits, rounded half-even to8 decimal places; descriptive, not calibrated.'})
    country_views={family:{} for family in ('lt_treas','lt_eqty')}
    for alias,code in COUNTRY_ALIASES.items():
        for family in country_views:
            group=groups.get(code,{});value=group.get('current_decompositions',{}).get(family,{}).get('12',{})
            tx3=group.get('current_decompositions',{}).get(family,{}).get('3',{})
            country_views[family][alias]={'status':'OK' if value.get('status')=='complete' else 'INCOMPLETE','code':code,'name':group.get('name'),
                'month':asof,'holdings_bn':bn(value.get('holdings_usd_million_decimal')),'d12m_holdings_bn':bn(value.get('holdings_change_usd_million_decimal')),
                'tx_3m_bn':bn(tx3.get('transactions_usd_million_decimal')),'tx_12m_bn':bn(value.get('transactions_usd_million_decimal')),
                'valchg_12m_bn':bn(value.get('valuation_change_usd_million_decimal')),'identity_gap_bn':bn(value.get('other_change_residual_usd_million_decimal')),
                'accel':None,'interpretation_authority':'descriptive_only','history':group.get('history')}
    cn=country_views['lt_treas']['china'];be=country_views['lt_treas']['belgium']
    paired={'status':'OK' if cn['status']==be['status']=='OK' else 'INCOMPLETE','composite':True,'month':asof,
        'name':'China plus Belgium — arithmetic sum','ultimate_owner_attribution_verified':False,
        'note':'Two reported jurisdictions summed at the same month. This is not a verified Euroclear or China custody adjustment.',
        'source_group_codes':['41408','10251'],'accel':None,'calls_eligible':False,'sizing_eligible':False}
    for key in ('holdings_bn','d12m_holdings_bn','tx_3m_bn','tx_12m_bn','valchg_12m_bn','identity_gap_bn'):
        paired[key]=float(Decimal(str(cn[key]))+Decimal(str(be[key]))) if cn[key] is not None and be[key] is not None else None
    country_views['lt_treas']['china_plus_belgium']=paired
    contexts={}
    for name,compile_context in (
        ('absorption',lambda:supplement.fiscal(refs['mspd'],read,at,maps['99996']['treas:net'],asof)),
        ('auctions',lambda:supplement.auctions(refs,read,at))):
        try:
            complete=compile_context();retained=shard(name,complete)
            contexts[name]={k:v for k,v in complete.items() if k not in ('rows','raw_dimensional_rows','layouts','records')}
            contexts[name].update(history=retained,calls_eligible=False,sizing_eligible=False)
            if name=='absorption':contexts[name]['rows']=complete['rows'][-13:]
        except Exception as exc:
            errors[name]='validation_'+type(exc).__name__
            contexts[name]={'status':'UNAVAILABLE','quality':{'status':'unavailable'},'calls_eligible':False,'sizing_eligible':False,
                'why':'Original acquisition or validation failed; no synthetic substitute.'}
    quality=native.quality(asof,refs['bulk']['acquired_at'],calendar,at)
    if errors or any(r['status'] not in ('exact','within_reporting_rounding') for r in checks) or not all(v['rolling_twelve_months_reconciled'] for v in split_rows.values()):quality['status']='partial'
    table_quality=native.quality(table['months'][0],refs['table5']['acquired_at'],calendar,at)
    if table_quality['status']!='fresh' or table['months'][0]!=asof:quality['status']='partial'
    if contexts['absorption'].get('quality',{}).get('status')!='fresh' or any(v.get('status')!='fresh' for v in contexts['auctions'].get('quality_by_source',{}).values()):quality['status']='partial'
    quality.update(generated_at=at,publication_date=None,calendar_acquired_at=(calendar or {}).get('acquired_at'),original_archive_acquired_at=refs['bulk']['acquired_at'])
    out={'contract':CONTRACT,'engine':'foreign-flows','version':'2.0.0','v':'2.0.0','generated_at':at,'as_of':at,'latest_month':asof,'status':'RESEARCH',
        'quality':quality,'source_status_codes':errors,'errors':len(errors),'call':None,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'archive':archive,'groups':groups,'flows_bn':flows,'hist_10y':history,'signals':signals,'holder_splits':split_rows,'composition_history':checks_ref,**contexts,
        'country_lt_treasury':country_views['lt_treas'],'country_lt_equity':country_views['lt_eqty'],
        'holdings_table':{'history':table_ref,'months':table['months'],'rows':len(table['rows']),'quality':table_quality,
            'distribution_checks':len(checks),'matching_checks':sum(v['status'] in ('exact','within_reporting_rounding') for v in checks)},
        'source_clocks':{k:v['acquired_at'] for k,v in refs.items()},'release_calendar':calendar,'native_observations':historical_rows,
        'legacy_preservation':inputs['legacy'],'new_release':False,'excluded':{},'warnings':[],
        'decision_qualification':{'status':'monitor_only','independent_root':'US_TREASURY:TIC:CSLT','return_model_qualified':False},
        'country_note':'Reported custody/residence is not ultimate beneficial ownership. Regions, countries, issuer sectors and official/private groups overlap; do not sum them together. Historical combined areas remain separate.',
        'vintage_note':'Complete currently distributed histories and original bytes. Current downloads do not establish information available at historical trading dates.'}
    return out,shards
