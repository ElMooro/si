"""A dated cross-asset evidence composition, with no inferred investor transfers."""
from collections import defaultdict
from datetime import date, datetime, timezone
from decimal import Decimal, localcontext
import copy, hashlib, json, re

CONTRACT='cross-asset-flow-research.v1'
PREFIX='data/flow-state-research/'
PRIVATE='audit-private/20260909-originals/flow-state-research/'
CURRENT='data/cross-asset-flow-state.json'
FLAGS=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')
PERMISSIONS={key:False for key in FLAGS}
PARENTS={
    'data/risk-regime.json':('risk-regime-research.v1','risk-regime-research','risk-regime-replay.v1'),
    'data/capital-inflows.json':('tic-original-research.v1','tic-research','tic-original-replay.v1'),
    'data/etf-true-flows.json':('etf-original-research.v1','etf-research','etf-original-replay.v1'),
    'data/dollar-radar.json':('dollar-original-research.v1','dollar-research','dollar-original-replay.v1'),
    'data/fx-quote-research.json':('fx-original-quote-research.v1','fx-quote-research','fx-original-replay.v1'),
}
CONTEXT=('data/gold-equity-rotation.json','data/dark-pool.json','data/polygon-fx-regime.json')
CAPTURE_KEYS=(CURRENT,*PARENTS,*CONTEXT)

def sha(raw):return hashlib.sha256(raw).hexdigest()
def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def strict(raw):
    def pairs(items):
        out={}
        for k,v in items:
            if k in out:raise ValueError('Duplicate JSON key')
            out[k]=v
        return out
    def invalid(value):raise ValueError('Nonfinite JSON value')
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid)
def clock(value):
    if not isinstance(value,str):raise ValueError('Explicit compilation clock required')
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('Timezone required')
    return result.astimezone(timezone.utc)
def dec(value):
    if not isinstance(value,str) or not re.fullmatch(r'-?\d+(?:\.\d+)?',value) or len(value)>80:raise ValueError('Finite source decimal string required')
    return Decimal(value)
def ds(value):return format(value,'f')
def digest(value):return sha(encoded(value))
def pointer(*parts):return '/'+('/'.join(str(p).replace('~','~0').replace('/','~1') for p in parts))
def original(ref,read):
    if (not isinstance(ref,dict) or not isinstance(ref.get('sha256'),str)
        or not re.fullmatch('[a-f0-9]{64}',ref['sha256']) or ref.get('key')!=PRIVATE+ref['sha256']+'.bin'):
        raise ValueError('Protected complete capture required')
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('Captured bytes differ')
    return raw
def dated(value,at):
    if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):raise ValueError('Explicit observation date required')
    d=date.fromisoformat(value)
    if d>clock(at).date():raise ValueError('Future observation cannot be promoted')
    return value

def bind_parent(key,packet,read,at):
    contract,namespace,run_contract=PARENTS[key];prefix='data/'+namespace+'/'
    if packet.get('contract')!=contract or any(packet.get(k) is not False for k in FLAGS[:3]):raise ValueError('Reviewed descriptive parent required')
    if packet.get('forecast_qualified') is True:raise ValueError('Composition cannot inherit forecast authority')
    if clock(packet['generated_at'])>clock(at):raise ValueError('Parent compiled after composition')
    identity=packet['replay'];run_key=identity['manifest_key']
    if not re.fullmatch(re.escape(prefix)+r'runs/[a-f0-9]{64}\.json',run_key):raise ValueError('Unreviewed parent run')
    raw=read(run_key);run=strict(raw)
    if run_key!=prefix+'runs/'+sha(raw)+'.json' or run.get('contract')!=run_contract or run.get('output_sha256')!=identity['output_sha256']:raise ValueError('Parent run differs')
    def artifact(ref,kind,ext):
        if not re.fullmatch('[a-f0-9]{64}',ref['sha256']) or ref['key']!=prefix+kind+'/'+ref['sha256']+'.'+ext:raise ValueError('Unreviewed parent artifact')
        body=read(ref['key'])
        if sha(body)!=ref['sha256'] or ('bytes' in ref and len(body)!=ref['bytes']):raise ValueError('Parent artifact differs')
        return body
    if not isinstance(strict(artifact(run['input'],'inputs','json')),dict):raise ValueError('Parent inputs required')
    output=artifact(run['output'],'outputs','json')
    if sha(output)!=identity['output_sha256'] or strict(output)!={k:v for k,v in packet.items() if k!='replay'}:raise ValueError('Published parent differs')
    if run['generated_at']!=packet['generated_at']:raise ValueError('Parent clock differs')
    if not isinstance(run['compilers'],dict) or not 0<len(run['compilers'])<=16:raise ValueError('Parent compilers required')
    for ref in run['compilers'].values():artifact(ref,'compilers','py')
    return {'contract':contract,'public_key':key,'replay':identity,'output':run['output'],
        'generated_at':packet['generated_at'],'publication_age_hours':round((clock(at)-clock(packet['generated_at'])).total_seconds()/3600,6),
        'publication_age_is_observation_freshness':False,'quality':copy.deepcopy(packet.get('quality')),
        'source_clocks':copy.deepcopy(packet.get('source_clocks')),'source_generated_at':packet.get('source_generated_at'),
        'run_input_output_compiler_hashes_match':True,'source_original_replay_performed_by_composition':False,**PERMISSIONS}

def ref(parent,parts):return {'parent':parent,'pointer':pointer(*parts)}
def categories(packet,at):
    result=[];memberships=defaultdict(list);seen=set();funds=packet['by_etf']
    for i,row in enumerate(packet['category_rotation']):
        cat=row['category']
        if not isinstance(cat,str) or cat in seen:raise ValueError('Distinct category identity required')
        seen.add(cat);configured=row['configured_members'];covered=row['covered_members'];unavailable=row['unavailable_members']
        for values in (configured,covered,unavailable):
            if not isinstance(values,list) or any(not isinstance(t,str) for t in values) or len(set(values))!=len(values):raise ValueError('Distinct fund membership required')
        if set(covered)&set(unavailable) or set(covered)|set(unavailable)!=set(configured) or row['n_etfs']!=len(covered):raise ValueError('Category coverage reconciliation differs')
        selected=[]
        for ticker in covered:
            fund=funds[ticker];window=fund['flow_windows']['5d']
            if fund.get('ticker')!=ticker or window.get('status')!='complete_descriptive_estimate':raise ValueError('Reviewed fund estimate required')
            start=dated(window['start_date'],at);end=dated(window['end_date'],at)
            if start>=end or row['period']!={'start_date':start,'end_date':end}:raise ValueError('Mixed category periods')
            if window['observations_available']!=5 or window['observations_required']!=5:raise ValueError('Five reported observations required')
            selected.append(dec(window['value_decimal']));memberships[ticker].append(cat)
        with localcontext() as ctx:
            ctx.prec=80
            value=sum(selected,Decimal(0)) if selected else None
        if value is None:
            if row['value_decimal'] is not None or row['net_flow_5d_usd'] is not None or row['period'] is not None:raise ValueError('Missing category must stay missing')
        elif value!=dec(row['value_decimal']) or float(value)!=row['net_flow_5d_usd']:raise ValueError('Category subtotal differs from fund estimates')
        direction='unavailable' if value is None else 'net_issuance_estimate' if value>0 else 'net_redemption_estimate' if value<0 else 'unchanged_estimate'
        result.append({'asset_class':cat,'category':cat,'net_flow_5d_usd':None if value is None else float(value),
            'value_decimal':None if value is None else ds(value),'unit':'usd','measurement':'issuer_nav_valued_share_changes',
            'direction':direction,'period':copy.deepcopy(row['period']),'configured_members':configured,'covered_members':covered,
            'unavailable_members':unavailable,'covered_count':len(covered),'configured_count':len(configured),
            'whole_market_total':False,'observed_cash_transfers':False,'source':ref('data/etf-true-flows.json',('category_rotation',i)),
            'fund_sources':[ref('data/etf-true-flows.json',('by_etf',ticker,'flow_windows','5d')) for ticker in covered],**PERMISSIONS})
    return result,[{'ticker':t,'categories':cats,'additive_across_categories':False} for t,cats in sorted(memberships.items()) if len(cats)>1]

def transactions(packet,at):
    rows=[]
    for asset,item in sorted(packet['by_asset_class'].items()):
        if item['unit']!='usd_bn' or not isinstance(item['series_id'],str):raise ValueError('TIC identity/unit differs')
        observed=dated(item['data_asof'],at)
        for period,field in (('latest','latest_month_b'),('rolling_12mo','rolling_12mo_b')):
            window=item[period];value=window['usd_bn_decimal']
            if value is not None:
                exact=dec(value)
                if float(exact)!=item[field] or dec(window['usd_million_decimal'])/Decimal(1000)!=exact:raise ValueError('TIC unit reconciliation differs')
                months=window['months']
                if window['status']!='complete' or window['missing_months'] or len(months)!=(1 if period=='latest' else 12):raise ValueError('Complete TIC period required')
                dates=[date.fromisoformat(dated(m,at)) for m in months]
                ordinals=[d.year*12+d.month for d in dates]
                if any(d.day!=1 for d in dates) or ordinals!=list(range(ordinals[0],ordinals[0]+len(dates))) or months[-1]!=observed:raise ValueError('TIC monthly coverage differs')
            elif item[field] is not None:raise ValueError('Missing transaction must stay missing')
            rows.append({'asset_class':asset,'series_id':item['series_id'],'measurement':'cross_border_securities_transactions',
                'unit':'usd_bn','value_decimal':value,'period':period,'observation_date':observed,'months':copy.deepcopy(window['months']),
                'missing_months':copy.deepcopy(window['missing_months']),'status':window['status'],
                'observation_age_days':(clock(at).date()-date.fromisoformat(observed)).days,
                'month_label_is_release_date':False,'price_or_valuation_change':False,
                'source':ref('data/capital-inflows.json',('by_asset_class',asset,period)),**PERMISSIONS})
    return rows

def dependencies(docs):
    roots=defaultdict(set)
    for key in ('data/risk-regime.json','data/etf-true-flows.json'):
        groups=docs[key]['dependency_roots'];groups=groups.values() if isinstance(groups,dict) else [groups]
        for group in groups:
            for root in group:roots[root.lower()].add(key)
    for sid in docs['data/dollar-radar.json']['series']:roots['fred:'+sid.lower()].add('data/dollar-radar.json')
    roots['us-treasury:tic-transactions'].add('data/capital-inflows.json')
    return {'roots':[{'source_identity':root,'parents':sorted(parents)} for root,parents in sorted(roots.items())],
        'shared_roots':[{'source_identity':root,'parents':sorted(parents),'independent_confirmation':False} for root,parents in sorted(roots.items()) if len(parents)>1],
        'dollar_source_families':copy.deepcopy(docs['data/dollar-radar.json']['dependency_graph']),
        'fx_quote_dependency_graph':copy.deepcopy(docs['data/fx-quote-research.json'].get('dependency_graph')),
        'complete_component_lineage_verified':False,'independent_investment_votes':0,
        'note':'Shared source identities and issuer families are not independent confirmations. Different source windows remain distinct; this is not a correlation or causal graph.'}

def compile_output(inputs,read):
    if inputs.get('contract')!='flow-state-inputs.v1' or set(inputs.get('captures',{}))!=set(CAPTURE_KEYS):raise ValueError('Complete reviewed input set required')
    at=inputs['generated_at'];clock(at);docs={}
    for key,item in inputs['captures'].items():
        if item['source_key']!=key or clock(item['acquired_at'])>clock(at):raise ValueError('Capture identity/clock differs')
        doc=strict(original(item['original'],read))
        if not isinstance(doc,dict):raise ValueError('Whole research object required')
        docs[key]=doc
    predecessor=docs[CURRENT]
    if not (predecessor.get('contract')==CONTRACT or (predecessor.get('engine')=='cross-asset-flow-state' and predecessor.get('version')=='1.0')):raise ValueError('Unreviewed predecessor')
    parents={key:bind_parent(key,docs[key],read,at) for key in PARENTS}
    estimates,overlap=categories(docs['data/etf-true-flows.json'],at);tic=docs['data/capital-inflows.json'];monthly=transactions(tic,at)
    context=[{'source_key':key,'original':inputs['captures'][key]['original'],'generated_at':docs[key].get('generated_at'),
        'as_of':docs[key].get('as_of'),'status':'retained_unqualified_context','measurement_promoted':False,**PERMISSIONS} for key in CONTEXT]
    empty_metals={key:None for key in ('gold_20d_pct','silver_20d_pct','gold_miners_20d_pct','long_bonds_20d_pct','dollar_20d_pct','spy_20d_pct','metals_state')}
    return {'contract':CONTRACT,'engine':'cross-asset-flow-state','version':'2.0.0','generated_at':at,
        'headline':'Dated ETF issuance estimates and monthly TIC transactions; no qualified allocation signal.',
        'parents':parents,'asset_class_rotation':estimates,'monthly_transactions':monthly,
        'category_overlap':overlap,'category_total':None,'category_total_reason':'Categories may overlap; issuer coverage is incomplete. No aggregate market-flow total is inferred.',
        'foreign_flows':{'regime':'MONITOR_ONLY','source_regime':tic.get('regime'),'by_asset_class':copy.deepcopy(tic['by_asset_class']),
            'holder_splits':copy.deepcopy(tic.get('holder_splits')),'observation_date':tic['data_asof'],'source_generated_at':tic['generated_at'],
            'quality':copy.deepcopy(tic['quality']),'replay':tic['replay'],**PERMISSIONS},
        'dollar_fx_carry':{'dollar_regime':None,'fx_roro_score':None,'fx_roro_regime':None,'em_basket_5d_pct':None,
            'gold_silver_ratio_chg_5d':None,'carry_drivers':[],'dollar_research':parents['data/dollar-radar.json'],'fx_research':parents['data/fx-quote-research.json']},
        'hard_assets_and_dollar':empty_metals,'dark_pool':{'accumulation_n':None,'distribution_n':None,'top_accumulation':[],'top_distribution':[]},
        'retained_contexts':context,'retained_predecessor':inputs['captures'][CURRENT]['original'],
        'dependency_graph':dependencies(docs),'sources':list(PARENTS)+list(CONTEXT),
        'quality':{'status':'dated_descriptive_composition','native_parents_bound':len(parents),'unqualified_context_packets':len(context),
            'freshness_inherited_from_compilation_time':False,'source_original_replay_performed_by_composition':False,
            'note':'Parent artifacts are hash-bound. Issuer estimates, transactions, prices and proxy labels are separate measurement families; composing them creates no new validation.'},
        'decision':{'status':'abstain','reason':'No out-of-sample net-of-cost qualification or portfolio mandate is bound to this composition.'},
        'call':None,'score':None,'regime':None,'risk_regime_score':None,'risk_regime':None,'posture':{},
        'independent_investment_votes':0,'portfolio_action':None,**PERMISSIONS}
