"""Dated cross-engine research synthesis, with zero forecasting authority.

Replay verifies retained upstream publications, not a second execution of their
provider parsers. Upstream run references remain the route to original evidence.
"""
from datetime import date,datetime,time,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import hashlib,json,math,re
import credit_research,volatility_research,eurodollar_research,aaii_research,insider_research,breadth_series,vrp_research

CONTRACT='extremes-native-research.v1'
PREFIX='data/extremes-research/'
PRIVATE='audit-private/20260909-originals/extremes-research/'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
SOURCES={
 'crisis':('data/crisis-composite.json','crisis-research.v1','crisis-research'),
 'breadth':('data/market-internals.json','breadth-native-research.v1','breadth-research'),
 'credit':('data/credit-stress.json',credit_research.CONTRACT,'credit-research'),
 'volatility':('data/vol-surface.json',volatility_research.CONTRACT,'volatility-research'),
 'funding':('data/eurodollar-stress.json',eurodollar_research.CONTRACT,'eurodollar-research'),
 'insider':('data/insider-aggregate.json',insider_research.CONTRACT,'insider-research'),
 'aaii':('data/aaii-sentiment.json','aaii-native-research.v1','aaii-research'),
 'fails':('data/settlement-fails.json','fr2004-fails-research.v1','fails-research'),
 'capitulation':('data/capitulation.json',CONTRACT,'extremes-research'),
 'valuation':('valuations-data.json',None,None),
 'retail':('data/retail-sentiment.json',None,None),
 'vrp':('data/vrp.json',vrp_research.CONTRACT,'vrp-research')}
INPUTS={
 'capitulation':('crisis','breadth','credit','volatility','funding','insider','fails'),
 'market-extremes':('valuation','breadth','aaii','credit','insider','retail','vrp','capitulation','fails')}
COMPANIONS=(credit_research,volatility_research,eurodollar_research,aaii_research,insider_research,breadth_series,vrp_research)

def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def clock(s):
    d=datetime.fromisoformat(s.replace('Z','+00:00'))
    if d.tzinfo is None:raise ValueError('Aware research clock required')
    return d.astimezone(timezone.utc)
def number(v):
    if type(v) not in (int,float) or not math.isfinite(v):raise ValueError('Finite measurement required')
    return v
def decimal(v):
    d=Decimal(str(v))
    if not d.is_finite():raise ValueError('Finite decimal required')
    return d
def root_id(sid):
    if not isinstance(sid,str) or not re.fullmatch('[A-Z][A-Z0-9_]{0,70}',sid):raise ValueError('Reviewed series identity required')
    return 'FRED:'+sid
def deadline(engine,at):
    if engine=='capitulation':
        # Existing rule: minute 45 every three UTC hours, plus 30-minute allowance.
        due=at.replace(hour=0,minute=45,second=0,microsecond=0)
        while due<=at:due+=timedelta(hours=3)
        return due+timedelta(minutes=30)
    if engine=='market-extremes':
        due=at.replace(hour=23,minute=0,second=0,microsecond=0)
        if due<=at:due+=timedelta(days=1)
        return due+timedelta(hours=2)
    raise ValueError('Reviewed engine required')

def identity(packet,name):
    """Body identity, type and authority checks; store also checks run/output bytes."""
    if not isinstance(packet,dict) or packet.get('contract')!=SOURCES[name][1]:return False
    if any(packet.get(k) is not False for k in ('calls_eligible','sizing_eligible','execution_eligible')):return False
    if any(packet.get(k,False) is not False for k in ('forecast_qualified','forecast_eligible')) or packet.get('call') is not None:return False
    ref=packet.get('replay',{});prefix='data/'+str(SOURCES[name][2])+'/'
    return bool(re.fullmatch(re.escape(prefix)+r'runs/[a-f0-9]{64}\.json',str(ref.get('manifest_key','')))
        and ref.get('output_sha256')==sha(encoded({k:v for k,v in packet.items() if k!='replay'})))

def observation(name,sid,label,value,unit,day,valid_until,packet,path,extra=None):
    number(value);date.fromisoformat(day);clock(valid_until)
    return {'source_engine':name,'series_id':sid,'label':label,'value':value,'unit':unit,'observation_date':day,
        'valid_until':valid_until,'source_generated_at':packet['generated_at'],'source_path':path,
        'upstream_replay':packet['replay'],**(extra or {}),**PERMISSIONS}

def project(name,p,at):
    """Keep dated measurements distinct; no default scores or synthetic votes."""
    rows=[];summary={};generated=clock(p['generated_at'])
    if generated>at:raise ValueError('Future upstream publication')
    if name in ('credit','volatility','funding','vrp'):
        adapter={'credit':credit_research,'volatility':volatility_research,'funding':eurodollar_research,'vrp':vrp_research}[name]
        ctx=adapter.context(p,at)
        if not ctx['available']:return [],{},'upstream_current_context_unavailable'
        for key,m in ctx['measurements'].items():
            raw=p['measurements'][key];sid=m['series_id'];day=m['observation_date']
            valid=min(clock(p['freshness']['pipeline_check_due_at']),clock(raw['source_valid_until']))
            if name=='credit':value=m['value_pct'];unit='Percent';label=m['definition']
            else:value=m['value'];unit=raw['source_unit'];label=raw.get('definition',m.get('label',sid))
            if isinstance(label,dict):label=label.get('title') or sid
            extra={'provider_family':'ICE_BofA' if sid.startswith('BAML') else 'Cboe' if name=='volatility' or sid.startswith('VIX') or sid=='VXVCLS' else 'S&P_Dow_Jones_Indices' if sid=='SP500' else 'Federal_Reserve_or_Treasury',
                'descriptive_statistics':raw.get('descriptive_statistics'),'source_url':m['source_url']}
            identity_id='CBOE:'+sid if name=='volatility' and raw['provider']=='Cboe' else root_id(sid)
            rows.append(observation(name,identity_id,label,value,unit,day,valid.isoformat(),p,'measurements.'+key,extra))
        summary={'interpretation':ctx.get('interpretation',ctx.get('note'))}
    elif name=='crisis':
        if (at-generated).total_seconds()>26*3600:return [],{},'upstream_publication_stale'
        for sid,m in p['measurements'].items():
            if m.get('quality',{}).get('status')!='fresh':continue
            if m.get('series_id')!=sid or m.get('unit')!=m.get('definition',{}).get('units'):raise ValueError('Crisis source unit differs')
            if m.get('calls_eligible') is not False or m.get('sizing_eligible') is not False:raise ValueError('Unexpected Crisis authority')
            observed=date.fromisoformat(m['observation_date']);freq=m['frequency'];age={'D':10,'W':21,'BW':35,'M':100,'Q':200,'SA':370,'A':550}[freq]
            acquired=clock(m['acquired_at']);source=clock(m['source_generated_at'])
            if not acquired<=source<=generated:raise ValueError('Crisis source clocks differ')
            valid=min(source+timedelta(hours=26),acquired+timedelta(hours=26),datetime.combine(observed+timedelta(days=age+1),time.min,timezone.utc))
            if not observed<=at.date() or at>=valid:continue
            value=number(m['value'])
            if float(decimal(m['value_decimal']))!=value:raise ValueError('Crisis exact value differs')
            rows.append(observation(name,root_id(sid),m['label'],value,m['unit'],str(observed),valid.isoformat(),p,'measurements.'+sid,
                {'source_url':'https://fred.stlouisfed.org/series/'+sid,'provider_family':'ICE_BofA' if sid.startswith('BAML') else 'Cboe' if sid.startswith('VIX') or sid=='VXVCLS' else 'Macro_FRED_distributed'}))
        summary={'ciss_context':'CISS remains available in the linked upstream research; it is not an extra score here.'}
    elif name=='breadth':
        if p.get('quality',{}).get('status')!='fresh':return [],{},'breadth_session_quality_unavailable'
        valid=min(generated+timedelta(hours=96),datetime.combine(date.fromisoformat(p['as_of'])+timedelta(days=6),time.min,timezone.utc))
        units={'ADVANCERS':'provider_symbol_count','DECLINERS':'provider_symbol_count','UNCHANGED':'provider_symbol_count','TRIN':'ratio',
            'PCT_ABOVE_50DMA':'percent','PCT_ABOVE_200DMA':'percent','NEW_HIGHS':'provider_symbol_count','NEW_LOWS':'provider_symbol_count'}
        for key,unit in units.items():
            series=breadth_series.native_suffix(p,key,p['as_of'],at);value=series.get(p['as_of'])
            if value is None:continue
            if p['field_units'][key]!=unit or p['latest'][key]!=[p['as_of'],value]:raise ValueError('Breadth definition or latest value differs')
            rows.append(observation(name,'JH_BREADTH:'+key,key.replace('_',' '),value,unit,p['as_of'],valid.isoformat(),p,'latest.'+key,
                {'provider_family':'FMP_US_symbol_sample','definition':p['definitions'].get(key,'Matched current and prior provider symbol closes.'),'source_url':'/market-internals.html'}))
        summary={'population':p['coverage'].get(p['as_of']),'universe':p['universe'],'interpretation':'Provider symbol population; current filters and survivorship limitations apply.'}
    elif name=='aaii':
        ctx=aaii_research.context(p,at)
        if not ctx['available']:return [],{},'upstream_current_survey_unavailable'
        for key,unit in (('bullish_pct','percent'),('neutral_pct','percent'),('bearish_pct','percent'),('bull_bear_spread_pp','percentage_points')):
            rows.append(observation(name,'AAII:'+key,key.replace('_',' '),ctx[key],unit,ctx['as_of'],p['quality']['freshness']['valid_until'],p,'observation.'+key,
                {'provider_family':'AAII_voluntary_member_survey','source_url':ctx['source']}))
        summary={'population':ctx['population']}
    elif name=='insider':
        ctx=insider_research.context(p,at)
        if not ctx['available']:return [],{},'upstream_current_filing_sample_unavailable'
        summary={k:ctx[k] for k in ('as_of','source_valid_until','coverage','note')}
        summary['transaction_windows']={k:{f:w[f] for f in ('from_date','through_date','buy_count','sell_count','buy_sell_ratio_count','population_complete','unknown_currency_rows')} for k,w in ctx['windows'].items()}
        summary['root']='FMP:SEC_filing_representations'
    elif name=='fails':
        q=p['quality'];acquired=clock(q['acquired_at'])
        valid=min(acquired+timedelta(hours=36),clock(q['next_expected_publication_date'])+timedelta(hours=24))
        if not acquired<=generated<=at<valid or q.get('status')!='fresh':return [],{},'upstream_weekly_fails_check_overdue'
        scopes={}
        for field,scope in (('headline','ust_ex_tips'),('treasury','treasury_incl_tips')):
            s=p[field];day=s['as_of']
            if s.get('scope_id')!=scope or s.get('unit')!='usd_bn' or s.get('complete') is not True:raise ValueError('Fails scope, unit or completeness differs')
            if not 0<=(at.date()-date.fromisoformat(day)).days<=21:continue
            if s.get('quality',{}).get('status')!='fresh':continue
            values={k:number(s[k]) for k in ('ftd_bn','ftr_bn','combined_bn')}
            if any(v<0 for v in values.values()) or any(s['field_units'].get(k)!='usd_bn' for k in values):raise ValueError('Fails field unit differs')
            if decimal(values['ftd_bn'])+decimal(values['ftr_bn'])!=decimal(values['combined_bn']):raise ValueError('Fails reconciliation differs')
            scopes[scope]={'as_of':day,'scope_id':scope,'unit':'usd_bn',**values,'valid_until':valid.isoformat(),**PERMISSIONS}
            for key,value in values.items():rows.append(observation(name,'FR2004:'+scope+':'+key,scope+' '+key,value,'usd_bn',day,valid.isoformat(),p,field+'.'+key,
                {'provider_family':'NYFed_FR2004C','source_url':'/fails.html','scope_id':scope}))
        summary={'scopes':scopes,'note':'Cumulative two-sided gross reported fails. These scopes overlap; never add them. Not defaults, losses, unique securities or capital flows.'}
    elif name=='capitulation':
        if p.get('engine')!='capitulation' or at>=clock(p['freshness']['pipeline_check_due_at']):return [],{},'nested_research_unavailable'
        summary={'nested_source_series':sorted(p['dependency_graph']['series']),'upstream_replay':p['replay'],
            'note':'Lineage reference only. Its constituent observations are not added again as independent confirmations.'}
    if name not in ('insider','capitulation') and not rows:return [],{},'no_current_native_measurements'
    return rows,summary,'retained_upstream_context'

def graph(rows,contexts):
    series={};families={};groups={};definitions={}
    for row in rows:
        sid=row['series_id'];series.setdefault(sid,set()).add(row['source_engine'])
        families.setdefault(row['provider_family'],set()).add(sid)
        group=(sid,row['observation_date'],row['unit']);groups.setdefault(group,[]).append(row)
        definitions.setdefault((sid,row['observation_date']),set()).add(row['unit'])
    conflicts=[];overlap=[]
    for (sid,day,unit),same in sorted(groups.items()):
        if len(same)<2:continue
        item={'series_id':sid,'observation_date':day,'unit':unit,'sources':sorted({r['source_engine'] for r in same})}
        vals={decimal(r['value']) for r in same}
        if len(vals)>1:conflicts.append({**item,'values':[{'source':r['source_engine'],'value':r['value']} for r in same]})
        else:overlap.append(item)
    if contexts.get('insider',{}).get('root'):
        series[contexts['insider']['root']]={'insider'};families.setdefault('FMP_SEC_filing_sample',set()).add(contexts['insider']['root'])
    return {'series':{k:sorted(v) for k,v in sorted(series.items())},'provider_families':{k:sorted(v) for k,v in sorted(families.items())},
        'same_series_date_overlaps':overlap,'same_series_date_conflicts':conflicts,'independent_investment_votes':0,
        'definition_conflicts':[{'series_id':sid,'observation_date':day,'units':sorted(units)} for (sid,day),units in sorted(definitions.items()) if len(units)>1],
        'nested_research':contexts.get('capitulation',{}),'note':'Series identity is not statistical independence. Different dates are retained separately; conflicting same-date values receive no automatic winner.'}

def compute(engine,packets,evidence,generated_at):
    if engine not in INPUTS or set(packets)!=set(INPUTS[engine]) or set(evidence)!=set(packets):raise ValueError('Exact reviewed input inventory required')
    at=clock(generated_at);rows=[];contexts={};eligibility={}
    with localcontext() as ctx:
        ctx.prec=50;ctx.rounding=ROUND_HALF_EVEN
        for name in INPUTS[engine]:
            p=packets[name];retained=evidence[name];usable=False;reason='legacy_or_missing_source_contract';source_rows=[];summary={}
            if p is not None and identity(p,name) and retained.get('upstream_identity_verified') is True:
                try:source_rows,summary,reason=project(name,p,at);usable=bool(source_rows or summary)
                except (KeyError,ValueError,TypeError,ArithmeticError,OverflowError):reason='upstream_schema_or_clock_invalid'
            elif retained.get('status')!='retained':reason=retained.get('status','source_unavailable')
            elif SOURCES[name][1] and p and p.get('contract')==SOURCES[name][1]:reason='upstream_identity_or_authority_invalid'
            rows.extend(source_rows);contexts[name]=summary
            eligibility[name]={'source_key':SOURCES[name][0],'research_context_available':usable,'reason':reason,'measurement_count':len(source_rows),
                'generated_at':p.get('generated_at') if isinstance(p,dict) else None,'retained_packet':retained.get('packet'),
                'upstream_replay':p.get('replay') if isinstance(p,dict) and usable else None,**PERMISSIONS}
    rows.sort(key=lambda r:(r['series_id'],r['source_engine'],r['observation_date']))
    dependency=graph(rows,contexts);current=sum(v['research_context_available'] for v in eligibility.values())
    return {'contract':CONTRACT,'schema_version':'2.0','engine':engine,'generated_at':generated_at,'as_of':max((r['observation_date'] for r in rows),default=None),
        'freshness':{'pipeline_check_due_at':deadline(engine,at).isoformat(),'basis':'Existing engine schedule plus explicit allowance; every measurement retains its own upstream deadline.'},
        'quality':{'status':'partial' if current else 'unavailable','available_contexts':current,'expected_contexts':len(INPUTS[engine]),'basis':'Retained upstream output identity and dated typed context; not strategy validation.',
            'original_provider_replayed_here':False,'point_in_time_forecast_validated':False,'independent_investment_votes':0},
        'measurements':rows,'contexts':contexts,'eligibility':eligibility,'dependency_graph':dependency,'input_evidence':evidence,
        'call':None,'portfolio_action':'WAIT','decision':{'verb':'WAIT','meaning':'Abstain from a new recommendation; not a target position or an instruction to hold existing risk.','eligible_votes':0,'reason':'No independently validated top/bottom forecasting or allocation policy is registered.'},**PERMISSIONS,
        'capitulation_score':None,'signal':None,'action':None,'stabilising':None,'smart_money_confirm':None,'insider_regime':None,'crisis_trend':None,'washout_components':[],'shopping_list':[],
        'posture':None,'posture_color':None,'cycle_position':None,'scores':{'top_risk':None,'capitulation':None},'top_canaries':[],'top_canaries_firing':[],'bottom':{},
        'headline':'Dated research context; no qualified market-turn signal.','interpretation':'Overlapping observations and source gaps remain visible; missing evidence is never neutral or zero.',
        'pd_settlement_fails':contexts.get('fails',{}),'history_reference':{'key':'data/'+engine+'-history.json','status':'legacy_unverified','used_for_forecasting':False},
        'portfolio_consequences':{'method':'Entered signed exposure multiplied by an explicit price shock','formula':'signed_USD_exposure * entered_price_shock_pct / 100','model_implied_shock':None,'target_allocation':None,
            'limitations':['No engine-inferred return or shock probability.','No FX, dividends, financing, fees, convexity or hedge response.','Position-specific risk requires separately entered or verified holdings.']},
        'replay_scope':'Exact synthesis from retained upstream publication bytes and retained upstream run/output identities. Upstream provider originals are not re-parsed by this compiler.'}
