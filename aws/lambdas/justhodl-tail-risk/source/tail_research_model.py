"""Bounded option snapshot research; vendor IV is not a recovered crash density."""
from collections import Counter
from datetime import date,datetime,timedelta,timezone
import hashlib,json,math,re
from urllib.parse import parse_qsl,urlencode,urlsplit,urlunsplit

CONTRACT='tail-native-research.v1';PREFIX='data/tail-research/';CURRENT='data/tail-risk.json'
PRIVATE='audit-private/20260909-originals/tail-research/'
SYMBOLS=('SPY','QQQ','IWM');MAX_PAGES=12;MAX_PAGE_BYTES=4*1024*1024;MAX_TOTAL_BYTES=32*1024*1024
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
SLOTS={'put10':('put',-.10,.02),'put25':('put',-.25,.03),'put50':('put',-.50,.05),'call25':('call',.25,.03),'call10':('call',.10,.02)}

def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def clock(value):
    if not isinstance(value,str):raise ValueError('Aware clock required')
    d=datetime.fromisoformat(value.replace('Z','+00:00'))
    if d.tzinfo is None:raise ValueError('Aware clock required')
    return d.astimezone(timezone.utc)
def number(x):
    try:return float(x) if type(x) in (int,float) and math.isfinite(x) else None
    except (OverflowError,ValueError):return None
def object_(x):return x if isinstance(x,dict) else {}
def integer(x):return x if type(x) is int and 0<=x<=9007199254740991 else None
def decode(raw):
    def reject(_):raise ValueError('Nonfinite source JSON')
    def unique(pairs):
        d={}
        for k,v in pairs:
            if k in d:raise ValueError('Duplicate source JSON key')
            d[k]=v
        return d
    return json.loads(raw,parse_constant=reject,object_pairs_hook=unique)

def initial_url(symbol,started):
    if symbol not in SYMBOLS:raise ValueError('Unreviewed underlying')
    day=clock(started).date()
    return 'https://api.polygon.io/v3/snapshot/options/'+symbol+'?'+urlencode({'expiration_date.gte':str(day+timedelta(days=25)),
        'expiration_date.lte':str(day+timedelta(days=75)),'sort':'ticker','order':'asc','limit':250})

def next_url(value,symbol):
    if not isinstance(value,str) or len(value)>12000:raise ValueError('Invalid pagination address')
    p=urlsplit(value)
    if (p.scheme!='https' or p.netloc!='api.polygon.io' or p.path!='/v3/snapshot/options/'+symbol or p.fragment
        or p.username or p.password):raise ValueError('Pagination must retain the reviewed provider and underlying')
    pairs=parse_qsl(p.query,keep_blank_values=True);seen=set();clean=[]
    for key,v in pairs:
        if key in seen or len(v)>10000:raise ValueError('Ambiguous pagination query')
        seen.add(key)
        if key=='apiKey':continue
        if key not in ('cursor','limit','sort','order','expiration_date.gte','expiration_date.lte'):raise ValueError('Unreviewed pagination query')
        clean.append((key,v))
    if not clean:raise ValueError('Pagination query required')
    return urlunsplit(('https','api.polygon.io',p.path,urlencode(clean),''))

def timestamp(value,received):
    """Provider nanoseconds are integers; preserve exact original digits separately."""
    if type(value) is not int or not 946684800000000000<=value<=4102444800000000000:return None
    d=datetime.fromtimestamp(value//1000000000,timezone.utc)+timedelta(microseconds=(value%1000000000)//1000)
    return d if d<=clock(received) else None

def quote(row,received):
    source=object_(row.get('last_quote'));bid=number(source.get('bid'));ask=number(source.get('ask'))
    stamp=timestamp(source.get('last_updated'),received);reasons=[]
    if bid is None or ask is None or bid<0 or ask<0:reasons.append('missing_or_invalid_bid_ask')
    elif ask<bid:reasons.append('crossed_quote')
    if stamp is None:reasons.append('missing_or_invalid_quote_clock')
    elif (clock(received)-stamp).total_seconds()>96*3600:reasons.append('quote_older_than_96_hours')
    sizes=[integer(source.get(k)) for k in ('bid_size','ask_size')]
    if any(v is None or v<=0 for v in sizes):reasons.append('missing_or_zero_quote_size')
    ok=not reasons;mid=(bid+ask)/2 if ok else None
    return {'bid':bid,'ask':ask,'midpoint':mid,'quoted_spread':ask-bid if ok else None,'unit':'USD_per_share',
        'bid_size':sizes[0],'ask_size':sizes[1],'observed_at':stamp.isoformat() if stamp else None,
        'timestamp_ns':str(source['last_updated']) if stamp else None,'timeframe':source.get('timeframe') if source.get('timeframe') in ('REAL-TIME','DELAYED') else None,
        'quality':{'status':'within_age_ceiling' if ok else 'unqualified','reasons':reasons,'quote_age_ceiling_hours':96,
            'exchange_session_verified':False,'executable_quote':False}}

def contract(row,symbol,started,page,row_index):
    if not isinstance(row,dict):return None,'non_object_contract'
    d=object_(row.get('details'));ticker=d.get('ticker');kind=d.get('contract_type');strike=number(d.get('strike_price'))
    m=re.fullmatch(r'O:'+symbol+r'(\d{6})([CP])(\d{8})',ticker if isinstance(ticker,str) else '')
    try:expiry=date.fromisoformat(d.get('expiration_date',''));symbol_expiry=date(2000+int(m[1][:2]),int(m[1][2:4]),int(m[1][4:])) if m else None
    except (ValueError,TypeError):return None,'invalid_contract_identity'
    first=clock(started).date()+timedelta(days=25);last=clock(started).date()+timedelta(days=75)
    if (not m or expiry!=symbol_expiry or kind!=('call' if m[2]=='C' else 'put') or strike is None or strike<=0
        or not math.isclose(strike,int(m[3])/1000,rel_tol=0,abs_tol=1e-8) or not first<=expiry<=last):return None,'contract_identity_or_requested_expiry_differs'
    if d.get('exercise_style')!='american' or type(d.get('shares_per_contract')) not in (int,float) or d['shares_per_contract']!=100 or d.get('additional_underlyings'):
        return None,'unreviewed_exercise_or_deliverable'
    u=object_(row.get('underlying_asset'))
    if u.get('ticker')!=symbol:return None,'underlying_identity_differs'
    received=page['acquired_at'];ustamp=timestamp(u.get('last_updated'),received);price=number(u.get('price'))
    if price is not None and price<=0:price=None
    underlying={'symbol':symbol,'value':price,'unit':'USD_per_share','observed_at':ustamp.isoformat() if ustamp else None,
        'timestamp_ns':str(u['last_updated']) if ustamp else None,'timeframe':u.get('timeframe') if u.get('timeframe') in ('REAL-TIME','DELAYED') else None}
    iv=number(row.get('implied_volatility'));iv=iv if iv is not None and iv>0 else None
    g=object_(row.get('greeks'));delta=number(g.get('delta'))
    if delta is not None and not (-1<=delta<=0 if kind=='put' else 0<=delta<=1):delta=None
    q=quote(row,received);qstamp=clock(q['observed_at']) if q['observed_at'] else None
    aligned=bool(q['quality']['status']=='within_age_ceiling' and ustamp and price and abs((qstamp-ustamp).total_seconds())<=60)
    return {'contract_id':ticker,'underlying':symbol,'contract_type':kind,'exercise_style':'american','shares_per_contract':100,
        'strike':strike,'strike_unit':'USD_per_share','expiration_date':str(expiry),'calendar_days_to_expiry':(expiry-clock(started).date()).days,
        'provider_implied_volatility':iv,'provider_iv_unit':'decimal_volatility','provider_iv_observed_at':None,
        'provider_delta':delta,'provider_delta_unit':'option_price_change_per_underlying_price_change','provider_greek_observed_at':None,
        'quote':q,'underlying_mark':underlying,'quote_and_underlying_within_60s':aligned,
        'strike_to_underlying_ratio':strike/price if aligned else None,'open_interest':integer(row.get('open_interest')),
        'open_interest_observed_at':None,'received_at':received,
        'evidence':{'original':page['original'],'page':page['page'],'row_index':row_index},**PERMISSIONS},None

def term(contracts,target):
    expiries=sorted({(r['calendar_days_to_expiry'],r['expiration_date']) for r in contracts})
    if not expiries:return {'target_calendar_days':target,'expiration_date':None,'calendar_days_to_expiry':None,'selections':{},'comparisons':{},'available':False}
    days,expiry=min(expiries,key=lambda x:(abs(x[0]-target),x[0]));rows=[r for r in contracts if r['expiration_date']==expiry];selections={}
    for label,(kind,wanted,tolerance) in SLOTS.items():
        candidates=[r for r in rows if r['contract_type']==kind and r['provider_delta'] is not None and r['provider_implied_volatility'] is not None and abs(r['provider_delta']-wanted)<=tolerance]
        chosen=min(candidates,key=lambda r:(abs(r['provider_delta']-wanted),r['contract_id'])) if candidates else None
        selections[label]={'target_delta':wanted,'maximum_delta_error':tolerance,'actual_delta_error':abs(chosen['provider_delta']-wanted) if chosen else None,'contract':chosen}
    comparisons={}
    for name,a,b in (('call25_minus_put25','call25','put25'),('call10_minus_put10','call10','put10'),('put10_minus_put50','put10','put50')):
        left=selections[a]['contract'];right=selections[b]['contract'];value=100*(left['provider_implied_volatility']-right['provider_implied_volatility']) if left and right else None
        comparisons[name]={'value':round(value,8) if value is not None else None,'unit':'volatility_percentage_points',
            'left_contract':left['contract_id'] if left else None,'right_contract':right['contract_id'] if right else None,
            'observed_at':None,'synchronized_market_observation':False,'meaning':'Difference of vendor model IV values in the captured sample; no independent IV clock or model specification.'}
    return {'target_calendar_days':target,'expiration_date':expiry,'calendar_days_to_expiry':days,'captured_expiry_contracts':len(rows),
        'selection_basis':'Closest listed expiry in the captured sample, then nearest reported delta inside an explicit tolerance; no interpolation or extrapolation.',
        'selections':selections,'comparisons':comparisons,'available':any(s['contract'] for s in selections.values())}

def compute(chains,started,generated,contexts):
    if set(chains)!=set(SYMBOLS):raise ValueError('Exact underlying inventory required')
    start=clock(started);end=clock(generated)
    if not 0<=(end-start).total_seconds()<=150:raise ValueError('Collection clock outside reviewed bound')
    indices=[];total_rows=total_eligible=0;lineage=[]
    for symbol in SYMBOLS:
        chain=chains[symbol];records={};duplicates=set();excluded=Counter();received=0
        for p in chain['pages']:
            if p.get('raw') is None:continue
            lineage.append({'underlying':symbol,'page':p['page'],'received_at':p['acquired_at'],'original':p['original'],'http_status':p['http_status']})
            if p['http_status']!=200:continue
            doc=decode(p['raw']);rows=doc.get('results',[]) if isinstance(doc,dict) else None
            if not isinstance(doc,dict) or doc.get('status') not in ('OK','DELAYED') or not isinstance(rows,list):excluded['invalid_provider_envelope']+=1;continue
            for index,row in enumerate(rows):
                received+=1;r,why=contract(row,symbol,started,p,index)
                if why:excluded[why]+=1;continue
                identity=r['contract_id']
                if identity in records:duplicates.add(identity)
                else:records[identity]=r
        for identity in duplicates:records.pop(identity,None)
        if duplicates:excluded['duplicate_contracts_removed']=len(duplicates)
        rows=list(records.values());total_rows+=received;total_eligible+=len(rows)
        terms=[term(rows,n) for n in (30,60)];same=bool(terms[0]['expiration_date'] and terms[0]['expiration_date']==terms[1]['expiration_date'])
        a=terms[0]['comparisons'].get('call25_minus_put25',{}).get('value');b=terms[1]['comparisons'].get('call25_minus_put25',{}).get('value')
        indices.append({'ticker':symbol,'spot':None,'front_exp':terms[0]['expiration_date'],'back_exp':terms[1]['expiration_date'],
            **{k:None for k in ('atm_iv','put10_iv','put25_iv','call25_iv','put_skew_slope','risk_reversal_25','risk_reversal_10','rr_term_slope','p_drop_5','p_drop_10','p_drop_20','rn_skew','rn_kurt','skew_index','tail_stress','skew_slope_pctile','crash_prob_pctile')},
            'sample':{'returned_rows':received,'eligible_identity_rows':len(rows),'excluded_reasons':dict(sorted(excluded.items())),
                'pages_received':len([p for p in chain['pages'] if p['status']=='received']),'pagination_complete':chain['stop']=='complete','stop_reason':chain['stop'],
                'capture_is_atomic':False,'exchange_chain_completeness_verified':False,'expiries_in_sample':sorted({r['expiration_date'] for r in rows}),
                'with_vendor_iv':sum(r['provider_implied_volatility'] is not None for r in rows),
                'with_qualified_quote':sum(r['quote']['quality']['status']=='within_age_ceiling' for r in rows)},
            'terms':terms,'same_selected_expiry':same,'descriptive_term_difference':{'value':round(b-a,8) if a is not None and b is not None and not same else None,
                'unit':'volatility_percentage_points','observed_at':None,'synchronized_market_observation':False,'meaning':'Back minus front vendor call25-put25 IV differences; actual selected expiry dates apply.'},**PERMISSIONS})
    return {'contract':CONTRACT,'schema_version':'2.0.0','version':'2.0.0','engine':'justhodl-tail-risk','generated_at':generated,'as_of':None,
        'indices':indices,'system_tail_gauge':None,'tail_regime':None,'tail_valuation':None,'score':None,'regime':None,'call':None,'portfolio_action':'WAIT',**PERMISSIONS,
        'quality':{'status':'partial','underlyings':3,'received_contract_rows':total_rows,'eligible_identity_rows':total_eligible,
            'complete_paginations':sum(c['stop']=='complete' for c in chains.values()),'vendor_iv_observation_time_verified':False,'independent_investment_votes':0},
        'freshness':{'collection_started_at':started,'collection_completed_at':generated,'sample_valid_until':(start+timedelta(hours=26)).isoformat(),
            'pipeline_check_due_at':(start+timedelta(hours=26)).isoformat(),'meaning':'Capture age only; individual quote clocks remain separate. No IV or Greek observation clock supplied.'},
        'lineage':{'sources':lineage,'provider':'polygon','independent_investment_votes':0},'preserved_context':contexts,
        'methodology':{'expiry_range_calendar_days':[25,75],'page_limit_per_underlying':MAX_PAGES,'selected_targets_calendar_days':[30,60],
            'density_qualified':False,'forecast_qualified':False,'vendor_model_specification_verified':False,
            'limits':['American ETF options require exercise/dividend treatment before a European risk-neutral-density method can apply.',
                'No extrapolated strikes, clipped densities, invented risk-free rate, normalized tail mass or physical crash-probability claim.',
                'Vendor IV and Greeks have no independent observation timestamp. Their captured differences are descriptive model outputs.',
                'Quote age ceilings and size checks do not establish executable prices, synchronized surfaces or complete sessions.',
                'Pagination can change during capture; duplicates are removed and a bounded incomplete chain is labeled incomplete.',
                'Protection cheap/expensive requires validated risk, premium and payoff assumptions; historical rank alone is insufficient.']},
        'portfolio_consequences':{'available':False,'reason':'No verified portfolio exposure, option position, fill, scenario probability or hedge recommendation.'}}
