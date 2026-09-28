"""Per-issuer reported classifications with whole source and exact acquisition clocks."""
from decimal import Decimal
import json,re,zlib
from urllib.parse import quote
from context_evidence_store import clock,validate_ref,sha
CONTRACT='issuer-classification-observations.v1'
FLAGS={k:False for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','ranking_eligible')}
HEAD='data/momentum-themes.json'
PRIVATE='audit-private/20260909-originals/momentum-leaders-research/classification-context/'
INPUTS={'momentum':'data/momentum-leaders.json'}
CACHE_KEY='data/_cache/ticker-profiles.json'
ALL_INPUTS={**INPUTS,'legacy_profile_cache':CACHE_KEY}
MAX_UNIVERSE=30
MAX_SOURCE_BYTES=2*1024*1024
PROFILE_TTL_SECONDS=7*24*60*60


def endpoint(ticker):
    if symbol(ticker) is None:raise ValueError('Literal issuer required')
    return 'https://financialmodelingprep.com/stable/profile?symbol='+quote(ticker,safe='')


def strict(raw,encoding=''):
    if not isinstance(raw,bytes) or len(raw)>16*1024*1024:raise ValueError('Whole bounded source required')
    packed=raw.startswith(b'\x1f\x8b')
    if encoding not in ('','identity','gzip') or encoding=='gzip' and not packed or encoding=='identity' and packed:raise ValueError('Source encoding mismatch')
    if packed:
        decoder=zlib.decompressobj(16+zlib.MAX_WBITS);raw=decoder.decompress(raw,16*1024*1024+1)
        if len(raw)>16*1024*1024 or decoder.unconsumed_tail or decoder.unused_data or not decoder.eof:raise ValueError('Whole bounded gzip member required')
    def pairs(items):
        out={}
        for key,value in items:
            if key in out:raise ValueError('Duplicate JSON key')
            out[key]=value
        return out
    def decimal(value):
        n=Decimal(value)
        if not n.is_finite() or abs(n)>Decimal('1e30') or n and abs(n)<Decimal('1e-30'):raise ValueError('Numeric bound exceeded')
        return n
    def bad(_):raise ValueError('Nonfinite JSON number')
    return json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_float=decimal,parse_constant=bad)

def symbol(value):return value if isinstance(value,str) and re.fullmatch(r'[A-Z0-9][A-Z0-9.\-^]{0,24}',value) else None

def selection(doc,generated):
    parent_flags=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified',
                  'independent_evidence_eligible','private_state_read_or_written')
    if (not isinstance(doc,dict) or doc.get('measurement_contract')!='leader-price-observations.v1' or
        doc.get('status')!='RESEARCH_ONLY' or any(doc.get(k) is not False for k in parent_flags) or
        doc.get('ranking_eligible',False) is not False or doc.get('call') is not None):
        raise ValueError('Recognized unqualified Momentum observation population required')
    stamp=clock(doc.get('generated_at'))
    if stamp is None or stamp>generated:raise ValueError('Parent publication clock absent or future')
    members=doc.get('universe_membership',{}).get('selected') if isinstance(doc.get('universe_membership'),dict) else None
    records=doc.get('request_records')
    if not isinstance(members,list) or not isinstance(records,list) or len(members)!=len(records):raise ValueError('Complete parent population and request coordinates required')
    selected=[];occurrences=[];seen=set()
    for i,(member,record) in enumerate(zip(members,records)):
        if not isinstance(member,dict) or not isinstance(record,dict):raise ValueError('Invalid source population member')
        ticker=symbol(member.get('ticker'))
        if ticker is None or record.get('ticker')!=ticker or type(member.get('request_index')) is not int or member['request_index']!=i:
            raise ValueError('Literal issuer and exact parent request index required')
        if ticker in seen:raise ValueError('Ambiguous parent issuer population')
        seen.add(ticker);included=i<MAX_UNIVERSE
        occurrences.append({'ticker':ticker,'source_pointer':'/universe_membership/selected/'+str(i),
            'request_pointer':'/request_records/'+str(i),'source_index':i,'selected':included,
            'status':'inherited_research_scope' if included else 'outside_original_thirty_limit'})
        if included:selected.append(ticker)
    return {'status':'inherited_research_scope' if members else 'reported_empty_research_scope',
        'selected_tickers':selected,'occurrences':occurrences,'maximum_tickers':MAX_UNIVERSE,
        'parent_generated_at':stamp.isoformat(),'parent_publication_age_seconds':(generated-stamp).total_seconds(),
        'selection_is_rank_or_recommendation':False,'historical_membership_verified':False,
        'definition':'The repaired parent observation population supplies research scope only. Legacy score thresholds and theme tiers are not recreated.'}

def content(attempt,sources,limit=16*1024*1024):
    ref=attempt.get('original_ref')
    if ref is None:return None
    validate_ref(ref,PRIVATE,'sources');raw=sources.get(ref['key'])
    if not isinstance(raw,bytes) or len(raw)!=ref['bytes'] or len(raw)>limit or sha(raw)!=ref['sha256']:raise ValueError('Whole source identity differs')
    return raw

def _clocks(attempt,generated):
    a=clock(attempt.get('requested_at'));b=clock(attempt.get('received_at'))
    if a is None or b is None or not a<=b<=generated:raise ValueError('Ordered aware acquisition clocks required')


def profile(raw,ticker,encoding=''):
    row={'ticker':ticker,'status':'source_unavailable','industry':None,'sector':None,'source_pointer':None,
         'issuer_symbol_matched':False,'classification_vintage':None}
    if raw is None:return row
    try:doc=strict(raw,encoding)
    except (ValueError,UnicodeError,zlib.error):row['status']='invalid_json';return row
    if not isinstance(doc,list) or len(doc)!=1 or not isinstance(doc[0],dict):
        row['status']='ambiguous_profile_shape';return row
    item=doc[0]
    if symbol(item.get('symbol'))!=ticker:row['status']='issuer_mismatch';return row
    def label(value):return value if isinstance(value,str) and 0<len(value)<=512 and value.strip() and not any(ord(c)<32 for c in value) else None
    row.update(industry=label(item.get('industry')),sector=label(item.get('sector')),source_pointer='/0',issuer_symbol_matched=True)
    row['status']='reported_classification' if row['industry'] is not None else 'industry_unavailable'
    return row


def reusable(previous,generated):
    """Only exact prior per-profile provenance; a new packet clock never renews it."""
    if (not isinstance(previous,dict) or previous.get('measurement_contract')!=CONTRACT or
        previous.get('status')!='research_only' or any(previous.get(k) is not False for k in FLAGS)):
        return {}
    stamp=clock(previous.get('generated_at'))
    if stamp is None or not stamp<=generated:return {}
    rows=previous.get('profile_sources')
    if not isinstance(rows,list) or len(rows)>MAX_UNIVERSE:return {}
    candidates={};seen=set();duplicates=set()
    for a in rows:
        if not isinstance(a,dict):continue
        ticker=symbol(a.get('ticker'))
        if ticker is None:continue
        if ticker in seen:duplicates.add(ticker)
        seen.add(ticker)
        start=clock(a.get('requested_at'));end=clock(a.get('received_at'))
        if (a.get('status')!='received' or type(a.get('http_status')) is not int or a['http_status']!=200 or
            a.get('endpoint')!=endpoint(ticker) or start is None or end is None or not start<=end<=stamp or
            not 0<=(generated-end).total_seconds()<=PROFILE_TTL_SECONDS):continue
        try:validate_ref(a.get('original_ref'),PRIVATE,'sources')
        except ValueError:continue
        if a['original_ref']['bytes']>MAX_SOURCE_BYTES or a.get('content_encoding','') not in ('','identity','gzip'):continue
        candidates[ticker]={k:a.get(k) for k in ('ticker','endpoint','status','http_status','requested_at','received_at','original_ref','content_encoding')}
    return {k:v for k,v in candidates.items() if k not in duplicates}


def build(inputs,attempts,sources,generated_at,previous=None):
    generated=clock(generated_at)
    if generated is None or not isinstance(inputs,dict) or set(inputs)!=set(ALL_INPUTS):raise ValueError('Exact original input graph required')
    public_inputs=[];parent=None
    for name,key in ALL_INPUTS.items():
        a=inputs[name]
        if not isinstance(a,dict) or a.get('source_key')!=key:raise ValueError('Unexpected input source')
        _clocks(a,generated);raw=content(a,sources)
        if a.get('status') not in ('received','source_read_unavailable') or (raw is None)!=(a['status']=='source_read_unavailable'):
            raise ValueError('Input outcome and whole body differ')
        if name=='momentum':
            if raw is None:raise ValueError('Original research population unavailable')
            parent=strict(raw,a.get('content_encoding',''))
        public_inputs.append({'source':name,'source_key':key,'status':a['status'],'original_ref':a.get('original_ref'),
            'requested_at':a['requested_at'],'received_at':a['received_at'],'interpretation_qualified':False})
    selected=selection(parent,generated);tickers=selected['selected_tickers'];cache=reusable(previous,generated)
    if not isinstance(attempts,list) or len(attempts)!=len(tickers):raise ValueError('Every selected issuer outcome required')
    rows=[];captures=[];groups={};valid=0
    for ticker,a in zip(tickers,attempts):
        if not isinstance(a,dict) or a.get('ticker')!=ticker or a.get('endpoint')!=endpoint(ticker):raise ValueError('Unexpected profile request')
        _clocks(a,generated);raw=content(a,sources,MAX_SOURCE_BYTES)
        status=a.get('status');http=a.get('http_status');network=a.get('network_attempted');acquisition=a.get('acquisition')
        if type(network) is not bool or http is not None and (type(http) is not int or not 100<=http<=599):raise ValueError('Literal network outcome required')
        if status not in ('received','http_error','transport_unavailable','source_limit_exceeded','not_attempted_budget','not_attempted_stop'):raise ValueError('Unknown request outcome')
        if acquisition=='retained_profile':
            candidate=cache.get(ticker)
            if candidate is None or any(a.get(k)!=v for k,v in candidate.items()) or network or raw is None:
                raise ValueError('Reused profile must bind its exact original acquisition and body')
        elif acquisition=='provider_request':
            if status=='received' and (raw is None or http!=200 or not network):raise ValueError('Whole successful response required')
            if status=='http_error' and (raw is None or http is None or http==200 or not network):raise ValueError('Whole HTTP error required')
            if status.startswith('not_attempted_') and (network or http is not None):raise ValueError('Unattempted request has HTTP outcome')
            if status in ('transport_unavailable','source_limit_exceeded') and not network:raise ValueError('Transport outcome without request')
            if status not in ('received','http_error') and raw is not None:raise ValueError('Partial body is not a whole response')
        else:raise ValueError('Declared acquisition required')
        row=profile(raw if status=='received' else None,ticker,a.get('content_encoding',''))
        row.update(original_ref=a.get('original_ref'),requested_at=a['requested_at'],received_at=a['received_at'],
            acquisition=acquisition,acquisition_age_seconds=(generated-clock(a['received_at'])).total_seconds() if status=='received' else None)
        if row['status']=='reported_classification':
            valid+=1;groups.setdefault(row['industry'],[]).append({'ticker':ticker,'source_ref':a['original_ref'],'source_pointer':'/0/industry'})
        rows.append(row);captures.append({k:a.get(k) for k in ('ticker','endpoint','status','http_status','network_attempted','acquisition','requested_at','received_at','original_ref','content_encoding')})
    if tickers and not valid:raise ValueError('No valid issuer classification; preserve prior publication')
    return {'producer':'justhodl-theme-classifier','schema_version':'2.0','measurement_contract':CONTRACT,
        'generated_at':generated_at,'status':'research_only','call':'WAIT','call_semantics':'abstain',**FLAGS,
        'selection':selected,'source_inputs':public_inputs,'profile_sources':captures,'classification_observations':rows,
        'industry_memberships':[{'industry':industry,'distinct_reported_issuers':len(members),'members':members,
            'comovement_established':False,'active_theme_qualified':False} for industry,members in sorted(groups.items())],
        'coverage':{'selected_issuers':len(tickers),'classified_issuers':valid,'unavailable_issuers':len(tickers)-valid,
            'provider_requests_attempted':sum(a['network_attempted'] for a in attempts),
            'retained_profiles_reused':sum(a['acquisition']=='retained_profile' for a in attempts),'independent_roots':None,'eligible_votes':0},
        'themes':{},'ticker_to_theme':{},'all_industries_seen':[],'unclassified':[],
        'n_momentum_leaders':None,'n_active_themes':None,'n_classified':None,'n_unknown':None,
        'legacy_cache_writes':0,'model_requests':0,'notifications_sent':0,
        'cache_policy':{'per_profile_max_age_seconds':PROFILE_TTL_SECONDS,'legacy_global_clock_qualified':False,
            'original_cache_unchanged':True,'acquisition_clock_is_classification_vintage':False},
        'quality':{'status':'research_only','observation_freshness':'unqualified'},
        'limitations':['Industry and sector are provider-reported classifications, not verified legal identities or observed co-movement.',
            'Shared industry does not establish an active theme, rotation, independent evidence, forecast or portfolio size.',
            'The first thirty members of the repaired parent population define research scope, not a rank or complete market universe.',
            'Acquisition date is not the first publication or effective date of a classification.',
            'Original legacy cache and source history are retained; its one global timestamp cannot qualify individual profiles.',
            'WAIT means abstain. No action, allocation, notification or investment authority is granted.']}
