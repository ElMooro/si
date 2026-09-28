"""Replayable original-scope volume observations; no detection or confirmation."""
from datetime import date,timedelta
from decimal import Decimal
import json,re,zlib
from urllib.parse import quote
from context_evidence_store import clock,validate_ref,sha
from volume_observations import CONTRACT,FLAGS,LOOKBACK_CALENDAR_DAYS,calculate
from momentum_research_boundary import exclusion

HEAD='data/velocity-acceleration.json'
PRIVATE='audit-private/20260909-originals/momentum-leaders-research/velocity-context/'
INPUTS={'momentum':'data/momentum-leaders.json','themes':'data/momentum-themes.json',
        'breakout':'data/momentum-breakout.json','options':'data/options-flow-scanner.json','buzz':'data/buzz-velocity.json'}
STATE_KEY='data/_state/velocity-acceleration-pending.json'
ALL_INPUTS={**INPUTS,'previous_pending_state':STATE_KEY}
MAX_SOURCE_BYTES=8*1024*1024
MAX_UNIVERSE=80


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
            'status':'inherited_research_scope' if included else 'outside_original_eighty_limit'})
        if included:selected.append(ticker)
    return {'status':'inherited_research_scope' if members else 'reported_empty_research_scope',
        'selected_tickers':selected,'occurrences':occurrences,'maximum_tickers':MAX_UNIVERSE,
        'parent_generated_at':stamp.isoformat(),'parent_publication_age_seconds':(generated-stamp).total_seconds(),
        'selection_is_rank_or_recommendation':False,'historical_membership_verified':False,
        'definition':'The repaired parent observation population supplies research scope only. Legacy score thresholds and theme tiers are not recreated.'}


def endpoint(ticker,as_of):
    if symbol(ticker) is None or type(as_of) is not date:raise ValueError('Literal issuer and acquisition date required')
    return ('https://financialmodelingprep.com/stable/historical-price-eod/full?symbol='+quote(ticker,safe='')+
        '&from='+(as_of-timedelta(days=LOOKBACK_CALENDAR_DAYS)).isoformat()+'&to='+as_of.isoformat())


def content(attempt,sources,limit=16*1024*1024):
    ref=attempt.get('original_ref')
    if ref is None:return None
    validate_ref(ref,PRIVATE,'sources');raw=sources.get(ref['key'])
    if not isinstance(raw,bytes) or len(raw)!=ref['bytes'] or len(raw)>limit or sha(raw)!=ref['sha256']:raise ValueError('Whole source identity differs')
    return raw


def _clocks(attempt,generated):
    a=clock(attempt.get('requested_at'));b=clock(attempt.get('received_at'))
    if a is None or b is None or not a<=b<=generated:raise ValueError('Ordered aware acquisition clocks required')


def build(input_attempts,attempts,sources,generated_at):
    generated=clock(generated_at)
    if generated is None or not isinstance(input_attempts,dict) or set(input_attempts)!=set(ALL_INPUTS):raise ValueError('Exact original input graph required')
    inputs=[];parent=None
    for name,key in ALL_INPUTS.items():
        a=input_attempts[name]
        if not isinstance(a,dict) or a.get('source_key')!=key:raise ValueError('Unexpected input source')
        _clocks(a,generated);raw=content(a,sources)
        if name=='breakout':
            if a.get('status')!='not_read_existing_research_exclusion' or raw is not None:raise ValueError('Existing breakout abstention must remain before storage')
        elif a.get('status') not in ('received','source_read_unavailable') or (raw is None)!=(a['status']=='source_read_unavailable'):
            raise ValueError('Input acquisition and whole original disagree')
        if name=='momentum':
            if raw is None:raise ValueError('Original parent population unavailable')
            parent=strict(raw,a.get('content_encoding',''))
        # Preserve all other originals without interpreting scores, confirmations
        # or private pending-state content as public decisions.
        inputs.append({'source':name,'source_key':key,'status':a['status'],'original_ref':a.get('original_ref'),
            'requested_at':a['requested_at'],'received_at':a['received_at'],'interpretation_qualified':False})
    selected=selection(parent,generated);expected=selected['selected_tickers']
    if not isinstance(attempts,list) or len(attempts)!=len(expected):raise ValueError('Every declared provider outcome required')
    captures=[];observations=[];measured=0
    outcomes=('received','http_error','transport_unavailable','source_limit_exceeded','not_attempted_budget','not_attempted_stop')
    for ticker,a in zip(expected,attempts):
        if not isinstance(a,dict) or a.get('ticker')!=ticker or a.get('endpoint')!=endpoint(ticker,generated.date()) or a.get('status') not in outcomes:
            raise ValueError('Unexpected provider request')
        _clocks(a,generated);raw=content(a,sources,MAX_SOURCE_BYTES);http=a.get('http_status');attempted=a.get('network_attempted')
        if type(attempted) is not bool or http is not None and (type(http) is not int or not 100<=http<=599):raise ValueError('Literal HTTP outcome required')
        if a['status']=='received' and (raw is None or http!=200 or not attempted):raise ValueError('Whole successful response required')
        if a['status']=='http_error' and (raw is None or http is None or http==200 or not attempted):raise ValueError('Whole HTTP error required')
        if a['status'].startswith('not_attempted_') and (attempted or http is not None):raise ValueError('Unattempted request has HTTP outcome')
        if a['status'] in ('transport_unavailable','source_limit_exceeded') and not attempted:raise ValueError('Transport outcome without request')
        if a['status'] not in ('received','http_error') and raw is not None:raise ValueError('Partial body cannot be a retained whole response')
        doc=None;parse_status='source_unavailable'
        if a['status']=='received':
            try:doc=strict(raw,a.get('content_encoding',''));parse_status='json_parsed'
            except (ValueError,UnicodeError,zlib.error):parse_status='invalid_json'
        row=calculate(doc,ticker,generated.date());row.update(history_original_ref=a.get('original_ref'),parse_status=parse_status)
        measured+=sum(m['status']=='measured' for m in row['measurements']);observations.append(row)
        captures.append({k:a.get(k) for k in ('ticker','endpoint','status','http_status','network_attempted','requested_at','received_at','original_ref')})
    if expected and not measured:raise ValueError('No valid dated measurement; preserve prior publication')
    packet={'engine':'velocity-acceleration','schema_version':'2.0','measurement_contract':CONTRACT,'generated_at':generated_at,
        'status':'research_only','call':'WAIT','call_semantics':'abstain',**FLAGS,'momentum_research_exclusion':exclusion(),
        'selection':selected,'source_inputs':inputs,
        'acquisitions':captures,'volume_observations':observations,'pending_state_policy':'Retained privately, unchanged; no promotion, expiration or confirmation.',
        'coverage':{'selected_tickers':len(expected),'planned_provider_requests':len(expected),'measured_descriptive_fields':measured,
            'independent_roots':None,'eligible_votes':0},'quality':{'status':'research_only','observation_freshness':'unqualified'},
        'trading_date':None,'new_session':None,'universe_size':len(expected),'by_tier':{},'themes':[],
        'model_requests':0,'notifications_sent':0,'pending_state_writes':0,
        'config':{'baseline_observations':20,'recent_observations':7,'request_calendar_span':LOOKBACK_CALENDAR_DAYS,'max_universe':MAX_UNIVERSE},
        'limitations':['Reported-date observations do not establish exchange sessions, adjusted prices or verified volume units.',
            'Slope, signed volume and floor change are transformations of one price/volume source, not independent confirmations.',
            'A first-order volume slope is not a second derivative, investor flow, institutional accumulation or demonstrated advance warning.',
            'The inherited population is research scope with composite ancestry; no historical membership or market coverage is established.',
            'No legacy score, tier, theme label or other engine output grants predictive or portfolio authority.',
            'WAIT means abstain. Original contexts and previous/current publications remain private and replayable.']}
    for key in ('fresh_fires','confirmed_today','aging','expired_today','actionable_tickers','emerging','watch'):packet[key]=[]
    for key in ('n_fired','n_emerging','n_watch','n_fresh','n_confirmed_today','n_aging','n_expired_today','n_actionable'):packet[key]=None
    return packet
