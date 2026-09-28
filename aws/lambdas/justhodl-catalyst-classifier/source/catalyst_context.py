"""Deterministic source-indexed catalyst research; no model, causal claim or size.

Original context stays in the existing anonymously denied audit namespace.
Public records contain source coordinates and reported calendar dates, never
portfolio weights, dossier text or unsupported catalyst classifications.
"""
from datetime import date, datetime, timezone
import hashlib, json, math, re, zlib

CONTRACT='catalyst-context-research.v1'
PRIVATE='audit-private/20260909-originals/momentum-leaders-research/catalyst-context/'
HEAD='data/catalysts.json'
MAX_BYTES=16*1024*1024
MAX_TOTAL=64*1024*1024
FLAGS=('calls_eligible','ranking_eligible','sizing_eligible','execution_eligible')
INPUTS={'positioning':'data/pump-positioning.json','momentum':'data/momentum-leaders.json',
 'research':'data/ticker-research-bundle.json','nlp':'data/pump-earnings-nlp.json',
 'earnings_cal':'data/earnings-tracker.json','themes':'data/momentum-themes.json','mechanics':'data/pump-mechanics.json'}


def sha(raw):return hashlib.sha256(raw).hexdigest()
def encode(obj):return (json.dumps(obj,sort_keys=True,ensure_ascii=False,allow_nan=False,separators=(',',':'))+'\n').encode('utf-8')


def decode(raw,encoding=''):
    if not isinstance(raw,bytes) or len(raw)>MAX_BYTES:raise ValueError('Whole bounded source required')
    gzip=raw.startswith(b'\x1f\x8b')
    if encoding not in ('','identity','gzip') or encoding=='gzip' and not gzip or encoding=='identity' and gzip:
        raise ValueError('Source encoding mismatch')
    decoded=raw
    if gzip:
        decoder=zlib.decompressobj(16+zlib.MAX_WBITS);decoded=decoder.decompress(raw,MAX_BYTES+1)
        if len(decoded)>MAX_BYTES or decoder.unconsumed_tail or not decoder.eof or decoder.unused_data:
            raise ValueError('Whole bounded gzip member required')
    def pairs(items):
        out={}
        for key,value in items:
            if key in out:raise ValueError('Duplicate JSON key')
            out[key]=value
        return out
    def number(value):
        n=float(value)
        if not math.isfinite(n):raise ValueError('Nonfinite source number')
        return n
    def constant(_):raise ValueError('Nonfinite JSON')
    return json.loads(decoded.decode('utf-8'),object_pairs_hook=pairs,parse_float=number,parse_constant=constant)


def identity(raw,kind):
    if kind not in ('sources','inputs','outputs','compilers','runs') or not isinstance(raw,bytes) or not 0<=len(raw)<=MAX_BYTES:
        raise ValueError('Whole reviewed artifact required')
    return {'key':PRIVATE+kind+'/'+sha(raw)+'.bin','sha256':sha(raw),'bytes':len(raw)}


def valid_identity(ref,kind):
    if (kind not in ('sources','inputs','outputs','compilers','runs') or not isinstance(ref,dict) or set(ref)!=set(('key','sha256','bytes')) or
        not isinstance(ref['sha256'],str) or not re.fullmatch('[a-f0-9]{64}',ref['sha256']) or
        type(ref['bytes']) is not int or not 0<=ref['bytes']<=MAX_BYTES or
        ref['key']!=PRIVATE+kind+'/'+ref['sha256']+'.bin'):
        raise ValueError('Exact protected source identity required')
    return ref


def ticker(value):
    return value if isinstance(value,str) and re.fullmatch(r'[A-Z0-9][A-Z0-9._-]{0,19}',value) else None


def clock(value):
    try:
        if not isinstance(value,str):return None
        stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
        return stamp.astimezone(timezone.utc) if stamp.tzinfo is not None else None
    except ValueError:return None


def pointer(value):return str(value).replace('~','~0').replace('/','~1')


def collection(doc,field):
    value=doc
    for part in field.split('/'):
        if not isinstance(value,dict):return None
        value=value.get(part)
    return value if isinstance(value,list) else None


def source_documents(attempts,sources):
    if set(attempts)!=set(INPUTS):raise ValueError('Every original source outcome required')
    docs={};summaries={}
    for name,key in INPUTS.items():
        attempt=attempts[name]
        if not isinstance(attempt,dict) or attempt.get('source_key')!=key:raise ValueError('Source identity mismatch')
        if attempt.get('status') not in ('received','source_read_unavailable'):raise ValueError('Unknown source outcome')
        summary={key:attempt.get(key) for key in ('source_key','status','requested_at','received_at','original_ref','content_encoding') if key in attempt}
        if attempt.get('status')!='received':
            if 'original_ref' in attempt:raise ValueError('Unavailable source cannot have an original')
            summaries[name]=summary;continue
        ref=valid_identity(attempt.get('original_ref'),'sources');raw=sources.get(ref['key'])
        if raw is None or identity(raw,'sources')!=ref:raise ValueError('Complete original differs')
        try:doc=decode(raw,attempt.get('content_encoding',''))
        except (ValueError,UnicodeError,zlib.error,RecursionError):
            summary['interpretation_status']='invalid_source_json';summaries[name]=summary;continue
        if not isinstance(doc,dict) or doc.get('status')=='error':
            summary['interpretation_status']='unavailable_source_document';summaries[name]=summary;continue
        summary['interpretation_status']='received_unqualified_context'
        summary['source_generated_at']=doc.get('generated_at') if clock(doc.get('generated_at')) is not None else None
        summary['observation_freshness_verified']=False
        docs[name]=doc;summaries[name]=summary
    return docs,summaries


def universe(docs,maximum=15):
    if maximum!=15:raise ValueError('Original fifteen-name scope required')
    sources=[('positioning','aggressive_basket/positions',None),('momentum','leaders',10)]
    selected=[];seen=set();occurrences=[];recognized=[]
    for name,field,limit in sources:
        rows=collection(docs.get(name),field)
        if rows is None:continue
        recognized.append(name)
        for index,row in enumerate(rows):
            symbol=ticker(row.get('ticker')) if isinstance(row,dict) else None
            in_original_window=limit is None or index<limit
            status='invalid_literal_ticker' if symbol is None else 'outside_original_source_window' if not in_original_window else 'duplicate_membership' if symbol in seen else 'original_queue_full' if len(selected)>=maximum else 'selected'
            occurrences.append({'source':name,'source_pointer':'/'+field+'/'+str(index),
                'ticker':symbol,'status':status,'selected':status=='selected'})
            if status=='selected':selected.append(symbol);seen.add(symbol)
    return {'selected_tickers':selected,'occurrences':occurrences,'recognized_selection_sources':recognized,
            'status':'unavailable' if not recognized else 'selected_original_scope' if selected else 'reported_empty_selection',
            'max_candidates':maximum,'momentum_leaders_window':10,'historical_membership_verified':False}


def coordinates(doc,name,symbol):
    """All exact ticker occurrences, without transferring original context text."""
    if not isinstance(doc,dict):return [],'source_unavailable'
    paths={'positioning':'aggressive_basket/positions','momentum':'leaders','earnings_cal':'upcoming_14d','mechanics':'candidates'}
    matches=[]
    if name in paths:
        field=paths[name];rows=collection(doc,field)
        if rows is None:return [],'source_collection_unavailable'
        for i,row in enumerate(rows):
            if isinstance(row,dict) and row.get('ticker')==symbol:matches.append(('/'+field+'/'+str(i),row))
    elif name in ('research','nlp'):
        rows=doc.get('research')
        if isinstance(rows,dict):
            row=rows.get(symbol)
            if isinstance(row,dict):
                if 'ticker' in row and row['ticker']!=symbol:return [],'conflicting_keyed_ticker'
                matches.append(('/research/'+pointer(symbol),row))
        elif isinstance(rows,list) and name=='research':
            for i,row in enumerate(rows):
                if isinstance(row,dict) and row.get('ticker')==symbol:matches.append(('/research/'+str(i),row))
        else:return [],'source_collection_unavailable'
    elif name=='themes':
        mapping=doc.get('ticker_to_theme')
        if not isinstance(mapping,dict):return [],'source_collection_unavailable'
        if symbol in mapping:
            matches.append(('/ticker_to_theme/'+pointer(symbol),mapping[symbol]))
            label=mapping[symbol];themes=doc.get('themes')
            if isinstance(label,str) and isinstance(themes,dict) and label in themes:
                matches.append(('/themes/'+pointer(label),themes[label]))
    return matches,'reported_occurrences' if matches else 'no_matching_literal_ticker'


def calendar_observation(row,path,ref):
    value=row.get('earnings_date');valid=False
    if isinstance(value,str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):
        try:date.fromisoformat(value);valid=True
        except ValueError:pass
    return {'source_pointer':path+'/earnings_date','original_ref':ref,
        'reported_date':value if valid else None,
        'status':'reported_calendar_date_unverified' if valid else 'unavailable_or_invalid_date',
        'event_occurred_verified':False,'earnings_beat_verified':False,'causal_price_effect_verified':False}


def build(attempts,sources,generated_at):
    generated=clock(generated_at)
    if generated is None:raise ValueError('Aware publication clock required')
    for attempt in attempts.values():
        requested=clock(attempt.get('requested_at'));received=clock(attempt.get('received_at'))
        if requested is None or received is None or not requested<=received<=generated:
            raise ValueError('Source collection clocks invalid')
    docs,summaries=source_documents(attempts,sources);membership=universe(docs)
    if membership['status']=='unavailable':raise ValueError('No recognized selection source; preserve previous publication')
    rows=[]
    for symbol in membership['selected_tickers']:
        evidence=[];dates=[]
        for name in INPUTS:
            matches,status=coordinates(docs.get(name),name,symbol)
            ref=summaries[name].get('original_ref')
            evidence.append({'source':name,'source_key':INPUTS[name],'status':status,
                'original_ref':ref,'source_pointers':[path for path,_ in matches],
                'matching_occurrences':len(matches),'independent_evidence':False})
            if name=='earnings_cal':dates.extend(calendar_observation(row,path,ref) for path,row in matches)
        rows.append({'ticker':symbol,'primary_catalyst':None,'catalyst_type':None,'catalyst_grade':None,
            'catalyst_date':None,'days_to_catalyst':None,'thesis_durability':None,'secondary_catalysts':[],
            'invalidation':None,'claude_reasoning':None,'source_evidence':evidence,'reported_calendar_observations':dates,
            'status':'source_context_unqualified','call':None,**{flag:False for flag in FLAGS}})
    return {'schema_version':'2.0','measurement_contract':CONTRACT,'generated_at':generated_at,
        'status':'research_only','model':None,'method':'deterministic_source_coordinates_no_model',
        'n_classified':0,'n_research_records':len(rows),'catalysts':rows,'ticker_to_catalyst':{r['ticker']:r for r in rows},
        'by_grade':{'A':[],'B':[],'C':[],'D':[]},'by_type':{},'flagged':[],
        'source_outcomes':summaries,'universe_membership':membership,
        'quality':{'status':'unqualified_research','cause_of_price_move_verified':False,'source_freshness_verified':False,
            'point_in_time_vintages_verified':False,'portfolio_consequences_verified':False},
        'call':None,**{flag:False for flag in FLAGS},'model_requests':0,'notifications_sent':0,
        'evidence_access':'Original context is retained in the existing protected audit namespace. Public records expose coordinates and reported calendar dates only.'}
