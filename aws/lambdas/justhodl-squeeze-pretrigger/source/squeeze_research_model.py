"""Candidate retained-input squeeze research; no probability, ranking or trade."""
from datetime import datetime
from decimal import Decimal
import hashlib,json,math,zlib
import short_position_context
import short_volume_context

CONTRACT='squeeze-pretrigger-research.v1'
INPUTS={'finra':'data/finra-short.json','short_interest':'data/short-interest-tickers.json','catalyst':'data/catalyst-calendar.json'}
FLAGS=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')
MAX_BYTES=16*1024*1024
MAX_ROWS=500

def strict(raw,encoding=''):
    if not isinstance(raw,bytes) or len(raw)>MAX_BYTES:raise ValueError('Bounded whole source required')
    gzip=raw.startswith(b'\x1f\x8b')
    if encoding not in ('','identity','gzip') or encoding=='gzip' and not gzip or encoding=='identity' and gzip:raise ValueError('Encoding mismatch')
    decoded=raw
    if gzip:
        decoder=zlib.decompressobj(16+zlib.MAX_WBITS);decoded=decoder.decompress(raw,MAX_BYTES+1)
        if len(decoded)>MAX_BYTES or decoder.unconsumed_tail or not decoder.eof or decoder.unused_data:raise ValueError('One whole bounded gzip member required')
    def pairs(items):
        out={}
        for key,value in items:
            if key in out:raise ValueError('Duplicate JSON key')
            out[key]=value
        return out
    def number(text):
        exact=Decimal(text);value=float(exact)
        if not math.isfinite(value) or Decimal(str(value))!=exact:raise ValueError('Nonrepresentable number')
        return value
    def constant(_):raise ValueError('Nonfinite number')
    result=json.loads(decoded.decode('utf-8'),object_pairs_hook=pairs,parse_float=number,parse_constant=constant)
    # JSON escape syntax permits isolated UTF-16 surrogate code units. They
    # cannot be emitted as UTF-8 and must not break the later public write.
    pending=[result]
    while pending:
        value=pending.pop()
        if isinstance(value,str):value.encode('utf-8')
        elif isinstance(value,dict):pending.extend(value);pending.extend(value.values())
        elif isinstance(value,list):pending.extend(value)
    return result

def reported_clock(value):
    if not isinstance(value,str):return None
    try:
        parsed=datetime.fromisoformat(value.replace('Z','+00:00'))
        return value if parsed.tzinfo is not None else None
    except ValueError:return None

def project(attempts,sources,generated_at,validate_ref,prefix):
    if set(attempts)!=set(INPUTS) or reported_clock(generated_at) is None:raise ValueError('Complete attempt inventory and aware publication clock required')
    metadata={};positions=short_position_context.descriptive_context(None)
    for name,key in INPUTS.items():
        attempt=attempts[name]
        if not isinstance(attempt,dict) or attempt.get('source_key')!=key:raise ValueError('Exact input binding required')
        meta={'artifact':key,'read_status':'unavailable','observation_freshness_verified':False,'identity_verified':False,**dict.fromkeys(FLAGS,False)}
        if attempt.get('status')=='source_read_unavailable':metadata[name]=meta;continue
        if attempt.get('status')!='received':raise ValueError('Unrecognized attempt status')
        ref=validate_ref(attempt.get('original_ref'),prefix,'sources');raw=sources.get(ref['key'])
        if not isinstance(raw,bytes) or len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=ref['sha256']:raise ValueError('Exact retained source required')
        meta.update(body_sha256=ref['sha256'],body_bytes=ref['bytes'],original_ref=ref,read_status='malformed')
        try:
            packet=strict(raw,attempt.get('content_encoding',''))
            if not isinstance(packet,dict):raise ValueError('Object packet required')
            status=packet.get('status')
            if bool(packet.get('error')) or isinstance(status,str) and status.lower() in ('error','failed','failure','unavailable'):
                meta['read_status']='reported_error';metadata[name]=meta;continue
            meta.update(read_status='parsed',reported_generated_at=reported_clock(packet.get('generated_at')),
                        reported_as_of=reported_clock(packet.get('as_of')))
            if name=='short_interest':positions=short_position_context.descriptive_context(packet)
            if name=='finra':
                # The boundary must continue to withhold any donor pressure or
                # squeeze score. Its clock-dependent reference status is not
                # projected; replay output depends only on the retained inputs.
                view=short_volume_context.decision_view(packet)
                if any(view[k] is not False for k in FLAGS) or view['independent_investment_votes']!=0:raise ValueError('Short-volume authority boundary differs')
        except (ValueError,TypeError,UnicodeError,OverflowError,RecursionError,zlib.error):
            meta['read_status']='malformed'
        metadata[name]=meta
    keys=sorted(positions['by_ticker']);selected=keys[:MAX_ROWS]
    reason='Squeeze probability and portfolio actions are withheld: float/borrow measurements, point-in-time identity, source freshness and an out-of-sample net-of-cost scorecard are not qualified.'
    return {'engine':'squeeze-pretrigger','version':'2.0.0','contract':CONTRACT,'measurement_contract':CONTRACT,
        'generated_at':generated_at,'as_of':generated_at,'as_of_semantics':'Publication time only; not an observation date.',
        'observation_date':None,'state':'UNQUALIFIED','previous_state':None,'state_description':reason,
        'portfolio_action':'WAIT','call':None,'signal_strength':None,**dict.fromkeys(FLAGS,False),
        'independent_investment_votes':0,'decision':{'abstain':True,'eligible_votes':0,'reason':reason},
        'quality':{'status':'unqualified','observation_freshness_verified':False,'independent_security_identity_verified':False},
        'inputs':metadata,'short_position_context':{'contract':positions['contract'],'status':positions['status'],
            'by_ticker':{key:positions['by_ticker'][key] for key in selected},'selection':'Lexical source-symbol order; descriptive bounded projection, not a rank.',
            'projected_tickers':len(selected),'available_unambiguous_tickers':len(keys),'omitted_from_projection':max(0,len(keys)-MAX_ROWS),
            'source_occurrences':positions['source_occurrences'],'ambiguous_symbol_count':len(positions['ambiguous_symbols']),
            'unresolved_occurrence_count':len(positions['unresolved_occurrences']),'identity_verified':False,
            'whole_source_body_retained':isinstance(metadata['short_interest'].get('original_ref'),dict),**dict.fromkeys(FLAGS,False)},
        'short_volume_definition':short_volume_context.NOTE,
        'summary':{'n_candidates_evaluated':None,'n_imminent_5of5':None,'n_pretrigger_4of5':None,'n_early_3of5':None,'n_total_setups':None,
            'feeds_available':{name:meta['read_status']=='parsed' for name,meta in metadata.items()},'availability_is_freshness':False},
        'current_readings':{'top_squeeze_tickers':[],'imminent_tickers':[],'pretrigger_tickers':[]},
        'imminent_setups':[],'pretrigger_setups':[],'early_setups':[],'trigger_conditions':[],
        'forward_expectations':{'1m':None,'2m':None,'wr':None,'basis':'No qualified historical or out-of-sample forecast.'},
        'recommended_trade':None,'historical_episodes':[],'why_now_explainer':reason,
        'sources':list(INPUTS.values()),'enrichment_policy':{'provider_profile_and_price_requests_enabled':False,'purpose':'Preserve existing feed evidence before adding qualified enrichment.'},
        'model_requests':0,'notifications_sent':0,'portfolio_writes':0,
        'replay_scope':'Retained donor bytes and compiler sources reproduce this descriptive projection and abstention; original provider measurements and forecasting remain unqualified.'}
