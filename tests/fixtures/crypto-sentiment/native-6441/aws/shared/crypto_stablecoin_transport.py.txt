"""Whole public stablecoin response acquisition; only called by existing producer."""
from base64 import b64encode,b64decode
from datetime import datetime,timezone,timedelta
import hashlib,json,math,re
from urllib.request import Request
from urllib.error import HTTPError

URL='https://stablecoins.llama.fi/stablecoins?includePrices=true'
HEADERS={'User-Agent':'Mozilla/5.0 (compatible; JustHodl/4.1)','Accept':'application/json'}

def stamp(value):
    if not isinstance(value,str):raise ValueError('Explicit UTC acquisition clock required')
    out=datetime.fromisoformat(value.replace('Z','+00:00'))
    if out.tzinfo is None or out.utcoffset()!=timedelta(0):raise ValueError('Explicit UTC acquisition clock required')
    return out

def whole(response,limit):
    parts=[];count=0
    while count<=limit:
        try:part=response.read(min(65536,limit+1-count))
        except Exception as exc:
            partial=getattr(exc,'partial',b'')
            if isinstance(partial,bytes):parts.append(partial[:limit+1-count])
            return b''.join(parts),False,'body_read_failed'
        if not isinstance(part,bytes):return b''.join(parts),False,'nonbinary_body'
        if not part:return b''.join(parts),True,None
        parts.append(part);count+=len(part)
    return b''.join(parts),False,'response_exceeds_capture_limit'

def acquire(opener,model,now=None):
    now=now or (lambda:datetime.now(timezone.utc))
    started=now().isoformat();raw=b'';complete=False;status=None;failure=None;identity=False;length=None
    try:
        try:response=opener(Request(URL,headers=HEADERS),timeout=15)
        except HTTPError as exc:response=exc
        with response:
            status=response.status if hasattr(response,'status') else response.code
            identity=response.geturl()==URL
            raw,complete,failure=whole(response,model.LIMIT)
            length=response.headers.get('Content-Length') if response.headers else None
            if complete and length is not None and (not isinstance(length,str) or not re.fullmatch('[0-9]+',length) or int(length)!=len(raw)):
                complete=False;failure='declared_content_length_mismatch'
    except Exception:
        failure='transport_failed'
    finished=now().isoformat()
    if stamp(started)>stamp(finished):raise ValueError('Reversed acquisition clocks')
    attempt={'request_url':URL,'acquisition_started_at':started,'acquisition_completed_at':finished,
             'http_status':status if type(status) is int else None,'response_complete':complete,
             'declared_content_length':length,'final_url_matches_request':identity,
             'transport_error':failure,'received_base64':b64encode(raw).decode('ascii'),
             'received_bytes':len(raw),'received_sha256':hashlib.sha256(raw).hexdigest()}
    return project(attempt,model)

def project(attempt,model):
    """Recompute whole descriptive packet from a complete original or explicit prefix."""
    expected={'request_url','acquisition_started_at','acquisition_completed_at','http_status','response_complete',
              'declared_content_length','final_url_matches_request','transport_error','received_base64','received_bytes','received_sha256'}
    if not isinstance(attempt,dict) or set(attempt)!=expected or attempt['request_url']!=URL:
        raise ValueError('Exact stablecoin acquisition ledger required')
    if stamp(attempt['acquisition_started_at'])>stamp(attempt['acquisition_completed_at']):raise ValueError('Reversed acquisition clocks')
    for key in ('response_complete','final_url_matches_request'):
        if type(attempt[key]) is not bool:raise ValueError('Typed acquisition state required')
    if attempt['http_status'] is not None and (type(attempt['http_status']) is not int or not 100<=attempt['http_status']<=599):raise ValueError('Typed HTTP status required')
    if attempt['transport_error'] not in (None,'body_read_failed','nonbinary_body','response_exceeds_capture_limit','declared_content_length_mismatch','transport_failed'):
        raise ValueError('Named transport outcome required')
    if not isinstance(attempt['received_base64'],str) or len(attempt['received_base64'])>4*((model.LIMIT+3)//3):
        raise ValueError('Bounded original encoding required')
    raw=b64decode(attempt['received_base64'],validate=True)
    if (type(attempt['received_bytes']) is not int or len(raw)!=attempt['received_bytes'] or len(raw)>model.LIMIT+1
        or hashlib.sha256(raw).hexdigest()!=attempt['received_sha256'] or b64encode(raw).decode('ascii')!=attempt['received_base64']):
        raise ValueError('Whole original or exact retained prefix differs')
    length=attempt['declared_content_length']
    if length is not None and not isinstance(length,str):raise ValueError('Original length header must be text')
    complete=attempt['response_complete']
    if complete and (len(raw)>model.LIMIT or (length is not None and (not re.fullmatch('[0-9]+',length) or int(length)!=len(raw)))):
        raise ValueError('Declared complete response is not complete')
    usable=complete and attempt['http_status']==200 and attempt['final_url_matches_request'] and attempt['transport_error'] is None and bool(raw)
    if usable:out=model.build(raw)
    else:
        out={'contract':model.CONTRACT,'status':'unavailable','stablecoins':[],
             'total_mcap':None,'total_mcap_fmt':None,'minting_count':None,'burning_count':None,'stable_count':None,'net_signal':'UNAVAILABLE',
             'reported_rows':None,'identified_rows':0,'unresolved_rows':None,
             'source_observation_clocks_available':False,'observation_freshness_verified':False,
             'population_comparability_verified':False,'cash_flow_verified':False,
             'reason':'source_response_unavailable_or_identity_unqualified',**model.DENIED}
    return {**out,'source_attempt':attempt,'source_authenticity_verified':False,'point_in_time_qualified':False}
