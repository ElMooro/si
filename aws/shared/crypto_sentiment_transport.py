"""Complete attempt capture for the producer's three existing requests."""
from base64 import b64decode,b64encode
from datetime import datetime,timezone
from urllib.error import HTTPError
from urllib.request import Request
import hashlib,math,re

REQUESTS=(('https://api.alternative.me/fng/?limit=31',15),
          ('https://api.alternative.me/fng/?limit=0',15),
          ('https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=max&interval=daily',20))
HEADERS={'User-Agent':'Mozilla/5.0 (compatible; JustHodl/4.1)','Accept':'application/json'}
ERRORS=(None,'transport_failed','body_read_failed','nonbinary_body','response_exceeds_capture_limit',
        'declared_content_length_mismatch','insufficient_remaining_time','upstream_prerequisite_unavailable')

def digest(raw):return hashlib.sha256(raw).hexdigest()

def lengths_match(lengths,size):
    # Header length is bounded before int conversion, preserving an anomalous
    # original header as evidence without triggering Python's digit limit.
    return not lengths or (len(lengths)==1 and isinstance(lengths[0],str)
        and 0<len(lengths[0])<=20 and re.fullmatch('[0-9]+',lengths[0]) is not None
        and int(lengths[0])==size)

def adequate_budget(value):
    return type(value) in (int,float) and math.isfinite(value) and value>=30000

def whole(response,limit,remaining_ms):
    parts=[];count=0
    while count<=limit:
        if not adequate_budget(remaining_ms()):return b''.join(parts),False,'insufficient_remaining_time'
        try:part=response.read(min(65536,limit+1-count))
        except Exception as exc:
            tail=getattr(exc,'partial',b'')
            if isinstance(tail,bytes):parts.append(tail[:limit+1-count])
            return b''.join(parts),False,'body_read_failed'
        if not isinstance(part,bytes):return b''.join(parts),False,'nonbinary_body'
        if not part:return b''.join(parts),True,None
        parts.append(part);count+=len(part)
    return b''.join(parts),False,'response_exceeds_capture_limit'

def capture(opener,url,timeout,model,now,remaining_ms,skip=False):
    if (url,timeout) not in REQUESTS:raise ValueError('Existing request only')
    started=now().isoformat();raw=b'';complete=False;status=None;error=None;identity=False;lengths=[]
    budget=remaining_ms()
    if skip:error='upstream_prerequisite_unavailable'
    elif not adequate_budget(budget):error='insufficient_remaining_time'
    else:
        try:
            try:response=opener(Request(url,headers=HEADERS),timeout=timeout)
            except HTTPError as exc:response=exc
            with response:
                status=response.status if hasattr(response,'status') else response.code
                identity=response.geturl()==url
                raw,complete,error=whole(response,model.LIMIT,remaining_ms)
                headers=response.headers
                if headers:
                    lengths=headers.get_all('Content-Length') if hasattr(headers,'get_all') else ([headers['Content-Length']] if 'Content-Length' in headers else [])
                    lengths=lengths or []
                if complete and not lengths_match(lengths,len(raw)):complete=False;error='declared_content_length_mismatch'
        except Exception:error='transport_failed'
    finished=now().isoformat()
    if model.clock(started)>model.clock(finished):raise ValueError('Reversed acquisition clock')
    attempt={'request_url':url,'timeout_seconds':timeout,'acquisition_started_at':started,
             'acquisition_completed_at':finished,'http_status':status if type(status) is int else None,
             'response_complete':complete,'final_url_matches_request':identity,'declared_content_lengths':lengths,
             'transport_error':error,'received_base64':b64encode(raw).decode('ascii'),
             'received_bytes':len(raw),'received_sha256':digest(raw)}
    validate(attempt,model)
    return attempt

def validate(attempt,model):
    keys={'request_url','timeout_seconds','acquisition_started_at','acquisition_completed_at','http_status',
          'response_complete','final_url_matches_request','declared_content_lengths','transport_error',
          'received_base64','received_bytes','received_sha256'}
    if not isinstance(attempt,dict) or set(attempt)!=keys:raise ValueError('Exact attempt ledger required')
    if type(attempt['timeout_seconds']) is not int or (attempt['request_url'],attempt['timeout_seconds']) not in REQUESTS:raise ValueError('Original request identity differs')
    if model.clock(attempt['acquisition_started_at'])>model.clock(attempt['acquisition_completed_at']):raise ValueError('Reversed acquisition clock')
    if any(type(attempt[name]) is not bool for name in ('response_complete','final_url_matches_request')):raise ValueError('Typed attempt state required')
    if attempt['http_status'] is not None and (type(attempt['http_status']) is not int or not 100<=attempt['http_status']<=599):raise ValueError('HTTP status required')
    if attempt['transport_error'] not in ERRORS:raise ValueError('Named attempt outcome required')
    lengths=attempt['declared_content_lengths']
    if not isinstance(lengths,list) or not all(isinstance(value,str) for value in lengths):raise ValueError('Whole length-header values required')
    encoded=attempt['received_base64']
    if not isinstance(encoded,str) or len(encoded)>4*((model.LIMIT+3)//3):raise ValueError('Bounded original encoding required')
    raw=b64decode(encoded,validate=True)
    if (type(attempt['received_bytes']) is not int or len(raw)!=attempt['received_bytes'] or len(raw)>model.LIMIT+1
        or digest(raw)!=attempt['received_sha256'] or b64encode(raw).decode('ascii')!=encoded):raise ValueError('Original bytes differ')
    if attempt['response_complete'] and (len(raw)>model.LIMIT or not lengths_match(lengths,len(raw))):raise ValueError('Complete response proof differs')
    return raw

def collect(opener,model,remaining_ms,now=None):
    now=now or (lambda:datetime.now(timezone.utc))
    attempts=[capture(opener,url,timeout,model,now,remaining_ms) for url,timeout in REQUESTS[:2]]
    # The predecessor never reached its price request after unusable recent
    # sentiment. Preserve that bound; still retain an explicit skipped attempt.
    first=attempts[0];raw=validate(first,model)
    usable=(first['response_complete'] and first['http_status']==200 and first['final_url_matches_request']
            and first['transport_error'] is None and bool(raw))
    prerequisite=(model.build(raw,source_url=REQUESTS[0][0],acquired_at=first['acquisition_completed_at']).get('status')=='descriptive') if usable else False
    attempts.append(capture(opener,*REQUESTS[2],model,now,remaining_ms,skip=not prerequisite))
    return project(attempts,model)

def reconcile(recent,full):
    """Compare the provider's two copies without counting two independent sources."""
    recent_rows=recent.get('series',[]) if recent.get('status')=='descriptive' else []
    full_rows=full.get('series',[]) if full.get('status')=='descriptive' else []
    by_clock={row['timestamp']:row for row in full_rows}
    disagreements=[]
    for row in recent_rows:
        other=by_clock.get(row['timestamp'])
        if other and row['value'] is not None and other['value'] is not None and row['value']!=other['value']:
            disagreements.append({'timestamp':row['timestamp'],'date':row['date'],
                                  'recent_value':row['value'],'full_value':other['value'],
                                  'recent_source_rows':row['source_rows'],'full_source_rows':other['source_rows']})
    current=recent.get('current');label=recent.get('label') if current is not None else None
    reason='reported_recent_endpoint_observation_only' if current is not None else 'recent_endpoint_current_unavailable'
    if recent_rows and full_rows:
        if recent_rows[-1]['timestamp']!=full_rows[-1]['timestamp']:
            current=label=None;reason='endpoint_latest_observation_clocks_differ'
        elif any(row['timestamp']==recent_rows[-1]['timestamp'] for row in disagreements):
            current=label=None;reason='endpoint_current_values_disagree'
    averages={}
    for name in ('7d','30d'):
        original=recent.get('averages',{}).get(name)
        if original is None:continue
        conflict=any(row['date'] in original['source_dates'] for row in disagreements)
        averages[name]={**original,'cross_response_conflict':conflict}
        if conflict:
            averages[name].update(status='unavailable',value=None,reason='endpoint_values_disagree_in_window')
    return {'current':current,'label':label,'current_reason':reason,
            'source_observation_at':recent.get('source_observation_at'),
            'current_source_attempt':0,'first_publication_at':None,
            'averages':averages,'avg_7d':averages.get('7d',{}).get('value'),
            'avg_30d':averages.get('30d',{}).get('value'),
            'source_disagreements':disagreements,'endpoint_copies_are_independent_sources':False}

def project(attempts,model):
    if not isinstance(attempts,list) or len(attempts)!=len(REQUESTS):raise ValueError('Complete original request population required')
    observations=[];previous=None
    for index,(attempt,(url,timeout)) in enumerate(zip(attempts,REQUESTS)):
        raw=validate(attempt,model)
        if attempt['request_url']!=url or attempt['timeout_seconds']!=timeout:raise ValueError('Original request sequence differs')
        started=model.clock(attempt['acquisition_started_at']);finished=model.clock(attempt['acquisition_completed_at'])
        if previous is not None and started<previous:raise ValueError('Sequential attempts overlap')
        previous=finished
        if index<2:
            usable=attempt['response_complete'] and attempt['http_status']==200 and attempt['final_url_matches_request'] and attempt['transport_error'] is None and bool(raw)
            observations.append(model.build(raw,source_url=url,acquired_at=attempt['acquisition_completed_at']) if usable else
                {'contract':model.CONTRACT,'status':'unavailable','current':None,'avg_7d':None,'avg_30d':None,
                 'source_url':url,'reason':'original_response_unavailable_or_identity_unqualified',**model.DENIED})
    return {'contract':'crypto-reported-bitcoin-sentiment.v1','source_attempts':attempts,
            'status':'descriptive' if any(row.get('status')=='descriptive' for row in observations) else 'unavailable',
            'recent_reported_observations':observations[0],'full_reported_observations':observations[1],
            'legacy_price_attempt_index':2,'synthetic_history_is_provider_data':False,
            'history':observations[0].get('history',[]),'full_history':observations[1].get('history',[]),
            'full_series':observations[1].get('series',[]),
            'projection_scope':'Original attempts and separate reported Bitcoin index observations. No price-derived prehistory, independent double vote or investment qualification.',
            **reconcile(*observations),**model.DENIED}
