"""Diagnostic Ticker-360 coverage; input availability never grants a vote."""
import hashlib,json,math,re
from decimal import Decimal

SOURCE='data/ticker-360.json'
LIMIT=16*1024*1024
FLAGS=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')


def blank():
    return {'contract':'ticker-coverage-context.v1','status':'unavailable','by_ticker':{},
            'indexed_rows':None,'ambiguous_symbols':[],'unresolved_rows':0,
            'additional_independent_votes':0,'source_qualified':False,
            'observation_freshness_verified':False,'original_body_retained':False,
            'reason':'Coverage is a source inventory, not independent agreement or validated investment evidence.',
            **dict.fromkeys(FLAGS,False)}


def context(packet):
    out=blank()
    if not isinstance(packet,dict) or packet.get('contract')!='ticker-360.v1' or not isinstance(packet.get('tickers'),dict):return out
    if packet.get('error') or packet.get('_error') or packet.get('status') in ('error','failed','failure','unavailable'):return out
    grouped={}
    for key,row in packet['tickers'].items():
        if not isinstance(key,str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9.:-]{0,31}',key):
            out['unresolved_rows']+=1;continue
        symbol=key.upper()
        if not isinstance(row,dict) or (row.get('ticker') is not None and row.get('ticker')!=symbol):
            out['unresolved_rows']+=1
            grouped.setdefault(symbol,[]).append({'ticker':symbol,'reported_ticker_key':key,
                'source_row':'/tickers/'+key,'status':'unresolved_identity_or_row'})
            continue
        domains=row.get('domains')
        names=sorted(domains) if isinstance(domains,dict) and all(isinstance(k,str) and re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,63}',k) and isinstance(v,dict) for k,v in domains.items()) else None
        count=row.get('coverage_count');count=count if type(count) is int and 0<=count<=9007199254740991 else None
        # Keep the producer's reported count visible, but do not silently repair it.
        matched=names is not None and count is not None and len(names)==count
        item={'ticker':symbol,'reported_ticker_key':key,'source_row':'/tickers/'+key,
              'reported_coverage_count':count,'coverage_count':count if matched else None,
              'domains':names if names is not None else [],'count_matches_domain_inventory':matched,
              'status':'descriptive_only' if matched else 'unreconciled_coverage',
              'independence_verified':False,'observation_freshness_verified':False,
              'additional_independent_votes':0,**dict.fromkeys(FLAGS,False)}
        grouped.setdefault(symbol,[]).append(item)
    for symbol,rows in grouped.items():
        if len(rows)!=1:out['ambiguous_symbols'].append({'ticker':symbol,'occurrences':rows})
        elif rows[0]['status']!='unresolved_identity_or_row':out['by_ticker'][symbol]=rows[0]
    out.update(status='descriptive_only',indexed_rows=len(packet['tickers']))
    return out


def load(client,bucket):
    body=None;meta={'artifact':SOURCE,'read_status':'unavailable','body_bytes':None,'body_sha256':None}
    def pairs(items):
        out={}
        for k,v in items:
            if k in out:raise ValueError('Duplicate JSON member')
            out[k]=v
        return out
    def number(v):
        n=float(v)
        if not math.isfinite(n) or n==0 and Decimal(v)!=0:raise ValueError('Unrepresentable JSON number')
        return n
    def constant(_):raise ValueError('Nonfinite JSON')
    try:
        response=client.get_object(Bucket=bucket,Key=SOURCE);body=response['Body'];length=response.get('ContentLength')
        if type(length) is not int or not 0<=length<=LIMIT:raise ValueError('Typed bounded complete length required')
        chunks=[];size=0
        while True:
            chunk=body.read(min(65536,LIMIT+1-size))
            if not isinstance(chunk,bytes):raise ValueError('Byte stream required')
            if not chunk:break
            size+=len(chunk)
            if size>LIMIT:raise ValueError('Bound exceeded')
            chunks.append(chunk)
        if size!=length:raise ValueError('Incomplete response')
        raw=b''.join(chunks);meta.update(body_bytes=size,body_sha256=hashlib.sha256(raw).hexdigest(),read_status='malformed')
        packet=json.loads(raw.decode('utf-8'),parse_float=number,parse_constant=constant,object_pairs_hook=pairs)
        out=context(packet);meta['read_status']='parsed' if isinstance(packet,dict) else 'malformed'
    except Exception:
        out=blank()
    finally:
        if body is not None:
            try:body.close()
            except Exception:pass
    return {**out,'acquisition':meta}
