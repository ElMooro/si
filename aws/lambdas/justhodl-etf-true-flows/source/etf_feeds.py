"""Desk feeds derived from the published research: one columnar series file per fund with a retained issuer history.

These are read-only views of the retained histories for chart.html's Inflows & outflows desk (flows:etfc/<TICKER>…),
written after a publication succeeds and never part of the replay contract: every value is a float of a decimal that
stands, with its original, in the retained history the feed names.
"""
from collections import deque
from decimal import Decimal
import json

CONTRACT='etf-creations-feed.v1'
INDEX_CONTRACT='etf-creations-index.v1'
PREFIX='data/etf-creations/'
SERIES=('flow','flow_5d','flow_20d','shares','nav','net_assets')
MAX_ROWS=8000

def _float(value):
    return None if value in (None,'') else float(Decimal(str(value)))

def rolling(flows,count):
    """Sum of the last `count` consecutive daily estimates (exact decimals), or None while any of them is missing."""
    out=[];window=deque()
    for value in flows:
        window.append(None if value is None else Decimal(str(value)))
        if len(window)>count:window.popleft()
        out.append(float(sum(window,Decimal(0))) if len(window)==count and all(v is not None for v in window) else None)
    return out

def feed(history,packet_generated_at):
    """The desk feed for one retained etf-native-history.v1 document."""
    if history.get('contract')!='etf-native-history.v1' or not isinstance(history.get('rows'),list):raise ValueError('issuer history contract differs')
    rows=history['rows'][-MAX_ROWS:]
    exact=[v.get('nav_valued_share_change_decimal') if v.get('flow_status')=='descriptive_estimate' else None for v in rows]
    flows=[_float(v) for v in exact]
    series={'flow':flows,'flow_5d':rolling(exact,5),'flow_20d':rolling(exact,20),
            'shares':[_float(v.get('shares_decimal')) for v in rows],'nav':[_float(v.get('nav_decimal')) for v in rows],'net_assets':[_float(v.get('net_assets_decimal')) for v in rows]}
    last=rows[-1] if rows else {}
    return {'contract':CONTRACT,'ticker':history['ticker'],'issuer':(history.get('identity') or {}).get('issuer'),'generated_at':packet_generated_at,
        'source':{'history':None,'rows':len(history['rows']),'first_date':history['rows'][0]['date'] if history['rows'] else None,'latest_date':last.get('date')},
        'unit':'USD','method':history.get('method'),'precision_policy':history.get('precision_policy'),'ledger':history.get('ledger'),
        'd':[v['date'] for v in rows],'series':series,
        'latest':{'date':last.get('date'),'flow':flows[-1] if flows else None,'flow_5d':series['flow_5d'][-1] if flows else None,'flow_20d':series['flow_20d'][-1] if flows else None,
                  'shares':series['shares'][-1] if rows else None,'nav':series['nav'][-1] if rows else None}}

def build(output,histories):
    """(key, bytes) feeds for every fund whose history this run retained, plus the index."""
    out={};funds=[]
    for ticker,entry in sorted(output['by_etf'].items()):
        ref=entry.get('history')
        if not ref or ref['key'] not in histories:continue
        history=json.loads(histories[ref['key']])
        doc=feed(history,output['generated_at']);doc['source']['history']={'key':ref['key'],'sha256':ref['sha256']}
        out[PREFIX+ticker+'.json']=json.dumps(doc,separators=(',',':'),allow_nan=False).encode()
        funds.append({'ticker':ticker,'issuer':doc['issuer'],'category':entry.get('category'),'source_status':entry.get('source_status'),'observations':doc['source']['rows'],
            'first_date':doc['source']['first_date'],'latest_date':doc['source']['latest_date'],'flow_5d_usd':entry.get('net_flow_5d_usd'),'flow_20d_usd':entry.get('net_flow_20d_usd'),'feed':PREFIX+ticker+'.json'})
    index={'contract':INDEX_CONTRACT,'generated_at':output['generated_at'],'packet':'data/etf-true-flows.json','funds':funds,'n_funds':len(funds),
        'note':'Daily net issuance at NAV per fund from the retained issuer histories; flows:etfc/<TICKER>.flow_5d on the Inflows & outflows desk.'}
    out[PREFIX+'index.json']=json.dumps(index,separators=(',',':'),allow_nan=False).encode()
    return out

def write(client,bucket,feeds):
    for key,body in sorted(feeds.items()):
        client.put_object(Bucket=bucket,Key=key,Body=body,ContentType='application/json',CacheControl='public, max-age=300')
    return len(feeds)
