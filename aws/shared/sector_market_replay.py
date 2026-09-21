"""Reconstruct selected daily market sources without executing downloaded code.

A source timestamp or the daily report's envelope alone is insufficient. This
reader binds the reviewed compilers, immutable run/input identities, exact
provider request and original bytes, then compares the published market row.
Unrelated daily report fields remain outside this selected-source proof.
"""
from copy import deepcopy
from pathlib import Path
from urllib.parse import quote
import hashlib,json,re
import daily_market_model
import daily_macro_model
from evidence_store import public_source_url

PREFIX='data/daily-research/';MAX=32*1024*1024

def sha(raw):return hashlib.sha256(raw).hexdigest()

def checked_json(ref,category,read):
    digest=ref.get('sha256','')
    if (not re.fullmatch('[a-f0-9]{64}',digest) or ref.get('key')!=PREFIX+category+'/'+digest+'.json'
        or type(ref.get('bytes')) is not int or not 0<ref['bytes']<=MAX):raise ValueError('Daily source artifact reference differs')
    raw=read(ref['key'])
    if not isinstance(raw,bytes) or len(raw)!=ref['bytes'] or sha(raw)!=digest:raise ValueError('Daily source artifact bytes differ')
    return json.loads(raw)


def pin(packet,read):
    if not isinstance(packet,dict) or packet.get('contract')!=daily_macro_model.CONTRACT:raise ValueError('Canonical daily market report required')
    replay=packet.get('replay') or {};key=replay.get('manifest_key','')
    if not re.fullmatch(re.escape(PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('Daily source run identity required')
    raw=read(key)
    if not isinstance(raw,bytes) or len(raw)>MAX or key!=PREFIX+'runs/'+sha(raw)+'.json':raise ValueError('Daily source run bytes differ')
    manifest=json.loads(raw)
    if manifest.get('contract')!='daily-research-replay.v1' or manifest.get('generated_at')!=packet.get('generated_at'):raise ValueError('Daily source run contract or clock differs')
    daily_market_model.clock(packet['generated_at'])
    if manifest.get('output_sha256')!=replay.get('output_sha256'):raise ValueError('Daily output reference differs')
    for module in (daily_market_model,daily_macro_model):
        body=Path(module.__file__).read_bytes();digest=sha(body)
        ref={'key':PREFIX+'compilers/'+digest+'.py','sha256':digest}
        if manifest.get('compilers',{}).get(module.__name__)!=ref or replay.get('compilers',{}).get(module.__name__)!=ref or read(ref['key'])!=body:raise ValueError('Matching reviewed market compiler required')
    inputs=checked_json(manifest['input'],'inputs',read)
    sources=inputs.get('auxiliary',{}).get('market_sources')
    if not isinstance(sources,dict) or sources.get('contract')!=daily_market_model.CONTRACT:raise ValueError('Daily market original-source contract required')
    universe=sources.get('universe')
    if not isinstance(universe,list) or len(universe)!=len(set(universe)):raise ValueError('Unique declared daily universe required')
    if daily_market_model.clock(inputs['auxiliary']['collected_at'])>daily_market_model.clock(packet['generated_at']):raise ValueError('Future daily collection')
    return manifest,inputs,sources


def original(source,symbol,read):
    evidence=source.get('evidence') or {};request=source.get('request') or {}
    if request.get('symbol')!=symbol:raise ValueError('Market source symbol differs')
    # The reviewed aggregate compiler separately validates all request fields,
    # calendar bounds, row dates, finite OHLCV, count and pagination semantics.
    url='https://api.polygon.io/v2/aggs/ticker/'+quote(symbol,safe='')+'/range/1/day/'+str(request.get('start'))+'/'+str(request.get('end'))+'?adjusted=true&sort=desc&limit=50000'
    url=public_source_url(url)
    digest=evidence.get('sha256','')
    if (evidence.get('contract')!='source-evidence.v1' or evidence.get('provider')!='polygon' or evidence.get('captured') is not True
        or evidence.get('source_url')!=public_source_url(url) or not re.fullmatch('[a-f0-9]{64}',digest)
        or evidence.get('key')!='data/evidence/polygon/'+sha(url.encode())+'/'+digest+'.bin.gz'
        or type(evidence.get('bytes')) is not int or not 0<evidence['bytes']<=MAX):raise ValueError('Market original request identity differs')
    raw=read(evidence['key'])
    if not isinstance(raw,bytes) or len(raw)!=evidence['bytes'] or sha(raw)!=digest or json.loads(raw)!=source.get('response'):raise ValueError('Market original bytes differ')
    return deepcopy(source)


def restore(packet,symbols,read):
    symbols=tuple(symbols)
    if (not symbols or len(symbols)>100 or len(symbols)!=len(set(symbols))
        or any(not isinstance(s,str) or not re.fullmatch(r'[A-Z][A-Z.\-]{0,6}',s) for s in symbols)):raise ValueError('Reviewed unique symbol list required')
    manifest,inputs,sources=pin(packet,read);out={}
    for symbol in symbols:
        source=sources.get('equities',{}).get(symbol)
        if source is None:out[symbol]=None;continue
        if symbol not in sources['universe']:raise ValueError('Market source outside declared universe')
        source=original(source,symbol,read);row=daily_market_model.equity(source,manifest['generated_at'])
        projected={**row,**{field:None for field in daily_macro_model.STOCK_AUTHORITY},'source_collected_at':inputs['auxiliary']['collected_at']}
        if packet.get('stocks',{}).get(symbol)!=projected:raise ValueError('Published market observation differs from original reconstruction: '+symbol)
        out[symbol]=source
    return out
