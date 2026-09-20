"""Bounded market collection, original-response replay and public-only refresh."""
import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import time
import urllib.request
from urllib.parse import urlencode
from evidence_store import capture, read_verified
from fred_level_io import body, immutable, publish
import daily_market_model
import price_model as model

COMPILERS = {'price_model.py':model,'daily_market_model.py':daily_market_model}


def reconstruct(reference, read, check_compilers=True):
    def verified(ref):
        raw = read(ref['key'])
        if len(raw)!=ref['bytes'] or model.digest(raw)!=ref['sha256']: raise ValueError('retained bytes differ')
        return raw
    manifest = json.loads(verified(reference))
    if manifest.get('contract')!='native-market-replay.v1' or set(manifest['compilers']) != set(COMPILERS):
        raise ValueError('reviewed replay contract required')
    for name, module in COMPILERS.items():
        raw = verified(manifest['compilers'][name])
        if check_compilers and raw != Path(module.__file__).read_bytes(): raise ValueError('matching reviewed compiler required')
    sources = copy.deepcopy(manifest['sources'])
    for source in sources.values(): source['response'] = json.loads(verified(source['evidence']))
    out = model.compile_observation(manifest['provider'],manifest['symbol'],sources,manifest['compiled_at'])
    if model.digest(model.encoded(out)) != manifest['output_sha256']: raise ValueError('source replay differs')
    return {**out,'evidence':{k:s['evidence'] for k,s in sources.items()},'replay':reference}


class Collector:
    def __init__(self,s3,bucket,fmp_key='',polygon_key='',now=None,opener=None,context=None,clock=None):
        self.s3,self.bucket,self.fmp_key,self.polygon_key = s3,bucket,fmp_key,polygon_key
        self.now = now or datetime.now(timezone.utc)
        self.clock = clock or (lambda:datetime.now(timezone.utc))
        self.opener = opener or urllib.request.urlopen
        self.context,self.started = context,time.monotonic()
        self.memo,self.failures,self.seeds,self.quotes = {},{},{},{}
        self.requests,self.reused,self.compilers = 0,set(),None

    def allowed(self):
        return self.requests < 96 and time.monotonic()-self.started < 420 and (self.context is None or self.context.get_remaining_time_in_millis()>=90000)

    def seed(self,rows):
        for row in rows:
            if row.get('contract_version')==model.CONTRACT and isinstance(row.get('replay'),dict):
                self.seeds[(row.get('provider'),row.get('provider_symbol'))] = row

    def retained(self,row):
        def read(key):
            if key.startswith('data/evidence/'):
                import gzip
                return gzip.decompress(body(self.s3.get_object(Bucket=self.bucket,Key=key)))
            if not key.startswith(model.PREFIX): raise ValueError('native public reference required')
            return body(self.s3.get_object(Bucket=self.bucket,Key=key))
        out = reconstruct(row['replay'],read)
        age = (self.now-model.clock(out['fetched_at'])).total_seconds()
        observed = (self.now-model.clock(out['age_reference_at'])).total_seconds()
        if not 0 <= age < 3600 or not 0 <= observed <= model.MAX_AGE: raise ValueError('cached quote expired')
        return out

    def fetch(self,provider,symbol,kind,request):
        if not self.allowed(): raise TimeoutError('bounded source collection exhausted')
        url = model.source_url(provider,symbol,kind,request)
        actual = url
        if provider=='fmp':
            if not self.fmp_key: raise ValueError('existing FMP credential unavailable')
            actual += '&'+urlencode({'apikey':self.fmp_key})
        if provider=='polygon':
            if not self.polygon_key: raise ValueError('existing Polygon credential unavailable')
            actual += ('&' if '?' in actual else '?')+urlencode({'adjusted':'true','apiKey':self.polygon_key})
        self.requests += 1
        req = urllib.request.Request(actual,headers={'User-Agent':'Mozilla/5.0' if provider=='yahoo' else 'JustHodl-native-prices/1.0'})
        with self.opener(req,timeout=15) as response: raw=response.read(4*1024*1024+1)
        if len(raw)>4*1024*1024: raise ValueError('source exceeds bound')
        acquired = self.clock()
        evidence = capture(self.s3,self.bucket,provider,actual,raw,acquired)
        if read_verified(self.s3,self.bucket,evidence)!=raw: raise ValueError('original readback differs')
        return {'response':json.loads(raw),'request':request,'evidence':evidence,'acquired_at':acquired.isoformat()}

    def prefetch_fmp(self,symbols):
        symbols = sorted(set(symbols))
        for offset in range(0,len(symbols),40):
            chunk = symbols[offset:offset+40]
            if not self.allowed(): break
            try:
                source = self.fetch('fmp',chunk[0],'quote',{'symbols':chunk})
                for symbol in chunk: self.quotes[symbol] = source
            except Exception as exc:
                for symbol in chunk: self.failures['fmp:'+symbol]=type(exc).__name__

    def get(self,provider,symbol,allowed=True):
        identity = (provider,symbol)
        if identity in self.memo: return copy.deepcopy(self.memo[identity])
        self.memo[identity]=None
        if identity in self.seeds:
            try:
                out=self.retained(self.seeds[identity])
                if (out['provider'],out['provider_symbol']) != identity: raise ValueError('seed identity differs')
                self.memo[identity]=out;self.reused.add(identity);return copy.deepcopy(out)
            except Exception: pass
        try:
            if not allowed or not self.allowed(): raise TimeoutError('bounded source collection exhausted')
            if provider=='yahoo': sources={'bars':self.fetch(provider,symbol,'bars',{'range':'5d','interval':'1d'})}
            elif provider=='fmp':
                quote=self.quotes.get(symbol) or self.fetch(provider,symbol,'quote',{'symbols':[symbol]})
                sources={'quote':quote,'profile':self.fetch(provider,symbol,'profile',{'symbol':symbol})}
            elif provider=='polygon':
                end=self.clock().astimezone(daily_market_model.ET).date()-timedelta(days=1)
                request={'symbol':symbol,'start':(end-timedelta(days=14)).isoformat(),'end':end.isoformat(),
                         'multiplier':1,'timespan':'day','adjusted':True,'sort':'desc','limit':50000}
                sources={'bars':self.fetch(provider,symbol,'bars',request)}
                if sources['bars']['response'].get('resultsCount')==0:
                    sources['previous']=self.fetch(provider,symbol,'previous',{'adjusted':True})
            else: raise ValueError('unsupported market provider')
            compiled_at=self.clock().isoformat()
            out=model.compile_observation(provider,symbol,sources,compiled_at)
            if self.compilers is None:
                self.compilers={}
                for name,module in COMPILERS.items():
                    raw=Path(module.__file__).read_bytes()
                    self.compilers[name]=immutable(self.s3,self.bucket,model.PREFIX+'compilers/'+model.digest(raw)+'.py',raw)
            manifest={'contract':'native-market-replay.v1','provider':provider,'symbol':symbol,'compiled_at':compiled_at,
                'sources':{k:{field:value for field,value in s.items() if field!='response'} for k,s in sources.items()},
                'compilers':self.compilers,'output_sha256':model.digest(model.encoded(out))}
            raw=model.encoded(manifest)
            ref=immutable(self.s3,self.bucket,model.PREFIX+'runs/'+model.digest(raw)+'.json',raw)
            out.update(evidence={k:s['evidence'] for k,s in sources.items()},replay=ref)
            self.memo[identity]=out;self.failures.pop(':'.join(identity),None)
            return copy.deepcopy(out)
        except Exception as exc:
            self.failures[':'.join(identity)]=type(exc).__name__;return None


def refresh_public(s3,bucket,key,collector,aliases,selected=None):
    response=s3.get_object(Bucket=bucket,Key=key);raw=body(response);packet=json.loads(raw)
    rows=packet['symbols'];groups={}
    for row in rows:
        identity=model.identity_for(row,aliases)
        if identity: groups.setdefault(identity,[]).append(row)
    if selected is not None:
        if not isinstance(selected,list) or not 1<=len(selected)<=64 or any(not isinstance(s,str) for s in selected):
            raise ValueError('select at most 64 existing public provider identities')
        wanted={':'.join(k):k for k in groups}
        if any(s not in wanted for s in selected): raise ValueError('unknown public provider identity')
        groups={wanted[s]:groups[wanted[s]] for s in sorted(set(selected))}
    else:
        def attempted(identity):
            clocks=[]
            for row in groups[identity]:
                try:
                    dt=model.clock(row['native_price_attempted_at'])
                    if dt<=collector.now: clocks.append(dt.isoformat())
                except (KeyError,ValueError,TypeError): pass
            return max(clocks) if clocks else ''
        priority={'AAPL','MSFT','NVDA','SPY','QQQ','BTCUSD','ETHUSD','DXY','GOLD','MOVE','000001'}
        urgent=[k for k,v in groups.items() if any(r['symbol'] in priority for r in v)]
        chosen=list(dict.fromkeys(urgent+sorted(groups,key=lambda k:(attempted(k),k))))[:64]
        groups={k:groups[k] for k in chosen}
    collector.prefetch_fmp([s for p,s in groups if p=='fmp'])
    updated=[]
    for identity,subset in groups.items():
        observation=collector.get(*identity)
        for row in subset:
            if observation:model.merge_observation(row,observation,aliases)
            else:model.mark_unavailable(row,identity)
            row['native_price_attempted_at']=collector.now.isoformat()
        if observation: updated.append(':'.join(identity))
    packet['price_observation_refresh']={'contract':model.CONTRACT,'generated_at':collector.clock().isoformat(),
        'updated_instruments':updated,'failures':collector.failures,'source_requests':collector.requests,
        'scope':'Selected public market observations only; catalog collection and other observation clocks unchanged'}
    counts={}
    for row in rows: counts[row['status']]=counts.get(row['status'],0)+1
    packet.update(status_counts=counts,n_live=counts.get('LIVE',0),coverage_pct=round(100*counts.get('LIVE',0)/max(1,len(rows)),1))
    publish(s3,bucket,key,packet,raw,response['ETag'])
    return {**packet['price_observation_refresh'],'ok':not collector.failures,'updated_rows':sum(len(groups[k]) for k in groups if ':'.join(k) in updated)}
