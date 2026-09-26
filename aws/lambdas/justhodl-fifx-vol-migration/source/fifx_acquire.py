"""One bounded, nonredirected public request per source; callers claim first."""
from datetime import datetime, timezone
import hashlib, urllib.parse, urllib.request, urllib.error
import fifx_catalog as catalog

MAX=8*1024*1024


def plan(stamp):
    clock=datetime.fromisoformat(stamp.replace('Z','+00:00'))
    if clock.tzinfo is None:raise ValueError('Acquisition timezone required')
    date=clock.astimezone(timezone.utc).date().isoformat()
    out={sid:'https://fred.stlouisfed.org/graph/fredgraph.csv?'+urllib.parse.urlencode(
        {'id':sid,'cosd':'1988-01-01','coed':date}) for sid in catalog.FRED}
    for sid in catalog.QUOTES[1:]:
        params={'range':'2y','interval':'1d'} if sid=='^VHSI' else {'period1':315532800,'period2':int(clock.timestamp()),'interval':'1d'}
        out[sid]='https://query1.finance.yahoo.com/v8/finance/chart/'+urllib.parse.quote(sid,safe='')+'?'+urllib.parse.urlencode(params)
    return out


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None


def acquire(url,timeout=20):
    if not 0<timeout<=20:raise ValueError('Bounded source timeout required')
    request=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0 (JustHodl original-source research)','Accept':'application/json,text/csv'})
    opener=urllib.request.build_opener(NoRedirect)
    try:response=opener.open(request,timeout=timeout)
    except urllib.error.HTTPError as error:response=error
    try:
        raw=response.read(MAX+1);status=response.code
        headers={k:v for k,v in response.headers.items() if k.lower() in ('date','etag','last-modified','content-type')}
    finally:response.close()
    if not 0<len(raw)<=MAX:raise ValueError('Complete bounded original required')
    return raw,{'source_url':url,'http_status':status,'headers':headers,'acquired_at':datetime.now(timezone.utc).isoformat(),
        'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}
