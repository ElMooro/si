"""One bounded, nonredirected public request per source; callers claim first."""
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import hashlib, re, urllib.parse, urllib.request, urllib.error
import fifx_catalog as catalog

MAX=8*1024*1024
MAX_CONCURRENT=4
ADMISSION_SECONDS=100


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


def complete(stream, limit=MAX, *, expected_length=None):
    """Read the whole binary response, including fragmented transports."""
    try:
        if type(limit) is not int or limit<=0:raise ValueError('Positive whole-byte bound required')
        if expected_length is not None and (type(expected_length) is not int or not 0<expected_length<=limit):
            raise ValueError('Complete declared artifact length required')
        chunks=[];size=0
        while True:
            requested=min(1024*1024,limit+1-size)
            chunk=stream.read(requested)
            if type(chunk) is not bytes or len(chunk)>requested:raise ValueError('Exact binary response fragment required')
            if not chunk:break
            size+=len(chunk)
            if size>limit:raise ValueError('Complete original exceeds reviewed byte bound')
            chunks.append(chunk)
        if not size or expected_length is not None and size!=expected_length:
            raise ValueError('Complete original differs from declared length')
        return b''.join(chunks)
    finally:stream.close()


def acquire(url,timeout=20):
    if type(timeout) not in (int,float) or not 0<timeout<=20:raise ValueError('Bounded source timeout required')
    request=urllib.request.Request(url,headers={'User-Agent':'Mozilla/5.0 (JustHodl original-source research)',
        'Accept':'application/json,text/csv','Accept-Encoding':'identity'})
    opener=urllib.request.build_opener(NoRedirect)
    try:response=opener.open(request,timeout=timeout)
    except urllib.error.HTTPError as error:response=error
    handed_to_reader=False
    try:
        status=response.code
        if type(status) is not int or not 200<=status<=599 or status==206 or response.geturl()!=url:
            raise ValueError('Exact original request and complete response status required')
        if response.headers.get('Content-Range') is not None or response.headers.get('Content-Encoding','identity').strip().lower() not in ('','identity'):
            raise ValueError('Whole unencoded original response required')
        lengths=response.headers.get_all('Content-Length') if hasattr(response.headers,'get_all') else (
            [response.headers['Content-Length']] if 'Content-Length' in response.headers else [])
        lengths=lengths or []
        if len(lengths)>1 or lengths and (type(lengths[0]) is not str or not re.fullmatch(r'[0-9]+',lengths[0])):
            raise ValueError('One exact original Content-Length required')
        expected=int(lengths[0]) if lengths else None
        headers={k:v for k,v in response.headers.items() if k.lower() in ('date','etag','last-modified','content-type')}
        handed_to_reader=True
        raw=complete(response,expected_length=expected)
    finally:
        if not handed_to_reader:response.close()
    return raw,{'source_url':url,'http_status':status,'headers':headers,'acquired_at':datetime.now(timezone.utc).isoformat(),
        'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}


def collect(source_plan, begin, finish, budget=ADMISSION_SECONDS):
    """Bound concurrent requests; serialize durable begin/result callbacks.

    The budget limits admitting new attempts, not total execution time. Each
    admitted source still has its original <=20-second socket timeout. No
    retries, redirects or extra source are introduced. All worker threads join
    before return, including on a retention failure; no writes outlive a run.
    """
    if type(source_plan) is not dict or not source_plan or any(
            sid not in catalog.SOURCES or sid=='^MOVE' or type(url) is not str
            for sid,url in source_plan.items()):
        raise ValueError('Reviewed unique acquisition plan required')
    if type(budget) not in (int,float) or not 1<budget<=ADMISSION_SECONDS:
        raise ValueError('Bounded acquisition admission budget required')
    deadline=time.monotonic()+budget
    lock=threading.Lock();aborted=threading.Event()
    def deliver(result):
        # Retain before this worker admits another source. Completed response
        # bodies cannot accumulate in an unbounded future-results queue.
        with lock:
            try:finish(*result)
            except Exception:
                aborted.set();raise
    def one(sid,url):
        with lock:
            if aborted.is_set():raise RuntimeError('Acquisition aborted before admission')
            remaining=deadline-time.monotonic()
            expired=remaining<=1
            if not expired:
                try:begin(sid)
                except Exception:
                    aborted.set();raise
        if expired:
            return deliver((sid,None,None,{'status':'acquisition_budget_exhausted'}))
        # Durable intent precedes the request. Callback latency can consume the
        # admission reserve; an already admitted attempt receives a tiny timeout
        # instead of a fresh twenty-second allowance after the deadline.
        timeout=min(20,max(0.001,deadline-time.monotonic()))
        try:raw,receipt=acquire(url,timeout=timeout)
        except Exception as exc:
            return deliver((sid,None,None,{'status':'unavailable','error_type':type(exc).__name__}))
        return deliver((sid,raw,receipt,{'status':'retained_response','http_status':receipt['http_status']}))
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT) as pool:
        futures=[pool.submit(one,sid,url) for sid,url in source_plan.items()]
        try:
            for future in as_completed(futures):
                future.result()
        except Exception:
            aborted.set()
            for future in futures:future.cancel()
            raise
