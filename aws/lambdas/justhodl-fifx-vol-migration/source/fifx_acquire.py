"""One bounded, nonredirected public request per source; callers claim first."""
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import time
import hashlib, urllib.parse, urllib.request, urllib.error
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
