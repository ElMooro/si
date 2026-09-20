"""Retain bounded official-calendar and public Yahoo futures originals on the runner.

At most five keyless provider requests; no engine invocation, account read or schedule change.
"""
from pathlib import Path
from datetime import datetime,timezone
import base64,gzip,hashlib,io,json,re,subprocess,sys,time,urllib.request,urllib.error,zipfile
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/shared')]
from ops_report import report
from release_package_evidence import shared_imports
import canonical_fred_replay

FN='justhodl-fedwatch-rate-probability';BUCKET='justhodl-dashboard-live'
PREFIX='audit-private/20260909-originals/fedwatch-research/'
PACKETS=('data/fedwatch.json','data/report-measurements.json')
SERIES=('DFEDTARU','DFEDTARL','DFF','FEDFUNDS')

def sha(raw):return hashlib.sha256(raw).hexdigest()
def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def bounded(stream,limit=32*1024*1024):
    try:raw=stream.read(limit+1)
    finally:stream.close()
    assert len(raw)<=limit,'Reviewed byte bound exceeded'
    return raw
def code(exc):return getattr(exc,'response',{}).get('Error',{}).get('Code')
def retain(s3,raw):
    key=PREFIX+sha(raw)+'.bin'
    try:s3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if code(exc) not in ('PreconditionFailed','ConditionalRequestConflict','409','412'):raise
    assert bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])==raw
    return {'key':key,'sha256':sha(raw),'bytes':len(raw)}
def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD',headers={'User-Agent':'justhodl-verify-release/1.0'}),timeout=25):return False
    except urllib.error.HTTPError as exc:return exc.code in (401,403,404)

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None

def capture(s3,url):
    started=datetime.now(timezone.utc).isoformat();request=urllib.request.Request(url,headers={'User-Agent':'JustHodl-Research/1.0','Accept':'application/json,text/html'})
    opener=urllib.request.build_opener(NoRedirect)
    try:
        with opener.open(request,timeout=20) as response:raw=bounded(response,2*1024*1024);status=response.status;kind=response.headers.get('Content-Type','')
    except urllib.error.HTTPError as exc:raw=bounded(exc,2*1024*1024);status=exc.code;kind=exc.headers.get('Content-Type','')
    return {'request_url':url,'started_at':started,'received_at':datetime.now(timezone.utc).isoformat(),
        'http_status':status,'content_type':kind,'original':retain(s3,raw)},raw

def main():
    s3=boto3.client('s3',region_name='us-east-1')
    with report('ops_5954_fedwatch_quote_preflight') as r:
        started=datetime.now(timezone.utc);pages={};summary={}
        url='https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'
        pages['calendar'],raw=capture(s3,url)
        r.kv(captured_source='calendar',original=pages['calendar']['original'],http_status=pages['calendar']['http_status'])
        summary['calendar']={'http_status':pages['calendar']['http_status'],'bytes':len(raw),
            'meeting_years':sorted(set(re.findall(r'(20[0-9]{2}) FOMC Meetings',raw.decode('utf-8','replace')))),
            'has_month_class':b'fomc-meeting__month' in raw,'has_date_class':b'fomc-meeting__date' in raw}
        months='FGHJKMNQUVXZ';at=started.year*12+started.month-1
        for offset in range(4):
            value=at+offset;year=value//12;month=value%12+1;symbol=f'ZQ{months[month-1]}{year%100:02d}.CBT'
            url='https://query1.finance.yahoo.com/v8/finance/chart/'+symbol+'?range=10d&interval=1d'
            page,raw=capture(s3,url);pages[symbol]=page;item={'http_status':page['http_status'],'bytes':len(raw),'requested_month':f'{year}-{month:02d}'}
            r.kv(captured_source=symbol,original=page['original'],http_status=page['http_status'])
            if page['http_status']==200:
                doc=json.loads(raw);result=(doc.get('chart',{}).get('result') or [])
                if result:
                    assert len(result)==1
                    record=result[0];meta=record.get('meta',{});timestamps=record.get('timestamp',[])
                    quotes=(record.get('indicators',{}).get('quote') or [{}]);assert len(quotes)==1
                    q=quotes[0];closes=q.get('close',[])
                    item.update(metadata={k:meta.get(k) for k in ('symbol','currency','exchangeName','fullExchangeName','instrumentType','firstTradeDate','regularMarketTime','exchangeTimezoneName','shortName','longName','dataGranularity','range','expireDate','regularMarketPrice')},
                        timestamps=timestamps,quote_fields=sorted(q),quote_array_lengths={k:len(v) for k,v in q.items() if isinstance(v,list)},
                        dated_closes=[{'index':i,'timestamp':timestamps[i] if i<len(timestamps) else None,'close':v,'volume':q.get('volume',[])[i] if i<len(q.get('volume',[])) else None} for i,v in enumerate(closes)],
                        adjusted_close_present=bool(record.get('indicators',{}).get('adjclose')))
                else:item['result_present']=False
            summary[symbol]=item
            if page['http_status'] in (401,403,429):
                summary['further_quote_requests_stopped']='Provider access or rate limit response; no alternate endpoint or authentication workaround'
                break
            time.sleep(1)
        manifest={'contract':'fedwatch-quote-preflight.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'started_at':started.isoformat(),'pages':pages,'coverage':summary,'engine_invocations':0,'private_account_reads':0,
            'paid_ai_calls':0,'notifications_sent':0,'portfolio_writes':0,'provider_requests':len(pages),
            'meaning':'Original source capture only. Daily close is not an official exchange settlement or executable quote; calendar end date is not independently verified policy effective time.'}
        ref=retain(s3,encoded(manifest))
        for item in [ref,*[v['original'] for v in pages.values()]]:
            assert denied('https://justhodl.ai/'+item['key']) and denied('https://'+BUCKET+'.s3.amazonaws.com/'+item['key'])
        r.kv(retained_manifest=ref,coverage=summary,provider_requests=len(pages),engine_invocations=0,private_account_reads=0,
            paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,originals_anonymously_denied=True)

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
