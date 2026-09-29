"""Actual auction-local transport on isolated synthetic HTTP objects only."""
from contextlib import contextmanager,ExitStack
from datetime import datetime,timezone
from pathlib import Path
from unittest.mock import patch
import hashlib,io,json,runpy,time,urllib.error,urllib.request
ROOT=Path(__file__).resolve().parents[1]
SOURCE=ROOT/'aws/lambdas/justhodl-auction-crisis-detector/source/_fred_shim.py'
URL='https://api.stlouisfed.org/fred/series/observations?series_id=SOFR&api_key=SYNTHETIC_SECRET&file_type=json&limit=5&sort_order=desc'
RAW=b'{"observations":[{"date":"2026-09-29","value":"4.95"}]}'
EPOCH=datetime(2026,9,29,7,tzinfo=timezone.utc).timestamp()


class Response(io.BytesIO):
    def __init__(self,raw=RAW,url=URL,status=200,headers=None):
        super().__init__(raw);self.url=url;self.status=status;self.read_sizes=[]
        self.headers={'Content-Length':str(len(raw))} if headers is None else headers
    def geturl(self):return self.url
    def read(self,size=-1):self.read_sizes.append(size);return super().read(size)


@contextmanager
def installed(upstream):
    with ExitStack() as stack:
        mock=stack.enter_context(patch.object(urllib.request,'urlopen',side_effect=upstream))
        stack.enter_context(patch.object(urllib.request,'_jh_fred_shim_installed',False,create=True))
        stack.enter_context(patch.object(time,'sleep'))
        scope=runpy.run_path(str(SOURCE))
        yield scope,mock


def unavailable(scope,call):
    try:call()
    except scope['FredResponseUnavailable'] as exc:
        assert 'SYNTHETIC_SECRET' not in str(exc) and 'api_key' not in str(exc)
        return str(exc)
    raise AssertionError('Invalid transport supplied a successful body')


def test_partial_http_response_is_not_relabelled_as_success_or_retried():
    response=Response(status=206)
    with installed(lambda *a,**k:response) as (scope,mock):
        assert 'partial' in unavailable(scope,lambda:urllib.request.urlopen(URL))
        assert mock.call_count==1 and response.closed and not scope['_CACHE'] and response.read_sizes==[]


def test_declared_length_mismatch_is_rejected_and_response_closed():
    for size in (len(RAW)-1,len(RAW)+1):
        response=Response(headers={'Content-Length':str(size)})
        with installed(lambda *a,**k:response) as (scope,mock):
            assert 'incomplete' in unavailable(scope,lambda:urllib.request.urlopen(URL))
            assert mock.call_count==1 and response.closed and not scope['_CACHE']


def test_size_bound_applies_to_the_original_upstream_read():
    response=Response(raw=b' '*(2*1024*1024+2),headers={})
    with installed(lambda *a,**k:response) as (scope,mock):
        unavailable(scope,lambda:urllib.request.urlopen(URL))
        assert response.read_sizes==[scope['_MAX_BODY']+1] and response.closed and mock.call_count==1


def test_invalid_content_headers_empty_and_non_json_bodies_do_not_enter_cache():
    cases=[(RAW,{'Content-Length':'-1'}),(RAW,{'Content-Length':'nan'}),(RAW,{'Content-Length':False}),
           (RAW,{'Content-Encoding':'gzip'}),(b'',{}),(b'<html>error</html>',{}),(b'{}',{}),
           (b'{"observations":[],"observations":[]}',{}),(b'{"observations":[],"x":NaN}',{}),
           (b'{"observations":[],"x":1e999}',{})]
    for raw,headers in cases:
        response=Response(raw,headers=headers)
        with installed(lambda *a,**k:response) as (scope,mock):
            unavailable(scope,lambda:urllib.request.urlopen(URL))
            assert not scope['_CACHE'] and mock.call_count==1 and response.closed


def test_redirected_response_cannot_borrow_request_identity():
    response=Response(url='https://example.invalid/not-the-source')
    with installed(lambda *a,**k:response) as (scope,mock):
        assert 'identity' in unavailable(scope,lambda:urllib.request.urlopen(URL))
        assert mock.call_count==1 and response.closed and not scope['_CACHE']


def test_fresh_body_has_standard_bounded_cursor_and_close_semantics():
    response=Response()
    with installed(lambda *a,**k:response) as (scope,mock),patch.object(time,'time',return_value=EPOCH):
        reader=urllib.request.urlopen(URL)
        assert reader.read(1)==RAW[:1] and reader.read()==RAW[1:] and reader.read()==b''
        assert reader.getcode()==200 and reader.geturl()==URL and response.closed
        reader.close()
        try:reader.read()
        except ValueError:pass
        else:raise AssertionError('Closed file remained readable')


def test_cache_hits_preserve_original_time_hash_and_independent_read_cursor():
    with installed(lambda *a,**k:Response()) as (scope,mock):
        with patch.object(time,'time',return_value=EPOCH):first=urllib.request.urlopen(URL)
        first.jh_fred_transport['body_sha256']='corrupted-consumer-copy';first.read(3)
        with patch.object(time,'time',return_value=EPOCH+300):cached=urllib.request.urlopen(URL)
        info=cached.jh_fred_transport
        assert mock.call_count==1 and cached.read()==RAW and info['cache_status']=='fresh_cache'
        assert info['cache_age_seconds']==300 and info['response_received_at']==datetime.fromtimestamp(EPOCH,timezone.utc).isoformat()
        assert info['body_sha256']==hashlib.sha256(RAW).hexdigest() and info['body_bytes']==len(RAW)
        assert 'SYNTHETIC_SECRET' not in json.dumps(info) and 'api_key' not in json.dumps(info)


def test_expired_cache_after_existing_four_attempts_is_explicitly_withheld():
    with installed([Response(),*[TimeoutError('api_key=SYNTHETIC_SECRET') for _ in range(4)]]) as (scope,mock):
        with patch.object(time,'time',return_value=EPOCH):urllib.request.urlopen(URL).close()
        with patch.object(time,'time',return_value=EPOCH+86400):
            assert 'expired_cache_withheld' in unavailable(scope,lambda:urllib.request.urlopen(URL))
        assert mock.call_count==5 and scope['_CACHE'][URL][1]==RAW


def test_transient_attempt_limit_and_existing_backoff_are_unchanged():
    with installed([urllib.error.HTTPError(URL,429,'retry',{},None) for _ in range(4)]) as (scope,mock):
        assert 'attempts_exhausted' in unavailable(scope,lambda:urllib.request.urlopen(URL))
        assert mock.call_count==4 and [args.args[0] for args in time.sleep.call_args_list]==[0.6,1.2,2.4,4.8]


def test_nonretryable_failure_is_sanitized_and_does_not_trigger_more_requests():
    with installed([urllib.error.HTTPError(URL,400,'api_key=SYNTHETIC_SECRET',{},None)]) as (scope,mock):
        assert 'nonretryable' in unavailable(scope,lambda:urllib.request.urlopen(URL))
        assert mock.call_count==1 and time.sleep.call_count==0


def test_expired_cache_refresh_and_clock_rollback_do_not_refresh_old_metadata():
    for delta in (1800,-1):
        newer=b'{"observations":[{"date":"2026-09-29","value":"5"}]}'
        with installed([Response(),Response(newer)]) as (scope,mock):
            with patch.object(time,'time',return_value=EPOCH):urllib.request.urlopen(URL).close()
            with patch.object(time,'time',return_value=EPOCH+delta):result=urllib.request.urlopen(URL)
            assert mock.call_count==2 and result.read()==newer and result.jh_fred_transport['cache_status']=='network'
            assert result.jh_fred_transport['response_received_at']==datetime.fromtimestamp(EPOCH+delta,timezone.utc).isoformat()


def test_other_hosts_and_endpoints_are_not_intercepted():
    for url in ('https://api.stlouisfed.org.evil.invalid/fred/series/observations',
                'https://example.invalid/?host=api.stlouisfed.org','https://api.stlouisfed.org/fred/series'):
        marker=object()
        with installed(lambda *a,**k:marker) as (scope,mock):
            assert urllib.request.urlopen(url) is marker and mock.call_count==1 and not scope['_CACHE']


def test_installation_is_idempotent():
    with installed(lambda *a,**k:Response()) as (scope,mock):
        original=urllib.request.urlopen;scope['_install']();assert urllib.request.urlopen is original


def test_acquisition_clock_cannot_reverse_inside_a_response():
    response=Response()
    with installed(lambda *a,**k:response) as (scope,mock),patch.object(time,'time',side_effect=[EPOCH,EPOCH,EPOCH-1]):
        assert 'clock_reversed' in unavailable(scope,lambda:urllib.request.urlopen(URL))
        assert mock.call_count==1 and response.closed


def engine():return runpy.run_path(str(ROOT/'tests/test_auction_cross_observations.py'))['engine']()


def test_actual_native_adapter_retains_body_bound_transport_record_without_promoting_authority():
    m=engine()
    with installed(lambda url,**kw:Response(url=url)) as (scope,mock):
        frame=m._fred_cross_source('SOFR',5)
    assert frame['read_status']=='received' and frame['response']['observations'][0]['value']=='4.95'
    assert frame['adapter_transport']['cache_status']=='network' and frame['adapter_transport']['body_sha256']==hashlib.sha256(RAW).hexdigest()
    assert frame['http_acquisition_time_verified'] is False and mock.call_count==1


def test_actual_native_adapter_cannot_accept_corrupt_expired_or_promoted_transport_records():
    m=engine()
    for mutation in ('hash','bytes','url','age','status','clock'):
        with installed(lambda url,**kw:Response(url=url)) as (scope,mock):
            current=m._fred_cross_source('SOFR',5);metadata=current['adapter_transport']
        if mutation=='hash':metadata['body_sha256']='0'*64
        elif mutation=='bytes':metadata['body_bytes']=False
        elif mutation=='url':metadata['source_url']='https://example.invalid/'
        elif mutation=='age':metadata['cache_age_seconds']=1800
        elif mutation=='status':metadata['cache_status']='stale_fallback'
        else:metadata['response_received_at']='2100-01-01T00:00:00Z'
        response=Response();response.jh_fred_transport=metadata
        with patch.object(m.urllib.request,'urlopen',return_value=response):frame=m._fred_cross_source('SOFR',5)
        assert frame['read_status']=='unavailable' and frame['response'] is None


def test_native_source_returns_unavailable_after_refresh_failure_without_reusing_old_values():
    m=engine();at=time.time()
    with installed([Response(url=URL.replace('SYNTHETIC_SECRET',m.FRED_KEY)),*[TimeoutError('synthetic') for _ in range(4)]]) as (scope,mock):
        with patch.object(time,'time',return_value=at):first=m._fred_cross_source('SOFR',5)
        with patch.object(time,'time',return_value=at+86400):second=m._fred_cross_source('SOFR',5)
    assert first['read_status']=='received' and second['read_status']=='unavailable' and second['response'] is None
    assert second['transport_error']=='fred_refresh_failed_expired_cache_withheld'
    assert mock.call_count==5


def test_native_cache_uses_one_clock_conversion_and_never_replaces_acquisition_with_read_time():
    m=engine()
    class DifferentClockPrecision(datetime):
        @classmethod
        def now(cls,tz=None):return cls.fromtimestamp(EPOCH-0.000001,tz)
    with installed(lambda url,**kw:Response(url=url)) as (scope,mock),patch.object(m,'datetime',DifferentClockPrecision):
        with patch.object(time,'time',return_value=EPOCH):first=m._fred_cross_source('SOFR',5)
        with patch.object(time,'time',return_value=EPOCH+300):cached=m._fred_cross_source('SOFR',5)
    assert first['read_status']==cached['read_status']=='received' and mock.call_count==1
    assert first['adapter_transport']['response_received_at']==cached['adapter_transport']['response_received_at']
    assert cached['adapter_transport']['cache_age_seconds']==300
    assert cached['adapter_read_at']!=cached['adapter_transport']['response_received_at']


if __name__=='__main__':
    tests=[fn for name,fn in list(globals().items()) if name.startswith('test_')]
    for test in tests:test()
    print('Auction FRED transport regressions passed:',len(tests))
