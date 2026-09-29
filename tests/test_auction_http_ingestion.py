"""Synthetic HTTP and pagination faults must not become successful empty research."""
from pathlib import Path
from contextlib import redirect_stdout
from copy import deepcopy
from unittest.mock import patch
import io,json,runpy,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
support=runpy.run_path(str(ROOT/'tests/deployment/test_auction_output_ownership.py'))

def loaded():
    store=support['Store']();scope=support['load']('auction-desk',store)
    return store,scope,scope['lambda_handler'].__globals__

def rejects(fn):
    try:fn()
    except (ValueError,OSError,TimeoutError) as error:return error
    raise AssertionError('Incomplete or ambiguous HTTP input accepted')

def page(rows,total=None,pages=1,**extras):
    return {'data':rows,'meta':{'total-count':len(rows) if total is None else total,'total-pages':pages,**extras}}

def test_all_fiscal_callers_require_pagination_even_without_a_proof_collector():
    for endpoint in ('auctions_query','buybacks_operations','buybacks_security_details'):
        for body in ([],{},None,{'error':'synthetic failure'}, {'data':[], 'meta':{}},
                     {'data':False,'meta':{'total-count':0,'total-pages':1}},page([{}],4,4)):
            store,scope,env=loaded();env['_get_json']=lambda *a,body=body,**k:body
            rejects(lambda:scope['fetch_fd'](endpoint,{'page[size]':1},max_pages=3))
            assert not store.writes

def test_complete_pages_preserve_whole_rows_duplicates_unknown_fields_zero_and_null():
    store,scope,env=loaded();row={'accepted':0,'missing':None,'unknown':{'whole':['retain']}}
    bodies=iter([page([row,row],3,2,count=2),page([{'last':True}],3,2,count=1)]);urls=[]
    env['_get_json']=lambda url:(urls.append(url) or next(bodies));proof=[]
    result=scope['fetch_fd']('buybacks_operations',{'page[size]':2,'sort':'-operation_date'},proof_out=proof)
    assert result==[row,row,{'last':True}] and len(urls)==2
    assert 'page[number]=1' in urls[0] and 'page[number]=2' in urls[1]
    assert all('page[size]=2' in url and 'sort=-operation_date' in url for url in urls)
    assert proof==[{'rows':3,'pages':2,'complete':True}]

def test_explicit_empty_population_is_valid_but_inconsistent_empty_metadata_is_not():
    store,scope,env=loaded();env['_get_json']=lambda *a:page([]);proof=[]
    assert scope['fetch_fd']('buybacks_operations',{},proof_out=proof)==[] and proof==[{'rows':0,'pages':1,'complete':True}]
    for body in (page([],1,1),page([],0,2),page([],False,1),page([],0,True),page([],0,1,count=False)):
        env['_get_json']=lambda *a,body=body:body;rejects(lambda:scope['fetch_fd']('buybacks_operations',{}))

def test_later_page_fault_drift_short_page_or_bad_count_never_emits_partial_proof():
    for second in (OSError('synthetic interrupted page'),page([],2,2),page([{}],3,3),page([None],2,2),page([{}],2,2,count=0)):
        store,scope,env=loaded();sequence=iter([page([{'first':0}],2,2),second]);proof=[]
        def read(url):
            body=next(sequence)
            if isinstance(body,Exception):raise body
            return body
        env['_get_json']=read;rejects(lambda:scope['fetch_fd']('buybacks_security_details',{'page[size]':1},proof_out=proof))
        assert proof==[] and not store.writes

def test_page_shape_limits_and_requested_size_are_checked_before_partial_success():
    for body in (page([{}],2,1),page([{},{}],1,1),page([[]],1,1),page([{}],1,1,count=True),page([{}],'1',1)):
        store,scope,env=loaded();env['_get_json']=lambda *a,body=body:body
        rejects(lambda:scope['fetch_fd']('buybacks_operations',{'page[size]':1}))
    store,scope,env=loaded();calls=[];env['_get_json']=lambda *a:calls.append(a)
    for size in (0,-1,True,'1'):
        rejects(lambda:scope['fetch_fd']('auctions_query',{'page[size]':size}))
    for limit in (0,-1,True,'3'):
        rejects(lambda:scope['fetch_fd']('auctions_query',{},max_pages=limit))
    assert not calls

def test_treasurydirect_error_shapes_are_rejected_but_all_valid_fields_survive():
    store,scope,env=loaded()
    for body in ({'error':'synthetic failure'},None,False,[None],[False],[[]]):
        env['_get_json']=lambda *a,body=body:body;rejects(lambda:scope['fetch_td']('auctioned'))
    for rows in ([],[{'zero':0,'missing':None,'extra':['all fields']} ]):
        env['_get_json']=lambda *a:rows;assert scope['fetch_td']('auctioned') is rows

def stream(raw,status=200,short=False):
    class Response(io.BytesIO):
        def read(self,n=-1):
            assert n>=0,'Unbounded HTTP read'
            return super().read(min(n,3) if short else n)
    response=Response(raw);response.status=status;return response

def test_http_reads_the_complete_bounded_body_and_closes_it_including_short_reads():
    store,scope,env=loaded();raw=b'{"zero":0,"missing":null,"all":{"fields":[1,2]}}'
    response=stream(raw,short=True)
    with patch('urllib.request.urlopen',return_value=response) as transport:
        assert scope['_get_json']('https://synthetic.invalid/input',timeout=7)==json.loads(raw)
    assert response.closed and transport.call_args.kwargs['timeout']==7
    for raw,status in [(raw+b'trailing',200),(raw,204),(b'x'*129,200)]:
        env['MAX_HTTP_BYTES']=128;response=stream(raw,status=status,short=True)
        with patch('urllib.request.urlopen',return_value=response):rejects(lambda:scope['_get_json']('https://synthetic.invalid/input'))
        assert response.closed

def test_http_declared_length_rejects_valid_json_prefixes_and_ambiguous_headers():
    from email.message import Message
    store,scope,env=loaded();env['MAX_HTTP_BYTES']=128
    for declared in ('3','1','-1','garbage','129',''):
        response=stream(b'{}');response.headers={'Content-Length':declared}
        with patch('urllib.request.urlopen',return_value=response):rejects(lambda:scope['_get_json']('https://synthetic.invalid/input'))
        assert response.closed
    headers=Message();headers['Content-Length']='2';headers['Content-Length']='2'
    for values in (headers,{'Content-Length':'2','Transfer-Encoding':'chunked'}):
        response=stream(b'{}');response.headers=values
        with patch('urllib.request.urlopen',return_value=response):rejects(lambda:scope['_get_json']('https://synthetic.invalid/input'))
        assert response.closed
    response=stream(b'{}',short=True);response.headers={'Content-Length':'2'}
    with patch('urllib.request.urlopen',return_value=response):assert scope['_get_json']('https://synthetic.invalid/input')=={}


def test_http_duplicate_fields_and_nonfinite_numbers_never_enter_measurements():
    store,scope,env=loaded()
    for raw in (b'{"data":[],"data":[1]}',b'{"outer":{"x":0,"x":1}}',b'{"x":NaN}',b'{"x":Infinity}',b'{"x":-Infinity}',b'{"x":1e999}',b'{'):
        response=stream(raw)
        with patch('urllib.request.urlopen',return_value=response):rejects(lambda:scope['_get_json']('https://synthetic.invalid/input'))
        assert response.closed
    response=stream(b'{"literal":"NaN","valid":0.25}')
    with patch('urllib.request.urlopen',return_value=response):assert scope['_get_json']('https://synthetic.invalid/input')=={'literal':'NaN','valid':0.25}

def test_http_interruption_closes_stream_without_returning_received_prefix():
    store,scope,env=loaded()
    class Broken(io.BytesIO):
        status=200
        def read(self,n=-1):raise TimeoutError('synthetic interrupted body')
    response=Broken(b'partial')
    with patch('urllib.request.urlopen',return_value=response):rejects(lambda:scope['_get_json']('https://synthetic.invalid/input'))
    assert response.closed

def test_actual_handler_preserves_operation_bank_when_pagination_is_incomplete():
    store,scope,env=loaded();raw=support['auction']();record=scope['norm_fd'](raw)
    store.docs[scope['HIST_KEY']]={'records':{scope['rec_key'](record):record}}
    previous={'saved':{'operation_date':'2026-09-28','accepted':0,'raw_extra':'retain whole old operation'}}
    store.docs[scope['BUY_KEY']]={'operations':deepcopy(previous),'superseded_operations':{}}
    def provider(url,*a,**k):
        if 'buybacks_' in url:return page([{'operation_date':'2026-09-28','accepted_amount':'999'}],40001,41)
        return {'error':'synthetic TreasuryDirect failure'}
    env.update(_get_json=provider,load_assets=lambda **k:{'series':{}},load_full_bank=lambda **k:{'rows':{}},fetch_fred_daily=lambda *a:{})
    with patch('urllib.request.urlopen',side_effect=AssertionError('No network')),redirect_stdout(io.StringIO()):
        assert scope['lambda_handler']({},None)['ok']
    assert store.docs[scope['BUY_KEY']]['operations']==previous and store.docs[scope['BUY_KEY']]['superseded_operations']=={}
    packet=store.docs[scope['OUT_KEY']]
    assert any('buybacks failed:' in n for n in packet['notes'])
    assert any('buyback details failed:' in n for n in packet['notes'])
    assert any('treasurydirect auctioned failed:' in n for n in packet['notes'])
    assert packet['decision']['call'] is None and packet['decision']['sizing_eligible'] is False

if __name__=='__main__':
    tests=[fn for name,fn in list(globals().items()) if name.startswith('test_')]
    for test in tests:test()
    print('Auction HTTP ingestion regressions passed:',len(tests))
