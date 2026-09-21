from pathlib import Path
from unittest.mock import patch
from io import BytesIO
import json,sys,unittest,urllib.error
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import fx_quote_capture as c
START='2026-06-23';END='2026-09-21';PAIR='EUR_USD'
def body(**kw):return json.dumps({'ticker':'C:EURUSD','status':'OK','resultsCount':1,'results':[{'t':1789689600000,'c':1.1723}],**kw}).encode()
class Response(BytesIO):status=200
class Opener:
    def __init__(self,values):self.values=iter(values);self.calls=[]
    def open(self,request,**kw):
        self.calls.append(request)
        item=next(self.values)
        if isinstance(item,Exception):raise item
        return Response(item)
class Tests(unittest.TestCase):
    def collect(self,values):
        originals=[];states=[];opener=Opener(values)
        def retain(raw):originals.append(raw);return {'sha256':c.sha(raw),'bytes':len(raw),'key':'synthetic/'+c.sha(raw)}
        budget=c.Budget()
        result=c.collect(PAIR,START,END,'synthetic-key',c.time.monotonic()+60,retain,budget,states.append,opener)
        return result,originals,states,opener,budget
    def test_exact_source_precision_and_all_null_rows_remain_in_original(self):
        raw=b'{"ticker":"C:EURUSD","status":"OK","resultsCount":3,"results":[{"c":1.1234567890123456789,"t":1789689600000},{"c":null},{"c":0}]}'
        result,originals,states,opener,budget=self.collect([raw])
        self.assertEqual(originals,[raw]);self.assertTrue(result['pagination_complete'])
        summary=c.summarize(c.decode(raw)['results']);self.assertEqual(summary['close_states'],{'positive':1,'null':1,'zero':1})
        self.assertEqual(str(c.decode(raw)['results'][0]['c']),'1.1234567890123456789')
        self.assertEqual(budget.requests,1);self.assertEqual(states[-1],result)
        self.assertNotIn('synthetic-key',json.dumps(result));self.assertFalse(result['bar_finality_independently_verified'])
    def test_rejects_foreign_hosts_private_addresses_wrong_pairs_and_changed_ranges(self):
        url=c.initial(PAIR,START,END)
        values=[url.replace('api.massive.com','elsewhere.example'),url.replace('https:','http:'),url.replace('C:EURUSD','C:USDJPY'),url.replace(START,'2026-06-24'),url+'#x',url.replace('api.massive.com','api.massive.com:443')]
        for bad in values:
            with self.assertRaises(ValueError):c.canonical(bad,PAIR,START,END)
    def test_query_scope_cannot_change_and_duplicate_members_are_rejected(self):
        url=c.initial(PAIR,START,END)
        for suffix in ('&limit=3','&mode=x','&cursor=','&apiKey=x&apiKey=y'):
            with self.assertRaises(ValueError):c.canonical(url+suffix,PAIR,START,END)
        for old,new in (('50000','50001'),('asc','desc'),('true','false')):
            with self.assertRaises(ValueError):c.canonical(url.replace(old,new),PAIR,START,END)
    def test_next_page_is_bound_and_embedded_key_removed(self):
        next_url='https://api.massive.com'+c.path(PAIR,START,END)+'?cursor=second&apiKey=provider-added-key'
        result,originals,states,opener,budget=self.collect([body(next_url=next_url),body(results=[],resultsCount=0)])
        self.assertEqual(len(originals),2);self.assertTrue(result['pagination_complete'])
        self.assertNotIn('provider-added-key',json.dumps(result));self.assertEqual(budget.requests,2)
    def test_repeated_cursor_and_invalid_host_preserve_originals_but_not_completeness(self):
        for url,stop in ((c.initial(PAIR,START,END),'pagination_cycle'),('https://elsewhere.example/x','invalid_pagination_address')):
            result,originals,*_=self.collect([body(next_url=url)])
            self.assertEqual(len(originals),1);self.assertEqual(result['stop'],stop);self.assertFalse(result['pagination_complete'])
    def test_wrong_response_identity_or_count_never_qualifies(self):
        for raw in (body(ticker='C:USDJPY'),body(resultsCount=4),body(resultsCount=True),body(status='NOT_AUTHORIZED')):
            result,originals,*_=self.collect([raw]);self.assertEqual(originals,[raw]);self.assertEqual(result['stop'],'invalid_provider_envelope')
    def test_empty_returned_result_is_not_market_zero(self):
        result,*_=self.collect([body(results=[],resultsCount=0)])
        self.assertTrue(result['pagination_complete']);self.assertEqual(c.summarize([])['close_states'],{})
    def test_a_binding_base_aggregate_limit_cannot_claim_complete(self):
        result,*_=self.collect([body(queryCount=50000)])
        self.assertEqual(result['stop'],'base_aggregate_limit_may_bind');self.assertFalse(result['pagination_complete'])
    def test_http_error_original_retained_without_retry_or_signal(self):
        error=urllib.error.HTTPError('https://api.massive.com',403,'forbidden',{},BytesIO(b'{"status":"NOT_AUTHORIZED"}'))
        result,originals,states,opener,budget=self.collect([error])
        self.assertEqual(result['stop'],'provider_http_failure');self.assertEqual(result['pages'][0]['http_status'],403)
        self.assertEqual(len(originals),1);self.assertEqual(budget.requests,1)
    def test_credential_reflection_never_reaches_retention(self):
        retained=[];opener=Opener([b'{"error":"synthetic-key"}'])
        with self.assertRaises(ValueError):c.request(c.initial(PAIR,START,END),PAIR,START,END,'synthetic-key',c.time.monotonic()+60,retained.append,c.Budget(),opener)
        self.assertEqual(retained,[])
    def test_expired_deadline_does_not_send_a_request(self):
        opener=Opener([]);budget=c.Budget()
        result=c.collect(PAIR,START,END,'synthetic-key',0,lambda x:None,budget,lambda x:None,opener)
        self.assertEqual(result['stop'],'time_budget');self.assertEqual(budget.requests,0)
    def test_timestamp_duplicates_and_malformed_closes_remain_explicit(self):
        result=c.summarize([{'t':1789689600000,'c':0},{'t':1789689600000,'c':-1},{'t':'1789689600000','c':True},{},None])
        self.assertEqual(result['returned_rows'],5);self.assertEqual(result['object_rows'],4)
        self.assertEqual(result['duplicate_timestamp_rows'],1);self.assertEqual(result['close_states'],{'zero':1,'negative':1,'invalid_type':1,'missing':1})
    def test_ambiguous_json_or_nonfinite_numbers_rejected(self):
        for raw in (b'{"c":1,"c":2}',b'{"c":NaN}'):
            with self.assertRaises(ValueError):c.decode(raw)
    def test_budget_and_window_bounds(self):
        budget=c.Budget(requests=1,byte_limit=1);budget.request();budget.received(1)
        with self.assertRaises(ValueError):budget.request()
        with self.assertRaises(ValueError):budget.received(1)
        with self.assertRaises(ValueError):c.path(PAIR,'2025-01-01',END)
        with self.assertRaises(ValueError):c.path('UNKNOWN',START,END)
    def test_page_bound_is_incomplete_and_retains_every_received_page(self):
        values=[body(next_url='https://api.massive.com'+c.path(PAIR,START,END)+'?cursor='+str(i)) for i in range(c.MAX_PAGES)]
        result,originals,states,opener,budget=self.collect(values)
        self.assertEqual(result['stop'],'page_limit');self.assertFalse(result['pagination_complete'])
        self.assertEqual(len(originals),c.MAX_PAGES);self.assertEqual(budget.requests,c.MAX_PAGES)
    def test_every_existing_pair_is_preserved(self):
        import ast
        tree=ast.parse((ROOT/'aws/lambdas/justhodl-polygon-fx-regime/source/lambda_function.py').read_text(encoding='utf-8'))
        assignment=next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='FX_PAIRS' for t in n.targets))
        self.assertEqual(c.PAIRS,ast.literal_eval(assignment.value));self.assertEqual(len(c.PAIRS),19)
if __name__=='__main__':unittest.main(verbosity=2)
