"""Economic, source, replay and publication checks; synthetic originals only."""
from pathlib import Path
from copy import deepcopy
from datetime import date,datetime,timezone
from decimal import Decimal,localcontext,ROUND_UP
from types import SimpleNamespace
from unittest.mock import patch
import hashlib,io,json,sys,time,unittest,urllib.error
SOURCE=Path(__file__).resolve().parents[1]/'source';sys.path.insert(0,str(SOURCE))
import insider_research_model as model
import insider_research_store as store
STAMP='2026-09-20T17:30:00+00:00'

class FakeError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}
class Storage:
    def __init__(self,objects=None):self.objects=dict(objects or {});self.writes=[];self.reads=[]
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key not in self.objects:raise FakeError('NoSuchKey')
        raw=self.objects[key];return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        old=self.objects.get(kw['Key'])
        if kw.get('IfNoneMatch')=='*' and old is not None:raise FakeError('PreconditionFailed')
        if kw.get('IfMatch') and (old is None or hashlib.sha256(old).hexdigest()!=kw['IfMatch']):raise FakeError('PreconditionFailed')
        self.objects[kw['Key']]=kw['Body'];self.writes.append(kw)
def row(**changes):
    base={'symbol':'TEST','companyCik':'123','reportingCik':'456','reportingName':'Example A','formType':'4',
        'filingDate':'2026-09-18','transactionDate':'2026-09-17','transactionType':'P-Purchase',
        'acquisitionOrDisposition':'A','securitiesTransacted':100,'securitiesOwned':9000,'price':12.125,
        'securityName':'Common Stock','url':'https://www.sec.gov/Archives/edgar/data/123/123456/form4.xml'}
    return {**base,**changes}
def fixture(rows=None):
    rows=rows if rows is not None else [row(),row(reportingCik='789'),row(transactionType='S-Sale',acquisitionOrDisposition='D',securitiesTransacted=200),row(transactionDate='2027-01-01'),row(formType='4/A'),row(price=0),row()]
    raw=model.encoded(rows);digest=model.sha(raw);key=model.PRIVATE+digest+'.bin'
    item={'page':0,'acquired_at':STAMP,'evidence':{'key':key,'sha256':digest,'bytes':len(raw),'provider':'Financial Modeling Prep','request_url':model.source_url(0),'access':'protected_AWS_IAM_source_archive'}}
    inputs={'contract':'insider-native-inputs.v1','started_at':STAMP,'generated_at':STAMP,'collection':{'pages':[item],'stop_reason':'page_limit','max_pages':1,'source_bytes':len(raw)}}
    return Storage({key:raw}),inputs
def packet():
    s,i=fixture();o=store.compile_output(i,store.reader(s,'b'));return s,i,{**o,'replay':store.retain(s,'b',i,o)}

class Native(unittest.TestCase):
    def test_sample_counts_reconcile_with_exclusions_and_duplicates(self):
        s,i=fixture();o=store.compile_output(i,store.reader(s,'b'));c=o['coverage']
        self.assertEqual((c['rows_received'],c['distinct_representation_count'],c['duplicate_representations'],c['eligible_representations'],c['excluded_representations']),(7,6,1,4,2))
        self.assertEqual(o['windows']['last_7d']['buy_count'],3);self.assertEqual(o['windows']['last_7d']['sell_count'],1)
        self.assertFalse(c['population_complete']);self.assertIsNone(o['n_transactions'])
    def test_missing_currency_is_not_zero_or_dollar_total(self):
        s,i=fixture();o=store.compile_output(i,store.reader(s,'b'));w=o['windows']['last_30d']
        self.assertIsNone(w['buy_usd']);self.assertIsNone(w['sell_usd']);self.assertIsNone(w['buy_sell_ratio_dollar']);self.assertEqual(w['unknown_currency_rows'],4)
        self.assertEqual(w['unpriced_rows'],1);self.assertEqual(w['amounts_by_explicit_currency'],{})
    def test_quantity_never_falls_back_to_shares_owned(self):
        for q in (None,0,-5,True,'NaN','bad'):
            r=model.classify(row(securitiesTransacted=q),date(2026,9,20));self.assertIn('missing_or_nonpositive_transacted_quantity',r['reasons'])
    def test_exact_codes_direction_and_form_not_prefix_guesses(self):
        for value in ('PURCHASE','PRIVATE','SOMETHING','P-Award','A-Award','M-Exercise'):
            self.assertIn('not_exact_P_or_S',model.classify(row(transactionType=value),date(2026,9,20))['reasons'])
        self.assertIn('direction_conflict_or_missing',model.classify(row(acquisitionOrDisposition='D'),date(2026,9,20))['reasons'])
        self.assertIn('not_Form_4',model.classify(row(formType='5'),date(2026,9,20))['reasons'])
    def test_future_or_undated_transactions_never_fallback_to_filing(self):
        for value in ('2027-05-14','2026-09-19',None,'invalid'):
            self.assertTrue(model.classify(row(transactionDate=value),date(2026,9,20))['reasons'])
    def test_distinct_filing_vs_transaction_windows_and_inclusive_bounds(self):
        s,i=fixture([row(transactionDate='2021-08-21'),row(transactionDate='2026-09-14',reportingCik='789'),row(transactionDate='2026-09-13',reportingCik='987')])
        o=store.compile_output(i,store.reader(s,'b'));self.assertEqual(o['windows']['last_7d']['buy_count'],1);self.assertEqual(o['filing_windows']['last_7d']['buy_count'],3)
        self.assertFalse(o['coverage']['population_complete']);self.assertGreater(o['data_coverage_days'],1000)
    def test_no_sale_ratio_null_and_no_buy_ratio_zero(self):
        for rows,expected in (([row()],None),([row(transactionType='S-Sale',acquisitionOrDisposition='D')],0)):
            s,i=fixture(rows);w=store.compile_output(i,store.reader(s,'b'))['windows']['last_30d'];self.assertEqual(w['buy_sell_ratio_count'],expected)
    def test_explicit_currency_buckets_do_not_mix(self):
        rows=[row(currency='USD',price='12.123456789'),row(currency='EUR',reportingCik='789',price='4.5')]
        s,i=fixture(rows);w=store.compile_output(i,store.reader(s,'b'))['windows']['last_30d']
        self.assertEqual(w['amounts_by_explicit_currency']['USD']['buy_exact'],'1212.345678900');self.assertIsNone(w['buy_usd'])
    def test_CIK_cluster_not_job_title_or_names_and_unqualified(self):
        s,i=fixture();o=store.compile_output(i,store.reader(s,'b'));c=o['notable_cluster_buys'][0]
        self.assertEqual(c['n_buyers'],2);self.assertEqual(c['issuer_cik'],'0000000123');self.assertIsNone(c['total_usd'])
        for k in model.PERMISSIONS:self.assertIs(c[k],False);self.assertIs(o[k],False)
        self.assertIsNone(o['regime']);self.assertIsNone(o['call']);self.assertEqual(o['portfolio_action'],'WAIT')
        self.assertIn('missing_CIK_identity',model.classify(row(reportingCik='',typeOfOwner='CEO'),date(2026,9,20))['reasons'])
    def test_unverified_plan_or_security_cannot_become_discretionary_common_stock(self):
        s,i=fixture([row(securityName='Stock Option',is10b51=True)]);o=store.compile_output(i,store.reader(s,'b'))
        self.assertEqual(o['windows']['last_7d']['plan_status'],'unknown_not_excluded');self.assertEqual(o['security_labels'],{'Stock Option':1})
        self.assertFalse(o['quality']['plan_classification_available'])
    def test_decimal_context_and_original_source_precision(self):
        s,i=fixture([row(currency='USD',price='12.123456789')]);normal=store.compile_output(i,store.reader(s,'b'))
        with localcontext() as ctx:
            ctx.prec=6;ctx.rounding=ROUND_UP;self.assertEqual(store.compile_output(i,store.reader(s,'b')),normal)
        self.assertEqual(model.decode(b'[{"price":1.234567890123456789}]')[0]['price'],Decimal('1.234567890123456789'))
    def test_next_scheduled_deadline_handles_weekends(self):
        self.assertEqual(model.due_at(STAMP),'2026-09-22T00:30:00+00:00')
        self.assertEqual(model.due_at('2026-09-18T22:31:00Z'),'2026-09-22T00:30:00+00:00')
        self.assertEqual(model.due_at('2026-09-18T17:00:00Z'),'2026-09-19T00:30:00+00:00')
    def test_original_identity_corruption_or_bad_clock_refused(self):
        for field,value in (('page',1),('acquired_at','2027-01-01T00:00:00Z')):
            s,i=fixture();i['collection']['pages'][0][field]=value
            with self.assertRaises(ValueError):store.compile_output(i,store.reader(s,'b'))
        s,i=fixture();key=next(iter(s.objects));s.objects[key]+=b' '
        with self.assertRaises(ValueError):store.compile_output(i,store.reader(s,'b'))
    def test_strict_provider_json_and_filing_link_origin(self):
        for raw in (b'{}',b'[NaN]',model.encoded([{}]*1001)):
            with self.assertRaises(ValueError):model.decode(raw)
        for url in ('https://www.sec.gov.evil.test/Archives/edgar/data/1/x','https://user:password@www.sec.gov/Archives/edgar/data/1/x','javascript:alert(1)'):
            self.assertIsNone(model.filing_url(url))
    def test_retained_original_compiler_and_output_replay(self):
        s,i,p=packet();self.assertEqual(store.replay(p['replay'],store.reader(s,'b')),{k:v for k,v in p.items() if k!='replay'})
        m=json.loads(s.objects[p['replay']['manifest_key']]);key=m['compilers']['insider_research_model']['key'];s.objects[key]+=b'\n'
        with self.assertRaises(ValueError):store.replay(p['replay'],store.reader(s,'b'))
    def test_original_is_private_and_current_publication_preserves_predecessor(self):
        s,i,p=packet();old=model.encoded({'generated_at':'2026-09-18T00:00:00Z','windows':{'original':123}});s.objects[store.CURRENT]=old
        self.assertTrue(store.publish(s,'b',p));self.assertEqual(s.objects[model.PRIVATE+model.sha(old)+'.bin'],old)
        future={**p,'generated_at':'2026-09-21T00:00:00Z'};s.objects[store.CURRENT]=model.encoded(future);self.assertFalse(store.publish(s,'b',p))
        with self.assertRaises(ValueError):store.reader(s,'b')('private-account/book.json')
    def test_duplicate_request_never_collects_or_publishes_again(self):
        s,i,p=packet();key=store.request_key('test-request');prior={'status':'running'};s.objects[key]=model.encoded(prior)
        with patch.object(store,'collect',side_effect=AssertionError('duplicate collection')):
            self.assertEqual(store.run(s,'b','test-request','execution','secret'),prior)
    def test_full_request_publishes_replay_and_failure_keeps_last_good(self):
        s,i=fixture()
        with patch.object(store,'now',return_value=STAMP),patch.object(store,'collect',return_value=i['collection']):
            result=store.run(s,'b','complete-request','execution','secret')
        self.assertEqual(result['status'],'complete');self.assertTrue(result['published'])
        prior=s.objects[store.CURRENT]
        with patch.object(store,'collect',side_effect=ValueError('secret-must-not-be-reported')):
            with self.assertRaises(RuntimeError) as caught:store.run(s,'b','failure-request','execution','secret')
        self.assertNotIn('secret-must-not-be-reported',str(caught.exception));self.assertEqual(s.objects[store.CURRENT],prior)
        status=json.loads(s.objects[store.request_key('failure-request')]);self.assertEqual(status['status'],'failed');self.assertNotIn('secret-must-not-be-reported',json.dumps(status))
    def test_malformed_code_is_excluded_without_crashing_other_rows(self):
        s,i=fixture([row(transactionType={'unexpected':'object'}),row()])
        output=store.compile_output(i,store.reader(s,'b'));self.assertEqual(output['coverage']['eligible_representations'],1)
        self.assertEqual(output['coverage']['excluded_reason_counts']['not_exact_P_or_S'],1)
    def test_provider_budget_repetition_and_failure_are_explicit(self):
        raw=model.encoded([row()])
        class Opener:
            def __init__(self):self.calls=0
            def open(self,req,timeout):self.calls+=1;r=io.BytesIO(raw);r.status=200;return r
        s=Storage();op=Opener()
        with patch.object(store.time,'sleep'):
            c=store.collect(s,'b','not-in-body',time.monotonic()+30,opener=op,max_pages=4)
        self.assertEqual(c['stop_reason'],'repeating_page');self.assertEqual(len(c['pages']),2);self.assertEqual(op.calls,2)
        with self.assertRaises(ValueError):store.collect(Storage(),'b','key',time.monotonic()-1,opener=op)
    def test_handler_HTTP_and_validate_are_read_only(self):
        import lambda_function as handler
        with patch.object(handler.boto3,'client',side_effect=AssertionError('no AWS')):
            self.assertEqual(handler.lambda_handler({'validate_only':True})['statusCode'],200)
        s,i,p=packet();s.objects[store.CURRENT]=model.encoded(p)
        with patch.object(handler.boto3,'client',return_value=s),patch.object(handler,'run',side_effect=AssertionError('no publish')):
            self.assertEqual(handler.lambda_handler({'httpMethod':'GET'})['statusCode'],200)

if __name__=='__main__':unittest.main()
