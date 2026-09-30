"""Current source with complete invented research documents and mocked book/sinks."""
from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import patch
import ast,base64,contextlib,copy,hashlib,io,json,unittest
from test_watchlist_sync import load_current
ROOT=Path(__file__).resolve().parents[4]
NOW=datetime(2026,9,30,8,35,tzinfo=timezone.utc)
class FrozenDateTime(datetime):
    @classmethod
    def now(cls,tz=None):return NOW if tz else NOW.replace(tzinfo=None)

class Research(unittest.TestCase):
    def setUp(self):self.mod=load_current();self.bodies=[];self.requests=[];self.writes=[];self.syncs=[];self.prices=[]
    def enrich(self,alpha=None,cs=None,ca=None,cb=None,regime=None,sent=None):
        indexes=[self.mod.index_by_symbol(rows) for rows in (alpha,cs,ca,cb,regime,sent)]
        return self.mod.enrich_symbol('AAA',{},*indexes)
    def read(self,raw=None,document=None,length=None,body=None):
        raw=raw if raw is not None else json.dumps({} if document is None else document).encode()
        self.body=body or io.BytesIO(raw)
        self.mod.s3.get_object=lambda **kw:{'Body':self.body,'ContentLength':len(raw) if length is None else length}
        with contextlib.redirect_stdout(io.StringIO()):return self.mod.load_s3_json('invented-research.json',{})
    def documents(self,alpha=None):
        return {self.mod.ALPHA_KEY:{'generated_at':'2020-01-01T00:00:00Z','stocks':[] if alpha is None else alpha},
                self.mod.CONFLUENCE_KEY:{'tier_s_confluence':[],'tier_a_confluence':[],'tier_b_confluence':[]},
                self.mod.REGIME_KEY:{'regime_picks':[]},self.mod.SENTIMENT_KEY:{'sentiment':[]}}
    def handler(self,documents=None,watchlist=None,positions=None,publisher=None):
        docs=self.documents() if documents is None else documents
        def get(**kw):
            self.requests.append(kw['Key']);value=docs[kw['Key']];raw=value if isinstance(value,bytes) else json.dumps(value).encode()
            body=io.BytesIO(raw);self.bodies.append(body);return {'Body':body,'ContentLength':len(raw)}
        self.mod.s3.get_object=get
        self.mod.query_pk=lambda pk:copy.deepcopy((positions or []) if pk=='POSITION' else (watchlist or []))
        def sync(document):self.syncs.append(copy.deepcopy(document));return {'added_S':[],'added_A':[],'removed_S':[],'removed_A':[]}
        self.mod.sync_auto_watchlist=sync
        def prices(symbols,**kwargs):self.prices.append(list(symbols));return {}
        self.mod.batch_fetch_prices=prices
        def publish(body,identity,context=None):
            if publisher is not None:publisher(body,identity,context)
            self.writes.append({'sink':'private','kind':'portfolio-snapshot','payload':json.loads(body),'body':body,'identity_bytes':identity})
        self.mod.publish_snapshot=publish
        self.mod.s3.put_object=lambda **kw:self.writes.append({'sink':'s3','request':kw})
        with patch.object(self.mod,'datetime',FrozenDateTime),patch.object(self.mod.time,'time',return_value=NOW.timestamp()),contextlib.redirect_stdout(io.StringIO()):
            result=self.mod.lambda_handler({},None)
        return result,self.writes[-2]['payload']
    def test_predecessor_complete_fixtures_are_inert_and_hash_bound(self):
        audit=json.loads((ROOT/'docs/audit/2026-09-30/portfolio-research-enrichment.json').read_bytes())
        for row in audit['fixtures'].values():
            raw=(ROOT/row['path']).read_bytes();self.assertEqual(len(raw),row['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),row['sha256'])
        data=json.loads((ROOT/'tests/fixtures/pre-snapshot-research-enrichment/complete-synthetic.json').read_bytes());self.assertEqual(len(data['cases']),7);self.assertEqual(data['cases'][-1]['error']['type'],'TypeError')
    def test_only_enrichment_reader_index_and_handler_change(self):
        def functions(path):return {n.name:ast.dump(n,include_attributes=False) for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef)}
        before=functions(ROOT/'tests/fixtures/pre-snapshot-research-enrichment/lambda_function.py.txt');after=functions(ROOT/'aws/lambdas/justhodl-portfolio-snapshot/source/lambda_function.py')
        self.assertEqual(set(after)-set(before),{'research_text','research_join','research_value','validate_snapshot_publication'});self.assertEqual({k for k in before if before[k]!=after[k]},{'load_s3_json','index_by_symbol','enrich_symbol','lambda_handler','batch_fetch_prices','fetch_polygon_latest','build_holdings_accounting'})
    def test_source_signal_and_risk_arrays_are_preserved_whole(self):
        row={'symbol':'AAA','top_signals':['s'+str(i) for i in range(105)],'risk_flags':['r'+str(i) for i in range(105)]}
        result=self.enrich(alpha=[row]);self.assertEqual(result['top_signals'],row['top_signals']);self.assertEqual(result['risk_flags'],row['risk_flags'])
    def test_full_sentiment_text_is_retained_without_truncation(self):
        text='Complete invented evidence π '+('long explanation '*1000);result=self.enrich(sent=[{'symbol':'AAA','sentimentReason':text}]);self.assertEqual(result['sentiment_reason'],text)
    def test_duplicate_symbols_are_not_silently_resolved(self):
        rows=[{'symbol':'AAA','alpha_score':10},{'symbol':'AAA','alpha_score':99}];index=self.mod.index_by_symbol(rows)
        self.assertNotIn('AAA',index);self.assertEqual(index.evidence('AAA')['zero_based_occurrences'],[0,1]);self.assertEqual(index.evidence('AAA')['status'],'AMBIGUOUS_DUPLICATE_SYMBOL');self.assertIsNone(self.enrich(alpha=rows)['alpha_score'])
    def test_even_identical_duplicates_are_explicit_occurrences(self):
        row={'symbol':'AAA','alpha_score':90};index=self.mod.index_by_symbol([row,row]);self.assertEqual(index.summary()['duplicate_symbol_occurrences'],{'AAA':[0,1]});self.assertEqual(index.summary()['source_row_count'],2)
    def test_invalid_identifiers_and_rows_have_retained_indices(self):
        index=self.mod.index_by_symbol([None,True,{'symbol':'aaa'},{'symbol':'A/B'},{'symbol':'AAA'}]);self.assertEqual(index.summary()['invalid_zero_based_occurrences'],[0,1,2,3]);self.assertEqual(index.evidence('AAA')['zero_based_occurrences'],[4])
    def test_empty_array_is_distinct_from_missing_or_malformed_array(self):
        empty=self.mod.index_by_symbol([]).summary();self.assertEqual(empty['source_row_count'],0);self.assertEqual(empty['source_shape'],'ARRAY')
        for rows in (None,False,{},'bad'):
            summary=self.mod.index_by_symbol(rows).summary();self.assertIsNone(summary['source_row_count']);self.assertEqual(summary['source_shape'],'UNAVAILABLE_OR_INVALID_ARRAY')
    def test_cross_tier_conflict_has_no_preferred_tier(self):
        row={'symbol':'AAA','confluence_count':7};result=self.enrich(cs=[row],ca=[row]);self.assertIsNone(result['confluence_tier']);self.assertIsNone(result['confluence_count']);self.assertEqual(result['research_evidence']['confluence_conflict'],'SYMBOL_OCCURS_IN_MULTIPLE_TIERS')
    def test_duplicate_single_tier_cannot_create_confluence(self):
        result=self.enrich(cs=[{'symbol':'AAA','confluence_count':1}]*2);self.assertIsNone(result['confluence_tier']);self.assertEqual(result['research_evidence']['confluence_s']['status'],'AMBIGUOUS_DUPLICATE_SYMBOL')
    def test_complete_a_and_b_components_are_preserved(self):
        row={'symbol':'AAA','confluence_count':0,'components_firing':['original'+str(i) for i in range(20)]}
        for key,tier in (('ca','A'),('cb','B')):
            result=self.enrich(**{key:[row]});self.assertEqual(result['confluence_tier'],tier);self.assertEqual(result['confluence_count'],0);self.assertEqual(result['components_firing'],row['components_firing'])
    def test_invalid_numeric_fields_stay_unavailable(self):
        for value in ('99',True,False,float('nan'),float('inf'),10**400,2**53,1e300,{},[]):
            result=self.enrich(alpha=[{'symbol':'AAA','alpha_score':value}],regime=[{'symbol':'AAA','regime_adj':value}],sent=[{'symbol':'AAA','sentimentScore':value}])
            for field in ('alpha_score','regime_adj','sentiment_score'):self.assertIsNone(result[field]);self.assertIn(field,result['research_evidence']['invalid_fields'])
    def test_measured_zero_and_signed_regime_values_survive(self):
        result=self.enrich(alpha=[{'symbol':'AAA','alpha_score':0}],regime=[{'symbol':'AAA','regime_adj':-1.5,'regime_adj_score':0}],sent=[{'symbol':'AAA','sentimentScore':0}])
        self.assertEqual(result['alpha_score'],0);self.assertEqual(result['regime_adj'],-1.5);self.assertEqual(result['regime_adj_score'],0);self.assertEqual(result['sentiment_score'],0)
    def test_alpha_range_rank_and_tier_are_typed(self):
        for value in (-1,101):self.assertIsNone(self.enrich(alpha=[{'symbol':'AAA','alpha_score':value}])['alpha_score'])
        for value in (True,'1',1.0,0,-1,2**53):self.assertIsNone(self.enrich(alpha=[{'symbol':'AAA','rank':value}])['rank'])
        for value in (True,'s','unknown',{}):self.assertIsNone(self.enrich(alpha=[{'symbol':'AAA','tier':value}])['tier'])
    def test_string_lists_and_malformed_text_are_not_coerced(self):
        result=self.enrich(alpha=[{'symbol':'AAA','top_signals':'TEXT','risk_flags':{},'name':True,'sector':[]}],sent=[{'symbol':'AAA','sentimentReason':False}])
        for field in ('top_signals','risk_flags','name','sector','sentiment_reason'):self.assertIsNone(result[field]);self.assertIn(field,result['research_evidence']['invalid_fields'])
        deep=[]
        for _ in range(70):deep=[deep]
        for bad in ({'unsafe_integer':2**53},{'deep':deep}):
            result=self.enrich(alpha=[{'symbol':'AAA','components':bad,'risk_flags':[bad]}],cs=[{'symbol':'AAA','components_firing':[bad],'confluence_count':2**53}])
            for field in ('components','risk_flags','components_firing','confluence_count'):self.assertIsNone(result[field]);self.assertIn(field,result['research_evidence']['invalid_fields'])
    def test_join_reference_identifies_source_array_and_exact_occurrence(self):
        result=self.enrich(alpha=[{'symbol':'BBB'},{'symbol':'AAA','alpha_score':50}]);trace=result['research_evidence']['alpha']
        self.assertEqual(trace['source_key'],self.mod.ALPHA_KEY);self.assertEqual(trace['array_path'],'stocks');self.assertEqual(trace['zero_based_occurrences'],[1]);self.assertFalse(trace['freshness_verified']);self.assertFalse(trace['allows_sizing'])
    def test_original_document_bytes_and_clock_survive_without_mutation(self):
        raw=b'{\n "generated_at":"2020-01-01T00:00:00Z", "stocks":[], "unknown":{"keep":true}}';result=self.read(raw=raw);trace=result.source_evidence
        self.assertEqual(base64.b64decode(trace['body']),raw);self.assertEqual(trace['body_sha256'],hashlib.sha256(raw).hexdigest());self.assertEqual(trace['body_bytes'],len(raw));self.assertTrue(trace['body_complete']);self.assertTrue(trace['stream_close_confirmed']);self.assertEqual(trace['declared_generated_at'],'2020-01-01T00:00:00Z');self.assertFalse(trace['freshness_verified']);self.assertEqual(json.loads(json.dumps(result)),json.loads(raw))
    def test_complete_invalid_body_is_retained_without_becoming_valid_source(self):
        raw=b'{"stocks":[],"stocks":[]}';result=self.read(raw=raw);trace=result.source_evidence;self.assertEqual(result,{});self.assertEqual(trace['status'],'UNAVAILABLE');self.assertTrue(trace['body_complete']);self.assertEqual(base64.b64decode(trace['body']),raw)
    def test_incomplete_body_never_claims_retained_complete_source(self):
        result=self.read(raw=b'{}',length=3);trace=result.source_evidence;self.assertFalse(trace['body_complete']);self.assertNotIn('body',trace);self.assertEqual(trace['status'],'UNAVAILABLE');self.assertTrue(self.body.closed)
    def test_oversized_source_is_withheld_without_reading_a_prefix(self):
        class Body(io.BytesIO):
            def read(self,*a):raise AssertionError('Oversized source must not be read')
        result=self.read(raw=b'{}',body=Body(b'{}'),length=self.mod.RESEARCH_SOURCE_MAX_BYTES+1);self.assertEqual(result.source_evidence['reason_code'],'SOURCE_BYTE_BOUND_OR_INVALID_LENGTH');self.assertTrue(self.body.closed);self.assertFalse(result.source_evidence['body_complete'])
    def test_cleanup_error_is_redacted_and_reported_separately(self):
        class Body(io.BytesIO):
            def close(self):raise OSError('INVENTED sensitive cleanup message')
        result=self.read(raw=b'{}',body=Body(b'{}'));trace=result.source_evidence;self.assertTrue(trace['body_complete']);self.assertFalse(trace['stream_close_confirmed']);self.assertNotIn('INVENTED sensitive',json.dumps(trace))
    def test_handler_retains_all_four_complete_original_documents(self):
        docs=self.documents([{'symbol':'AAA','alpha_score':80,'unknown_field':['keep',1]}]);_,payload=self.handler(docs,watchlist=[{'symbol':'AAA'}]);research=payload['research']
        self.assertEqual(set(research['source_documents']),set(docs));self.assertTrue(all(b.closed for b in self.bodies));self.assertFalse(research['freshness_verified']);self.assertFalse(research['allows_sizing'])
        for key,document in docs.items():self.assertEqual(json.loads(base64.b64decode(research['source_documents'][key]['body'])),document)
        self.assertEqual(research['source_documents'][self.mod.ALPHA_KEY]['declared_generated_at'],'2020-01-01T00:00:00Z')
    def test_text_score_cannot_crash_handler_and_unknown_sorts_after_zero(self):
        docs=self.documents([{'symbol':'AAA','alpha_score':'99'},{'symbol':'BBB','alpha_score':0},{'symbol':'CCC','alpha_score':1}]);_,payload=self.handler(docs,watchlist=[{'symbol':s} for s in ('AAA','BBB','CCC')])
        self.assertEqual([w['symbol'] for w in payload['watchlist']],['CCC','BBB','AAA']);self.assertIsNone(payload['watchlist'][-1]['alpha_score']);self.assertFalse(payload['capital_book']['allows_new_entries'])
    def test_duplicate_source_occurrences_are_inspectable_in_handler_output(self):
        rows=[{'symbol':'AAA','alpha_score':1},{'symbol':'AAA','alpha_score':99}];_,payload=self.handler(self.documents(rows),watchlist=[{'symbol':'AAA'}]);self.assertIsNone(payload['watchlist'][0]['alpha_score']);self.assertEqual(payload['research']['joins']['alpha']['duplicate_symbol_occurrences'],{'AAA':[0,1]});self.assertEqual(json.loads(base64.b64decode(payload['research']['source_documents'][self.mod.ALPHA_KEY]['body']))['stocks'],rows)
    def test_bad_optional_source_does_not_claim_empty_research_or_lose_book(self):
        docs=self.documents();docs[self.mod.ALPHA_KEY]=b'[]';_,payload=self.handler(docs,positions=[{'symbol':'AAA','qty':1,'cost_basis_per_share':10}]);self.assertEqual(len(payload['positions']),1);self.assertEqual(payload['research']['source_documents'][self.mod.ALPHA_KEY]['status'],'UNAVAILABLE');self.assertIsNone(payload['research']['joins']['alpha']['source_row_count']);self.assertIsNone(payload['positions'][0]['alpha_score'])
    def test_total_source_bound_prevents_sync_marks_and_publication(self):
        docs=self.documents();lengths=[len(json.dumps(docs[k]).encode()) for k in docs];self.mod.RESEARCH_SOURCE_MAX_BYTES=max(lengths[0],lengths[1])+1
        with self.assertRaisesRegex(ValueError,'Complete research source evidence exceeds'):self.handler(docs)
        self.assertEqual(self.syncs,[]);self.assertEqual(self.prices,[]);self.assertEqual(self.writes,[]);self.assertTrue(all(b.closed for b in self.bodies));self.assertEqual(len(self.requests),2)
    def test_final_mirror_wire_bound_checks_spaced_encoding_before_both_sinks(self):
        _,payload=self.handler();wire=len(self.mod.encode_snapshot(payload));self.writes=[];self.mod.SNAPSHOT_MIRROR_MAX_BYTES=wire
        self.handler();self.assertEqual(len(self.writes),2);self.writes=[];self.mod.SNAPSHOT_MIRROR_MAX_BYTES=wire-1
        with self.assertRaisesRegex(ValueError,'Complete snapshot exceeds'):self.handler()
        self.assertEqual(self.writes,[])
    def test_missing_source_read_evidence_is_never_fabricated(self):
        document={};self.assertFalse(hasattr(document,'source_evidence'))
        # Existing mocks and callers can still use ordinary dictionaries; source proof stays absent.
        with patch.object(self.mod,'load_s3_json',return_value=document):_,payload=self.handler()
        self.assertTrue(all(row['status']=='READ_EVIDENCE_UNAVAILABLE' and row['body_complete'] is False for row in payload['research']['source_documents'].values()))

if __name__=='__main__':unittest.main()
