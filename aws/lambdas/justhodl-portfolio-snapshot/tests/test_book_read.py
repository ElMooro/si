"""Complete invented DynamoDB pages; actual current snapshot handler, no private I/O."""
from pathlib import Path
from decimal import Decimal
from datetime import datetime,timezone
from unittest.mock import patch
import ast,copy,hashlib,json,types,unittest
from test_watchlist_sync import load_current
ROOT=Path(__file__).resolve().parents[4]
def position(symbol='AAA',**fields):
    return {'pk':'POSITION','sk':symbol,'symbol':symbol,'qty':Decimal('10'),'cost_basis_per_share':Decimal('100'),'cost_basis_total':Decimal('1000'),'notes':'Invented owner note',**fields}
class BookReads(unittest.TestCase):
    def setUp(self):
        self.mod=load_current();self.calls=[];self.pages=[{'Items':[position()]}]
        def query(**request):
            self.calls.append(copy.deepcopy(request))
            if request['ExpressionAttributeValues'][':pk']=='WATCHLIST':return {'Items':[]}
            index=sum(r['ExpressionAttributeValues'][':pk']=='POSITION' for r in self.calls)-1
            value=self.pages[min(index,len(self.pages)-1)]
            if isinstance(value,Exception):raise value
            return copy.deepcopy(value)
        self.mod.table=types.SimpleNamespace(query=query)
    def test_complete_original_failures_and_handler_are_inert(self):
        audit=json.loads((ROOT/'docs/audit/2026-09-30/portfolio-book-read-integrity.json').read_bytes())
        for row in audit['fixtures'].values():
            raw=(ROOT/row['path']).read_bytes();self.assertEqual(len(raw),row['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),row['sha256'])
        data=json.loads((ROOT/'tests/fixtures/pre-portfolio-book-read/complete-synthetic.json').read_bytes());self.assertEqual(len(data['cases']),4);self.assertEqual(len(data['cases'][0]['writes']),2)
    def test_only_book_reader_and_handler_change_from_exact_predecessor(self):
        def functions(path):return {n.name:ast.dump(n,include_attributes=False) for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef)}
        before=functions(ROOT/'tests/fixtures/pre-portfolio-book-read/lambda_function.py.txt');after=functions(ROOT/'aws/lambdas/justhodl-portfolio-snapshot/source/lambda_function.py')
        self.assertEqual(set(after)-set(before),{'parse_previous_close','read_previous_close_body','research_text','research_join','research_value','validate_snapshot_publication'});self.assertEqual({k for k in before if before[k]!=after[k]},{'query_pk','lambda_handler','fetch_polygon_latest','enrich_symbol','load_s3_json','index_by_symbol'})
    def test_every_page_is_consistent_and_all_exact_values_are_retained(self):
        first=position(qty=Decimal('9007199254740993'),cost_basis_per_share=Decimal('0.10000000000000000001'),extra={'entire':[Decimal('2.001'),False,None]});second=position('BBB')
        self.pages=[{'Items':[first],'LastEvaluatedKey':{'pk':'POSITION','sk':'AAA'}},{'Items':[second]}]
        result=self.mod.query_pk('POSITION');self.assertEqual(result,[first,second]);self.assertIsInstance(result[0]['qty'],Decimal)
        self.assertTrue(all(r['ConsistentRead'] is True for r in self.calls));self.assertEqual(len(self.calls),2)
    def test_explicit_empty_list_is_empty_and_missing_items_is_unavailable(self):
        self.pages=[{'Items':[]}];self.assertEqual(self.mod.query_pk('POSITION'),[])
        for value in ({},{'Items':None},{'Items':'bad'},{'Items':{}},[],None):
            self.pages=[value];self.calls=[]
            with self.assertRaises(self.mod.BookReadUnavailable):self.mod.query_pk('POSITION')
    def test_missing_later_page_cannot_return_partial_book(self):
        self.pages=[{'Items':[position()],'LastEvaluatedKey':{'pk':'POSITION','sk':'AAA'}},{}]
        with self.assertRaises(self.mod.BookReadUnavailable):self.mod.query_pk('POSITION')
        self.assertEqual(len(self.calls),2)
    def test_interrupted_later_page_has_only_fixed_redacted_error(self):
        self.pages=[{'Items':[position()],'LastEvaluatedKey':{'pk':'POSITION','sk':'AAA'}},RuntimeError('Invented sensitive source text')]
        with self.assertRaises(self.mod.BookReadUnavailable) as error:self.mod.query_pk('POSITION')
        self.assertEqual(str(error.exception),'Complete book page unavailable')
    def test_repeated_cursor_and_repeated_identity_fail_without_loop(self):
        self.pages=[{'Items':[],'LastEvaluatedKey':{'pk':'POSITION','sk':'AAA'}}]
        with self.assertRaises(self.mod.BookReadUnavailable):self.mod.query_pk('POSITION')
        self.assertEqual(len(self.calls),2)
        self.calls=[];self.pages=[{'Items':[position()],'LastEvaluatedKey':{'pk':'POSITION','sk':'AAA'}},{'Items':[position()]}]
        with self.assertRaises(self.mod.BookReadUnavailable):self.mod.query_pk('POSITION')
    def test_malformed_continuation_never_starts_another_query(self):
        for cursor in (True,0,'',[],{'pk':'WATCHLIST','sk':'AAA'},{'pk':'POSITION','sk':''},{'pk':'POSITION','sk':True},{'pk':'POSITION','sk':'AAA','unknown':'extra'}):
            self.calls=[];self.pages=[{'Items':[],'LastEvaluatedKey':cursor}]
            with self.assertRaises(self.mod.BookReadUnavailable):self.mod.query_pk('POSITION')
            self.assertEqual(len(self.calls),1)
    def test_malformed_storage_identity_fails_but_invalid_symbol_is_retained(self):
        for row in (None,True,[],{'pk':'OTHER','sk':'AAA'},{'pk':'POSITION','sk':False},{'pk':'POSITION','sk':''}):
            self.pages=[{'Items':[row]}]
            with self.assertRaises(self.mod.BookReadUnavailable):self.mod.query_pk('POSITION')
        invalid=position(symbol=None);invalid['sk']='VALID-STORAGE-KEY';self.pages=[{'Items':[invalid]}];self.assertEqual(self.mod.query_pk('POSITION'),[invalid])
    def test_empty_continuation_and_empty_interior_page_are_valid(self):
        self.pages=[{'Items':[],'LastEvaluatedKey':{'pk':'POSITION','sk':'AAA'}},{'Items':[position('BBB')],'LastEvaluatedKey':{}}]
        self.assertEqual(self.mod.query_pk('POSITION'),[position('BBB')])
    def test_row_page_and_byte_bounds_never_truncate(self):
        self.assertEqual((self.mod.BOOK_MAX_ROWS,self.mod.BOOK_MAX_PAGES,self.mod.BOOK_MAX_BYTES,self.mod.BOOK_READ_SECONDS),(10000,100,8*1024*1024,20))
        self.mod.BOOK_MAX_ROWS=1;self.pages=[{'Items':[position(),position('BBB')]}]
        with self.assertRaises(self.mod.BookReadUnavailable):self.mod.query_pk('POSITION')
        self.mod.BOOK_MAX_ROWS=10000;self.mod.BOOK_MAX_PAGES=1;self.pages=[{'Items':[],'LastEvaluatedKey':{'pk':'POSITION','sk':'AAA'}}]
        with self.assertRaises(self.mod.BookReadUnavailable):self.mod.query_pk('POSITION')
        self.mod.BOOK_MAX_PAGES=100;self.mod.BOOK_MAX_BYTES=10;self.pages=[{'Items':[position()]}]
        with self.assertRaises(self.mod.BookReadUnavailable):self.mod.query_pk('POSITION')
    def test_acceptance_deadline_is_checked_after_sdk_read(self):
        with patch.object(self.mod.time,'monotonic',side_effect=[0,0,21]):
            with self.assertRaises(self.mod.BookReadUnavailable):self.mod.query_pk('POSITION')
        self.assertEqual(len(self.calls),1)
    def test_unencodable_metadata_and_invalid_unicode_are_unavailable(self):
        for extra in (b'not JSON source',{1,2},'\ud800'):
            self.pages=[{'Items':[position(extra=extra)]}]
            with self.assertRaises(self.mod.BookReadUnavailable):self.mod.query_pk('POSITION')
    def handler(self):
        self.mod.load_s3_json=lambda key,default:copy.deepcopy(default)
        self.mod.sync_auto_watchlist=lambda _:{'added_S':[],'added_A':[],'removed_S':[],'removed_A':[],'status':'SKIPPED_INVALID_SOURCE'}
        self.prices=[];self.writes=[]
        def marks(symbols):
            self.prices.append(list(symbols));return {s:{'price':110,'as_of_unix_ms':int(datetime.now(timezone.utc).timestamp()*1000)} for s in symbols}
        self.mod.batch_fetch_prices=marks
        self.mod.publish_private=lambda kind,payload:self.writes.append(('private',copy.deepcopy(payload)))
        self.mod.s3.put_object=lambda **kw:self.writes.append(('s3',copy.deepcopy(kw)))
        return self.mod.lambda_handler({},None)
    def test_invalid_private_book_prevents_marks_and_both_publications(self):
        for pages in ([{}],[{'Items':[position()],'LastEvaluatedKey':{'pk':'POSITION','sk':'AAA'}},{}]):
            self.pages=pages;self.calls=[]
            with self.assertRaises(self.mod.BookReadUnavailable):self.handler()
            self.assertEqual(self.prices,[]);self.assertEqual(self.writes,[])
    def test_current_handler_preserves_exact_source_and_read_scope(self):
        precise=position(qty=Decimal('0.10000000000000000001'));self.pages=[{'Items':[precise]}]
        self.assertEqual(self.handler()['statusCode'],200);payload=self.writes[0][1]
        self.assertEqual(payload['accounting']['source_positions'][0]['qty'],{'source_number_type':'Decimal','representation':'0.10000000000000000001'})
        self.assertEqual(payload['accounting']['source_watchlist'],[])
        read=payload['accounting']['book_read'];self.assertEqual(read['consistency'],'STRONGLY_CONSISTENT_PAGES_NOT_ATOMIC_SNAPSHOT');self.assertEqual(read['position_count'],1);self.assertEqual(read['watchlist_count'],0)
        for key in ('started_at','completed_at'):self.assertIsNotNone(datetime.fromisoformat(read[key]).tzinfo)
        self.assertFalse(read['account_reconciled']);self.assertFalse(payload['capital_book']['allows_new_entries']);self.assertEqual(len(self.writes),2)
    def test_genuinely_empty_complete_book_can_publish_explicit_zero(self):
        self.pages=[{'Items':[]}];self.assertEqual(self.handler()['statusCode'],200);payload=self.writes[0][1]
        self.assertEqual(payload['positions'],[]);self.assertEqual(payload['portfolio_summary']['total_market_value'],0);self.assertEqual(payload['accounting']['book_read']['position_count'],0)

if __name__=='__main__':unittest.main()
