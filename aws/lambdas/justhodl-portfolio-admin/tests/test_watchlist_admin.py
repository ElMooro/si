"""Current admin and sync source with complete invented records; no actual private I/O."""
import ast,copy,hashlib,importlib.util,json,sys,types,unittest
from pathlib import Path
from boto3.dynamodb.types import TypeDeserializer
import run_tests as support
ROOT=Path(__file__).resolve().parents[4]
META={'pk':'SYNC_META','sk':'AUTO_WATCHLIST_V1','version':'11111111-1111-4111-8111-111111111111','source_generated_at':'2026-09-30T07:00:00Z','source_sha256':'a'*64,'updated_at':'2026-09-30T07:01:00Z'}
def row(symbol='AAA',source='AUTO_TIER_S'):
    return {'pk':'WATCHLIST','sk':symbol,'symbol':symbol,'source':source,'added_at':'2026-09-29T00:00:00Z','notes':'Invented owner note','extra':{'complete':[1,2,3]}}
class Rejected(Exception):
    response={'Error':{'Code':'TransactionCanceledException'}}
class Transactions:
    def __init__(self,table):self.table=table;self.calls=[];self.before=None;self.fail=None;self.ack={'ResponseMetadata':{'HTTPStatusCode':200}}
    def transact_write_items(self,**request):
        self.calls.append(copy.deepcopy(request))
        if self.before:self.before(self.table)
        if self.fail:raise self.fail
        decoder=TypeDeserializer();decoded=[];keys=set()
        for action in request['TransactItems']:
            kind,body=next(iter(action.items()));body=copy.deepcopy(body)
            for name in ('Key','ExpressionAttributeValues'):
                if name in body:body[name]={k:decoder.deserialize(v) for k,v in body[name].items()}
            key=(body['Key']['pk'],body['Key']['sk']);assert key not in keys;keys.add(key)
            try:self.table.condition(self.table.items.get(key),body)
            except RuntimeError:raise Rejected() from None
            decoded.append((kind,key))
        after=copy.deepcopy(self.table.items)
        for kind,key in decoded:
            if kind=='Delete':after.pop(key)
            else:assert kind=='ConditionCheck'
        self.table.items=after
        return copy.deepcopy(self.ack)
class WatchlistAdmin(unittest.TestCase):
    def setUp(self):
        self.mod,self.table=support._load_admin([row(),row('BBB','AUTO_TIER_A'),row('MAN','MANUAL'),META]);self.tx=Transactions(self.table);self.mod._ddb_client=self.tx
        self.refresh=[];self.mod._lam=types.SimpleNamespace(invoke=lambda **kw:self.refresh.append(kw) or {'StatusCode':202})
    def dispatch(self,action,**values):return self.mod._dispatch({'action':action,**values})
    def writes(self):return [r for r in self.table.calls if r[0] in {'put_item','update_item','delete_item'}]
    def test_complete_inert_originals_are_byte_bound(self):
        audit=json.loads((ROOT/'docs/audit/2026-09-30/watchlist-admin-integrity.json').read_bytes())
        for entry in audit['fixtures'].values():
            raw=(ROOT/entry['path']).read_bytes();self.assertEqual(len(raw),entry['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),entry['sha256'])
        evidence=json.loads((ROOT/'tests/fixtures/pre-watchlist-admin-integrity/complete-synthetic.json').read_bytes());self.assertEqual(len(evidence['cases']),8)
    def test_existing_record_cannot_be_replaced(self):
        before=copy.deepcopy(self.table.items);status,result=self.dispatch('add_watchlist',symbol='AAA')
        self.assertEqual(status,409);self.assertEqual(before,self.table.items);self.assertFalse(result['ok'])
    def test_typed_create_preserves_source_notes_and_tags(self):
        status,result=self.dispatch('add_watchlist',symbol=' new ',source='CUSTOM_RESEARCH',notes='')
        self.assertEqual(status,200);self.assertEqual(result['item']['symbol'],'NEW');self.assertEqual(result['item']['source'],'CUSTOM_RESEARCH');self.assertEqual(result['item']['notes'],'');self.assertRegex(result['item']['record_etag'],r'^[0-9a-f]{64}$');self.assertEqual(self.refresh,[])
    def test_invalid_create_fields_never_read_or_write(self):
        for args in ({'symbol':True},{'symbol':'A B'},{'symbol':'NEW','source':None},{'symbol':'NEW','source':''},{'symbol':'NEW','source':' MANUAL'},{'symbol':'NEW','notes':True},{'symbol':'NEW','notes':'x'*20001},{'symbol':'NEW','qty':1}):
            self.assertEqual(self.dispatch('add_watchlist',**args)[0],400)
        self.assertEqual(self.table.calls,[])
    def test_unknown_add_acknowledgement_is_not_success(self):
        self.table.response_override={};status,result=self.dispatch('add_watchlist',symbol='NEW')
        self.assertEqual(status,503);self.assertEqual(result['error_code'],'WRITE_UNCONFIRMED');self.assertIn(('WATCHLIST','NEW'),self.table.items);self.assertEqual(len(self.writes()),1)
    def test_exact_token_remove_preserves_unrelated_records(self):
        item=self.table.items['WATCHLIST','AAA'];status,result=self.dispatch('remove_watchlist',symbol='AAA',expected_record_etag=self.mod._record_tag(item))
        self.assertEqual(status,200);self.assertTrue(result['existed']);self.assertEqual(result['removed_item']['extra'],item['extra']);self.assertNotIn(('WATCHLIST','AAA'),self.table.items);self.assertIn(('WATCHLIST','MAN'),self.table.items);self.assertEqual(self.refresh,[])
    def test_stale_and_invalid_removal_tokens_do_not_write(self):
        for value in ('0'*64,None,True,'short'):
            self.assertIn(self.dispatch('remove_watchlist',symbol='AAA',expected_record_etag=value)[0],(400,409))
        self.assertEqual(self.writes(),[])
    def test_concurrent_manual_conversion_protects_removal(self):
        self.table.before_write=lambda t:t.items['WATCHLIST','AAA'].update(source='MANUAL',notes='Invented new owner note')
        status,result=self.dispatch('remove_watchlist',symbol='AAA');self.assertEqual(status,409);self.assertEqual(self.table.items['WATCHLIST','AAA']['source'],'MANUAL')
    def test_missing_removal_is_noop_and_unknown_ack_never_retries(self):
        self.assertFalse(self.dispatch('remove_watchlist',symbol='ABSENT')[1]['changed']);self.assertEqual(self.writes(),[])
        self.table.response_override={};status,result=self.dispatch('remove_watchlist',symbol='AAA');self.assertEqual(status,503);self.assertEqual(result['error_code'],'WRITE_UNCONFIRMED');self.assertEqual(len(self.writes()),1)
    def test_clear_uses_one_atomic_transaction_and_exact_owners(self):
        for item in (row('CUSTOM','AUTO_CUSTOM'),row('UNKNOWN',None),row('BADOWNER',[])):self.table.items[item['pk'],item['sk']]=item
        before_meta=copy.deepcopy(self.table.items['SYNC_META','AUTO_WATCHLIST_V1']);status,result=self.dispatch('clear_auto_watchlist')
        self.assertEqual(status,200);self.assertEqual(result['deleted_symbols'],['AAA','BBB']);self.assertEqual(result['deleted_count'],2);self.assertEqual(len(self.tx.calls),1);self.assertEqual(self.writes(),[])
        self.assertEqual(self.table.items['SYNC_META','AUTO_WATCHLIST_V1'],before_meta)
        self.assertEqual({k[1] for k in self.table.items if k[0]=='WATCHLIST'},{'MAN','CUSTOM','UNKNOWN','BADOWNER'});self.assertTrue(result['later_sync_may_add_rows']);self.assertEqual(self.refresh,[])
    def test_complete_second_page_is_included_and_all_reads_consistent(self):
        original=self.table.query;rows=[row(),row('BBB','AUTO_TIER_A'),row('MAN','MANUAL')]
        def query(**kw):
            self.table.calls.append(('query',copy.deepcopy(kw)))
            return {'Items':rows[1:]} if 'ExclusiveStartKey' in kw else {'Items':rows[:1],'LastEvaluatedKey':{'pk':'WATCHLIST','sk':'AAA'}}
        self.table.query=query;status,result=self.dispatch('clear_auto_watchlist');self.assertEqual(status,200);self.assertEqual(result['deleted_count'],2)
        self.assertTrue(all(r[1]['ConsistentRead'] is True for r in self.table.calls if r[0] in ('query','get_item')))
    def test_page_failure_and_cursor_cycle_prevent_any_transaction(self):
        self.table.query=lambda **kw:{'Items':[],'LastEvaluatedKey':{'pk':'WATCHLIST','sk':'AAA'}}
        self.assertEqual(self.dispatch('clear_auto_watchlist')[0],503);self.assertEqual(self.tx.calls,[])
        self.table.query=lambda **kw:(_ for _ in ()).throw(RuntimeError('Invented read failure'))
        self.assertEqual(self.dispatch('clear_auto_watchlist')[0],503);self.assertEqual(self.tx.calls,[])
    def test_one_concurrent_conversion_cancels_all_deletions(self):
        self.tx.before=lambda t:t.items['WATCHLIST','AAA'].update(source='MANUAL')
        status,result=self.dispatch('clear_auto_watchlist');self.assertEqual(status,409);self.assertIn(('WATCHLIST','BBB'),self.table.items);self.assertIn(('WATCHLIST','AAA'),self.table.items);self.assertNotIn('deleted_count',result);self.assertEqual(len(self.tx.calls),1)
    def test_absent_standard_field_added_concurrently_cancels_all(self):
        self.tx.before=lambda t:t.items['WATCHLIST','AAA'].update(sync_version='new revision')
        self.assertEqual(self.dispatch('clear_auto_watchlist')[0],409);self.assertIn(('WATCHLIST','BBB'),self.table.items)
    def test_concurrent_sync_checkpoint_change_cancels_all(self):
        self.tx.before=lambda t:t.items['SYNC_META','AUTO_WATCHLIST_V1'].update(version='22222222-2222-4222-8222-222222222222')
        self.assertEqual(self.dispatch('clear_auto_watchlist')[0],409);self.assertIn(('WATCHLIST','AAA'),self.table.items)
    def test_absent_checkpoint_is_guarded_without_creating_fake_provenance(self):
        self.table.items.pop(('SYNC_META','AUTO_WATCHLIST_V1'));status,result=self.dispatch('clear_auto_watchlist')
        self.assertEqual(status,200);self.assertNotIn(('SYNC_META','AUTO_WATCHLIST_V1'),self.table.items)
        self.setUp();self.table.items.pop(('SYNC_META','AUTO_WATCHLIST_V1'));self.tx.before=lambda t:t.items.update({('SYNC_META','AUTO_WATCHLIST_V1'):copy.deepcopy(META)})
        self.assertEqual(self.dispatch('clear_auto_watchlist')[0],409);self.assertIn(('WATCHLIST','AAA'),self.table.items)
    def test_full_population_token_rejects_stale_clear(self):
        status,result=self.dispatch('list',filter='WATCHLIST');self.assertEqual(status,200);token=result['watchlist_etag'];self.assertEqual(len(result['watchlist']),3);self.assertTrue(all('record_etag' in r for r in result['watchlist']))
        self.table.items['WATCHLIST','MAN']['notes']='Invented changed owner note';self.assertEqual(self.dispatch('clear_auto_watchlist',expected_watchlist_etag=token)[0],409);self.assertEqual(self.tx.calls,[])
    def test_more_than_99_selected_rows_is_not_split_or_truncated(self):
        self.table.items={(r['pk'],r['sk']):r for r in [row('S'+str(i)) for i in range(100)]}
        self.assertEqual(self.dispatch('clear_auto_watchlist')[0],413);self.assertEqual(len(self.table.items),100);self.assertEqual(self.tx.calls,[])
    def test_transaction_byte_bound_cannot_partially_delete(self):
        for i in range(15):r=row('S'+str(i));r['notes']='x'*250000;self.table.items[r['pk'],r['sk']]=r
        self.assertEqual(self.dispatch('clear_auto_watchlist')[0],413);self.assertEqual(self.tx.calls,[])
    def test_unknown_transaction_acknowledgement_does_not_claim_deletions(self):
        for ack in ({},{'ResponseMetadata':{'HTTPStatusCode':True}},{'ResponseMetadata':{'HTTPStatusCode':200.0}}):
            self.setUp();self.tx.ack=ack;status,result=self.dispatch('clear_auto_watchlist');self.assertEqual(status,503);self.assertNotIn('deleted_count',result);self.assertNotIn(('WATCHLIST','AAA'),self.table.items);self.assertEqual(len(self.tx.calls),1)
    def test_transaction_failure_has_no_unconditional_fallback(self):
        self.tx.fail=PermissionError('Invented denied transaction');before=copy.deepcopy(self.table.items);status,result=self.dispatch('clear_auto_watchlist')
        self.assertEqual(status,503);self.assertEqual(before,self.table.items);self.assertEqual(self.writes(),[]);self.assertEqual(len(self.tx.calls),1)
    def test_empty_and_manual_only_population_do_not_mutate(self):
        for rows in ([],[row('MAN','MANUAL')]):
            self.table.items={(r['pk'],r['sk']):r for r in rows};status,result=self.dispatch('clear_auto_watchlist');self.assertEqual(status,200);self.assertFalse(result['changed']);self.assertEqual(result['deleted_count'],0)
        self.assertEqual(self.tx.calls,[])
    def test_invalid_selected_identity_and_unexpected_fields_fail_before_write(self):
        self.table.items['WATCHLIST','AAA']['symbol']='BBB';self.assertEqual(self.dispatch('clear_auto_watchlist')[0],409);self.assertEqual(self.tx.calls,[])
        self.assertEqual(self.dispatch('clear_auto_watchlist',limit=1)[0],400);self.assertEqual(self.dispatch('remove_watchlist',symbol='AAA',source='MANUAL')[0],400)
    def test_population_token_does_not_depend_on_query_order(self):
        a,b=row(),row('BBB','AUTO_TIER_A');self.assertEqual(self.mod._watchlist_tag([a,b]),self.mod._watchlist_tag([b,a]))
    def test_clear_preserves_sync_checkpoint_idempotency(self):
        snapshot=support._load_snapshot({});status,result=self.dispatch('clear_auto_watchlist');self.assertEqual(status,200)
        meta=self.table.items['SYNC_META','AUTO_WATCHLIST_V1']
        desired={'AAA':'AUTO_TIER_S','BBB':'AUTO_TIER_A'}
        provenance={'source_generated_at':meta['source_generated_at'],'source_sha256':meta['source_sha256']}
        operations,out=snapshot.plan([r for (pk,sk),r in self.table.items.items() if pk=='WATCHLIST'],meta,desired,provenance,None,'invented-table','22222222-2222-4222-8222-222222222222')
        self.assertEqual(operations,[]);self.assertEqual(out['status'],'UNCHANGED');self.assertNotIn(('WATCHLIST','AAA'),self.table.items)
    def test_current_position_and_http_functions_are_unchanged(self):
        def functions(path):return {n.name:ast.dump(n,include_attributes=False) for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef)}
        before=functions(ROOT/'tests/fixtures/pre-watchlist-admin-integrity/lambda_function.py.txt');after=functions(ROOT/'aws/lambdas/justhodl-portfolio-admin/source/lambda_function.py')
        modified={'_observed_condition','add_watchlist','remove_watchlist','clear_auto_watchlist','list_items'}
        self.assertTrue(set(before)<=set(after))
        for name,source in before.items():
            if name not in modified:self.assertEqual(source,after[name],name)
    def test_complete_transaction_matches_local_aws_service_model(self):
        from botocore.session import Session
        from botocore.validate import validate_parameters
        self.assertEqual(self.dispatch('clear_auto_watchlist')[0],200)
        shape=Session().get_service_model('dynamodb').operation_model('TransactWriteItems').input_shape
        validate_parameters(self.tx.calls[0],shape)

if __name__=='__main__':unittest.main()
