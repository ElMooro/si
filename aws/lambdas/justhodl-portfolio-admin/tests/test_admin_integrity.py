"""Complete invented owner records and mocked DDB/refresh; no private I/O."""
import base64,copy,hashlib,json,types,unittest
from decimal import Decimal
from pathlib import Path
import run_tests as support

ROOT=Path(__file__).resolve().parents[4]
BASE={'pk':'POSITION','sk':'AAA','symbol':'AAA','qty':Decimal('10'),'cost_basis_per_share':Decimal('100'),
      'cost_basis_total':Decimal('1000'),'position_type':'LONG','notes':'invented owner note'}


class AdminIntegrity(unittest.TestCase):
    def setUp(self):
        self.mod,self.table=support._load_admin([BASE]);self.refresh=[]
        self.mod._lam=types.SimpleNamespace(invoke=lambda **kw:self.refresh.append(kw) or {'StatusCode':202})

    def dispatch(self,action,**values):return self.mod._dispatch({'action':action,**values})
    def writes(self):return [row for row in self.table.calls if row[0] in {'put_item','update_item','delete_item'}]
    def http(self,raw,**changes):
        self.mod._admin_token=lambda:'invented-token'
        event={'requestContext':{'http':{'method':'POST'}},'headers':{'x-justhodl-token':'invented-token','origin':'https://justhodl.ai'},'body':raw,**changes}
        response=self.mod.lambda_handler(event,None);return response['statusCode'],json.loads(response['body']) if response.get('body') else {}

    def test_complete_original_fixtures_are_inert_and_byte_bound(self):
        audit=json.loads((ROOT/'docs/audit/2026-09-30/portfolio-admin-integrity.json').read_bytes())
        for row in audit['fixtures'].values():
            raw=(ROOT/row['path']).read_bytes();self.assertEqual(len(raw),row['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),row['sha256'])
        prior=json.loads((ROOT/'tests/fixtures/pre-portfolio-admin-integrity/complete-synthetic.json').read_bytes())
        self.assertEqual(len(prior['cases']),9)
        self.assertEqual(json.loads(prior['cases'][0]['response']['body'])['err'],'No update fields provided')

    def test_manager_legacy_field_names_apply_all_intended_changes(self):
        status,result=self.dispatch('update_position',symbol='AAA',new_qty=20,new_cost_basis_per_share=100,new_stop_loss=95,new_target_weight=5,new_notes='edited')
        self.assertEqual(status,200);self.assertTrue(result['ok']);row=self.table.items['POSITION','AAA']
        self.assertEqual([row[k] for k in ('qty','cost_basis_total','stop_loss','target_weight_pct','notes')],[20,2000,95,5,'edited'])
        self.assertEqual(result['snapshot_refresh'],'queued');self.assertEqual(len(self.refresh),1)

    def test_conflicting_aliases_and_unknown_fields_fail_before_reads(self):
        for values in ({'qty':20,'new_qty':21},{'qty':1,'new_qty':True},{'new_quantitty':20}):
            status,result=self.dispatch('update_position',symbol='AAA',**values)
            self.assertEqual(status,400);self.assertFalse(result['ok'])
        self.assertEqual(self.table.calls,[]);self.assertEqual(self.refresh,[])

    def test_typed_numbers_reject_booleans_strings_absence_and_nonfinite(self):
        for value in (True,False,'10','NaN',None,float('nan'),float('inf'),[],{}):
            status,result=self.dispatch('add_position',symbol='BBB',qty=value,cost_basis_per_share=100)
            self.assertEqual(status,400);self.assertFalse(result['ok'])
        self.assertEqual(self.writes(),[])

    def test_add_cannot_replace_existing_owner_state(self):
        before=copy.deepcopy(self.table.items)
        status,result=self.dispatch('add_position',symbol='AAA',qty=20,cost_basis_per_share=90)
        self.assertEqual(status,409);self.assertEqual(result['error_code'],'CONCURRENT_EDIT');self.assertEqual(before,self.table.items);self.assertFalse(self.refresh)

    def test_missing_stored_basis_cannot_default_to_zero(self):
        self.table.items['POSITION','AAA'].pop('cost_basis_per_share')
        status,result=self.dispatch('update_position',symbol='AAA',qty=20)
        self.assertEqual(status,400);self.assertFalse(result['ok']);self.assertFalse(self.writes())
        status,result=self.dispatch('update_position',symbol='AAA',notes='metadata still editable')
        self.assertEqual(status,200);self.assertNotIn('cost_basis_per_share',self.table.items['POSITION','AAA'])

    def test_exact_decimal_basis_and_signed_quantity_are_preserved(self):
        status,result=self.dispatch('add_position',symbol='BBB',qty=0.1,cost_basis_per_share=0.2)
        self.assertEqual(status,200);self.assertEqual(self.table.items['POSITION','BBB']['cost_basis_total'],Decimal('0.02'))
        status,result=self.dispatch('update_position',symbol='BBB',qty=-0.1)
        self.assertEqual(result['updated']['position_type'],'SHORT');self.assertEqual(self.table.items['POSITION','BBB']['cost_basis_total'],Decimal('-0.02'))
        status,result=self.dispatch('update_position',symbol='BBB',qty=0,cost_basis_per_share=0)
        self.assertEqual(result['updated']['cost_basis_total'],0);self.assertEqual(result['updated']['position_type'],'LONG')

    def test_invalid_cost_stop_and_numeric_overflow_never_write(self):
        for values in ({'cost_basis_per_share':-1},{'stop_loss':0},{'stop_loss':-1},{'target_weight_pct':True},{'qty':10**400}):
            status,result=self.dispatch('update_position',symbol='AAA',**values)
            self.assertEqual(status,400);self.assertFalse(result['ok'])
        self.assertFalse(self.writes())

    def test_concurrent_cost_change_is_detected_even_without_old_updated_at(self):
        def other(table):table.items['POSITION','AAA'].update(cost_basis_per_share=Decimal('200'),cost_basis_total=Decimal('2000'),updated_at='invented-new-clock')
        self.table.before_write=other
        status,result=self.dispatch('update_position',symbol='AAA',qty=20)
        self.assertEqual(status,409);self.assertEqual(self.table.items['POSITION','AAA']['qty'],10)
        self.assertEqual(self.table.items['POSITION','AAA']['cost_basis_total'],2000);self.assertFalse(self.refresh)

    def test_concurrent_metadata_and_new_version_presence_are_protected(self):
        for change in ({'notes':'new owner note'},{'mutation_id':'new owner version'},{'updated_at':'new owner clock'}):
            self.table.items={('POSITION','AAA'):copy.deepcopy(BASE)}
            self.table.before_write=lambda table:table.items['POSITION','AAA'].update(change)
            status,result=self.dispatch('update_position',symbol='AAA',qty=20)
            self.assertEqual(status,409);self.assertEqual(self.table.items['POSITION','AAA']['qty'],10)

    def test_stale_form_token_blocks_before_write_and_retry(self):
        tag=self.mod._item_view(self.table.items['POSITION','AAA'])['record_etag']
        status,result=self.dispatch('update_position',symbol='AAA',qty=20,expected_record_etag=tag)
        self.assertEqual(status,200);self.assertNotEqual(result['updated']['record_etag'],tag)
        writes=len(self.writes());status,result=self.dispatch('update_position',symbol='AAA',qty=30,expected_record_etag=tag)
        self.assertEqual(status,409);self.assertEqual(result['error_code'],'STALE_EDIT');self.assertEqual(len(self.writes()),writes)
        status,result=self.dispatch('update_position',symbol='AAA',qty=30,expected_record_etag=None);self.assertEqual(status,400)

    def test_record_token_binds_all_source_fields_and_ignores_map_order(self):
        row=copy.deepcopy(BASE);tag=self.mod._record_tag(row)
        self.assertEqual(tag,self.mod._record_tag(dict(reversed(list(row.items())))))
        row['unrecognized']={'values':[True,0,None]};self.assertNotEqual(tag,self.mod._record_tag(row))
        row['cost_basis_total']=Decimal('9')
        view=self.mod._item_view(row)
        self.assertEqual(view['cost_basis_total'],9);self.assertEqual(view['computed_cost_basis_total'],1000)
        self.assertEqual(view['basis_reconciliation_status'],'MISMATCH');self.assertEqual(row['cost_basis_total'],9)

    def test_stop_update_cannot_create_a_missing_position(self):
        status,result=self.dispatch('set_stop_loss',symbol='BBB',stop_price=90)
        self.assertEqual(status,404);self.assertNotIn(('POSITION','BBB'),self.table.items);self.assertFalse(self.writes())

    def test_explicit_null_clears_optional_stop_but_not_quantity(self):
        self.table.items['POSITION','AAA']['stop_loss']=Decimal(90)
        status,result=self.dispatch('set_stop_loss',symbol='AAA',stop_price=None)
        self.assertEqual(status,200);self.assertNotIn('stop_loss',result['updated'])
        status,result=self.dispatch('update_position',symbol='AAA',qty=None);self.assertEqual(status,400)

    def test_conditional_delete_protects_a_concurrent_owner_edit(self):
        self.table.before_write=lambda table:table.items['POSITION','AAA'].update(notes='changed')
        status,result=self.dispatch('remove_position',symbol='AAA')
        self.assertEqual(status,409);self.assertEqual(self.table.items['POSITION','AAA']['notes'],'changed')

    def test_absent_delete_is_a_noop_without_refresh(self):
        status,result=self.dispatch('remove_position',symbol='BBB')
        self.assertEqual(status,200);self.assertFalse(result['existed']);self.assertFalse(result['changed'])
        self.assertEqual(result['snapshot_refresh'],'not_requested');self.assertFalse(self.refresh)

    def test_unknown_write_ack_does_not_claim_success_or_queue_refresh(self):
        self.table.response_override={}
        status,result=self.dispatch('update_position',symbol='AAA',qty=20)
        self.assertEqual(status,503);self.assertEqual(result['error_code'],'WRITE_UNCONFIRMED')
        self.assertEqual(self.table.items['POSITION','AAA']['qty'],20);self.assertFalse(self.refresh)

    def test_refresh_transport_failure_or_bad_ack_is_not_reported_queued(self):
        for response in ({},None,{'StatusCode':'202'},{'StatusCode':500}):
            self.mod._lam=types.SimpleNamespace(invoke=lambda **kw:response)
            status,result=self.dispatch('update_position',symbol='AAA',notes='changed')
            self.assertEqual(status,200);self.assertEqual(result['snapshot_refresh'],'unconfirmed')
        self.mod._lam=types.SimpleNamespace(invoke=lambda **kw:(_ for _ in ()).throw(IOError('invented')))
        status,result=self.dispatch('update_position',symbol='AAA',notes='changed again')
        self.assertTrue(result['ok']);self.assertEqual(result['snapshot_refresh'],'unconfirmed')

    def test_complete_pagination_preserves_all_rows_and_source_metadata(self):
        rows=[copy.deepcopy(BASE),{**BASE,'symbol':'BBB','sk':'BBB','unknown':{'zero':0}}];calls=[]
        def query(**kw):
            calls.append(kw)
            return {'Items':[rows[1]]} if 'ExclusiveStartKey' in kw else {'Items':[rows[0]],'LastEvaluatedKey':{'pk':'POSITION','sk':'AAA'}}
        self.table.query=query
        status,result=self.dispatch('list',filter='POSITION')
        self.assertEqual(status,200);self.assertEqual(result['counts']['positions'],2);self.assertTrue(result['list_complete'])
        self.assertEqual(result['positions'][1]['unknown'],{'zero':0});self.assertTrue(all(c['ConsistentRead'] for c in calls))
        self.assertIn('NOT_ATOMIC_SNAPSHOT',result['list_consistency'])

    def test_failed_duplicate_or_repeated_pages_never_return_partial_books(self):
        variants=[{'Items':[BASE],'LastEvaluatedKey':{'pk':'POSITION','sk':'AAA'}},{'Items':[],'LastEvaluatedKey':{'pk':'POSITION','sk':'AAA'}},{'Items':None}]
        for response in variants:
            self.table.query=lambda **kw:response
            status,result=self.dispatch('list',filter='POSITION')
            self.assertIn(status,(409,503));self.assertFalse(result['ok']);self.assertNotIn('positions',result)

    def test_list_failure_after_first_page_is_an_error_not_empty(self):
        def query(**kw):
            if 'ExclusiveStartKey' in kw:raise IOError('invented continuation failure')
            return {'Items':[BASE],'LastEvaluatedKey':{'pk':'POSITION','sk':'AAA'}}
        self.table.query=query
        status,result=self.dispatch('list',filter='POSITION');self.assertEqual(status,503);self.assertNotIn('positions',result)

    def test_invalid_symbols_filters_or_event_types_never_mutate(self):
        for symbol in (True,None,{},'',"AAA');alert(1)//",'a'*16):
            status,result=self.dispatch('add_position',symbol=symbol,qty=10,cost_basis_per_share=100);self.assertEqual(status,400)
        for value in (False,{},'UNKNOWN'):
            status,result=self.dispatch('list',filter=value);self.assertEqual(status,400)
        self.assertEqual(self.mod._dispatch([])[0],400);self.assertFalse(self.writes())

    def test_http_duplicate_keys_nonfinite_overflow_and_invalid_unicode_are_rejected(self):
        for raw in ('{"action":"list","action":"add_position"}','{"action":"list","n":NaN}','{"action":"list","n":1e999}','{"action":"list","n":"\\ud800"}','[]','null'):
            self.assertEqual(self.http(raw)[0],400)
        self.assertEqual(self.table.calls,[])

    def test_http_auth_and_method_fail_before_account_reads(self):
        for headers in ({},{'x-justhodl-token':'wrong'},{'x-justhodl-token':'invented-token','origin':'https://evil.invalid'}):
            self.assertEqual(self.http('{"action":"list"}',headers=headers)[0],403)
        self.assertEqual(self.http('{}',requestContext={'http':{'method':'GET'}})[0],405)
        self.assertEqual(self.http('{}',requestContext={})[0],400);self.assertEqual(self.table.calls,[])

    def test_missing_configured_token_cannot_authorize_absent_header(self):
        self.mod._admin_token=lambda:None
        response=self.mod.lambda_handler({'requestContext':{'http':{'method':'POST'}},'headers':{},'body':'{"action":"list"}'},None)
        self.assertEqual(response['statusCode'],503);self.assertEqual(self.table.calls,[])

    def test_base64_body_requires_valid_encoding_and_complete_object(self):
        status,result=self.http(base64.b64encode(b'{"action":"list","filter":"POSITION"}').decode(),isBase64Encoded=True)
        self.assertEqual(status,200);self.assertEqual(result['counts']['positions'],1)
        for raw in ('@@@','/w==',base64.b64encode(b'[]').decode()):self.assertEqual(self.http(raw,isBase64Encoded=True)[0],400)

    def test_read_and_write_unknown_fields_are_preserved(self):
        self.table.items['POSITION','AAA']['unknown']={'nested':['owner',False,0,None]}
        status,result=self.dispatch('update_position',symbol='AAA',qty=20)
        self.assertEqual(result['updated']['unknown'],{'nested':['owner',False,0,None]})
        self.assertTrue(self.table.calls[0][1]['ConsistentRead'])

    def test_oversized_inputs_and_unbounded_pages_fail_explicitly(self):
        status,result=self.dispatch('update_position',symbol='AAA',notes='x'*70000);self.assertEqual(status,413);self.assertFalse(self.writes())
        count=0
        def query(**kw):
            nonlocal count
            count+=1;return {'Items':[],'LastEvaluatedKey':{'pk':'POSITION','sk':str(count)}}
        self.table.query=query;status,result=self.dispatch('list',filter='POSITION')
        self.assertEqual(status,413);self.assertEqual(count,100);self.assertNotIn('positions',result)


if __name__=='__main__':unittest.main()
