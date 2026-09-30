"""Complete invented table/source cases; no AWS, provider or account reads."""
from pathlib import Path
from datetime import datetime, timezone, timedelta
import copy
import ast
import importlib.util
import io
import json
import sys
import types
import unittest
from unittest.mock import patch
from boto3.dynamodb.types import TypeDeserializer, TypeSerializer

SOURCE = Path(__file__).resolve().parents[1] / 'source'
sys.path.insert(0, str(SOURCE))
sys.path.insert(0,str(SOURCE.parents[2]/'shared'))
_fake=types.ModuleType('boto3');_fake.client=lambda *a,**k:types.SimpleNamespace();_fake.resource=lambda *a,**k:types.SimpleNamespace(Table=lambda n:None)
_spec=importlib.util.spec_from_file_location('current_snapshot_sync',SOURCE/'lambda_function.py');sync=importlib.util.module_from_spec(_spec)
with patch.dict(sys.modules,{'boto3':_fake}):_spec.loader.exec_module(sync)
NOW = datetime(2026, 9, 30, 5, 0, tzinfo=timezone.utc)


def frame(rows=None):
    rows = rows if rows is not None else [('NEW', 95), ('MID', 84), ('LOW', 60)]
    stocks = []
    tiers = {t: 0 for t in 'SABCD'}
    for i, (symbol, score) in enumerate(rows):
        tier = 'S' if score >= 90 else 'A' if score >= 80 else 'B' if score >= 70 else 'C' if score >= 50 else 'D'
        stocks.append({'symbol': symbol, 'alpha_score': score, 'rank': i + 1, 'tier': tier, 'complete_invented_extra': {'value': i}})
        tiers[tier] += 1
    return {'stocks': stocks, 'count': len(stocks), 'scored_count': len(stocks), 'tier_distribution': tiers, 'model_version': 'synthetic-v1', 'generated_at': NOW.isoformat(), 'inputs': {'screener_generated_at': NOW.isoformat()}}


def item(symbol='OLD', source='AUTO_TIER_S'):
    return {'pk': 'WATCHLIST', 'sk': symbol, 'symbol': symbol, 'source': source, 'added_at': '2026-09-29T05:00:00+00:00', 'notes': 'Complete invented owner note', 'extra': {'keep': 'entire original metadata'}}


class ConditionalConflict(Exception):
    response = {'Error': {'Code': 'TransactionCanceledException'}}


class Table:
    name = 'invented-portfolio-table'
    def __init__(self, rows=None, meta=None, before_write=None):
        rows = rows if rows is not None else [item(), item('MAN', 'MANUAL')]
        self.items = {(r['pk'], r['sk']): copy.deepcopy(r) for r in rows}
        if meta is not None:
            self.items[('SYNC_META', 'AUTO_WATCHLIST_V1')] = copy.deepcopy(meta)
        self.reads, self.writes, self.before_write = [], [], before_write
        self.fail = None
    def get_item(self, **request):
        assert request['ConsistentRead'] is True
        self.reads.append(copy.deepcopy(request))
        row = self.items.get(tuple(request['Key'][k] for k in ('pk', 'sk')))
        return {'Item': copy.deepcopy(row)} if row is not None else {}
    def query(self, **request):
        assert request['ConsistentRead'] is True
        self.reads.append(copy.deepcopy(request))
        return {'Items': copy.deepcopy([r for k, r in self.items.items() if k[0] == 'WATCHLIST'])}
    def transact_write_items(self, **request):
        self.writes.append(copy.deepcopy(request))
        if self.before_write:
            self.before_write(self)
        if self.fail:
            raise self.fail
        decoded = []
        deserializer = TypeDeserializer()
        for action in request['TransactItems']:
            kind, raw = next(iter(action.items()))
            body = copy.deepcopy(raw)
            for field in ('Item', 'Key', 'ExpressionAttributeValues'):
                if field in body:
                    body[field] = {k: deserializer.deserialize(v) for k, v in body[field].items()}
            decoded.append((kind, body))
        # Independently evaluate DynamoDB equality/absence guards before ANY edit.
        for kind, body in decoded:
            target = body.get('Key', body.get('Item'))
            old = self.items.get((target['pk'], target['sk']), {})
            names, values = body.get('ExpressionAttributeNames', {}), body.get('ExpressionAttributeValues', {})
            for term in body['ConditionExpression'].split(' AND '):
                if term.startswith('attribute_not_exists('):
                    alias = term[len('attribute_not_exists('):-1]
                    valid = names.get(alias, alias) not in old
                else:
                    alias, placeholder = term.split(' = ')
                    valid = names[alias] in old and old[names[alias]] == values[placeholder]
                if not valid:
                    raise ConditionalConflict()
        next_items = copy.deepcopy(self.items)
        for kind, body in decoded:
            target = body.get('Key', body.get('Item'));key = (target['pk'], target['sk'])
            if kind == 'Delete':
                next_items.pop(key)
            elif kind == 'Put':
                next_items[key] = body['Item']
            else:
                names, values = body['ExpressionAttributeNames'], body['ExpressionAttributeValues']
                for term in body['UpdateExpression'][4:].split(', '):
                    name, value = term.split(' = ');next_items[key][names[name]] = values[value]
        self.items = next_items
        return {'ResponseMetadata': {'HTTPStatusCode': 200}}


class WatchlistSyncTests(unittest.TestCase):
    def run_sync(self, table, data=None, now=NOW):
        return sync.sync_watchlist(table, table, frame() if data is None else data, now)
    def test_missing_empty_or_malformed_source_cannot_even_read_private_table(self):
        for data in ({}, {'stocks': []}, {'stocks': 'UNKNOWN'}, False, [], None):
            table = Table();before = copy.deepcopy(table.items)
            result = sync.sync_watchlist(table, table, data, NOW)
            self.assertEqual(result['status'], 'SKIPPED_INVALID_SOURCE');self.assertEqual(table.items, before)
            self.assertEqual(table.reads, []);self.assertEqual(table.writes, [])
    def test_complete_valid_revision_is_one_atomic_transaction(self):
        table = Table();manual = copy.deepcopy(table.items[('WATCHLIST', 'MAN')]);result = self.run_sync(table)
        self.assertEqual(result['status'], 'APPLIED');self.assertEqual(len(table.writes), 1)
        self.assertNotIn(('WATCHLIST', 'OLD'), table.items);self.assertIn(('WATCHLIST', 'NEW'), table.items)
        self.assertEqual(table.items[('WATCHLIST', 'MAN')], manual)
        self.assertEqual(result['added_S'], ['NEW']);self.assertEqual(result['added_A'], ['MID'])
    def test_manual_conversion_after_query_rejects_every_auto_mutation(self):
        def owner(t):t.items[('WATCHLIST', 'OLD')] = item('OLD', 'MANUAL')
        table = Table(before_write=owner);result = self.run_sync(table)
        self.assertEqual(result['status'], 'CONFLICT');self.assertEqual(table.items[('WATCHLIST', 'OLD')]['source'], 'MANUAL')
        self.assertNotIn(('WATCHLIST', 'NEW'), table.items);self.assertNotIn(('SYNC_META', 'AUTO_WATCHLIST_V1'), table.items)
    def test_concurrent_manual_add_cannot_be_overwritten(self):
        def owner(t):t.items[('WATCHLIST', 'NEW')] = item('NEW', 'MANUAL')
        table = Table(before_write=owner);result = self.run_sync(table)
        self.assertEqual(result['status'], 'CONFLICT');self.assertIn(('WATCHLIST', 'OLD'), table.items)
        self.assertEqual(table.items[('WATCHLIST', 'NEW')]['source'], 'MANUAL')
    def test_existing_manual_and_unknown_ownership_are_preserved(self):
        table = Table([item('NEW', 'MANUAL'), item('MID', 'IMPORT')]);before = copy.deepcopy(table.items)
        self.assertEqual(self.run_sync(table)['status'], 'APPLIED')
        for key, row in before.items():self.assertEqual(table.items[key], row)
    def test_tier_change_preserves_complete_metadata_and_uses_update(self):
        table = Table([item('NEW', 'AUTO_TIER_A')]);before = copy.deepcopy(table.items[('WATCHLIST', 'NEW')]);result = self.run_sync(table)
        after = table.items[('WATCHLIST', 'NEW')]
        for key in ('extra', 'notes', 'added_at'):self.assertEqual(after[key], before[key])
        self.assertEqual(after['source'], 'AUTO_TIER_S');self.assertEqual(result['removed_A'], ['NEW'])
    def test_same_revision_is_idempotent_and_does_not_resurrect_owner_deletion(self):
        table = Table();self.run_sync(table);table.items.pop(('WATCHLIST', 'NEW'));writes = len(table.writes)
        self.assertEqual(self.run_sync(table)['status'], 'UNCHANGED');self.assertEqual(len(table.writes), writes)
        self.assertNotIn(('WATCHLIST', 'NEW'), table.items)
    def test_older_or_equal_conflicting_revision_is_refused(self):
        table = Table();self.run_sync(table);before = copy.deepcopy(table.items)
        for stamp_value in ((NOW-timedelta(minutes=1)).isoformat(), NOW.isoformat()):
            data = frame();data['generated_at'] = stamp_value;data['extra'] = 'different whole source'
            self.assertEqual(self.run_sync(table, data)['status'], 'SKIPPED_INVALID_STATE');self.assertEqual(table.items, before)
    def test_concurrent_newer_sync_checkpoint_refuses_old_transaction(self):
        def advance(t):t.items[('SYNC_META', 'AUTO_WATCHLIST_V1')] = {**sync.META_KEY, 'version': 'newer'}
        table = Table(before_write=advance);result = self.run_sync(table)
        self.assertEqual(result['status'], 'CONFLICT');self.assertIn(('WATCHLIST', 'OLD'), table.items)
    def test_invalid_clocks_quality_and_complete_counts_never_mutate(self):
        variants = []
        for value in (None, True, '2026-09-30', (NOW+timedelta(minutes=6)).isoformat(), (NOW-timedelta(hours=4)).isoformat()):
            data = frame();data['generated_at'] = value;variants.append(data)
        for key, value in [('count', True), ('count', 2), ('scored_count', 2), ('quality', {'status': 'stale'}), ('tier_distribution', {})]:
            data = frame();data[key] = value;variants.append(data)
        data = frame();data['inputs']['screener_generated_at'] = (NOW-timedelta(hours=121)).isoformat();variants.append(data)
        for data in variants:
            table = Table();self.assertEqual(self.run_sync(table, data)['status'], 'SKIPPED_INVALID_SOURCE');self.assertEqual(table.writes, [])
    def test_invalid_rows_duplicate_identity_and_boolean_scores_are_rejected(self):
        variants = []
        for field, value in [('symbol', True), ('symbol', 'MID'), ('rank', True), ('rank', 2), ('tier', 'A'), ('alpha_score', True), ('alpha_score', float('nan')), ('alpha_score', 10**1000)]:
            data = frame();data['stocks'][0][field] = value;variants.append(data)
        data = frame();data['stocks'][1] = None;variants.append(data)
        for data in variants:
            table = Table();result = self.run_sync(table, data);self.assertEqual(result['status'], 'SKIPPED_INVALID_SOURCE');self.assertEqual(table.reads, [])
    def test_valid_nonempty_universe_without_top_tiers_can_remove_autos(self):
        table = Table();result = self.run_sync(table, frame([('LOW', 60)]))
        self.assertEqual(result['status'], 'APPLIED');self.assertNotIn(('WATCHLIST', 'OLD'), table.items)
    def test_no_silent_transaction_truncation(self):
        table = Table([item('OLD'+str(i)) for i in range(100)])
        result = self.run_sync(table);self.assertEqual(result['status'], 'SKIPPED_INVALID_STATE');self.assertEqual(len(table.items), 100);self.assertEqual(table.writes, [])
    def test_limits_follow_entire_validated_source_order(self):
        data = frame([(f'S{i}', 95) for i in range(13)]+[(f'A{i}', 85) for i in range(18)])
        table = Table([]);result = self.run_sync(table, data)
        self.assertEqual(len(result['added_S']), 10);self.assertEqual(len(result['added_A']), 15)
        self.assertNotIn(('WATCHLIST', 'S10'), table.items);self.assertNotIn(('WATCHLIST', 'A15'), table.items)
    def test_unknown_acknowledgement_does_not_claim_success(self):
        table = Table();table.fail = TimeoutError('invented transport timeout')
        result = self.run_sync(table);self.assertEqual(result['status'], 'WRITE_UNCONFIRMED');self.assertEqual(result['added_S'], [])
        table=Table();original=table.transact_write_items
        def missing_ack(**kw):original(**kw);return {}
        table.transact_write_items=missing_ack
        result=self.run_sync(table);self.assertEqual(result['status'],'WRITE_UNCONFIRMED');self.assertEqual(result['added_S'],[])
        self.assertIn(('SYNC_META','AUTO_WATCHLIST_V1'),table.items)
    def test_complete_pagination_is_read_and_repeated_cursor_rejected(self):
        table = Table();rows = list(table.items.values());calls = []
        def query(**request):
            calls.append(request)
            return {'Items': [rows[-1]]} if 'ExclusiveStartKey' in request else {'Items': [rows[0]], 'LastEvaluatedKey': {'pk':'WATCHLIST','sk':'OLD'}}
        table.query = query
        self.assertEqual(self.run_sync(table)['status'], 'APPLIED');self.assertEqual(len(calls), 2)
        table = Table();table.query=lambda **kw:{'Items': [], 'LastEvaluatedKey': {'pk':'WATCHLIST','sk':'OLD'}}
        self.assertEqual(self.run_sync(table)['reason_codes'], ['WATCHLIST_PAGINATION_REPEATED']);self.assertEqual(table.writes, [])
    def test_existing_note_edit_or_absent_row_blocks_transaction(self):
        for action in ('note', 'delete'):
            def owner(t):
                if action == 'note':t.items[('WATCHLIST', 'OLD')]['notes'] = 'owner changed note'
                else:t.items.pop(('WATCHLIST', 'OLD'))
            table=Table(before_write=owner);self.assertEqual(self.run_sync(table)['status'],'CONFLICT');self.assertNotIn(('WATCHLIST','NEW'),table.items)


def load_current():
    root=SOURCE.parents[2];sys.path.insert(0,str(root/'shared'))
    fake=types.ModuleType('boto3');fake.client=lambda *a,**k:types.SimpleNamespace();fake.resource=lambda *a,**k:types.SimpleNamespace(Table=lambda n:Table())
    spec=importlib.util.spec_from_file_location('current_snapshot_reader',SOURCE/'lambda_function.py');mod=importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules,{'boto3':fake}):spec.loader.exec_module(mod)
    return mod


class SourceReaderTests(unittest.TestCase):
    def test_reviewed_accounting_repair_preserves_watchlist_and_source_helpers(self):
        root=SOURCE.parents[3]
        def functions(path):return {n.name:n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef)}
        before=functions(root/'tests/fixtures/pre-snapshot-accounting/lambda_function.py.txt');after=functions(SOURCE/'lambda_function.py')
        self.assertEqual({name for name in before if ast.dump(before[name])!=ast.dump(after[name])},{'enrich_symbol','lambda_handler','query_pk','fetch_polygon_latest','load_s3_json','index_by_symbol'})
        self.assertEqual({name for name in after if name not in before},{'accounting_number','accounting_round','accounting_sum','accounting_source','accounting_symbol','build_holdings_accounting','parse_previous_close','read_previous_close_body','research_text','research_join','research_value'})
    def test_complete_body_and_eof_are_required_and_closed(self):
        mod=load_current();raw=json.dumps(frame()).encode();body=io.BytesIO(raw)
        mod.s3.get_object=lambda **kw:{'Body':body,'ContentLength':len(raw)}
        self.assertEqual(mod.load_s3_json('invented',{}),frame());self.assertTrue(body.closed)
    def test_invalid_lengths_json_types_duplicates_and_nonfinite_are_rejected(self):
        mod=load_current()
        for raw,length in [(b'{}',True),(b'{}',3),(b'{}',1),(b'[]',2),(b'{"stocks":[],"stocks":[]}',None),(b'{"n":NaN}',None),(b'{"n":1e999}',None),(b'{"s":"\\ud800"}',None),(b'\xff',1)]:
            body=io.BytesIO(raw);mod.s3.get_object=lambda **kw:{'Body':body,'ContentLength':len(raw) if length is None else length}
            self.assertEqual(mod.load_s3_json('invented',{}),{});self.assertTrue(body.closed)
    def test_failed_source_read_preserves_watchlist_through_actual_handler(self):
        mod=load_current();table=Table();before=copy.deepcopy(table.items);mod.table=table;mod.ddb_client=table
        mod.s3.get_object=lambda **kw:(_ for _ in ()).throw(IOError('invented failure'))
        mod.query_pk=lambda pk:[] if pk=='POSITION' else list(table.items.values())
        mod.batch_fetch_prices=lambda symbols:{};published=[];mod.publish_private=lambda kind,doc:published.append(doc);mod.s3.put_object=lambda **kw:None
        result=mod.lambda_handler({},None)
        self.assertEqual(result['statusCode'],200);self.assertEqual(table.items,before);self.assertEqual(table.writes,[])
        self.assertEqual(published[0]['watchlist_sync']['status'],'SKIPPED_INVALID_SOURCE');self.assertEqual(len(published[0]['watchlist']),2)


if __name__=='__main__':unittest.main()
