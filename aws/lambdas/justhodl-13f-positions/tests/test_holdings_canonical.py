"""Actual native fixtures, publication failures and the retired-writer boundary."""
import ast
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch

import holdings_canonical as c
import holdings_native as m
import holdings_store as store
from test_holdings_store import MemoryS3, S3Error
from test_native_holdings import original_filing, CURRENT, PRIOR


def fixture():
    s3 = MemoryS3()
    a = m.resolve_period([original_filing(CURRENT)], True)
    b = m.resolve_period([original_filing(PRIOR)], True)
    compared = m.compare(a, b)
    doc = {'contract': 'holdings-native-fund.v1', 'fund': 'BERKSHIRE', 'cik': '0001067983',
           'official_name': 'BERKSHIRE HATHAWAY INC', 'periods': {'2026-06-30': a, '2026-03-31': b},
           'current_holdings_period': '2026-06-30', 'prior_holdings_period': '2026-03-31', 'comparison': compared}
    raw = m.encoded(doc); sha = hashlib.sha256(raw).hexdigest(); key = m.PREFIX + 'funds/' + sha + '.json'
    s3.objects[key] = raw
    summary = {'cik': doc['cik'], 'official_name': doc['official_name'], 'current_holdings_period': '2026-06-30',
               'prior_holdings_period': '2026-03-31', 'comparison_counts': compared['counts'],
               'detail': store.reference(key, raw), **m.PERMISSION}
    research = {'contract': m.CONTRACT, 'generated_at': '2026-09-19T22:00:00Z', 'source_generated_at': '2026-09-19T21:59:00Z',
                'funds': {'BERKSHIRE': summary}, 'report_period_cohorts': {'2026-06-30': ['BERKSHIRE']},
                'current_cohort_count': 1, 'required_period_for_current_cohort': '2026-06-30',
                'quality': {'status': 'descriptive_source_research'}, 'methodology_url': m.GUIDANCE,
                'limitations': ['SYNTHETIC fixture; original complete Berkshire filings.'], 'legacy_snapshot': {}, **m.PERMISSION}
    binding = {'manifest_key': m.PREFIX + 'runs/' + 'a'*64 + '.json', 'output_sha256': m.digest(research)}
    output = m.encoded(research); out_key = m.PREFIX + 'outputs/' + m.digest(research) + '.json'
    s3.objects[out_key] = output
    s3.objects[binding['manifest_key']] = m.encoded({'output': store.reference(out_key, output)})
    s3.objects[m.CURRENT] = m.encoded({**research, 'replay': binding})
    return s3, research, binding


class CanonicalHoldings(unittest.TestCase):
    def test_complete_real_quantities_indexed_without_trade_or_ticker_guess(self):
        s3, research, binding = fixture()
        products, artifacts = c.build(research, binding, store.reader(s3, 'fixture'))
        self.assertEqual(set(products), set(c.KEYS))
        packet = products[c.CURRENT]
        index = json.loads(artifacts[packet['security_indexes']['2026-06-30']['key']])
        self.assertEqual(index['manager_comparison_count'], packet['manager_comparison_count'])
        ko = next(v for v in index['securities'].values() if v['identity']['cusip'] == '191216100')
        self.assertEqual(ko['reported_managers'][0]['disclosure_status'], 'reported_quantity_unchanged')
        ref = ko['reported_managers'][0]['fund_detail']
        doc = store.verified(ref, store.reader(s3, 'fixture'), 'funds')
        pos = doc['periods']['2026-06-30']['positions'][ko['reported_managers'][0]['position_id']]
        self.assertEqual(pos['reported_quantity'], '400000000')
        self.assertEqual(len(pos['row_ids']), 10)
        self.assertEqual(packet['by_fund']['BERKSHIRE']['name'], 'BERKSHIRE HATHAWAY INC')
        for value in products.values():
            self.assertFalse(value['calls_eligible']); self.assertIsNone(value['call'])
            self.assertIsNone(value['transaction_inference']['net_capital_flow_usd'])
            self.assertFalse(value['compatibility']['consumer_migration_complete'])
        self.assertEqual(packet['aggregate_by_ticker'], {})
        self.assertEqual(products[c.KEYS[1]]['t'], {})
        self.assertEqual(packet['most_bought'], [])

    def test_corrupt_complete_fund_fails_before_product_creation(self):
        s3, research, binding = fixture()
        s3.objects[research['funds']['BERKSHIRE']['detail']['key']] += b' '
        with self.assertRaisesRegex(ValueError, 'bytes differ'):
            c.build(research, binding, store.reader(s3, 'fixture'))
        self.assertEqual(s3.writes, [])

    def test_source_binding_cannot_be_replaced_by_forged_eligibility(self):
        s3, research, binding = fixture(); research['calls_eligible'] = True
        with self.assertRaisesRegex(ValueError, 'binding differs'):
            c.build(research, binding, store.reader(s3, 'fixture'))

    def test_unsupported_research_compiler_cannot_be_executed(self):
        s3, _, binding = fixture()
        with self.assertRaisesRegex(ValueError, 'run hash differs'):
            c.source(binding['manifest_key'], store.reader(s3, 'fixture'))

    def test_complete_old_products_preserved_and_commit_pointer_written_last(self):
        s3, research, binding = fixture()
        old = {key: m.encoded({'whole_legacy': key, 'rows': list(range(100))}) for key in c.KEYS}
        s3.objects.update(old)
        with patch.object(c, 'source', return_value=(research, binding)):
            result = c.run(s3, 'fixture')
        self.assertTrue(result['published'])
        self.assertEqual(result['changed_aliases'], [*c.KEYS[1:], c.CURRENT])
        self.assertEqual([key for key, _ in s3.writes if key in c.KEYS], [*c.KEYS[1:], c.CURRENT])
        for key, raw in old.items():
            self.assertEqual(s3.objects[store.PRIVATE + hashlib.sha256(raw).hexdigest() + '.bin'], raw)
        manifest = c.verified(result['canonical_replay'], store.reader(s3, 'fixture'), 'runs')
        with patch.object(c, 'source', return_value=(research, binding)):
            products = c.replay(manifest, store.reader(s3, 'fixture'))
        for key in c.KEYS:
            alias = json.loads(s3.objects[key])
            self.assertEqual(alias.pop('canonical_replay'), result['canonical_replay'])
            self.assertEqual(alias, products[key])
        n = len([key for key, _ in s3.writes if key in c.KEYS])
        with patch.object(c, 'source', return_value=(research, binding)):
            repeated = c.run(s3, 'fixture')
        self.assertEqual(repeated['changed_aliases'], [])
        self.assertEqual(n, len([key for key, _ in s3.writes if key in c.KEYS]))

    def test_partial_alias_failure_does_not_commit_and_retry_repairs_same_bytes(self):
        s3, research, binding = fixture(); old = b'{"whole":"previous source"}'
        for key in c.KEYS: s3.objects[key] = old
        real_put = s3.put_object
        def fail(**kwargs):
            if kwargs['Key'] == c.KEYS[3]: raise RuntimeError('injected sidecar failure')
            return real_put(**kwargs)
        with patch.object(c, 'source', return_value=(research, binding)), patch.object(s3, 'put_object', side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, 'injected'):
                c.run(s3, 'fixture')
        self.assertEqual(s3.objects[c.CURRENT], old)
        with patch.object(c, 'source', return_value=(research, binding)):
            repaired = c.run(s3, 'fixture')
        self.assertTrue(repaired['published'])
        self.assertTrue(all(json.loads(s3.objects[k])['canonical_replay'] == repaired['canonical_replay'] for k in c.KEYS))

    def test_concurrent_commit_pointer_change_is_not_overwritten(self):
        s3, research, binding = fixture(); s3.objects[c.CURRENT] = b'{"old":true}'
        real_put = s3.put_object
        def race(**kwargs):
            if kwargs['Key'] == c.CURRENT:
                s3.objects[c.CURRENT] = b'{"concurrent":"writer"}'
            return real_put(**kwargs)
        with patch.object(c, 'source', return_value=(research, binding)), patch.object(s3, 'put_object', side_effect=race):
            with self.assertRaises(S3Error): c.run(s3, 'fixture')
        self.assertEqual(s3.objects[c.CURRENT], b'{"concurrent":"writer"}')

    def test_newer_source_cannot_be_replaced_by_old_research(self):
        s3, research, binding = fixture()
        newer = m.encoded({'contract': c.CONTRACT, 'source_generated_at': '2026-09-20T00:00:00Z'})
        s3.objects[c.CURRENT] = newer
        with patch.object(c, 'source', return_value=(research, binding)):
            result = c.run(s3, 'fixture')
        self.assertFalse(result['published'])
        self.assertEqual(s3.objects[c.CURRENT], newer)
        self.assertFalse(any(key in c.KEYS for key, _ in s3.writes))

    def test_normal_fresh_schedule_never_calls_legacy_or_collects_twice(self):
        s3, _, _ = fixture()
        s3.objects[m.CURRENT] = m.encoded({'source_generated_at': datetime.now(timezone.utc).isoformat()})
        with patch.object(store, 'handle', side_effect=AssertionError('unexpected collection')), patch.object(c, 'run', return_value={'published': True}) as run:
            result = c.handle({}, s3, 'fixture', 'fixture-UA')
        self.assertEqual(result['statusCode'], 200); run.assert_called_once()

    def test_new_filing_collects_then_publishes_with_authentic_invocation_id(self):
        response = {'statusCode': 200, 'body': '{"published":true}'}
        order = []
        def acquire(*args, **kwargs): order.append('collect'); return response
        def publish(*args): order.append('publish'); return {'published': True}
        with patch.object(store, 'handle', side_effect=acquire) as collection, patch.object(c, 'run', side_effect=publish):
            c.handle({'trigger': 'new_filing'}, object(), 'fixture', 'fixture-UA', request_id='real-runtime-id')
        self.assertEqual(order, ['collect', 'publish'])
        self.assertEqual(collection.call_args.args[0]['request_id'], 'holdings-real-runtime-id')
        self.assertEqual(collection.call_args.args[0]['action'], 'holdings_research_collect')

    def test_read_action_does_not_publish_or_collect(self):
        s3, _, _ = fixture()
        with patch.object(c, 'run', side_effect=AssertionError('unexpected publication')):
            result = c.handle({'action': 'holdings_research_read'}, s3, 'fixture', 'fixture-UA')
        self.assertEqual(result['statusCode'], 200); self.assertEqual(s3.writes, [])

    def test_unknown_action_cannot_reach_historical_producer(self):
        with self.assertRaisesRegex(ValueError, 'retired'):
            c.handle({'action': 'legacy'}, object(), 'fixture', 'fixture-UA')

    def test_actual_lambda_entrypoint_dispatches_all_events_to_native_boundary(self):
        tree = ast.parse((Path(__file__).parents[1]/'source/lambda_function.py').read_text(encoding='utf-8'))
        handler = next(v for v in tree.body if isinstance(v, ast.FunctionDef) and v.name == 'lambda_handler')
        code = ast.Module(body=[handler], type_ignores=[])
        namespace = {'s3': object(), 'S3_BUCKET': 'fixture', 'USER_AGENT': 'fixture-UA',
                     'legacy_lambda_handler': lambda *_: self.fail('retired writer reached')}
        exec(compile(ast.fix_missing_locations(code), '<actual-native-dispatch>', 'exec'), namespace)
        for event in (None, {}, {'trigger': 'new_filing'}, {'action': 'holdings_research_refresh'}):
            with patch.object(c, 'handle', return_value='canonical') as handle:
                self.assertEqual(namespace['lambda_handler'](event, None), 'canonical')
                self.assertEqual(handle.call_args.args[0], event)


if __name__ == '__main__': unittest.main()
