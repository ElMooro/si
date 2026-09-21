from copy import deepcopy
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
import sys, unittest
from unittest.mock import patch
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/shared'))
import massive_research_model as model
import massive_research_store as store

STAMP = '2026-09-21T16:00:00+00:00'
COMPILED = '2026-09-21T17:00:00+00:00'


class Error(Exception):
    def __init__(self, code): self.response = {'Error': {'Code': code}}


class Memory:
    def __init__(self): self.data = {}; self.reads = []; self.writes = []; self.corrupt = None
    def get_object(self, Bucket, Key):
        self.reads.append(Key)
        if Key not in self.data: raise Error('NoSuchKey')
        raw = self.data[Key]
        return {'Body': BytesIO(raw + b' ' if self.corrupt == Key else raw), 'ETag': model.sha(raw)}
    def put_object(self, Bucket, Key, Body, **kwargs):
        if kwargs.get('IfNoneMatch') == '*' and Key in self.data: raise Error('PreconditionFailed')
        if 'IfMatch' in kwargs and (Key not in self.data or model.sha(self.data[Key]) != kwargs['IfMatch']): raise Error('PreconditionFailed')
        self.writes.append((Key, kwargs)); self.data[Key] = Body


def make_parent(memory, kind, output):
    _, contract, prefix, run_contract, run_kind = model.SOURCES[kind]
    body = {'contract': contract, 'generated_at': STAMP, **model.PERMISSIONS, **output}
    raw = model.encoded(body); out = {'key': prefix+'outputs/'+model.sha(raw)+'.json', 'sha256': model.sha(raw), 'bytes': len(raw)}
    memory.data[out['key']] = raw
    run = {'contract': run_contract, 'generated_at': body['generated_at'], 'output': out, 'output_sha256': out['sha256']}
    if run_kind is not None: run['kind'] = run_kind
    raw = model.encoded(run); key = prefix+'runs/'+model.sha(raw)+'.json'; memory.data[key] = raw
    return {**body, 'replay': {'manifest_key': key, 'output_sha256': out['sha256']}}


def option_body(stamp=STAMP):
    return {'generated_at': stamp,
        'chains': {'SPY': {'coverage': {'returned_rows': 2, 'eligible_identity_rows': 2},
            'reported_open_interest': {'calls': {'value': '0', 'unit': 'contracts'}, 'puts': {'value': '2', 'unit': 'contracts'}, 'observed_at': None}}},
        'quality': {'returned_rows': 2, 'identity_eligible_rows': 2, 'acquisition_review_due_at': '2026-09-21T18:00:00+00:00'}}


def fixture():
    m = Memory(); read = store.reader(m, 'bucket')
    options_old = make_parent(m, 'options', option_body('2026-09-21T15:00:00+00:00'))
    options = make_parent(m, 'options', option_body())
    populations = make_parent(m, 'populations', {'source_run': options_old['replay'],
        'source_capture_completed_at': options_old['generated_at'],
        'underlyings': {'SPY': {'totals': {'counts': {'identity_eligible_rows': 2}}, 'expiry_strike_groups': 1}},
        'quality': {'counts': {'identity_eligible_rows': 2}, 'acquisition_review_due_at': COMPILED}})
    flow_row = {'source_valid_until': '2026-09-22T00:00:00+00:00', 'aligned_windows': {'5': {'flow_usd_decimal': '0'}}}
    holding_row = {'current': {'source_valid_until': '2026-09-22T00:00:00+00:00'}}
    flows = make_parent(m, 'fund_flows', {'funds': {'SPY': flow_row}, 'source_valid_until': '2026-09-22T00:00:00+00:00'})
    holdings = make_parent(m, 'holdings', {'funds': {'SPY': holding_row}, 'source_valid_until': '2026-09-22T00:00:00+00:00'})
    desk = make_parent(m, 'etf_desk', {'canonical_sources': {'flows': {'replay': flows['replay'], 'generated_at': flows['generated_at']},
        'holdings': {'replay': holdings['replay'], 'generated_at': holdings['generated_at']}},
        'funds': {'SPY': {'flows': flow_row, 'holdings': holding_row,
            'profiles': {'current': {'source_valid_until': '2026-09-22T00:00:00+00:00'}}}},
        'quality': {'additional_funds': []}})
    docs = dict(zip(model.SOURCES, (options, populations, desk, flows, holdings)))
    def capture(key, doc):
        raw = model.encoded(doc); m.data[key] = raw; original = store.protect(m, 'bucket', raw, read)
        return {'source_key': key, 'acquired_at': STAMP, 'status': 'retained', 'original': original}
    sources = {model.SOURCES[kind][0]: capture(model.SOURCES[kind][0], doc) for kind, doc in docs.items()}
    for key in model.CONTEXTS:
        sources[key] = capture(key, {'version': 'legacy', 'status': 'QUARANTINED', 'identity_ok': False,
            'n_products_with_data': 0, 'unrecognized_original_field': [1, 2, 3]})
    predecessors = {key: capture(key, {'version': '2.0.0', 'tickers': {'SPY': {'prepump_score': 99}}}) for key in model.PREDECESSORS}
    inputs = {'contract': 'massive-composite-inputs.v1', 'generated_at': COMPILED, 'sources': sources, 'predecessors': predecessors}
    return m, inputs, docs


class ModelTests(unittest.TestCase):
    def setUp(self): self.memory, self.inputs, self.docs = fixture(); self.read = store.reader(self.memory, 'bucket')
    def build(self): return model.build(self.inputs, self.read)
    def replace(self, kind, body):
        doc = make_parent(self.memory, kind, body); key = model.SOURCES[kind][0]
        self.inputs['sources'][key]['original'] = store.protect(self.memory, 'bucket', model.encoded(doc), self.read)
        return doc
    def test_same_family_different_vintages_no_independent_votes(self):
        out = self.build(); graph = out['dependency_graph']
        family = next(f for f in graph['measurement_families'] if f['family'] == 'option_chain')
        self.assertEqual(len(family['nodes']), 2); self.assertEqual(len(graph['nodes']), 6)
        self.assertEqual(len(graph['measurement_families']), 4); self.assertEqual(graph['independent_investment_votes'], 0)
        self.assertFalse(graph['statistical_independence_established']); self.assertEqual(out['portfolio_action'], 'WAIT')
    def test_repeat_and_serialized_inputs_reproduce(self):
        self.assertEqual(self.build(), model.build(model.strict(model.encoded(self.inputs)), self.read))
    def test_true_zero_and_unknown_clock_stay_in_exact_parent(self):
        out = self.build(); row = next(x for x in out['instruments']['SPY'] if x['source'] == 'options')
        node = out['dependency_graph']['nodes'][row['node']]
        body, _ = model.parent('options', node['replay'], self.read, model.clock(COMPILED))
        self.assertEqual(body['chains']['SPY']['reported_open_interest']['calls']['value'], '0')
        self.assertIsNone(body['chains']['SPY']['reported_open_interest']['observed_at'])
        self.assertEqual(row['pointer'], '/chains/SPY')
    def test_review_overdue_uses_source_deadline_at_boundary(self):
        out = self.build(); self.assertTrue(out['sources']['populations']['source_review_overdue'])
        self.assertFalse(out['sources']['options']['source_review_overdue'])
        self.inputs['generated_at'] = '2026-09-22T19:00:00+00:00'
        self.assertTrue(self.build()['sources']['options']['source_review_overdue'])
    def test_unknown_deadline_not_fresh(self):
        body = option_body(); body['quality']['acquisition_review_due_at'] = None; self.replace('options', body)
        self.assertIsNone(self.build()['sources']['options']['source_review_overdue'])
    def test_missing_source_is_not_zero_or_fallback(self):
        c = self.inputs['sources'][model.SOURCES['options'][0]]; c.update(status='missing', original=None)
        out = self.build(); self.assertEqual(out['sources']['options']['status'], 'missing')
        self.assertNotIn('coverage', out['sources']['options']); self.assertEqual(out['quality']['bound_native_sources'], 4)
        self.assertFalse(any(x['source'] == 'options' for x in out['instruments']['SPY']))
    def test_incomplete_inventory_rejected(self):
        del self.inputs['sources'][model.CONTEXTS[0]]
        with self.assertRaises(ValueError): self.build()
    def test_future_source_rejected(self):
        self.replace('options', option_body('2026-09-22T16:00:00+00:00'))
        with self.assertRaisesRegex(ValueError, 'later'): self.build()
    def test_future_capture_rejected(self):
        self.inputs['sources'][model.CONTEXTS[0]]['acquired_at'] = '2026-09-22T00:00:00Z'
        with self.assertRaisesRegex(ValueError, 'later'): self.build()
    def test_native_authority_self_claim_rejected(self):
        self.replace('options', {**option_body(), 'calls_eligible': True})
        with self.assertRaisesRegex(ValueError, 'authority'): self.build()
    def test_wrong_contract_retained_unqualified(self):
        self.replace('options', {**option_body(), 'contract': 'some-new-model'})
        self.assertEqual(self.build()['sources']['options']['status'], 'unqualified_source_contract')
    def test_duplicate_json_retained_but_unqualified(self):
        key = model.SOURCES['options'][0]
        self.inputs['sources'][key]['original'] = store.protect(self.memory, 'bucket', b'{"a":1,"a":2}', self.read)
        self.assertEqual(self.build()['sources']['options']['status'], 'invalid_source_document')
    def test_count_reconciliation(self):
        body = option_body(); body['quality']['returned_rows'] = 3; self.replace('options', body)
        with self.assertRaisesRegex(ValueError, 'totals'): self.build()
    def test_boolean_count_not_accepted(self):
        body = option_body(); body['chains']['SPY']['coverage']['returned_rows'] = True; self.replace('options', body)
        with self.assertRaisesRegex(ValueError, 'count'): self.build()
    def test_population_source_clock_not_relabelled(self):
        body = {k: v for k, v in self.docs['populations'].items() if k != 'replay'}
        body['source_capture_completed_at'] = STAMP; self.replace('populations', body)
        with self.assertRaisesRegex(ValueError, 'capture clock'): self.build()
    def test_population_cannot_precede_its_parent(self):
        body = {k: v for k, v in self.docs['populations'].items() if k != 'replay'}
        body['generated_at'] = '2026-09-21T14:00:00Z'; self.replace('populations', body)
        with self.assertRaisesRegex(ValueError, 'later than its view'): self.build()
    def test_eligible_count_cannot_exceed_returned_rows(self):
        body = option_body(); body['chains']['SPY']['coverage']['eligible_identity_rows'] = 3
        body['quality']['identity_eligible_rows'] = 3; self.replace('options', body)
        with self.assertRaisesRegex(ValueError, 'totals'): self.build()
    def test_run_hash_tampering_rejected(self):
        key = self.docs['options']['replay']['manifest_key']; self.memory.data[key] += b' '
        with self.assertRaises(ValueError): self.build()
    def test_output_tampering_rejected(self):
        replay = self.docs['options']['replay']; run = model.strict(self.memory.data[replay['manifest_key']])
        self.memory.data[run['output']['key']] += b' '
        with self.assertRaises(ValueError): self.build()
    def test_publication_cannot_differ_from_retained_output(self):
        doc = deepcopy(self.docs['options']); doc['chains']['SPY']['reported_open_interest']['calls']['value'] = '999'
        key = model.SOURCES['options'][0]; self.inputs['sources'][key]['original'] = store.protect(self.memory, 'bucket', model.encoded(doc), self.read)
        with self.assertRaisesRegex(ValueError, 'publication differs'): self.build()
    def test_legacy_quarantine_and_whole_original_preserved(self):
        out = self.build(); row = out['contexts']['data/polygon-futures-curves.json']
        self.assertFalse(row['reported_identity_ok']); self.assertEqual(row['reported_products_with_data'], 0)
        raw = model.original(row['capture']['original'], self.read)
        self.assertEqual(model.strict(raw)['unrecognized_original_field'], [1, 2, 3])
        self.assertFalse(row['calls_eligible'])
    def test_parent_read_cannot_reach_private_account(self):
        body = {k: v for k, v in self.docs['populations'].items() if k != 'replay'}
        body['source_run'] = {'manifest_key': 'data/trade-tickets.json', 'output_sha256': 'a'*64}; self.replace('populations', body)
        with self.assertRaises(ValueError): self.build()
        self.assertNotIn('data/trade-tickets.json', self.memory.reads)
    def test_no_false_original_replay_certification(self):
        out = self.build(); self.assertFalse(out['verification']['original_provider_replay_performed_by_composite'])
        self.assertTrue(out['verification']['composition_replay'])


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.memory, self.inputs, self.docs = fixture(); self.read = store.reader(self.memory, 'bucket')
        frozen = patch.object(store, 'now', return_value='2026-09-23T00:00:00Z')
        frozen.start(); self.addCleanup(frozen.stop)
    def retain(self):
        out = model.build(self.inputs, self.read)
        return out, store.retain(self.memory, 'bucket', self.inputs, out, self.read, lambda **kw: None)
    def test_full_composition_replay(self):
        out, identity = self.retain(); self.assertEqual(store.replay(identity, store.reader(self.memory, 'bucket')), out)
    def test_changed_compiler_rejected(self):
        _, identity = self.retain()
        with patch.object(store, 'COMPILERS', (model,)):
            with self.assertRaisesRegex(ValueError, 'compiler inventory'): store.replay(identity, self.read)
    def test_candidate_writes_no_heads_and_duplicate_request_cannot_execute(self):
        with patch.object(store, 'now', return_value=COMPILED):
            status = store.run(self.memory, 'bucket', 'candidate', 'runner', publish_current=False)
        self.assertFalse(status['published']); self.assertEqual(status['aliases'], {})
        self.assertFalse(any(key == model.CURRENT for key, _ in self.memory.writes))
        before = len(self.memory.writes)
        self.assertEqual(store.run(self.memory, 'bucket', 'candidate', 'different-runner'), status)
        self.assertEqual(len(self.memory.writes), before)
    def test_native_and_alias_publication_empty_ranks(self):
        with patch.object(store, 'now', return_value=COMPILED): status = store.run(self.memory, 'bucket', 'native', 'runner')
        self.assertTrue(status['published']); self.assertTrue(all(status['aliases'].values()))
        doc = model.strict(self.memory.data['data/massive-signals.json'])
        self.assertEqual(doc['top_prepump'], []); self.assertEqual(doc['market'], {}); self.assertFalse(doc['calls_eligible'])
        for key, args in self.memory.writes:
            if key in (model.CURRENT, *model.PREDECESSORS): self.assertEqual(args['CacheControl'], 'no-store')
    def test_recovery_keeps_original_clock_and_does_not_capture_again(self):
        out, identity = self.retain(); self.memory.reads.clear()
        with patch.object(store, 'now', return_value='2026-09-22T00:00:00Z'):
            status = store.run(self.memory, 'bucket', 'recovery', 'runner', recover_run=identity, publish_current=False)
        self.assertEqual(status['generated_at'], COMPILED)
        self.assertFalse(set(self.memory.reads) & set(model.CAPTURE_KEYS))
    def test_old_recovery_cannot_roll_back_newer_head(self):
        out, identity = self.retain(); packet = {**out, 'replay': identity}
        later = {**packet, 'generated_at': '2026-09-22T00:00:00Z'}; self.memory.data[model.CURRENT] = model.encoded(later)
        with patch.object(store, 'now', return_value='2026-09-23T00:00:00Z'):
            self.assertFalse(store.conditional(self.memory, 'bucket', model.CURRENT, packet, self.read))
    def test_same_clock_conflict_rejected(self):
        out, identity = self.retain(); packet = {**out, 'replay': identity}
        self.memory.data[model.CURRENT] = model.encoded({**packet, 'new_field': 1})
        with self.assertRaisesRegex(ValueError, 'same-clock'): store.conditional(self.memory, 'bucket', model.CURRENT, packet, self.read)
    def test_new_composition_cannot_refresh_an_older_source(self):
        out, identity = self.retain(); packet = {**out, 'replay': identity}
        self.memory.data[model.CURRENT] = model.encoded(packet)
        older = deepcopy(packet); older['generated_at'] = '2026-09-22T00:00:00Z'
        older['sources']['options']['source_generated_at'] = '2026-09-20T00:00:00Z'
        with patch.object(store, 'now', return_value='2026-09-23T00:00:00Z'):
            self.assertFalse(store.conditional(self.memory, 'bucket', model.CURRENT, older, self.read))
        self.assertEqual(model.strict(self.memory.data[model.CURRENT]), packet)
    def test_concurrent_legacy_change_cannot_be_retired(self):
        out, identity = self.retain(); packet = store.compatibility({**out, 'replay': identity}, model.PREDECESSORS[0])
        self.memory.data[model.PREDECESSORS[0]] = b'{"different":true}'
        with self.assertRaisesRegex(ValueError, 'predecessor changed'):
            store.conditional(self.memory, 'bucket', model.PREDECESSORS[0], packet, self.read)
    def test_failed_build_keeps_last_good_head_and_records_failure(self):
        before = b'{"retained":true}'; self.memory.data[model.CURRENT] = before
        with patch.object(model, 'build', side_effect=ValueError('failure')):
            with self.assertRaises(RuntimeError): store.run(self.memory, 'bucket', 'failed', 'runner')
        self.assertEqual(self.memory.data[model.CURRENT], before)
        status = model.strict(self.memory.data[store.request_key('failed')]); self.assertEqual(status['status'], 'failed')
        self.assertIn('retained_input', status)
    def test_unreviewed_read_and_capture_fail_before_s3(self):
        n = len(self.memory.reads)
        for action in (lambda: self.read('data/trade-tickets.json'), lambda: store.capture(self.memory, 'bucket', 'data/trade-tickets.json', self.read)):
            with self.assertRaises(ValueError): action()
        self.assertEqual(len(self.memory.reads), n)
    def test_original_readback_corruption_is_rejected(self):
        raw = b'whole retained object'; self.memory.corrupt = model.ref(raw, 'originals')['key']
        with self.assertRaisesRegex(ValueError, 'readback'): store.protect(self.memory, 'bucket', raw, self.read)


if __name__ == '__main__': unittest.main()
