"""Conservation, parsing and permission boundaries for every registered view."""
from copy import deepcopy
from pathlib import Path
import hashlib
import json
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/ops/checks'))
import signal_board_candidate as candidate
import verify_signal_board_inventory as independent

NOW = '2026-09-26T09:00:00+00:00'
ROWS = json.loads((ROOT/'assets/signal-board-registry.json').read_bytes())['feeds']


def fixture():
    bodies, receipts = {}, {}
    def put(key, body, status=200):
        digest = hashlib.sha256(body).hexdigest()
        ref = {'key': 'test/originals/'+digest+'.bin', 'sha256': digest, 'bytes': len(body)}
        bodies[ref['key']] = body
        receipts[key] = {'source_key': key, 'status': 'public_sidecar_retained' if status == 200 else 'whole_http_error_retained',
                         'http_status': status, 'original': ref, 'requested_at': '2026-09-26T08:00:00Z', 'received_at': '2026-09-26T08:00:01Z'}
    for key in {r['source_key'] for r in ROWS}:
        if key in candidate.PRIVATE:
            receipts[key] = {'source_key': key, 'status': 'excluded_private_account_input', 'requested': False}
        else:
            put(key, candidate.encoded({'source': key, 'generated_at': '2026-09-26T06:00:00Z', 'calls_eligible': True,
                                       'signal': 2, 'replay': {'verified': True}, 'history': [0, None, -5]}))
    return bodies, receipts, put


class Tests(unittest.TestCase):
    def setUp(self):
        self.bodies, self.captures, self.put = fixture()
        self.reads = []
        def read(ref):
            self.reads.append(ref['key'])
            return self.bodies[ref['key']]
        self.read = read

    def build(self):
        return candidate.build(ROWS, self.captures, self.read, NOW)

    def verify(self, output):
        return independent.verify(output, ROWS, self.captures, self.read)

    def test_full_population_shared_sources_and_no_votes(self):
        out = self.build()
        self.assertEqual(len(self.reads), 96)
        self.assertEqual(out['n_engines'], 99)
        self.assertIsNone(out['n_stale'])
        shared = [r for r in out['engines'] if r['source_key'] == 'data/liquidity-inflection.json']
        self.assertEqual(len(shared), 2)
        self.assertTrue(all(r['source_views'] == 2 and r['signal'] is None and r['stale'] is None for r in shared))
        proof = self.verify(out)
        self.assertEqual(proof['whole_responses_checked'], 96)
        self.assertFalse(proof['original_provider_verified'])
        self.assertFalse(proof['predictive_validation_performed'])
        self.assertIsNone(proof['independent_original_roots'])

    def test_private_rows_conserved_without_reads_or_refs(self):
        out = self.build()
        for key in candidate.PRIVATE:
            self.assertEqual(out['sources'][key]['status'], 'private_input_excluded')
            self.assertIsNone(out['sources'][key]['original'])
        self.captures['data/sizing.json']['requested'] = True
        with self.assertRaises(ValueError): self.build()

    def test_no_declared_permission_or_replay_can_authorize_a_vote(self):
        key = 'data/canary-warroom.json'
        self.put(key, b'{"calls_eligible":true,"sizing_eligible":1,"execution_eligible":"false","forecast_qualified":false,"replay":{},"composite_signal":2}')
        out = self.build(); item = out['sources'][key]
        self.assertEqual(item['permission_declarations'], {'calls_eligible': 'declared_true', 'sizing_eligible': 'invalid', 'execution_eligible': 'invalid', 'forecast_qualified': 'declared_false'})
        self.assertTrue(item['replay_declared'])
        self.assertFalse(item['calls_eligible'])
        self.assertIsNone(out['composite_signal'])
        self.verify(out)

    def test_whole_large_and_precise_source_is_never_truncated(self):
        key = 'data/canary-warroom.json'
        raw = b'{"large_integer":9007199254740993,"precise":0.12345678901234567890123456789,"history":[' + b','.join([b'0', b'null', b'-5']*30000) + b']}'
        self.put(key, raw)
        out = self.build(); ref = out['sources'][key]['original']
        self.assertEqual(self.read(ref), raw)
        self.assertGreater(ref['bytes'], 200000)
        self.verify(out)

    def test_error_and_empty_bodies_remain_whole_without_parsing(self):
        self.put('data/ici-flows.json', b'<html>complete denied</html>', 403)
        self.put('screener/mean-reversion.json', b'', 404)
        out = self.build()
        self.assertEqual(out['source_status_counts']['http_error'], 2)
        self.assertEqual(out['sources']['screener/mean-reversion.json']['original']['bytes'], 0)
        self.verify(out)

    def test_missing_transport_is_not_zero_or_reused_data(self):
        key = 'data/canary-warroom.json'
        self.captures[key] = {'source_key': key, 'status': 'transport_or_size_unavailable', 'error_type': 'TimeoutError'}
        out = self.build()
        self.assertEqual(out['sources'][key]['status'], 'transport_unavailable')
        self.assertIsNone(out['sources'][key]['original'])
        self.verify(out)

    def test_ambiguous_json_and_nonobjects_have_distinct_failures(self):
        key = 'data/canary-warroom.json'
        for raw in (b'{"x":1,"x":2}', b'{"x":NaN}', b'{"x":Infinity}', b'\xff', b'broken'):
            self.put(key, raw); out = self.build()
            self.assertEqual(out['sources'][key]['status'], 'invalid_json'); self.verify(out)
        for raw in (b'[]', b'null', b'0', b'false'):
            self.put(key, raw); out = self.build()
            self.assertEqual(out['sources'][key]['status'], 'non_object_json'); self.verify(out)

    def test_publication_clock_is_never_an_observation_clock(self):
        key = 'data/canary-warroom.json'
        cases = [('2026-09-27T06:00:00Z', 'future'), ('2026-09-25T06:00:00-04:00', 'reported_past'),
                 ('2026-09-25T06:00:00', 'invalid'), ('2026-02-30T06:00:00Z', 'invalid'), (20260925, 'invalid'), (None, 'not_reported')]
        for value, status in cases:
            self.put(key, candidate.encoded({'generated_at': value, 'quality': {'status': 'fresh'}})); out = self.build()
            self.assertEqual(out['sources'][key]['publication']['status'], status)
            self.assertEqual(out['sources'][key]['observation_freshness'], 'unverified'); self.verify(out)

    def test_changed_source_or_receipt_fails_before_acceptance(self):
        out = self.build(); key = 'data/canary-warroom.json'; ref = self.captures[key]['original']
        self.bodies[ref['key']] += b' '
        with self.assertRaises(ValueError): self.verify(out)
        with self.assertRaises(ValueError): self.build()
        self.bodies[ref['key']] = self.bodies[ref['key']][:-1]
        self.captures[key]['received_at'] = '2026-09-26T08:00:02Z'
        with self.assertRaises(ValueError): self.verify(out)

    def test_registry_loss_extra_capture_and_identity_drift_fail(self):
        for rows in (ROWS[:-1], ROWS + [ROWS[0]], [{**r, 'engine': 'same'} for r in ROWS]):
            with self.assertRaises(ValueError): candidate.build(rows, self.captures, self.read, NOW)
        self.captures['unexpected'] = {}
        with self.assertRaises(ValueError): self.build()
        del self.captures['unexpected']; self.captures['data/canary-warroom.json']['source_key'] = 'other'
        with self.assertRaises(ValueError): self.build()

    def test_independent_verifier_rejects_tampered_authority_counts_and_rows(self):
        original = self.build()
        mutations = [lambda o: o.update(calls_eligible=True), lambda o: o.update(composite_signal=0),
                     lambda o: o['engines'][0].update(signal=0), lambda o: o['engines'][0].update(stale=False),
                     lambda o: o['engines'].pop(), lambda o: o['dependency_graph'].update(independent_original_roots=98),
                     lambda o: o['source_status_counts'].update(derived_packet_retained=95),
                     lambda o: o['sources']['data/canary-warroom.json'].update(observation_freshness='fresh'),
                     lambda o: o['sources']['data/canary-warroom.json']['publication'].update(status='future')]
        for mutation in mutations:
            altered = deepcopy(original); mutation(altered)
            with self.assertRaises(ValueError): self.verify(altered)

    def test_replay_deterministic_and_inputs_unchanged(self):
        before = deepcopy(self.captures)
        self.assertEqual(candidate.encoded(self.build()), candidate.encoded(self.build()))
        self.assertEqual(self.captures, before)
        for value in ('2026-09-26', 'bad', None):
            with self.assertRaises(ValueError): candidate.build(ROWS, self.captures, self.read, value)


if __name__ == '__main__': unittest.main(verbosity=2)
