"""Actual capture, full compiler/oracle/replay and durable multi-job boundaries."""
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
import contextlib, io, json, sys, unittest, urllib.parse
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/p) for p in ('aws/shared', 'aws/ops/checks', 'tests')]
import capital_structure_refresh as refresh
import capital_structure_source as source
import capital_structure_producer as producer
import share_structure_campaign as campaign
import share_structure_sources as capture
from test_capital_structure_research import fixture
from test_capital_structure_producer import S3, Conflict

STAMP = '2026-09-25T12:00:00Z'


class Response(io.BytesIO):
    status = 200
    def __init__(self, raw):
        super().__init__(raw); self.headers = {'content-length': str(len(raw))}


class Tests(unittest.TestCase):
    def setup_cycle(self):
        f = fixture(); s3 = S3(f['files']); calls = []; checks = []
        manifest = json.loads(f['files'][f['manifest']['key']]); old = json.loads(f['files'][manifest['plan']['key']])
        document = campaign.plan(['ABC'], old['baseline'], old['baseline'], old['baseline'], lambda: STAMP)
        def transport(request, timeout):
            url = request.full_url.split('&apikey=')[0]; calls.append(url)
            self.assertEqual(timeout, 25)
            self.assertIn(url, f['originals'])
            return Response(f['files'][f['originals'][url]['key']])
        def audit(client, keys, phase):
            checks.append((phase, set(keys)))
            for key in keys: refresh.access.urls(key)
            return {'summary': {'all_denied': True}, 'test_only': True}
        def invoke(phase, run='123', **extra):
            return refresh.run(s3, run, phase, 'synthetic-managed-credential', clock=lambda: STAMP,
                audit=audit, identity_fetch=lambda: f['identity'], source_transport=transport, **extra)
        stack = contextlib.ExitStack()
        stack.enter_context(patch.object(refresh, 'NAMES', 1)); stack.enter_context(patch.object(refresh, 'REQUESTS', 7))
        stack.enter_context(patch.object(refresh, 'PARTS', 1)); stack.enter_context(patch.object(refresh, 'baseline_plan', return_value=document))
        stack.enter_context(patch.object(capture.Rate, 'acquire', return_value=None))
        self.addCleanup(stack.close)
        return f, s3, calls, checks, invoke

    def test_complete_multiphase_capture_reconstructs_every_original_before_readiness(self):
        f, s3, calls, checks, invoke = self.setup_cycle()
        invoke('plan'); self.assertNotIn(producer.READY, s3.files)
        invoke('part-1'); self.assertEqual(len(calls), 7); self.assertNotIn(producer.READY, s3.files)
        out = invoke('qualify')
        self.assertTrue(out['qualification']['ready_advanced']); self.assertEqual(out['counts']['complete_sources'], 7)
        ready = json.loads(s3.files[producer.READY]); self.assertEqual(ready['qualification']['original_rows_checked'], 7)
        self.assertIs(ready['qualification']['production_measurement_formulas_imported'], False)
        self.assertNotIn(producer.CURRENT, s3.files)
        control = json.loads(s3.files[refresh.CONTROL]); self.assertEqual(control['status'], 'complete')
        self.assertEqual(control['completed_phases'], ['plan', 'part-1', 'qualify'])
        self.assertEqual([p for p, keys in checks], ['plan', 'part-1', 'qualify'])
        self.assertTrue(all(v['Key'].startswith((source.PRIVATE, source.PREFIX)) for v in s3.writes))
        self.assertEqual(out['producer_invocations'], 0)

    def test_duplicate_skipped_and_cross_run_phases_cannot_acquire_sources(self):
        f, s3, calls, checks, invoke = self.setup_cycle(); invoke('plan')
        for phase, run in [('plan','123'), ('qualify','123'), ('part-1','999'), ('plan','999')]:
            with self.assertRaises(ValueError): invoke(phase, run)
        self.assertEqual(calls, [])
        invoke('part-1')
        with self.assertRaises(ValueError): invoke('part-1')
        self.assertEqual(len(calls), 7)

    def test_failure_stops_future_days_and_keeps_complete_successes_for_review(self):
        f, s3, calls, checks, invoke = self.setup_cycle(); invoke('plan')
        original = capture.capture
        def fail(client, request_id, spec, *args, **kwargs):
            if spec['endpoint'] == 'quote': raise ValueError('Synthetic source failure')
            return original(client, request_id, spec, *args, **kwargs)
        with patch.object(capture, 'capture', side_effect=fail):
            with self.assertRaises(ValueError): invoke('part-1')
        count = len(calls)
        for phase, run in [('part-1','123'), ('qualify','123'), ('plan','456')]:
            with self.assertRaises(ValueError): invoke(phase, run)
        self.assertEqual(len(calls), count); self.assertNotIn(producer.READY, s3.files)
        self.assertEqual(json.loads(s3.files[refresh.CONTROL])['status'], 'failed')
        journal = json.loads(s3.files[capture.request_key(refresh.request_id('123'), 'batch:1')])
        self.assertTrue(journal['captures']); self.assertEqual(journal['status'], 'failed')

    def test_runner_loss_after_claim_cannot_be_retried_or_skipped(self):
        f, s3, calls, checks, invoke = self.setup_cycle(); invoke('plan')
        state, etag, raw = refresh.load_control(s3); state['active_phase'] = 'part-1'
        refresh.save_control(s3, state, etag)
        with self.assertRaises(ValueError): invoke('part-1')
        with self.assertRaises(ValueError): invoke('plan', '456')
        self.assertEqual(calls, [])

    def test_private_access_failure_prevents_qualification(self):
        f, s3, calls, checks, invoke = self.setup_cycle(); invoke('plan'); invoke('part-1')
        def forbidden(*a, **k): self.fail('Readiness must not run after privacy failure')
        with self.assertRaises(ValueError):
            refresh.run(s3, '123', 'qualify', clock=lambda: STAMP,
                audit=lambda *a: {'summary': {'all_denied': False}}, qualification=forbidden)
        self.assertNotIn(producer.READY, s3.files)

    def test_cas_conflict_cannot_start_another_provider_phase(self):
        f, s3, calls, checks, invoke = self.setup_cycle(); invoke('plan'); s3.race = True
        with self.assertRaises(Conflict): invoke('part-1')
        self.assertEqual(calls, [])

    def test_new_completed_cycle_preserves_entire_prior_control(self):
        f, s3, calls, checks, invoke = self.setup_cycle(); invoke('plan'); invoke('part-1'); invoke('qualify')
        old = s3.files[refresh.CONTROL]
        with self.assertRaises(ValueError): invoke('plan', '456')
        refresh.run(s3, '456', 'plan', clock=lambda:'2026-09-26T12:00:00Z',
            audit=lambda *a: {'summary':{'all_denied':True}}, identity_fetch=lambda:f['identity'])
        state = json.loads(s3.files[refresh.CONTROL]); self.assertEqual(s3.files[state['previous_cycle']['key']], old)
        self.assertEqual(len(calls), 7)

    def test_boundaries_reject_invalid_identity_and_overdue_cycles(self):
        for value in ('', '1/../x', 123, 'a'*40):
            with self.assertRaises(ValueError): refresh.request_id(value)
        f, s3, calls, checks, invoke = self.setup_cycle(); invoke('plan')
        with self.assertRaises(ValueError): refresh.run(s3, '123', 'part-1', clock=lambda: '2026-09-26T12:00:00Z')
        self.assertEqual(calls, [])

    def test_real_population_gate_rejects_a_partial_accepted_candidate_before_acquisition(self):
        s3 = S3({refresh.ACCEPTED_STATUS: source.encoded({'request_id':refresh.ACCEPTED_REQUEST,'status':'complete',
            'counts':{'reported_names':500,'provider_responses':3000},'qualification':{'all_original_rows_conserved':True}})})
        with self.assertRaises(ValueError): refresh.baseline_plan(s3, lambda: STAMP)
        self.assertEqual(s3.writes, [])

    def test_exact_full_population_plan_has_six_batches_and_all_11305_requests(self):
        labels = sorted('TEST'+str(i) for i in range(1615)); s3 = S3({})
        retain = lambda value: capture.retain(s3, source.encoded(value))
        inventory = retain({'candidate_provider_labels':labels,'candidate_provider_label_count':1615,'all_current_rows_conserved':True})
        baseline = retain({'status':'complete','inventory':inventory})
        plan = retain({'reported_symbols':labels,'baseline':baseline,'probe':baseline,'accounting':baseline})
        manifest = retain({'plan':plan,'reported_symbols':labels})
        s3.files[refresh.ACCEPTED_STATUS] = source.encoded({'request_id':refresh.ACCEPTED_REQUEST,'status':'complete',
            'source_manifest':manifest,'counts':{'reported_names':1615,'provider_responses':11305},
            'qualification':{'all_original_rows_conserved':True}})
        result = refresh.baseline_plan(s3, lambda: STAMP)
        self.assertEqual(result['reported_symbols'], labels); self.assertEqual(result['batches'], 6)
        population = [spec for part in range(1,7) for spec in campaign.specifications(result, part)]
        self.assertEqual(len(population), 11305); self.assertEqual(len({r['url'] for r in population}),11305)

    def test_changed_retained_response_cannot_advance_ready(self):
        f, s3, calls, checks, invoke = self.setup_cycle(); invoke('plan'); invoke('part-1')
        s3.files[next(iter(f['originals'].values()))['key']] = b'[]'
        with self.assertRaises(ValueError): invoke('qualify')
        self.assertNotIn(producer.READY, s3.files); self.assertEqual(len(calls), 7)

    def test_transport_applies_stricter_start_rate_without_retries(self):
        from unittest.mock import Mock
        limiter = Mock(); opener = Mock(); request = object()
        with patch.object(capture, 'Rate', return_value=limiter) as factory, patch.object(refresh.urllib.request, 'build_opener', return_value=opener):
            transport = refresh.transport(); transport(request, 25)
        factory.assert_called_once_with(interval=2.0); limiter.acquire.assert_called_once_with()
        opener.open.assert_called_once_with(request, timeout=25)

    def test_workflow_serializes_every_batch_and_preserves_native_schedule(self):
        text = (ROOT/'.github/workflows/capital-structure-source-refresh.yml').read_text()
        accounting = (ROOT/'.github/workflows/statement-source-refresh.yml').read_text()
        self.assertIn('group: statement-source-refresh', text); self.assertIn('group: statement-source-refresh', accounting)
        self.assertIn('cancel-in-progress: false', text); self.assertIn("cron: '15 20 * * *'", text)
        phases = ['plan']+['part-'+str(i) for i in range(1,7)]+['qualify']
        for i, phase in enumerate(phases):
            self.assertEqual(text.count('run_capital_structure_refresh.py '+phase+'\n'), 1)
            if i: self.assertIn('needs: '+phases[i-1].replace('-','_')+'\n', text)
        self.assertNotIn('invoke', text); self.assertNotIn('continue-on-error', text)
        self.assertEqual(refresh.INTERVAL, 2.0)


if __name__ == '__main__': unittest.main(verbosity=2)
