from pathlib import Path
from copy import deepcopy
import json,sys,unittest
from datetime import date,timedelta
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6282_estimate_calendar_acceptance as op
import test_estimate_observations_acceptance as existing


class Tests(unittest.TestCase):
    def test_actual_writer_full_calendar_and_estimate_replay_is_read_only(self):
        raw,prior,m=existing.Tests().publication();before=list(m.writes);out=op.publication(raw,prior)
        self.assertTrue(out['calendar_originals_verified']);self.assertTrue(out['current_compiler_publication_verified'])
        self.assertEqual(out['calendar_received_occurrences'],1);self.assertFalse(out['provider_universe_complete']);self.assertFalse(out['market_session_qualified'])
        self.assertFalse(out['investment_authority']);self.assertEqual(before,m.writes)

    def test_clock_source_projection_filter_and_transport_forgery_are_rejected(self):
        raw,prior,_=existing.Tests().publication();p=json.loads(raw)
        edits=[lambda p:p['calendar_rows'][0].update(session='BMO'),lambda p:p['calendar_rows'][0].update(time='10:00:00'),
            lambda p:p['calendar_rows'][0].update(source_index=99),lambda p:p['calendar_evidence'].update(excluded=[{'source_index':3}]),
            lambda p:p['calendar_evidence'].update(pagination_status='complete'),lambda p:p['calendar_acquisition'].update(original_sha256='bad'),
            lambda p:p['calendar_acquisition'].update(received_at='2099-01-01T00:00:00Z'),lambda p:p.update(min_importance=True),
            lambda p:p.update(calendar_original_http_retained=False),lambda p:p['calendar_transport'].update(follow_redirects=True),
            lambda p:p['calendar_transport'].update(automatic_retries=True),lambda p:p['calendar_acquisition'].update(end_date='2030-01-01')]
        for edit in edits:
            q=deepcopy(p);edit(q)
            with self.assertRaises(ValueError):op.publication(json.dumps(q).encode(),prior)

    def test_priority_is_replayed_even_when_a_permuted_packet_is_internally_consistent(self):
        n=existing.native;memory=n.Memory();prior=memory.data['data/estimate-revisions.json'];today=n.NOW
        values=[{'ticker':'TEST','date':(date.fromisoformat(today)+timedelta(days=1)).isoformat(),'importance':5},
            {'ticker':'TEST','date':today,'importance':2},{'ticker':'TEST','date':today,'importance':4}]
        scope=n.native(memory,fetch_calendar=lambda:values);scope['_estimate_fetch']=lambda s:n.acquisition(stamp=n.datetime.now(n.timezone.utc).isoformat());scope['lambda_handler']()
        raw=memory.data['data/estimate-revisions.json'];p=json.loads(raw);self.assertEqual([r['calendar_index'] for r in p['request_records']],[2,1,0]);op.publication(raw,prior)
        p['request_records'].reverse()
        for i,row in enumerate(p['request_records']):row['request_index']=i
        # Original observation replay allows a valid permutation; the calendar check must reject it.
        op.transport.publication(json.dumps(p).encode(),prior)
        with self.assertRaisesRegex(ValueError,'selected calendar order'):op.publication(json.dumps(p).encode(),prior)

    def test_old_publication_stays_pending_and_no_external_mutation_scope_exists(self):
        for version in ('3.1.0','3.2.0','3.2.1'):
            out=op.publication(json.dumps({'measurement_contract':op.original.compiler().CONTRACT,'version':version}).encode())
            self.assertFalse(out['current_compiler_publication_verified']);self.assertFalse(out['calendar_originals_verified'])
        source=Path(op.__file__).read_text(encoding='utf-8')
        for forbidden in ('invoke(', 'put_object(', 'update_function', 'get_secret_value', 'list_objects'):self.assertNotIn(forbidden,source)
        self.assertEqual(op.KEY,'data/estimate-revisions.json')


if __name__=='__main__':unittest.main(verbosity=2)
