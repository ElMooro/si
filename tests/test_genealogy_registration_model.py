from copy import deepcopy
import unittest
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'aws/ops/checks'))
import genealogy_registration_model as model


def record(i,source='a',symbol='AAA',direction='UP',at='2026-09-18T19:00:00Z',**extra):
    fid=f'{i:064x}'
    return {'forecast_id':fid,'key':'data/research-forecasts/records/'+fid+'.json','sha256':'a'*64,
            'registered_at':at,'source_generated_at':at,'source_bytes_sha256':'b'*64,
            'instrument':{'instrument_id':'equity:US:'+symbol,'asset_class':'equity','currency':'USD'},
            'source_key':'data/'+source+'.json','direction':direction,'source_issue':None,'identity_issue':None,**extra}


def audit(rows,**extra):
    from collections import Counter
    return {'contract':'genealogy-public-archive-audit.v1','record_body_and_storage_checks_complete':True,
            'records':rows,'validated_records':len(rows),'registration_counts_by_source':dict(Counter(r['source_key'] for r in rows)),
            'cutoff':'2026-09-28T18:00:00Z','unreferenced_record_ids':[], 'failures':[], 'reference_errors':[], 'reference_conflicts':[],
            'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False,**extra}


class Chronology(unittest.TestCase):
    def test_repeated_registrations_do_not_multiply_a_source_pair(self):
        rows=[record(1),record(2,at='2026-09-19T19:00:00Z'),record(3,source='b',at='2026-09-18T19:00:01Z')]
        out=model.compile_archive(audit(rows))
        self.assertEqual(out['retained_records'],3);self.assertEqual(len(out['first_registrations']),2)
        self.assertEqual(out['compared'],1);self.assertEqual(out['same_instrument_comparisons'][0]['b_minus_a_elapsed_microseconds'],1000000)
        self.assertEqual(out['first_registrations'][0]['registration_count'],2)
        self.assertIsNone(out['pair_summaries'][0]['independent_evidence_count'])

    def test_instrument_and_direction_must_both_match(self):
        rows=[record(1),record(2,source='b',symbol='BBB'),record(3,source='c',direction='DOWN')]
        self.assertEqual(model.compile_archive(audit(rows))['compared'],0)

    def test_all_pairs_and_all_tied_first_records_are_preserved(self):
        rows=[record(i+1,source=f'source-{i:03d}') for i in range(100)]+[record(101,source='source-000')]
        out=model.compile_archive(audit(rows))
        self.assertEqual(out['possible_comparisons'],4950);self.assertEqual(out['compared'],4950)
        self.assertEqual(out['uncompared'],0)
        self.assertTrue(all(r['registration_order']=='same_timestamp' for r in out['same_instrument_comparisons']))
        self.assertEqual(len(out['first_registrations'][0]['first_registration_ids']),2)

    def test_timezones_and_subsecond_intervals_are_not_array_positions(self):
        rows=[record(1,at='2026-09-18T15:00:00.000001-04:00'),record(2,source='b',at='2026-09-18T19:00:00.000003Z')]
        row=model.compile_archive(audit(rows))['same_instrument_comparisons'][0]
        self.assertEqual(row['b_minus_a_elapsed_microseconds'],2);self.assertEqual(row['b_minus_a_utc_calendar_days'],0)

    def test_excluded_echo_identity_and_orphan_rows_remain_visible_without_voting(self):
        rows=[record(1),record(2,source='b',source_issue='summary_echo'),record(3,source='c',identity_issue='crypto_collision'),record(4,source='d')]
        out=model.compile_archive(audit(rows,unreferenced_record_ids=[rows[3]['forecast_id']]))
        self.assertEqual((out['archive_records'],out['retained_records'],out['excluded_records']),(4,1,3))
        self.assertEqual(out['compared'],0);self.assertEqual(out['exclusions'][2]['reasons'],['no_completed_capture_reference'])
        self.assertFalse(out['calls_eligible']);self.assertFalse(out['sizing_eligible'])

    def test_bad_population_clocks_or_authority_cannot_become_a_chronology(self):
        original=audit([record(1)])
        mutations=[lambda a:a.update(validated_records=True),lambda a:a.update(record_body_and_storage_checks_complete=False),
                   lambda a:a.update(calls_eligible=True),lambda a:a.update(registration_counts_by_source={}),
                   lambda a:a.update(registration_counts_by_source={'data/a.json':True}),lambda a:a.pop('failures'),
                   lambda a:a['records'][0].update(registered_at='2026-09-29T00:00:00Z'),
                   lambda a:a['records'][0].update(registered_at='2026-09-18'),lambda a:a['records'][0].pop('source_issue')]
        for edit in mutations:
            changed=deepcopy(original);edit(changed)
            with self.assertRaises(ValueError):model.compile_archive(changed)
        with self.assertRaises(ValueError):model.compile_archive(audit([record(1),record(1)]))


if __name__=='__main__':unittest.main()
