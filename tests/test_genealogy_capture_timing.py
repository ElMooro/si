from copy import deepcopy
from datetime import datetime,timezone,timedelta
from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops/checks')]
import genealogy_capture_timing as model

BASE=datetime(2026,9,18,12,tzinfo=timezone.utc)


def snapshot(source,minute,members=('AAA',),eligible=True,unsupported=0):
    return {'source_key':'data/'+source+'.json','source_received_at':(BASE+timedelta(minutes=minute)).isoformat(),
        'source_generated_at':BASE.isoformat(),'source_bytes_sha256':'b'*64,'capture_source_index':0,
        'eligible':eligible,'exclusion_reasons':[] if eligible else ['missing_source_clock'],
        'unsupported_identity_count':unsupported,
        'explicit_members':[{'instrument_id':'equity:US:'+m,'direction':'UP'} for m in members]}


def capture(i,*sources):
    return {'capture_key':f'data/research-forecasts/captures/{i:064x}.json','capture_sha256':f'{i:064x}',
        'started_at':BASE.isoformat(),'generated_at':(BASE+timedelta(days=1)).isoformat(),
        'candidate_scan_complete':True,'sources':list(sources)}


def compile(*contexts):return model.compile_contexts(list(contexts),'2026-09-20T00:00:00Z')


class Timing(unittest.TestCase):
    def test_partially_resolved_identity_cannot_supply_an_absence_or_fake_reentry(self):
        out=compile(capture(1,snapshot('a',0,(),unsupported=1)),capture(2,snapshot('a',5)))
        self.assertIsNone(out['intervals'][0]['lower_exclusive_utc'])
        out=compile(capture(1,snapshot('a',0)),capture(2,snapshot('a',5,(),unsupported=1)),capture(3,snapshot('a',10)))
        self.assertEqual(len(out['intervals']),1)
        self.assertEqual(out['identity_partial_source_snapshots'],1)
        out=compile(capture(1,snapshot('a',0,())),capture(2,snapshot('a',5,('BBB',),unsupported=1)),capture(3,snapshot('a',10,('AAA','BBB'))))
        first=next(row for row in out['intervals'] if row['instrument_id']=='equity:US:AAA')
        self.assertEqual(first['lower_exclusive_utc'],BASE.isoformat())

    def test_polling_order_does_not_resolve_initial_signal_order(self):
        out=compile(capture(1,snapshot('a',1),snapshot('b',2)))
        pair=out['comparisons'][0]
        self.assertEqual(pair['interval_order'],'unresolved_prior_observation_missing')
        self.assertEqual(len(pair['shared_presence_capture_keys']),1)
        self.assertTrue(all(r['lower_exclusive_utc'] is None for r in out['intervals']))
        self.assertFalse(out['calls_eligible'])

    def test_only_nonoverlapping_bounded_membership_intervals_get_an_order(self):
        out=compile(capture(1,snapshot('a',0,())),capture(2,snapshot('a',10)),
                    capture(3,snapshot('b',10,())),capture(4,snapshot('b',20)))
        self.assertEqual(out['comparisons'][0]['interval_order'],'a_observed_presence_before_b_bounded_entry')
        self.assertFalse(out['comparisons'][0]['causal_order_qualified'])
        overlap=compile(capture(1,snapshot('a',0,()),snapshot('b',1,())),capture(2,snapshot('a',10),snapshot('b',11)))
        self.assertEqual(overlap['comparisons'][0]['interval_order'],'overlapping_observation_intervals')
        one_sided=compile(capture(1,snapshot('a',0)),capture(2,snapshot('b',10,())),capture(3,snapshot('b',20)))
        self.assertIsNone(one_sided['comparisons'][0]['a_lower_exclusive_utc'])
        self.assertEqual(one_sided['comparisons'][0]['interval_order'],'a_observed_presence_before_b_bounded_entry')

    def test_missing_and_ineligible_states_are_not_observed_absence(self):
        out=compile(capture(1,snapshot('a',0)),capture(2,snapshot('a',5,(),False)),capture(3,snapshot('a',10)))
        self.assertEqual(len(out['intervals']),1);self.assertEqual(out['ineligible_source_snapshots'],1)
        out=compile(capture(1,snapshot('a',0,())),capture(2,snapshot('a',5,(),False)),capture(3,snapshot('a',10)))
        self.assertEqual(out['intervals'][0]['lower_exclusive_utc'],BASE.isoformat())
        self.assertEqual(out['intervals'][0]['ineligible_snapshots_since_previous_observation'],1)

    def test_repeated_presence_and_same_clock_duplicates_do_not_create_new_events(self):
        out=compile(capture(1,snapshot('a',1)),capture(2,snapshot('a',1)),capture(3,snapshot('a',2)))
        self.assertEqual(len(out['intervals']),1);self.assertEqual(out['same_timestamp_duplicate_snapshots'],1)
        self.assertEqual(len(out['intervals'][0]['presence_receipts']),2)

    def test_conflicting_same_clock_membership_is_not_arbitrarily_ordered(self):
        out=compile(capture(1,snapshot('a',0,())),capture(2,snapshot('a',5,())),
                    capture(3,snapshot('a',5)),capture(4,snapshot('a',10)))
        self.assertEqual(len(out['same_timestamp_ambiguities']),1)
        self.assertIsNone(out['intervals'][0]['lower_exclusive_utc'])
        self.assertEqual(out['first_observations'][0]['status'],'ambiguous_initial_membership')
        only=compile(capture(1,snapshot('a',5,()),snapshot('b',5)),capture(2,snapshot('a',5)))
        self.assertEqual(only['first_observed_groups'],2)
        self.assertEqual(len(only['comparisons']),1)
        self.assertEqual(only['comparisons'][0]['interval_order'],'unresolved_initial_membership_ambiguity')

    def test_later_resolved_reentry_cannot_replace_the_censored_first_interval(self):
        out=compile(capture(1,snapshot('a',1),snapshot('b',0,())),capture(2,snapshot('a',5,())),
                    capture(3,snapshot('a',10)),capture(4,snapshot('b',4)))
        self.assertEqual(len([r for r in out['intervals'] if r['source_key']=='data/a.json']),2)
        self.assertEqual(out['comparisons'][0]['interval_order'],'unresolved_prior_observation_missing')

    def test_all_capture_and_member_identities_are_required(self):
        original=capture(1,snapshot('a',0))
        for mutation in (lambda c:c.update(candidate_scan_complete=1),lambda c:c['sources'][0].update(eligible=1),
                         lambda c:c['sources'][0].update(eligible=False),lambda c:c['sources'][0]['explicit_members'].append({'instrument_id':'equity:US:AAA','direction':'UP'})):
            changed=deepcopy(original);mutation(changed)
            with self.assertRaises(ValueError):compile(changed)
        with self.assertRaises(ValueError):compile(original,original)

    def test_all_source_pairs_remain_present_even_when_none_are_resolved(self):
        out=compile(capture(1,*[snapshot(f'source-{n:03d}',n) for n in range(100)]))
        self.assertEqual(out['possible_comparisons'],4950)
        self.assertEqual(len(out['comparisons']),4950)
        self.assertEqual(out['comparison_status_counts'],{'unresolved_prior_observation_missing':4950})


if __name__=='__main__':unittest.main()
