"""Actual Calls frozen compiler path retains complete typed liquidity periods."""
from copy import deepcopy
from pathlib import Path
import sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
from calls_free_brief import build
from calls_research_replay import canonical,compile_frozen,digest,prepare,project,replay

KEY='data/liquidity-flow.json';AT='2026-09-28T20:05:00Z'


def packet():
    return {'contract':'liquidity-flow-research.v1','generated_at':'2026-09-28T12:35:00Z',
        'current':{'net_liquidity_b':0,'observation_dates':{'WALCL':'2026-09-23','WTREGEN':'2026-09-23','RRPONTSYD':'2026-09-25'}},
        'quality':{'status':'fresh','observation_date':'2026-09-23'}}


def prepared(value,at=AT):return prepare(lambda key:value if key==KEY else {},at)


class CallsPeriods(unittest.TestCase):
    def test_real_prepare_path_preserves_all_three_periods_and_reproduces_the_formatter(self):
        original=packet();before=deepcopy(original);bundle=prepared(original)
        direct=build(lambda key:original if key==KEY else {},AT)
        frozen=bundle['payload']['output'];row=frozen['evidence'][0]
        self.assertEqual(bundle['payload']['inputs'][KEY]['projection'],original)
        self.assertEqual(frozen['brief_md'],direct['brief_md'])
        self.assertEqual(row['component_observation_dates'],original['current']['observation_dates'])
        self.assertEqual(row['period_alignment_status'],'complete_oldest_date_matches')
        self.assertEqual(row['value'],0);self.assertEqual(row['quality_status'],'fresh')
        self.assertIn('WTREGEN weekly average ending 2026-09-23',frozen['brief_md'])
        self.assertIn('RRP daily operation 2026-09-25',frozen['brief_md'])
        self.assertEqual(replay(bundle)['status'],'reproduced');self.assertEqual(original,before)
        self.assertFalse(row['calls_eligible']);self.assertFalse(frozen['sizing_eligible'])
        self.assertEqual(frozen['call_verb'],'WAIT');self.assertEqual(frozen['coverage']['eligible_votes'],0)

    def test_period_revision_changes_frozen_input_and_evidence_identity_without_mutating_prior_run(self):
        original=packet();first=prepared(original);before=canonical(first)
        original['current']['observation_dates']['RRPONTSYD']='2026-09-24';second=prepared(original)
        self.assertNotEqual(first['payload']['inputs'][KEY]['sha256'],second['payload']['inputs'][KEY]['sha256'])
        self.assertNotEqual(first['payload']['output']['evidence'][0]['evidence_id'],second['payload']['output']['evidence'][0]['evidence_id'])
        self.assertEqual(canonical(first),before)
        for value in (first,second):self.assertEqual(replay(value)['status'],'reproduced')

    def test_incomplete_mismatched_and_future_periods_keep_value_but_cannot_claim_freshness(self):
        for change,reason in ((None,'incomplete_component_dates'),('2026-09-22','oldest_date_mismatch'),
                             ('2026-09-29','future_component_date')):
            original=packet();original['current']['observation_dates']['RRPONTSYD']=change
            bundle=prepared(original);out=bundle['payload']['output'];row=out['evidence'][0]
            self.assertEqual(row['value'],0);self.assertEqual(row['period_alignment_status'],reason)
            self.assertEqual(row['quality_status'],'unverified')
            self.assertIn(row['series_id'],out['quality']['missing'])
            self.assertFalse(out['evidence_inventory']['permission_mask'][0]['may_inform_research'])
            self.assertEqual(replay(bundle)['status'],'reproduced')

    def test_new_fields_are_exact_typed_public_values_not_arbitrary_upstream_strings(self):
        for bad in ('PRIVATE-CANARY','2026-09-25 PRIVATE-CANARY','2026-09-25T00:00:00Z',
                    '2026-02-30','2026-9-25',True,{},[],None):
            original=packet();original['current']['observation_dates']['RRPONTSYD']=bad
            original['current']['observation_dates']['ACCOUNT']='PRIVATE-CANARY'
            original['notes']='PRIVATE-CANARY';original['snapshot']={'positions':['PRIVATE-CANARY']}
            bundle=prepared(original);row=bundle['payload']['output']['evidence'][0]
            self.assertIsNone(row['component_observation_dates']['RRPONTSYD'])
            self.assertNotIn('PRIVATE-CANARY',canonical(bundle).decode())
            self.assertNotIn('ACCOUNT',canonical(bundle).decode())
            self.assertEqual(replay(bundle)['status'],'reproduced')
        for contract in ('PRIVATE-CANARY','liquidity-flow-research.v999',{},[]):
            original=packet();original['contract']=contract
            self.assertNotIn('contract',project(KEY,original))
            self.assertNotIn('PRIVATE-CANARY',canonical(prepared(original)).decode())

    def test_tampered_period_cannot_replay_by_only_rehashing_outer_payload(self):
        bundle=prepared(packet());bundle['payload']['inputs'][KEY]['projection']['current']['observation_dates']['WALCL']='2026-09-24'
        bundle['payload_sha256']=digest(bundle['payload']);bundle['run_id']='calls-research-'+bundle['payload_sha256']
        with self.assertRaisesRegex(ValueError,'input projection'):replay(bundle)
        bundle['payload']['inputs'][KEY]['sha256']=digest(bundle['payload']['inputs'][KEY]['projection'])
        bundle['payload_sha256']=digest(bundle['payload']);bundle['run_id']='calls-research-'+bundle['payload_sha256']
        with self.assertRaisesRegex(ValueError,'reproduced brief differs'):replay(bundle)

    def test_calendar_eligibility_uses_same_utc_day_for_equivalent_timestamps(self):
        original=packet()
        utc=build(lambda key:original if key==KEY else {},'2026-09-28T20:05:00Z')
        offset=build(lambda key:original if key==KEY else {},'2026-09-29T10:05:00+14:00')
        self.assertEqual(utc['evidence'],offset['evidence']);self.assertEqual(utc['coverage'],offset['coverage'])
        with self.assertRaisesRegex(ValueError,'timezone-aware'):build(lambda _:original,'2026-09-28T20:05:00')

    def test_packet_clock_cannot_be_missing_future_or_old_while_claiming_fresh(self):
        for stamp,status in ((None,'unverified'),('PRIVATE-CANARY','unverified'),
                             ('2026-09-28','unverified'),('2026-09-28T19:00:00','unverified'),
                             ('2026-09-28T20:05:01Z','unavailable'),('2026-09-27T18:04:59Z','stale')):
            original=packet();original['generated_at']=stamp
            bundle=prepared(original);row=bundle['payload']['output']['evidence'][0]
            self.assertEqual(row['quality_status'],status,(stamp,row));self.assertEqual(row['value'],0)
            self.assertFalse(bundle['payload']['output']['evidence_inventory']['permission_mask'][0]['may_inform_research'])
            self.assertNotIn('PRIVATE-CANARY',canonical(bundle).decode())
            self.assertEqual(replay(bundle)['status'],'reproduced')
        original=packet();original['generated_at']='2026-09-27T18:05:00Z'
        row=prepared(original)['payload']['output']['evidence'][0]
        self.assertEqual(row['quality_status'],'fresh');self.assertEqual(row['source_age_seconds'],26*3600)
        original['quality']['status']='partial'
        self.assertEqual(prepared(original)['payload']['output']['evidence'][0]['quality_status'],'partial')

    def test_regenerating_the_brief_does_not_renew_frozen_source_publication(self):
        first=prepared(packet());later=compile_frozen(first['payload']['inputs'],'2026-09-29T14:35:01Z')
        self.assertEqual(later['evidence'][0]['source_clock_status'],'stale')
        self.assertEqual(later['evidence'][0]['source_generated_at'],'2026-09-28T12:35:00+00:00')
        self.assertEqual(later['evidence'][0]['quality_status'],'stale')
        self.assertEqual(later['evidence'][0]['value'],0)
        self.assertEqual(first['payload']['output']['evidence'][0]['quality_status'],'fresh')


if __name__=='__main__':unittest.main(verbosity=2)
