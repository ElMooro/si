"""Offline corruption boundaries for the read-only scheduled acceptance check."""
from pathlib import Path
import importlib.util,sys,unittest
from copy import deepcopy
ROOT=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('scheduled_acceptance',ROOT/'aws/ops/staged/ops_6178_scheduled_research_acceptance.py')
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


def fixture():
    policy={'files':{'reviewed':'hash'}}
    capture={'contract':'prospective-research-capture.v1','identity_policy':policy,'generated_at':'same',
        'coverage':{'candidate_scan_complete':False},'sizing_eligible':False,'promotion_eligible':False,'protocol_ref':{'same':'protocol'},
        'records':[{'created':True}],'sources':[{'observations':[{'origin':'rank_observation'}],'eligibility_reasons':[],'unsupported_identity_count':2}]}
    head={k:deepcopy(capture[k]) for k in ('identity_policy','generated_at','coverage','sizing_eligible','promotion_eligible')}
    head.update(schema_version='prospective-research-summary.v1',protocol=capture['protocol_ref'],records_in_capture=1,new_records=1,rank_observations=1,ineligible_sources=0,unsupported_identity_count=2)
    batch={'identity_policy':policy,'sizing_eligible':False,'promotion_eligible':False,'results':[{'status':'PENDING_FORWARD_WINDOW'}],
        'status_counts':{'PENDING_FORWARD_WINDOW':1},'net_return_pct':None,'portfolio_pnl':None}
    return head,capture,{**deepcopy(batch),'batch':{'ref':'sha'}},batch,policy


class Tests(unittest.TestCase):
    def test_complete_heads_and_all_counts_reconcile(self):
        self.assertEqual(m.reconcile(*fixture())['records_in_capture'],1)
    def test_partial_counts_protocol_policy_and_authority_tampering_fail(self):
        for mutate in (lambda h,c,o,b,p:c['records'].clear(),lambda h,c,o,b,p:h.update(records_in_capture=True),
                       lambda h,c,o,b,p:c.update(identity_policy={}),lambda h,c,o,b,p:c.update(protocol_ref={}),
                       lambda h,c,o,b,p:c.update(coverage={}),lambda h,c,o,b,p:o.update(sizing_eligible=True),
                       lambda h,c,o,b,p:b['results'].clear()):
            args=fixture();mutate(*args)
            with self.assertRaises(ValueError):m.reconcile(*args)
    def test_matching_mutable_batch_cannot_forge_totals_or_portfolio_results(self):
        for mode in ('counts','profit'):
            h,c,o,b,p=fixture()
            if mode=='counts':b['status_counts']['PENDING_FORWARD_WINDOW']=2
            else:b['portfolio_pnl']=0
            o={**deepcopy(b),'batch':{'ref':'sha'}}
            with self.assertRaises(ValueError):m.reconcile(h,c,o,b,p)


if __name__=='__main__':unittest.main()
