from pathlib import Path
from fractions import Fraction
import ast,copy,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/ops/checks'))
import bond_flow_candidate as model
import verify_bond_flow as independent
AT='2026-09-26T19:30:00Z'


def fund(ticker,value):
    return {'ticker':ticker,'identity':{'ticker':ticker,'currency':'USD'},
        'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'source_status':'retained_native_history','evidence_tier':'issuer_nav_valued_share_change_estimate',
        'quality':{'status':'recent_source_check'},'observation_date':'2026-09-25',
        'source':{'acquired_at':'2026-09-26T14:59:00Z','evidence':{'captured':True,'sha256':'a'*64}},
        'history':{'key':'data/etf-research/histories/'+'b'*64+'.json','sha256':'b'*64,'bytes':100},
        'flow_windows':{str(n)+'d':{'start_date':'2026-09-17','end_date':'2026-09-24','observations_required':n,'observations_available':n,
            'status':'complete_descriptive_estimate','excluded_dates':[],'value_decimal':value,'precision_sensitivity_decimal':'0.01'} for n in (1,5,20)}}


def packet():return {'contract':'etf-original-research.v1','generated_at':'2026-09-26T15:00:00Z',
    'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'aggregation_period':{'end_date':'2026-09-24'},
    'by_etf':{'A':fund('A','123.45'),'B':fund('B','-234.56')}}


def compile(p=None,scope=None,at=AT):return model.compile_cohorts(model.encoded(p or packet()),at,scope or {'test':['A','B']})


class Tests(unittest.TestCase):
    def test_exact_decimal_net_issuance_with_complete_configured_scope(self):
        out=compile();self.assertEqual(out['configured_funds'],2);self.assertEqual(out['source_funds'],2)
        for row in out['cohorts']['test']['windows'].values():
            self.assertEqual(row['complete_cohort_decimal'],'-111.11');self.assertEqual(row['coverage_subtotal_decimal'],'-111.11')
            self.assertEqual(row['precision_sensitivity_decimal'],'0.02');self.assertFalse(row['whole_market_total'])
            # Independent rational summation on the original input values.
            self.assertEqual(Fraction(row['coverage_subtotal_decimal']),sum(Fraction(packet()['by_etf'][m['ticker']]['flow_windows'][str(row['observations'])+'d']['value_decimal']) for m in row['members']))
        self.assertEqual(out['cohorts']['test']['flow_21d_usd'],None)
        self.assertIn('20_observations',out['cohorts']['test']['windows']);self.assertNotIn('21d',out['cohorts']['test']['windows'])
        for key in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified'):self.assertIs(out[key],False)
        self.assertIsNone(out['equity_to_bond_transfer']);self.assertIsNone(out['duration_tilt'])
        self.assertIs(out['source']['original_replay_verified_here'],False)

    def test_no_coverage_is_missing_and_partial_zero_is_not_a_complete_total(self):
        p=packet();p['by_etf']['A']['flow_windows']['5d']['value_decimal']='0';del p['by_etf']['B']
        row=compile(p)['cohorts']['test']['windows']['5_observations']
        self.assertEqual(row['coverage_subtotal_decimal'],'0');self.assertIsNone(row['complete_cohort_decimal'])
        self.assertEqual(row['included_count'],1);self.assertEqual(row['configured_count'],2);self.assertEqual(len(row['excluded']),1)
        p['by_etf']={};row=compile(p)['cohorts']['test']['windows']['5_observations'];self.assertIsNone(row['coverage_subtotal_decimal']);self.assertEqual(row['status'],'unavailable')

    def test_dates_must_match_at_both_endpoints_before_summing(self):
        p=packet();p['by_etf']['B']['flow_windows']['5d']['start_date']='2026-09-18'
        row=compile(p)['cohorts']['test']['windows']['5_observations'];self.assertEqual(row['status'],'unaligned_windows');self.assertIsNone(row['coverage_subtotal_decimal']);self.assertEqual(len(row['members']),2)

    def test_staleness_currency_identity_units_and_counts_are_not_guessed(self):
        changes=[lambda r:r['identity'].update(currency='EUR'),lambda r:r['identity'].update(ticker='C'),
            lambda r:r['source'].update(acquired_at='2026-09-20T00:00:00Z'),lambda r:r.update(observation_date='2026-09-10'),
            lambda r:r['source'].update(acquired_at='2026-09-26T18:00:00Z'),lambda r:r.update(calls_eligible=True),
            lambda r:r['flow_windows']['5d'].update(value_decimal='NaN'),lambda r:r['flow_windows']['5d'].update(value_decimal=12.3),
            lambda r:r['flow_windows']['5d'].update(observations_available=False),lambda r:r['flow_windows']['5d'].update(excluded_dates=['2026-09-21']),
            lambda r:r['history'].update(key='data/private.json')]
        for change in changes:
            p=packet();change(p['by_etf']['A']);row=compile(p)['cohorts']['test']['windows']['5_observations'];self.assertEqual(row['included_count'],1);self.assertEqual(row['excluded'][0]['ticker'],'A')

    def test_duplicate_members_and_ambiguous_or_expired_packets_fail(self):
        for scope in ({'x':['A','A']},{'x':['A'],'y':['A']},{'x':[]}):
            with self.assertRaises(ValueError):compile(scope=scope)
        p=packet();p['calls_eligible']=True
        with self.assertRaises(ValueError):compile(p)
        for at in ('2026-09-26T14:00:00Z','2026-09-29T00:00:00Z','2026-09-26T19:30:00'):
            with self.assertRaises(ValueError):compile(at=at)
        with self.assertRaises(ValueError):model.compile_cohorts(b'{"x":1,"x":2}',AT)

    def test_original_cohort_definition_is_preserved_exactly(self):
        tree=ast.parse((ROOT/'aws/lambdas/justhodl-bond-desk/source/lambda_function.py').read_text(encoding='utf-8'))
        node=next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='BUCKETS' for t in n.targets))
        self.assertEqual(model.BUCKETS,ast.literal_eval(node.value));self.assertEqual(len(model.BUCKETS),11)

    def test_independent_complete_membership_and_rational_checks_detect_tampering(self):
        p=packet();raw=model.encoded(p);out=compile(p)
        proof=independent.verify(raw,out,{'test':['A','B']},AT)
        self.assertEqual(proof['rational_sums'],6);self.assertEqual(proof['members_checked'],6)
        changes=[lambda v:v['cohorts']['test']['windows']['5_observations'].update(coverage_subtotal_decimal='-111.10'),
            lambda v:v['cohorts']['test']['windows']['5_observations']['members'].pop(),
            lambda v:v['cohorts']['test']['windows']['5_observations']['members'][0].update(unit='EUR'),
            lambda v:v.update(calls_eligible=True),lambda v:v.update(independent_votes=False),
            lambda v:v['source'].update(bytes=len(raw)+1),lambda v:v['cohorts']['test'].update(flow_21d_usd=0)]
        for change in changes:
            modified=copy.deepcopy(out);change(modified)
            with self.assertRaises(ValueError):independent.verify(raw,modified,{'test':['A','B']},AT)
        del p['by_etf']['B'];out=compile(p)
        self.assertEqual(independent.verify(model.encoded(p),out,{'test':['A','B']},AT)['partial_windows'],3)
        p['by_etf']={};out=compile(p)
        self.assertEqual(independent.verify(model.encoded(p),out,{'test':['A','B']},AT)['unavailable_windows'],3)

    def test_small_exact_decimals_are_not_rounded_by_cohort_sum(self):
        p=packet()
        for row in p['by_etf'].values():row['flow_windows']['5d']['value_decimal']='0.'+'0'*60+'1'
        out=compile(p);value=out['cohorts']['test']['windows']['5_observations']['coverage_subtotal_decimal']
        self.assertEqual(Fraction(value),Fraction(2,10**61))
        p['by_etf']['A']['flow_windows']['5d']['value_decimal']='0.'+'0'*80+'1'
        self.assertEqual(compile(p)['cohorts']['test']['windows']['5_observations']['included_count'],1)

if __name__=='__main__':unittest.main()
