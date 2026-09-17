"""FR2004 regressions execute discovery, the real handler and its output contract."""
import copy
import importlib.util
import json
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[4]
SOURCE = Path(__file__).resolve().parents[1] / 'source'
sys.path[:0] = [str(SOURCE), str(ROOT/'aws/shared')]
from pd_integrity import complete_sum, canonical_fails, observation_quality, METHOD
with patch.dict(sys.modules, {'boto3': types.SimpleNamespace(client=lambda *a, **k: None)}):
    spec = importlib.util.spec_from_file_location('pd_test_engine', SOURCE/'lambda_function.py')
    engine = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(engine)

NOW = datetime.now(timezone.utc)
D = (NOW-timedelta(days=8)).date().isoformat()
DATES = [(NOW-timedelta(days=8+7*i)).date().isoformat() for i in range(55)][::-1]
FAILS = {'generated_at': NOW.isoformat(), 'treasury': {
    'as_of': D, 'unit': 'USD_bn_par', 'complete': True,
    'ftd_bn': 40, 'ftr_bn': 50, 'gross_bn': 90, 'quality': {'status': 'fresh'},
    'scope': 'US_TREASURY_INCLUDING_TIPS', 'gross': [[D,90]]}}
BUCKETS = {'L13': 'DUE IN 13 MONTHS OR LESS', 'G13': 'GREATER THAN 13 MONTHS BUT LESS THAN 5 YEARS',
           'G5L10': 'GREATER THAN 5 YEARS BUT LESS THAN 10 YEARS', 'G10': 'GREATER THAN 10 YEARS'}
CATALOG = []
for suffix, desc in BUCKETS.items():
    for grade in ('', 'BEL'):
        kid = 'PDPOSCSBND-'+grade+suffix
        CATALOG += [{'keyid':kid,'description':'CORPORATE '+desc},
                    {'keyid':kid+'C','description':'CORPORATE '+desc+' - Change From Previous Week'}]
CATALOG += [{'keyid':'PDPOSCSCP','description':'CORPORATE COMMERCIAL PAPER'},
            {'keyid':'PDPOSCS-TOT','description':'CORPORATE TOTAL'},
            {'keyid':'PDPOSCS-TOTC','description':'COMMERCIAL PAPER Change From Previous Week'},
            {'keyid':'PDTRCS-TOT','description':'TRANSACTIONS CORPORATE'},
            {'keyid':'PDTRGST-TOT','description':'TRANSACTIONS TIPS'},
            {'keyid':'PDTRGS-EXTB','description':'TRANSACTIONS EX TIPS'},
            {'keyid':'PDTRGS-EXTBC','description':'TRANSACTIONS Change From Previous Week'}]
for prefix in ('PDSIRRA-', 'PDSORA-'):
    for suffix in ('UTSETTOT','UTSTTOT','CDTOT'):
        CATALOG.append({'keyid':prefix+suffix,'description':'FINANCING'})


def run(missing=None, stale=False):
    writes = {}
    spec_doc = {'classes': {'TREASURY_COUPONS': [
        {'keyid':'PDSI2NSP','tenor_y':2}, {'keyid':'PDSI5NSP','tenor_y':5}]}}
    def get(url, *a, **k):
        if 'list/timeseries' in url:
            return {'pd': {'timeseries': CATALOG}}
        kid = url.rsplit('/',1)[-1].replace('.json','')
        dates = [] if kid == missing else DATES[:-4] if stale else DATES
        value = 100000 if kid.endswith('C') else 1000
        return {'pd': {'timeseries': [{'asofdate':d,'value':str(value)} for d in dates]}}
    def read(k, d=None):
        return copy.deepcopy(spec_doc if k==engine.SPEC_KEY else FAILS if k=='data/settlement-fails.json' else d)
    with patch.object(engine,'_j',side_effect=read), patch.object(engine,'_get',side_effect=get), \
         patch.object(engine,'s3',types.SimpleNamespace(put_object=lambda **kw:writes.update({kw['Key']:json.loads(kw['Body'])}))), \
         patch.object(engine.time,'sleep'), patch.object(engine,'_emit_signal',side_effect=AssertionError('unvalidated signal emitted')):
        result=engine.lambda_handler({'suppress_alerts':True})
    assert result['ok']
    return writes[engine.OUT], writes


class Integrity(unittest.TestCase):
    def test_complete_sum_never_uses_missing_as_zero(self):
        self.assertEqual(complete_sum([{'a':1,'b':3},{'a':-1}]), {'a':0})
        self.assertEqual(complete_sum([{'a':1},{}]), {})

    def test_real_discovery_excludes_changes_and_finds_cp_level(self):
        out,writes=run()
        self.assertEqual(out['corporate']['net_bonds_b'],8)
        self.assertEqual(out['corporate']['cp_b'],1)
        self.assertEqual(out['corporate']['net_under5y_b'],4)
        self.assertEqual(len(writes[engine.SPEC_KEY]['pos']['corp']),10)

    def test_canonical_fails_is_identical_and_not_rescaled(self):
        out,_=run()
        self.assertEqual(out['settlement_fails']['treasury'],FAILS['treasury'])
        self.assertTrue(out['settlement_fails']['usable'])

    def test_bad_fails_never_becomes_calm(self):
        for mutate in (lambda d:d['treasury'].update(gross_bn=0),
                       lambda d:d['treasury']['quality'].update(status='stale'),
                       lambda d:d.update(generated_at='2020-01-01T00:00:00Z'),
                       lambda d:d['treasury'].update(ftd_bn=float('nan'))):
            d=copy.deepcopy(FAILS);mutate(d)
            self.assertFalse(canonical_fails(d)['usable'])
            self.assertIsNone(canonical_fails(d)['treasury'])

    def test_missing_bond_bucket_does_not_fall_back_to_different_scope(self):
        out,_=run('PDPOSCSBND-G10')
        self.assertIsNone(out['corporate'])

    def test_specific_issues_not_full_treasury_and_no_net_velocity(self):
        out,_=run()
        self.assertIsNone(out['net_treasury_total_b'])
        self.assertEqual(out['specific_issue_net_settled_b'],2)
        self.assertIsNone(out['corporate']['turnover_velocity'])
        self.assertIsNone(out['call'])
        self.assertFalse(out['execution_eligible'])

    def test_transactions_are_daily_averages_and_tips_separate(self):
        out,_=run()
        self.assertEqual(out['transactions']['CORPORATE']['daily_average_b'],1)
        self.assertIsNone(out['transactions']['CORPORATE']['weekly_b'])
        self.assertIn('TIPS',out['transactions'])
        self.assertIn('TREASURY',out['transactions'])

    def test_treasury_financing_has_only_two_collateral_classes(self):
        out,_=run()
        t=out['financing']['treasury']
        self.assertEqual(t['reverse_repo_in_b'],2)
        self.assertEqual(t['gross_two_sided_b'],4)

    def test_missing_specific_issue_does_not_publish_partial_sum(self):
        out,_=run('PDSI2NSP')
        self.assertIsNone(out['specific_issue_net_settled_b'])
        self.assertIsNone(out['net_positions_usd_b']['TREASURY_COUPONS'])
        self.assertNotEqual(out['quality']['status'],'fresh')

    def test_stale_future_and_invalid_dates_are_unavailable(self):
        out,_=run(stale=True)
        self.assertEqual(out['corporate']['regime'],'UNKNOWN')
        self.assertIsNone(out['specific_issue_net_settled_b'])
        self.assertEqual(observation_quality('2999-01-01')['status'],'invalid')
        self.assertEqual(observation_quality(None)['status'],'incomplete')


if __name__=='__main__':
    unittest.main()
