from pathlib import Path
import importlib.util
import json
import copy
import sys
import unittest
ROOT=Path(__file__).resolve().parents[2];sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6236_buyback_measurements_acceptance as op
spec=importlib.util.spec_from_file_location('buyback_native_fixture',ROOT/'aws/lambdas/justhodl-buyback-engine/tests/test_measurements.py')
fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)


class Tests(unittest.TestCase):
    def test_legacy_packet_remains_pending(self):
        self.assertEqual(op.publication(b'{"version":"1.1.0","tickers":{}}')['status'],'pending_original_schedule_publication')

    def test_complete_recomputation_and_tamper_rejection(self):
        ns,writes,_=fixture.MeasurementTests().handler();ns['lambda_handler']();p=writes[op.KEY]
        result=op.publication(json.dumps(p).encode());self.assertEqual(result['issuer_rows'],1);self.assertEqual(result['cashflow_observations'],4)
        self.assertFalse(result['provider_originals_replayed'])
        p['tickers']['TEST']['net_buyback_ttm']=500
        with self.assertRaises(ValueError):op.publication(json.dumps(p).encode())

    def test_forecast_and_population_mutations_are_rejected(self):
        for field,value in [('buyback_score',99),('high_conviction_pump',True),('auth_pct_mcap',10)]:
            ns,writes,_=fixture.MeasurementTests().handler();ns['lambda_handler']();p=writes[op.KEY];p['tickers']['TEST'][field]=value
            with self.assertRaises(ValueError):op.publication(json.dumps(p).encode())


    def test_exact_numeric_boolean_types_and_required_projected_fields(self):
        ns,writes,_=fixture.MeasurementTests().handler();ns['lambda_handler']();original=writes[op.KEY]
        for change in (lambda p:p['tickers']['TEST'].update(calls_eligible=0),
                       lambda p:p.update(n_research_rows=1.0),lambda p:p.update(n_scored=False),
                       lambda p:p['tickers']['TEST']['measurements']['cashflow_observations'][0].update(eligible=1),
                       lambda p:p['tickers']['TEST'].pop('call')):
            p=copy.deepcopy(original);change(p)
            with self.assertRaises(ValueError):op.publication(json.dumps(p).encode())


if __name__=='__main__':unittest.main()
