import importlib.util
import io
import json
import sys
import types
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch
with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:None),
                            'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:'test'),
                            '_fred_shim':types.SimpleNamespace()}):
    spec=importlib.util.spec_from_file_location('manufacturing_test',Path(__file__).resolve().parents[1]/'source/lambda_manufacturing_agent.py')
    e=importlib.util.module_from_spec(spec);spec.loader.exec_module(e)
now=datetime.now(timezone.utc)
MONTH=now.year*12+now.month-2
DATES=[f'{(MONTH-i)//12:04d}-{(MONTH-i)%12+1:02d}-01' for i in range(6)]
OBS=[{'date':d,'value':str(104.6-i)} for i,d in enumerate(DATES)]


class Integrity(unittest.TestCase):
    def test_production_index_is_not_pmi(self):
        r=e.measure_monthly('FRANCE_MANUFACTURING_PRODUCTION_INDEX','FRAPRMNTO01IXOBM',OBS)
        self.assertEqual(r['current'],104.6)
        self.assertEqual(r['unit'],'index_source_base')
        self.assertEqual(r['signal'],'OBSERVATION_ONLY')
        self.assertFalse(r['score_eligible'])
        self.assertEqual(e.interpret_manufacturing_signal('MANUFACTURING_PMI',104.6),'CHECK_DATA')

    def test_monthly_changes_have_actual_calendar_endpoints(self):
        r=e.measure_monthly('INDUSTRIAL_PRODUCTION','INDPRO',OBS)
        self.assertEqual(r['changes'],{'1M':1,'3M':3})
        r=e.measure_monthly('INDUSTRIAL_PRODUCTION','INDPRO',OBS[:1]+OBS[2:])
        self.assertIsNone(r['changes']['1M'])
        self.assertEqual(r['changes']['3M'],3)

    def test_missing_ism_never_defaults_to_fifty(self):
        r=e.analyze_global_manufacturing({})
        self.assertIsNone(r['ism_level'])
        self.assertIsNone(r['expansion_probability'])
        self.assertEqual(r['us_cycle'],'UNKNOWN')

    def test_check_data_cannot_vote_in_composite(self):
        r=e.analyze_global_manufacturing({'ISM_COMPOSITE':{'current':104.6,'signal':'CHECK_DATA','quality':{'status':'fresh'}}})
        self.assertIsNone(r['ism_level'])
        self.assertIn('ISM_COMPOSITE',r['excluded_from_composites'])

    def test_regional_balance_uses_zero_not_fifty(self):
        self.assertEqual(e.interpret_manufacturing_signal('EMPIRE_STATE',3),'POSITIVE_BALANCE')
        self.assertEqual(e.interpret_manufacturing_signal('DALLAS_FED',-3),'NEGATIVE_BALANCE')
        self.assertEqual(e.interpret_manufacturing_signal('PHILLY_FED',0),'ZERO_BALANCE')

    def test_stale_france_is_historical_only(self):
        r=e.measure_monthly('FRANCE_MANUFACTURING_PRODUCTION_INDEX','FRAPRMNTO01IXOBM',[{'date':'2023-01-01','value':'104.6'}])
        self.assertIsNone(r['current'])
        self.assertEqual(r['last_observed_value'],104.6)
        self.assertEqual(r['signal'],'UNAVAILABLE')

    def test_actual_handler_preserves_api_and_publishes_canonical_file(self):
        writes={}
        fake=types.SimpleNamespace(put_object=lambda **kw:writes.update({kw['Key']:json.loads(kw['Body'])}))
        with patch.object(e.boto3,'client',return_value=fake),patch.object(e.urllib.request,'urlopen',side_effect=lambda *a,**k:io.BytesIO(json.dumps({'observations':OBS}).encode())):
            r=e.lambda_handler({'suppress_alerts':True},None)
        doc=json.loads(r['body'])
        self.assertEqual(doc,writes['data/manufacturing.json'])
        self.assertIn('EMPIRE_STATE',doc['us_manufacturing'])
        self.assertNotIn('FRANCE_MANUFACTURING_PMI',doc['global_manufacturing'])
        self.assertIsNone(doc['ecb_data']['eurozone_sentiment'])
        self.assertEqual(doc['recommendations'],[])
        self.assertIsNone(doc['analysis']['ism_level'])


if __name__=='__main__':unittest.main()
