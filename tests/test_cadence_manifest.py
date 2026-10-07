"""Scheduled producer classification for the published fleet cadence manifest."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('cadence', ROOT/'aws/tools/build_cadence_manifest.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class CadenceTests(unittest.TestCase):
    def manifest(self, config, source='OUT_KEY = "data/example.json"\n'):
        with tempfile.TemporaryDirectory() as temp:
            engine = Path(temp)/'aws/lambdas/justhodl-example'
            (engine/'source').mkdir(parents=True)
            (engine/'config.json').write_text(json.dumps(config), encoding='utf-8')
            (engine/'source/lambda_function.py').write_text(source, encoding='utf-8')
            return m.build_manifest(temp)['outputs']['example']
    def test_scheduler_weekday_is_scheduled(self):
        row = self.manifest({'eventbridge_scheduler':{'cron':'cron(15 16 ? * MON-FRI *)'}})
        self.assertEqual(row, {'cadence_hours':24.0,'cron':'cron(15 16 ? * MON-FRI *)','engine':'justhodl-example'})
    def test_classic_declaration_preserved_when_both_services_exist(self):
        row = self.manifest({'schedule':{'cron':'rate(6 hours)'},'eventbridge_scheduler':{'cron':'rate(1 hour)'}})
        self.assertEqual(row['cadence_hours'], 6.0)
    def test_string_classic_declaration_unchanged(self):
        self.assertEqual(self.manifest({'schedule':'rate(30 minutes)'})['cadence_hours'], .5)
    def test_unscheduled_engine_stays_event_driven(self):
        self.assertIsNone(self.manifest({})['cadence_hours'])
    def test_disabled_legacy_declaration_can_use_scheduler(self):
        self.assertEqual(self.manifest({'schedule':None,'eventbridge_scheduler':{'cron':'rate(4 hours)'}})['cadence_hours'], 4.0)
    def test_environment_output_key_is_recognized(self):
        row = self.manifest({'eventbridge_scheduler':{'cron':'rate(1 day)'}},
                            'S3_KEY_OUT = os.environ.get("S3_KEY_OUT", "data/example.json")\n')
        self.assertEqual(row['engine'], 'justhodl-example')
        self.assertEqual(row['cadence_hours'], 24)
    def test_actual_master_ranker_owns_its_output_with_daily_cadence(self):
        row = m.build_manifest(str(ROOT))['outputs']['master-ranker']
        self.assertEqual(row, {'cadence_hours':24.0,'cron':'cron(15 16 ? * MON-FRI *)','engine':'justhodl-master-ranker'})
    def test_scheduler_enrichment_does_not_reassign_existing_classic_owner(self):
        with tempfile.TemporaryDirectory() as temp:
            for name,config in [('justhodl-a',{'eventbridge_scheduler':{'cron':'rate(1 hour)'}}),
                                ('justhodl-b',{'schedule':{'cron':'rate(1 day)'}})]:
                engine = Path(temp)/'aws/lambdas'/name
                (engine/'source').mkdir(parents=True)
                (engine/'config.json').write_text(json.dumps(config), encoding='utf-8')
                (engine/'source/lambda_function.py').write_text('OUT_KEY="data/shared.json"\n', encoding='utf-8')
            row = m.build_manifest(temp)['outputs']['shared']
            self.assertEqual(row['engine'], 'justhodl-b')
            self.assertEqual(row['cadence_hours'], 24)


if __name__ == '__main__':
    unittest.main()
