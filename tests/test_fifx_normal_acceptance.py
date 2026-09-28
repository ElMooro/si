"""The acceptance probe cannot expand into private or downstream live reads."""
from copy import deepcopy
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
path = ROOT/'aws/ops/staged/ops_6315_fifx_normal_publication.py'
if not path.exists():path = ROOT/'aws/ops/STAGED/ops_6315_fifx_normal_publication.py'
spec = importlib.util.spec_from_file_location('fifx_acceptance',path)
probe = importlib.util.module_from_spec(spec);spec.loader.exec_module(probe)


class Tests(unittest.TestCase):
    def test_unreviewed_original_paths_fail_before_client_access(self):
        read, counts = probe.original_reader(object())
        for key in ('data/prospective-outcomes.json','data/report-measurements.json','data/bond-vol.json',
                    'audit-private/20260909-originals/fifx-vol-research/requests/'+64*'a'+'.json',
                    'data/fifx-vol-research/runs/../current.json','https://other.invalid'):
            with self.assertRaisesRegex(ValueError,'Unreviewed'):
                read(key)
        self.assertEqual(counts,{'objects':0,'bytes':0})

    def test_runtime_guard_rejects_different_receipt_code_and_schedule(self):
        value={'receipt':{'status':'matched','commit':probe.EXPECTED},'code_sha256':probe.CODE_SHA,
            'source_files_checked':14,'timeout':900,'memory_mb':2048,'handler_bytes':1496,
            'function_name':probe.FN,'runtime':'python3.12','handler':'lambda_function.lambda_handler',
            'architectures':['x86_64'],'ephemeral_storage_mb':512,
            'schedules':[{'kind':'EventBridge Scheduler','name':'justhodl-fifx-vol-daily','state':'ENABLED',
                'expression':'cron(20 21 ? * MON-FRI *)','timezone':'UTC','native_targets':1,'group':'default'}]}
        probe.check_runtime(value)
        for change in (lambda d:d['receipt'].update(commit='a'*40),lambda d:d.update(code_sha256='other'),
                       lambda d:d.update(timeout=180),lambda d:d.update(schedules=[]),
                       lambda d:d['schedules'][0].update(expression='rate(1 minute)')):
            bad=deepcopy(value);change(bad)
            with self.assertRaises(ValueError):probe.check_runtime(bad)

    def test_unreviewed_packet_rejected_before_replay(self):
        with self.assertRaisesRegex(ValueError,'Whole reviewed'):
            probe.publication(b'{}',lambda key: self.fail('Unexpected replay read'))


if __name__ == '__main__':
    unittest.main()
