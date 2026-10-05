"""Pure invented release metadata; no AWS/provider access."""
import copy,importlib.util
from pathlib import Path
import unittest
from unittest.mock import Mock
R=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('market_native_acceptance',R/'aws/ops/staged/ops_6483_census_additional_acceptance.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
def record(fn):
    return {**copy.deepcopy(module.SPECS[fn]['controls']),'source_files_checked':len(module.SPECS[fn]['source_hashes']),
      'handler_bytes':123,'code_sha256':'invented','receipt':{'status':'matched','commit':'invented-commit'}}
class AcceptanceTests(unittest.TestCase):
    def test_exact_source_receipt_and_controls_both_functions(self):
        for fn in module.SPECS:
            got=module.normalized(record(fn),fn,'invented-commit');self.assertEqual(got['function_name'],fn)
    def test_enabled_schedule_cannot_be_disabled(self):
        fn='justhodl-symdir';value=record(fn);disabled=next(r for r in value['schedules'] if r['state']=='ENABLED');disabled['state']='DISABLED'
        with self.assertRaises(ValueError):module.normalized(value,fn,'invented-commit')
    def test_every_control_and_source_count_matters(self):
        for fn in module.SPECS:
            for key in ('timeout','memory_mb','architectures','role','handler','runtime','ephemeral_storage_mb','source_files_checked'):
                value=record(fn);value[key]=None
                with self.subTest(fn=fn,key=key),self.assertRaises(ValueError):module.normalized(value,fn,'invented-commit')
    def test_exact_commit_not_green_run(self):
        for fn in module.SPECS:
            with self.assertRaises(ValueError):module.normalized(record(fn),fn,'different-commit')
    def test_missing_or_duplicate_binding_fails(self):
        fn='justhodl-symdir'
        for mutate in (lambda r:r.pop(),lambda r:r.append(copy.deepcopy(r[0]))):
            value=record(fn);mutate(value['schedules'])
            with self.assertRaises(ValueError):module.normalized(value,fn,'invented-commit')
    def test_only_schedule_order_is_normalized(self):
        fn='justhodl-symdir';value=record(fn);value['schedules'].reverse()
        self.assertEqual(module.normalized(value,fn,'invented-commit'),module.normalized(record(fn),fn,'invented-commit'))
    def test_receipt_boundary_blocks_all_other_data(self):
        client=Mock();wrapper=module.ReceiptOnly(client,'justhodl-symdir')
        wrapper.get_object(Bucket=module.BUCKET,Key='data/ops/releases/justhodl-symdir.json')
        for key in ('data/private/invented.json','data/warm/tv-bars/_index.json','data/ops/releases/justhodl-tv-bars.json'):
            with self.assertRaises(ValueError):wrapper.get_object(Bucket=module.BUCKET,Key=key)
        self.assertEqual(client.get_object.call_count,1)
if __name__=='__main__':unittest.main()
