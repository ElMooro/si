"""Complete packaged chronology tests. All original/storage clients are synthetic."""
from pathlib import Path
import importlib,json,sys,tempfile,types,unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[4];SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path[:0]=[str(SOURCE),str(ROOT/'aws/shared'),str(ROOT/'tests')]
import lambda_function as handler
import genealogy_native_publication as publication
import genealogy_native_runtime as native
import genealogy_public_archive as archive
import genealogy_cache_checkpoint as checkpoint
import genealogy_revision_cache as revision
import genealogy_streamed_pipeline
from test_genealogy_public_archive import fixture
from test_genealogy_revision_cache import Client,Cache
from test_genealogy_native_publication import DiskStore,Publications
from test_genealogy_native_runtime import Runtime
from test_genealogy_cache_checkpoint import Checkpoints


class Packaged(unittest.TestCase):
    def test_every_packaged_calculation_module_matches_the_accepted_candidate(self):
        candidates={'genealogy_research_model','genealogy_public_archive','genealogy_registration_model','genealogy_capture_timing'}
        for name in publication.COMPILERS:
            module=importlib.import_module(name);path=Path(module.__file__)
            if name in candidates:expected=ROOT/'aws/ops/checks/genealogy_native_candidate'/path.name
            elif (SOURCE/path.name).exists():expected=ROOT/'aws/ops/checks'/path.name
            else:expected=ROOT/'aws/shared'/path.name
            self.assertEqual(path.read_bytes(),expected.read_bytes(),name)
            if (SOURCE/path.name).exists():self.assertEqual(path.parent,SOURCE)

    def test_handler_uses_only_synthetic_originals_and_ignores_event_overrides(self):
        originals,_,_=fixture();source=Client(originals)
        with tempfile.TemporaryDirectory() as directory:
            store=DiskStore(Path(directory)/'objects')
            class Combined:
                def get_object(self,**kw):
                    return (source if kw['Key'].startswith(archive.PREFIX) else store).get_object(**kw)
                def put_object(self,**kw):return store.put_object(**kw)
                def head_object(self,**kw):return source.head_object(**kw)
                def get_paginator(self,name):return source.get_paginator(name)
            with patch('boto3.client',return_value=Combined()) as factory:
                result=handler.lambda_handler({'cutoff':'invalid','key':'private','write':False},
                    types.SimpleNamespace(get_remaining_time_in_millis=lambda:600000))
            self.assertTrue(result['published']);self.assertEqual(result['original_objects'],4)
            self.assertFalse(result['calls_eligible']);self.assertFalse(result['sizing_eligible'])
            self.assertEqual(factory.call_args.args,('s3',));self.assertEqual(factory.call_count,1)
            self.assertTrue(all(k.startswith(publication.PREFIX) for k in store.writes))
            self.assertNotIn('data/signal-genealogy.json',store.writes)

    def test_insufficient_or_malformed_runtime_stops_before_client_construction(self):
        for value in (1,True,None,149999):
            with self.subTest(value=value),patch('boto3.client') as factory:
                with self.assertRaisesRegex(RuntimeError,'runtime_budget_inventory'):
                    handler.lambda_handler({},types.SimpleNamespace(get_remaining_time_in_millis=lambda:value))
                factory.assert_not_called()

    def test_original_schedule_is_preserved_and_resources_are_explicit(self):
        config=json.loads((SOURCE.parent/'config.json').read_bytes())
        self.assertEqual(config['schedule']['rule_name'],'signal-genealogy-daily')
        self.assertEqual(config['schedule']['cron'],'cron(40 6 * * ? *)')
        self.assertEqual((config['memory'],config['timeout']),(2048,600))


if __name__=='__main__':
    suite=unittest.TestSuite()
    for case in (Packaged,Runtime,Publications,Cache,Checkpoints):
        suite.addTests(unittest.defaultTestLoader.loadTestsFromTestCase(case))
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)
