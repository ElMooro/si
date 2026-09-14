"""Owned model source for Gear B: manifests + pins are the only trust, and launch_sft runs the owned recipe."""
import importlib.util
import io
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws/lambdas/justhodl-ai/source'))
sys.path.insert(0, str(ROOT / 'tests/factory'))
from test_factory import MemoryS3  # noqa: E402
import gear_b_own as own  # noqa: E402


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, str(ROOT / path))
    loaded = importlib.util.module_from_spec(spec); spec.loader.exec_module(loaded)
    return loaded


PRI = 'private-test'


def put(s3, key, doc):
    s3.put_object(Bucket=PRI, Key=key, Body=json.dumps(doc).encode())


def good_manifest():
    return {'schema_version': 'factory-base-weights.v1', 'model_id': 'qwen2-5-coder-7b-instruct', 'repo': 'Qwen/Qwen2.5-Coder-7B-Instruct',
            'revision': 'c03e6d358207e414f1eca0bb1e51b0e1d3a6bd4a', 'license': 'apache-2.0', 'manifest_sha256': 'a' * 64,
            's3_prefix': 's3://private-test/factory/models/base/qwen2-5-coder-7b-instruct/c03e6d35/',
            'files': [{'path': 'config.json', 'sha256': 'b' * 64, 'bytes': 700}, {'path': 'model-00001-of-00004.safetensors', 'sha256': 'c' * 64, 'bytes': 3_900_000_000}],
            'total_bytes': 3_900_000_700}


def good_pin(image='123456789012.dkr.ecr.us-east-1.amazonaws.com/justhodl/factory-train:hf-pt2.3@sha256:' + 'd' * 64):
    return {'schema_version': 'factory-training-pin.v1', 'training_image': image, 'require_digest': True,
            'bundle_uri': 's3://private-test/factory/training/bundles/train-8adf3de0456d115d.tar.gz', 'bundle_sha256': 'e' * 64,
            'program': 'train_qlora.py', 'max_steps': 400, 'lora_r': 16, 'learning_rate': '2e-4', 'max_seq_len': 2048, 'epochs': 1,
            'default_instance': 'ml.g5.2xlarge', 'instances': ['ml.g5.2xlarge'], 'pinned_at': '2026-09-13T21:00:00Z'}


class OwnSpecTests(unittest.TestCase):
    def setUp(self):
        self.s3 = MemoryS3()
        self.control = {'model_source': 'own', 'model_id': 'qwen2-5-coder-7b-instruct'}
        put(self.s3, own.BASE_PREFIX + 'qwen2-5-coder-7b-instruct/manifest.json', good_manifest())
        put(self.s3, own.PIN_KEY, good_pin())

    def test_spec_shape_matches_launch_sft_contract(self):
        spec = own.own_spec(self.s3, PRI, self.control)
        self.assertTrue(spec['training_supported'] and spec['training_image'].startswith('123456789012.dkr.ecr'))
        self.assertEqual(spec['training_image'], '123456789012.dkr.ecr.us-east-1.amazonaws.com/justhodl/factory-train@sha256:' + 'd' * 64)   # digest only, never tag+digest
        self.assertEqual(spec['training_image_pinned'], good_pin()['training_image'])
        self.assertEqual(spec['training_artifact'], good_manifest()['s3_prefix'])
        self.assertEqual(spec['training_script'], good_pin()['bundle_uri'])
        self.assertEqual(spec['hyperparameters']['sagemaker_program']['default'], 'train_qlora.py')
        self.assertEqual(spec['license'], 'apache-2.0')

    def test_refusals(self):
        for patch, why in (({'license': 'llama2'}, 'license'), ({'files': [{'path': 'config.json', 'sha256': 'zz', 'bytes': 1}]}, 'unhashed'),
                           ({'model_id': 'other'}, 'model_id'), ({'files': [{'path': 'config.json', 'sha256': 'b' * 64, 'bytes': 1}]}, 'weight shards')):
            m = {**good_manifest(), **patch}
            put(self.s3, own.BASE_PREFIX + 'qwen2-5-coder-7b-instruct/manifest.json', m)
            with self.assertRaises(own.OwnSpecRefused, msg=why) as ctx:
                own.own_spec(self.s3, PRI, self.control)
            self.assertIn(why.split()[0], str(ctx.exception))
        put(self.s3, own.BASE_PREFIX + 'qwen2-5-coder-7b-instruct/manifest.json', good_manifest())
        put(self.s3, own.PIN_KEY, good_pin(image='123456789012.dkr.ecr.us-east-1.amazonaws.com/justhodl/factory-train:hf-pt2.3'))
        with self.assertRaises(own.OwnSpecRefused) as ctx:
            own.own_spec(self.s3, PRI, self.control)
        self.assertIn('digest', str(ctx.exception))
        put(self.s3, own.PIN_KEY, {**good_pin(), 'training_image': 'docker.io/library/python:3.12'})
        with self.assertRaises(own.OwnSpecRefused):
            own.own_spec(self.s3, PRI, self.control)

    def test_launch_sft_runs_the_owned_recipe_on_the_owned_weights(self):
        gb = module('gear_b_under_test', 'aws/lambdas/justhodl-ai/source/gear_b.py')
        spec = own.own_spec(self.s3, PRI, self.control)
        calls = {}

        class FakeSM:
            def create_training_job(self, **kw):
                calls['job'] = kw
                return {'TrainingJobArn': 'arn:test'}
        gb.validate_control = lambda control, policy: None
        gb.cg.hourly_price = lambda pricing, s3, bucket, it, family='hosting': {'usd_per_hour': 1.212, 'source': 'test'}
        gb._job_records = lambda s3, bucket: []
        gb.budget_check = lambda *a, **k: None
        control = {**self.control, 'instance_type': 'ml.g5.2xlarge', 'max_runtime_s': 3600, 'lora': {'lora_r': '32'}}
        manifest = {'ok': True, 'eligibility_digest': 'f' * 64, 'train_sha256': '1' * 64, 'holdout_digest': '2' * 64, 'generation': 1, 'prefix': 'factory/gearb/datasets/gen-1/'}
        record = gb.launch_sft(FakeSM(), self.s3, spec=spec, role_arn='arn:aws:iam::123456789012:role/x', private_bucket=PRI, control=control,
                               policy={}, manifest=manifest, projected={}, pricing=None, region='us-east-1')
        job = calls['job']
        self.assertEqual(job['AlgorithmSpecification']['TrainingImage'], '123456789012.dkr.ecr.us-east-1.amazonaws.com/justhodl/factory-train@sha256:' + 'd' * 64)
        self.assertEqual(job['HyperParameters']['sagemaker_program'], 'train_qlora.py')
        self.assertEqual(job['HyperParameters']['sagemaker_submit_directory'], good_pin()['bundle_uri'])
        self.assertEqual(job['HyperParameters']['lora_r'], '32')                     # control overrides the pin
        channels = {c['ChannelName']: c['DataSource']['S3DataSource']['S3Uri'] for c in job['InputDataConfig']}
        self.assertEqual(channels['model'], good_manifest()['s3_prefix'])
        self.assertTrue(job['EnableManagedSpotTraining'])
        self.assertEqual(record['model_id'], 'qwen2-5-coder-7b-instruct')

    def test_recipe_refuses_without_rows_or_weights(self):
        import tempfile
        recipe = module('train_qlora_under_test', 'factory/training/train_qlora.py')
        with tempfile.TemporaryDirectory() as tmp:
            recipe.TRAIN_DIR = Path(tmp) / 'training'; recipe.MODEL_DIR = Path(tmp) / 'model'; recipe.OUT_DIR = Path(tmp) / 'out'
            recipe.TRAIN_DIR.mkdir(); recipe.MODEL_DIR.mkdir()
            self.assertEqual(recipe.main(), 3)
            self.assertEqual(json.loads((recipe.OUT_DIR / 'train_manifest.json').read_text())['status'], 'refused')
            (recipe.TRAIN_DIR / 'rows.jsonl').write_text(json.dumps({'prompt': 'def f():', 'completion': ' return 1'}) + '\n')
            self.assertEqual(recipe.main(), 4)
        pin = module('pin_under_test', 'scripts/factory_training_pin.py')
        data = pin.bundle_bytes()
        self.assertEqual(data, pin.bundle_bytes())          # deterministic bundle -> content-addressed key
        import tarfile
        names = sorted(tarfile.open(fileobj=io.BytesIO(data), mode='r:gz').getnames())
        self.assertEqual(names, ['generate.py', 'requirements.txt', 'train_qlora.py'])

    def test_burst_generator_refuses_holdout_tasks_and_plans_sampling(self):
        import tempfile
        gen = module('generate_under_test', 'factory/training/generate.py')
        self.assertEqual(gen.plan({'samples_per_task': '8', 'temperature': '1.1'}, 'burst')['samples_per_task'], 8)
        self.assertEqual(gen.plan({'samples_per_task': '8', 'temperature': '1.1'}, 'exam'), {**gen.plan({}, 'exam')})
        self.assertEqual(gen.plan({}, 'exam')['temperature'], 0.0)
        self.assertEqual(gen.plan({'samples_per_task': '99'}, 'burst')['samples_per_task'], 16)
        with tempfile.TemporaryDirectory() as tmp:
            tasks = Path(tmp) / 'tasks'; tasks.mkdir()
            (tasks / 't.jsonl').write_text('\n'.join(json.dumps(r) for r in [
                {'task_id': 'a', 'prompt': 'def f():', 'family': 'code'},
                {'task_id': 'h', 'prompt': 'secret', 'family': 'code', 'holdout': True},
                {'task_id': 'b', 'prompt': 'def g():'}]) + '\n')
            rows, refused = gen.load_tasks(tasks, 'burst', 100)
            self.assertEqual(([r['task_id'] for r in rows], refused), (['a', 'b'], 1))
            rows, refused = gen.load_tasks(tasks, 'exam', 100)
            self.assertEqual(([r['task_id'] for r in rows], refused), (['a', 'h', 'b'], 0))
        spec = own.burst_spec(self.s3, PRI, self.control, tasks_uri='s3://private-test/factory/curriculum/code/2026-09-13/', samples_per_task=6, temperature=0.9)
        self.assertEqual(spec['hyperparameters']['sagemaker_program']['default'], 'generate.py')
        self.assertEqual(spec['hyperparameters']['samples_per_task']['default'], '6')
        self.assertTrue(spec['training_image'].endswith('@sha256:' + 'd' * 64) and ':hf-pt2.3@' not in spec['training_image'])
        exam = own.burst_spec(self.s3, PRI, self.control, mode='exam', tasks_uri='s3://private-test/factory/exams/code/', adapter_uri='s3://private-test/factory/champions/gen-3/adapter/')
        self.assertEqual((exam['hyperparameters']['temperature']['default'], exam['hyperparameters']['adapter_generation']['default']), ('0.0', 'adapter'))
        with self.assertRaises(own.OwnSpecRefused):
            own.burst_spec(self.s3, PRI, self.control)


if __name__ == '__main__':
    unittest.main(verbosity=2)
