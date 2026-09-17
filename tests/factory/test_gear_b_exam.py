"""The exam launches and decides itself inside the Gear B tick (2026-09-17).

A trained candidate (exam.status pending_exam) -> adapter extracted from the job artifact (create-if-absent manifest) ->
one frozen-holdout exam job (prompts only, greedy, capped) -> exam_running; the runner grades it (factory-exam.yml) ->
decide_pending applies the shared promotion contract against the pinned base exam. One exam in flight at a time; while
one is in flight the tick refuses to launch training (single spot instance).
"""
import io
import json
import sys
import tarfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws' / 'lambdas' / 'justhodl-ai' / 'source'))
sys.path.insert(0, str(ROOT / 'aws' / 'shared'))
sys.path.insert(0, str(ROOT / 'tests' / 'factory'))
from test_factory import MemoryS3  # noqa: E402
from test_gear_b import FakeSageMaker, PRI, PUB  # noqa: E402
import gear_b  # noqa: E402
import gear_b_own as own  # noqa: E402

BASE_ID = 'qwen2-5-coder-7b-instruct'


class ListingSageMaker(FakeSageMaker):
    def __init__(self, statuses=None, inflight=None):
        super().__init__(statuses); self.inflight = list(inflight or [])
    def list_training_jobs(self, **kw):
        return {'TrainingJobSummaries': [{'TrainingJobName': n} for n in self.inflight if kw.get('NameContains', '') in n]}


def artifact_bytes(status='trained'):
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode='w:gz') as t:
        for name, data in (('train_manifest.json', json.dumps({'status': status, 'rows': 570, 'steps': 18, 'train_loss': 0.4, 'adapter_sha256': 'f' * 64, 'base_revision': 'c03e6d35'}).encode()),
                           ('adapter/adapter_config.json', b'{"r": 16}'), ('adapter/adapter_model.safetensors', b'\x00' * 64)):
            info = tarfile.TarInfo(name); info.size = len(data); t.addfile(info, io.BytesIO(data))
    return buf.getvalue()


class ExamTests(unittest.TestCase):
    def setUp(self):
        self.cloud = MemoryS3()
        self.policy = dict(gear_b.cg.DEFAULT_POLICY, daily_budget_usd=20.0, training_max_runtime_s=3600)
        self.control = {'schema_version': 'gearb-control.v1', 'enabled': True, 'model_id': BASE_ID, 'model_source': 'own', 'model_version': 'c03e6d358207e414f1eca0bb1e51b0e1d3a6bd4a',
                        'instance_type': 'ml.g5.2xlarge', 'exam_instance_type': 'ml.g5.2xlarge', 'daily_budget_usd': 20.0, 'season_cap_usd': 600.0,
                        'max_runtime_s': 3 * 3600, 'exam_max_runtime_s': 3600, 'min_sft_rows': 3, 'max_family_share': 0.6, 'max_jobs_per_day': 1,
                        'approved_by': 'Khalid (chat 2026-09-13)', 'approved_at': '2026-09-13T00:00:00Z'}
        self.put(gear_b.CONTROL_KEY, self.control)
        self.put(own.BASE_PREFIX + BASE_ID + '/manifest.json', {'schema_version': 'factory-base-weights.v1', 'model_id': BASE_ID, 'repo': 'Qwen/Qwen2.5-Coder-7B-Instruct',
                 'revision': 'c03e6d358207e414f1eca0bb1e51b0e1d3a6bd4a', 'license': 'apache-2.0', 'manifest_sha256': 'a' * 64,
                 's3_prefix': 's3://%s/factory/models/base/%s/c03e6d35/' % (PRI, BASE_ID),
                 'files': [{'path': 'config.json', 'sha256': 'b' * 64, 'bytes': 700}, {'path': 'model-00001-of-00004.safetensors', 'sha256': 'c' * 64, 'bytes': 3_900_000_000}], 'total_bytes': 3_900_000_700})
        self.put(own.PIN_KEY, {'schema_version': 'factory-training-pin.v1', 'training_image': '123456789012.dkr.ecr.us-east-1.amazonaws.com/justhodl/factory-train:hf@sha256:' + 'd' * 64,
                 'require_digest': True, 'bundle_uri': 's3://%s/factory/training/bundles/train-1.tar.gz' % PRI, 'bundle_sha256': 'e' * 64, 'program': 'train_qlora.py',
                 'max_steps': 400, 'lora_r': 16, 'learning_rate': '2e-4', 'max_seq_len': 2048, 'epochs': 3, 'default_instance': 'ml.g5.2xlarge', 'instances': ['ml.g5.2xlarge']})
        self.cloud.put_object(Bucket=PRI, Key='factory/exams/code/prompts-only/humaneval-frozen-1/prompts.jsonl', Body=b'{"task_id": "HumanEval/0"}\n')
        self.cloud.put_object(Bucket=PRI, Key='factory/champions/gen-9/output/model.tar.gz', Body=artifact_bytes())
        self.put(gear_b.CANDIDATE_PREFIX + 'gen-9.json', {'schema_version': 'gearb-candidate.v1', 'generation': 9, 'artifact': 's3://%s/factory/champions/gen-9/output/model.tar.gz' % PRI,
                 'job_name': 'jh-gearb-gen9-x', 'trained_at': '2026-09-17T02:00:00Z', 'exam': {'status': 'pending_exam'}})
        gear_b.cg.hourly_price = lambda pricing, s3, bucket, it, family='hosting': {'usd_per_hour': 1.515, 'source': 'test'}

    def put(self, key, obj, bucket=PRI):
        self.cloud.put_object(Bucket=bucket, Key=key, Body=json.dumps(obj))

    def get(self, key, bucket=PRI):
        return json.loads(self.cloud.get_object(Bucket=bucket, Key=key)['Body'].read())

    def examine(self, sm):
        return gear_b.examine_pending(sm, self.cloud, private_bucket=PRI, control=gear_b.load_control(self.cloud, PRI), role_arn='r', pricing=object(), region='us-east-1')

    def test_candidate_becomes_one_exam_job_with_the_adapter_and_never_a_second(self):
        sm = ListingSageMaker()
        out = self.examine(sm)
        self.assertTrue(out['launched']); self.assertTrue(out['adapter_extracted']); self.assertEqual(len(sm.created), 1)
        kw = sm.created[0]
        self.assertTrue(kw['TrainingJobName'].startswith('jh-exam-gen9-'))
        channels = {c['ChannelName']: c['DataSource']['S3DataSource']['S3Uri'] for c in kw['InputDataConfig']}
        self.assertEqual(channels['adapter'], 's3://%s/factory/champions/gen-9/adapter/' % PRI)
        self.assertEqual(channels['tasks'], 's3://%s/factory/exams/code/prompts-only/humaneval-frozen-1/' % PRI)
        self.assertEqual((kw['HyperParameters']['mode'], kw['HyperParameters']['temperature'], kw['HyperParameters']['adapter_generation']), ('exam', '0.0', 'gen-9'))
        self.assertEqual(kw['StoppingCondition']['MaxRuntimeInSeconds'], 3600)
        self.assertEqual(self.get('factory/champions/gen-9/manifest.json')['files'][0]['path'], 'adapter_config.json')
        self.assertEqual(self.get('factory/champions/gen-9/adapter/adapter_config.json'), {'r': 16})
        cand = self.get(gear_b.CANDIDATE_PREFIX + 'gen-9.json')
        self.assertEqual(cand['exam']['status'], 'exam_running'); self.assertEqual(cand['exam']['job_name'], kw['TrainingJobName'])
        rec = self.get(gear_b.EXAM_JOBS_PREFIX + kw['TrainingJobName'] + '.json')
        self.assertEqual((rec['kind'], rec['exam_generation'], rec['launched_by']), ('exam', 'gen-9', 'gear_b.examine_pending'))
        self.assertIsNone(self.examine(sm)); self.assertEqual(len(sm.created), 1)   # exam_running -> nothing pending

    def test_inflight_exam_blocks_a_second_and_the_tick_refuses_training(self):
        sm = ListingSageMaker(inflight=['jh-exam-gen8-earlier'])
        out = self.examine(sm)
        self.assertEqual(out['skipped'], 'an exam is in flight'); self.assertEqual(sm.created, [])
        tick = gear_b.tick(sm, self.cloud, private_bucket=PRI, public_bucket=PUB, policy=self.policy, role_arn='r', projected={}, pricing=object(), describe_card=lambda *a, **k: None)
        self.assertIn('an exam is in flight', tick['refusal']); self.assertIsNone(tick['launched']); self.assertEqual(sm.created, [])

    def test_untrained_manifest_is_refused(self):
        self.cloud.put_object(Bucket=PRI, Key='factory/champions/gen-9/output/model.tar.gz', Body=artifact_bytes(status='refused'))
        with self.assertRaises(gear_b.GearBRefused):
            self.examine(ListingSageMaker())

    def test_decision_promotes_only_a_measured_independent_improvement(self):
        base = {'schema_version': 'factory-exam-result.v1', 'evaluation_id': 'humaneval-frozen-1', 'independent': True, 'held_out': True, 'generation': 'gen-0',
                'n': 164, 'passed': 135, 'score': 0.8232, 'critical_failures': 0, 'missing_completions': 0}
        self.put(gear_b.EXAM_RESULTS_PREFIX + 'base.json', base)
        self.put(gear_b.CANDIDATE_PREFIX + 'gen-9.json', {'schema_version': 'gearb-candidate.v1', 'generation': 9, 'artifact': 'x', 'job_name': 'jh-gearb-gen9-x',
                 'exam': {'status': 'exam_running', 'job_name': 'jh-exam-gen9-a', 'adapter_prefix': 'factory/champions/gen-9/adapter/'}})
        self.assertIsNone(gear_b.decide_pending(self.cloud, PRI))                       # no result yet -> nothing decided
        self.put(gear_b.EXAM_RESULTS_PREFIX + 'gen-9-111.json', dict(base, generation='gen-9', passed=130, score=0.7927, burst='jh-exam-gen9-a', run_id='111'))
        out = gear_b.decide_pending(self.cloud, PRI)
        self.assertEqual(out['decisions'][0]['decision']['reason'], 'no_measured_improvement')
        self.assertEqual(self.get(gear_b.CANDIDATE_PREFIX + 'gen-9.json')['exam']['status'], 'rejected')
        self.assertIsNone(gear_b.get_json(self.cloud, PRI, gear_b.CHAMPION_KEY))
        # a second candidate that beats the base on the same frozen evaluation is promoted
        self.put(gear_b.CANDIDATE_PREFIX + 'gen-10.json', {'schema_version': 'gearb-candidate.v1', 'generation': 10, 'artifact': 'x', 'job_name': 'jh-gearb-gen10-x',
                 'exam': {'status': 'exam_running', 'job_name': 'jh-exam-gen10-b', 'adapter_prefix': 'factory/champions/gen-10/adapter/'}})
        self.put(gear_b.EXAM_RESULTS_PREFIX + 'gen-10-222.json', dict(base, generation='gen-10', passed=140, score=0.8537, burst='jh-exam-gen10-b', run_id='222'))
        out = gear_b.decide_pending(self.cloud, PRI)
        self.assertTrue(out['decisions'][0]['decision']['eligible']); self.assertAlmostEqual(out['decisions'][0]['delta'], 0.0305, places=4)
        champ = self.get(gear_b.CHAMPION_KEY)
        self.assertEqual((champ['generation'], champ['adapter'], champ['release_status']), (10, 'factory/champions/gen-10/adapter/', 'awaiting_owner_release'))
        self.assertEqual(self.get(gear_b.CANDIDATE_PREFIX + 'gen-10.json')['exam']['status'], 'promoted')


if __name__ == '__main__':
    unittest.main()
