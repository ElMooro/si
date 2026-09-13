"""Gear B (capped weight training), the frozen holdout + anonymized drills, and the isolated verifier."""
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws/shared'))
sys.path.insert(0, str(ROOT / 'aws/lambdas/justhodl-ai/source'))
sys.path.insert(0, str(ROOT / 'scripts'))
sys.path.insert(0, str(ROOT / 'tests/factory'))

from test_factory import MemoryS3  # noqa: E402
from factory_core import make_season  # noqa: E402
import gear_b  # noqa: E402


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, str(ROOT / path))
    loaded = importlib.util.module_from_spec(spec); spec.loader.exec_module(loaded)
    return loaded


holdout = module('factory_test_holdout', 'scripts/factory_holdout.py')
PRI, PUB = 'private', 'public'


class FakeSageMaker:
    def __init__(self, statuses=None):
        self.created, self.statuses = [], statuses or {}
    def create_training_job(self, **kw):
        self.created.append(kw); return {'TrainingJobArn': 'arn:fake:' + kw['TrainingJobName']}
    def describe_training_job(self, TrainingJobName):
        st = self.statuses.get(TrainingJobName, 'InProgress')
        d = {'TrainingJobStatus': st}
        if st == 'Completed':
            d.update(BillableTimeInSeconds=1800, TrainingTimeInSeconds=2400, ModelArtifacts={'S3ModelArtifacts': 's3://private/factory/champions/gen-1/output/model.tar.gz'})
        return d


class Base(unittest.TestCase):
    def setUp(self):
        self.cloud = MemoryS3()
        self.policy = dict(gear_b.cg.DEFAULT_POLICY, daily_budget_usd=20.0, training_max_runtime_s=3600)
        self.control = {'schema_version': 'gearb-control.v1', 'enabled': True, 'model_id': 'huggingface-llm-qwen2-5-coder-7b-instruct',
                        'instance_type': 'ml.g5.2xlarge', 'exam_instance_type': 'ml.g5.2xlarge', 'daily_budget_usd': 20.0, 'season_cap_usd': 600.0,
                        'max_runtime_s': 3 * 3600, 'min_sft_rows': 3, 'max_family_share': 0.6, 'max_jobs_per_day': 1,
                        'approved_by': 'Khalid (chat 2026-09-13)', 'approved_at': '2026-09-13T00:00:00Z'}
        self.holdout = {'schema_version': 'factory-holdout.v1', 'frozen_at': '2026-09-13T20:00:00Z',
                        'code': {'task_ids': ['HumanEval/0'], 'source_shas': ['deadbeef']}}
        self.spec = {'model_id': 'huggingface-llm-qwen2-5-coder-7b-instruct', 'training_supported': True, 'training_image': '123.dkr.ecr/x:y',
                     'training_script': 's3://jumpstart/train.tar.gz', 'training_artifact': 's3://jumpstart/model/', 'hyperparameters': {'epoch': {'default': 3}}}
        self.pricing = object()
        self._price = {'usd_per_hour': 1.515, 'source': 'test'}
        gear_b.cg.hourly_price = lambda pricing, s3, bucket, it, family='hosting': dict(self._price)
    def put(self, bucket, key, obj):
        self.cloud.put_object(Bucket=bucket, Key=key, Body=json.dumps(obj))
    def get(self, bucket, key):
        return json.loads(self.cloud.get_object(Bucket=bucket, Key=key)['Body'].read())
    def verified(self, i, family='mbpp', license='CC-BY-4.0', task_id=None, passed=True):
        return {'verified_by': 'owner_runner', 'passed': passed, 'kind': 'public_benchmark_train', 'family': family, 'license': license,
                'source_url': 'https://example.org/%d' % i, 'task_id': task_id or 'mbpp-%d' % i, 'source_sha': 'sha%d' % i,
                'prompt': 'Write a function %d' % i, 'solution': 'def f%d():\n    return %d\n' % (i, i)}


class ControlTests(Base):
    def test_absent_control_is_off_and_never_creates(self):
        sm = FakeSageMaker()
        out = gear_b.tick(sm, self.cloud, private_bucket=PRI, public_bucket=PUB, policy=self.policy, role_arn='r', projected={}, pricing=self.pricing,
                          describe_card=lambda m, v: self.spec)
        self.assertIn('absent', out['refusal']); self.assertEqual(sm.created, [])
        self.assertEqual(gear_b.public_status(self.cloud, PRI, self.policy)['status'], 'off')
    def test_control_above_policy_budget_refused(self):
        self.put(PRI, gear_b.CONTROL_KEY, dict(self.control, daily_budget_usd=25.0))
        self.assertIn('exceeds cost-guard', gear_b.validate_control(gear_b.load_control(self.cloud, PRI), self.policy))
    def test_control_needs_owner_approval_and_allowed_instance(self):
        self.put(PRI, gear_b.CONTROL_KEY, dict(self.control, approved_by=None))
        self.assertIn('approval', gear_b.validate_control(gear_b.load_control(self.cloud, PRI), self.policy))
        self.put(PRI, gear_b.CONTROL_KEY, dict(self.control, instance_type='ml.p4d.24xlarge'))
        self.assertIn('large GPU', gear_b.validate_control(gear_b.load_control(self.cloud, PRI), self.policy))


class CurateTests(Base):
    def test_curation_rules(self):
        rows = [gear_b._row_from_verified('k', self.verified(i)) for i in range(4)]
        rows.append(gear_b._row_from_verified('k', self.verified(9, license='Proprietary')))
        rows.append(gear_b._row_from_verified('k', self.verified(10, task_id='HumanEval/0')))
        rows.append(dict(rows[0]))  # exact duplicate
        rows.append(dict(rows[1], source='factory/traces/_reject/x.json'))
        rows.append(gear_b._row_from_verified('k', self.verified(11, family='apps')))
        cur = gear_b.curate([r for r in rows if r], self.holdout, dict(self.control, max_family_share=0.6))
        self.assertEqual(cur['dropped']['license'], 1); self.assertEqual(cur['dropped']['holdout'], 1)
        self.assertEqual(cur['dropped']['duplicate'], 1); self.assertEqual(cur['dropped']['reject_prefix'], 1)
        self.assertGreaterEqual(cur['dropped']['diversity'], 1)
        self.assertTrue(all(r['license'] in gear_b.ALLOWED_LICENSES for r in cur['rows']))
        self.assertEqual(cur['holdout_digest'], gear_b.digest(self.holdout))
    def test_unverified_or_failed_rows_never_map(self):
        self.assertIsNone(gear_b._row_from_verified('k', self.verified(1, passed=False)))
        self.assertIsNone(gear_b._row_from_verified('k', dict(self.verified(1), verified_by='student')))
        self.assertIsNone(gear_b._row_from_gear_a_trace('k', {'ok': False, 'prompt': 'p', 'candidate': 'c'}))
        self.assertEqual(gear_b._row_from_gear_a_trace('k', {'ok': True, 'prompt': 'p', 'candidate': 'c', 'family': 'lambda-identity'})['license'], 'own')


class DatasetAndLaunchTests(Base):
    def seed(self, n=3):
        self.put(PRI, gear_b.CONTROL_KEY, self.control); self.put(PRI, gear_b.HOLDOUT_KEY, self.holdout)
        for i in range(n):
            self.put(PRI, gear_b.CURRICULUM_VERIFIED_PREFIX + 'row%d.json' % i, self.verified(i, family='mbpp' if i % 2 else 'apps'))
    def test_no_holdout_refuses_before_any_write(self):
        self.put(PRI, gear_b.CONTROL_KEY, self.control)
        with self.assertRaises(gear_b.GearBRefused):
            gear_b.build_dataset(self.cloud, PRI, PUB, gear_b.load_control(self.cloud, PRI))
        self.assertFalse([k for b, k in self.cloud.rows if k.startswith(gear_b.DATASET_PREFIX)])
    def test_floor_not_met_writes_nothing_and_says_how_many(self):
        self.seed(2)
        st = gear_b.build_dataset(self.cloud, PRI, PUB, gear_b.load_control(self.cloud, PRI))
        self.assertFalse(st['ok']); self.assertEqual(st['missing_rows'], 1)
        self.assertFalse([k for b, k in self.cloud.rows if k.startswith(gear_b.DATASET_PREFIX)])
    def test_dataset_written_once_per_generation_with_digests(self):
        self.seed(3)
        m = gear_b.build_dataset(self.cloud, PRI, PUB, gear_b.load_control(self.cloud, PRI))
        self.assertTrue(m['ok']); self.assertEqual(m['generation'], 1); self.assertEqual(m['kept'], 3)
        train = self.cloud.get_object(Bucket=PRI, Key=m['prefix'] + 'train.jsonl')['Body'].read()
        self.assertEqual(gear_b.sha256_bytes(train), m['train_sha256'])
        self.assertEqual(len(train.decode().strip().splitlines()), 3)
        self.assertIn('instruction', json.loads(train.decode().splitlines()[0]))
        self.assertEqual(m['holdout_digest'], gear_b.digest(self.holdout))
        m2 = gear_b.build_dataset(self.cloud, PRI, PUB, gear_b.load_control(self.cloud, PRI))
        self.assertEqual(m2['generation'], 2)
    def test_launch_refuses_unpriced_then_launches_once_inside_caps(self):
        self.seed(3)
        m = gear_b.build_dataset(self.cloud, PRI, PUB, gear_b.load_control(self.cloud, PRI))
        control = gear_b.load_control(self.cloud, PRI); sm = FakeSageMaker()
        self._price = {'usd_per_hour': None, 'source': 'none'}
        with self.assertRaises(gear_b.GearBRefused):
            gear_b.launch_sft(sm, self.cloud, spec=self.spec, role_arn='r', private_bucket=PRI, control=control, policy=self.policy, manifest=m, projected={}, pricing=self.pricing, region='us-east-1')
        self.assertEqual(sm.created, [])
        self._price = {'usd_per_hour': 1.515, 'source': 'test'}
        rec = gear_b.launch_sft(sm, self.cloud, spec=self.spec, role_arn='r', private_bucket=PRI, control=control, policy=self.policy, manifest=m, projected={}, pricing=self.pricing, region='us-east-1')
        self.assertEqual(len(sm.created), 1); kw = sm.created[0]
        self.assertTrue(kw['EnableManagedSpotTraining']); self.assertEqual(kw['StoppingCondition']['MaxRuntimeInSeconds'], 3 * 3600)
        self.assertEqual(kw['Environment']['JH_GEARB_ELIGIBILITY_DIGEST'], m['eligibility_digest'])
        self.assertEqual(kw['HyperParameters']['peft_type'], 'lora'); self.assertEqual(kw['HyperParameters']['epoch'], '1')
        self.assertIn({'Key': 'jh-factory', 'Value': 'gearb-gen-1'}, kw['Tags'])
        self.assertAlmostEqual(rec['cap_usd'], 1.515 * 3, places=3)
        self.assertEqual(self.get(PRI, gear_b.JOBS_PREFIX + rec['job_name'] + '.json')['status'], 'launching')
        with self.assertRaises(gear_b.GearBRefused) as ctx:   # second launch the same day
            gear_b.launch_sft(sm, self.cloud, spec=self.spec, role_arn='r', private_bucket=PRI, control=control, policy=self.policy, manifest=m, projected={}, pricing=self.pricing, region='us-east-1')
        self.assertIn('max_jobs_per_day', str(ctx.exception)); self.assertEqual(len(sm.created), 1)
    def test_budget_caps_refuse_before_create(self):
        self.seed(3)
        m = gear_b.build_dataset(self.cloud, PRI, PUB, gear_b.load_control(self.cloud, PRI))
        self.put(PRI, gear_b.CONTROL_KEY, dict(self.control, daily_budget_usd=4.0))
        control = gear_b.load_control(self.cloud, PRI); sm = FakeSageMaker()
        with self.assertRaises(gear_b.GearBRefused) as ctx:
            gear_b.launch_sft(sm, self.cloud, spec=self.spec, role_arn='r', private_bucket=PRI, control=control, policy=self.policy, manifest=m, projected={}, pricing=self.pricing, region='us-east-1')
        self.assertIn('daily budget', str(ctx.exception)); self.assertEqual(sm.created, [])
        with self.assertRaises(gear_b.GearBRefused):
            gear_b.launch_sft(sm, self.cloud, spec=dict(self.spec, training_supported=False), role_arn='r', private_bucket=PRI, control=gear_b.load_control(self.cloud, PRI), policy=self.policy, manifest=m, projected={}, pricing=self.pricing, region='us-east-1')
    def test_tick_reuses_unlaunched_dataset_polls_and_records_candidate(self):
        self.seed(3); sm = FakeSageMaker()
        out = gear_b.tick(sm, self.cloud, private_bucket=PRI, public_bucket=PUB, policy=self.policy, role_arn='r', projected={}, pricing=self.pricing, describe_card=lambda m, v: self.spec)
        self.assertEqual(out['built']['generation'], 1); self.assertIsNotNone(out['launched']); job = out['launched']['job_name']
        out2 = gear_b.tick(sm, self.cloud, private_bucket=PRI, public_bucket=PUB, policy=self.policy, role_arn='r', projected={}, pricing=self.pricing, describe_card=lambda m, v: self.spec)
        self.assertEqual(out2['refusal'], 'a job is still running'); self.assertEqual(len(sm.created), 1)
        sm.statuses[job] = 'Completed'
        updates = gear_b.poll_jobs(sm, self.cloud, PRI)
        self.assertEqual(updates[0]['status'], 'Completed')
        cand = self.get(PRI, gear_b.CANDIDATE_PREFIX + 'gen-1.json')
        self.assertEqual(cand['exam']['status'], 'pending_exam'); self.assertTrue(cand['artifact'].endswith('model.tar.gz'))
        self.assertEqual(self.get(PRI, gear_b.JOBS_PREFIX + job + '.json')['billed_usd_estimate'], round(1.515 * 0.5, 4))
        self.assertEqual(len([k for b, k in self.cloud.rows if k.startswith(gear_b.DATASET_PREFIX) and k.endswith('manifest.json')]), 1)
    def test_public_status_has_money_and_counts_but_no_identifiers(self):
        self.seed(3); sm = FakeSageMaker()
        gear_b.tick(sm, self.cloud, private_bucket=PRI, public_bucket=PUB, policy=self.policy, role_arn='r', projected={}, pricing=self.pricing, describe_card=lambda m, v: self.spec)
        st = gear_b.public_status(self.cloud, PRI, self.policy)
        self.assertEqual(st['status'], 'armed'); self.assertTrue(st['holdout']['frozen'])
        self.assertEqual(st['jobs']['total'], 1); self.assertGreater(st['budget']['committed_today_usd'], 0)
        blob = json.dumps(st)
        self.assertNotIn('jh-gearb-gen', blob); self.assertNotIn('arn:', blob); self.assertNotIn('Write a function', blob)
    def test_promotion_needs_baseline_and_shared_contract(self):
        self.assertFalse(gear_b.promotion({'score': 0.9}, None)['eligible'])
        base = {'evaluation_id': 'humaneval-v1', 'independent': True, 'held_out': True, 'n': 164, 'score': 0.61, 'critical_failures': 0}
        cand = dict(base, score=0.66)
        self.assertTrue(gear_b.promotion(cand, base)['eligible'])
        self.assertFalse(gear_b.promotion(dict(cand, score=0.61), base)['eligible'])


class HoldoutTests(unittest.TestCase):
    def setUp(self):
        self.season = make_season(datetime(2026, 9, 13, 15, tzinfo=timezone.utc))
    def rows(self, n, start=100.0, drift=0.004):
        out, px = [], start
        for i in range(n):
            o = px; c = px * (1 + drift); px = c
            out.append(('2022-01-%02d' % (3 + i), {'o': o, 'c': c, 'h': max(o, c) * 1.001, 'l': min(o, c) * 0.999, 'v': 1000.0 + i}))
        return out
    def test_drill_is_anonymous_and_labeled_from_future_bars(self):
        rows = self.rows(25)
        drill = holdout.anonymize(rows[:20], rows[20:25], 'SPY', self.season)
        blob = json.dumps(drill)
        self.assertNotIn('2022', blob); self.assertNotIn('SPY', blob)
        self.assertEqual(drill['bars'][0]['o'], 100.0); self.assertEqual(len(drill['bars']), 20)
        self.assertEqual(drill['labels']['direction'], 'UP'); self.assertEqual(drill['labels']['regime'], 'TREND'); self.assertFalse(drill['labels']['crisis'])
    def test_train_window_overlapping_holdout_is_a_leak(self):
        self.assertTrue(holdout.overlaps('2020-03-02', '2020-03-27', holdout.HOLDOUT_BLOCKS))
        self.assertFalse(holdout.overlaps('2022-06-01', '2022-06-30', holdout.HOLDOUT_BLOCKS))
        for block in holdout.TRAIN_BLOCKS:
            self.assertFalse(holdout.overlaps(block['start'], block['end'], holdout.HOLDOUT_BLOCKS), block['id'])
    def test_freeze_refuses_to_overwrite_and_records_code_holdout(self):
        class WH:
            def __init__(self): self.rows = {}; self.private = 'private'; self.public = 'public'
            def get(self, bucket, key):
                raw = self.rows.get((bucket, key)); return (raw, holdout.sha(raw)) if raw else (None, None)
            def put_if_absent(self, bucket, key, body):
                if (bucket, key) in self.rows: return 'exists'
                self.rows[(bucket, key)] = body; return 'written'
        wh = WH()
        holdout.build_block = lambda wh_, block, split, season, step=5: ([], [])   # no warehouse in the unit test
        out = holdout.freeze(wh, season=self.season, dry_run=False, git_sha='abc', code_holdout={'task_ids': ['HumanEval/1', 'HumanEval/0'], 'source_shas': ['s1'], 'license': 'MIT'})
        self.assertEqual(out['manifest_write'], 'written'); self.assertEqual(out['manifest']['code']['task_ids'], ['HumanEval/0', 'HumanEval/1'])
        self.assertIsNotNone(out['manifest']['frozen_at'])
        again = holdout.freeze(wh, season=self.season, dry_run=False, git_sha='abc', code_holdout={'task_ids': []})
        self.assertEqual(again['status'], 'exists'); self.assertEqual(again['manifest']['code']['task_ids'], ['HumanEval/0', 'HumanEval/1'])


class VerifierTests(unittest.TestCase):
    def test_only_passes_are_emitted(self):
        rows = [{'task_id': 'ok', 'solution': 'def add(a, b):\n    return a + b\n', 'tests': 'assert add(1, 2) == 3'},
                {'task_id': 'bad', 'solution': 'def add(a, b):\n    return a - b\n', 'tests': 'assert add(1, 2) == 3'},
                {'task_id': 'hang', 'solution': 'import time\nwhile True:\n    time.sleep(1)\n', 'tests': 'assert True', 'timeout_s': 1},
                {'task_id': 'malformed'}]
        with tempfile.TemporaryDirectory() as tmp:
            src, out = os.path.join(tmp, 'c.jsonl'), os.path.join(tmp, 'v.jsonl')
            Path(src).write_text('\n'.join(json.dumps(r) for r in rows) + '\n')
            proc = subprocess.run([sys.executable, str(ROOT / 'scripts/factory_code_verify.py'), src, out], capture_output=True, text=True, timeout=60)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            lines = [json.loads(l) for l in Path(out).read_text().splitlines()]
            self.assertEqual([l['task_id'] for l in lines if 'task_id' in l], ['ok'])
            self.assertEqual(lines[-1]['_report'], {'seen': 4, 'passed': 1, 'failed': 2, 'timeouts': 1, 'malformed': 1})


if __name__ == '__main__':
    unittest.main(verbosity=2)
