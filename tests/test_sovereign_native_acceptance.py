"""Delayed acceptance keeps the intended release identity in shallow checkouts."""
from copy import deepcopy
from pathlib import Path
from unittest.mock import patch
import json
import hashlib
import os
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'aws/ops/staged'))
import ops_6209_sovereign_native_acceptance as op


class Tests(unittest.TestCase):
    def setUp(self):
        self.baseline = json.loads(op.BASELINE.read_text(encoding='utf-8'))
        self.actual = deepcopy(self.baseline['sovereign_code_acceptance']['actual_runtime'])

    def test_real_shallow_checkout_misidentifies_unchanged_file_as_boundary_commit(self):
        parent = Path(os.environ.get('TEMP', tempfile.gettempdir())).resolve()
        with tempfile.TemporaryDirectory(prefix='sovereign-history-test-', dir=parent) as tmp:
            root = Path(tmp).resolve()
            self.assertTrue(root.is_relative_to(parent))
            original, clone = root/'original', root/'shallow'
            original.mkdir()
            def git(*args, cwd=original):
                return subprocess.check_output(['git', *args], cwd=cwd, stderr=subprocess.DEVNULL, text=True).strip()
            git('init', '--quiet', '--initial-branch=main')
            git('config', 'user.name', 'Fixture'); git('config', 'user.email', 'fixture@example.invalid')
            (original/'engine.py').write_text('source = 1\n', encoding='utf-8')
            git('add', '.'); git('commit', '--quiet', '-m', 'engine')
            intended = git('rev-parse', 'HEAD')
            for i in range(2):
                (original/'note.txt').write_text(str(i), encoding='utf-8')
                git('add', '.'); git('commit', '--quiet', '-m', 'unrelated')
            git('clone', '--quiet', '--depth=2', '--no-checkout', original.as_uri(), str(clone), cwd=root)
            wrong = git('log', '-1', '--format=%H', '--', 'engine.py', cwd=clone)
            self.assertNotEqual(wrong, intended)
            self.assertEqual(git('rev-parse', '--is-shallow-repository', cwd=clone), 'true')
            self.assertEqual(op.verify_runtime(self.actual), self.baseline['commit'])
            complete = root/'complete'
            git('clone', '--quiet', '--filter=blob:none', original.as_uri(), str(complete), cwd=root)
            self.assertEqual(git('rev-parse', '--is-shallow-repository', cwd=complete), 'false')
            self.assertEqual(git('log', '-1', '--format=%H', '--', 'engine.py', cwd=complete), intended)
            self.assertEqual((complete/'engine.py').read_text(encoding='utf-8'), 'source = 1\n')

    def test_actual_workflow_changes_only_history_retrieval_preserving_authority_and_time_limits(self):
        original = (ROOT/'tests/fixtures/pre-full-history-run-ops-direct.yml.txt').read_bytes()
        self.assertEqual(len(original), 3035)
        self.assertEqual(hashlib.sha256(original).hexdigest(), '7f752d6b318c86c5c803c700eea1bbe6a9c679636bb2b9f6e4c7a89d257b239c')
        current = (ROOT/'.github/workflows/run-ops-direct.yml').read_text(encoding='utf-8')
        restored = current.replace('          # Delayed release checks need true path history, not a shallow boundary.\n'
                                  '          # Fetch old blobs on demand; the current checkout remains complete.\n'
                                  '          fetch-depth: 0\n          filter: blob:none', '          fetch-depth: 50')
        self.assertEqual(restored, original.decode('utf-8'))

    def test_accepted_package_works_without_unavailable_old_git_history(self):
        with patch.object(op.subprocess, 'check_output', side_effect=AssertionError('Shallow history is not release evidence')):
            self.assertEqual(op.verify_runtime(self.actual), '657cc27c4c95daf5372c1ba3c2fe7c9e408b7ddd')

    def test_foreign_receipt_or_code_is_not_accepted(self):
        for change in ('receipt', 'package', 'sources'):
            value = deepcopy(self.actual)
            if change == 'receipt': value['receipt']['commit'] = 'a'*40
            if change == 'package': value['code_sha256'] = 'another package'
            if change == 'sources': value['source_files_checked'] = 2
            with self.assertRaises(ValueError): op.verify_runtime(value)

    def test_runtime_schedule_or_alias_drift_cannot_pass(self):
        for key, value in (('memory_mb', 1024), ('timeout', 900), ('architectures', ['arm64']),
                           ('schedules', []), ('ephemeral_storage_mb', 1024), ('active_alias', {'version': '2'})):
            changed = deepcopy(self.actual); changed[key] = value
            with self.assertRaises(ValueError): op.verify_runtime(changed)

    def test_foreign_baseline_identity_cannot_self_authorize_a_new_release(self):
        changed = deepcopy(self.baseline); changed['commit'] = 'a'*40
        with patch.object(Path, 'read_text', return_value=json.dumps(changed)):
            with self.assertRaises(ValueError): op.verify_runtime(self.actual)


if __name__ == '__main__': unittest.main(verbosity=2)
