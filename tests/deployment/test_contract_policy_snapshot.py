"""Public contract builds must use fresh, immutable, complete access rules."""
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import build_page_data_contracts as compiler
from gen_engine_manifest import build

KEY = 'data/invented-series.json'


def policy(root, private=False, mirror=False):
    path = root / 'aws/shared/private_artifact.py'
    path.parent.mkdir(parents=True, exist_ok=True)
    text = ('# Invented policy; importing it would fail.\n'
            'raise RuntimeError("policy source must remain inert")\n'
            f'MIRRORED_ARTIFACTS = { {KEY: "invented"} if mirror else {}!r}\n'
            'PRIVATE_ARTIFACT_ALIASES = {}\n'
            f'PRIVATE_KEYS = { {KEY} if private else set()!r}\n'
            'PRIVATE_PREFIXES = ()\n')
    path.write_text(text, encoding='utf-8')
    return path


def repository(root):
    source = root / 'aws/lambdas/invented/source'
    source.mkdir(parents=True)
    (source.parent / 'config.json').write_text(json.dumps({'handler': 'lambda_function.lambda_handler'}), encoding='utf-8')
    (source / 'lambda_function.py').write_text(
        f'def lambda_handler(event,context):\n client.get_object(Key={KEY!r})\n client.put_object(Key={KEY!r})\n',
        encoding='utf-8')
    (root / 'engine-manifest.json').write_text(json.dumps(build(root)), encoding='utf-8')
    (root / 'desk.html').write_text(f'<html><head></head><body><script>fetch("/{KEY}")</script></body></html>', encoding='utf-8')
    (root / 'config').mkdir(exist_ok=True)


def test_same_process_reloads_changed_policy_without_manual_cache_clear():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        path = policy(root)
        with patch.object(compiler, 'ROOT', root):
            assert compiler.public_key(KEY)
            policy(root, private=True)
            assert not compiler.public_key(KEY)
            policy(root)
            assert compiler.public_key(KEY)
            # Equal byte count and mtime cannot certify equal source content.
            original = path.read_bytes()
            padded = original + b'#' + b' ' * 100 + b'\n'
            path.write_bytes(padded)
            stamp = path.stat()
            compiler.access_rules()
            changed = original.replace(b'PRIVATE_PREFIXES = ()', b"PRIVATE_PREFIXES = ('data/',)")
            changed += b'#' + b' ' * (100 - (len(changed) - len(original))) + b'\n'
            assert len(changed) == len(padded)
            path.write_bytes(changed)
            os.utime(path, ns=(stamp.st_atime_ns, stamp.st_mtime_ns))
            assert not compiler.public_key(KEY)


def test_access_rules_cannot_be_poisoned_by_mutating_returned_values():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        policy(root, mirror=True)
        with patch.object(compiler, 'ROOT', root):
            mirrors, private, prefixes = compiler.access_rules()
            for mutate in (lambda: mirrors.clear(), lambda: private.clear()):
                try:
                    mutate()
                except (AttributeError, TypeError):
                    pass
                else:
                    raise AssertionError('Cached access rules were mutable')
            assert mirrors[KEY] == 'invented' and KEY in private and isinstance(prefixes, tuple)
            assert not compiler.public_key(KEY)


def test_different_roots_do_not_reuse_another_policy():
    with tempfile.TemporaryDirectory() as directory:
        roots = [Path(directory) / 'public', Path(directory) / 'restricted']
        policy(roots[0]); policy(roots[1], private=True)
        for root, expected in ((roots[0], True), (roots[1], False), (roots[0], True)):
            with patch.object(compiler, 'ROOT', root):
                assert compiler.public_key(KEY) is expected


def test_missing_or_invalid_policy_does_not_fall_back_to_public():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        path = policy(root)
        with patch.object(compiler, 'ROOT', root):
            assert compiler.public_key(KEY)
            for bad in (None, b'MIRRORED_ARTIFACTS = {', b'# missing policy declarations\n'):
                if bad is None:
                    path.unlink()
                else:
                    path.write_bytes(bad)
                try:
                    compiler.public_key(KEY)
                except (OSError, SyntaxError, ValueError):
                    pass
                else:
                    raise AssertionError('Unavailable policy became public access')


def test_unknown_computed_policy_expressions_require_explicit_review():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory); path = policy(root)
        original = path.read_text(encoding='utf-8')
        for old, new in [('PRIVATE_KEYS = set()', 'PRIVATE_KEYS = calculate_keys()'),
                         ('PRIVATE_PREFIXES = ()', 'PRIVATE_PREFIXES = () + calculate_prefixes()'),
                         ('PRIVATE_ARTIFACT_ALIASES = {}', "PRIVATE_ARTIFACT_ALIASES = {'data/alias.json':'missing.json'}"),
                         ('PRIVATE_PREFIXES = ()', 'PRIVATE_PREFIXES = (4,)')]:
            path.write_text(original.replace(old, new), encoding='utf-8')
            with patch.object(compiler, 'ROOT', root):
                try:
                    compiler.public_key(KEY)
                except ValueError:
                    pass
                else:
                    raise AssertionError('Unreviewed access expression was accepted')


def test_archive_policy_cannot_be_computed_before_its_private_keys():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory); path = policy(root, private=True)
        lines = path.read_text(encoding='utf-8').splitlines()
        key_line = next(line for line in lines if line.startswith('PRIVATE_KEYS'))
        lines.remove(key_line)
        lines[-1] = "PRIVATE_PREFIXES = () + tuple('history/archive/feed/'+key+'/' for key in sorted(set(PRIVATE_KEYS)|{value.removeprefix('data/') for value in PRIVATE_KEYS}))"
        lines.append(key_line)
        path.write_text('\n'.join(lines)+'\n', encoding='utf-8')
        with patch.object(compiler, 'ROOT', root):
            try:
                compiler.access_rules()
            except ValueError as error:
                assert 'precedes its dependencies' in str(error)
            else:
                raise AssertionError('Archive privacy was computed from an incomplete key set')


def test_repeated_whole_contract_updates_outputs_and_dependency_visibility():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        policy(root); repository(root)
        with patch.object(compiler, 'ROOT', root):
            public = compiler.contract(root)['engines']['invented']
            assert public['outputs'][0]['access'] == 'public'
            assert public['dependency_groups'][0]['public_references'][0]['key'] == KEY
            policy(root, private=True)
            restricted = compiler.contract(root)['engines']['invented']
            assert restricted['outputs'] == [] and restricted['restricted_count'] == 1
            assert restricted['dependency_groups'][0]['public_references'] == []
            assert restricted['dependency_groups'][0]['withheld_or_dynamic_count'] == 1
            policy(root, mirror=True)
            authenticated = compiler.contract(root)['engines']['invented']
            assert authenticated['outputs'][0]['access'] == 'owner_authenticated'
            assert authenticated['dependency_groups'][0]['public_references'] == []


def test_policy_drift_during_build_refuses_before_artifact_writes():
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        policy(root); repository(root)
        destination = root / 'config/page-data-contracts.json'
        destination.write_bytes(b'whole previous generated artifact\n')
        site = root / 'site'
        site.mkdir(); page = site / 'desk.html'; page.write_bytes(b'whole previous built page\n')
        original_scan = compiler.scan_pages

        def changing_scan(repo):
            result = original_scan(repo)
            policy(root, private=True)
            return result

        with patch.object(compiler, 'ROOT', root), patch.object(compiler, 'scan_pages', changing_scan), patch.object(sys, 'argv', ['build_page_data_contracts.py', '--site', str(site)]):
            try:
                compiler.main()
            except ValueError as error:
                assert 'policy changed during' in str(error)
            else:
                raise AssertionError('Mixed-policy compilation was accepted')
        assert destination.read_bytes() == b'whole previous generated artifact\n'
        assert page.read_bytes() == b'whole previous built page\n'
