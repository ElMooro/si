"""Release checkout pinning against real local Git history; no AWS/network."""
import runpy
import subprocess
import tempfile
from contextlib import contextmanager
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def pin(*args, **kwargs):
    return runpy.run_path(str(ROOT / 'scripts/pin_release_checkout.py'))['pin_checkout'](*args, **kwargs)


def git(repo, *args):
    return subprocess.check_output(['git', *args], cwd=repo, text=True, stderr=subprocess.DEVNULL).strip()


def commit(repo, name, *, workflow=None):
    (repo / 'source.txt').write_text(name)
    if workflow is not None:
        path = repo / '.github/workflows/deploy-lambdas.yml'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(workflow)
    git(repo, 'add', '.')
    git(repo, 'commit', '-m', name)
    return git(repo, 'rev-parse', 'HEAD')


@contextmanager
def history(changed_workflow=False):
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        remote = root / 'origin.git'
        remote.mkdir()
        git(remote, 'init', '--bare')
        repo = root / 'repo'
        repo.mkdir()
        git(repo, 'init')
        git(repo, 'config', 'user.name', 'Offline fixture')
        git(repo, 'config', 'user.email', 'fixture@example.invalid')
        git(repo, 'remote', 'add', 'origin', str(remote))
        base = commit(repo, 'base', workflow='name: Release\non: push\njobs: {}\n')
        expected = commit(repo, 'expected-release')
        triggering = commit(repo, 'observer-advance', workflow='name: Different release\non: push\njobs: {}\n' if changed_workflow else None)
        git(repo, 'push', 'origin', 'HEAD:refs/heads/main')
        yield repo, base, expected, triggering


def rejects_without_checkout(repo, expected, triggering, **kwargs):
    before = git(repo, 'rev-parse', 'HEAD')
    try:
        pin(repo, expected, triggering, **kwargs)
    except (ValueError, RuntimeError, subprocess.CalledProcessError):
        pass
    else:
        raise AssertionError('Unsafe release checkout accepted')
    assert git(repo, 'rev-parse', 'HEAD') == before


def test_observer_advance_with_identical_workflow_pins_exact_earlier_release():
    with history() as (repo, base, expected, triggering):
        result = pin(repo, expected, triggering, base=base)
        assert result['release_sha'] == expected
        assert result['triggering_sha'] == triggering
        assert result['workflow_definition_equal'] is True and result['exact_checkout'] is True
        assert git(repo, 'rev-parse', 'HEAD') == expected
        assert (repo / 'source.txt').read_text() == 'expected-release'
        assert git(repo, 'rev-parse', '--abbrev-ref', 'HEAD') == 'HEAD'


def test_changed_workflow_rejects_pin_and_preserves_triggering_checkout():
    with history(changed_workflow=True) as (repo, base, expected, triggering):
        rejects_without_checkout(repo, expected, triggering, base=base)


def test_divergent_release_is_not_accepted_as_triggering_ancestor():
    with history() as (repo, base, expected, triggering):
        git(repo, 'checkout', '--detach', base)
        divergent = commit(repo, 'divergent-source')
        git(repo, 'push', 'origin', 'HEAD:refs/heads/divergent')
        git(repo, 'checkout', '--detach', triggering)
        rejects_without_checkout(repo, divergent, triggering, base=base)


def test_unrelated_base_cannot_authorize_expected_release():
    with history() as (repo, base, expected, triggering):
        git(repo, 'checkout', '--detach', base)
        divergent = commit(repo, 'divergent-base')
        git(repo, 'push', 'origin', 'HEAD:refs/heads/divergent')
        git(repo, 'checkout', '--detach', triggering)
        rejects_without_checkout(repo, expected, triggering, base=divergent)


def test_invalid_commit_identifiers_reject_without_checkout():
    with history() as (repo, base, expected, triggering):
        for invalid in ('main', expected[:7], '--detach', 'x'*40, expected+';echo unsafe'):
            rejects_without_checkout(repo, invalid, triggering, base=base)
            rejects_without_checkout(repo, expected, invalid, base=base)
            rejects_without_checkout(repo, expected, triggering, base=invalid)


def test_current_checkout_must_equal_triggering_commit():
    with history() as (repo, base, expected, triggering):
        git(repo, 'checkout', '--detach', expected)
        rejects_without_checkout(repo, expected, triggering, base=base)


def test_unexpected_workflow_event_cannot_pin_checkout():
    with history() as (repo, base, expected, triggering):
        rejects_without_checkout(repo, expected, triggering, event_name='pull_request', base=base)


def test_no_expected_release_reports_current_trigger_without_pinning():
    with history() as (repo, base, expected, triggering):
        branch = git(repo, 'rev-parse', '--abbrev-ref', 'HEAD')
        result = pin(repo, '', triggering)
        assert result['release_sha'] == triggering and result['triggering_sha'] == triggering
        assert result['exact_checkout'] is True
        assert git(repo, 'rev-parse', 'HEAD') == triggering
        assert git(repo, 'rev-parse', '--abbrev-ref', 'HEAD') == branch
