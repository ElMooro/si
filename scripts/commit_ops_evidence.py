"""Commit staged ops changes while preserving concurrent main-branch edits.

Only STATE.md and engine-manifest.json may be regenerated after a conflict.
Any conflict in product code, configuration or evidence requires review.
"""
import argparse
import os
from pathlib import Path
import subprocess

GENERATED = {'STATE.md', 'engine-manifest.json'}


def git(root, *args, check=True):
    result = subprocess.run(['git', *args], cwd=root, text=True, capture_output=True,
                            env={**os.environ, 'GIT_EDITOR':'true'})
    if check and result.returncode:
        raise RuntimeError('git_operation_failed_' + args[0])
    return result


def reconcile(root, *, regenerate=True):
    result = git(root, 'rebase', 'origin/main', check=False)
    generated_conflict = False
    while result.returncode:
        conflicts = set(git(root, 'diff', '--name-only', '--diff-filter=U').stdout.splitlines())
        if not conflicts or not conflicts <= GENERATED:
            git(root, 'rebase', '--abort', check=False)
            raise RuntimeError('concurrent_ops_change_requires_review')
        # In a rebase, ours is the new upstream. Discard only stale generated
        # snapshots; regenerate from the successfully reconciled source below.
        git(root, 'checkout', '--ours', '--', *sorted(conflicts))
        git(root, 'add', '--', *sorted(conflicts))
        generated_conflict = True
        result = git(root, 'rebase', '--continue', check=False)
    if generated_conflict and regenerate:
        for script in ('scripts/gen_engine_manifest.py', 'aws/ops/_gen_state.py'):
            subprocess.run(['python3', script], cwd=root, check=True,
                           stdout=subprocess.DEVNULL)
        git(root, 'add', '--', *sorted(GENERATED))
        if git(root, 'diff', '--cached', '--quiet', check=False).returncode:
            git(root, 'commit', '-m', 'ops: regenerate reconciled state metadata [skip-deploy] [skip-ops]')
    return generated_conflict


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--run-id', type=int, required=True)
    parser.add_argument('--job-status', choices=('success','failure','cancelled'), required=True)
    args = parser.parse_args(); root = Path.cwd()
    if not git(root, 'diff', '--cached', '--quiet', check=False).returncode:
        print('No staged ops changes'); return
    git(root, 'commit', '-m', f'ops: auto-commit from run {args.run_id} ({args.job_status}) [skip-deploy] [skip-ops]')
    for attempt in range(3):
        git(root, 'fetch', 'origin', 'main')
        reconcile(root)
        if not git(root, 'push', 'origin', 'HEAD:refs/heads/main', check=False).returncode:
            print('Ops evidence committed to main'); return
    raise RuntimeError('ops_evidence_push_failed_after_three_attempts')


if __name__ == '__main__':
    main()
