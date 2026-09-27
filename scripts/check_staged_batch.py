#!/usr/bin/env python3
"""Check that a reviewed file inventory is fully staged before commit.

  python3 scripts/check_staged_batch.py --inventory /tmp/expected-files.json

The inventory is a JSON list of repository-relative POSIX paths. Run AFTER every
pull/rebase and immediately before commit. This neither stages nor commits files.
It detects autostash restoring tracked edits to the worktree but not the index.
It may create a Git tree object, but never changes the index, worktree or refs.
The returned Git tree identifies the checked staged bytes; it is not deploy proof.
"""
from pathlib import Path, PurePosixPath
import argparse
import json
import subprocess
import sys


class BatchError(ValueError):
    pass


def git(root, *args):
    result=subprocess.run(['git',*args],cwd=root,stdout=subprocess.PIPE,stderr=subprocess.PIPE,check=False)
    if result.returncode:raise BatchError('Git check failed: '+args[0])
    return result.stdout


def paths(raw):
    return {part.decode('utf-8') for part in raw.split(b'\0') if part}


def check(root, inventory):
    if not isinstance(inventory,list) or not inventory or any(not isinstance(p,str) for p in inventory):
        raise BatchError('Nonempty explicit file inventory required')
    for name in inventory:
        path=PurePosixPath(name)
        if not name or ':' in name or '\\' in name or '\x00' in name or path.is_absolute() or '..' in path.parts or '.' in path.parts or str(path)!=name:
            raise BatchError('Canonical repository-relative paths required')
    if len(set(inventory))!=len(inventory):raise BatchError('Duplicate inventory path')
    expected=set(inventory)
    if git(root,'ls-files','--unmerged','-z'):raise BatchError('Unresolved index conflicts')
    staged=paths(git(root,'diff','--cached','--no-renames','--name-only','-z'))
    unstaged=paths(git(root,'diff','--no-renames','--name-only','-z'))
    missing=sorted(expected-staged);extra=sorted(staged-expected)
    if missing or extra or unstaged:
        raise BatchError(json.dumps({'missing_from_index':missing,'unexpected_staged':extra,
                                     'unstaged_tracked':sorted(unstaged)},sort_keys=True))
    # Expected additions must be staged; unrelated untracked files are not added.
    # Git's tree records every staged byte, including files well above connector limits.
    return {'status':'complete_staged_inventory','files':sorted(staged),
            'head':git(root,'rev-parse','HEAD').decode().strip(),
            'staged_tree':git(root,'write-tree').decode().strip(),
            'deployment_verified':False}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory',required=True,type=Path)
    parser.add_argument('--root',type=Path,default=Path(__file__).resolve().parents[1])
    args=parser.parse_args()
    print(json.dumps(check(args.root,json.loads(args.inventory.read_text(encoding='utf-8'))),sort_keys=True))


if __name__=='__main__':
    try:main()
    except (BatchError,OSError,ValueError,UnicodeError) as exc:
        print('Staged batch rejected: '+str(exc),file=sys.stderr)
        sys.exit(1)
