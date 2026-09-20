"""Identify direct/transitive Lambda users of changed shared modules, including literal dynamic imports."""
from pathlib import Path
import argparse,re,sys
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/ops/checks'))
from release_package_evidence import shared_imports


def dependents(root,paths):
    changed=set()
    for path in paths:
        if not re.fullmatch(r'aws/shared/[A-Za-z0-9_]+\.py',path):raise ValueError('top-level shared module path required')
        changed.add(Path(path).stem)
    if not changed:return []
    result=[]
    for directory in sorted((root/'aws/lambdas').iterdir()):
        source=directory/'source'
        if directory.name=='_archived' or not source.is_dir():continue
        files=list(source.rglob('*.py'))
        try:imports={p.stem for p in shared_imports(root,files)}
        except (SyntaxError,UnicodeError):
            # Preserve unrelated legacy inventory; a relevant unparseable user
            # must fail its normal deploy compilation instead of being omitted.
            imports={name for name in changed if any(re.search(r'\b'+re.escape(name)+r'\b',p.read_text(encoding='utf-8',errors='replace')) for p in files)}
        if imports & changed:result.append(directory.name)
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('paths',nargs='+');args=parser.parse_args()
    print(' '.join(dependents(ROOT,args.paths)))
