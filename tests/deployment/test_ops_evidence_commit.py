"""Real local Git races: generated metadata recovers; product conflicts block."""
from pathlib import Path
import runpy
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
MODULE = runpy.run_path(str(ROOT/'scripts/commit_ops_evidence.py'))


def run(folder, *args):
    return subprocess.check_output(['git', *args], cwd=folder, text=True,
                                   stderr=subprocess.DEVNULL).strip()


def commit(folder, message):
    run(folder, 'add', '.'); run(folder, 'commit', '-m', message)


def setup(folder):
    folder=Path(folder); origin=folder/'origin.git'; seed=folder/'seed'; worker=folder/'worker'
    run(folder, 'init', '--bare', '--initial-branch=main', str(origin))
    run(folder, 'clone', str(origin), str(seed))
    run(seed,'config','user.email','fixture@example.invalid'); run(seed,'config','user.name','Fixture')
    (seed/'engine-manifest.json').write_text('old generated snapshot\n')
    (seed/'product.py').write_text('original product\n')
    commit(seed,'baseline'); run(seed,'push','origin','main')
    run(folder,'clone',str(origin),str(worker))
    run(worker,'config','user.email','fixture@example.invalid'); run(worker,'config','user.name','Fixture')
    return seed,worker


def test_generated_snapshot_race_preserves_new_source_and_ops_receipt():
    with tempfile.TemporaryDirectory() as folder:
        seed,worker=setup(folder)
        (seed/'engine-manifest.json').write_text('new product snapshot\n')
        (seed/'product.py').write_text('new product\n')
        commit(seed,'new product');run(seed,'push','origin','main')
        (worker/'engine-manifest.json').write_text('stale ops generation\n')
        (worker/'receipt.json').write_text('{"ok":true}\n');commit(worker,'ops evidence')
        run(worker,'fetch','origin','main')
        assert MODULE['reconcile'](worker,regenerate=False) is True
        assert (worker/'engine-manifest.json').read_text()=='new product snapshot\n'
        assert (worker/'product.py').read_text()=='new product\n'
        assert (worker/'receipt.json').read_text()=='{"ok":true}\n'
        assert run(worker,'status','--porcelain')==''


def test_conflicting_product_change_aborts_without_discarding_evidence():
    with tempfile.TemporaryDirectory() as folder:
        seed,worker=setup(folder)
        (seed/'product.py').write_text('concurrent product\n')
        commit(seed,'concurrent product');run(seed,'push','origin','main')
        (worker/'product.py').write_text('ops product change\n')
        (worker/'receipt.json').write_text('{"ok":true}\n');commit(worker,'ops evidence')
        before=run(worker,'rev-parse','HEAD');run(worker,'fetch','origin','main')
        try:MODULE['reconcile'](worker,regenerate=False)
        except RuntimeError as exc:assert str(exc)=='concurrent_ops_change_requires_review'
        else:raise AssertionError('product conflict must block automatic publication')
        assert run(worker,'rev-parse','HEAD')==before
        assert (worker/'receipt.json').is_file()
        assert run(worker,'status','--porcelain')==''
