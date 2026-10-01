"""Shared no-paid policy and caller regressions run before every Lambda release."""
from pathlib import Path
import subprocess,sys
ROOT=Path(__file__).resolve().parents[2]

def test_no_paid_shared_entrypoints_and_repaired_callers():
    for script in ('tests/test_no_paid_adapters.py','tests/test_no_paid_callers.py','tests/test_dataset_identity.py','aws/ops/checks/test_no_paid_adapter_acceptance.py'):
        subprocess.run([sys.executable,str(ROOT/script)],cwd=ROOT,check=True)
