"""Source-owned schema and actual public validator boundary tests."""
from pathlib import Path
import runpy
ROOT = Path(__file__).resolve().parents[4]
if __name__ == "__main__":
    scope = runpy.run_path(str(ROOT / "tests/deployment/test_reviewed_contract_overlays.py"))
    tests = [(name, test) for name, test in scope.items() if name.startswith("test_") and callable(test)]
    for name, test in tests:
        test()
    print("Reviewed contract tests:", len(tests))
