"""
Test bootstrap. Two path additions, neither of which ships a duplicate of
fleet code:

1. source/ so `import lambda_function`, `import scoring`, etc. work.
2. aws/shared/ (the fleet's real, single copy of impact_mapper.py /
   evidence_weights.py) -- resolved relative to this file's position
   inside a real repo checkout (aws/lambdas/justhodl-invest/tests/ ->
   up to repo root -> aws/shared), or from INVEST_TEST_SHARED_DIR if set.
   At actual deploy time this is irrelevant: _lambda_deploy_helpers.
   build_zip() bundles aws/shared/*.py into the Lambda zip automatically.
   This is ONLY so `pytest` can import lambda_function.py in isolation
   before anything ever reaches AWS.
"""
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve()
SOURCE_DIR = HERE.parents[1] / "source"
sys.path.insert(0, str(SOURCE_DIR))

_candidates = []
env_dir = os.environ.get("INVEST_TEST_SHARED_DIR")
if env_dir:
    _candidates.append(Path(env_dir))
# aws/lambdas/justhodl-invest/tests -> parents[3] == aws  ->  aws/shared
_candidates.append(HERE.parents[3] / "shared")

for cand in _candidates:
    if (cand / "impact_mapper.py").is_file():
        sys.path.insert(0, str(cand))
        break
