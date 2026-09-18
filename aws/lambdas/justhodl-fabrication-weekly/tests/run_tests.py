import importlib.util
import json
from pathlib import Path
import sys
import types
from unittest.mock import patch

root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(root.parents[1] / "shared"))
from provenance import wrap


class S3:
    def put_object(self, **kw): self.output = json.loads(kw["Body"])
s3 = S3()
with patch.dict(sys.modules, {"boto3": types.SimpleNamespace(client=lambda *a, **kw: s3)}):
    spec = importlib.util.spec_from_file_location("fabrication_fixture", root / "source/lambda_function.py")
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)

def get(key):
    if key == "data/audit/fabrication-weekly.json": return {"history": [{"as_of": "2026-01-01", "flagship_coverage_avg_pct": 100}]}
    if key == "data/audit/fabrication-sites.json": return {}
    if key == mod.FLAGSHIPS[-1]: return None
    return {"value": wrap(42, "fixture", evidence={"captured": True, "key": "not-replayed"})}
mod._get = get
mod.lambda_handler({}, None)
doc = s3.output
assert doc["flagship_coverage_avg_pct"] == 100
assert doc["replay_verified_pct"] == 0
assert doc["sizing_eligible"] is False and doc["schema_version"] == "2.0"
assert doc["flagship_provenance"][mod.FLAGSHIPS[-1]]["data_unavailable"]
assert "coverage_contract" not in doc["history"][0] and doc["history"][-1]["coverage_contract"] == "provenance-coverage.v2"
assert "configured list" in doc["note"]
print("Fabrication coverage tests passed: structural != replay, missing feed, legacy history, permissions")
