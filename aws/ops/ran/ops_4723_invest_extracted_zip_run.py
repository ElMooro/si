"""
ops/4723 — run read_leg_value() using modules imported from the EXACT
extracted deployed zip (not this checkout's copy), and print every file in
that zip. ops 4721 only grepped for one marker substring in one file;
this rules out a name collision from aws/shared/*.py bundling (deploy-
lambdas.yml bundles ALL of aws/shared/, not a scoped subset -- if any
shared file is also named fleet_io.py/causal_graph.py/scoring.py, Python's
import order could silently shadow this engine's own modules).
"""
import importlib
import io
import json
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

FUNCTION_NAME = "justhodl-invest"
REGION = "us-east-1"

lam = boto3.client("lambda", region_name=REGION)


def main():
    with report("4723_invest_extracted_zip_run") as rep:
        rep.heading("ops 4723 — run against modules imported from the exact deployed zip")

        cfg = lam.get_function(FunctionName=FUNCTION_NAME)
        code_url = cfg["Code"]["Location"]
        with urllib.request.urlopen(code_url, timeout=30) as r:
            zip_bytes = r.read()
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
        names = sorted(zf.namelist())

        rep.section("1. Full file list in the deployed zip")
        rep.kv(n_files=len(names))
        for n in names:
            rep.log(f"  {n}")
        dupes = [n for n in names if names.count(n) > 1]
        collision_candidates = [n for n in names
                                 if Path(n).name in ("fleet_io.py", "causal_graph.py",
                                                      "scoring.py", "lambda_function.py")]
        rep.kv(exact_name_matches_for_our_modules=collision_candidates,
               duplicate_entries=list(set(dupes)))

        rep.section("2. Extract and run read_leg_value from THIS EXACT zip")
        tmpdir = tempfile.mkdtemp(prefix="invest_zip_")
        zf.extractall(tmpdir)
        rep.log(f"  extracted to {tmpdir}")

        # Clear any prior import of these names in this process, then put
        # the extracted dir FIRST on sys.path, exactly like /var/task.
        for mod in ("fleet_io", "causal_graph", "scoring", "lambda_function"):
            sys.modules.pop(mod, None)
        sys.path.insert(0, tmpdir)

        fleet_io = importlib.import_module("fleet_io")
        causal_graph = importlib.import_module("causal_graph")
        rep.log(f"  fleet_io loaded from: {fleet_io.__file__}")
        rep.log(f"  causal_graph loaded from: {causal_graph.__file__}")
        rep.kv(fleet_io_has_bracket_search=hasattr(fleet_io, "_BRACKET_RE"))

        test_leg = None
        for ind in causal_graph.LEADING_INDICATORS:
            for leg in ind.legs:
                if leg.leg_id == "korea_export_value_yoy_flash":
                    test_leg = leg
        rep.log(f"  leg.source = {test_leg.source!r}")

        key, path = fleet_io.parse_source(test_leg.source)
        doc = fleet_io.get_json(key)
        rep.log(f"  get_json({key!r}) is None: {doc is None}")
        if doc:
            rep.log(f"  doc keys: {sorted(doc.keys())}")
        val = fleet_io.dig(doc, path) if doc else None
        rep.log(f"  dig -> {val!r}")
        resolved = fleet_io.read_leg_value(test_leg.source)
        rep.log(f"  read_leg_value -> {resolved!r}")

        rep.section("3. Now run causal_graph's FULL leg list through read_leg_value, from the extracted zip")
        n_ok, n_none = 0, 0
        for ind in causal_graph.LEADING_INDICATORS:
            for leg in ind.legs:
                v = fleet_io.read_leg_value(leg.source)
                rep.log(f"  {leg.leg_id:32s} -> {v!r}")
                if v is None:
                    n_none += 1
                else:
                    n_ok += 1
        rep.kv(resolved_ok=n_ok, resolved_none=n_none)

        rep.section("Verdict")
        if resolved is not None and n_ok > 0:
            rep.ok("modules imported from the EXACT deployed zip resolve legs correctly "
                   "when run from this ops script's process -- if the real Lambda invoke "
                   "still returns none, the difference is something about the Lambda "
                   "execution environment itself (network path, cold-start timing, or "
                   "something not reproducible outside it), not the code or the S3 data.")
        else:
            rep.fail("legs failed to resolve even from the exact extracted zip -- the bug "
                     "IS in the code/data, not the execution environment. Re-examine.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("EXTRACTED ZIP RUN ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
