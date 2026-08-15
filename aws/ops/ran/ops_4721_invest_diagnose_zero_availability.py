"""
ops/4721 — diagnose why every leg showed available=0 in ops 4720's live
run, when ops 4716's probe proved at least
fleet:data/asia-leads.json:korea_exports.yoy_pct resolves to 47.96.

Two checks, in order:
1. Zip-settle: download the LIVE deployed code package and grep it for a
   marker string only the just-pushed fleet_io.py contains. AUTONOMY.md's
   own known-traps section documents this exact race (deploy-lambdas.yml
   and another workflow can disagree about which artifact is live) --
   check it before assuming the bug is in the source rather than in what
   actually got deployed.
2. If the code IS current: import the real causal_graph + fleet_io from
   this checkout and call read_leg_value() directly against real S3 for
   one confirmed-good leg, printing the raw intermediate values (parsed
   key/path, loaded doc's actual top-level keys, dig() result) so the
   break point is visible instead of guessed.
"""
import json
import sys
import zipfile
import io
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "lambdas" / "justhodl-invest" / "source"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

FUNCTION_NAME = "justhodl-invest"
REGION = "us-east-1"
MARKER = "_BRACKET_RE"  # only exists in the fixed fleet_io.py

lam = boto3.client("lambda", region_name=REGION)


def main():
    with report("4721_invest_diagnose_zero_availability") as rep:
        rep.heading("ops 4721 — diagnose universal leg unavailability")

        rep.section("1. Zip-settle: is the deployed code actually current?")
        cfg = lam.get_function(FunctionName=FUNCTION_NAME)
        code_url = cfg["Code"]["Location"]
        last_modified = cfg["Configuration"]["LastModified"]
        code_sha = cfg["Configuration"]["CodeSha256"]
        rep.kv(last_modified=last_modified, code_sha256=code_sha[:16])
        import urllib.request
        with urllib.request.urlopen(code_url, timeout=30) as r:
            zip_bytes = r.read()
        zf = zipfile.ZipFile(io.BytesIO(zip_bytes))
        names = zf.namelist()
        rep.kv(zip_file_count=len(names))
        fleet_io_present = "fleet_io.py" in names
        rep.kv(fleet_io_in_zip=fleet_io_present)
        if fleet_io_present:
            content = zf.read("fleet_io.py").decode("utf-8", errors="replace")
            has_marker = MARKER in content
            rep.kv(deployed_fleet_io_has_bracket_search=has_marker,
                   deployed_fleet_io_bytes=len(content))
            if not has_marker:
                rep.fail(f"  DEPLOYED CODE IS STALE -- the live zip's fleet_io.py does "
                         f"NOT contain '{MARKER}'. deploy-lambdas.yml ran (see ops 4717/"
                         f"resmoke) but the artifact it deployed predates the fix. This "
                         f"is the bug, not the source.")
            else:
                rep.ok(f"  deployed fleet_io.py IS current (contains '{MARKER}')")
        also_shared = [n for n in names if n in ("impact_mapper.py", "evidence_weights.py")]
        rep.kv(shared_modules_bundled=also_shared)

        rep.section("2. Direct read_leg_value() call against real S3")
        import importlib
        if "fleet_io" in sys.modules:
            importlib.reload(sys.modules["fleet_io"])
        import fleet_io  # noqa: E402
        import causal_graph  # noqa: E402

        test_leg = None
        for ind in causal_graph.LEADING_INDICATORS:
            for leg in ind.legs:
                if leg.leg_id == "korea_export_value_yoy_flash":
                    test_leg = leg
        if test_leg is None:
            rep.fail("  could not find korea_export_value_yoy_flash in causal_graph "
                     "(this checkout may itself be stale -- unexpected)")
            return

        rep.log(f"  leg.source = {test_leg.source!r}")
        key, path = fleet_io.parse_source(test_leg.source)
        rep.log(f"  parsed: key={key!r} path={path!r}")
        doc = fleet_io.get_json(key)
        rep.log(f"  doc is None: {doc is None}")
        if doc is not None:
            rep.log(f"  doc top-level keys: {sorted(doc.keys())}")
        val = fleet_io.dig(doc, path) if doc is not None else None
        rep.log(f"  dig(doc, path) = {val!r}")
        resolved = fleet_io.read_leg_value(test_leg.source)
        rep.log(f"  read_leg_value(source) = {resolved!r}")
        if resolved == 47.96 or (isinstance(resolved, float) and abs(resolved - 47.96) < 0.5):
            rep.ok(f"  resolves correctly in THIS checkout's code: {resolved}")
        else:
            rep.fail(f"  does NOT resolve as expected (got {resolved!r}, "
                     f"expected ~47.96) -- bug is in the current source, not staleness")

        rep.section("Verdict")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("DIAGNOSE ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
