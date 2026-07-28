"""
ops_3996 — PROBE: is the VAULT ARTIFACT itself the truncated thing?

Census recorded n_paths=401 for data/tradingview.json at size=391KB — far
too small for 561 symbols with note snippets. Either (a) the live vault
artifact has shrunk (a vault-engine regression from the v3.7/v3.8 edits,
or an unexplained 15:48 UTC write), and the census is faithfully walking a
broken input; or (b) the artifact is whole and the DEPLOYED census walk
code differs from repo. This probe answers both with primary evidence.
"""
import io
import json
import sys
import urllib.request
import zipfile as zf
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def main():
    with report("3996_vault_artifact_truth") as rep:
        rep.heading("ops 3996 — vault artifact truth + deployed census walk dump")

        rep.section("A. the live vault artifact itself")
        v = json.loads(s3.get_object(Bucket=BUCKET,
                                     Key="data/tradingview.json")["Body"].read())
        syms = v.get("symbols") or []
        names = [r.get("symbol") for r in syms]
        rep.kv(marker=v.get("marker"), version=v.get("version"),
               generated_at=v.get("generated_at"),
               n_symbols_field=v.get("n_symbols"), len_symbols=len(syms),
               n_live=v.get("n_live"))
        for w in ("EUBUND", "JP02Y", "NO03Y", "PETOT", "JPLG", "US10Y",
                  "USM0", "XAUUSD"):
            rep.log(f"  contains {w}: {w in names}")
        rep.log(f"  first 6: {names[:6]}")
        rep.log(f"  last 6:  {names[-6:]}")
        n_num = 0
        if syms:
            n_num = sum(1 for k2, v2 in syms[0].items()
                        if isinstance(v2, (int, float)) and not isinstance(v2, bool))
            rep.log(f"  numeric fields on row0: {n_num} "
                    f"({[k2 for k2, v2 in syms[0].items() if isinstance(v2, (int, float)) and not isinstance(v2, bool)]})")
        rep.kv(expected_paths_if_walked_fully=len(syms) * max(1, n_num))

        rep.section("B. deployed census walk() — the list branch, verbatim")
        info = lam.get_function(FunctionName="justhodl-data-census")
        dep = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
            info["Code"]["Location"], timeout=60).read()
        )).read("lambda_function.py").decode()
        rep.kv(deployed_marker_line=[l for l in dep.splitlines()
                                     if l.startswith("MARKER")][0])
        i = dep.find("elif isinstance(o, list)")
        rep.log("  " + "\n  ".join(dep[i:i + 700].splitlines()))
        j = dep.find("pl = walk(doc")
        rep.log("  CALL SITE: " + "\n  ".join(dep[j:j + 300].splitlines()[:6]))

        rep.section("C. vault ENGINE deployed marker (is v3.8 live?)")
        info2 = lam.get_function(FunctionName="justhodl-tradingview")
        dep2 = zf.ZipFile(io.BytesIO(urllib.request.urlopen(
            info2["Code"]["Location"], timeout=60).read()
        )).read("lambda_function.py").decode()
        rep.kv(vault_engine_marker=[l for l in dep2.splitlines()
                                    if l.startswith("MARKER")][0],
               last_modified=info2["Configuration"].get("LastModified"))

        rep.ok("PROBE DONE — A decides shrunken-artifact vs census bug; "
               "B settles the deployed-code question; C dates the vault code")


if __name__ == "__main__":
    main()
