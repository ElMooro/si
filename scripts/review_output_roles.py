#!/usr/bin/env python3
"""scripts/review_output_roles.py - re-bind config/engine-output-roles.json family rows to the writer functions as they
stand in the source today.

A family exclusion (role internal_input_cache etc. for a key like data/warm/blackswan/*.json) is tied by
scripts/build_page_data_contracts.py to the reviewed function bodies: the row records the source file, the proof lines
and the ast sha256 of every function that writes the family.  When the writer moves or changes - 2026-10-20: the seven
blackswan engines' resolver ladders moved verbatim into aws/shared/blackswan_resolver.py - the row must be re-reviewed
and re-bound, or the Pages deploy gate refuses the build ("Internal family source review drift").

Usage: python3.12 scripts/review_output_roles.py [--check] <engine> [<engine> ...]
  * reads engine-manifest.json (regenerate it first: python3 scripts/gen_engine_manifest.py)
  * for every family row of the named engines: source := the file holding the proofs, lines := the proof lines,
    functions := innermost functions containing those lines (name + ast sha256), purpose/role untouched
  * --check only reports rows that would change (exit 1 if any)
Run it with the runner's Python (3.12): ast.dump output - hence the hashes - differs between minor versions.
"""
from pathlib import Path
import ast
import hashlib
import json
import sys

ROOT = Path(__file__).resolve().parents[1]
ROLES = ROOT / "config/engine-output-roles.json"


def fn_hash(node):
    return hashlib.sha256(ast.dump(node, include_attributes=False).encode()).hexdigest()


def rebind(row, engine):
    key = row["key"]
    proofs = engine.get("write_evidence", {}).get(key, [])
    if not proofs:
        return None, "no write evidence for %s" % key
    files = sorted({p.get("repository_path") or "aws/lambdas/%s/source/%s" % (engine["engine"], p["file"]) for p in proofs})
    if len(files) != 1:
        return None, "%s: proofs span %d files %s" % (key, len(files), files)
    source = files[0]
    lines = sorted({int(p["line"]) for p in proofs})
    tree = ast.parse((ROOT / source).read_text(encoding="utf-8"))
    funcs = [n for n in ast.walk(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))]
    chosen = {}
    for line in lines:
        covering = [n for n in funcs if n.lineno <= line <= n.end_lineno]
        if not covering:
            return None, "%s: line %d of %s is outside every function" % (key, line, source)
        inner = min(covering, key=lambda n: n.end_lineno - n.lineno)
        chosen[inner.name] = inner
    new = dict(row)
    new["evidence"] = {"source": source, "lines": lines,
                       "functions": [{"name": n, "ast_sha256": fn_hash(node)} for n, node in sorted(chosen.items())]}
    return new, None


def main(argv):
    check = "--check" in argv
    names = [a for a in argv if not a.startswith("--")]
    manifest = json.loads((ROOT / "engine-manifest.json").read_text(encoding="utf-8"))
    engines = {e["engine"]: e for e in manifest["engines"]}
    raw = ROLES.read_text(encoding="utf-8")
    doc = json.loads(raw)
    changed, problems = [], []
    for i, row in enumerate(doc["roles"]):
        if row["engine"] not in names or "*" not in row["key"]:
            continue
        engine = engines.get(row["engine"])
        if engine is None:
            problems.append("%s: not in engine-manifest.json" % row["engine"])
            continue
        new, err = rebind(row, engine)
        if err:
            problems.append("%s %s" % (row["engine"], err))
            continue
        if new["evidence"] != row["evidence"]:
            changed.append((row["engine"], row["key"], row["evidence"]["source"], new["evidence"]["source"],
                            [f["name"] for f in new["evidence"]["functions"]]))
            doc["roles"][i] = new
    for c in changed:
        print("rebind %s %s: %s -> %s via %s" % c)
    for p in problems:
        print("PROBLEM", p)
    if not check and changed:
        ROLES.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    print("%d row(s) rebound, %d problem(s)%s" % (len(changed), len(problems), " (check only)" if check else ""))
    return 1 if problems or (check and changed) else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
