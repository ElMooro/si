#!/usr/bin/env python3
"""Regenerate data/engine-manifest.json from the repo (ops 3436).
Engine -> written data/ keys (put_object/OUT_KEY greps) + description from
config.json. Runs on every ops run via run-ops.yml, so ask-desk and
strategist always reason over a fresh fleet map."""
import json, os, re, sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
LAMBDAS = ROOT / "aws" / "lambdas"
WRITE_PAT = re.compile(r'Key\s*=\s*[fF]?["\'](data/[a-z0-9_\-/{}\.]+?\.json)["\']')
CONST_PAT = re.compile(r'OUT_KEY\s*=\s*.*?["\'](data/[a-z0-9_\-/\.]+?\.json)["\']')

def main():
    engines = []
    for d in sorted(LAMBDAS.iterdir()):
        src = d / "source" / "lambda_function.py"
        if not src.exists():
            continue
        try:
            code = src.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        keys = sorted({m for m in WRITE_PAT.findall(code) + CONST_PAT.findall(code)
                       if "{" not in m})[:12]
        desc = ""
        cfg = d / "config.json"
        if cfg.exists():
            try:
                desc = (json.loads(cfg.read_text()).get("description") or "")[:140]
            except Exception:
                pass
        if not desc:
            head = code.split('"""', 2)
            desc = (head[1].strip().splitlines()[0][:140] if len(head) > 2 else "")
        engines.append({"engine": d.name, "keys": keys, "description": desc})
    doc = {"generated_at": datetime.now(timezone.utc).isoformat(),
           "source": "scripts/gen_engine_manifest.py (repo grep, per ops run)",
           "n_engines": len(engines), "engines": engines}
    out = ROOT / "engine-manifest.json"
    out.write_text(json.dumps(doc, separators=(",", ":")))
    print(f"[manifest] {len(engines)} engines -> {out}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
