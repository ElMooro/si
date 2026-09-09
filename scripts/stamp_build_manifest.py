#!/usr/bin/env python3
"""Stamp every shipped HTML page and hash the complete deploy artifact."""
from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import sys
from pathlib import Path


def stamp(site, commit):
    if not re.fullmatch(r"[a-f0-9]{40}", commit):
        raise ValueError("A full source commit SHA is required")
    pages = sorted(site.rglob("*.html"))
    if not pages:
        raise ValueError("No pages found in build artifact")
    for page in pages:
        content = page.read_text()
        content = re.sub(r'<meta\s+name=[\'"]jh-build-commit[\'"][^>]*>\s*', "", content, flags=re.I)
        tag = f'<meta name="jh-build-commit" content="{commit}">'
        if re.search(r"</head\s*>", content, re.I):
            content = re.sub(r"</head\s*>", tag + "\n</head>", content, count=1, flags=re.I)
        else:
            content = tag + "\n" + content
        page.write_text(content)
    hashes = {
        p.relative_to(site).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in sorted(site.rglob("*")) if p.is_file() and p.name != "build-manifest.json"
    }
    manifest = {"schema_version": "2.0", "commit_sha": commit, "page_count": len(pages),
                "file_count": len(hashes), "files_sha256": hashes,
                "chart_pro_sha256": hashes.get("chart-pro.html")}
    (site / "build-manifest.json").write_text(json.dumps(manifest, sort_keys=True, separators=(",", ":")) + "\n")
    return manifest


if __name__ == "__main__":
    commit = os.environ.get("GITHUB_SHA") or subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    result = stamp(Path(sys.argv[1] if len(sys.argv) > 1 else "_site"), commit)
    print(f"Stamped {result['page_count']} pages and {result['file_count']} artifact hashes for {commit}")
