#!/usr/bin/env python3
"""Block known retired credentials and unmistakable secret formats without echoing values."""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TOKENS = re.compile(r"[A-Za-z0-9_-]{12,}")
FORMATS = [
    ("github-token", re.compile(r"(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{50,})")),
    ("aws-access-id", re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")),
    ("private-key", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
]


def findings(text, retired):
    result = []
    for line_no, line in enumerate(text.splitlines(), 1):
        if any(hashlib.sha256(m.group().encode()).hexdigest() in retired for m in TOKENS.finditer(line)):
            result.append((line_no, "retired-provider-credential"))
        for name, pattern in FORMATS:
            if pattern.search(line):
                result.append((line_no, name))
    return result


def main():
    retired = set(json.loads((ROOT / "tests/security/retired-secret-sha256.json").read_text())["sha256"])
    paths = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT).decode().split("\0")
    failures = []
    for relative in filter(None, paths):
        try:
            content = (ROOT / relative).read_text()
        except UnicodeError:
            continue
        for line, kind in findings(content, retired):
            failures.append({"path": relative, "line": line, "kind": kind})
    # Locations only: neither matched values nor surrounding source enter logs.
    print(json.dumps({"files_scanned": len(paths) - 1, "findings": failures}, indent=2))
    return int(bool(failures))


if __name__ == "__main__":
    sys.exit(main())
