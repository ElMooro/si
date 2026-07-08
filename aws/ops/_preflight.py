#!/usr/bin/env python3
"""Preflight linter -- catch push-cycle failures BEFORE they cost a
push -> Actions -> poll -> pull round trip (~3-6 min each).

Every failure class in here has burned a real run at least once:
  - Lambda Description > 256 chars (ValidationException at CreateFunction)
  - missing sys.exit(1) (green-on-fail, auto-move, rebase collision)
  - CRLF endings (breaks Lambda handlers)
  - duplicate ops numbers across pending/ + ran/ (parallel sessions)
  - py_compile errors
Warns (won't block): classic EventBridge put_rule (rule cap SATURATED --
use EventBridge Scheduler), sync lam.invoke without a botocore
read_timeout Config, S3 put_object to a non-data/ root key.

Usage: python aws/ops/_preflight.py <file-or-dir> [...]
       (ops scripts, lambda source files, or lambda dirs; config.json
        descriptions are checked for any lambda paths given)
Exit 1 on any hard fail.
"""
import json
import py_compile
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OPS = ROOT / "aws" / "ops"
HARD, WARN = [], []


def check_desc_literals(text, label):
    for m in re.finditer(r"description\s*=\s*(['\"])((?:[^\\]|\\.)*?)\1",
                        text, re.S):
        if len(m.group(2)) > 256:
            HARD.append("%s: description literal %d chars (>256 breaks "
                        "CreateFunction)" % (label, len(m.group(2))))


def check_py(path):
    rel = str(path.relative_to(ROOT))
    raw = path.read_bytes()
    if b"\r\n" in raw:
        HARD.append("%s: CRLF line endings" % rel)
    text = raw.decode("utf-8", "replace")
    try:
        py_compile.compile(str(path), doraise=True)
    except Exception as e:
        HARD.append("%s: py_compile: %s" % (rel, str(e)[:160]))
        return
    check_desc_literals(text, rel)
    is_ops = path.parent == OPS / "pending"
    if is_ops:
        if "sys.exit(1)" not in text:
            HARD.append("%s: no sys.exit(1) anywhere -- fails would go "
                        "green and auto-move" % rel)
        m = re.match(r"ops_(\d+)_", path.name)
        if m:
            num = m.group(1)
            dups = [p.name for d in ("pending", "ran")
                    for p in (OPS / d).glob("ops_%s_*.py" % num)
                    if p.resolve() != path.resolve()]
            if dups:
                HARD.append("%s: ops number %s already used by %s"
                            % (rel, num, dups))
    if re.search(r"\bput_rule\s*\(", text):
        WARN.append("%s: classic EventBridge put_rule -- rule cap is "
                    "SATURATED; use EventBridge Scheduler" % rel)
    if (re.search(r"\.invoke\s*\(", text)
            and "read_timeout" not in text
            and "InvocationType" not in text.replace('"Event"', "")):
        WARN.append("%s: sync .invoke() without botocore "
                    "Config(read_timeout=...) -- 60s default client "
                    "timeout" % rel)
    for m in re.finditer(r"Key\s*=\s*(['\"])([^'\"]+)\1", text):
        k = m.group(2)
        if ("put_object" in text and not k.startswith(
                ("data/", "archive/", "learning/", "screener/", "tools/",
                 "llm-cache/", "equity-research/", "track-record/"))
                and "/" not in k and k.endswith(".json")):
            WARN.append("%s: put_object to ROOT key '%s' -- outputs "
                        "belong under data/" % (rel, k))


def check_config(path):
    rel = str(path.relative_to(ROOT))
    try:
        c = json.loads(path.read_text())
    except Exception as e:
        HARD.append("%s: bad JSON: %s" % (rel, str(e)[:120]))
        return
    if len(c.get("description") or "") > 256:
        HARD.append("%s: config description %d chars (>256)"
                    % (rel, len(c["description"])))


def main():
    targets = [Path(a).resolve() for a in sys.argv[1:]]
    if not targets:
        print("usage: _preflight.py <files-or-dirs...>")
        sys.exit(2)
    files = []
    for t in targets:
        if t.is_dir():
            files += list(t.rglob("*.py")) + list(t.rglob("config.json"))
        else:
            files.append(t)
    for f in files:
        if f.name == "config.json":
            check_config(f)
        elif f.suffix == ".py":
            check_py(f)
    for w in WARN:
        print("WARN: %s" % w)
    for h in HARD:
        print("FAIL: %s" % h)
    if HARD:
        sys.exit(1)
    print("PREFLIGHT PASS (%d file(s), %d warn(s))" % (len(files),
                                                       len(WARN)))


if __name__ == "__main__":
    main()
