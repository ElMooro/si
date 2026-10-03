#!/usr/bin/env python3
"""Guard the structure loader so Node tests do not see document."""
from pathlib import Path

def main():
    p = Path("jh-chart-distribution.js")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "jh-chart-distribution.js"
    t = p.read_text(encoding="utf-8")
    old = "if (document.getElementById(\"jh-struct-src\")) return;"
    new = "if (typeof document === \"undefined\") return;\n  if (document.getElementById(\"jh-struct-src\")) return;"
    if "typeof document === \"undefined\"" in t and "jh-struct-src" in t:
        print("already guarded")
        return 0
    if old not in t:
        raise SystemExit("loader needle missing")
    p.write_text(t.replace(old, new, 1), encoding="utf-8")
    print("guarded structure loader")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
