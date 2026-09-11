#!/usr/bin/env python3
from pathlib import Path
TARGET = Path(__file__).resolve().parents[2] / "lambdas/justhodl-position-sizer/source/lambda_function.py"

def main():
    t = TARGET.read_text()
    if "load_constitution" in t:
        print("already consumes constitution")
        return 0
    a = "from public_brain_projection import sanitize_public\n"
    b = a + "from consume_brain import load_constitution, overlay_payload\n"
    if a not in t:
        raise SystemExit("import miss")
    t = t.replace(a, b, 1)
    a = '    brain = rj("data/brain.json") or {}\n'
    b = a + "    constitution = load_constitution(s3)\n"
    if a not in t:
        raise SystemExit("brain load miss")
    t = t.replace(a, b, 1)
    a = '''    directive = brain.get("directive") or {}
    posture = (directive.get("risk_posture") or "balanced").lower()
'''
    b = '''    if constitution.get("ok"):
        posture = (constitution.get("risk_posture") or "balanced")
    else:
        posture = ((brain.get("directive") or {}).get("risk_posture") or "balanced")
    posture = str(posture).lower()
'''
    if a not in t:
        raise SystemExit("posture miss")
    t = t.replace(a, b, 1)
    a = '           "caveat": "Sizes shrink automatically when the macro regime deteriorates."}'
    b = a + "\n    out = overlay_payload(out, constitution)"
    if a not in t:
        raise SystemExit("out miss")
    t = t.replace(a, b, 1)
    t = t.replace('"version": "1.0"', '"version": "1.1"', 1)
    TARGET.write_text(t)
    print("patched", TARGET)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
