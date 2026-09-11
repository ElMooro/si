#!/usr/bin/env python3
from pathlib import Path
TARGET = Path(__file__).resolve().parents[2] / "lambdas/justhodl-best-setups/source/lambda_function.py"

def main():
    t = TARGET.read_text()
    if "load_constitution" in t:
        print("best-setups already on constitution")
        return 0
    a = "from public_brain_projection import sanitize_public\n"
    b = a + "from consume_brain import load_constitution\n"
    if a not in t:
        raise SystemExit("import miss")
    t = t.replace(a, b, 1)
    a = '''    brain = read_json("data/brain.json") or {}
    brain_directive = brain.get("directive") or {}
'''
    b = '''    brain = read_json("data/brain.json") or {}
    constitution = load_constitution(s3)
    if constitution.get("ok"):
        brain_directive = {
            "themes": constitution.get("themes") or [],
            "sector_tilts": constitution.get("sector_tilts") or {},
            "hard_rules": constitution.get("hard_rules") or [],
            "risk_posture": constitution.get("risk_posture"),
        }
    else:
        brain_directive = brain.get("directive") or {}
'''
    if a not in t:
        raise SystemExit("brain block miss")
    t = t.replace(a, b, 1)
    TARGET.write_text(t)
    print("patched", TARGET)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
