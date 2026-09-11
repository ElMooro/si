#!/usr/bin/env python3
from pathlib import Path
TARGET = Path(__file__).resolve().parents[2] / "lambdas/justhodl-devils-advocate/source/lambda_function.py"

def main():
    t = TARGET.read_text()
    if "load_constitution" in t:
        print("devils already on constitution")
        return 0
    a = "from public_brain_projection import devils_public\n"
    b = a + "from consume_brain import load_constitution\n"
    if a not in t:
        raise SystemExit("import miss")
    t = t.replace(a, b, 1)
    a = '''    brain = read_json("data/brain.json") or {}
    directive = brain.get("directive") or {}
'''
    b = '''    brain = read_json("data/brain.json") or {}
    constitution = load_constitution(s3)
    if constitution.get("ok"):
        directive = {
            "hard_rules": constitution.get("hard_rules") or [],
            "avoid": constitution.get("avoid") or [],
            "themes": constitution.get("themes") or [],
            "risk_posture": constitution.get("risk_posture"),
        }
    else:
        directive = brain.get("directive") or {}
'''
    if a not in t:
        raise SystemExit("directive miss")
    t = t.replace(a, b, 1)
    TARGET.write_text(t)
    print("patched", TARGET)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
