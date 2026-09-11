#!/usr/bin/env python3
from pathlib import Path
TARGET = Path(__file__).resolve().parents[2] / "lambdas/justhodl-my-brief/source/lambda_function.py"

def main():
    t = TARGET.read_text()
    if "load_constitution" in t:
        print("my-brief already on constitution")
        return 0
    a = "from public_brain_projection import brief_public\n"
    b = a + "from consume_brain import load_constitution\n"
    if a not in t:
        raise SystemExit("import miss")
    t = t.replace(a, b, 1)
    a = '''    brain = rj("data/brain.json") or {}
    directive = brain.get("directive")
    if not directive:
'''
    b = '''    brain = rj("data/brain.json") or {}
    constitution = load_constitution(s3)
    directive = None
    if constitution.get("ok"):
        directive = {
            "investor_profile": constitution.get("investor_profile"),
            "hard_rules": constitution.get("hard_rules"),
            "themes": constitution.get("themes"),
            "sector_tilts": constitution.get("sector_tilts"),
            "risk_posture": constitution.get("risk_posture"),
            "signal_emphasis": constitution.get("signal_emphasis"),
            "avoid": constitution.get("avoid"),
            "regime_read": constitution.get("regime_read"),
        }
    else:
        directive = brain.get("directive")
    if not directive:
'''
    if a not in t:
        raise SystemExit("directive miss")
    t = t.replace(a, b, 1)
    TARGET.write_text(t)
    print("patched", TARGET)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
