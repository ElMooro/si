#!/usr/bin/env python3
from pathlib import Path
TARGET = Path(__file__).resolve().parents[2] / "lambdas/justhodl-ask/source/lambda_function.py"

def main():
    t = TARGET.read_text()
    if "load_constitution" in t:
        print("ask already on constitution")
        return 0
    # add import after first import block — look for boto3 or json import near top
    needle = "import json"
    if "from consume_brain import load_constitution" not in t:
        t = t.replace("import json", "import json\nfrom consume_brain import load_constitution", 1)
    a = '''    brain = read_json("data/brain.json") or {}
    ctx["_brain"] = {"prompt_block": brain.get("prompt_block"), "tickers": brain.get("mentioned_tickers"),
                     "directive": brain.get("directive")}
'''
    b = '''    brain = read_json("data/brain.json") or {}
    constitution = load_constitution(s3)
    directive = None
    if constitution.get("ok"):
        directive = {k: constitution.get(k) for k in
                     ("investor_profile", "hard_rules", "themes", "sector_tilts",
                      "risk_posture", "signal_emphasis", "avoid", "regime_read")}
    else:
        directive = brain.get("directive")
    ctx["_brain"] = {"directive": directive,
                     "constitution_ok": bool(constitution.get("ok")),
                     "prompt_block": None if constitution.get("ok") else brain.get("prompt_block")}
'''
    if a not in t:
        raise SystemExit("ask brain block miss")
    t = t.replace(a, b, 1)
    TARGET.write_text(t)
    print("patched", TARGET)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
