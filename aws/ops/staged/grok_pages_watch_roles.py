#!/usr/bin/env python3
"""Trigger pages deploy and watch engine-output-roles on site builds."""
from pathlib import Path
REPO = Path(__file__).resolve().parents[3]
YML = REPO / ".github/workflows/pages.yml"
LAY = REPO / "config/home-layout.json"
NEEDLE = '      - "config/home-layout.json"\n'
INSERT = NEEDLE + '      - "config/engine-output-roles.json"\n'

def main():
    y = YML.read_text()
    if "config/engine-output-roles.json" in y:
        print("yml already lists roles")
    elif NEEDLE not in y:
        raise SystemExit("pages.yml anchor miss")
    else:
        YML.write_text(y.replace(NEEDLE, INSERT, 1))
        print("patched", YML)
    t = LAY.read_text()
    old = "Fusion engines appended 2026-09-11."
    new = "Fusion engines appended 2026-09-11. Pages gate: sb_earn warm cache."
    if new in t:
        print("layout note already stamped")
    elif old not in t:
        raise SystemExit("home-layout note miss")
    else:
        LAY.write_text(t.replace(old, new, 1))
        print("patched", LAY)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
