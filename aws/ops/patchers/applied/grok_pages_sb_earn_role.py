#!/usr/bin/env python3
"""Align engine-output-roles with stock-buying v1.5.2 warm key (sb_earn, not sb_sur)."""
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
# parents[2] = aws/; roles live at repo root
TARGET = ROOT.parent / "config/engine-output-roles.json"
OLD = "data/warm/blackswan/sb_sur_*.json"
NEW = "data/warm/blackswan/sb_earn_*.json"

def main():
    text = TARGET.read_text()
    if NEW in text and OLD not in text:
        print("already clean:", TARGET)
        return 0
    if OLD not in text:
        raise SystemExit("anchor miss: " + OLD)
    TARGET.write_text(text.replace(OLD, NEW))
    print("patched", TARGET)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
