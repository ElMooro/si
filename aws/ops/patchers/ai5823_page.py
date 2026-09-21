#!/usr/bin/env python3
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "ai.html"
NEEDLES = ("</body>", "</BODY>")
TAG = '<script src="/jh-ai-command.js?v=5823"></script>\n'

def main():
    t = TARGET.read_text()
    if "jh-ai-command.js" in t:
        print("already wired"); return
    for n in NEEDLES:
        if n in t:
            TARGET.write_text(t.replace(n, TAG + n, 1))
            print("wired", n)
            return
    raise SystemExit("no body tag")

if __name__ == "__main__":
    main()
