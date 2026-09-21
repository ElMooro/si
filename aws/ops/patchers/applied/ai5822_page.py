#!/usr/bin/env python3
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "ai.html"
TAG = '<script src="/jh-ai-command.js?v=5822"></script>\n'

def main():
    t = TARGET.read_text()
    if "jh-ai-command.js" in t:
        print("already"); return
    if "</body>" not in t:
        raise SystemExit("no body")
    TARGET.write_text(t.replace("</body>", TAG + "</body>", 1))
    print("wired ai.html")

if __name__ == "__main__":
    main()
