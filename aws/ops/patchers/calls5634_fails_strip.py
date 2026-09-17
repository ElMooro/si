#!/usr/bin/env python3
from pathlib import Path
ROOT = Path(__file__).resolve().parents[3]
TAG = '<script src="/jh-pd-fails-strip.js?v=5633"></script>\n'
for rel in ("calls.html", "defcon.html", "eurodollar.html", "liquidity.html"):
    p = ROOT / rel
    if not p.exists():
        print("skip", rel); continue
    t = p.read_text()
    if "jh-pd-fails-strip.js" in t:
        print("already", rel); continue
    if "</body>" not in t:
        print("nobody", rel); continue
    p.write_text(t.replace("</body>", TAG + "</body>", 1))
    print("wired", rel)
