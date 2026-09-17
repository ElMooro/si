#!/usr/bin/env python3
"""ops 5633 — load FTD/FTR strip on liquidity, risk-regime, eurodollar, macro pages."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TAG = '<script src="/jh-pd-fails-strip.js?v=5633"></script>\n'
PAGES = [
    "liquidity.html",
    "risk-regime.html",
    "fails.html",
    "eurodollar.html",
    "euro-dollar.html",
    "macro-data.html",
    "macro-rooms.html",
    "defcon.html",
]


def main() -> None:
    n = 0
    for rel in PAGES:
        p = ROOT / rel
        if not p.exists():
            print("skip missing", rel)
            continue
        t = p.read_text()
        if "jh-pd-fails-strip.js" in t:
            print("already", rel)
            continue
        if "</body>" not in t:
            print("no body", rel)
            continue
        p.write_text(t.replace("</body>", TAG + "</body>", 1))
        n += 1
        print("wired", rel)
    if n == 0:
        print("nothing wired (all missing or already tagged)")


if __name__ == "__main__":
    main()
