#!/usr/bin/env python3
from pathlib import Path

def main():
    p = Path("jh-chart-engine.js")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "jh-chart-engine.js"
    t = p.read_text()
    if "window.lastBars=d" in t or "window.lastBars = d" in t:
        print("already clean")
        return 0
    old = "wipe(); lastBars=d;"
    new = "wipe(); lastBars=d; try{window.lastBars=d;window.jhActive=active;}catch(e){}"
    if old not in t:
        raise SystemExit("lastBars assign drifted")
    p.write_text(t.replace(old, new, 1))
    print("exposed lastBars")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
