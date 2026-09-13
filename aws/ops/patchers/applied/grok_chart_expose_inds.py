#!/usr/bin/env python3
from pathlib import Path

def main():
    p = Path("jh-chart-engine.js")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "jh-chart-engine.js"
    t = p.read_text()
    needle = "wipe(); lastBars=d; try{window.lastBars=d;window.jhActive=active;"
    extra = "window.INDS=INDS;window.OSC=OSC;window.paint=paint;"
    if "window.INDS=INDS" in t:
        print("already")
        return 0
    if needle in t:
        t = t.replace(needle, needle + extra, 1)
        p.write_text(t)
        print("exposed INDS/paint")
    else:
        print("hook drifted")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
