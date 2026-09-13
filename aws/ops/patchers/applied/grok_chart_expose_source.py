#!/usr/bin/env python3
from pathlib import Path

def main():
    p = Path("jh-chart-engine.js")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "jh-chart-engine.js"
    t = p.read_text()
    needle = "wipe(); lastBars=d; try{window.lastBars=d;window.jhActive=active;}catch(e){}"
    if "window.lastSource" in t:
        print("already")
        return 0
    if needle not in t:
        raise SystemExit("paint hook drifted")
    t = t.replace(needle, needle[:-11] + "window.lastSource=lastSource;}catch(e){}", 1)
    p.write_text(t)
    print("exposed lastSource")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
