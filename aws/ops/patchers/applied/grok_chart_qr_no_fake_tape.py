#!/usr/bin/env python3
from pathlib import Path

OLD = '''    if(!el) return;
    var rows=tape.prints.slice().reverse();'''
# we'll replace the waiting copy instead
WAIT = "Waiting for tape"
WAIT2 = "No public prints for this symbol (warehouse is daily bars; tick tape is crypto/Binance only)"
LOAD = "loading prints…"
LOAD2 = "no tick tape"

def main():
    p = Path("jh-chart-engine.js")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "jh-chart-engine.js"
    t = p.read_text()
    if WAIT in t:
        t = t.replace(WAIT, WAIT2, 1)
        print("empty copy")
    if LOAD in t:
        t = t.replace(LOAD, LOAD2, 1)
        print("bar copy")
    old_iv = "tapeT=setInterval(function(){ if(liveOn && !replay.on) loadTape(false); }, 2500);"
    new_iv = "tapeT=setInterval(function(){ if(liveOn && !replay.on && /USDT$|BUSD$|USDC$/.test(resolveSym(active).ticker)) loadTape(false); }, 4000);"
    if old_iv in t:
        t = t.replace(old_iv, new_iv, 1)
        print("poll crypto only")
    p.write_text(t)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
