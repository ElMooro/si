#!/usr/bin/env python3
from pathlib import Path

OFF = [
    ('{id:"sma20",n:"SMA 20",c:"#26c6da",on:1', '{id:"sma20",n:"SMA 20",c:"#26c6da",on:0'),
    ('{id:"sma50",n:"SMA 50",c:"#2962ff",on:1', '{id:"sma50",n:"SMA 50",c:"#2962ff",on:0'),
    ('{id:"sma200",n:"SMA 200",c:"#ff6d00",on:1', '{id:"sma200",n:"SMA 200",c:"#ff6d00",on:0'),
    ('{id:"sma250",n:"SMA 250",c:"#e91e63",on:1', '{id:"sma250",n:"SMA 250",c:"#e91e63",on:0'),
    ('{id:"ema250",n:"EMA 250",c:"#089981",on:1', '{id:"ema250",n:"EMA 250",c:"#089981",on:0'),
    ('{id:"vwap",n:"VWAP",c:"#ab47bc",on:1', '{id:"vwap",n:"VWAP",c:"#ab47bc",on:0'),
    ('{id:"pc",n:"Prev close",c:"#787b86",on:1', '{id:"pc",n:"Prev close",c:"#787b86",on:0'),
    ('{id:"cvd",n:"CVD (est.)",on:1', '{id:"cvd",n:"CVD (est.)",on:0'),
]

def main():
    p = Path("jh-chart-engine.js")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "jh-chart-engine.js"
    t = p.read_text()
    n = 0
    for a, b in OFF:
        if a in t:
            t = t.replace(a, b, 1)
            n += 1
    # do not auto-draw NY VWAP line unless study on
    t = t.replace(
        "try{if(window.jhNyVwap){",
        "try{if(window.jhNyVwap && window.INDS && INDS.some(function(i){return i.id===\"vwap\"&&i.on;})){",
        1,
    )
    p.write_text(t)
    print("cleared", n)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
