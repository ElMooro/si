#!/usr/bin/env python3
"""Warehouse /ohlc bars use `value` = share volume. Ignoring it zeros volScore and skips the only good tape."""
from pathlib import Path

OLD = "volume:+(b.volume||b.v||b.vol||b.Volume||0)"
NEW = (
    "volume:+(function(){var cand=b.volume!=null?b.volume:(b.v!=null?b.v:(b.vol!=null?b.vol:(b.Volume!=null?b.Volume:b.value)));"
    "var n=+cand;if(!isFinite(n)||n<0)return 0;if(b.volume==null&&b.v==null&&b.vol==null&&b.Volume==null&&n>0&&n<c3*8)return 0;return n;})()"
)

def main():
    p = Path("jh-chart-engine.js")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "jh-chart-engine.js"
    t = p.read_text()
    if OLD in t:
        t = t.replace(OLD, NEW, 1)
        print("value-as-volume restored for warehouse schema")
    elif "c3*8" in t:
        print("already")
    else:
        print("volume assign drifted")
    olds = "var scored=volScore(d);\n          if(scored<d.length*0.2 && i<urls.length-1) continue;"
    news = "var scored=volScore(d);\n          if(!raw.warehouse_key && scored<d.length*0.2 && i<urls.length-1) continue;"
    if olds in t:
        t = t.replace(olds, news, 1)
        print("warehouse exempt from volScore skip")
    p.write_text(t)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
