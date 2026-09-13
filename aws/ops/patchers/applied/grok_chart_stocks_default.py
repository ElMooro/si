#!/usr/bin/env python3
from pathlib import Path

def main():
    eng = Path("jh-chart-engine.js")
    html = Path("chart.html")
    if not eng.exists():
        root = Path(__file__).resolve().parents[3]
        eng, html = root / "jh-chart-engine.js", root / "chart.html"
    t = eng.read_text()
    old = 'var TABS = ["BTCUSDT","ETHB","PEPEUSDT","CNEQ","PURR","BMNR","ATO"];'
    new = 'var TABS = ["SPY","QQQ","IWM","AAPL","MSFT","NVDA","AMZN","META","TSLA","XLE","TLT","GLD"];'
    if old in t:
        t = t.replace(old, new, 1)
    t = t.replace('var active="PEPEUSDT"', 'var active="SPY"', 1)
    eng.write_text(t)
    print("engine tabs -> stocks")
    if html.exists():
        h = html.read_text()
        tag = '<script src="/jh-chart-stock-desk.js?v=1"></script>'
        if tag not in h:
            h = h.replace('<script src="/jh-chart-engine.js?v=v12.1"></script>',
                          '<script src="/jh-chart-engine.js?v=v12.1"></script>\n<script src="/jh-chart-stock-desk.js?v=1"></script>')
            if tag not in h:
                h = h.replace("</body>", tag + "\n</body>")
            html.write_text(h)
            print("html overlay hooked")
        else:
            print("html already hooked")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
