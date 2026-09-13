#!/usr/bin/env python3
from pathlib import Path

def main():
    p = Path("jh-chart-engine.js")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "jh-chart-engine.js"
    t = p.read_text()
    dirty = "volume:+(b.volume||b.v||b.value||b.vol||0)"
    clean = "volume:+(b.volume||b.v||b.vol||b.Volume||0)"
    if dirty in t:
        t = t.replace(dirty, clean, 1)
        print("stopped treating price value as volume")
    elif clean in t:
        print("volume field already clean")
    else:
        print("volume assign drifted — skip")
    oldm = "chart.priceScale(\"vol\").applyOptions({ scaleMargins:{ top:0.82, bottom:0 } });"
    newm = "chart.priceScale(\"vol\").applyOptions({ scaleMargins:{ top:0.74, bottom:0 } });"
    if oldm in t:
        t = t.replace(oldm, newm, 1)
        print("taller volume pane")
    oldc = "c=chart.addCandlestickSeries({ upColor: kind===\"hollow\"?BG:UP, downColor:DN, borderUpColor:UP, borderDownColor:DN, wickUpColor:UP, wickDownColor:DN });"
    newc = "c=chart.addCandlestickSeries({ upColor: kind===\"hollow\"?BG:UP, downColor:DN, borderVisible:true, borderUpColor:UP, borderDownColor:DN, wickVisible:true, wickUpColor:UP, wickDownColor:DN });"
    if oldc in t:
        t = t.replace(oldc, newc, 1)
        print("explicit wicks/borders")
    p.write_text(t)
    html = Path("chart.html") if Path("chart.html").exists() else Path(__file__).resolve().parents[3] / "chart.html"
    if html.exists():
        h = html.read_text()
        tag = '<script src="/jh-chart-quality.js?v=1"></script>'
        if tag not in h:
            if "jh-chart-stock-desk.js" in h:
                h = h.replace(
                    '<script src="/jh-chart-stock-desk.js?v=1"></script>',
                    '<script src="/jh-chart-stock-desk.js?v=1"></script>\n<script src="/jh-chart-quality.js?v=1"></script>',
                )
            else:
                h = h.replace("</body>", tag + "\n</body>")
            html.write_text(h)
            print("quality script hooked")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
