#!/usr/bin/env python3
from pathlib import Path

def main():
    html = Path("chart.html")
    eng = Path("jh-chart-engine.js")
    if not html.exists():
        root = Path(__file__).resolve().parents[3]
        html, eng = root / "chart.html", root / "jh-chart-engine.js"
    h = html.read_text()
    for tag in [
        '<script src="/jh-chart-vwap.js?v=1"></script>',
        '<script src="/jh-chart-rail.js?v=1"></script>',
    ]:
        if tag.split("?")[0].split("/")[-1] not in h:
            h = h.replace(
                '<script src="/jh-chart-engine.js',
                tag + "\n<script src=\"/jh-chart-engine.js",
                1,
            )
    html.write_text(h)
    t = eng.read_text()
    if "mk.length>3" not in t and "setMarkers(mk)" in t:
        t = t.replace(
            "if(mk&&mk.length)c.setMarkers(mk)",
            "if(mk&&mk.length){mk=mk.slice(-3);c.setMarkers(mk)}",
            1,
        )
        print("capped markers")
    if "jhNyVwap" not in t and "addLine(" in t:
        # best-effort: after main setData candles
        hook = "else c.setData(display);"
        extra = "else c.setData(display);try{if(window.jhNyVwap){var vw=window.jhNyVwap(display);if(vw&&vw.length){var vs=chart.addLineSeries({color:\"#ff6d00\",lineWidth:1,lastValueVisible:true,priceLineVisible:false,title:\"VWAP NY\"});vs.setData(vw);series.push(vs);}}}catch(e){}\n        "
        if hook in t:
            t = t.replace(hook, extra, 1)
            print("vwap line")
    eng.write_text(t)
    print("patched")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
