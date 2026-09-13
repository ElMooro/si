#!/usr/bin/env python3
from pathlib import Path

def main():
    eng = Path("jh-chart-engine.js")
    html = Path("chart.html")
    if not eng.exists():
        root = Path(__file__).resolve().parents[3]
        eng, html = root / "jh-chart-engine.js", root / "chart.html"
    t = eng.read_text()
    hook = "else c.setData(display);"
    add = (
        "else c.setData(display);"
        "try{if(window.jhVolEvents){var mk=window.jhVolEvents(display);if(mk&&mk.length)c.setMarkers(mk);}}catch(e){}"
    )
    if "jhVolEvents" in t and "setMarkers" in t:
        print("markers already")
    elif hook not in t:
        raise SystemExit("setData hook missing")
    else:
        t = t.replace(hook, add, 1)
        eng.write_text(t)
        print("markers hooked")
    if html.exists():
        h = html.read_text()
        tag = '<script src="/jh-chart-vol-events.js?v=1"></script>'
        if tag not in h:
            if "jh-chart-quality.js" in h:
                h = h.replace(
                    '<script src="/jh-chart-quality.js?v=1"></script>',
                    '<script src="/jh-chart-vol-events.js?v=1"></script>\n<script src="/jh-chart-quality.js?v=1"></script>',
                )
            else:
                h = h.replace("</body>", tag + "\n</body>")
            html.write_text(h)
            print("html script")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
