#!/usr/bin/env python3
from pathlib import Path

def main():
    html = Path("chart.html")
    eng = Path("jh-chart-engine.js")
    if not html.exists():
        root = Path(__file__).resolve().parents[3]
        html, eng = root / "chart.html", root / "jh-chart-engine.js"
    h = html.read_text()
    tag = '<script src="/jh-chart-tape.js?v=1"></script>'
    if tag not in h:
        h = h.replace(
            '<script src="/jh-chart-vol-events.js?v=1"></script>',
            '<script src="/jh-chart-vol-events.js?v=1"></script>\n' + tag,
        )
        if tag not in h:
            h = h.replace(
                '<script src="/jh-chart-engine.js',
                tag + '\n<script src="/jh-chart-engine.js',
                1,
            )
        html.write_text(h)
        print("tape script")
    t = eng.read_text()
    old = "if(window.jhVolEvents){var mk=window.jhVolEvents(display);if(mk&&mk.length)c.setMarkers(mk);}"
    new = "var fn=window.jhTapeRead||window.jhVolEvents;if(fn){var out=fn(display);var mk=out&&out.markers?out.markers:out;if(mk&&mk.length)c.setMarkers(mk);if(out&&out.panel){var q=document.getElementById(\"quote\"); if(q&&!/LIVERMORE/.test(q.textContent)){var n=document.createElement(\"div\");n.style.cssText=\"flex-basis:100%;font-size:10px;color:var(--mut)\";n.textContent=out.panel;q.appendChild(n);}}}"
    if "jhTapeRead" in t and "out.panel" in t:
        print("engine already")
    elif old in t:
        t = t.replace(old, new, 1)
        eng.write_text(t)
        print("engine uses tape read")
    else:
        print("marker hook drifted")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
