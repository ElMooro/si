#!/usr/bin/env python3
from pathlib import Path

def main():
    html = Path("chart.html")
    eng = Path("jh-chart-engine.js")
    if not html.exists():
        root = Path(__file__).resolve().parents[3]
        html, eng = root / "chart.html", root / "jh-chart-engine.js"
    h = html.read_text()
    tag = '<script src="/jh-chart-rs.js?v=1"></script>'
    if tag not in h and "jh-chart-rs.js" not in h:
        if "jh-chart-tape.js" in h:
            h = h.replace(
                '<script src="/jh-chart-tape.js?v=2"></script>',
                '<script src="/jh-chart-tape.js?v=3"></script>\n' + tag,
            )
            if "jh-chart-rs.js" not in h:
                h = h.replace(
                    '<script src="/jh-chart-tape.js?v=1"></script>',
                    '<script src="/jh-chart-tape.js?v=3"></script>\n' + tag,
                )
        if "jh-chart-rs.js" not in h:
            h = h.replace('<script src="/jh-chart-engine.js', tag + '\n<script src="/jh-chart-engine.js', 1)
        html.write_text(h)
        print("html rs")
    t = eng.read_text()
    extra = "try{if(window.jhRsReady){window.jhRsReady(display).then(function(rs){if(rs&&rs.length&&c.setMarkers){var cur=c.markers||[];try{c.setMarkers((window.jhTapeRead?((window.jhTapeRead(display).markers)||[]):[]).concat(rs));}catch(e2){}}});}}catch(e){}"
    if "jhRsReady" in t:
        print("engine already rs")
    elif "jhTapeRead" in t:
        # append after existing tape try block by unique string
        needle = "if(out&&out.panel)"
        if needle in t:
            t = t.replace(
                "if(out&&out.panel)",
                extra + "if(out&&out.panel)",
                1,
            )
            eng.write_text(t)
            print("rs hook")
        else:
            print("no panel hook")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
