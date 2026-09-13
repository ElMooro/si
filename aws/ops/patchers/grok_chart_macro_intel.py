#!/usr/bin/env python3
from pathlib import Path

def main():
    html = Path("chart.html")
    eng = Path("jh-chart-engine.js")
    if not html.exists():
        root = Path(__file__).resolve().parents[3]
        html, eng = root / "chart.html", root / "jh-chart-engine.js"
    h = html.read_text()
    if "jh-chart-macro-intel.js" not in h:
        h = h.replace(
            '<script src="/jh-chart-rs.js?v=1"></script>',
            '<script src="/jh-chart-rs.js?v=1"></script>\n<script src="/jh-chart-macro-intel.js?v=1"></script>',
        )
        if "jh-chart-macro-intel.js" not in h:
            h = h.replace(
                '<script src="/jh-chart-engine.js',
                '<script src="/jh-chart-macro-intel.js?v=1"></script>\n<script src="/jh-chart-engine.js',
                1,
            )
        html.write_text(h)
        print("html hooked")
    t = eng.read_text()
    if "jhMacroIntel" in t:
        print("engine already")
        return 0
    needle = "if(out&&out.panel)"
    extra = "try{if(window.jhMacroIntel){window.jhMacroIntel(display).then(function(mm){});}}catch(e){}"
    if needle in t:
        t = t.replace(needle, extra + needle, 1)
        eng.write_text(t)
        print("engine hook")
    else:
        print("no hook")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
