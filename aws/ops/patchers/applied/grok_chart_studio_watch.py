#!/usr/bin/env python3
from pathlib import Path

CSS = """<style id=jh-studio-css>
#watch{width:28px;min-width:28px;overflow:hidden;transition:width .18s ease;border-left:1px solid var(--bd,#2a2e39)}
#watch:hover,#watch.open,#watch:focus-within{width:320px;min-width:280px;overflow:auto}
#quote .jh-dump{display:none}
</style>"""

def main():
    html = Path("chart.html")
    if not html.exists():
        html = Path(__file__).resolve().parents[3] / "chart.html"
    h = html.read_text()
    if "jh-studio-css" not in h:
        h = h.replace("<head>", "<head>" + CSS, 1)
    if "jh-chart-studio.js" not in h:
        h = h.replace(
            '<script src="/jh-chart-engine.js',
            '<script src="/jh-chart-studio.js?v=1"></script>\n<script src="/jh-chart-engine.js',
            1,
        )
    html.write_text(h)
    print("studio + hover watch")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
