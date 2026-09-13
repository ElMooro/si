#!/usr/bin/env python3
from pathlib import Path

def main():
    html = Path("chart.html")
    if not html.exists():
        html = Path(__file__).resolve().parents[3] / "chart.html"
    h = html.read_text()
    if "jh-chart-ai-desk.js" not in h:
        h = h.replace(
            '<script src="/jh-chart-macro-intel.js?v=1"></script>',
            '<script src="/jh-chart-macro-intel.js?v=1"></script>\n<script src="/jh-chart-ai-desk.js?v=1"></script>',
        )
        if "jh-chart-ai-desk.js" not in h:
            h = h.replace(
                '<script src="/jh-chart-engine.js',
                '<script src="/jh-chart-ai-desk.js?v=1"></script>\n<script src="/jh-chart-engine.js',
                1,
            )
        html.write_text(h)
        print("hooked")
    else:
        print("already")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
