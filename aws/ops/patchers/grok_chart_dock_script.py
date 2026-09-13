#!/usr/bin/env python3
from pathlib import Path

def main():
    p = Path("chart.html")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "chart.html"
    h = p.read_text()
    tag = '<script src="/jh-chart-dock.js?v=1"></script>'
    if "jh-chart-dock.js" not in h:
        h = h.replace(
            '<script src="/jh-chart-engine.js',
            tag + '\n<script src="/jh-chart-engine.js',
            1,
        )
        p.write_text(h)
        print("dock script")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
