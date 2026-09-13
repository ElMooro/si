#!/usr/bin/env python3
from pathlib import Path

def main():
    p = Path("chart.html")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "chart.html"
    h = p.read_text()
    tag = '<script src="/jh-chart-vol-events.js?v=1"></script>'
    h = h.replace(tag + "\n", "")
    h = h.replace(tag, "")
    h = h.replace(
        '<script src="/jh-chart-engine.js',
        tag + '\n<script src="/jh-chart-engine.js',
        1,
    )
    p.write_text(h)
    print("vol-events before engine")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
