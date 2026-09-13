#!/usr/bin/env python3
from pathlib import Path

def main():
    p = Path("chart.html")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "chart.html"
    h = p.read_text()
    h = h.replace("jh-chart-vol-events.js?v=1", "jh-chart-vol-events.js?v=2")
    h = h.replace("jh-chart-tape.js?v=1", "jh-chart-tape.js?v=2")
    p.write_text(h)
    print("cache bust v2")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
