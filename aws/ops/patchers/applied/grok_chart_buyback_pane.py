#!/usr/bin/env python3
"""Wire Buyback pane script on chart.html. Idempotent."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
CHART = ROOT / "chart.html"
TAG = '<script src="/jh-chart-buyback.js?v=20261002-buyback"></script>'
NEEDLE = '<script src="/jh-chart-engine.js?v=20261002ac-shelves"></script>'

def main() -> None:
    t = CHART.read_text()
    if "jh-chart-buyback.js" in t:
        print("chart already wired")
        return
    if NEEDLE not in t:
        raise SystemExit("engine script tag not found")
    CHART.write_text(t.replace(NEEDLE, NEEDLE + "\n" + TAG, 1))
    print("wired jh-chart-buyback.js")

if __name__ == "__main__":
    main()
