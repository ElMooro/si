#!/usr/bin/env python3
"""ops 5625 — load jh-chart-risk.js on chart.html."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "chart.html"
NEEDLE = '<script src="/jh-chart-quality.js?v=20260915ae-qx"></script>'
INSERT = NEEDLE + '\n<script src="/jh-chart-risk.js?v=20260917a"></script>'


def main() -> None:
    t = TARGET.read_text()
    if "jh-chart-risk.js" in t:
        print("already wired")
        return
    if NEEDLE not in t:
        raise SystemExit("quality script tag not found")
    TARGET.write_text(t.replace(NEEDLE, INSERT, 1))
    print("wired jh-chart-risk.js")


if __name__ == "__main__":
    main()
