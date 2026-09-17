#!/usr/bin/env python3
"""ops 5626 — insert jh-chart-risk.js before </body> on chart.html."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "chart.html"
TAG = '<script src="/jh-chart-risk.js?v=20260917b"></script>\n'


def main() -> None:
    t = TARGET.read_text()
    if "jh-chart-risk.js" in t:
        print("already wired")
        return
    key = "</body>"
    if key not in t:
        raise SystemExit("no </body>")
    t = t.replace(key, TAG + key, 1)
    TARGET.write_text(t)
    print("wired before </body>", TARGET.stat().st_size)


if __name__ == "__main__":
    main()
