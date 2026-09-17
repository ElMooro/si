#!/usr/bin/env python3
"""ops 5627 — load jh-chart-risk.js on chart-pro.html."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "chart-pro.html"
TAG = '<script src="/jh-chart-risk.js?v=20260917c"></script>\n'


def main() -> None:
    t = TARGET.read_text()
    if "jh-chart-risk.js" in t:
        print("already wired")
        return
    if "</body>" not in t:
        raise SystemExit("no body")
    TARGET.write_text(t.replace("</body>", TAG + "</body>", 1))
    print("wired chart-pro.html")


if __name__ == "__main__":
    main()
