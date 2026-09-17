#!/usr/bin/env python3
"""ops 5629 — point chart.html at cache-bust jh-chart-risk-v3.js."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "chart.html"
NEW = '<script src="/jh-chart-risk-v3.js?v=5629"></script>'


def main() -> None:
    t = TARGET.read_text()
    if "jh-chart-risk-v3.js" in t:
        print("already v3")
        return
    if "jh-chart-risk.js" in t:
        import re
        t2, n = re.subn(r'<script src="/jh-chart-risk\.js[^"']*"></script>', NEW, t, count=1)
        if n != 1:
            raise SystemExit("risk script tag not replaced")
        TARGET.write_text(t2)
        print("retargeted to v3")
        return
    if "</body>" not in t:
        raise SystemExit("no body")
    TARGET.write_text(t.replace("</body>", NEW + "\n</body>", 1))
    print("inserted v3 before body")


if __name__ == "__main__":
    main()
