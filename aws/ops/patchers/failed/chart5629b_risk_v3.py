#!/usr/bin/env python3
"""ops 5629b — point chart.html at jh-chart-risk-v3.js."""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "chart.html"
NEW = '<script src="/jh-chart-risk-v3.js?v=5629"></script>'
PAT = re.compile(r"<script src=\"/jh-chart-risk\.js[^"]*\"></script>")


def main() -> None:
    t = TARGET.read_text()
    if "jh-chart-risk-v3.js" in t:
        print("already v3")
        return
    if PAT.search(t):
        TARGET.write_text(PAT.sub(NEW, t, count=1))
        print("retargeted to v3")
        return
    if "</body>" not in t:
        raise SystemExit("no body")
    TARGET.write_text(t.replace("</body>", NEW + "\n</body>", 1))
    print("inserted v3 before body")


if __name__ == "__main__":
    main()
