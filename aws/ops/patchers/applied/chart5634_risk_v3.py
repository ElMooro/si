#!/usr/bin/env python3
from pathlib import Path
import re
ROOT = Path(__file__).resolve().parents[3]
TARGET = ROOT / "chart.html"
NEW = '<script src="/jh-chart-risk-v3.js?v=5634"></script>'

def main():
    t = TARGET.read_text()
    if "jh-chart-risk-v3.js" in t:
        print("already v3"); return
    t2, n = re.subn(r'<script src="/jh-chart-risk[^"\']*"></script>', NEW, t, count=1)
    if n != 1:
        if "</body>" in t:
            TARGET.write_text(t.replace("</body>", NEW+"\n</body>", 1))
            print("inserted before body"); return
        raise SystemExit("no risk tag and no body")
    TARGET.write_text(t2); print("retargeted chart.html")
if __name__ == "__main__":
    main()
