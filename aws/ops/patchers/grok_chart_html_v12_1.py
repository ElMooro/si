#!/usr/bin/env python3
from pathlib import Path

def main():
    p = Path("chart.html")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "chart.html"
    t = p.read_text()
    if "jh-chart-engine.js?v=v12.1" in t and ">v12 QR</span>" in t:
        print("already clean")
        return 0
    t = t.replace("jh-chart-engine.js?v=tv12", "jh-chart-engine.js?v=v12.1")
    t = t.replace("jh-chart-engine.js?v=e01fe1ce", "jh-chart-engine.js?v=v12.1")
    t = t.replace('<span id="cd"></span>', '<span id="cd">v12 QR</span>')
    if "v=v12.1" not in t:
        raise SystemExit("script tag not found")
    p.write_text(t)
    print("patched", p)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
