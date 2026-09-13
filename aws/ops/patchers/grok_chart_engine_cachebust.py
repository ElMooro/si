#!/usr/bin/env python3
from pathlib import Path

def main():
    p = Path("chart.html")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "chart.html"
    h = p.read_text()
    h2 = h.replace("jh-chart-engine.js?v=v12.1", "jh-chart-engine.js?v=v12.2")
    h2 = h2.replace("jh-chart-engine.js?v=v12.2.1", "jh-chart-engine.js?v=v12.2")
    if h2 == h:
        print("no engine tag change", "engine.js" in h)
    else:
        p.write_text(h2)
        print("engine query v12.2")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
