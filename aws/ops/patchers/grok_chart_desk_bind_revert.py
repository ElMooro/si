#!/usr/bin/env python3
"""Remove the two desk-expose lines. Khalid did not ask for an engine edit."""
from pathlib import Path

def main():
    p = Path("jh-chart-engine.js")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "jh-chart-engine.js"
    t = p.read_text(encoding="utf-8")
    chart = " try{window.jhDeskChart=chart;}catch(e){}"
    series = " try{window.jhDeskSeries=c;}catch(e){}"
    if chart not in t and series not in t:
        print("already clean")
        return 0
    if chart in t:
        t = t.replace(chart, "", 1)
    if series in t:
        t = t.replace(series, "", 1)
    p.write_text(t, encoding="utf-8")
    print("reverted desk expose")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
