#!/usr/bin/env python3
"""Publish the host chart and candle series for the desk overlay.

LightweightCharts.createChart is not writable, so a wrap in vwap.js never
installs. The engine already owns the chart. Expose it.
"""
from pathlib import Path

def main():
    p = Path("jh-chart-engine.js")
    if not p.exists():
        p = Path(__file__).resolve().parents[3] / "jh-chart-engine.js"
    t = p.read_text(encoding="utf-8")
    if "window.jhDeskChart=chart" in t and "window.jhDeskSeries=c" in t:
        print("already clean")
        return 0
    old_chart = "chart=mkChart(host);"
    new_chart = "chart=mkChart(host); try{window.jhDeskChart=chart;}catch(e){}"
    if old_chart not in t:
        raise SystemExit("mkChart assign drifted")
    if t.count(old_chart) != 1:
        raise SystemExit("mkChart assign not unique")
    old_series = "title:active,\n          priceFormat:pxF\n        });"
    new_series = "title:active,\n          priceFormat:pxF\n        }); try{window.jhDeskSeries=c;}catch(e){}"
    if old_series not in t:
        raise SystemExit("candle series assign drifted")
    if t.count(old_series) != 1:
        raise SystemExit("candle series assign not unique")
    t = t.replace(old_chart, new_chart, 1).replace(old_series, new_series, 1)
    p.write_text(t, encoding="utf-8")
    print("exposed desk chart")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
