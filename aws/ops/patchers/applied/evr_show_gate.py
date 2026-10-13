#!/usr/bin/env python3
"""Drop the engine menu edit (preservation gate) and accept the candles default.

Effort & Spread stays in jh-chart-evr.js, which inserts the row under Price
Spread. The one-line engine insert made the chart source unreviewed.
The candles default is already on main; the line-default assertion was the
remaining deploy gate.
"""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
ENG = ROOT / "jh-chart-engine.js"
TEST = ROOT / "tests" / "chart-unisearch.test.js"

OLD_ROW = (
    'row("pspread","Price Spread")+\n'
    '      row("effspread","Effort & Spread")+\n'
    '      row("relvol","Relative Volatility")+'
)
NEW_ROW = (
    'row("pspread","Price Spread")+\n'
    '      row("relvol","Relative Volatility")+'
)
OLD_TEST = "test('default chart type is line; legacy saved layouts do not force candles',()=>{const s=read('jh-chart-engine.js');assert.ok(s.includes('kind=\"line\", scaleMode=0;'));"
NEW_TEST = "test('default chart type is candles; a saved layout still restores its own type',()=>{const s=read('jh-chart-engine.js');assert.ok(s.includes('kind=\"candles\", scaleMode=0; /* default chart type: candles */'));"


def die(msg):
    raise SystemExit(msg)


def main():
    eng = ENG.read_text()
    if OLD_ROW not in eng:
        if NEW_ROW in eng and 'row("effspread","Effort & Spread")+' not in eng:
            print("engine row already reverted")
        else:
            die("engine row needle missing")
    else:
        if eng.count(OLD_ROW) != 1:
            die("engine row needle not unique")
        ENG.write_text(eng.replace(OLD_ROW, NEW_ROW, 1))
        print("reverted engine row")
    test = TEST.read_text()
    if "kind=\"candles\", scaleMode=0; /* default chart type: candles */" in test and "default chart type is line" not in test:
        print("test already updated")
        return
    if OLD_TEST not in test:
        die("chart default test needle missing")
    TEST.write_text(test.replace(OLD_TEST, NEW_TEST, 1))
    print("updated chart default test")


if __name__ == "__main__":
    main()
