#!/usr/bin/env python3
"""Restore mergeByDay. The volume rule lives on the worker, which every tape reads.

The chart copy of this function is hash-locked by the page tests. Leaving it
edited blocks every later site deploy and does not change the bars.
The replacement must be the pre-guard function plus its single trailing
newline. A blank line changes the file hash and the page tests fail again.
"""
from pathlib import Path

OLD = """  function mergeByDay(base, over){
    var m={}, i, t, b;
    function put(row, prefer){
      t=utcMidnight(row.time); if(!t) return;
      if(prefer || !m[t]) m[t]={time:t,open:row.open,high:row.high,low:row.low,close:row.close,volume:reportedVolume(row.volume)};
    }
    for(i=0;i<(base||[]).length;i++) put(base[i], false);
    for(i=0;i<(over||[]).length;i++) put(over[i], true);
    return Object.keys(m).map(Number).sort(function(a,b){return a-b;}).map(function(k){return m[k];});
  }
"""


def root():
    if Path("jh-chart-engine.js").exists():
        return Path(".")
    return Path(__file__).resolve().parents[3]


def main():
    path = root() / "jh-chart-engine.js"
    text = path.read_text(encoding="utf-8")
    if "volume unit guard: 20x" not in text:
        print("engine merge already restored")
        return
    start = text.find("  function mergeByDay(base, over){")
    end = text.find("  function uniq(", start)
    if start < 0 or end < 0 or not text[end:].startswith("  function uniq("):
        raise SystemExit("mergeByDay bounds missing")
    path.write_text(text[:start] + OLD + text[end:], encoding="utf-8")
    check = path.read_text(encoding="utf-8")
    if "volume unit guard: 20x" in check or OLD not in check:
        raise SystemExit("restore did not land")
    print("engine merge restored")


if __name__ == "__main__":
    main()
