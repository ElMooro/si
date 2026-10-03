#!/usr/bin/env python3
"""Revert the unreviewed worker volume edit so Pages can deploy the chart fix."""
from pathlib import Path

def root():
    p = Path("cloudflare/workers/justhodl-data-proxy/src/index.js")
    if p.exists():
        return Path(".")
    return Path(__file__).resolve().parents[3]

def main():
    base = root()
    idx = base / "cloudflare/workers/justhodl-data-proxy/src/index.js"
    wh = base / "cloudflare/workers/justhodl-data-proxy/src/warehouse-ohlc.js"
    t = idx.read_text(encoding="utf-8")
    t2 = t.replace(", alignCryptoVolume", "", 1)
    t2 = t2.replace("alignCryptoVolume(mergeBarsPrefer(hist, warm.bars))", "mergeBarsPrefer(hist, warm.bars)", 1)
    if t2 == t:
        print("index already clean")
    else:
        idx.write_text(t2, encoding="utf-8")
        print("index reverted")
    w = wh.read_text(encoding="utf-8")
    start = w.find("\nexport function alignCryptoVolume")
    if start < 0:
        print("warehouse already clean")
        return 0
    wh.write_text(w[:start].rstrip() + "\n", encoding="utf-8")
    print("warehouse reverted")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
