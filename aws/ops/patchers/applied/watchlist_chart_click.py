#!/usr/bin/env python3
"""Point watchlist clicks at a chart ticker from the symbol map. Does not delete lists."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve()
# runner executes from repo root; this file lives in aws/ops/patchers
ROOT = Path.cwd()
TV = ROOT / "jh-chart-tvwatch.js"
MAN = ROOT / "tests/fixtures/watchlist-correctness/source-transition.json"

OLD = """function openSym(s) {
    var q = document.getElementById("q");
    if (!q) { if (window.jhOpenSymbol) window.jhOpenSymbol(s); return; }
    q.value = s;
    q.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
    paintCard(s);
  }"""

NEW = r"""var symMap = null, symMapReady = 0, symMapReq = null;
  function loadSymMap() {
    if (symMapReq) return symMapReq;
    symMapReq = fetch("/data/symbol-map.json").then(function (r) { return r.ok ? r.json() : null; }).then(function (j) {
      if (!j || !j.map) return;
      var o = Object.create(null);
      Object.keys(j.map).forEach(function (k) { o[String(k).toUpperCase()] = j.map[k]; });
      symMap = o;
    }).catch(function () {}).then(function () { symMapReady = 1; });
    return symMapReq;
  }
  loadSymMap();
  function chartSymbol(s) {
    s = String(s || "").trim();
    if (!s || s.indexOf("###") === 0) return "";
    var u = s.toUpperCase(), hit = symMap && symMap[u];
    if (hit && hit.source === "MARKET" && hit.id && /^[A-Z0-9.^=-]{1,24}$/.test(String(hit.id).toUpperCase())) return String(hit.id).toUpperCase();
    if (hit && hit.source === "FRED" && /^[A-Z0-9]+$/.test(hit.id || "")) return "FRED:" + hit.id;
    if (hit && hit.source === "COINGECKO" && /^[a-z]{2,6}$/.test(hit.id || "")) return String(hit.id).toUpperCase() + "-USD";
    if (hit) return "";
    var venue = "", bare = u, cut = u.indexOf(":");
    if (cut > 0) { venue = u.slice(0, cut); bare = u.slice(cut + 1); }
    var us = { NASDAQ: 1, NYSE: 1, AMEX: 1, ARCA: 1, BATS: 1, IEX: 1, OTC: 1 };
    if (us[venue] && /^[A-Z][A-Z0-9.-]{0,11}$/.test(bare)) return bare;
    if (venue === "CBOE" && bare === "VIX") return "^VIX";
    if (venue === "INDEX") {
      var idx = { SPX: "^GSPC", SP500: "^GSPC", NDX: "^NDX", DJI: "^DJI", RUT: "^RUT", VIX: "^VIX", DXY: "DX-Y.NYB", BTCUSD: "BTC-USD" };
      return idx[bare] || "";
    }
    if (venue === "TVC") {
      var tv = { US10Y: "FRED:DGS10", US02Y: "FRED:DGS2", US05Y: "FRED:DGS5", US30Y: "FRED:DGS30", VIX: "^VIX", DXY: "DX-Y.NYB", GOLD: "GC=F", USOIL: "CL=F", SPX: "^GSPC" };
      return tv[bare] || "";
    }
    if (/^(BINANCE|COINBASE|BITSTAMP|KRAKEN|BYBIT|CRYPTO):/.test(u)) {
      var t = bare.replace(/(USDT|USDC|BUSD)$/, "").replace(/USD$/, "");
      if (/^[A-Z0-9]{2,8}$/.test(t)) return t + "-USD";
      return "";
    }
    if (/^(FX|FX_IDC|OANDA):/.test(u)) {
      var pair = bare.replace(/[^A-Z]/g, "");
      var cc = ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD", "CNY", "CNH", "HKD", "SEK", "NOK", "DKK", "MXN", "ZAR", "SGD"];
      if (pair.length === 6 && cc.indexOf(pair.slice(0, 3)) >= 0 && cc.indexOf(pair.slice(3)) >= 0) return pair + "=X";
      return "";
    }
    if (/^FRED:[A-Z0-9]+$/.test(u)) return u;
    var fut = { ES: "ES=F", NQ: "NQ=F", YM: "YM=F", RTY: "RTY=F", CL: "CL=F", GC: "GC=F", SI: "SI=F", NG: "NG=F", ZN: "ZN=F", ZB: "ZB=F", ZF: "ZF=F", ZT: "ZT=F", HG: "HG=F", "6E": "6E=F", "6J": "6J=F", "6B": "6B=F", "6A": "6A=F" };
    var fm = /^([A-Z0-9]{1,3})1!$/.exec(bare);
    if (fm && fut[fm[1]] && /^(CME|CME_MINI|CBOT|NYMEX|COMEX):/.test(u)) return fut[fm[1]];
    var alias = { VIX: "^VIX", DXY: "DX-Y.NYB", GOLD: "GC=F", USOIL: "CL=F", WTI: "CL=F", SPX: "^GSPC", NDX: "^NDX", RUT: "^RUT", US10Y: "FRED:DGS10", US02Y: "FRED:DGS2", US30Y: "FRED:DGS30", BTCUSD: "BTC-USD", ETHUSD: "ETH-USD", BTC: "BTC-USD", ETH: "ETH-USD" };
    if (!venue && alias[u]) return alias[u];
    if (!venue && /^[A-Z][A-Z0-9.-]{0,11}$/.test(u)) return u;
    return "";
  }
  function openSym(s) {
    if (!symMapReady) { loadSymMap().then(function () { openSym(s); }); return; }
    var chart = chartSymbol(s);
    if (!chart) { toast("No series on this tape for " + s); paintCard(s); return; }
    var q = document.getElementById("q");
    if (q && typeof q.onkeydown === "function") {
      q.value = chart;
      q.onkeydown({ key: "Enter", preventDefault: function () {}, stopPropagation: function () {} });
    } else if (q) {
      q.value = chart;
      q.dispatchEvent(new KeyboardEvent("keydown", { key: "Enter", bubbles: true }));
    } else if (window.jhOpenSymbol && /^[A-Z0-9.-]+$/.test(chart)) window.jhOpenSymbol(chart);
    if (chart !== String(s || "").trim().toUpperCase()) toast(String(s) + " -> " + chart);
    paintCard(s);
  }"""

def main():
    text = TV.read_text(encoding="utf-8")
    if "function chartSymbol(" in text:
        print("already applied")
        return
    if text.count(OLD) != 1:
        raise SystemExit(f"openSym needle count {text.count(OLD)}")
    new_text = text.replace(OLD, NEW, 1)
    start = new_text.find(NEW)
    if start < 0 or new_text.count(NEW) != 1:
        raise SystemExit("replacement did not land once")
    end = start + len(NEW)
    if text != new_text[:start] + OLD + new_text[end:]:
        raise SystemExit("splice does not round-trip to the previous file")
    man = json.loads(MAN.read_text(encoding="utf-8"))
    entry = man["jh-chart-tvwatch.js"]
    old_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    if entry["after_sha256"] != old_hash:
        raise SystemExit(f"manifest after {entry['after_sha256']} != file {old_hash}")
    entry["edits"].append({"start": start, "end": end, "before": OLD, "after": NEW})
    entry["after_sha256"] = hashlib.sha256(new_text.encode("utf-8")).hexdigest()
    TV.write_text(new_text, encoding="utf-8")
    MAN.write_text(json.dumps(man, indent=2) + "\n", encoding="utf-8")
    print("patched", entry["after_sha256"], "bytes", len(new_text.encode()))

if __name__ == "__main__":
    main()
