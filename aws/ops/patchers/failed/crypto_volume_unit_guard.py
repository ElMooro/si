#!/usr/bin/env python3
"""Keep one volume unit on every joined tape.

Warehouse price still wins on a shared day. Volume stays with the warehouse
only when it is the same unit as the other tape (within 20x). A larger
disagreement keeps the supplementary volume, so a coin count cannot overwrite
a dollar tape. Equities that already agree are unchanged. Crypto only reaches
the worker join; the chart join uses the same rule for every symbol.
"""
import hashlib
import json
from pathlib import Path


MARKER = "volume unit guard: 20x"
OLD_MERGE = """  function mergeByDay(base, over){
    var m={}, i, t, b;
    function put(row, prefer){
      t=utcMidnight(row.time); if(!t) return;
      if(prefer || !m[t]) m[t]={time:t,open:row.open,high:row.high,low:row.low,close:row.close,volume:reportedVolume(row.volume)};
    }
    for(i=0;i<(base||[]).length;i++) put(base[i], false);
    for(i=0;i<(over||[]).length;i++) put(over[i], true);
    return Object.keys(m).map(Number).sort(function(a,b){return a-b;}).map(function(k){return m[k];});
  }"""
NEW_MERGE = """  function mergeByDay(base, over){
    /* volume unit guard: 20x — price follows the newer row; a different volume unit does not. */
    var m={}, i, t;
    function put(row, prefer){
      t=utcMidnight(row.time); if(!t) return;
      var vol=reportedVolume(row.volume), prev=m[t];
      if(!prev){ m[t]={time:t,open:row.open,high:row.high,low:row.low,close:row.close,volume:vol}; return; }
      if(!prefer) return;
      if(!(prev.volume>0)){ /* base has no volume; the newer figure fills it */ }
      else if(!(vol>0)) vol=prev.volume;
      else { var ratio=prev.volume/vol; if(ratio>=20 || ratio<=0.05) vol=prev.volume; }
      m[t]={time:t,open:row.open,high:row.high,low:row.low,close:row.close,volume:vol};
    }
    for(i=0;i<(base||[]).length;i++) put(base[i], false);
    for(i=0;i<(over||[]).length;i++) put(over[i], true);
    return Object.keys(m).map(Number).sort(function(a,b){return a-b;}).map(function(k){return m[k];});
  }"""
NEW_FN = """async function extendCryptoDaily(ticker, warm) {
  if (!warm || !isCryptoWarehouse(warm.warehouse_key) || (warm.span && warm.span !== \"day\") || (warm.mult && warm.mult !== 1)) return warm;
  let hist = [], historySource = \"yahoo\";
  try { hist = await fetchYahooDaily(ticker); } catch (eY) { hist = []; }
  if (hist.length < 2) {
    historySource = \"binance\";
    try { hist = await fetchBinanceDaily(ticker); } catch (eB) { hist = []; }
  }
  if (hist.length < 2) return warm;
  const merged = mergeBarsPrefer(hist, warm.bars);
  const older = new Map();
  function dayKey(t) {
    t = Number(t) || 0;
    if (t > 1e12) t = Math.floor(t / 1000);
    if (!Number.isFinite(t) || t <= 0) return 0;
    return Math.floor(t / 86400) * 86400;
  }
  function volOf(row) {
    if (!row || typeof row !== \"object\") return null;
    const v = row.value != null ? row.value : row.volume;
    return typeof v === \"number\" && Number.isFinite(v) && v >= 0 ? v : null;
  }
  for (const row of hist) {
    const t = dayKey(row && row.time);
    if (t) older.set(t, volOf(row));
  }
  for (const row of merged) {
    if (!older.has(row.time)) continue;
    const oldVol = older.get(row.time);
    const newVol = volOf(row);
    if (!(oldVol > 0)) continue;
    if (!(newVol > 0) || oldVol / newVol >= 20 || oldVol / newVol <= 0.05) row.value = oldVol;
  }
  if (merged.length <= warm.bars.length) return warm;
  return Object.assign({}, warm, {
    bars: merged,
    source: \"warehouse+\" + historySource,
    history_source: historySource,
    history_n: hist.length,
    history_added_n: merged.length - warm.bars.length,
    history_join_policy: \"Warehouse price wins on overlapping UTC dates. Volume stays on the warehouse row when it is the same unit as the supplementary row (within 20x). A larger disagreement keeps the supplementary volume so a coin count cannot overwrite a dollar tape. Equities are not joined here.\",
    yahoo_n: historySource === \"yahoo\" ? hist.length : 0,
    binance_n: historySource === \"binance\" ? hist.length : 0,
    warehouse_n: warm.bars.length
  });
}"""


def choose(base_vol, over_vol):
    """Mirror the chart rule: base is written first, over supplies the price."""
    if not (base_vol and base_vol > 0):
        return over_vol
    if not (over_vol and over_vol > 0):
        return base_vol
    ratio = base_vol / over_vol
    if ratio >= 20 or ratio <= 0.05:
        return base_vol
    return over_vol


def root():
    if Path("jh-chart-engine.js").exists():
        return Path(".")
    return Path(__file__).resolve().parents[3]


def patch_engine(base):
    path = base / "jh-chart-engine.js"
    text = path.read_text(encoding="utf-8")
    if MARKER in text and OLD_MERGE not in text:
        print("engine already guarded")
        return
    if OLD_MERGE not in text:
        raise SystemExit("engine mergeByDay needle missing")
    if text.count(OLD_MERGE) != 1:
        raise SystemExit("engine mergeByDay needle not unique")
    path.write_text(text.replace(OLD_MERGE, NEW_MERGE, 1), encoding="utf-8")
    print("engine volume guard written")


def patch_worker(base):
    path = base / "cloudflare/workers/justhodl-data-proxy/src/index.js"
    text = path.read_text(encoding="utf-8")
    start = text.find("async function extendCryptoDaily")
    end = text.find("\n// Verify a Stripe webhook", start)
    if start < 0 or end < 0:
        raise SystemExit("extendCryptoDaily bounds missing")
    current = text[start:end]
    body = NEW_FN.rstrip("\n") + ("\n" if current.endswith("\n") else "")
    if current == body:
        print("worker already guarded")
        return
    if not current.startswith("async function extendCryptoDaily"):
        raise SystemExit("extendCryptoDaily start mismatch")
    text = text[:start] + body + text[end:]
    if text.count(body) != 1:
        raise SystemExit("new extendCryptoDaily is not unique")
    fixture = base / "tests/fixtures/worker-crypto-source/transition.json"
    pred_path = base / "tests/fixtures/worker-crypto-source/predecessor/index.js"
    trans = json.loads(fixture.read_text(encoding="utf-8"))
    reversed_src = text.replace(body, trans["before"], 1)
    predecessor = pred_path.read_text(encoding="utf-8")
    if reversed_src != predecessor:
        raise SystemExit("worker preservation reverse does not match predecessor")
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    source = hashlib.sha256(reversed_src.encode("utf-8")).hexdigest()
    if source != trans["source_sha256"]:
        raise SystemExit("predecessor hash drifted")
    trans["after"] = body
    trans["candidate_sha256"] = digest
    path.write_text(text, encoding="utf-8")
    fixture.write_text(json.dumps(trans, indent=2) + "\n", encoding="utf-8")
    print("worker volume guard written", digest)


def main():
    assert choose(40_000_000_000, 100_000) == 40_000_000_000
    assert choose(1_000_000, 1_000_100) == 1_000_100
    assert choose(5_000_000, None) == 5_000_000
    assert choose(None, 80_000) == 80_000
    base = root()
    patch_engine(base)
    patch_worker(base)


if __name__ == "__main__":
    main()
