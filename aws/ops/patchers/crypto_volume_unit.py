from pathlib import Path

FN = '''
export function alignCryptoVolume(bars) {
  if (!Array.isArray(bars) || bars.length < 40) return bars;
  function med(i, n) {
    const s = [];
    for (let k = i; k < i + n && k < bars.length; k++) {
      const b = bars[k];
      if (b && b.close > 0 && b.value > 0) s.push(b.value / b.close);
    }
    if (s.length < 10) return null;
    s.sort((a, b) => a - b);
    return s[s.length >> 1];
  }
  let cut = -1;
  for (let i = 30; i < bars.length - 20; i++) {
    const before = med(i - 30, 30);
    const after = med(i, 20);
    if (before > 1000 && after != null && after < 100) { cut = i; break; }
  }
  if (cut < 0) return bars;
  return bars.map((b, i) => {
    if (i < cut || !b || !(b.close > 0) || !(b.value > 0) || b.value / b.close >= 100) return b;
    return Object.assign({}, b, { value: b.value * b.close, volume_unit: "quote" });
  });
}
'''

ohlc = Path("cloudflare/workers/justhodl-data-proxy/src/warehouse-ohlc.js")
text = ohlc.read_text()
if "function alignCryptoVolume" not in text:
    ohlc.write_text(text.rstrip() + "\n" + FN)
idx = Path("cloudflare/workers/justhodl-data-proxy/src/index.js")
src = idx.read_text()
src2 = src.replace(
    "mergeBarsPrefer, yahooResultToBars",
    "mergeBarsPrefer, alignCryptoVolume, yahooResultToBars",
    1,
)
src2 = src2.replace(
    "const merged = mergeBarsPrefer(hist, warm.bars);",
    "const merged = alignCryptoVolume(mergeBarsPrefer(hist, warm.bars));",
    1,
)
if src2 != src:
    idx.write_text(src2)
print("crypto volume unit patch applied")
