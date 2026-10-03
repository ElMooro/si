/* Historical bars come from the warehouse, regardless of their age.
 * A missing interval is not fabricated from daily candles. Vendor requests
 * are permitted only for an empty bank or an explicit forming-session tail.
 */
const DAY = 86400;

// Typed quantities stay distinct from missing, invalid and conflicting aliases.
function measuredVolume(value) {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null;
}

function rowVolume(row) {
  if (!row || typeof row !== 'object') return null;
  if (Array.isArray(row)) return measuredVolume(row[5]);
  let value = null, seen = false;
  for (const name of ['volume', 'value', 'v', 'vol', 'Volume']) {
    if (!Object.prototype.hasOwnProperty.call(row, name)) continue;
    const next = measuredVolume(row[name]);
    if (next === null || (seen && next !== value)) return null;
    value = next; seen = true;
  }
  return seen ? value : null;
}

function sumVolume(left, right) {
  left = measuredVolume(left); right = measuredVolume(right);
  if (left === null || right === null) return null;
  const sum = left + right;
  return Number.isFinite(sum) && !(left > 0 && right > 0 && (sum === left || sum === right)) ? sum : null;
}

// Binance's documented kline tuple places a decimal string at ordinal 5.
// Decimal decoding is local to this codec; blanks/booleans are never quantities.
function binanceVolume(value) {
  if (typeof value === 'number') return measuredVolume(value);
  if (typeof value !== 'string' || !/^[0-9]+(?:\.[0-9]+)?$/.test(value)) return null;
  const number = Number(value);
  return Number.isFinite(number) && !(number === 0 && /[1-9]/.test(value)) ? number : null;
}

export function normalizeBars(rows) {
  const out = new Map();
  for (const r of rows || []) {
    if (!r || typeof r !== 'object') continue;
    const a = Array.isArray(r) ? r : [r.time ?? r.date ?? r.t, r.open ?? r.o, r.high ?? r.h, r.low ?? r.l, r.close ?? r.c, r.volume ?? r.value ?? r.v];
    let t = typeof a[0] === 'number' ? a[0] : Date.parse(a[0]) / 1000;
    if (t > 1e12) t /= 1000;
    const prices = a.slice(1, 5).map(v => v == null || v === '' ? NaN : Number(v));
    if (!Number.isFinite(t) || prices.length !== 4 || !prices.every(Number.isFinite)) continue;
    out.set(Math.floor(t), {time: Math.floor(t), open: prices[0], high: prices[1], low: prices[2], close: prices[3], value: rowVolume(r)});
  }
  return [...out.values()].sort((a, b) => a.time - b.time);
}

export function aggregateBars(bars, span, mult = 1, sourceSpan = 'day', sourceMult = 1) {
  const rank = {minute: 1, hour: 2, day: 3, week: 4, month: 5};
  if (!rank[span] || !rank[sourceSpan] || rank[span] < rank[sourceSpan]) return [];
  if (span === sourceSpan && (mult < sourceMult || mult % sourceMult)) return [];
  // Weeks cannot be split into calendar months, nor arbitrary coarser bars
  // split across a requested boundary. Daily banks can form weeks/months.
  if (sourceSpan === 'week' && span === 'month') return [];
  if (sourceMult > 1 && span !== sourceSpan) return [];
  if (span === sourceSpan && mult === sourceMult) return bars;
  const groups = new Map();
  for (const b of bars) {
    let key;
    if (span === 'month') {
      const d = new Date(b.time * 1000), m = Math.floor((d.getUTCFullYear()*12+d.getUTCMonth())/mult)*mult;
      key = Date.UTC(Math.floor(m/12), m%12, 1)/1000;
    } else {
      const width = ({minute:60, hour:3600, day:DAY, week:7*DAY})[span]*mult;
      const offset = span === 'week' ? 4*DAY : 0; // Monday, Jan 5 1970
      key = Math.floor((b.time-offset)/width)*width+offset;
    }
    const old = groups.get(key);
    if (old) { old.high = Math.max(old.high,b.high); old.low = Math.min(old.low,b.low); old.close = b.close; old.value = sumVolume(old.value, b.value); }
    else groups.set(key, {...b, time:key, value:measuredVolume(b.value)});
  }
  return [...groups.values()];
}

export async function warehouseOHLC(base, symbol, span, mult = 1, configuredEndpoint = '') {
  let endpoint = configuredEndpoint;
  if (!endpoint) {
    const ep = await fetch(base+'/data/symdir/endpoint.json', {cf:{cacheTtl:300,cacheEverything:true}});
    if (!ep.ok) throw new Error('warehouse endpoint unavailable');
    const doc = await ep.json();
    endpoint = doc.url || doc.function_url || doc.endpoint || '';
  }
  if (!endpoint) throw new Error('warehouse endpoint missing');
  const r = await fetch(endpoint.replace(/\/$/,'')+'/warehouse-ohlc?symbol='+encodeURIComponent(symbol)+'&span='+span+'&mult='+mult, {cf:{cacheTtl:60,cacheEverything:true}});
  if (!r.ok) throw new Error('warehouse lookup failed');
  const doc = await r.json();
  if (doc.warehouse_empty === true) return null;
  if (doc.warehouse_empty !== false || !doc.warehouse_key) throw new Error('invalid warehouse receipt');
  const bars = aggregateBars(normalizeBars(doc.bars),span,mult,doc.source_span,doc.source_mult);
  if (!bars.length) throw new Error('invalid banked bars');
  return {bars,source:'warehouse',warehouse_key:doc.warehouse_key,last_modified:doc.last_modified,span,mult};
}

export function formingSession(now = new Date()) {
  const p = Object.fromEntries(new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',weekday:'short',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(now).map(x=>[x.type,x.value]));
  const minutes = Number(p.hour)*60+Number(p.minute);
  return !['Sat','Sun'].includes(p.weekday) && minutes >= 570 && minutes <= 960;
}

export function utcDay(t) {
  t = Number(t) || 0;
  if (t > 1e12) t = Math.floor(t / 1000);
  if (!Number.isFinite(t) || t <= 0) return 0;
  return Math.floor(t / 86400) * 86400;
}

export function yahooChartSymbol(ticker) {
  const t = String(ticker || "").trim().toUpperCase();
  if (/USDT$/.test(t)) return t.slice(0, -4) + "-USD";
  if (/BUSD$/.test(t)) return t.slice(0, -4) + "-USD";
  if (/USDC$/.test(t)) return t.slice(0, -4) + "-USD";
  if (t === "BTCUSD") return "BTC-USD";
  if (t === "ETHUSD") return "ETH-USD";
  return t;
}

export function binanceSymbol(ticker) {
  const t = String(ticker || "").trim().toUpperCase();
  if (/(USDT|BUSD|USDC)$/.test(t)) return t.replace(/BUSD$|USDC$/, "USDT");
  if (/^[A-Z0-9]{2,10}-USD$/.test(t)) return t.replace("-USD", "USDT");
  if (t === "BTCUSD") return "BTCUSDT";
  if (t === "ETHUSD") return "ETHUSDT";
  return t;
}

export function isCryptoWarehouse(key) {
  return /crypto-bars/i.test(String(key || ""));
}

/* newer wins on the same UTC day. Use Yahoo (or Binance) as `older` and the
 * katlin Polygon bank as `newer` so 2020+ prints stay on the warehouse tape
 * and 2014–2020 (Yahoo BTC-USD) fills the hole. Equities never call this. */
export function mergeBarsPrefer(older, newer) {
  const m = new Map();
  function put(row) {
    const t = utcDay(row && row.time);
    if (!t) return;
    m.set(t, {
      time: t,
      open: row.open, high: row.high, low: row.low, close: row.close,
      value: rowVolume(row)
    });
  }
  for (const row of older || []) {
    const t = utcDay(row && row.time);
    if (!t) continue;
    const vol = rowVolume(row);
    m.set(t, {
      time: t,
      open: row.open, high: row.high, low: row.low, close: row.close,
      value: vol
    });
  }
  for (const row of newer || []) put(row);
  return [...m.values()].sort((a, b) => a.time - b.time);
}

export function yahooResultToBars(data) {
  const res = data && data.chart && data.chart.result && data.chart.result[0];
  const ts = (res && res.timestamp) || [];
  const q = (res && res.indicators && res.indicators.quote && res.indicators.quote[0]) || {};
  const bars = [];
  for (let i = 0; i < ts.length; i++) {
    if (q.close && q.close[i] != null && q.open && q.open[i] != null) {
      bars.push({
        time: ts[i],
        open: q.open[i], high: q.high[i], low: q.low[i], close: q.close[i],
        value: measuredVolume(q.volume && q.volume[i])
      });
    }
  }
  return bars;
}

export function binanceKlinesToBars(rows) {
  const bars = [];
  for (const b of rows || []) {
    if (!b || b[0] == null || b[4] == null) continue;
    const t = Math.floor(Number(b[0]) / 1000);
    if (!Number.isFinite(t) || t <= 0) continue;
    bars.push({
      time: t,
      open: +b[1], high: +b[2], low: +b[3], close: +b[4],
      value: binanceVolume(b[5])
    });
  }
  return bars;
}

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
