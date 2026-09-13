/* Historical bars come from the warehouse, regardless of their age.
 * A missing interval is not fabricated from daily candles. Vendor requests
 * are permitted only for an empty bank or an explicit forming-session tail.
 */
const DAY = 86400;

export function normalizeBars(rows) {
  const out = new Map();
  for (const r of rows || []) {
    const a = Array.isArray(r) ? r : [r.time ?? r.date ?? r.t, r.open ?? r.o, r.high ?? r.h, r.low ?? r.l, r.close ?? r.c, r.volume ?? r.value ?? r.v];
    let t = typeof a[0] === 'number' ? a[0] : Date.parse(a[0]) / 1000;
    if (t > 1e12) t /= 1000;
    const prices = a.slice(1, 5).map(v => v == null || v === '' ? NaN : Number(v));
    if (!Number.isFinite(t) || prices.length !== 4 || !prices.every(Number.isFinite)) continue;
    out.set(Math.floor(t), {time: Math.floor(t), open: prices[0], high: prices[1], low: prices[2], close: prices[3], value: Number(a[5]) || 0});
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
    if (old) { old.high = Math.max(old.high,b.high); old.low = Math.min(old.low,b.low); old.close = b.close; old.value += b.value; }
    else groups.set(key, {...b, time:key});
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
