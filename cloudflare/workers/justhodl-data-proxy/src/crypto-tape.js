/* Same-coin rule and today's forming bar for the banked crypto tape.
 *
 * Khalid 2026-11-21: "i cant pull btc on chart ... i want all my crypto and stocks and etfs to refresh for my data to stay accurate".
 * The banked daily bars (katlin, Polygon X:<COIN>USD - Coinbase's own candles to the cent, ops 6881) end at the last completed UTC day;
 * index.js joins an outside daily history onto them for the older years and for today. Measured on all 93 banked coins (ops 6881):
 * the outside ticker of the same letters is ANOTHER coin for ARB, TON, AERO and JUP (closes 99% away) - their older "history" and a
 * 99% lower bar for today were being joined onto the tape - and 22 coins got no outside bars at all, so their tape ended yesterday.
 *
 * Nothing here fetches. Pure functions so the rule can be tested bar by bar.
 */
import { utcDay, aggregateBars } from './warehouse-ohlc.js';

export const SAME_COIN = { min_days: 5, window_days: 30, max_median_distance: 0.03 };

function close(row) {
  const c = row && Number(row.close);
  return Number.isFinite(c) && c > 0 ? c : 0;
}

/* Is `outside` the same coin as `banked`? On the newest `window_days` UTC days both carry - at least `min_days` of them - the
 * median distance between closes must be within `max_median_distance`. Venues of one coin sit well inside 1% on a daily close;
 * another coin under the same letters sits tens of percent away. Too few shared days is "cannot tell", which is a no.
 * A bank younger than `min_days` has nothing to judge by and is joined as it always was (`unjudged`); the banked coins hold
 * years of days (the shortest of the 40 measured in ops 6880 holds 983). */
export function sameCoinDaily(outside, banked, rule = SAME_COIN) {
  const bank = new Map();
  for (const row of banked || []) {
    const t = utcDay(row && row.time), c = close(row);
    if (t && c) bank.set(t, c);
  }
  if (bank.size < rule.min_days) return { same: true, days: 0, median: null, unjudged: true };
  const seen = new Set(), distance = [];
  const rows = (outside || []).filter(r => r && utcDay(r.time)).sort((a, b) => utcDay(b.time) - utcDay(a.time));
  for (const row of rows) {
    const t = utcDay(row.time), c = close(row);
    if (!c || seen.has(t) || !bank.has(t)) continue;
    seen.add(t);
    distance.push(Math.abs(c / bank.get(t) - 1));
    if (distance.length >= rule.window_days) break;
  }
  if (distance.length < rule.min_days) return { same: false, days: distance.length, median: null };
  distance.sort((a, b) => a - b);
  const median = distance[Math.floor(distance.length / 2)];
  return { same: median <= rule.max_median_distance, days: distance.length, median };
}

/* BTC-USD, BTCUSD, BTCUSDT, BTCUSDC -> the Coinbase Exchange product BTC-USD. */
export function coinbaseProduct(ticker) {
  const t = String(ticker || "").trim().toUpperCase();
  let base = "";
  if (/^[A-Z0-9]{1,10}-USD$/.test(t)) base = t.slice(0, -4);
  else if (/(USDT|USDC|BUSD)$/.test(t)) base = t.slice(0, -4);
  else if (/USD$/.test(t)) base = t.slice(0, -3);
  return /^[A-Z0-9]{1,10}$/.test(base) ? base + "-USD" : "";
}

/* Coinbase Exchange candles are [time, low, high, open, close, volume], newest first. */
export function coinbaseCandlesToBars(rows) {
  const bars = [];
  for (const r of Array.isArray(rows) ? rows : []) {
    if (!Array.isArray(r) || r.length < 6) continue;
    const t = Number(r[0]), low = Number(r[1]), high = Number(r[2]), open = Number(r[3]), c = Number(r[4]), v = Number(r[5]);
    if (!Number.isFinite(t) || t <= 0 || ![low, high, open, c].every(x => Number.isFinite(x) && x > 0)) continue;
    bars.push({ time: Math.floor(t), open, high, low, close: c, value: Number.isFinite(v) && v >= 0 ? v : null });
  }
  return bars.sort((a, b) => a.time - b.time);
}

/* The days after the bank's last completed day, up to today (UTC), from a same-coin venue: `pack.bars` up to the bank's last
 * day stay as they are, anything later is replaced by the venue's rows. Returns null when the venue adds nothing. */
export function formingBars(pack, venueRows, bankLastDay, today) {
  const add = new Map();
  for (const row of venueRows || []) {
    const t = utcDay(row && row.time);
    if (!t || t <= bankLastDay || t > today || !close(row)) continue;
    add.set(t, { time: t, open: row.open, high: row.high, low: row.low, close: row.close, value: row.value == null ? null : row.value });
  }
  if (!add.size) return null;
  const kept = (pack.bars || []).filter(b => utcDay(b && b.time) <= bankLastDay);
  return { bars: kept.concat([...add.values()].sort((a, b) => a.time - b.time)), added: add.size };
}

export function utcToday(nowMs) {
  return utcDay(Math.floor(Number(nowMs) / 1000));
}

export function bankLastDay(bars) {
  let last = 0;
  for (const row of bars || []) {
    const t = utcDay(row && row.time);
    if (t > last) last = t;
  }
  return last;
}

/* ---------------------------------------------------------------- intraday bars for a banked coin
 * The Polygon plan carries no crypto intraday (ops 6880: 403 on every crypto intraday call), so the hourly and minute views of a
 * coin were answered with DAILY bars (ops 6882: "BTC-USD at 1h: spacing 1440 min"). Intraday bars come from a venue's public
 * klines, and only after that venue's daily candles pass the same-coin rule against the bank. */
const BINANCE_STEPS = { minute: [30, 15, 5, 3, 1], hour: [12, 8, 6, 4, 2, 1] };
const COINBASE_STEPS = { minute: [15, 5, 1], hour: [6, 1] };

/* Which interval each venue is asked for: the largest one it publishes that divides the requested bar (45m from 15m, 3h from 1h). */
export function intradayPlan(span, mult) {
  mult = Number(mult);
  if (!BINANCE_STEPS[span] || !Number.isInteger(mult) || mult < 1) return null;
  const unit = span === "minute" ? 60 : 3600, pick = steps => steps.find(s => mult % s === 0);
  const b = pick(BINANCE_STEPS[span]), c = pick(COINBASE_STEPS[span]);
  return {
    span, mult, seconds: unit * mult,
    binance: { interval: b + (span === "minute" ? "m" : "h"), mult: b, seconds: unit * b },
    coinbase: { granularity: unit * c, mult: c, seconds: unit * c }
  };
}

/* How many pages of `perPage` bars cover `days`, within `cap`. */
export function intradayPages(days, sourceSeconds, perPage, cap) {
  const want = Math.ceil((Number(days) > 0 ? Number(days) : 1) * 86400 / (sourceSeconds * perPage));
  return Math.max(1, Math.min(cap, want));
}

/* Venue rows (already {time, open, high, low, close, value}) -> ascending, one per bar start, rebuilt to the requested bar. */
export function intradayBars(rows, plan, venue) {
  const seen = new Map();
  for (const r of rows || []) {
    const t = Number(r && r.time);
    if (Number.isFinite(t) && t > 0 && close(r)) seen.set(Math.floor(t), r);
  }
  const sorted = [...seen.values()].sort((a, b) => a.time - b.time);
  /* `volume` by name: the chart reads a bare `value` as volume only on banked answers. */
  return aggregateBars(sorted, plan.span, plan.mult, plan.span, plan[venue].mult)
    .map(b => ({ time: b.time, open: b.open, high: b.high, low: b.low, close: b.close, volume: b.value == null ? null : b.value }));
}
