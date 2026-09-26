/* jh-reskin-skip */
/* Read-only display projection: retained measurements never become a fresh score. */
(function (root, factory) {
  'use strict';
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.JHLiquidityLegacyQuality = api;
})(typeof window === 'object' ? window : this, function () {
  'use strict';
  const MAX_AGE_MS = 26 * 3600000;
  function clock(value) {
    if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.test(value)) return null;
    const date = value.slice(0, 10), day = Date.parse(date + 'T00:00:00Z'), stamp = Date.parse(value);
    return Number.isFinite(day) && new Date(day).toISOString().slice(0, 10) === date && Number.isFinite(stamp) ? stamp : null;
  }
  function assess(packet, now = Date.now()) {
    packet = packet && typeof packet === 'object' ? packet : {};
    const supplied = [packet.generated_at, packet.meta?.generated_at, packet.quality?.publication_date].filter(v => v != null);
    const stamps = supplied.map(clock);
    // An invalid or newer wrapper cannot hide an older retained publication.
    const published = stamps.length && stamps.every(v => v !== null) ? Math.min(...stamps) : null;
    const future = stamps.some(v => v !== null && v > now);
    const usable = Number.isFinite(now) && published !== null && !future && now - published <= MAX_AGE_MS && packet.quality?.status === 'fresh';
    return {usable, published_at: published === null ? null : new Date(published).toISOString(),
      reason: published === null ? 'unknown_publication_time' : future ? 'future_publication_time' : now - published > MAX_AGE_MS ? 'expired_publication' : packet.quality?.status !== 'fresh' ? 'source_unavailable' : 'legacy_measurements_only',
      forecast_qualified: false, sizing_eligible: false};
  }
  function project(packet, now = Date.now()) {
    packet = packet && typeof packet === 'object' ? packet : {};
    const quality = assess(packet, now), core = quality.usable ? {...(packet.core || {})} : {};
    core.net_liquidity = {...(quality.usable ? core.net_liquidity || {} : {}),
      value_bn: quality.usable ? core.net_liquidity?.value_bn ?? null : null,
      score: null, label: quality.usable ? 'DESCRIPTIVE' : 'UNAVAILABLE'};
    const view = {...packet, core,
      regime: {trend: 'UNVALIDATED', structure: 'UNVALIDATED', delta_4w_bn: null, delta_13w_bn: null},
      spy_signal: {direction: null, confidence: null, validated: false, basis: 'Descriptive observations do not establish a directional call or position size.'},
      components: {tga_analysis: {signal: 'UNVALIDATED'}, rrp_analysis: {signal: 'UNVALIDATED'}},
      calls_eligible: false, sizing_eligible: false};
    if (!quality.usable) {
      for (const key of ['money_supply', 'dollar', 'yields', 'funding', 'soma', 'reserves', 'catalog', 'part4']) view[key] = {};
    }
    // Chart history remains the retained snapshot, never a silently refreshed series.
    return {view, quality};
  }
  return {MAX_AGE_MS, assess, project};
});
