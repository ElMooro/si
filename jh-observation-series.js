/* Scalar observation history. No provider requests, price inference or trade authority. */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) module.exports = factory();
  else root.JHObservationSeries = factory();
})(typeof window === 'object' ? window : globalThis, function () {
  'use strict';
  var VERSION = 'chart-observations.v1', EA = 'CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX';
  function object(v) { return v !== null && typeof v === 'object' && !Array.isArray(v); }
  function numeric(v) {
    if (typeof v === 'string') {
      if (!/^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/.test(v.trim())) return null;
      var text = v.trim(); v = Number(text);
      // An unrepresentable nonzero decimal is unavailable, never measured zero.
      if (v === 0 && /[1-9]/.test(text.split(/[eE]/)[0])) return null;
    }
    return typeof v === 'number' && Number.isFinite(v) ? v : null;
  }
  function day(y, m, d) {
    if (y < 1 || m < 1 || m > 12 || d < 1 || d > 31) return null;
    var date = new Date(0); date.setUTCFullYear(y, m - 1, d); date.setUTCHours(0, 0, 0, 0);
    return date.getUTCFullYear() === y && date.getUTCMonth() === m - 1 && date.getUTCDate() === d ? date : null;
  }
  function period(raw, frequency) {
    if (typeof raw !== 'string') return null;
    var m = /^(\d{4})-(\d{2})(?:-(\d{2}))?$/.exec(raw);
    if (!m) return null;
    var monthly = !m[3], date = day(+m[1], +m[2], monthly ? 1 : +m[3]);
    if (!date || (frequency === 'D' && monthly) || (frequency === 'M' && !monthly)) return null;
    if (monthly) { date.setUTCMonth(date.getUTCMonth() + 1); date.setUTCDate(0); }
    return { time: date.getTime() / 1000, original_period: raw, period_end: date.toISOString().slice(0, 10),
      clock: monthly ? 'calendar_month_end_UTC_display_coordinate' : 'source_calendar_date_UTC_display_coordinate',
      source_availability_at: null, date_precision: monthly ? 'month' : 'day' };
  }
  function unavailable(doc, requested, path, reason, matches) {
    return { d: [], src: 'Observation history unavailable: ' + reason, evidence: {
      contract: VERSION, requested_id: requested, packet_path: path, status: 'unavailable', reason: reason,
      matches: matches || [], whole_packet: doc, source_freshness_verified: false, source_equivalence_verified: false,
      calls_eligible: false, sizing_eligible: false, records: [], transformations: [], observation_kind: 'scalar' } };
  }
  function exactKeys(map, requested) {
    if (!object(map) || typeof requested !== 'string' || !requested) return [];
    return Object.keys(map).filter(function (key) { return key.toLowerCase() === requested.toLowerCase(); });
  }
  function compile(doc, requested, path, key, row, format, pointer) {
    var result = unavailable(doc, requested, path, null), e = result.evidence;
    e.selected_id = key; e.selected_path = pointer; e.selected_series = row;
    e.source_frequency = typeof row.freq === 'string' ? row.freq : null;
    e.unit = typeof row.unit === 'string' && row.unit.trim() ? row.unit : null;
    e.packet_generated_at = doc.generated_at || null; e.source_acquired_at = row.acquired_at || null;
    e.source_published_at = row.source_published_at || null; e.source_quality = row.quality || null;
    e.points_scope = row.points_scope || 'Received series only; full upstream history is unverified';
    e.volume = { value: null, reason: 'Not reported by this scalar observation series' };
    e.plotting_projection = 'Open/high/low/close are the same scalar, not market OHLC';
    var ds = row.d, vs = row.v, ps = format === 'warehouse' ? row.obs : row.points, n;
    if (format === 'dv' && (!Array.isArray(ds) || !Array.isArray(vs))) { e.reason = 'malformed_parallel_arrays'; return result; }
    if (format !== 'dv' && !Array.isArray(ps)) { e.reason = 'malformed_points'; return result; }
    n = format === 'dv' ? Math.max(ds.length, vs.length) : ps.length;
    var groups = new Map();
    for (var i = 0; i < n; i++) {
      var p = format !== 'dv' ? ps[i] : null;
      var paired = format === 'dv' ? i in ds && i in vs : Array.isArray(p) && p.length >= 2;
      var rawDate = format === 'dv' ? ds[i] : Array.isArray(p) ? p[0] : null;
      var rawValue = format === 'dv' ? vs[i] : Array.isArray(p) ? p[1] : null;
      var stamp = period(rawDate, format === 'warehouse' ? 'D' : e.source_frequency), value = numeric(rawValue);
      var record = { ordinal: i, source_path: pointer + (format === 'dv' ? '.d/.v[' : format === 'warehouse' ? '.obs[' : '.points[') + i + ']',
        raw_period: rawDate === undefined ? null : rawDate, raw_value: rawValue === undefined ? null : rawValue,
        accepted: false, reason: !paired ? 'unpaired_or_malformed_row' : !stamp ? 'invalid_period_or_frequency' : value === null ? 'missing_or_invalid_scalar' : null,
        coordinate: stamp, value: value };
      e.records.push(record);
      if (!record.reason) { var group = groups.get(stamp.time) || []; group.push(record); groups.set(stamp.time, group); }
    }
    Array.from(groups.keys()).sort(function (a, b) { return a - b; }).forEach(function (time) {
      var group = groups.get(time), value = group[0].value;
      if (group.some(function (r) { return r.value !== value; })) {
        group.forEach(function (r) { r.reason = 'conflicting_duplicate_period'; }); return;
      }
      group.forEach(function (r) { r.accepted = true; r.reason = group.length > 1 ? 'equal_duplicate_retained_once' : null; });
      result.d.push({ time: time, open: value, high: value, low: value, close: value, volume: null,
        observation_ordinals: group.map(function (r) { return r.ordinal; }) });
    });
    e.rejected_records = e.records.filter(function (r) { return !r.accepted; }).length;
    e.accepted_records = e.records.length - e.rejected_records; e.plotted_points = result.d.length;
    e.status = result.d.length ? e.rejected_records ? 'partial' : 'observations' : 'unavailable';
    e.reason = result.d.length ? null : 'no_valid_observations';
    e.transformations.push('Strict finite scalar and calendar validation; sorted by period coordinate; equal duplicates plotted once; conflicting duplicates withheld; all records retained');
    result.src = key + ' · ' + result.d.length + ' scalar observations · unit ' + (e.unit || 'unverified') +
      ' · ' + (e.source_frequency || 'frequency unverified') + ' · ' + e.rejected_records + ' excluded records · observation dates, not release times';
    return result;
  }
  function cq(doc, sym) {
    var path = '/data/cryptoquant-series.json', want = String(sym || '').replace(/^CQ:/i, '');
    var keys = exactKeys(doc && doc.series, want);
    if (keys.length !== 1) return unavailable(doc, sym, path, keys.length ? 'ambiguous_series_identity' : 'unknown_series_identity', keys);
    var key = keys[0], row = doc.series[key];
    if (!object(row)) return unavailable(doc, sym, path, 'malformed_series', keys);
    var result = compile(doc, sym, path, key, row, 'dv', 'series[' + JSON.stringify(key) + ']');
    var twins = exactKeys(doc.twins, key);
    result.evidence.proxy_histories = twins.map(function (id) { return { id: id, source_path: 'twins[' + JSON.stringify(id) + ']',
      whole_series: doc.twins[id], joined: false, reason: 'Cross-provider definition, unit and methodology equivalence are unverified' }; });
    if (twins.length) result.src += ' · proxy history retained separately';
    return result;
  }
  function ciss(doc, sym) {
    var path = '/data/ciss-stress.json', want = String(sym || '').replace(/^CISS:/i, '');
    if (!doc || !Array.isArray(doc.series)) return unavailable(doc, sym, path, 'malformed_series_list');
    var alias = want.toLowerCase() === 'ea', hits = [];
    doc.series.forEach(function (row, ordinal) {
      if (object(row) && (alias ? row.key === EA : row.id === want || row.key === want)) hits.push({ row: row, ordinal: ordinal });
    });
    if (hits.length !== 1) return unavailable(doc, sym, path, hits.length ? 'ambiguous_series_identity' : 'unknown_series_identity', hits.map(function (h) { return h.ordinal; }));
    var chosen = hits[0], result = compile(doc, sym, path, chosen.row.key || chosen.row.id, chosen.row, 'points', 'series[' + chosen.ordinal + ']');
    result.evidence.identity_alias = alias ? { requested: 'ea', canonical_key: EA } : null;
    return result;
  }
  function warehouse(doc, sym, path) {
    var want = String(sym || ''), packetPath = path || '/series?id=' + encodeURIComponent(want);
    if (!object(doc)) return unavailable(doc, want, packetPath, 'warehouse_packet_unavailable');
    if (!/^[^:\s]+:.+$/.test(want) || typeof doc.id !== 'string' || doc.id.toLowerCase() !== want.toLowerCase())
      return unavailable(doc, want, packetPath, 'packet_identity_mismatch', object(doc) && typeof doc.id === 'string' ? [doc.id] : []);
    var result = compile(doc, want, packetPath, doc.id, doc, 'warehouse', '$'), e = result.evidence;
    e.reported_provider = typeof doc.provider === 'string' ? doc.provider : null;
    e.reported_source = typeof doc.source === 'string' ? doc.source : null;
    e.packet_reported_as_of = typeof doc.as_of === 'string' ? doc.as_of : null;
    e.received_calendar_basis = 'Warehouse-normalized calendar dates. Native normalization may collapse periods and duplicates; original provider rows and period precision are unverified.';
    e.points_scope = 'Complete received warehouse obs array; upstream original history and transformations remain unverified';
    e.upstream_originals_verified = false;
    e.identity_resolution = { requested: want, reported: doc.id, rule: 'Full native directory id, case-insensitive; no suffix or substring matching' };
    e.transformations.push('Validate received normalized calendar dates without interpreting packet as_of as observation acquisition or first publication');
    return result;
  }
  return { contract: VERSION, numeric: numeric, period: period, cq: cq, ciss: ciss, warehouse: warehouse };
});
