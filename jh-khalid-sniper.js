/* Khalid backend evidence renderer. Legacy pure measurements retained for pinned offline research only. */
(function (root) {
  "use strict";
  function num(x) { return typeof x === "number" && isFinite(x) ? x : null; }
  function volOf(b) { return num(b.volume) || num(b.value) || 0; }
  function median(a) {
    var b = a.filter(function (x) { return x != null && isFinite(x); }).sort(function (x, y) { return x - y; });
    if (!b.length) return null;
    return b[(b.length - 1) >> 1];
  }
  function smaAt(c, i, n) {
    if (i < n - 1) return null;
    var s = 0, k;
    for (k = i - n + 1; k <= i; k++) s += c[k];
    return s / n;
  }
  function rsiAt(c, i, n) {
    if (i < n) return null;
    var ag = 0, al = 0, k, ch, g, l;
    for (k = 1; k <= n; k++) {
      ch = c[k] - c[k - 1];
      ag += Math.max(ch, 0); al += Math.max(-ch, 0);
    }
    ag /= n; al /= n;
    for (k = n + 1; k <= i; k++) {
      ch = c[k] - c[k - 1];
      g = Math.max(ch, 0); l = Math.max(-ch, 0);
      ag = (ag * (n - 1) + g) / n;
      al = (al * (n - 1) + l) / n;
    }
    if (al === 0) return 100;
    return 100 - 100 / (1 + ag / al);
  }
  function atrAt(d, i, n) {
    if (i < n) return null;
    var s = 0, k, tr, a;
    for (k = 1; k <= n; k++) {
      tr = Math.max(d[k].high - d[k].low, Math.abs(d[k].high - d[k - 1].close), Math.abs(d[k].low - d[k - 1].close));
      s += tr;
    }
    a = s / n;
    for (k = n + 1; k <= i; k++) {
      tr = Math.max(d[k].high - d[k].low, Math.abs(d[k].high - d[k - 1].close), Math.abs(d[k].low - d[k - 1].close));
      a = (a * (n - 1) + tr) / n;
    }
    return a;
  }
  function bbWidth(c, i) {
    var m = smaAt(c, i, 20);
    if (m == null || m === 0) return null;
    var s = 0, k;
    for (k = i - 19; k <= i; k++) s += (c[k] - m) * (c[k] - m);
    return (4 * Math.sqrt(s / 20)) / m;
  }
  function box(id, label, pass, detail, required) {
    return { id: id, label: label, pass: pass === true, detail: detail, required: required !== false };
  }
  function pct(x) { return x == null || !isFinite(x) ? "—" : (x >= 0 ? "+" : "") + x.toFixed(1) + "%"; }

  function score(raw, meta) {
    meta = meta || {};
    var d = (raw || []).filter(function (b) { return b && num(b.close) && num(b.high) && num(b.low); });
    if (d.length > 560) d = d.slice(-560);
    var checks = [];
    var i = d.length - 1;
    if (i < 320) {
      var short = "Need 320 daily bars to score this. Have " + d.length + ".";
      checks = [
          box("book", "S&P 500, Nasdaq-100, ETF, metal, bond, or crypto", !!meta.book, meta.book || "Not tagged", true),
          box("ma250", "Below the 250-day", false, short, true),
          box("offhigh", "At least 50% off the high", false, short, true),
          box("rsi", "RSI washed out", false, short, true),
          box("spread", "Very tight price spread", false, short, true),
          box("bb", "Tight Bollinger bands", false, short, true),
          box("volty", "Very low volatility", false, short, true),
          box("volume", "Shrinking volume", false, short, true),
          box("flat", "Flat moving average", false, short, true),
          box("support", "On 3-month support", false, short, true),
          box("higher", "Higher low", false, short, true),
          box("exhaust", "Selling climax or capitulation", false, short, true),
          box("demand", "Demand showing", false, short, true),
          box("value", "Cheap: PEG under 1, or P/E and P/S under the industry", false, short, true),
          box("sector", "Industry ETF under, at, or just through its 200-day", false, short, true),
          box("flows", "ETF inflows, institutions, smart money", false, short, true)
        ].concat(meta.cryptoLinked ? [box("crypto", "ETH or BTC turned while this is still on its low", false, short, true)] : []).concat([
          box("ma300", "Below the 300-day", false, short, false),
          box("double", "Double bottom", false, short, false),
          box("campaign", "Marked bottom or end of accumulation", false, short, false),
          box("catalyst", "Booming industry or other catalyst", false, short, false),
          box("momentum", "Momentum turning up", false, short, false)
        ]);
      var requiredN = 0;
      checks.forEach(function (c) { if (c.required) requiredN++; });
      return {
        checks: checks,
        sniper: false, drawdown: null, close: i >= 0 ? d[i].close : null, passed: 0, required: requiredN
      };
    }
    var c = d.map(function (b) { return b.close; });
    var px = c[i];
    var sma250 = smaAt(c, i, 250);
    var sma300 = smaAt(c, i, 300);
    var sma20 = smaAt(c, i, 20);
    var sma20p = smaAt(c, i - 10, 20);
    var rsi = rsiAt(c, i, 14);
    var atr14 = atrAt(d, i, 14);
    var atr100 = atrAt(d, i, 100);
    var hi = -Infinity, k, lo63 = Infinity, loPrior = Infinity;
    var from = Math.max(0, i - 503);
    for (k = from; k <= i; k++) if (d[k].high > hi) hi = d[k].high;
    for (k = i - 62; k <= i; k++) if (d[k].low < lo63) lo63 = d[k].low;
    for (k = i - 140; k < i - 62; k++) if (d[k].low < loPrior) loPrior = d[k].low;
    var dd = hi ? px / hi - 1 : null;
    var under250 = sma250 != null && px < sma250;
    var allowedBook = { STOCK: 1, ETF: 1, COMMODITY: 1, BOND: 1, CRYPTO: 1 };
    checks.push(box("book", "S&P 500, Nasdaq-100, ETF, metal, bond, or crypto", !!allowedBook[meta.assetClass], meta.book || meta.assetClass || "Not in the book", true));
    checks.push(box("ma250", "Below the 250-day", under250,
      sma250 == null ? "250-day average unavailable" : "Close " + px.toFixed(2) + " vs 250-day " + sma250.toFixed(2) + " (" + pct((px / sma250 - 1) * 100) + ")", true));
    checks.push(box("offhigh", "At least 50% off the high", dd != null && dd <= -0.50,
      "Off the 504-day high by " + pct(dd == null ? null : dd * 100) + ". Deeper is better.", true));
    var rsiPass = rsi != null && rsi <= 45;
    checks.push(box("rsi", "RSI washed out", rsiPass,
      rsi == null ? "RSI unavailable" : "RSI(14) " + rsi.toFixed(1) + (rsi <= 30 ? " — oversold" : rsi <= 45 ? " — washed, not deeply oversold" : " — not washed out") + ". Gate is 45 so a finished base is not rejected for leaving the panic print. Deep oversold is under 30.", true));
    var widths = [];
    for (k = Math.max(20, i - 251); k <= i; k++) widths.push(bbWidth(c, k));
    var width = widths[widths.length - 1];
    var below = widths.filter(function (w) { return w != null && width != null && w <= width; }).length;
    var bbp = width == null ? null : 100 * below / widths.filter(function (w) { return w != null; }).length;
    checks.push(box("bb", "Tight Bollinger bands", bbp != null && bbp <= 20,
      bbp == null ? "Band width unavailable" : (bbp <= 1 ? "Bandwidth is the tightest of the past year." : "Bandwidth sits in the lowest " + bbp.toFixed(0) + "% of the past year. Gate is 20%."), true));
    var spreads = [];
    for (k = i - 59; k <= i; k++) spreads.push((d[k].high - d[k].low) / d[k].close);
    var recentSpread = median(spreads.slice(-10));
    var priorSpread = median(spreads.slice(0, 40));
    checks.push(box("spread", "Very tight price spread", recentSpread != null && priorSpread && recentSpread <= priorSpread * 0.75,
      recentSpread == null ? "Spread unavailable" : "Last 10-day spread " + (recentSpread * 100).toFixed(2) + "% vs prior " + (priorSpread * 100).toFixed(2) + "%. Must be at least 25% tighter.", true));
    var vols = [];
    for (k = i - 59; k <= i; k++) vols.push(volOf(d[k]));
    var recentVol = median(vols.slice(-12));
    var priorVol = median(vols.slice(0, 40));
    checks.push(box("volume", "Shrinking volume", recentVol != null && priorVol && priorVol > 0 && recentVol <= priorVol * 0.75,
      priorVol ? "Recent volume is " + (recentVol / priorVol).toFixed(2) + "× the prior 40 days. Gate is 0.75× or lower." : "No volume on the bars", true));
    var rel = atr14 && atr100 ? atr14 / atr100 : null;
    checks.push(box("volty", "Very low volatility", rel != null && rel <= 0.85,
      rel == null ? "Range unavailable" : "14-day range is " + rel.toFixed(2) + "× the 100-day range. Gate is 0.85×.", true));
    var flat = sma20 && sma20p ? Math.abs(sma20 / sma20p - 1) : null;
    checks.push(box("flat", "Flat moving average", flat != null && flat <= 0.012,
      flat == null ? "20-day average unavailable" : "20-day average moved " + pct(flat * 100) + " over 10 sessions. Gate is 1.2%.", true));
    var distSup = lo63 ? px / lo63 - 1 : null;
    checks.push(box("support", "On 3-month support", distSup != null && distSup >= -0.005 && distSup <= 0.035,
      distSup == null ? "Support unavailable" : "Close is " + pct(distSup * 100) + " from the 63-day low (" + lo63.toFixed(2) + "). Must be on it, not through it and not more than 3.5% above.", true));
    var higher = lo63 && loPrior && lo63 > loPrior * 1.005;
    checks.push(box("higher", "Higher low", higher,
      "63-day low " + (lo63 ? lo63.toFixed(2) : "—") + " vs the prior low " + (loPrior && isFinite(loPrior) ? loPrior.toFixed(2) : "—") + ".", true));
    var panic = false, panicDetail = "No capitulation bar in the last 90 sessions";
    for (k = Math.max(30, i - 90); k <= i - 3; k++) {
      var ret = d[k].close / d[k - 1].close - 1;
      var baseV = median(d.slice(k - 20, k).map(volOf));
      var rv = baseV ? volOf(d[k]) / baseV : 0;
      var loc = (d[k].high - d[k].low) ? (d[k].close - d[k].low) / (d[k].high - d[k].low) : 0;
      if (ret <= -0.03 && rv >= 1.7 && loc >= 0.25) { panic = true; panicDetail = "Capitulation on bar " + (i - k) + " sessions ago: " + pct(ret * 100) + " on " + rv.toFixed(1) + "× volume, closed off the low."; break; }
    }
    checks.push(box("exhaust", "Selling climax or capitulation", panic, panicDetail, true));
    var ups = 0;
    for (k = i - 5; k <= i; k++) if (d[k].close > d[k].open) ups++;
    var demand = px > lo63 && ups >= 2 && c[i] >= c[i - 3] * 0.995;
    checks.push(box("demand", "Demand showing", demand,
      ups + " of the last 6 bars closed up, and the close is holding the recent low.", true));
    var klass = meta.assetClass || "STOCK";
    var nonStock = klass === "ETF" || klass === "COMMODITY" || klass === "BOND" || klass === "CRYPTO";
    if (nonStock) {
      checks.push(box("value", "Cheap: PEG under 1, or P/E and P/S under the industry", true, "Not a stock. P/E and PEG are not applied.", true));
    } else if (!meta.valuation) {
      checks.push(box("value", "Cheap: PEG under 1, or P/E and P/S under the industry", false, "No P/E, PEG or P/S on the S&P book for this name.", true));
    } else {
      var v = meta.valuation;
      var pegOk = v.peg != null && v.peg > 0 && v.peg < 1;
      var peOk = v.pe != null && v.pe > 0 && v.sectorPe != null && v.pe <= v.sectorPe * 0.80;
      var psOk = v.ps != null && v.ps > 0 && v.sectorPs != null && v.ps <= v.sectorPs * 0.80;
      checks.push(box("value", "Cheap: PEG under 1, or P/E and P/S under the industry", pegOk || peOk || psOk,
        "PEG " + (v.peg == null ? "—" : v.peg.toFixed(2)) + " (want under 1). P/E " + (v.pe == null ? "—" : v.pe.toFixed(1)) + " vs industry " + (v.sectorPe == null ? "—" : v.sectorPe.toFixed(1)) + ". P/S " + (v.ps == null ? "—" : v.ps.toFixed(2)) + " vs industry " + (v.sectorPs == null ? "—" : v.sectorPs.toFixed(2)) + ".", true));
    }
    if (meta.sectorEtf == null) {
      checks.push(box("sector", "Industry ETF under, at, or just through its 200-day", nonStock, nonStock ? "This is the vehicle, not a single stock." : "No sector ETF mapped.", true));
    } else {
      var sv = meta.sectorEtf.vs200;
      var sectorOk = sv != null && sv >= -15 && sv <= 8;
      checks.push(box("sector", "Industry ETF under, at, or just through its 200-day", sectorOk,
        (meta.sectorEtf.symbol || "ETF") + " is " + pct(sv) + " versus its 200-day. Allowed band is 15% under to 8% over. Not an extended market.", true));
    }
    if (!meta.flows) {
      checks.push(box("flows", "ETF inflows, institutions, smart money", false, "No verified fund-flow or 13F net buy on this name. Missing is not a yes.", true));
    } else {
      checks.push(box("flows", "ETF inflows, institutions, smart money", true, meta.flows, true));
    }
    if (meta.cryptoLinked) {
      checks.push(box("crypto", "ETH or BTC turned while this is still on its low", !!meta.cryptoLead, meta.cryptoNote || "No fresh ether or bitcoin turn while this name is still sitting on its own low.", true));
    }
    checks.push(box("ma300", "Below the 300-day", sma300 != null && px < sma300,
      sma300 == null ? "300-day unavailable" : "Close vs 300-day " + sma300.toFixed(2) + " (" + pct((px / sma300 - 1) * 100) + "). Better, not required.", false));
    var swings = [];
    for (k = i - 130; k <= i - 5; k++) {
      var isLow = true, j;
      for (j = k - 5; j <= k + 5; j++) if (j !== k && d[j] && d[j].low < d[k].low) isLow = false;
      if (isLow) swings.push(k);
    }
    var dbl = false, dblDetail = "No double bottom in the last 130 sessions";
    var a, b;
    for (a = 0; a < swings.length; a++) for (b = a + 1; b < swings.length; b++) {
      var gap = swings[b] - swings[a];
      if (gap < 10 || gap > 90) continue;
      var p1 = d[swings[a]].low, p2 = d[swings[b]].low;
      if (Math.abs(p1 - p2) / Math.max(p1, p2) > 0.04) continue;
      var valley = -Infinity, t;
      for (t = swings[a]; t <= swings[b]; t++) if (d[t].high > valley) valley = d[t].high;
      if (valley < Math.max(p1, p2) * 1.04) continue;
      if (px <= Math.max(p1, p2) * 1.06) { dbl = true; dblDetail = "Two lows " + p1.toFixed(2) + " and " + p2.toFixed(2) + ", " + gap + " sessions apart."; }
    }
    checks.push(box("double", "Double bottom", dbl, dblDetail, false));
    checks.push(box("campaign", "Marked bottom or end of accumulation", !!meta.campaign, meta.campaign || "Not on the live bottom board.", false));
    checks.push(box("catalyst", "Booming industry or other catalyst", !!meta.catalyst, meta.catalyst || "No industry-boom or contract catalyst on the tape.", false));
    var macdUp = false;
    if (i >= 40) {
      var e12 = null, e26 = null, seed12 = 0, seed26 = 0;
      var k12 = 2 / 13, k26 = 2 / 27;
      for (k = 0; k <= i; k++) {
        if (k < 12) seed12 += c[k];
        if (k === 11) e12 = seed12 / 12;
        else if (k > 11) e12 = c[k] * k12 + e12 * (1 - k12);
        if (k < 26) seed26 += c[k];
        if (k === 25) e26 = seed26 / 26;
        else if (k > 25) e26 = c[k] * k26 + e26 * (1 - k26);
      }
      var h0 = e12 - e26;
      var h1p = null, e12b = null, e26b = null, s12 = 0, s26 = 0;
      for (k = 0; k <= i - 3; k++) {
        if (k < 12) s12 += c[k];
        if (k === 11) e12b = s12 / 12;
        else if (k > 11) e12b = c[k] * k12 + e12b * (1 - k12);
        if (k < 26) s26 += c[k];
        if (k === 25) e26b = s26 / 26;
        else if (k > 25) e26b = c[k] * k26 + e26b * (1 - k26);
      }
      h1p = e12b - e26b;
      macdUp = h0 != null && h1p != null && h0 > h1p && rsi != null && rsi < 55;
    }
    checks.push(box("momentum", "Momentum turning up", macdUp, macdUp ? "MACD line is rising and RSI is still under 55." : "MACD is not turning up yet.", false));

    var required = checks.filter(function (x) { return x.required; });
    var sniper = required.every(function (x) { return x.pass; });
    return { checks: checks, sniper: sniper, drawdown: dd, close: px, passed: required.filter(function (x) { return x.pass; }).length, required: required.length };
  }

  // Frozen offline callers retain the legacy measurement function. It has no UI
  // readiness authority; neither mount consumes its boolean nor fetches bars.
  root.jhSniperScore = function (bars, meta) {
    var result = score(bars, meta);
    result.qualification_authority = false;
    result.measurement_label = "Legacy technical observations; not backend or requested-strategy qualification";
    return result;
  };
  var SCHEMA = "khalid-qualification.v1";
  var BACKEND = "khalid-existing-readiness.v1";
  var REQUESTED = "khalid-requested-strategy.unresolved.v1";
  var STATES = ["PASS", "FAIL", "UNAVAILABLE", "UNRESOLVED"];
  var GATES = ["location", "compression", "supply", "structure", "legacy_flow", "catalyst", "rsi", "dilution", "confidence", "vetoes", "reward_risk", "risk_permission", "trigger"];
  var REQUESTED_IDS = ["universe","ma250","offhigh","rsi","spread","bb","volatility","volume","flat","support_3m","higher_low","capitulation","demand","valuation","industry","true_flow","resilience","crypto_relative","ma300","double_bottom","campaign","catalyst","momentum"];
  function unavailable(reason) { return { valid: false, status: "UNAVAILABLE", reason: reason, rows: [] }; }
  var GATE_UNITS = { location: "percent", compression: "percentile", supply: "ratio", structure: "boolean",
    legacy_flow: "count", catalyst: "count", rsi: "index", dilution: "boolean", confidence: "fraction",
    vetoes: "count", reward_risk: "ratio", risk_permission: "boolean", trigger: "boolean" };
  function validCriterionValue(c, version) {
    if (!c) return false;
    if (version === REQUESTED) return c.status === "UNRESOLVED" && c.value === null && c.unit === null;
    var unit = GATE_UNITS[c.id];
    if (!unit || c.unit !== unit) return false;
    if (c.status === "UNAVAILABLE") return c.value === null;
    if (c.status !== "PASS" && c.status !== "FAIL") return false;
    if (unit === "boolean") return typeof c.value === "boolean" && c.value === (c.status === "PASS");
    if (typeof c.value !== "number" || !Number.isFinite(c.value)) return false;
    if (unit === "count") return Number.isInteger(c.value) && c.value >= 0;
    // Numeric pass/fail thresholds belong exclusively to the backend evaluator.
    return true;
  }
  function validCriterion(c, version) {
    return validCriterionValue(c, version) && typeof c.id === "string" && typeof c.label === "string" && typeof c.definition === "string" &&
      c.definition_version === version && typeof c.applicability === "string" && STATES.indexOf(c.status) >= 0 &&
      Object.prototype.hasOwnProperty.call(c, "value") && Object.prototype.hasOwnProperty.call(c, "unit") &&
      c.clocks && typeof c.clocks.evaluated_at === "string" && Object.prototype.hasOwnProperty.call(c.clocks, "effective_at") &&
      Object.prototype.hasOwnProperty.call(c.clocks, "available_at") && typeof c.clocks.clock_note === "string" && typeof c.provenance === "string";
  }
  function project(feed, now, ticker) {
    var p = feed && feed.qualification_evidence;
    if (!p || p.schema_version !== SCHEMA) return unavailable("Missing or unsupported backend qualification contract.");
    var time = Date.parse(p.generated_at), expiry = Date.parse(p.expires_at);
    if (!Number.isFinite(time) || !Number.isFinite(expiry) || p.generated_at !== feed.generated_at ||
        time > now || expiry <= now || expiry <= time || expiry - time > 26 * 3600000)
      return unavailable("Qualification publication is stale, future-dated or clock-mismatched.");
    if (!/^[a-f0-9]{64}$/.test(p.revision || "") || !Array.isArray(p.rows) || !Array.isArray(feed.opportunity_radar))
      return unavailable("Missing revision or candidate inventory.");
    if (!Array.isArray(p.backend_definitions) || p.backend_definitions.length !== GATES.length ||
        !GATES.every(function (id) { return p.backend_definitions.filter(function (d) { return d && d.id === id; }).length === 1; }) ||
        !Array.isArray(p.requested_criteria) || p.requested_criteria.length !== 23 ||
        !REQUESTED_IDS.every(function (id) { return p.requested_criteria.filter(function (c) { return c && c.id === id; }).length === 1; }) ||
        !p.requested_criteria.every(function (c) { return validCriterion(c, REQUESTED) && c.status === "UNRESOLVED" && c.clocks.evaluated_at === p.generated_at; }) ||
        !p.rows.every(function (r) { return r && typeof r === "object" && r.existing_backend_qualification && r.requested_strategy_qualification; }))
      return unavailable("Missing or conflicting criterion definitions.");
    var keys = new Set(), bad = false;
    var expanded = p.rows.map(function (raw) {
      var b = raw.existing_backend_qualification, q = raw.requested_strategy_qualification;
      if (b.criterion_definitions_ref !== "backend_definitions" || q.criteria_ref !== "requested_criteria" || !Array.isArray(b.criteria)) bad = true;
      return Object.assign({}, raw, {
        existing_backend_qualification: Object.assign({}, b, { criteria: (Array.isArray(b.criteria) ? b.criteria : []).map(function (c) {
          var def = p.backend_definitions.find(function (d) { return c && d.id === c.id; }) || {};
          return Object.assign({}, def, c, {clocks: b.clocks});
        }) }), requested_strategy_qualification: Object.assign({}, q, {criteria: p.requested_criteria})
      });
    });
    expanded.forEach(function (r) {
      var b = r.existing_backend_qualification, q = r.requested_strategy_qualification;
      var match = /^opportunity_radar\/(\d+)$/.exec(r.source_path || "");
      var candidate = match && feed.opportunity_radar[Number(match[1])];
      var key = r.asset_class + ":" + r.ticker;
      if (typeof r.ticker !== "string" || !r.ticker || typeof r.asset_class !== "string" || !r.asset_class || !candidate || keys.has(key) || r.artifact_revision !== p.revision || candidate.ticker !== r.ticker || candidate.asset_class !== r.asset_class ||
          !b || !q || b.schema_version !== BACKEND || q.schema_version !== REQUESTED || b.ticker !== r.ticker || q.ticker !== r.ticker ||
          b.asset_class !== r.asset_class || q.asset_class !== r.asset_class || b.reported_action !== candidate.action ||
          STATES.indexOf(b.status) < 0 || q.status !== "UNRESOLVED" || q.complete !== false ||
          !b.clocks || b.clocks.evaluated_at !== p.generated_at || !q.clocks || q.clocks.evaluated_at !== p.generated_at ||
          !Array.isArray(b.input_sources) || typeof b.source_attribution !== "string" ||
          typeof b.authority !== "string" || typeof q.authority !== "string" || typeof b.reason !== "string" || typeof b.criterion_clocks !== "string" ||
          !Array.isArray(b.sources) || (b.status === "PASS" && (!b.sources.length || !b.input_sources.length || !b.input_sources.every(function (name) { return b.sources.some(function (s) { return s && s.name === name; }); }) || !b.sources.every(function (s) { return s && s.status === "FRESH" && Number.isFinite(Date.parse(s.as_of)) && Date.parse(s.as_of) <= now &&
              typeof s.max_age_h === "number" && Number.isFinite(s.max_age_h) && s.max_age_h > 0 && now - Date.parse(s.as_of) <= s.max_age_h * 3600000; }))) || !Array.isArray(b.criteria) || !Array.isArray(q.criteria) || q.criteria.length !== 23 ||
          !q.criteria.every(function (c) { return validCriterion(c, REQUESTED) && c.status === "UNRESOLVED" && c.clocks.evaluated_at === p.generated_at; }) ||
          !b.criteria.every(function (c) { return validCriterion(c, BACKEND) && c.clocks.evaluated_at === p.generated_at; }) ||
          (b.status !== "UNAVAILABLE" && (b.criteria.length !== GATES.length || !GATES.every(function (id) { return b.criteria.filter(function (c) { return c.id === id; }).length === 1; }))) ||
          (b.status === "PASS" && (b.reported_action !== "READY_TO_SNIPE" || !b.criteria.every(function (c) { return c.status === "PASS"; })))) bad = true;
      keys.add(key);
    });
    if (bad || p.rows.length !== feed.opportunity_radar.length) return unavailable("Candidate identity, field coverage, definition or revision mismatch.");
    var rows = expanded.filter(function (r) { return !ticker || r.ticker === ticker; });
    if (ticker && !rows.length) return unavailable("No backend qualification for " + ticker + ". Browser measurements cannot supply it.");
    return { valid: true, status: "AVAILABLE", rows: rows, packet: p };
  }
  root.jhSniperQualification = project;
  if (typeof document === "undefined") return;

  function el(tag, text, cls) {
    var n = document.createElement(tag);
    if (text != null) n.textContent = String(text);
    if (cls) n.className = cls;
    return n;
  }
  function value(v) { return v == null ? "Unavailable" : typeof v === "object" ? JSON.stringify(v) : String(v); }
  function pair(dl, name, v) { dl.append(el("dt", name), el("dd", value(v))); }
  function clocks(parent, c) {
    var dl = el("dl", null, "sn-facts");
    pair(dl, "Evaluated", c.evaluated_at); pair(dl, "Effective", c.effective_at); pair(dl, "Available", c.available_at);
    pair(dl, "Clock limits", c.clock_note); parent.append(dl);
  }
  function criterion(c) {
    var details = el("details", null, "sn-criterion");
    details.append(el("summary", c.label + " — " + c.status));
    details.append(el("p", c.definition));
    var dl = el("dl", null, "sn-facts");
    pair(dl, "Criterion", c.id); pair(dl, "Definition", c.definition_version); pair(dl, "Applicability", c.applicability);
    pair(dl, "Value", c.value); pair(dl, "Unit", c.unit); pair(dl, "Provenance", c.provenance); details.append(dl);
    clocks(details, c.clocks); return details;
  }
  function contract(parent, title, c) {
    var section = el("section", null, "sn-contract");
    section.append(el("h4", title + " — " + c.status), el("p", c.authority));
    if (c.reason) section.append(el("p", c.reason));
    var meta = el("details"); meta.append(el("summary", "Definition and provenance"));
    var dl = el("dl", null, "sn-facts");
    pair(dl, "Contract", c.schema_version); pair(dl, "Ticker", c.ticker); pair(dl, "Asset class", c.asset_class);
    if (Object.prototype.hasOwnProperty.call(c, "complete")) { pair(dl, "Complete", c.complete); pair(dl, "Criteria", c.criteria_ref); }
    if (Object.prototype.hasOwnProperty.call(c, "reported_action")) {
      pair(dl, "Input sources", c.input_sources); pair(dl, "Source attribution limits", c.source_attribution);
      pair(dl, "Criterion definitions", c.criterion_definitions_ref); pair(dl, "Criterion clocks", c.criterion_clocks);
      pair(dl, "Published action", c.reported_action); pair(dl, "Scoring action before lifecycle controls", c.scoring_action);
    }
    meta.append(dl); clocks(meta, c.clocks);
    (c.sources || []).forEach(function (s) {
      var sd = el("dl", null, "sn-facts");
      Object.keys(s).forEach(function (k) { pair(sd, k, s[k]); }); meta.append(sd);
    });
    section.append(meta);
    c.criteria.forEach(function (check) { section.append(criterion(check)); });
    parent.append(section);
  }
  var STYLE = ".sn-evidence{color:inherit;max-width:100%;font:14px/1.5 system-ui;overflow-wrap:anywhere}.sn-evidence h2{font-size:22px}.sn-tools{display:flex;gap:12px;flex-wrap:wrap;margin:16px 0}.sn-tools label{display:grid;gap:4px}.sn-tools input,.sn-tools select,.sn-tools button{font:inherit;padding:8px;max-width:100%;background:#151c28;color:#e5e7eb;border:1px solid #637184;border-radius:5px}.sn-tools button:disabled{opacity:.45;cursor:default}.sn-card{border:1px solid #637184;border-radius:8px;padding:14px;margin:14px 0}.sn-pair{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:18px}.sn-criterion{border-top:1px solid #485468;padding:8px 0}.sn-evidence summary{cursor:pointer;min-height:32px}.sn-facts{display:grid;grid-template-columns:minmax(85px,1fr) minmax(0,3fr);gap:4px 12px}.sn-facts dt{color:#9ca3af}.sn-facts dd{margin:0}.sn-evidence :focus-visible{outline:3px solid #f3b942;outline-offset:3px}@media(max-width:760px){.sn-pair{grid-template-columns:1fr}.sn-card{padding:10px}.sn-tools>*{max-width:100%}.sn-facts{grid-template-columns:1fr}.sn-facts dd{margin-bottom:8px}}";
  function revalidationDeadline(packet) {
    var deadline = Date.parse(packet.expires_at);
    packet.rows.forEach(function (row) {
      var backend = row.existing_backend_qualification;
      if (backend.status !== "PASS") return;
      backend.sources.forEach(function (source) {
        // Native freshness accepts age == SLA; first invalid JS millisecond is +1.
        var firstStale = Math.floor(Date.parse(source.as_of) + source.max_age_h * 3600000) + 1;
        if (Number.isFinite(firstStale)) deadline = Math.min(deadline, firstStale);
      });
    });
    return deadline;
  }
  function mount(host, options) {
    if (!host) return;
    if (host._qualificationCleanup) host._qualificationCleanup();
    options = options || {};
    var symbol = options.ticker || (root.location && root.location.pathname === "/chart.html" && root.jhActive) || null;
    var ticket = {}; host._qualificationTicket = ticket;
    host.replaceChildren(el("p", "Loading backend qualification evidence…"));
    // Exactly one static publication. No bars, vendor proxy, source fetch or local scoring.
    return root.fetch("/data/khalid.json", { cache: "no-store" }).then(function (r) {
      if (!r.ok) throw new Error("Backend publication unavailable"); return r.json();
    }).then(function (feed) {
      if (host._qualificationTicket !== ticket) return;
      var projection = project(feed, Date.now(), symbol);
      var shell = el("section", null, "sn-evidence"); shell.append(el("style", STYLE), el("h2", "Backend readiness and requested strategy"));
      shell.append(el("p", "Existing backend readiness and the requested strategy have separate contracts. Technical chart measurements have no qualification authority."));
      if (!projection.valid) { shell.append(el("p", "UNAVAILABLE — " + projection.reason)); host.replaceChildren(shell); return; }
      var p = projection.packet;
      shell.append(el("p", "Published " + p.generated_at + "; display expires " + p.expires_at + "."));
      shell.append(el("p", p.historical_validation));
      var pd = el("details"); pd.append(el("summary", "Publication provenance"));
      var dl = el("dl", null, "sn-facts"); pair(dl, "Contract", p.schema_version); pair(dl, "Revision", p.revision); pair(dl, "Freshness scope", p.freshness_scope); pd.append(dl); shell.append(pd);
      var controls = el("div", null, "sn-tools"), label = el("label", "Find ticker"), search = el("input"); search.type = "search"; label.append(search);
      var catlabel = el("label", "Asset class"), select = el("select");
      ["ALL"].concat(Array.from(new Set(projection.rows.map(function (r) { return r.asset_class; }))).sort()).forEach(function (cat) { var opt = el("option", cat); opt.value = cat; select.append(opt); }); catlabel.append(select);
      var button = el("button", "Show backend-ready only"); button.type = "button"; button.setAttribute("aria-pressed", "false");
      controls.append(label, catlabel, button); shell.append(controls);
      var count = el("p"); count.setAttribute("role", "status"); var list = el("div"); shell.append(count, list); host.replaceChildren(shell);
      var onlyReady = false, pageIndex = 0, pageSize = 10;
      var previous = el("button", "Previous"), next = el("button", "Next"); previous.type = next.type = "button";
      controls.append(previous, next);
      function renderRows() {
        // Revalidate time on every interaction; an old mounted panel cannot remain qualified.
        var current = project(feed, Date.now(), symbol);
        list.replaceChildren();
        if (!current.valid) { count.textContent = "UNAVAILABLE — " + current.reason; return false; }
        var shown = current.rows.filter(function (r) {
          return r.ticker.toUpperCase().indexOf(search.value.trim().toUpperCase()) >= 0 && (select.value === "ALL" || r.asset_class === select.value) &&
            (!onlyReady || r.existing_backend_qualification.status === "PASS");
        });
        pageIndex = Math.min(pageIndex, Math.max(0, Math.ceil(shown.length / pageSize) - 1));
        previous.disabled = pageIndex === 0; next.disabled = (pageIndex + 1) * pageSize >= shown.length;
        count.textContent = shown.length + " of " + current.rows.length + " backend candidates match; page " + (pageIndex + 1) + ". Requested strategy remains unresolved.";
        shown.slice(pageIndex * pageSize, (pageIndex + 1) * pageSize).forEach(function (r) {
          var article = el("article", null, "sn-card"); article.append(el("h3", r.ticker + " · " + (r.name || r.ticker) + " · " + r.asset_class));
          var link = el("a", "Open chart"); link.href = "/chart.html?s=" + encodeURIComponent(r.ticker); article.append(link);
          var provenance = el("details"); provenance.append(el("summary", "Candidate provenance"));
          var identity = el("dl", null, "sn-facts"); pair(identity, "Source path", r.source_path); pair(identity, "Artifact revision", r.artifact_revision); provenance.append(identity); article.append(provenance);
          var grid = el("div", null, "sn-pair"); contract(grid, "Existing backend qualification", r.existing_backend_qualification); contract(grid, "Requested strategy qualification", r.requested_strategy_qualification); article.append(grid); list.append(article);
        });
        return true;
      }
      search.addEventListener("input", function () { pageIndex = 0; renderRows(); }); select.addEventListener("change", function () { pageIndex = 0; renderRows(); });
      previous.addEventListener("click", function () { pageIndex--; renderRows(); next.focus(); });
      next.addEventListener("click", function () { pageIndex++; renderRows(); previous.focus(); });
      button.addEventListener("click", function () { onlyReady = !onlyReady; button.setAttribute("aria-pressed", String(onlyReady)); button.textContent = onlyReady ? "Show every candidate" : "Show backend-ready only"; renderRows(); });
      var timer = null;
      function cleanup() {
        if (timer !== null) root.clearTimeout(timer);
        timer = null;
        document.removeEventListener("visibilitychange", onVisibility);
        root.removeEventListener("pageshow", refreshValidity);
        root.removeEventListener("focus", refreshValidity);
        if (host._qualificationCleanup === cleanup) host._qualificationCleanup = null;
      }
      function refreshValidity() {
        if (timer !== null) root.clearTimeout(timer);
        timer = null;
        if (host._qualificationTicket !== ticket || !host.isConnected) { cleanup(); return; }
        if (!renderRows()) { cleanup(); return; }
        timer = root.setTimeout(refreshValidity, Math.max(1, revalidationDeadline(p) - Date.now()));
      }
      function onVisibility() { if (document.visibilityState !== "hidden") refreshValidity(); }
      host._qualificationCleanup = cleanup;
      document.addEventListener("visibilitychange", onVisibility);
      root.addEventListener("pageshow", refreshValidity);
      root.addEventListener("focus", refreshValidity);
      refreshValidity();
    }).catch(function () { if (host._qualificationTicket === ticket) host.replaceChildren(el("p", "UNAVAILABLE — Backend qualification could not be loaded.")); });
  }
  root.jhSniperMount = mount;
  function boot() { var host = document.getElementById("k-sniper-host"); if (host) mount(host); }
  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot); else boot();
})(typeof window !== "undefined" ? window : globalThis);
