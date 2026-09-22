/* Shared CryptoQuant harvest fuse — series + twins + onchain + cq-feed snapshots
 * + public catalog (armed / catalog-only). Chartable = harvest series (twins
 * extend some to 2010). Extra cq-feed fields without a series bank are EOD
 * snapshots, never a 2-bar fake chart. Armed spec rows await the next EOD
 * pull (1y Professional window). Catalog-only rows are not banked.
 * Does not call api.cryptoquant.com. Does not invent pre-harvest history.
 */
(function (global) {
  "use strict";
  var PACK = null, PENDING = null;
  var CATN = {
    market_indicator: "Valuation & cycle",
    exchange_flows: "Exchange flows",
    market_data: "Derivatives & leverage",
    flow_indicator: "Flow indicators",
    miner_flows: "Miner flows",
    network_indicator: "Network indicators",
    network_data: "Network activity",
    stablecoins: "Stablecoins",
    eth: "Ethereum",
    eth2: "ETH 2.0 staking",
    mempool: "Mempool",
    lightning: "Lightning",
    erc20: "ERC-20",
    xrp: "XRP",
    trx: "TRON",
    v2_community: "v2 community",
    other: "Other"
  };
  var FIELD_Q = {
    a_sopr: ["asopr", "a sopr", "adjusted sopr"],
    sth_sopr: ["sth sopr", "short term holder sopr", "sthsopr"],
    lth_sopr: ["lth sopr", "long term holder sopr", "lthsopr"],
    nup: ["nup", "net unrealized profit"],
    nul: ["nul", "net unrealized loss"],
    block_interval: ["block interval", "block time"],
    is_shutdown: ["shutdown index", "exchange shutdown"],
    flow_total: ["in-house", "in house", "inhouse", "in-house-flow"],
    flow_mean: ["in-house mean"],
    reserve_usd: ["reserve usd", "exchange reserve usd"],
    inflow_top10: ["inflow top10", "top 10 inflow"],
    outflow_top10: ["outflow top10", "top 10 outflow"],
    long_liquidations: ["long liquidations", "long liq"],
    short_liquidations: ["short liquidations", "short liq"],
    taker_sell_ratio: ["taker sell"],
    taker_buy_volume: ["taker buy volume"],
    coinbase_premium_index: ["coinbase premium index"],
    stock_to_flow_reversion: ["s2f reversion", "stock to flow reversion"],
    supply_new: ["new supply", "issuance"],
    addresses_count_receiver: ["receiver addresses"],
    addresses_count_sender: ["sender addresses"],
    transactions_count_inflow: ["exchange tx inflow"],
    transactions_count_outflow: ["exchange tx outflow"],
    cdd: ["cdd", "coin days destroyed", "coindays"],
    sa_cdd: ["sa cdd", "supply adjusted cdd"],
    average_dormancy: ["dormancy", "average dormancy"],
    mvrv_ratio_zscore: ["mvrv z", "mvrv z-score", "mvrv zscore", "z-score"],
    total_value_staked: ["eth2", "eth 2", "staked eth", "tvl staked"],
    apparent_demand: ["apparent demand"]
  };

  function loadJson(url, fetchFn) {
    fetchFn = fetchFn || global.fetch;
    return fetchFn(url, { cache: "no-store" }).then(function (r) {
      if (!r || !r.ok) throw new Error("http " + (r && r.status));
      return r.json();
    });
  }
  function stripPath(p) {
    return String(p || "").replace(/^\/+/, "").replace(/\/+$/, "");
  }
  function nice(s) {
    return String(s || "").replace(/[_-]+/g, " ").replace(/\b\w/g, function (c) { return c.toUpperCase(); });
  }
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return ({ "&": "&" + "amp;", "<": "&" + "lt;", ">": "&" + "gt;", '"': "&" + "quot;", "'": "&#39;" })[c];
    });
  }
  function fmt(v) {
    if (v == null || v === "" || !isFinite(+v)) return "—";
    v = +v;
    var a = Math.abs(v);
    if (a >= 1e12) return (v / 1e12).toFixed(2) + "T";
    if (a >= 1e9) return (v / 1e9).toFixed(2) + "B";
    if (a >= 1e6) return (v / 1e6).toFixed(2) + "M";
    if (a >= 1000) return v.toLocaleString(undefined, { maximumFractionDigits: 1 });
    if (a >= 1) return v.toLocaleString(undefined, { maximumFractionDigits: 4 });
    if (a >= 0.01) return v.toFixed(4);
    return v.toExponential(2);
  }
  function zcol(z) {
    z = +z;
    if (!isFinite(z)) return "";
    if (z > 0.5) return "dn";
    if (z < -0.5) return "up";
    return "nt";
  }
  function mergeDv(a, b) {
    var m = {}, i, t;
    function put(row, prefer) {
      if (!row || !row.d || !row.v) return;
      for (i = 0; i < row.d.length && i < row.v.length; i++) {
        t = String(row.d[i]).slice(0, 10);
        if (!t) continue;
        if (prefer || m[t] == null) m[t] = +row.v[i];
      }
    }
    put(a, false);
    put(b, true);
    var dates = Object.keys(m).sort();
    return { d: dates, v: dates.map(function (k) { return m[k]; }) };
  }
  function dvBars(d, v) {
    var out = [], i;
    if (!d || !v) return out;
    for (i = 0; i < d.length && i < v.length; i++) {
      var t = Math.floor(Date.parse(String(d[i]).length <= 10 ? d[i] + "T00:00:00Z" : d[i]) / 1000);
      var c = +v[i];
      if (!isFinite(t) || t <= 0 || !isFinite(c)) continue;
      out.push({ time: t, open: c, high: c, low: c, close: c, volume: 0 });
    }
    return out;
  }
  function blobOf() {
    var parts = [];
    var i;
    for (i = 0; i < arguments.length; i++) if (arguments[i]) parts.push(String(arguments[i]));
    return parts.join(" ").toLowerCase();
  }
  function seriesRow(id, ser, twin, onM, specM) {
    var label = (onM && onM.label) || (specM && specM.label) || nice(id);
    var cat = (onM && onM.category) || (specM && specM.category) || "other";
    var unit = (onM && onM.unit) || (specM && specM.unit) || "";
    var first = (twin && twin.d && twin.d[0]) || (ser && ser.d && ser.d[0]) || "";
    var last = (ser && ser.d && ser.d[ser.d.length - 1]) || (twin && twin.d && twin.d[twin.d.length - 1]) || "";
    var n = ((ser && ser.d && ser.d.length) || 0);
    if (twin && twin.d && twin.d.length > n) n = twin.d.length;
    var extra = "CryptoQuant EOD · " + n + " pts " + String(first).slice(0, 10) + " → " + String(last).slice(0, 10);
    extra += (String(first).slice(0, 4) < "2025") ? " · twins+harvest" : " · harvest (not live)";
    return {
      id: id,
      s: "CQ:" + id,
      name: label,
      category: cat,
      unit: unit,
      n: n,
      first: first,
      last: last,
      twin: !!(twin && twin.d && twin.d.length),
      extra: extra,
      chartable: true,
      type: "onchain",
      cat: "chain",
      blob: blobOf(id, id.replace(/_/g, " "), label, cat, unit, "cryptoquant onchain cq")
    };
  }

  function build(docs) {
    var seriesDoc = docs.series || {};
    var onchain = docs.onchain || {};
    var feed = docs.feed || {};
    var spec = docs.spec || {};
    var cat = docs.catalog || {};
    var universe = docs.universe || {};
    var series = seriesDoc.series || {};
    var twins = seriesDoc.twins || {};
    var metrics = onchain.metrics || {};
    var specRows = Array.isArray(spec.metrics) ? spec.metrics : [];
    var specByName = {};
    var specByPath = {};
    var covered = {};
    specRows.forEach(function (m) {
      if (!m || !m.name) return;
      specByName[m.name] = m;
      var p = stripPath(m.path);
      specByPath[p] = m;
      if (m.resolved_key) covered[p + "|" + m.resolved_key] = m.name;
    });
    var chartable = [];
    Object.keys(series).forEach(function (id) {
      chartable.push(seriesRow(id, series[id], twins[id], metrics[id], specByName[id]));
    });
    chartable.sort(function (a, b) { return a.id < b.id ? -1 : a.id > b.id ? 1 : 0; });
    var liveCover = {};
    chartable.forEach(function (row) {
      var sm = specByName[row.id] || {};
      var p = stripPath(sm.path);
      if (p && sm.resolved_key) liveCover[p + "|" + sm.resolved_key] = row.id;
    });
    var feedMetrics = feed.metrics || {};
    var snaps = [];
    var snapCover = {};
    Object.keys(feedMetrics).forEach(function (pk) {
      var row = feedMetrics[pk] || {};
      var path = stripPath(row.path || pk.replace(/_/g, "/"));
      var fields = row.fields || {};
      var prev = row.prev || {};
      Object.keys(fields).forEach(function (fk) {
        var coverId = liveCover[path + "|" + fk] || covered[path + "|" + fk];
        if (coverId && series[coverId]) return;
        var s = "CQSNAP:" + path + ":" + fk;
        var aliases = FIELD_Q[fk] || [];
        var name = nice((path.split("/")[0] || "btc").toUpperCase() + " " + fk);
        snapCover[path + "|" + fk] = 1;
        snaps.push({
          s: s,
          path: path,
          field: fk,
          name: name,
          value: fields[fk],
          prev: prev[fk],
          asof: row.asof || "",
          extra: "CryptoQuant EOD snapshot · no harvest series (cq-feed limit=2)",
          chartable: false,
          type: "onchain",
          cat: "chain",
          blob: blobOf(s, path, fk, fk.replace(/_/g, " "), name, aliases.join(" "), "snapshot cryptoquant cq-feed")
        });
      });
    });
    snaps.sort(function (a, b) { return a.s < b.s ? -1 : 1; });
    var armed = [];
    var docsOnly = [];
    var urows = Array.isArray(universe.rows) ? universe.rows : [];
    urows.forEach(function (r) {
      if (!r || !r.id) return;
      var p = stripPath(r.path);
      var fk = r.field || "";
      var key = p + "|" + fk;
      if (liveCover[key] && series[liveCover[key]]) return;
      if (snapCover[key]) return;
      var aliases = FIELD_Q[fk] || [];
      var label = (r.name || nice(fk || r.id)) + (fk && r.name && String(r.name).toLowerCase().indexOf(String(fk).replace(/_/g, " ")) < 0 ? " · " + fk : "");
      var blob = blobOf(r.id, r.name, r.path, fk, fk.replace(/_/g, " "), r.group, r.category, aliases.join(" "), "cryptoquant cq");
      if (r.status === "armed") {
        armed.push({
          s: "CQARM:" + r.id,
          id: r.id,
          path: p,
          field: fk,
          name: label,
          group: r.group || "",
          category: r.category || "",
          extra: "CryptoQuant armed · 1y Professional window on next EOD pull · not live, no invented history",
          chartable: false,
          type: "onchain",
          cat: "chain",
          blob: blob + " armed cqarm"
        });
      } else {
        docsOnly.push({
          s: "CQDOC:" + r.id,
          id: r.id,
          path: p,
          field: fk,
          name: label,
          group: r.group || "",
          category: r.category || "",
          extra: "CryptoQuant catalog-only · not harvested (token/symbol/pair or matrix/entity-list)",
          chartable: false,
          type: "onchain",
          cat: "chain",
          blob: blob + " catalog cqdoc"
        });
      }
    });
    return {
      generated_at: onchain.generated_at || seriesDoc.generated_at || feed.generated_at || "",
      plan_note: onchain.plan_note || spec.plan_note || universe.plan_note || "Professional tier: 1y API window; series accrue daily toward 2000d; 2010+ context via Coin Metrics twins",
      series: series,
      twins: twins,
      btc: seriesDoc.btc || null,
      onchain: onchain,
      feed: feed,
      spec: spec,
      catalog: cat.catalog || cat,
      universe: universe,
      chartable: chartable,
      snaps: snaps,
      armed: armed,
      docs: docsOnly,
      n_series: chartable.length,
      n_snaps: snaps.length,
      n_armed: armed.length,
      n_docs: docsOnly.length,
      n_feed: Object.keys(feedMetrics).length,
      n_twins: Object.keys(twins).length,
      n_v1: universe.n_v1 || 0,
      n_v2: universe.n_v2 || 0
    };
  }

  function load(fetchFn) {
    if (PACK) return Promise.resolve(PACK);
    if (PENDING) return PENDING;
    fetchFn = fetchFn || global.fetch;
    PENDING = Promise.all([
      loadJson("/data/cryptoquant-series.json", fetchFn).catch(function () { return {}; }),
      loadJson("/data/cryptoquant-onchain.json", fetchFn).catch(function () { return {}; }),
      loadJson("/data/cq-feed.json", fetchFn).catch(function () { return {}; }),
      loadJson("/data/cq-catalog.json", fetchFn).catch(function () { return {}; }),
      loadJson("/data/config/cryptoquant-spec.json", fetchFn).catch(function () { return {}; }),
      loadJson("/cq-universe.json", fetchFn).catch(function () {
        return loadJson("/assets/cq-universe.json", fetchFn).catch(function () { return {}; });
      })
    ]).then(function (arr) {
      PACK = build({ series: arr[0], onchain: arr[1], feed: arr[2], catalog: arr[3], spec: arr[4], universe: arr[5] });
      PENDING = null;
      return PACK;
    }, function (err) {
      PENDING = null;
      throw err;
    });
    return PENDING;
  }

  function pack() { return PACK; }

  function reset() { PACK = null; PENDING = null; }

  function searchHits(q, limit) {
    limit = limit || 24;
    var n = String(q || "").toLowerCase().replace(/[^a-z0-9:+.\- /_]+/g, " ").replace(/\s+/g, " ").trim();
    if (!n || !PACK) return [];
    if (/cq|on.?chain|cryptoquant/.test(n)) limit = Math.max(limit, 120);
    var out = [], i, row, sc;
    function score(blob, s, name) {
      if (s.toLowerCase() === n || s.toLowerCase() === "cq:" + n) return 100;
      if (String(name || "").toLowerCase() === n) return 96;
      if (blob.indexOf(n) === 0) return 90;
      if (("cq:" + blob).indexOf(n) >= 0) return 88;
      if (blob.indexOf(n) >= 0) return 80;
      var parts = n.split(" ");
      if (parts.length > 1 && parts.every(function (p) { return p && blob.indexOf(p) >= 0; })) return 74;
      return 0;
    }
    for (i = 0; i < PACK.chartable.length; i++) {
      row = PACK.chartable[i];
      sc = score(row.blob, row.s, row.name);
      if (sc) out.push({ s: row.s, name: row.name, extra: row.extra, type: "onchain", cat: "chain", chartable: true, score: sc, suggest: true });
    }
    for (i = 0; i < PACK.snaps.length; i++) {
      row = PACK.snaps[i];
      sc = score(row.blob, row.s, row.name);
      if (sc) out.push({ s: row.s, name: row.name, extra: row.extra, type: "onchain", cat: "chain", chartable: false, score: Math.min(sc, 86), suggest: true });
    }
    for (i = 0; i < (PACK.armed || []).length; i++) {
      row = PACK.armed[i];
      sc = score(row.blob, row.s, row.name);
      if (sc) out.push({ s: row.s, name: row.name, extra: row.extra, type: "onchain", cat: "chain", chartable: false, score: Math.min(sc, 78), suggest: true });
    }
    for (i = 0; i < (PACK.docs || []).length; i++) {
      row = PACK.docs[i];
      sc = score(row.blob, row.s, row.name);
      if (sc) out.push({ s: row.s, name: row.name, extra: row.extra, type: "onchain", cat: "chain", chartable: false, score: Math.min(sc, 62), suggest: true });
    }
    out.sort(function (a, b) { return (b.score || 0) - (a.score || 0); });
    return out.slice(0, limit);
  }

  function seriesMeta() {
    if (!PACK) return [];
    return PACK.chartable.map(function (r) {
      return { id: r.id, name: r.name, q: [r.id, r.id.replace(/_/g, " "), r.name.toLowerCase()], category: r.category, unit: r.unit };
    });
  }

  function isChartable(sym) {
    var k = String(sym || "").replace(/^CQ:/i, "");
    return !!(PACK && PACK.series && PACK.series[k]);
  }

  function cqRow(k) {
    if (!PACK) return null;
    k = String(k || "");
    var ser = PACK.series[k] || PACK.series[k.toLowerCase()] || null;
    var twin = PACK.twins[k] || PACK.twins[k.toLowerCase()] || null;
    if (!ser) {
      var want = k.toLowerCase().replace(/^btc_/, "");
      Object.keys(PACK.series).forEach(function (id) {
        if (!ser && id.toLowerCase().indexOf(want) >= 0) ser = PACK.series[id];
      });
    }
    if (!twin) {
      var want2 = k.toLowerCase().replace(/^btc_/, "");
      Object.keys(PACK.twins).forEach(function (id) {
        if (!twin && id.toLowerCase().indexOf(want2) >= 0) twin = PACK.twins[id];
      });
    }
    if (ser && twin) return mergeDv(twin, ser);
    return ser || twin || null;
  }

  function klines(sym) {
    var s = String(sym || "");
    if (/^CQSNAP:|^CQARM:|^CQDOC:/i.test(s)) return Promise.resolve(null);
    if (!/^CQ:/i.test(s)) return Promise.resolve(null);
    return load().then(function () {
      var row = cqRow(s.replace(/^CQ:/i, ""));
      if (!row) return null;
      var d = dvBars(row.d, row.v);
      if (d.length < 8) return null;
      var src = "CryptoQuant EOD · " + d.length + " pts " + String(row.d[0]).slice(0, 10) + " → " + String(row.d[row.d.length - 1]).slice(0, 10);
      src += (String(row.d[0]).slice(0, 4) < "2025") ? " · twins+harvest" : " · harvest (not live)";
      return { d: d, src: src };
    });
  }

  function filterPane(q) {
    if (typeof document === "undefined") return;
    q = String(q || "").toLowerCase();
    var nodes = document.querySelectorAll("#pane-cq [data-cqhit]");
    var i;
    for (i = 0; i < nodes.length; i++) {
      var blob = nodes[i].getAttribute("data-cqhit") || "";
      nodes[i].style.display = !q || blob.indexOf(q) >= 0 ? "" : "none";
    }
  }

  function paneHTML(intel) {
    intel = intel || {};
    var P = PACK;
    var h = "<div class='pane' id='pane-cq'>";
    if (!P || !(P.n_series || P.n_armed || P.n_docs)) {
      if (intel.status && intel.status !== "ok") {
        return h + "<div class='card'><div class='card-title'>CRYPTOQUANT</div><div class='stat-sm'>feed unavailable: " + esc(intel.error || "no harvest") + "</div></div></div>";
      }
      return h + "<div class='card'><div class='card-title'>CRYPTOQUANT</div><div class='stat-sm'>harvest not loaded</div></div></div>";
    }
    var on = P.onchain || {};
    var m = on.metrics || {};
    var fc = on.forecasts || {};
    h += "<style>.cq-chart{display:inline-block;font-size:10px;color:var(--cyan);text-decoration:none;border:1px solid var(--cyan);padding:2px 8px;border-radius:4px;letter-spacing:.4px}.cq-chart:hover{background:var(--bbg)}.cq-snap td{font-size:10px}</style>";
    h += "<div class='grid g3' style='margin-bottom:12px'>";
    h += "<div class='card'><div class='card-title'>COMPOSITE ON-CHAIN RISK (z)</div><div class='stat-xl mono " + zcol(on.composite_onchain_risk_z) + "'>" + fmt(on.composite_onchain_risk_z) + "</div><div class='stat-sm'>cryptoquant-onchain · " + P.n_series + " harvest series · " + P.n_twins + " twins to 2010</div></div>";
    h += "<div class='card'><div class='card-title'>HARVEST FUSE</div>";
    h += "<div class='metric'><span class='metric-name'>series (chartable)</span><span class='metric-val mono'>" + P.n_series + "</span></div>";
    h += "<div class='metric'><span class='metric-name'>cq-feed paths</span><span class='metric-val mono'>" + P.n_feed + "</span></div>";
    h += "<div class='metric'><span class='metric-name'>extra snapshots</span><span class='metric-val mono'>" + P.n_snaps + "</span></div>";
    h += "<div class='metric'><span class='metric-name'>armed (next EOD)</span><span class='metric-val mono'>" + P.n_armed + "</span></div>";
    h += "<div class='metric'><span class='metric-name'>catalog-only</span><span class='metric-val mono'>" + P.n_docs + "</span></div>";
    h += "<div class='metric'><span class='metric-name'>public v1+v2</span><span class='metric-val mono'>" + ((P.n_v1 || 0) + (P.n_v2 || 0) || "—") + "</span></div>";
    h += "<div class='metric'><span class='metric-name'>generated</span><span class='metric-val mono'>" + esc(String(P.generated_at).slice(0, 16).replace("T", " ")) + "</span></div></div>";
    h += "<div class='card'><div class='card-title'>PLAN WINDOW</div><div class='stat-sm'>" + esc(P.plan_note) + "</div><div class='stat-sm' style='margin-top:8px'>Search any id from chart.html (CQ:btc_mvrv, CDD, dormancy, MVRV Z, ETH2). Extra fields without a series bank are snapshots. Armed names chart after the next EOD pull — never a fake 2-bar. Token/symbol/pair and age matrices stay catalog-only.</div></div>";
    h += "</div>";
    var im = (intel.metrics) || {};
    var intelKeys = Object.keys(im);
    if (intelKeys.length) {
      h += "<div class='card' style='margin-bottom:12px'><div class='card-title'>INTEL JOIN (" + intelKeys.length + " headlines · not the harvest catalog)</div><div class='grid g5'>";
      intelKeys.forEach(function (k) {
        var mm = im[k] || {};
        h += "<div><div class='stat-sm'>" + esc(nice(k)) + "</div><div class='stat-big mono' style='font-size:18px'>" + fmt(mm.value) + "</div><div class='stat-sm'>" + esc(mm.asof || "") + "</div></div>";
      });
      h += "</div></div>";
    }
    h += "<div class='card' style='margin-bottom:12px'><input id='cqf' placeholder='filter MVRV, CDD, dormancy, a_sopr, ETH2, lightning…' style='width:100%;background:var(--bg1);border:1px solid var(--brd);color:var(--t1);padding:8px 10px;border-radius:6px;font:12px IBM Plex Mono,monospace' oninput='window.JHCqFuse&&JHCqFuse.filterPane(this.value)'></div>";
    var byCat = {};
    P.chartable.forEach(function (row) {
      var c = row.category || "other";
      if (!byCat[c]) byCat[c] = [];
      byCat[c].push(row);
    });
    var catOrder = Object.keys(CATN);
    Object.keys(byCat).forEach(function (ck) {
      if (catOrder.indexOf(ck) < 0) catOrder.push(ck);
    });
    catOrder.forEach(function (ck) {
      var rows = byCat[ck];
      if (!rows || !rows.length) return;
      h += "<div class='card-title' style='margin:14px 0 8px'>" + esc(CATN[ck] || nice(ck)) + " · " + rows.length + "</div>";
      h += "<div class='grid' style='grid-template-columns:repeat(auto-fill,minmax(220px,1fr));margin-bottom:12px'>";
      rows.forEach(function (row) {
        var mm = m[row.id] || {};
        var z = mm.z365;
        h += "<div class='card' data-cqhit='" + esc((row.blob || "").replace(/'/g, "")) + "'>";
        h += "<div class='card-title'>" + esc(row.name) + (row.twin ? " · 2010→" : "") + "</div>";
        h += "<div class='stat-big mono'>" + fmt(mm.value != null ? mm.value : (P.series[row.id] && P.series[row.id].v && P.series[row.id].v[P.series[row.id].v.length - 1])) + "</div>";
        h += "<div class='stat-sm " + zcol(z) + "'>z " + (isFinite(+z) ? ((+z > 0 ? "+" : "") + Number(z).toFixed(2)) : "—") + (isFinite(+mm.pctl_1y) ? " · " + mm.pctl_1y + "th pctl" : "") + "</div>";
        if (mm.hist_read) h += "<div class='stat-sm' style='margin-top:6px'>" + esc(String(mm.hist_read).slice(0, 220)) + "</div>";
        h += "<div style='margin-top:8px'><a class='cq-chart' href='/chart.html?s=CQ:" + esc(row.id) + "'>Chart CQ:" + esc(row.id) + "</a></div>";
        h += "<div class='stat-sm' style='margin-top:4px'>" + esc(row.extra) + "</div></div>";
      });
      h += "</div>";
    });
    if (P.snaps.length) {
      h += "<div class='card' style='margin-top:12px'><div class='card-title'>CQ-FEED EXTRA FIELDS · " + P.snaps.length + " SNAPSHOTS · NO HARVEST SERIES</div>";
      h += "<div class='stat-sm' style='margin-bottom:8px'>Professional feed returns latest+prev only for these sibling fields (aSOPR, STH/LTH SOPR, in-house flow, block interval, liquidation sides, …). Not plotted. Search them from chart.html — click opens this desk.</div>";
      h += "<table class='cq-snap'><tr><th>field</th><th>path</th><th>latest</th><th>prev</th><th>asof</th></tr>";
      P.snaps.forEach(function (sn) {
        var dlt = (typeof sn.value === "number" && typeof sn.prev === "number") ? (sn.value - sn.prev) : null;
        var dc = dlt == null ? "" : (dlt > 0 ? "up" : dlt < 0 ? "dn" : "");
        h += "<tr data-cqhit='" + esc((sn.blob || "").replace(/'/g, "")) + "' data-snap='" + esc(sn.path + ":" + sn.field) + "'><td class='mono'>" + esc(sn.field) + "</td><td class='mono'>" + esc(sn.path) + "</td><td class='mono'>" + fmt(sn.value) + "</td><td class='mono " + dc + "'>" + (dlt == null ? "—" : ((dlt > 0 ? "+" : "") + fmt(dlt))) + "</td><td class='mono'>" + esc(sn.asof || "—") + "</td></tr>";
      });
      h += "</table></div>";
    }
    if (P.armed && P.armed.length) {
      h += "<div class='card' style='margin-top:12px'><div class='card-title'>ARMED · " + P.armed.length + " · AWAITING FIRST EOD PULL</div>";
      h += "<div class='stat-sm' style='margin-bottom:8px'>In the Professional spec (CDD, dormancy, ETH2, lightning, XRP/TRX, v2 MVRV Z / apparent demand, …). History starts at the 1y API window on the next harvest — not invented, not a 2-bar chart. Click a search hit to land here.</div>";
      h += "<table class='cq-snap'><tr><th>id</th><th>field</th><th>path</th><th>group</th></tr>";
      P.armed.forEach(function (row) {
        h += "<tr data-cqhit='" + esc((row.blob || "").replace(/'/g, "")) + "' data-arm='" + esc(row.id) + "'><td class='mono'>" + esc(row.id) + "</td><td class='mono'>" + esc(row.field || "") + "</td><td class='mono'>" + esc(row.path || "") + "</td><td>" + esc(row.group || row.category || "") + "</td></tr>";
      });
      h += "</table></div>";
    }
    if (P.docs && P.docs.length) {
      h += "<div class='card' style='margin-top:12px'><div class='card-title'>CATALOG-ONLY · " + P.docs.length + " · NOT HARVESTED</div>";
      h += "<div class='stat-sm' style='margin-bottom:8px'>Token/symbol/pair endpoints, age-distribution matrices, entity lists, discovery, miner-company directories. Searchable names, no series bank.</div>";
      h += "<table class='cq-snap'><tr><th>id</th><th>path</th><th>group</th></tr>";
      P.docs.forEach(function (row) {
        h += "<tr data-cqhit='" + esc((row.blob || "").replace(/'/g, "")) + "' data-doc='" + esc(row.id) + "'><td class='mono'>" + esc(row.id) + "</td><td class='mono'>" + esc(row.path || "") + "</td><td>" + esc(row.group || row.category || "") + "</td></tr>";
      });
      h += "</table></div>";
    }
    if (fc && (fc.btc || fc.method)) {
      h += "<div class='card' style='margin-top:12px'><div class='card-title'>FORECASTS · PROVISIONAL · LEDGER-GRADED</div>";
      h += "<table><tr><th>asset</th><th>1m</th><th>3m</th><th>6m</th><th>1y</th></tr>";
      ["btc", "eth", "alt_basket"].forEach(function (A) {
        var F = fc[A] || {};
        h += "<tr><td class='mono'>" + esc(A) + "</td>";
        [30, 90, 180, 365].forEach(function (hz) {
          var c = F["h" + hz];
          var e = c && c.exp_pct;
          h += "<td class='mono " + (e > 0 ? "up" : e < 0 ? "dn" : "") + "'>" + (isFinite(+e) ? ((e > 0 ? "+" : "") + e + "%") : "—") + "</td>";
        });
        h += "</tr>";
      });
      h += "</table><div class='stat-sm' style='margin-top:8px'>" + esc(fc.method || "") + "</div></div>";
    }
    if (on.ai_master_brief) {
      h += "<div class='card' style='margin-top:12px'><div class='card-title'>ON-CHAIN AI MASTER BRIEF</div><div class='ai-report'>" + esc(on.ai_master_brief) + "</div></div>";
    }
    if (intel.ai_master_brief && intel.ai_master_brief !== on.ai_master_brief) {
      h += "<div class='card' style='margin-top:12px'><div class='card-title'>CRYPTOQUANT AI MASTER BRIEF (intel join)</div><div class='ai-report'>" + esc(intel.ai_master_brief) + "</div></div>";
    }
    return h + "</div>";
  }

  global.JHCqFuse = {
    load: load,
    pack: pack,
    reset: reset,
    searchHits: searchHits,
    seriesMeta: seriesMeta,
    isChartable: isChartable,
    klines: klines,
    paneHTML: paneHTML,
    filterPane: filterPane,
    fmt: fmt,
    esc: esc,
    CATN: CATN
  };
})(typeof window !== "undefined" ? window : globalThis);
