/* Shared CryptoQuant series, separate proxy histories, reported snapshots and catalog.
 * Only exact primary observations are chartable. Configured/catalog-only rows
 * have no accepted history. Download success does not establish source freshness.
 * Does not call api.cryptoquant.com. Does not invent pre-harvest history.
 */
(function (global) {
  "use strict";
  var PACK = null, PENDING = null, LOAD_GENERATION = 0;
  var SOURCE_KEYS = ["series", "onchain", "feed", "catalog", "spec", "universe"];
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
    apparent_demand: ["apparent demand"],
    number_of_nodes: ["lightning", "ln nodes", "lightning nodes"],
    number_of_channels: ["lightning channels"],
    capacity: ["lightning capacity"],
    etf_flow: ["etf demand", "etf flow"],
    bull_bear: ["bull bear", "bull-bear"]
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
    v = global.JHObservationSeries ? global.JHObservationSeries.numeric(v) : null;
    if (v === null) return "—";
    if (v === 0) return "0";
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
    z = global.JHObservationSeries ? global.JHObservationSeries.numeric(z) : null;
    if (z === null) return "";
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
  function seriesRow(id, ser, twin, onM, specM, sourceDoc) {
    var reportedLabel = (onM && onM.label) || (specM && specM.label);
    var label = typeof reportedLabel === "string" && reportedLabel.trim() ? reportedLabel : nice(id);
    var reportedCategory = (onM && onM.category) || (specM && specM.category);
    var cat = typeof reportedCategory === "string" && reportedCategory.trim() ? reportedCategory : "other";
    var doc = sourceDoc;
    var parsed = global.JHObservationSeries ? global.JHObservationSeries.cq(doc, "CQ:" + id) : null;
    var bars = parsed ? parsed.d : [], records = parsed ? parsed.evidence.records.filter(function(r){return r.accepted;}) : [];
    var dates = records.map(function(r){return r.coordinate.original_period;}).sort();
    var unit = parsed && parsed.evidence.unit || "";
    return { id:id, s:"CQ:"+id, name:label, category:cat, unit:unit, n:bars.length,
      first:dates[0]||"", last:dates[dates.length-1]||"", twin:!!twin,
      extra:parsed ? parsed.src : "Observation parser unavailable", chartable:bars.length>0,
      type:"onchain", cat:"chain", blob:blobOf(id,id.replace(/_/g," "),label,cat,unit,"cryptoquant onchain cq") };
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
    var specByName = Object.create(null);
    var specByPath = Object.create(null);
    var covered = Object.create(null);
    specRows.forEach(function (m) {
      if (!m || !m.name) return;
      specByName[m.name] = m;
      var p = stripPath(m.path);
      specByPath[p] = m;
      if (m.resolved_key) covered[p + "|" + m.resolved_key] = m.name;
    });
    var chartable = [];
    Object.keys(series).forEach(function (id) {
      chartable.push(seriesRow(id, series[id], twins[id], metrics[id], specByName[id], seriesDoc));
    });
    chartable.sort(function (a, b) { return a.id < b.id ? -1 : a.id > b.id ? 1 : 0; });
    var liveCover = Object.create(null);
    chartable.forEach(function (row) {
      var sm = specByName[row.id] || {};
      var p = stripPath(sm.path);
      if (p && sm.resolved_key) liveCover[p + "|" + sm.resolved_key] = row.id;
    });
    var feedMetrics = feed.metrics || {};
    var snaps = [];
    var snapCover = Object.create(null);
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
        var numeric = global.JHObservationSeries && global.JHObservationSeries.numeric;
        var current = numeric ? numeric(fields[fk]) : null, previous = numeric ? numeric(prev[fk]) : null;
        var dlt = current !== null && previous !== null && Number.isFinite(current - previous) ? current - previous : null;
        var extra = fmt(fields[fk]) + " · " + String(row.asof || "").slice(0, 10);
        extra += " · cq-feed reported snapshot · freshness unverified";
        snapCover[path + "|" + fk] = 1;
        snaps.push({
          s: s,
          path: path,
          field: fk,
          name: name,
          value: fields[fk],
          prev: prev[fk],
          dlt: dlt,
          asof: row.asof || "",
          extra: extra,
          chartable: false,
          type: "onchain",
          cat: "chain",
          blob: blobOf(s, path, path.replace(/[\/\-]+/g, " "), fk, fk.replace(/_/g, " "), name, aliases.join(" "), "snapshot cryptoquant cq-feed reported print")
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
          extra: "CryptoQuant configured · accepted primary history unavailable",
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
      plan_note: onchain.plan_note || spec.plan_note || universe.plan_note || "No source plan note reported",
      source_documents: docs,
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
      n_series: docs.series ? chartable.length : null,
      n_chartable: docs.series ? chartable.filter(function(r){return r.chartable;}).length : null,
      n_snaps: docs.feed ? snaps.length : null,
      n_armed: docs.universe ? armed.length : null,
      n_docs: docs.universe ? docsOnly.length : null,
      n_feed: docs.feed ? Object.keys(feedMetrics).length : null,
      n_twins: docs.series ? Object.keys(twins).length : null,
      n_v1: Number.isSafeInteger(universe.n_v1) && universe.n_v1 >= 0 ? universe.n_v1 : null,
      n_v2: Number.isSafeInteger(universe.n_v2) && universe.n_v2 >= 0 ? universe.n_v2 : null
    };
  }

  function load(fetchFn) {
    if (PENDING) return PENDING;
    if (!global.JHObservationCache) return Promise.reject(new Error("Observation cache unavailable"));
    var generation = LOAD_GENERATION, cache = global.JHObservationCache.shared();
    PENDING = Promise.all(SOURCE_KEYS.map(function(key){return cache.read(key, fetchFn);})).then(function(results){
      if (generation !== LOAD_GENERATION) throw new Error("Observation load superseded");
      var docs = {}, statuses = {};
      results.forEach(function(result,index){docs[SOURCE_KEYS[index]]=result.packet;statuses[SOURCE_KEYS[index]]=result.cache;});
      var next = build(docs); next.source_cache = statuses;
      next.coverage_complete = results.every(function(result){return result.packet !== null;});
      next.source_freshness_verified = false; next.calls_eligible = false; next.sizing_eligible = false;
      PACK = next; PENDING = null; return PACK;
    }).catch(function(error){if(generation===LOAD_GENERATION)PENDING=null;throw error;});
    return PENDING;
  }

  function pack() { return PACK; }

  function reset() { LOAD_GENERATION++; PACK = null; PENDING = null; if(global.JHObservationCache)global.JHObservationCache.shared().reset(SOURCE_KEYS); }

  function searchHits(q, limit) {
    limit = limit || 24;
    var n = String(q || "").toLowerCase().replace(/[^a-z0-9:+.\- /_]+/g, " ").replace(/\s+/g, " ").trim();
    if (!n || !PACK) return [];
    if (/cq|on.?chain|cryptoquant|mvrv|sopr|nupl|hashrate|puell|a_sopr|in-house|block.interval|cdd|dormancy|eth2|lightning|mvrv.?z|xrp|trx|stablecoin|mempool|miner|realized|exchange.?flow|whale|ssr|nvt|coin.?day|apparent.?demand|etf.?demand|utxo|hodl|asopr|mpi|netflow|reserve/.test(n)) limit = Math.max(limit, 400);
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
      if (sc) out.push({ s: row.s, name: row.name, extra: row.extra, type: "onchain", cat: "chain", chartable: row.chartable, score: sc, suggest: true });
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
    var k=String(sym||"").replace(/^CQ:/i,"").toLowerCase();
    var matches=PACK?PACK.chartable.filter(function(row){return row.id.toLowerCase()===k;}):[];
    return matches.length===1 && matches[0].chartable;
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
    if (!/^CQ:/i.test(s)) return Promise.resolve(null);
    if (!global.JHObservationSeries || !global.JHObservationCache) return Promise.resolve({d:[],src:"Observation history unavailable: required module not loaded"});
    return global.JHObservationCache.shared().read("series").then(function(result){
      var parsed=global.JHObservationSeries.cq(result.packet,s);parsed.evidence.transport_cache=result.cache;
      parsed.src += " · download " + result.cache.state + " · source freshness unverified";
      return parsed;
    });
  }

  function cacheHTML() {
    if(!global.JHObservationCache)return "<p>Observation downloads unavailable: cache module missing.</p>";
    var cache=global.JHObservationCache.shared();
    return "<div class='card' data-cq-cache><div class='card-title'>SOURCE DOWNLOAD STATUS</div><p>Download checks do not verify observation freshness. Cached packets and rejected replacements remain separate. Counts describe received subsets only.</p><div style='overflow:auto' tabindex='0' role='region' aria-label='Observation source download status'><table><tr><th>Source</th><th>Download state</th><th>Packet received</th><th>Retry</th></tr>"+
      SOURCE_KEYS.map(function(key){var c=cache.status(key);return "<tr><td>"+esc(c.packet_path)+"</td><td>"+esc(global.JHObservationCache.label(c.state))+"</td><td>"+esc(c.received_at||"Unavailable")+"</td><td>"+c.retry_after_s+" s"+(c.last_error?" · "+esc(c.last_error.kind):"")+"</td></tr>";}).join("")+"</table></div></div>";
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
    var h = "<div class='pane' id='pane-cq'>" + cacheHTML();
    if (!P || !(P.n_series || P.n_snaps || P.n_armed || P.n_docs)) {
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
    h += "<div class='card'><div class='card-title'>COMPOSITE ON-CHAIN RISK (z)</div><div class='stat-xl mono " + zcol(on.composite_onchain_risk_z) + "'>" + fmt(on.composite_onchain_risk_z) + "</div><div class='stat-sm'>cryptoquant-onchain · " + fmt(P.n_series) + " received primary series · " + fmt(P.n_twins) + " separate proxy histories</div></div>";
    h += "<div class='card'><div class='card-title'>HARVEST FUSE</div>";
    h += "<div class='metric'><span class='metric-name'>received primary series</span><span class='metric-val mono'>" + fmt(P.n_series) + "</span></div>";
    h += "<div class='metric'><span class='metric-name'>cq-feed paths</span><span class='metric-val mono'>" + fmt(P.n_feed) + "</span></div>";
    h += "<div class='metric'><span class='metric-name'>reported snapshots (cq-feed)</span><span class='metric-val mono'>" + fmt(P.n_snaps) + "</span></div>";
    h += "<div class='metric'><span class='metric-name'>configured, no history</span><span class='metric-val mono'>" + fmt(P.n_armed) + "</span></div>";
    h += "<div class='metric'><span class='metric-name'>catalog-only</span><span class='metric-val mono'>" + fmt(P.n_docs) + "</span></div>";
    h += "<div class='metric'><span class='metric-name'>public v1+v2</span><span class='metric-val mono'>" + (P.n_v1 !== null && P.n_v2 !== null ? fmt(P.n_v1 + P.n_v2) : "—") + "</span></div>";
    h += "<div class='metric'><span class='metric-name'>generated</span><span class='metric-val mono'>" + esc(String(P.generated_at).slice(0, 16).replace("T", " ")) + "</span></div></div>";
    h += "<div class='card'><div class='card-title'>REPORTED SOURCE NOTE</div><div class='stat-sm'>" + esc(P.plan_note) + "</div><div class='stat-sm' style='margin-top:8px'>Search any id from chart.html (CQ:btc_mvrv, CDD, dormancy, MVRV Z, ETH2, lightning, XRP). Snapshot dates and plan notes are source claims, not verified freshness or guaranteed history. Only valid exact primary observations are plotted. Proxy histories are separate; token/symbol/pair and age matrices stay catalog-only.</div></div>";
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
    var byCat = Object.create(null);
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
        var numeric = global.JHObservationSeries && global.JHObservationSeries.numeric;
        var z = numeric ? numeric(mm.z365) : null, percentile = numeric ? numeric(mm.pctl_1y) : null;
        if(percentile !== null && (percentile < 0 || percentile > 100)) percentile = null;
        h += "<div class='card' data-cqhit='" + esc((row.blob || "").replace(/'/g, "")) + "'>";
        h += "<div class='card-title'>" + esc(row.name) + (row.twin ? " · separate proxy" : "") + "</div>";
        h += "<div class='stat-big mono'>" + fmt(mm.value != null ? mm.value : (P.series[row.id] && P.series[row.id].v && P.series[row.id].v[P.series[row.id].v.length - 1])) + "</div>";
        h += "<div class='stat-sm " + zcol(z) + "'>z " + (z !== null ? ((z > 0 ? "+" : "") + fmt(z)) : "—") + (percentile !== null ? " · " + esc(fmt(percentile)) + "th pctl (reported)" : "") + "</div>";
        if (mm.hist_read) h += "<div class='stat-sm' style='margin-top:6px'>" + esc(String(mm.hist_read).slice(0, 220)) + "</div>";
        h += "<div style='margin-top:8px'><a class='cq-chart' href='/chart.html?s=CQ:" + esc(row.id) + "'>Chart CQ:" + esc(row.id) + "</a></div>";
        h += "<div class='stat-sm' style='margin-top:4px'>" + esc(row.extra) + "</div></div>";
      });
      h += "</div>";
    });
    if (P.snaps.length) {
      h += "<div class='card-title' style='margin:14px 0 8px'>REPORTED CQ-FEED SNAPSHOTS · " + P.snaps.length + " PRINTS</div>";
      h += "<div class='stat-sm' style='margin-bottom:8px'>Every received cq-feed field and previous value is retained. Invalid values stay unavailable; dates are reported, freshness is unverified. Snapshots do not create historical bars.</div>";
      h += "<div class='grid' style='grid-template-columns:repeat(auto-fill,minmax(200px,1fr));margin-bottom:12px'>";
      P.snaps.forEach(function (sn) {
        var dc = sn.dlt == null ? "" : (sn.dlt > 0 ? "up" : sn.dlt < 0 ? "dn" : "");
        h += "<div class='card' data-cqhit='" + esc((sn.blob || "").replace(/'/g, "")) + "' data-snap='" + esc(sn.path + ":" + sn.field) + "'>";
        h += "<div class='card-title'>" + esc(sn.name) + "</div>";
        h += "<div class='stat-big mono'>" + fmt(sn.value) + "</div>";
        h += "<div class='stat-sm " + dc + "'>" + (sn.dlt == null ? "—" : ((sn.dlt > 0 ? "+" : "") + fmt(sn.dlt))) + " vs prev · " + esc(sn.asof || "—") + "</div>";
        h += "<div class='stat-sm' style='margin-top:6px'>" + esc(sn.path + " · " + sn.field) + "</div>";
        h += "<div class='stat-sm' style='margin-top:4px'>" + esc(sn.extra) + "</div></div>";
      });
      h += "</div>";
    }
    if (P.armed && P.armed.length) {
      h += "<div class='card' style='margin-top:12px'><div class='card-title'>ARMED · " + P.armed.length + " · PRIMARY HISTORY UNAVAILABLE</div>";
      h += "<div class='stat-sm' style='margin-bottom:8px'>In the Professional spec (CDD, dormancy, ETH2, lightning, XRP/TRX, v2 MVRV Z / apparent demand, …). No history or timing of a future successful harvest is guaranteed. Click a search hit to land here.</div>";
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
      h += "<div class='card' style='margin-top:12px'><div class='card-title'>REPORTED FORECASTS · QUALIFICATION UNVERIFIED</div>";
      h += "<table><tr><th>asset</th><th>1m</th><th>3m</th><th>6m</th><th>1y</th></tr>";
      ["btc", "eth", "alt_basket"].forEach(function (A) {
        var F = fc[A] || {};
        h += "<tr><td class='mono'>" + esc(A) + "</td>";
        [30, 90, 180, 365].forEach(function (hz) {
          var c = F["h" + hz];
          var e = global.JHObservationSeries ? global.JHObservationSeries.numeric(c && c.exp_pct) : null;
          h += "<td class='mono " + (e > 0 ? "up" : e < 0 ? "dn" : "") + "'>" + (e !== null ? ((e > 0 ? "+" : "") + esc(e) + "%") : "—") + "</td>";
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
