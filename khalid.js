(function () {
  "use strict";
  var PROXY = (window.JUSTHODL_AUTH_CONFIG && window.JUSTHODL_AUTH_CONFIG.syncBase) || "https://justhodl-data-proxy.raafouis.workers.dev";
  var state = {
    data: null, filter: "ALL", query: "", assetClass: "ALL", industry: "ALL",
    capBucket: "ALL", sortKey: "score", sortDirection: "desc", page: 1,
    pageSize: 25, detailReturnFocus: null
  };
  var $ = function (id) { return document.getElementById(id); };

  function node(tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (text != null) n.textContent = String(text);
    return n;
  }
  function set(id, value) { var n = $(id); if (n) n.textContent = value == null || value === "" ? "--" : String(value); }
  function pct(value) { return value == null ? "--" : Number(value).toFixed(1) + "%"; }
  function num(value, digits) { return value == null || Number.isNaN(Number(value)) ? "--" : Number(value).toFixed(digits == null ? 1 : digits); }
  function ago(iso) {
    var t = Date.parse(iso || "");
    if (Number.isNaN(t)) return "unknown";
    var m = Math.max(0, Math.round((Date.now() - t) / 60000));
    return m < 2 ? "now" : m < 60 ? m + "m ago" : m < 2880 ? Math.round(m / 60) + "h ago" : Math.round(m / 1440) + "d ago";
  }
  function actionLabel(value) { return String(value || "WAIT").replace(/_/g, " "); }
  function line(label, copy) {
    var row = node("div", "k-line");
    row.append(node("b", "", label), node("span", "", copy));
    return row;
  }
  function clear(n) { while (n.firstChild) n.removeChild(n.firstChild); }
  function textValue(value) {
    if (value == null || value === "") return "--";
    if (Array.isArray(value)) return value.map(textValue).join(" · ");
    if (typeof value === "object") {
      return Object.keys(value).map(function (key) {
        var item = value[key];
        return key.replace(/_/g, " ") + ": " + (typeof item === "object" ? textValue(item) : String(item));
      }).join(" · ");
    }
    return String(value);
  }
  function criteriaCount(row) {
    var value = row.criteria;
    if (Array.isArray(value)) return value.length;
    if (value && typeof value === "object") return Object.keys(value).filter(function (key) { return value[key] === true || (value[key] && value[key].passed === true); }).length;
    return value == null ? null : Number(value);
  }
  function industryLabel(row) { return row.industry || row.category || row.sector || null; }
  function sortValue(row, key) {
    if (key === "criteria") return criteriaCount(row);
    if (key === "industry") return industryLabel(row);
    if (key === "vs_200d") return row.technical && row.technical.vs_200d_pct;
    if (key === "rr") return row.risk_reward && row.risk_reward.ratio;
    if (key === "stage") return row.discovery_stage || row.action;
    return row[key];
  }
  function compareRows(a, b) {
    var av = sortValue(a, state.sortKey), bv = sortValue(b, state.sortKey);
    var aNull = av == null || av === "" || (typeof av === "number" && Number.isNaN(av));
    var bNull = bv == null || bv === "" || (typeof bv === "number" && Number.isNaN(bv));
    if (aNull || bNull) return aNull === bNull ? String(a.ticker || "").localeCompare(String(b.ticker || "")) : (aNull ? 1 : -1);
    var result = typeof av === "number" && typeof bv === "number"
      ? av - bv
      : String(av).localeCompare(String(bv), undefined, { numeric: true, sensitivity: "base" });
    return state.sortDirection === "asc" ? result : -result;
  }

  async function fetchData() {
    var paths = ["/data/khalid.json", PROXY + "/data/khalid.json"];
    var last;
    for (var i = 0; i < paths.length; i += 1) {
      try {
        var res = await fetch(paths[i] + (paths[i].indexOf("?") > -1 ? "&" : "?") + "t=" + Date.now(), { cache: "no-store", headers: { Accept: "application/json" } });
        if (!res.ok) throw new Error("HTTP " + res.status);
        return await res.json();
      } catch (err) { last = err; }
    }
    throw last || new Error("Khalid data is unavailable");
  }

  function renderCommand(data) {
    var stance = data.stance || "WAIT";
    set("stance", actionLabel(stance));
    $("stance").className = "k-stance " + (/BUY/.test(stance) ? "ready" : /CASH|DEFENSIVE|HOLD/.test(stance) ? "defensive" : "");
    set("confidence", Math.round((data.confidence || 0) * 100) + "% confidence");
    set("headline", data.decision && data.decision.headline);
    set("plain-english", data.decision && data.decision.plain_english);
    set("setup-score", data.score == null ? "--" : Math.round(data.score));
    set("risk-score", data.risk_score == null ? "--" : Math.round(data.risk_score));
    set("risk-mode", data.risk_control && data.risk_control.mode);
    set("shelter", data.risk_control && data.risk_control.default_shelter && data.risk_control.default_shelter.primary);
    set("shelter-why", data.risk_control && data.risk_control.default_shelter && data.risk_control.default_shelter.why);
    set("bond-regime", data.risk_control && data.risk_control.bond_market && data.risk_control.bond_market.regime);
    set("bond-summary", data.risk_control && data.risk_control.bond_market && (data.risk_control.bond_market.summary || data.risk_control.bond_market.note));
    set("capital-decision", (data.risk_board && data.risk_board.capital_decision) || (data.decision && data.decision.capital_decision));
    set("exposure-cap", ((data.risk_board && data.risk_board.exposure_cap_pct) == null ? "--" : data.risk_board.exposure_cap_pct + "%"));
    $("capital-banner").className = "k-capital-banner " + ((data.risk_board && data.risk_board.allows_new_entries) ? "invest" : "cash");
    var reasons = $("risk-reasons"); clear(reasons);
    ((data.risk_control && data.risk_control.reasons) || []).forEach(function (x) { reasons.append(line("GATE", x)); });

    var kpis = $("kpis"); clear(kpis);
    [
      ["READY", data.decision && data.decision.selected_count],
      ["HIGH CONVICTION", data.decision && data.decision.high_conviction_count],
      ["TRACKED", data.decision && data.decision.opportunities_tracked],
      ["EXECUTION UNIVERSE", data.decision && data.decision.universe_scored],
      ["FRESH INPUTS", data.coverage && data.coverage.fresh],
      ["STALE / MISSING", data.coverage ? (data.coverage.stale + data.coverage.missing) : null]
    ].forEach(function (item) {
      var box = node("div", "k-kpi");
      box.append(node("span", "", item[0]), node("strong", "", item[1] == null ? "--" : item[1]));
      kpis.append(box);
    });
  }

  function opportunities(data) {
    return (data.opportunity_radar && data.opportunity_radar.length)
      ? data.opportunity_radar
      : [].concat(data.selected || [], data.building_bases || [], data.watch_reclaims || [], data.crypto_watch || []);
  }
  function fillSelect(id, values, allLabel) {
    var select = $(id), current = select.value || "ALL"; clear(select);
    var all = node("option", "", allLabel); all.value = "ALL"; select.append(all);
    values.filter(Boolean).filter(function (value, index, allValues) { return allValues.indexOf(value) === index; })
      .sort().forEach(function (value) {
        var option = node("option", "", value); option.value = value; select.append(option);
      });
    select.value = values.indexOf(current) > -1 ? current : "ALL";
  }
  function filteredOpportunities() {
    var q = state.query.toUpperCase();
    return opportunities(state.data).filter(function (r) {
      return (state.filter === "ALL" || r.action === state.filter || r.discovery_stage === state.filter) &&
        (state.assetClass === "ALL" || r.asset_class === state.assetClass) &&
        (state.industry === "ALL" || industryLabel(r) === state.industry) &&
        (state.capBucket === "ALL" || r.cap_bucket === state.capBucket) &&
        (!q || String(r.ticker || "").toUpperCase().indexOf(q) > -1 ||
          String(r.name || "").toUpperCase().indexOf(q) > -1 ||
          String(industryLabel(r) || "").toUpperCase().indexOf(q) > -1);
    }).sort(compareRows);
  }
  function renderOpportunities() {
    var list = $("opportunity-list"); clear(list);
    var changes = $("opportunity-changes"); clear(changes);
    var changeData = state.data.opportunity_changes || {};
    [["NEW", changeData.new], ["PROMOTED", changeData.promoted], ["DEMOTED", changeData.demoted], ["DATA HOLD", changeData.held], ["DORMANT", changeData.dormant]].forEach(function (item) {
      var chip = node("span"); chip.append(node("b", "", (item[1] || []).length), document.createTextNode(" " + item[0])); changes.append(chip);
    });
    var allRows = opportunities(state.data);
    fillSelect("asset-class-filter", allRows.map(function (row) { return row.asset_class; }), "All asset classes");
    fillSelect("industry-filter", allRows.map(industryLabel), "All industries / categories");
    fillSelect("cap-filter", allRows.map(function (row) { return row.cap_bucket; }), "All cap buckets");
    $("asset-class-filter").value = state.assetClass;
    $("industry-filter").value = state.industry;
    $("cap-filter").value = state.capBucket;
    var rows = filteredOpportunities();
    rows.slice(0, 9).forEach(function (row) {
      var card = node("article", "k-card");
      card.tabIndex = 0; card.setAttribute("role", "button"); card.setAttribute("aria-label", "Open " + row.ticker + " analysis");
      var top = node("div", "k-card-top");
      var ident = node("div"); ident.append(node("span", "k-ticker", row.ticker), node("span", "k-name", row.name || row.asset_class));
      var status = row.discovery_stage || row.action;
      var badge = node("span", "k-action " + (status === "ENTRY_READY" ? "ready" : ""), actionLabel(status));
      top.append(ident, badge);
      var score = node("div", "k-card-score");
      var meter = node("div", "k-meter"); var fill = node("i"); fill.style.width = Math.max(0, Math.min(100, row.score || 0)) + "%"; meter.append(fill);
      score.append(meter, node("strong", "", num(row.score, 1)));
      var copy = node("p", "k-card-copy", row.plain_english || "Evidence is still being assembled.");
      var facts = node("div", "k-card-facts");
      [
        ["ASSET", row.asset_class || "--"],
        ["SOURCES", row.source_count == null ? "--" : row.source_count],
        ["ENTRY", actionLabel((row.entry_trigger && row.entry_trigger.state) || row.action || "WAIT")]
      ].forEach(function (x) { var f = node("div", "k-fact"); f.append(node("span", "", x[0]), node("b", "", x[1])); facts.append(f); });
      card.append(top, score, copy, facts);
      card.addEventListener("click", function () { showDetail(row, card); });
      card.addEventListener("keydown", function (ev) { if (ev.key === "Enter" || ev.key === " ") { ev.preventDefault(); showDetail(row, card); } });
      list.append(card);
    });
    var body = $("opportunity-table-body"); clear(body);
    var pageCount = Math.max(1, Math.ceil(rows.length / state.pageSize));
    state.page = Math.min(state.page, pageCount);
    var start = (state.page - 1) * state.pageSize;
    rows.slice(start, start + state.pageSize).forEach(function (row) {
      var tr = node("tr");
      var tickerCell = node("td"), open = node("button", "k-table-open", row.ticker);
      open.type = "button"; open.setAttribute("aria-label", "Open " + row.ticker + " analysis");
      open.addEventListener("click", function () { showDetail(row, open); }); tickerCell.append(open);
      var values = [
        tickerCell, row.asset_class, num(row.score, 1),
        criteriaCount(row) == null ? "--" : criteriaCount(row),
        industryLabel(row) || "--", row.cap_bucket || "--",
        row.technical && row.technical.vs_200d_pct != null ? pct(row.technical.vs_200d_pct) : "--",
        row.risk_reward && row.risk_reward.ratio != null ? num(row.risk_reward.ratio, 2) + "x" : "--",
        actionLabel(row.discovery_stage || row.action)
      ];
      values.forEach(function (value) {
        if (value && value.nodeType) tr.append(value);
        else tr.append(node("td", "", value));
      });
      body.append(tr);
    });
    document.querySelectorAll("[data-sort]").forEach(function (button) {
      var th = button.closest("th");
      th.setAttribute("aria-sort", button.dataset.sort === state.sortKey ? (state.sortDirection === "asc" ? "ascending" : "descending") : "none");
    });
    set("page-status", "Page " + state.page + " of " + pageCount + " · " + rows.length + " results");
    $("page-prev").disabled = state.page <= 1;
    $("page-next").disabled = state.page >= pageCount;
    $("opportunity-empty").hidden = rows.length > 0;
  }

  function renderAssets(data) {
    var grid = $("asset-grid"); clear(grid);
    (data.asset_views || []).forEach(function (view) {
      var card = node("article", "k-asset " + String(view.stance || "").toLowerCase());
      card.append(node("h3", "", view.asset_class), node("b", "", actionLabel(view.stance)), node("p", "", view.reason));
      grid.append(card);
    });
    var domains = $("domain-list"); clear(domains);
    (data.domains || []).forEach(function (d) {
      var row = node("div", "k-domain");
      var bar = node("div", "k-domain-bar"); var fill = node("i"); fill.style.width = Math.max(0, Math.min(100, d.score || 0)) + "%"; bar.append(fill);
      row.append(node("span", "k-domain-name", d.domain), bar, node("span", "k-domain-state", actionLabel(d.direction)));
      domains.append(row);
    });
    var crypto = $("crypto-list"); clear(crypto);
    (data.crypto_watch || []).slice(0, 7).forEach(function (r) { crypto.append(line(r.ticker, r.plain_english)); });
    if (!(data.crypto_watch || []).length) crypto.append(line("CLEAR", "No crypto setup meets the long-horizon watch criteria."));
  }

  function renderRisk(data) {
    var risks = $("risk-list"), contradictions = $("contradiction-list"), catalysts = $("catalyst-list");
    clear(risks); clear(contradictions); clear(catalysts);
    (data.risks || []).slice(0, 14).forEach(function (x) { risks.append(line(x.severity || x.scope, x.risk)); });
    (data.contradictions || []).slice(0, 14).forEach(function (x) { contradictions.append(line(x.ticker || "SYSTEM", x.issue)); });
    (data.catalysts || []).slice(0, 14).forEach(function (x) { catalysts.append(line(x.ticker || "SYSTEM", x.label + ": " + x.detail)); });
    set("risk-count", (data.risks || []).length);
    set("contradiction-count", (data.contradictions || []).length);
    set("catalyst-count", (data.catalysts || []).length);
    if (!(data.risks || []).length) risks.append(line("CLEAR", "No active system veto is reported."));
    if (!(data.contradictions || []).length) contradictions.append(line("NONE", "No material conflict is reported in the top queue."));
    if (!(data.catalysts || []).length) catalysts.append(line("NONE", "No verified catalyst cleared the current queue."));
    var board = data.risk_board || {};
    set("risk-board-decision", board.capital_decision);
    set("risk-board-cap", board.exposure_cap_pct == null ? "--" : board.exposure_cap_pct + "%");
    set("risk-board-score", board.risk_score == null ? "--" : num(board.risk_score, 1));
    set("risk-board-method", board.method);
    var domains = $("risk-board-domains"); clear(domains);
    (board.domains || []).forEach(function (domain) {
      var card = node("article", "k-risk-card severity-" + String(domain.severity || "unknown").toLowerCase());
      var head = node("div", "k-risk-card-head");
      var title = node("div"); title.append(node("span", "", domain.label), node("small", "", domain.domain + " · " + domain.status + (domain.age_h == null ? "" : " · " + num(domain.age_h, 1) + "h")));
      head.append(title, node("strong", "", domain.score == null ? actionLabel(domain.severity) : num(domain.score, 1)));
      card.append(head, node("p", "", domain.summary));
      var metrics = node("div", "k-risk-metrics");
      (domain.metrics || []).forEach(function (metric) {
        metrics.append(line(metric.label, textValue(metric.value) + (metric.unit || "")));
      });
      card.append(metrics); domains.append(card);
    });
    var boardConflicts = $("board-conflicts"); clear(boardConflicts);
    (board.conflicts || []).forEach(function (conflict) { boardConflicts.append(line(conflict.label, conflict.detail)); });
    if (!(board.conflicts || []).length) boardConflicts.append(line("NONE", "No material disagreement is reported across authoritative lenses."));
    set("board-conflict-count", (board.conflicts || []).length);
    var breadth = $("breadth-clusters"); clear(breadth);
    (data.breadth_clusters || []).forEach(function (cluster) {
      var card = node("article", "k-breadth-card");
      card.append(node("span", "k-action", actionLabel(cluster.state)), node("h3", "", cluster.group), node("strong", "", num(cluster.score, 1)));
      card.append(node("p", "", cluster.plain_english));
      card.append(line("LEADERS", (cluster.leaders || []).join(", ")));
      breadth.append(card);
    });
    if (!(data.breadth_clusters || []).length) breadth.append(node("p", "k-empty", "No peer group has enough members for breadth confirmation."));
  }

  function renderMethod(data) {
    var hard = $("hard-gates"), rules = $("evidence-rules"), body = $("input-table");
    clear(hard); clear(rules); clear(body);
    ((data.methodology && data.methodology.hard_gates) || []).forEach(function (x) { hard.append(node("li", "", x)); });
    ((data.methodology && data.methodology.evidence_rules) || []).forEach(function (x) { rules.append(node("li", "", x)); });
    (data.inputs || []).forEach(function (input) {
      var tr = node("tr");
      [
        input.key, input.domain, input.status,
        input.age_h == null ? "--" : num(input.age_h, 1) + "h",
        input.max_age_h + "h", input.producer
      ].forEach(function (value, i) {
        var td = node("td", i === 2 ? "status-" + String(value).toLowerCase() : "", value);
        tr.append(td);
      });
      body.append(tr);
    });
    var coverage = data.coverage || {};
    set("coverage-badge", Math.round((coverage.ratio || 0) * 100) + "% FRESH");
  }

  function showDetail(row, opener) {
    var box = $("detail-content"); clear(box);
    var title = node("h2", "", row.ticker); title.id = "detail-title";
    box.append(node("p", "k-eyebrow", row.asset_class + " · " + actionLabel(row.discovery_stage || row.action)), title, node("p", "k-detail-summary", row.plain_english));
    var stats = node("div", "k-detail-grid");
    [
      ["Discovery", num(row.score, 1)], ["Coverage", pct(row.component_coverage == null ? null : row.component_coverage * 100)], ["Entry", actionLabel((row.entry_trigger && row.entry_trigger.state) || row.action)],
      ["RSI", num(row.technical && row.technical.rsi, 1)], ["R / R", row.risk_reward && row.risk_reward.ratio != null ? num(row.risk_reward.ratio, 2) + "x" : "--"], ["Dilution", pct(row.dilution && row.dilution.yoy_pct)]
    ].forEach(function (x) { var s = node("div", "k-detail-stat"); s.append(node("span", "", x[0]), node("b", "", x[1])); stats.append(s); });
    box.append(stats);
    box.append(node("h3", "", "CLASSIFICATION + MOMENTUM"));
    box.append(
      line("INDUSTRY / CATEGORY", industryLabel(row) || "--"),
      line("CAP BUCKET", row.cap_bucket || "--"),
      line("MARKET CAP", row.market_cap == null ? "--" : textValue(row.market_cap)),
      line("MOMENTUM", textValue(row.momentum))
    );
    box.append(node("h3", "", "CRITERIA + GATES"));
    box.append(line("CRITERIA", textValue(row.criteria)), line("GATES", textValue(row.gates)));
    if (row.lifecycle) {
      box.append(node("h3", "", "LIFECYCLE"));
      box.append(
        line("FIRST SEEN", ago(row.lifecycle.first_seen)),
        line("OBSERVATIONS", row.lifecycle.observations),
        line("PRIOR STAGE", actionLabel(row.lifecycle.prior_stage || "NEW")),
        line("MAX SCORE", num(row.lifecycle.max_score, 1))
      );
    }
    if ((row.why_underappreciated || []).length) {
      box.append(node("h3", "", "WHY IT MAY BE UNDERAPPRECIATED"));
      row.why_underappreciated.forEach(function (x) { box.append(line("EDGE", x)); });
    }
    if ((row.catalysts || []).length) {
      box.append(node("h3", "", "POTENTIAL UNLOCKS"));
      row.catalysts.forEach(function (x) { box.append(line("CATALYST", x)); });
    }
    box.append(node("h3", "", "RISK / REWARD"));
    var rr = row.risk_reward || {}, dump = row.dump_risk || {};
    box.append(
      line("R / R", rr.ratio == null ? "--" : num(rr.ratio, 2) + "x"),
      line("PIVOT / STOP / TARGET", [rr.pivot, rr.stop, rr.target_2 || rr.target_1].map(textValue).join(" / ")),
      line("EMPIRICAL DUMP LOSS", dump.empirical_loss_pct == null ? "--" : pct(dump.empirical_loss_pct))
    );
    if (dump.structural_estimate) {
      box.append(line(
        dump.structural_estimate.label || "STRUCTURAL DUMP-RISK ESTIMATE — NOT A PROBABILITY",
        pct(dump.structural_estimate.estimated_loss_pct) + " · " + (dump.structural_estimate.method || "")
      ));
    }
    (dump.evidence || []).forEach(function (item) { box.append(line("DUMP EVIDENCE", textValue(item))); });
    if ((row.risks || []).length) {
      box.append(node("h3", "", "THESIS RISKS"));
      row.risks.forEach(function (x) { box.append(line("RISK", x)); });
    }
    box.append(node("h3", "", "ENTRY DISCIPLINE"));
    var entry = row.entry_trigger || {};
    box.append(line("DAILY", entry.daily || "--"), line("4H", entry.four_hour || "--"), line("INVALIDATE", entry.invalidation || "--"));
    Object.keys(row.evidence || {}).forEach(function (key) {
      var items = row.evidence[key] || [];
      if (!items.length) return;
      box.append(node("h3", "", key.toUpperCase()));
      items.forEach(function (item) {
        var e = node("div", "k-evidence-item");
        e.append(node("b", "", (item.label || key) + (item.value == null ? "" : " · " + item.value)), node("span", "", (item.detail || "--") + " [" + (item.source || "unspecified") + "]"));
        box.append(e);
      });
    });
    if ((row.vetoes || []).length) { box.append(node("h3", "", "VETOES")); row.vetoes.forEach(function (x) { box.append(line("BLOCK", x)); }); }
    if ((row.cautions || []).length) { box.append(node("h3", "", "CAUTIONS")); row.cautions.forEach(function (x) { box.append(line("WATCH", x)); }); }
    var detail = $("detail");
    state.detailReturnFocus = opener || document.activeElement;
    detail.inert = false;
    detail.classList.add("open");
    detail.setAttribute("aria-hidden", "false");
    $("detail-close").focus();
  }
  function closeDetail() {
    var detail = $("detail");
    if (!detail.classList.contains("open")) return;
    detail.classList.remove("open");
    detail.setAttribute("aria-hidden", "true");
    detail.inert = true;
    if (state.detailReturnFocus && state.detailReturnFocus.isConnected) {
      state.detailReturnFocus.focus();
    }
    state.detailReturnFocus = null;
  }

  function render(data) {
    state.data = data;
    $("loading").hidden = true; $("error").hidden = true; $("dashboard").hidden = false;
    var live = document.querySelector(".k-live");
    live.className = "k-live " + (data.status === "OK" ? "" : data.status === "DEGRADED" ? "stale" : "bad");
    set("feed-status", data.status || "UNKNOWN");
    set("updated-at", "Updated " + ago(data.generated_at));
    renderCommand(data); renderOpportunities(); renderAssets(data); renderRisk(data); renderMethod(data);
  }
  async function load() {
    $("loading").hidden = false; $("error").hidden = true;
    try { render(await fetchData()); }
    catch (err) {
      $("loading").hidden = true; $("dashboard").hidden = true; $("error").hidden = false;
      set("error-copy", "Khalid could not load its decision artifact. " + err.message);
      document.querySelector(".k-live").className = "k-live bad"; set("feed-status", "OFFLINE");
    }
  }

  document.addEventListener("click", function (ev) {
    var view = ev.target.closest("[data-view]");
    if (view) {
      document.querySelectorAll("[data-view]").forEach(function (x) { x.classList.toggle("active", x === view); });
      var target = $(view.dataset.view); if (target) target.scrollIntoView({ behavior: "smooth", block: "start" });
    }
    var filter = ev.target.closest("[data-filter]");
    if (filter) {
      state.filter = filter.dataset.filter;
      state.page = 1;
      document.querySelectorAll("[data-filter]").forEach(function (x) { x.classList.toggle("active", x === filter); });
      renderOpportunities();
    }
    var sort = ev.target.closest("[data-sort]");
    if (sort) {
      if (state.sortKey === sort.dataset.sort) {
        state.sortDirection = state.sortDirection === "asc" ? "desc" : "asc";
      } else {
        state.sortKey = sort.dataset.sort;
        state.sortDirection = sort.dataset.sort === "score" ? "desc" : "asc";
      }
      state.page = 1;
      renderOpportunities();
    }
  });
  $("asset-search").addEventListener("input", function (ev) { state.query = ev.target.value.trim(); state.page = 1; renderOpportunities(); });
  [
    ["asset-class-filter", "assetClass"],
    ["industry-filter", "industry"],
    ["cap-filter", "capBucket"]
  ].forEach(function (binding) {
    $(binding[0]).addEventListener("change", function (ev) {
      state[binding[1]] = ev.target.value;
      state.page = 1;
      renderOpportunities();
    });
  });
  $("page-prev").addEventListener("click", function () { if (state.page > 1) { state.page -= 1; renderOpportunities(); } });
  $("page-next").addEventListener("click", function () { state.page += 1; renderOpportunities(); });
  $("detail-close").addEventListener("click", closeDetail);
  document.addEventListener("keydown", function (ev) {
    if (ev.key === "Escape") closeDetail();
    if (ev.key === "Tab" && $("detail").classList.contains("open")) {
      ev.preventDefault();
      $("detail-close").focus();
    }
  });
  $("refresh-btn").addEventListener("click", load);
  $("retry-btn").addEventListener("click", load);
  load();
}());
