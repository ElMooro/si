var JustHodlRiskArtifactValidation = (function () {
  "use strict";
  var VERSION = "1.0.0";
  var MAX_ARTIFACT_AGE_MS = 2 * 60 * 60 * 1000;
  var FUTURE_TOLERANCE_MS = 5 * 60 * 1000;
  var HOUR_MS = 60 * 60 * 1000;
  var STATUSES = ["OK", "DEGRADED", "DATA_HOLD"];
  var MODES = ["DATA_HOLD", "DEFENSIVE", "SELECTIVE", "SELECTIVE_RISK_ON"];
  var DECISIONS = ["STAY IN CASH / SHORT-TERM TREASURIES", "INVEST SELECTIVELY", "WAIT IN CASH / SHORT-TERM TREASURIES"];
  var SOURCE_STATUSES = ["FRESH", "STALE", "MISSING", "INVALID", "UNKNOWN"];
  var CRITICAL_SOURCES = ["risk_gate", "crisis", "bond_warroom", "eurodollar_stress", "credit_composite"];

  function own(obj, key) { return Object.prototype.hasOwnProperty.call(obj, key); }
  function object(value) { return !!value && typeof value === "object" && !Array.isArray(value); }
  function finite(value) { return typeof value === "number" && Number.isFinite(value); }
  function integer(value) { return finite(value) && value >= 0 && Math.floor(value) === value; }
  function fail(message) { return { ok: false, error: message }; }
  function timestamp(value) {
    if (typeof value !== "string" || !value.trim()) return null;
    var parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  function sameNumber(left, right) { return finite(left) && finite(right) && Math.abs(left - right) < 1e-9; }
  function countBy(rows, status) {
    return rows.reduce(function (count, row) { return count + (row.status === status ? 1 : 0); }, 0);
  }
  function validateFreshSource(row, generatedAt, nowMs) {
    if (!finite(row.age_h) || !finite(row.max_age_h) || row.age_h < 0 || row.max_age_h <= 0 || row.age_h > row.max_age_h) {
      return fail("Critical source " + row.name + " has invalid freshness bounds");
    }
    var asOf = timestamp(row.as_of);
    if (asOf === null) return fail("Critical source " + row.name + " has no valid as_of");
    if (asOf - nowMs > FUTURE_TOLERANCE_MS) return fail("Critical source " + row.name + " is future-dated");
    var generatedAge = Math.max(0, (generatedAt - asOf) / HOUR_MS);
    if (Math.abs(generatedAge - row.age_h) > 0.11) return fail("Critical source " + row.name + " age is contradictory");
    if ((nowMs - asOf) / HOUR_MS > row.max_age_h + (FUTURE_TOLERANCE_MS / HOUR_MS)) {
      return fail("Critical source " + row.name + " is no longer fresh");
    }
    return { ok: true };
  }
  function validateRiskArtifact(data, now) {
    var nowMs = now == null ? Date.now() : (now instanceof Date ? now.getTime() : Number(now));
    if (!Number.isFinite(nowMs)) return fail("Validation time is invalid");
    if (!object(data)) return fail("Malformed risk artifact");
    if (data.engine !== "justhodl-khalid-risk") return fail("Unexpected risk artifact engine");
    if (data.schema_version !== VERSION || data.version !== VERSION) return fail("Unsupported risk schema/version");
    var generatedAt = timestamp(data.generated_at);
    if (generatedAt === null) return fail("generated_at is missing or invalid");
    if (generatedAt - nowMs > FUTURE_TOLERANCE_MS) return fail("generated_at is too far in the future");
    if (nowMs - generatedAt > MAX_ARTIFACT_AGE_MS) return fail("Risk artifact is stale");
    var required = ["as_of", "status", "policy", "capital_decision", "exposure_cap_pct", "risk_score", "plain_english", "coverage", "freshness", "treasury_fails", "domains", "conflicts", "source_health", "reasons", "methodology", "risk_board"];
    var missing = required.filter(function (key) { return !own(data, key); });
    if (missing.length) return fail("Risk artifact is missing " + missing.join(", "));
    if (STATUSES.indexOf(data.status) < 0 || !object(data.policy) || !object(data.risk_board)) return fail("Risk status/policy is invalid");
    var policy = data.policy, board = data.risk_board;
    if (MODES.indexOf(policy.mode) < 0 || board.mode !== policy.mode) return fail("Risk mode is invalid or inconsistent");
    if (DECISIONS.indexOf(data.capital_decision) < 0 || board.capital_decision !== data.capital_decision) return fail("Capital decision is invalid or inconsistent");
    if (typeof policy.allows_new_entries !== "boolean" || board.allows_new_entries !== policy.allows_new_entries ||
        (own(data, "allows_new_entries") && data.allows_new_entries !== policy.allows_new_entries)) {
      return fail("allows_new_entries is missing or inconsistent");
    }
    var cap = data.exposure_cap_pct;
    if (!finite(cap) || cap < 0 || cap > 100 || !sameNumber(policy.exposure_cap_pct, cap) || !sameNumber(board.exposure_cap_pct, cap)) {
      return fail("Exposure cap is invalid or inconsistent");
    }
    if (data.risk_score !== null && (!finite(data.risk_score) || data.risk_score < 0 || data.risk_score > 100)) return fail("Risk score is invalid");
    if (typeof data.plain_english !== "string" || !data.plain_english.trim() || !Array.isArray(data.domains) ||
        !Array.isArray(data.conflicts) || !Array.isArray(data.reasons) || !object(data.methodology) ||
        !object(data.coverage) || !object(data.freshness) || !object(data.treasury_fails) || !Array.isArray(data.source_health)) {
      return fail("Risk artifact shape is invalid");
    }
    if (policy.mode === "DATA_HOLD") {
      if (data.status !== "DATA_HOLD" || policy.allows_new_entries || cap !== 0 ||
          data.capital_decision !== "STAY IN CASH / SHORT-TERM TREASURIES") return fail("DATA_HOLD policy is contradictory");
    } else {
      if (data.status === "DATA_HOLD") return fail("DATA_HOLD status contradicts the policy mode");
      if (policy.mode === "DEFENSIVE") {
        if (policy.allows_new_entries || cap > 10 || data.capital_decision !== "STAY IN CASH / SHORT-TERM TREASURIES") return fail("DEFENSIVE policy is contradictory");
      } else if (!policy.allows_new_entries || cap <= 0 || data.capital_decision !== "INVEST SELECTIVELY" ||
                 (policy.mode === "SELECTIVE" && cap > 50)) return fail("Entry policy is contradictory");
    }
    var seen = {}, badCritical = [];
    for (var i = 0; i < data.source_health.length; i += 1) {
      var row = data.source_health[i];
      if (!object(row) || typeof row.name !== "string" || seen[row.name] || typeof row.critical !== "boolean" ||
          SOURCE_STATUSES.indexOf(row.status) < 0) return fail("Source-health rows are malformed or duplicated");
      seen[row.name] = true;
      if (CRITICAL_SOURCES.indexOf(row.name) >= 0 && row.critical !== true) return fail("Required critical source is not marked critical: " + row.name);
      if (row.critical) {
        if (row.status !== "FRESH") badCritical.push(row.name);
        else {
          var sourceFreshness = validateFreshSource(row, generatedAt, nowMs);
          if (!sourceFreshness.ok) return sourceFreshness;
        }
      }
    }
    for (var c = 0; c < CRITICAL_SOURCES.length; c += 1) {
      if (!seen[CRITICAL_SOURCES[c]]) return fail("Critical source-health row is missing: " + CRITICAL_SOURCES[c]);
    }
    if (badCritical.length && policy.mode !== "DATA_HOLD") return fail("Unhealthy critical source did not force DATA_HOLD");
    var unhealthySources = data.source_health.some(function (row) { return row.status !== "FRESH"; });
    var healthStatus = policy.mode === "DATA_HOLD" ? "DATA_HOLD" : unhealthySources ? "DEGRADED" : "OK";
    if (data.status !== healthStatus) return fail("Risk status contradicts source health");
    var coverage = data.coverage, names = { FRESH: "fresh", STALE: "stale", MISSING: "missing", INVALID: "invalid", UNKNOWN: "unknown" };
    if (coverage.status !== data.status || !integer(coverage.total) || coverage.total !== data.source_health.length) return fail("Coverage status/total is inconsistent");
    var freshCount = 0;
    for (var status in names) {
      if (own(names, status)) {
        var field = names[status], actual = countBy(data.source_health, status);
        if (!integer(coverage[field]) || coverage[field] !== actual) return fail("Coverage " + field + " count is inconsistent");
        if (status === "FRESH") freshCount = actual;
      }
    }
    if (!finite(coverage.ratio) || coverage.ratio < 0 || coverage.ratio > 1 ||
        Math.abs(coverage.ratio - (coverage.total ? freshCount / coverage.total : 0)) > 0.00011) return fail("Coverage ratio is inconsistent");
    var expectedFreshness = data.status === "OK" ? "FRESH" : data.status === "DEGRADED" ? "DEGRADED" : "DATA_HOLD";
    if (data.freshness.status !== expectedFreshness) return fail("Freshness status is inconsistent");
    if (data.treasury_fails.status === "FRESH" &&
        (data.treasury_fails.scope !== "US_TREASURY_INCLUDING_TIPS" || data.treasury_fails.unit !== "USD_bn_par" ||
         !finite(data.treasury_fails.ftd_bn) || !finite(data.treasury_fails.ftr_bn) || !finite(data.treasury_fails.gross_bn))) {
      return fail("Fresh Treasury-fails data is malformed");
    }
    return { ok: true, generatedAt: generatedAt };
  }
  return {
    version: VERSION,
    maxArtifactAgeMs: MAX_ARTIFACT_AGE_MS,
    futureToleranceMs: FUTURE_TOLERANCE_MS,
    validateRiskArtifact: validateRiskArtifact
  };
}());

if (typeof module === "object" && module.exports) {
  module.exports = JustHodlRiskArtifactValidation;
} else {
(function () {
  "use strict";

  var PROXY = (window.JUSTHODL_AUTH_CONFIG && window.JUSTHODL_AUTH_CONFIG.syncBase) || "https://justhodl-data-proxy.raafouis.workers.dev";
  var DATA_PATH = "/data/khalid-risk.json";
  var $ = function (id) { return document.getElementById(id); };

  function isObj(value) { return !!value && typeof value === "object" && !Array.isArray(value); }
  function has(value) { return value !== undefined && value !== null && value !== ""; }
  function node(tag, cls, text) {
    var n = document.createElement(tag);
    if (cls) n.className = cls;
    if (has(text)) n.textContent = String(text);
    return n;
  }
  function clear(n) { while (n && n.firstChild) n.removeChild(n.firstChild); }
  function set(id, value, fallback) {
    var n = $(id);
    if (n) n.textContent = has(value) ? String(value) : (fallback == null ? "—" : String(fallback));
  }
  function path(obj, value) {
    var parts = String(value || "").split(".").filter(Boolean), cur = obj;
    for (var i = 0; i < parts.length; i += 1) {
      if (cur == null) return undefined;
      cur = cur[parts[i]];
    }
    return cur;
  }
  function pick(obj, paths) {
    for (var i = 0; i < paths.length; i += 1) {
      var value = path(obj, paths[i]);
      if (has(value)) return value;
    }
    return undefined;
  }
  function keyForm(value) { return String(value || "").toLowerCase().replace(/[^a-z0-9]/g, ""); }
  function findLoose(obj, names, depth, seen) {
    if (!obj || typeof obj !== "object" || (depth || 0) > 7) return undefined;
    seen = seen || [];
    if (seen.indexOf(obj) >= 0) return undefined;
    seen.push(obj);
    var wanted = names.map(keyForm), keys = Object.keys(obj);
    for (var i = 0; i < keys.length; i += 1) {
      if (wanted.indexOf(keyForm(keys[i])) >= 0 && has(obj[keys[i]])) return obj[keys[i]];
    }
    for (var j = 0; j < keys.length; j += 1) {
      var child = obj[keys[j]];
      if (child && typeof child === "object") {
        var found = findLoose(child, names, (depth || 0) + 1, seen);
        if (found !== undefined) return found;
      }
    }
    return undefined;
  }
  function list(value) {
    if (!has(value)) return [];
    if (Array.isArray(value)) return value.filter(function (x) { return has(x); });
    if (isObj(value)) return Object.keys(value).map(function (key) {
      var item = value[key];
      if (isObj(item)) return Object.assign({ key: key }, item);
      return { label: key, detail: item };
    });
    return [value];
  }
  function label(value) { return String(value || "").replace(/[_-]+/g, " ").replace(/\b\w/g, function (m) { return m.toUpperCase(); }); }
  function fmt(value, unit) {
    if (!has(value)) return "—";
    if (typeof value === "number") {
      var result = Math.abs(value) >= 1000 ? value.toLocaleString(undefined, { maximumFractionDigits: 1 }) : String(Number(value.toFixed(3)));
      return result + (unit || "");
    }
    if (typeof value === "boolean") return value ? "Yes" : "No";
    if (Array.isArray(value)) return value.map(function (x) { return fmt(x); }).join(" · ");
    if (isObj(value)) return "Reported";
    return String(value);
  }
  function timeText(value) {
    if (!has(value)) return "—";
    var t = Date.parse(String(value));
    if (Number.isNaN(t)) return String(value);
    return new Date(t).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
  }
  function severityClass(value) { return keyForm(value || "unknown"); }
  function detailText(item) {
    if (!isObj(item)) return fmt(item);
    return fmt(pick(item, ["detail", "summary", "reason", "message", "description", "value", "status"]));
  }
  function itemLabel(item, fallback) {
    if (!isObj(item)) return fallback;
    return pick(item, ["label", "name", "domain", "key", "code", "severity", "status"]) || fallback;
  }

  async function fetchRisk() {
    var urls = [DATA_PATH, PROXY + DATA_PATH], last;
    for (var i = 0; i < urls.length; i += 1) {
      try {
        var response = await fetch(urls[i] + "?t=" + Date.now(), { cache: "no-store", headers: { Accept: "application/json" } });
        if (!response.ok) throw new Error("HTTP " + response.status);
        var data = await response.json();
        var validation = JustHodlRiskArtifactValidation.validateRiskArtifact(data, Date.now());
        if (!validation.ok) throw new Error(validation.error);
        return data;
      } catch (error) { last = error; }
    }
    throw last || new Error("Risk artifact unavailable");
  }

  function metricObject(data, names) {
    var value = findLoose(data, names);
    return isObj(value) ? value : { value: value };
  }
  function metricValue(metric) {
    return pick(metric, ["value", "level", "count", "amount", "latest", "current", "total"]);
  }
  function renderFails(data) {
    var root = pick(data, ["treasury_fails", "us_treasury_fails", "settlement_fails", "risk_board.us_treasury_fails"]) || {};
    var stats = isObj(root.stats) ? root.stats : {};
    function normalizedMetric(side, valueKey) {
      var legacy = metricObject(root, side === "gross" ? ["combined", "combined_fails", "total_fails", "fails_combined"] : side === "ftd" ? ["fails_to_deliver", "fail_to_deliver", "deliver_fails", "ftd"] : ["fails_to_receive", "fail_to_receive", "receive_fails", "ftr"]);
      if (has(root[valueKey])) {
        return Object.assign({}, isObj(stats[side]) ? stats[side] : {}, {
          value: root[valueKey],
          unit: root.unit === "USD_bn_par" ? " USD bn par" : root.unit,
          regime: root.regime,
          as_of: root.as_of
        });
      }
      return legacy;
    }
    var metrics = [
      ["FAILS TO DELIVER", normalizedMetric("ftd", "ftd_bn")],
      ["FAILS TO RECEIVE", normalizedMetric("ftr", "ftr_bn")],
      ["COMBINED FAILS", normalizedMetric("gross", "gross_bn")]
    ];
    var grid = $("kr-fails-grid"); clear(grid);
    metrics.forEach(function (entry) {
      var metric = entry[1], card = node("article", "kr-fail");
      card.append(node("span", "", entry[0]), node("strong", "", fmt(metricValue(metric), pick(metric, ["unit", "units"]))));
      var dl = node("dl");
      [
        ["Percentile", pick(metric, ["percentile", "pctile", "pct"])],
        ["Z-score", pick(metric, ["z", "z_score", "zscore"])],
        ["Regime", pick(metric, ["regime", "state", "status"])],
        ["As of", timeText(pick(metric, ["as_of", "asof", "date", "timestamp", "updated_at"]))]
      ].forEach(function (pair) {
        var box = node("div"); box.append(node("dt", "", pair[0]), node("dd", "", fmt(pair[1]))); dl.append(box);
      });
      card.append(dl); grid.append(card);
    });
  }

  function renderLineList(id, value, emptyLabel, badge) {
    var box = $(id), items = list(value); clear(box);
    items.forEach(function (item) {
      var row = node("div", "kr-line");
      row.append(node("b", "", itemLabel(item, badge)), node("span", "", detailText(item)));
      box.append(row);
    });
    if (!items.length) {
      var empty = node("div", "kr-line empty");
      empty.append(node("b", "", "NOT REPORTED"), node("span", "", emptyLabel));
      box.append(empty);
    }
    return items.length;
  }

  function renderCoverage(data) {
    var coverage = pick(data, ["coverage", "data_quality.coverage", "risk_board.coverage"]) || {};
    var freshness = pick(data, ["freshness", "data_quality.freshness", "risk_board.freshness"]) || {};
    var target = $("kr-coverage"); clear(target);
    var entries = [
      ["Coverage", pick(coverage, ["ratio", "pct", "percent", "coverage_pct"])],
      ["Fresh", pick(coverage, ["fresh", "fresh_count"])],
      ["Stale", pick(coverage, ["stale", "stale_count"])],
      ["Missing", pick(coverage, ["missing", "missing_count"])],
      ["Freshness", pick(freshness, ["status", "state", "label"])],
      ["Oldest input", pick(freshness, ["oldest_age_h", "max_age_h", "oldest"])]
    ];
    entries.forEach(function (entry) {
      if (!has(entry[1])) return;
      var value = entry[1], suffix = "";
      if (entry[0] === "Coverage" && typeof value === "number") { value = value <= 1 ? value * 100 : value; suffix = "%"; }
      if (entry[0] === "Oldest input" && typeof value === "number") suffix = "h";
      var box = node("div"); box.append(node("dt", "", entry[0]), node("dd", "", fmt(value, suffix))); target.append(box);
    });
    if (!target.children.length) {
      var none = node("div"); none.append(node("dt", "", "Coverage"), node("dd", "", "Not supplied")); target.append(none);
    }
    set("kr-data-state", pick(freshness, ["status", "state"]) || pick(coverage, ["status", "state"]), "UNKNOWN");
  }

  function domainArray(data) {
    var value = pick(data, ["independent_risk_domains", "risk_domains", "risk_board.domains", "domains"]);
    if (Array.isArray(value)) return value;
    if (isObj(value)) return Object.keys(value).map(function (key) {
      return isObj(value[key]) ? Object.assign({ domain: key }, value[key]) : { domain: key, value: value[key] };
    });
    return [];
  }
  function renderDomains(data) {
    var domains = domainArray(data), box = $("kr-domains"); clear(box);
    domains.forEach(function (domain) {
      if (!isObj(domain)) domain = { domain: String(domain) };
      var article = node("article", "kr-domain"), head = node("header");
      var name = pick(domain, ["label", "domain", "name", "key"]) || "Unlabelled domain";
      var severity = pick(domain, ["severity", "regime", "state", "status"]) || "unknown";
      head.append(node("h3", "", label(name)), node("span", "kr-badge " + severityClass(severity), label(severity)));
      article.append(head, node("p", "", pick(domain, ["plain_english", "summary", "interpretation", "reason", "detail"]) || "No interpretation supplied."));
      var dl = node("dl"), skip = /^(label|domain|name|key|severity|regime|state|status|plain_english|summary|interpretation|reason|detail|metrics)$/;
      var metrics = list(domain.metrics);
      if (!metrics.length) {
        Object.keys(domain).filter(function (key) { return !skip.test(key) && !isObj(domain[key]) && !Array.isArray(domain[key]); }).slice(0, 8).forEach(function (key) {
          metrics.push({ label: key, value: domain[key] });
        });
      }
      metrics.forEach(function (metric) {
        var dt = itemLabel(metric, "Metric"), value = isObj(metric) ? pick(metric, ["value", "level", "score", "status"]) : metric;
        var wrap = node("div"); wrap.append(node("dt", "", label(dt)), node("dd", "", fmt(value, isObj(metric) ? metric.unit : ""))); dl.append(wrap);
      });
      article.append(dl); box.append(article);
    });
    if (!domains.length) box.append(node("p", "kr-empty", "No independent risk domains were supplied by the artifact."));
    set("kr-domain-count", domains.length + (domains.length === 1 ? " domain" : " domains"));
  }

  function sourceArray(data) {
    var value = pick(data, ["source_health.sources", "source_health.inputs", "source_health", "inputs", "data_quality.sources"]);
    if (Array.isArray(value)) return value;
    if (isObj(value)) return Object.keys(value).filter(function (key) {
      return !/^(ok|total|healthy|failed|stale|missing|status|ratio|coverage)$/i.test(key);
    }).map(function (key) {
      return isObj(value[key]) ? Object.assign({ source: key }, value[key]) : { source: key, status: value[key] };
    });
    return [];
  }
  function renderSources(data) {
    var sources = sourceArray(data), body = $("kr-sources"); clear(body);
    sources.forEach(function (source) {
      var tr = node("tr");
      var status = pick(source, ["status", "state", "health"]) || "unknown";
      var age = pick(source, ["age_h", "age", "as_of", "asof", "updated_at", "timestamp"]);
      if (typeof age === "number") age = fmt(age, "h");
      else age = timeText(age);
      [
        [pick(source, ["source", "name", "label", "key", "producer"]), ""],
        [status, "kr-status-" + keyForm(status)],
        [age, ""],
        [pick(source, ["coverage", "coverage_pct", "ratio", "detail"]), ""]
      ].forEach(function (cell) { tr.append(node("td", cell[1], fmt(cell[0]))); });
      body.append(tr);
    });
    $("kr-source-empty").hidden = sources.length > 0;
    var health = pick(data, ["source_health", "data_quality.source_health"]) || {};
    var ok = Array.isArray(health) ? sources.filter(function (source) { return String(source.status || "").toUpperCase() === "FRESH"; }).length : pick(health, ["ok", "healthy"]);
    var total = Array.isArray(health) ? sources.length : pick(health, ["total", "count"]);
    set("kr-source-summary", has(ok) && has(total) ? ok + " / " + total + " healthy" : pick(health, ["status", "state"]), "Unknown");
  }

  function render(data) {
    var board = isObj(data.risk_board) ? data.risk_board : data;
    var decision = pick(data, ["risk_board.capital_decision", "capital_decision", "decision.capital_decision", "decision.posture", "posture"]);
    var cap = pick(data, ["risk_board.exposure_cap_pct", "exposure_cap_pct", "risk_control.exposure_cap_pct", "gross_exposure_cap_pct"]);
    var mode = pick(data, ["risk_board.mode", "policy.mode", "risk_board.risk_mode", "risk_control.mode", "risk_mode", "mode"]);
    var asof = pick(data, ["as_of", "asof", "generated_at", "updated_at", "timestamp", "risk_board.as_of"]);
    var interpretation = pick(data, ["plain_english", "interpretation", "risk_board.plain_english", "risk_board.interpretation", "decision.plain_english", "summary"]);

    $("kr-loading").hidden = true; $("kr-error").hidden = true; $("kr-board").hidden = false;
    set("kr-decision", decision, "Decision not supplied");
    set("kr-cap", has(cap) ? (typeof cap === "number" ? fmt(cap, "%") : String(cap)) : null);
    set("kr-mode", mode);
    set("kr-asof", timeText(asof));
    set("kr-interpretation", interpretation, "No plain-English interpretation was supplied by the risk engine.");
    $("kr-title").closest(".kr-decision").classList.toggle("safe", /allow|normal|risk.?on|deploy/i.test(String(decision || "")) && !/hold|defens|cash|wait|veto|block/i.test(String(decision || "")));

    var vetoes = pick(data, ["hard_vetoes", "risk_board.hard_vetoes", "risk_control.hard_vetoes", "vetoes"]);
    var tighteners = pick(data, ["tighteners", "risk_board.tighteners", "risk_control.tighteners", "exposure_tighteners"]);
    set("kr-veto-count", renderLineList("kr-vetoes", vetoes, "No hard-veto list was supplied.", "VETO") + " reported");
    set("kr-tightener-count", renderLineList("kr-tighteners", tighteners, "No tightener list was supplied.", "TIGHTEN") + " reported");
    var conflicts = pick(data, ["conflicts", "disagreements", "risk_board.conflicts", "risk_board.disagreements", "coordination.disagreements"]);
    set("kr-conflict-count", renderLineList("kr-conflicts", conflicts, "No conflict or disagreement list was supplied.", "CONFLICT") + " reported");
    renderCoverage(data); renderFails(data); renderDomains(data); renderSources(data);
  }

  async function load() {
    $("kr-loading").hidden = false; $("kr-error").hidden = true; $("kr-board").hidden = true;
    try { render(await fetchRisk()); }
    catch (error) {
      $("kr-loading").hidden = true; $("kr-board").hidden = true; $("kr-error").hidden = false;
      set("kr-error-copy", "No capital decision can be shown safely. " + (error && error.message ? error.message : "The risk artifact could not be read."));
    }
  }

  $("kr-retry").addEventListener("click", load);
  $("kr-refresh").addEventListener("click", load);
  if (window.JustHodlAuth && window.JustHodlAuth.init) {
    try { window.JustHodlAuth.init(); } catch (error) { /* Anonymous mode remains usable. */ }
  }
  window.JustHodlRisk = {
    render: render,
    load: load,
    normalizeDomains: domainArray,
    sourceArray: sourceArray,
    validateRiskArtifact: JustHodlRiskArtifactValidation.validateRiskArtifact
  };
  load();
}());
}
