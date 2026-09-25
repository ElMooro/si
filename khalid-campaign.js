/* jh-reskin-skip */
/* Khalid campaign strip v1.0.0 — BOTTOM / ACCUM / PUMP from live justhodl-bottom.
   Projects campaign from data/bottom.json states. Does not invent a second pump feed.
   Marker: KHALID_CAMPAIGN_V1 */
(function () {
  if (window.__jhKhalidCampaign) return;
  window.__jhKhalidCampaign = true;

  var BOTTOM_STATES = { CLIMAX: 1, TESTING: 1, NO_RALLY: 1, NO_TEST_BREAKOUT: 1 };
  var ACCUM_STATES = { ST_CONFIRMED: 1, TRIGGERED: 1 };
  var PUMP_STATES = { MARKUP: 1, COMPLETED: 1 };
  var ABORT_STATES = { FAILED: 1, STOPPED: 1 };

  function campaignOf(row) {
    var s = String((row && row.state) || "").toUpperCase();
    if (PUMP_STATES[s]) return "PUMP";
    if (ACCUM_STATES[s]) return "ACCUM";
    if (BOTTOM_STATES[s]) return "BOTTOM";
    if (ABORT_STATES[s]) return "ABORT";
    return "";
  }

  function isHinge(row) {
    if (!row) return false;
    var slope = Number(row.approach_vol_slope);
    var rng = Number(row.approach_range_x);
    var vr = Number(row.st_vol_ratio_sc);
    if (!isFinite(slope) || !isFinite(rng) || !isFinite(vr)) return false;
    return slope < 0 && rng < 1 && vr <= 0.4;
  }

  function proofs(row) {
    var out = [];
    if (!row) return out;
    if (row.sc_date) out.push({ n: 1, id: "SC", ok: true, note: "climax " + (row.sc_vol_x != null ? Number(row.sc_vol_x).toFixed(1) + "×" : "") });
    else out.push({ n: 1, id: "SC", ok: false, note: "no selling climax" });
    if (row.ar_high != null) out.push({ n: 2, id: "AR", ok: true, note: "rally " + (row.ar_rally_pct != null ? Number(row.ar_rally_pct).toFixed(1) + "%" : "") });
    else out.push({ n: 2, id: "AR", ok: false, note: "no automatic rally" });
    if (row.st_date) out.push({ n: 3, id: "ST", ok: true, note: (row.st_depth_class || "test") + " vol " + (row.st_vol_ratio_sc != null ? Number(row.st_vol_ratio_sc).toFixed(2) + "×SC" : "") });
    else out.push({ n: 3, id: "ST", ok: false, note: "no secondary test" });
    var spring = String(row.st_depth_class || "").toUpperCase() === "SPRING" || row.spring === true;
    out.push({ n: 4, id: "SPRING", ok: spring, note: spring ? "undercut recovered" : (row.st_depth_class || "no spring") });
    var sos = !!(row.trigger_date || (row.marks && row.marks.SOS) || PUMP_STATES[String(row.state || "").toUpperCase()]);
    out.push({ n: 5, id: "SOS/LPS", ok: sos, note: sos ? (row.trigger_date || "markup") : "wait LPS / do not chase SOS" });
    return out;
  }

  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"']/g, function (c) {
      return ({ "&": "&", "<": "<", ">": ">", '"': """, "'": "&#39;" })[c];
    });
  }

  function fetchBottom() {
    var paths = ["/data/bottom.json", "https://justhodl.ai/data/bottom.json"];
    var last;
    function one(i) {
      if (i >= paths.length) return Promise.reject(last || new Error("bottom.json unavailable"));
      return fetch(paths[i] + (paths[i].indexOf("?") > -1 ? "&" : "?") + "t=" + Date.now(), {
        cache: "no-store",
        headers: { Accept: "application/json" }
      }).then(function (res) {
        if (!res.ok) throw new Error("HTTP " + res.status);
        return res.json();
      }).catch(function (err) {
        last = err;
        return one(i + 1);
      });
    }
    return one(0);
  }

  function project(doc) {
    var board = (doc && (doc.board || doc.board_all)) || [];
    var rows = [];
    var seen = {};
    board.forEach(function (r) {
      if (!r || !r.ticker) return;
      var key = String(r.ticker).toUpperCase() + "|" + (r.frame || "D");
      if (seen[key]) return;
      seen[key] = 1;
      var camp = campaignOf(r);
      if (!camp) return;
      rows.push({
        ticker: String(r.ticker).toUpperCase(),
        name: r.company || r.name || "",
        frame: r.frame || "D",
        state: r.state,
        campaign: camp,
        grade: r.grade || "",
        score: r.score,
        last: r.last,
        hinge: isHinge(r),
        proofs: proofs(r),
        sc_date: r.sc_date,
        st_date: r.st_date,
        trigger_date: r.trigger_date,
        st_vol_ratio_sc: r.st_vol_ratio_sc,
        st_depth_class: r.st_depth_class,
        actionable: !!r.actionable
      });
    });
    rows.sort(function (a, b) {
      var rank = { PUMP: 0, ACCUM: 1, BOTTOM: 2, ABORT: 3 };
      var d = (rank[a.campaign] || 9) - (rank[b.campaign] || 9);
      if (d) return d;
      return (Number(b.score) || 0) - (Number(a.score) || 0);
    });
    return {
      generated_at: doc && doc.generated_at,
      version: doc && (doc.version || doc.schema_version),
      rows: rows,
      counts: {
        BOTTOM: rows.filter(function (r) { return r.campaign === "BOTTOM"; }).length,
        ACCUM: rows.filter(function (r) { return r.campaign === "ACCUM"; }).length,
        PUMP: rows.filter(function (r) { return r.campaign === "PUMP"; }).length,
        ABORT: rows.filter(function (r) { return r.campaign === "ABORT"; }).length,
        HINGE: rows.filter(function (r) { return r.hinge; }).length
      }
    };
  }

  function ensureMount() {
    var existing = document.getElementById("k-campaign");
    if (existing) return existing;
    var host = document.createElement("section");
    host.id = "k-campaign";
    host.className = "k-section";
    host.setAttribute("data-jh-marker", "KHALID_CAMPAIGN_V1");
    host.innerHTML =
      '<div class="k-section-head"><div><p class="k-eyebrow">WYCKOFF CAMPAIGN</p><h2>Bottom · Accumulation · Start of pump</h2></div>' +
      '<span class="k-badge" id="k-campaign-badge">live bottom.json</span></div>' +
      '<p class="k-lede" id="k-campaign-lede">Proofs from the tape: SC → AR → ST → Spring → SOS/LPS. Hinge = volume dry-up + tight range + quiet test. LPS is the entry; do not chase SOS.</p>' +
      '<div class="k-filter-grid" id="k-campaign-filters" aria-label="Campaign filters">' +
      '<button type="button" data-camp="ALL" class="on">All</button>' +
      '<button type="button" data-camp="BOTTOM">Bottom</button>' +
      '<button type="button" data-camp="ACCUM">Accum</button>' +
      '<button type="button" data-camp="PUMP">Pump</button>' +
      '<button type="button" data-camp="HINGE">Hinge</button>' +
      '</div>' +
      '<div id="k-campaign-kpis" class="k-kpi-row"></div>' +
      '<div id="k-campaign-list" class="k-lines"></div>';
    var method = document.getElementById("method");
    var filters = document.querySelector(".k-filter-grid");
    if (filters && filters.parentNode) filters.parentNode.insertBefore(host, filters.nextSibling);
    else if (method && method.parentNode) method.parentNode.insertBefore(host, method);
    else document.body.appendChild(host);
    return host;
  }

  function ensureSafeguards() {
    var grid = document.querySelector(".k-source-grid");
    if (!grid || grid.querySelector("[data-jh-campaign-src]")) return;
    var bits = [
      ["BOTTOM", "Selling climax is stopping action, not a buy. Wait for AR + quiet ST.", "/bottom.html"],
      ["HINGE / SPRINGBOARD", "Vol dry-up + tight range + higher lows is the coil. Break is not the entry — LPS after SOS is.", "/wyckoff-desk.html?tab=bottom"],
      ["PUMP / SOS", "Markup above the AR high is start of the bull run. Do not chase the jump; buy the back-test (LPS).", "/wyckoff-desk.html?tab=pump"]
    ];
    bits.forEach(function (b) {
      var a = document.createElement("a");
      a.href = b[2];
      a.setAttribute("data-jh-campaign-src", b[0]);
      a.innerHTML = "<b>" + esc(b[0]) + "</b><span>" + esc(b[1]) + "</span>";
      grid.appendChild(a);
    });
  }

  var state = { camp: "ALL", proj: null };

  function render() {
    var proj = state.proj;
    var kpis = document.getElementById("k-campaign-kpis");
    var list = document.getElementById("k-campaign-list");
    var badge = document.getElementById("k-campaign-badge");
    if (!proj) {
      if (list) list.innerHTML = "<p>Loading bottom desk…</p>";
      return;
    }
    if (badge) badge.textContent = (proj.version || "1.2.0") + (proj.generated_at ? " · " + String(proj.generated_at).slice(0, 16) : "");
    if (kpis) {
      kpis.innerHTML = ["BOTTOM", "ACCUM", "PUMP", "ABORT", "HINGE"].map(function (k) {
        return "<div><span>" + k + "</span><b>" + (proj.counts[k] || 0) + "</b></div>";
      }).join("");
    }
    var rows = proj.rows.filter(function (r) {
      if (state.camp === "ALL") return r.campaign !== "ABORT";
      if (state.camp === "HINGE") return r.hinge;
      return r.campaign === state.camp;
    }).slice(0, 24);
    if (!list) return;
    if (!rows.length) {
      list.innerHTML = "<p>No names on this campaign slice in the current bottom harvest.</p>";
      return;
    }
    list.innerHTML = rows.map(function (r) {
      var ptxt = r.proofs.map(function (p) { return (p.ok ? "✓" : "·") + p.id; }).join(" ");
      return "<div class=k-line data-t='" + esc(r.ticker) + "'>" +
        "<b>" + esc(r.ticker) + "</b> " +
        "<span class=pill>" + esc(r.campaign) + "</span> " +
        esc(r.state) + " · " + esc(r.grade || "") + " · " + (r.score != null ? Number(r.score).toFixed(0) : "—") +
        (r.hinge ? " · HINGE" : "") +
        " <span>" + esc(ptxt) + "</span></div>";
    }).join("");
    list.querySelectorAll("[data-t]").forEach(function (el) {
      el.style.cursor = "pointer";
      el.onclick = function () {
        var t = el.getAttribute("data-t");
        if (t) location.href = "/chart.html?s=" + encodeURIComponent(t);
      };
    });
  }

  function bindFilters() {
    var box = document.getElementById("k-campaign-filters");
    if (!box) return;
    box.querySelectorAll("[data-camp]").forEach(function (b) {
      b.onclick = function () {
        state.camp = b.getAttribute("data-camp") || "ALL";
        box.querySelectorAll("[data-camp]").forEach(function (x) { x.className = x === b ? "on" : ""; });
        render();
      };
    });
  }

  function boot() {
    ensureMount();
    ensureSafeguards();
    bindFilters();
    fetchBottom().then(function (doc) {
      state.proj = project(doc);
      window.jhKhalidCampaign = state.proj;
      render();
    }).catch(function (err) {
      var list = document.getElementById("k-campaign-list");
      if (list) list.innerHTML = "<p>Bottom desk unavailable: " + esc(err && err.message ? err.message : err) + "</p>";
    });
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
