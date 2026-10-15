/* Corp bonds desk. Reads data/corp-bonds.json. Descriptive only. */
(function () {
  var root = document.getElementById("root");
  var ts = document.getElementById("ts");
  var state = { country: "ALL", q: "", show: 250, tab: "registered", sort: "demand", dir: "desc", sector: "ALL", book: "ALL", iq: "", ishow: 200, onlyChg: false };

  function esc(v) {
    return String(v == null ? "" : v).replace(/[&<>"']/g, function (c) {
    return ({ "&": "\u0026amp;", "<": "\u0026lt;", ">": "\u0026gt;", '"': "\u0026quot;", "'": "\u0026#39;" })[c];
    });
  }
  function num(v, d) {
    if (v == null || !isFinite(Number(v))) return "—";
    return Number(v).toLocaleString("en-US", { maximumFractionDigits: d == null ? 0 : d, minimumFractionDigits: d == null ? 0 : d });
  }
  function signed(v, d) {
    if (v == null || !isFinite(Number(v))) return "—";
    var n = Number(v);
    var cls = n > 0 ? "t-neg" : n < 0 ? "t-pos" : "t-mute";
    return '<span class="' + cls + '">' + (n > 0 ? "+" : "") + num(n, d) + "</span>";
  }
  function money(v) {
    if (v == null || !isFinite(Number(v))) return "—";
    var n = Number(v);
    var abs = Math.abs(n);
    if (abs >= 1e9) return (n / 1e9).toLocaleString("en-US", { maximumFractionDigits: 2, minimumFractionDigits: 2 }) + "bn";
    if (abs >= 1e6) return (n / 1e6).toLocaleString("en-US", { maximumFractionDigits: 1, minimumFractionDigits: 1 }) + "m";
    return num(n, 0);
  }
  function cmp(a, b, key) {
    var av = a[key], bv = b[key];
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    if (typeof av === "number" && typeof bv === "number") return av - bv;
    return String(av).localeCompare(String(bv));
  }
  function sectorBars(rows, labelKey) {
    var list = (rows || []).filter(function (r) { return r.demand != null; }).slice(0, 18);
    if (!list.length) return "<p class='note'>No demand rows in this packet.</p>";
    var max = Math.max.apply(null, list.map(function (r) { return r.demand; }));
    return '<div class="bars">' + list.map(function (r) {
      var width = Math.max(2, Math.round(100 * r.demand / max));
      var name = r[labelKey] || "Not labelled";
      return '<div class="bar-row"><div class="bar-name" title="' + esc(name) + '">' + esc(name) + '</div><div class="bar-track"><div class="bar-fill" style="width:' + width + '%"></div></div><div class="bar-val">' + money(r.demand) + "</div></div>";
    }).join("") + "</div>";
  }
  function issueBonds(book) {
    var q = state.iq.trim().toLowerCase();
    return (book.bonds || []).filter(function (b) {
      if (state.sector !== "ALL" && (b.sector || "") !== state.sector) return false;
      if (state.book !== "ALL" && b.book !== state.book) return false;
      if (state.onlyChg && b.chg == null) return false;
      if (!q) return true;
      return (b.issuer + " " + (b.country || "") + " " + (b.sector || "")).toLowerCase().indexOf(q) >= 0;
    });
  }
  function sortedIssues(book) {
    var rows = issueBonds(book).slice();
    var dir = state.dir === "asc" ? 1 : -1;
    rows.sort(function (a, b) {
      var base = cmp(a, b, state.sort);
      if (base === 0) base = cmp(a, b, "issuer");
      return base * dir;
    });
    return rows;
  }
  function companyPanel(p) {
    var book = p.issues;
    if (!book || book.status === "unavailable" || !(book.bonds || []).length) {
      return '<div class="panel"><h3>Company book</h3><p class="note">The holdings files did not print this run. Nothing here is filled in with a zero.</p></div>';
    }
    var sectors = (book.sectors || []).slice().filter(function (r) { return r.sector; }).sort(function (a, b) { return (b.demand || 0) - (a.demand || 0); });
    var issuers = (book.issuers || []).slice().sort(function (a, b) { return (b.demand || 0) - (a.demand || 0); });
    var sectorOpts = '<option value="ALL">All industries</option>' + sectors.map(function (r) {
      return '<option value="' + esc(r.sector) + '"' + (state.sector === r.sector ? " selected" : "") + ">" + esc(r.sector) + "</option>";
    }).join("");
    var books = {};
    (book.bonds || []).forEach(function (b) { if (b.book) books[b.book] = 1; });
    var bookOpts = '<option value="ALL">All books</option>' + Object.keys(books).sort().map(function (name) {
      return '<option value="' + esc(name) + '"' + (state.book === name ? " selected" : "") + ">" + esc(name) + "</option>";
    }).join("");
    function on(sort, dir, only) {
      return state.sort === sort && state.dir === dir && !!state.onlyChg === !!only ? " on" : "";
    }
    var presets = '<div class="chips">'
      + '<button class="chip' + on("sector", "asc", false) + '" data-sort="sector" data-dir="asc">Industry A–Z</button>'
      + '<button class="chip' + on("sector", "desc", false) + '" data-sort="sector" data-dir="desc">Industry Z–A</button>'
      + '<button class="chip' + on("chg", "desc", true) + '" data-sort="chg" data-dir="desc" data-chg="1">Biggest rate increase</button>'
      + '<button class="chip' + on("chg", "asc", true) + '" data-sort="chg" data-dir="asc" data-chg="1">Biggest rate decrease</button>'
      + '<button class="chip' + on("demand", "desc", false) + '" data-sort="demand" data-dir="desc">Demand by company</button>'
      + '<button class="chip' + on("rate", "desc", false) + '" data-sort="rate" data-dir="desc">Highest yield</button>'
      + '<button class="chip' + on("rate", "asc", false) + '" data-sort="rate" data-dir="asc">Lowest yield</button>'
      + "</div>";
    var filters = '<div class="chips">' + presets + '</div><div class="chips">'
      + '<select class="search" id="sec">' + sectorOpts + "</select>"
      + '<select class="search" id="book">' + bookOpts + "</select>"
      + '<input class="search" id="iq" placeholder="Issuer, industry, country" value="' + esc(state.iq) + '">'
      + "</div>";
    var all = sortedIssues(book);
    var rows = all.slice(0, state.ishow);
    function th(key, label) {
      var mark = state.sort === key ? (state.dir === "asc" ? " ↑" : " ↓") : "";
      return '<th data-col="' + key + '" style="cursor:pointer">' + label + mark + "</th>";
    }
    var head = "<tr>" + th("issuer", "Issuer") + th("sector", "Industry") + th("country", "Country") + th("book", "Book") + th("ccy", "Ccy") + th("coupon", "Coupon") + th("maturity", "Maturity") + th("rate", "Yield") + th("chg", "Yield chg") + th("demand", "Demand") + "</tr>";
    var body = rows.map(function (b) {
      var chgBp = b.chg == null ? null : b.chg * 100;
      return "<tr><td>" + esc(b.issuer) + "</td><td>" + esc(b.sector || "—") + "</td><td>" + esc(b.country || "—") + "</td><td class='t-mute'>" + esc(b.book || "") + "</td><td>" + esc(b.ccy || "—") + "</td><td>" + num(b.coupon, 2) + "</td><td>" + esc(b.maturity || "—") + "</td><td>" + (b.rate == null ? "<span class='t-mute'>—</span>" : num(b.rate, 2) + "%") + "</td><td>" + signed(chgBp, 1) + "</td><td>" + money(b.demand) + "</td></tr>";
    }).join("");
    var more = all.length > rows.length
      ? '<p class="note"><button class="chip" id="imore">Show more (' + rows.length + " of " + all.length + ")</button></p>"
      : '<p class="note">' + rows.length + " bonds in this filter.</p>";
    var changeNote = book.n_with_change
      ? (num(book.n_with_change, 0) + " bonds have a yield change versus the previous snapshot.")
      : "Yield change is blank on this run because there is no previous snapshot yet. The level is real. The change prints on the next desk run.";
    return '<div class="panel"><h3>Demand by industry <span class="tag">' + num(book.n_sectors, 0) + " industries · " + money(book.demand) + " held</span></h3>" + sectorBars(sectors, "sector") + "</div>"
      + '<div class="panel"><h3>Demand by company <span class="tag">largest issuers in the scanned funds</span></h3>' + sectorBars(issuers, "issuer") + "</div>"
      + '<div class="panel"><h3>Every scanned bond <span class="tag">' + num(book.n_bonds, 0) + " bonds · " + num(book.n_issuers, 0) + " issuers · " + esc(book.as_of || "") + "</span></h3>"
      + filters
      + "<div class='scroll'><table>" + head + body + "</table></div>" + more
      + "<p class='note'>" + changeNote + " Yield is yield-to-worst when the file prints it, else yield-to-maturity, in percent. The change column is basis points; up is red because a higher yield is stress. Demand is market value inside the scanned funds, summed when more than one fund holds the bond. " + esc(book.note || "") + "</p></div>";
  }

  function rowBy(list, key, value) {
    for (var i = 0; i < (list || []).length; i++) if (list[i][key] === value) return list[i];
    return null;
  }

  function kpis(p) {
    var sleeves = p.sleeves || [];
    var ig = rowBy(sleeves, "id", "us_ig");
    var hy = rowBy(sleeves, "id", "us_hy");
    var em = rowBy(sleeves, "id", "em");
    var asia = rowBy(sleeves, "id", "asia_usd");
    var euro = p.euro || {};
    var breadth = (((p.trace || {}).corporate || {}).breadth || {}).rows || [];
    var all = rowBy(breadth, "category", "all securities");
    var cards = [
      ["US IG OAS", ig ? num(ig.level, 0) + " bp" : "—", ig ? "5y pct " + num(ig.pct_5y, 0) : "FRED", "info"],
      ["US HY OAS", hy ? num(hy.level, 0) + " bp" : "—", hy ? signed(hy.chg_5d, 1) + " bp / 5d" : "FRED", "neg"],
      ["EM corp OAS", em ? num(em.level, 0) + " bp" : "—", em ? em.as_of : "FRED", "gold"],
      ["Asia USD OAS", asia ? num(asia.level, 0) + " bp" : "—", asia ? "5y pct " + num(asia.pct_5y, 0) : "FRED", "accent"],
      ["Euro bonds", num(euro.n_bonds, 0), num(euro.n_issuers, 0) + " issuers · " + (euro.file_date || "ECB"), "info"],
      ["TRACE issues", all ? num((all.advances || 0) + (all.declines || 0) + (all.unchanged || 0), 0) : "—", all ? num(all.advances, 0) + " up / " + num(all.declines, 0) + " down" : "tape", "pos"]
    ];
    return '<div class="kpi-grid">' + cards.map(function (c) {
      return '<div class="kpi ' + c[3] + '"><div class="label">' + esc(c[0]) + '</div><div class="val">' + c[1] + '</div><div class="sub">' + c[2] + "</div></div>";
    }).join("") + "</div>";
  }

  function sleeveChart(p) {
    var rows = (p.sleeves || []).filter(function (r) { return r.kind === "oas" && r.level != null; });
    if (!rows.length) return '<p class="note">No OAS sleeves in this packet yet.</p>';
    var max = Math.max.apply(null, rows.map(function (r) { return r.level; }));
    return '<div class="bars">' + rows.map(function (r) {
      var hot = r.pct_5y != null && r.pct_5y >= 80;
      var warm = r.pct_5y != null && r.pct_5y >= 60 && r.pct_5y < 80;
      var cls = hot ? "hot" : warm ? "warm" : "";
      var width = Math.max(2, Math.round(100 * r.level / max));
      return '<div class="bar-row"><div class="bar-name">' + esc(r.label) + '</div><div class="bar-track"><div class="bar-fill ' + cls + '" style="width:' + width + '%"></div></div><div class="bar-val">' + num(r.level, 0) + " bp</div></div>";
    }).join("") + "</div>";
  }

  function sleeveTable(p) {
    var rows = p.sleeves || [];
    if (!rows.length) return '<p class="note">Sleeves still loading.</p>';
    var head = "<tr><th>Sleeve</th><th>Region</th><th></th><th>Level</th><th>1d</th><th>5d</th><th>1m</th><th>3m</th><th>1y</th><th>5y pct</th><th>As of</th></tr>";
    var body = rows.map(function (r) {
      var unit = r.unit === "bp" ? " bp" : "%";
      return "<tr><td>" + esc(r.label) + "</td><td class='t-mute'>" + esc(r.region) + "</td><td class='t-mute'>" + esc(r.fred) + "</td><td>" + num(r.level, r.unit === "bp" ? 0 : 2) + unit + "</td><td>" + signed(r.chg_1d, 1) + "</td><td>" + signed(r.chg_5d, 1) + "</td><td>" + signed(r.chg_21d, 1) + "</td><td>" + signed(r.chg_63d, 1) + "</td><td>" + signed(r.chg_252d, 1) + "</td><td>" + num(r.pct_5y, 0) + "</td><td class='t-mute'>" + esc(r.as_of) + "</td></tr>";
    }).join("");
    return "<div class='scroll'><table>" + head + body + "</table></div><p class='note'>OAS is basis points. A wider spread is stress, so positive changes are red. 5y pct is where today's level sits inside the sleeve's own trailing five years. Gold and red bars on the chart are the 60th and 80th percentiles.</p>";
  }

  function breadthTable(block) {
    var rows = (block && block.rows) || [];
    if (!rows.length) return "<p class='note'>This tape did not answer.</p>";
    var head = "<tr><th>Grade</th><th>Up</th><th>Down</th><th>Unch</th><th>Trades</th><th>Volume</th><th>52w high</th><th>52w low</th></tr>";
    var body = rows.map(function (r) {
      return "<tr><td>" + esc(r.category) + "</td><td class='t-pos'>" + num(r.advances, 0) + "</td><td class='t-neg'>" + num(r.declines, 0) + "</td><td>" + num(r.unchanged, 0) + "</td><td>" + num(r.trades, 0) + "</td><td>" + num(r.volume, 1) + "</td><td>" + num(r.high_52w, 0) + "</td><td>" + num(r.low_52w, 0) + "</td></tr>";
    }).join("");
    return "<table>" + head + body + "</table><p class='note'>As of " + esc(block.as_of || "") + ". " + esc(block.volume_unit || "") + ".</p>";
  }

  function flowTable(block) {
    var rows = (block && block.rows) || [];
    if (!rows.length) return "<p class='note'>No customer-flow rows.</p>";
    var interesting = rows.filter(function (r) {
      return /buy|sell|all/i.test(r.side || "");
    });
    var head = "<tr><th>Grade</th><th>Side</th><th>Trades</th><th>Transactions</th><th>Volume</th></tr>";
    var body = interesting.map(function (r) {
      var cls = /buy/i.test(r.side) ? "t-pos" : /sell/i.test(r.side) ? "t-neg" : "";
      return "<tr><td>" + esc(r.category) + "</td><td class='" + cls + "'>" + esc(r.side) + "</td><td>" + num(r.trades, 0) + "</td><td>" + num(r.transactions, 0) + "</td><td>" + num(r.volume, 1) + "</td></tr>";
    }).join("");
    return "<div class='scroll'><table>" + head + body + "</table></div>";
  }

  function tracePanel(p) {
    var trace = p.trace || {};
    var book = state.tab === "144a" ? (trace.rule_144a || {}) : (trace.corporate || {});
    var chips = '<div class="chips"><button class="chip' + (state.tab === "registered" ? " on" : "") + '" data-tab="registered">Registered corporates</button><button class="chip' + (state.tab === "144a" ? " on" : "") + '" data-tab="144a">Rule 144A</button></div>';
    return chips + '<div class="split"><div><h3 style="margin-top:0">Breadth <span class="tag">' + esc((book.breadth || {}).as_of || "") + '</span></h3>' + breadthTable(book.breadth) + '</div><div><h3 style="margin-top:0">Who is lifting <span class="tag">customer buy vs sell</span></h3>' + flowTable(book.sentiment) + "</div></div>";
  }

  function wall(euro) {
    var rows = (euro && euro.maturity_wall) || [];
    if (!rows.length) return "<p class='note'>No euro maturity wall yet.</p>";
    var max = Math.max.apply(null, rows.map(function (r) { return r.n; }));
    return '<div class="wall" title="Bonds maturing each year">' + rows.map(function (r) {
      var h = Math.max(4, Math.round(110 * r.n / max));
      var label = String(r.year).slice(2);
      return '<i style="height:' + h + 'px" title="' + esc(r.year) + ": " + r.n + '"><span>' + (rows.length > 18 && Number(label) % 2 ? "" : label) + "</span></i>";
    }).join("") + "</div>";
  }

  function countries(euro) {
    var rows = (euro && euro.by_country) || [];
    var max = rows.reduce(function (m, r) { return Math.max(m, r.n); }, 1);
    return '<div class="bars">' + rows.map(function (r) {
      var on = state.country === r.country ? " hot" : "";
      return '<div class="bar-row"><div class="bar-name">' + esc(r.country) + '</div><div class="bar-track"><div class="bar-fill' + on + '" style="width:' + Math.round(100 * r.n / max) + '%"></div></div><div class="bar-val">' + num(r.n, 0) + "</div></div>";
    }).join("") + "</div>";
  }

  function filteredBonds(euro) {
    var q = state.q.trim().toLowerCase();
    return ((euro && euro.bonds) || []).filter(function (b) {
      if (state.country !== "ALL" && b.country !== state.country) return false;
      if (!q) return true;
      return (b.issuer + " " + b.isin).toLowerCase().indexOf(q) >= 0;
    });
  }

  function bondTable(euro) {
    var all = filteredBonds(euro);
    var rows = all.slice(0, state.show);
    var issuers = {};
    (euro.issuers || []).forEach(function (row) { issuers[row.issuer] = row; });
    var chips = '<div class="chips"><button class="chip' + (state.country === "ALL" ? " on" : "") + '" data-ccy="ALL">All countries</button>' +
      (euro.by_country || []).map(function (r) {
        return '<button class="chip' + (state.country === r.country ? " on" : "") + '" data-ccy="' + esc(r.country) + '">' + esc(r.country) + " " + r.n + "</button>";
      }).join("") + "</div>";
    var head = "<tr><th>Maturity</th><th>Years</th><th>Issuer</th><th>ISIN</th><th>Cpn</th><th>NCB</th><th>CDS 5Y</th></tr>";
    var body = rows.map(function (b) {
      var cds = issuers[b.issuer] || {};
      var cdsCell = cds.cds_bp != null ? num(cds.cds_bp, 0) + " bp" : "<span class='t-mute'>—</span>";
      return "<tr><td>" + esc(b.maturity) + "</td><td>" + num(b.years, 1) + "</td><td>" + esc(b.issuer) + "</td><td class='t-mute'>" + esc(b.isin) + "</td><td>" + num(b.coupon, 3) + "</td><td>" + esc(b.country) + "</td><td class='t-gold' title='" + esc(cds.cds_name || "") + "'>" + cdsCell + "</td></tr>";
    }).join("");
    var more = all.length > rows.length ? '<p class="note"><button class="chip" id="more">Show all ' + all.length + " bonds</button></p>" : '<p class="note">' + rows.length + " of " + all.length + " bonds in this filter.</p>";
    return chips + '<input class="search" id="q" placeholder="Issuer or ISIN" value="' + esc(state.q) + '">' +
      "<div class='scroll'><table>" + head + body + "</table></div>" + more;
  }

  function issuerTable(euro) {
    var rows = (euro.issuers || []).slice().sort(function (a, b) { return b.n - a.n; }).slice(0, 40);
    var head = "<tr><th>Issuer</th><th>NCB</th><th>Bonds</th><th>Coupon</th><th>Nearest</th><th>CDS</th></tr>";
    var body = rows.map(function (r) {
      var cpn = r.coupon_min == null ? "—" : num(r.coupon_min, 2) + (r.coupon_max != r.coupon_min ? "–" + num(r.coupon_max, 2) : "");
      return "<tr><td>" + esc(r.issuer) + "</td><td>" + esc(r.country) + "</td><td>" + r.n + "</td><td>" + cpn + "</td><td>" + esc(r.nearest || "") + "</td><td class='t-gold'>" + (r.cds_bp != null ? num(r.cds_bp, 0) + " bp" : "—") + "</td></tr>";
    }).join("");
    return "<div class='scroll'><table>" + head + body + "</table></div>";
  }

  function coverage(p) {
    var c = p.coverage || {};
    var misses = ((p.sources || {}).fred) || {};
    var missed = Object.keys(misses).filter(function (k) { return misses[k] !== "live"; });
    return "<p>" + esc(c.us || "") + "</p><p>" + esc(c.companies || "") + "</p><p>" + esc(c.euro_issues || "") + "</p><p>" + esc(c.international_indices || "") + "</p><p class='note'>Not in this book: " + esc((c.not_issue_level || []).join(" · ")) + "</p>" +
      (missed.length ? "<p class='note'>FRED sleeves that did not print this run: " + esc(missed.join(", ")) + "</p>" : "");
  }

  function render(p) {
    var euro = p.euro || {};
    ts.textContent = "as of " + (p.as_of || "") + " · " + String(p.generated_at || "").replace("T", " ").slice(0, 16) + "Z · v" + (p.version || "");
    root.innerHTML =
      '<div class="read">' + esc(p.read || "") + "</div>" +
      kpis(p) +
      companyPanel(p) +
      '<div class="panel"><h3>Global corporate sleeves <span class="tag">ICE BofA OAS, wider = more stress</span></h3>' + sleeveChart(p) + "</div>" +
      '<div class="panel"><h3>Sleeve changes <span class="tag">bp for OAS, percent for yields</span></h3>' + sleeveTable(p) + "</div>" +
      '<div class="panel"><h3>US TRACE tape <span class="tag">every disseminated corporate print, rolled up</span></h3>' + tracePanel(p) + "</div>" +
      '<div class="split"><div class="panel"><h3>Euro maturity wall <span class="tag">' + num(euro.n_bonds, 0) + " bonds</span></h3>" + wall(euro) + '</div><div class="panel"><h3>Euro book by purchasing NCB</h3>' + countries(euro) + "</div></div>" +
      '<div class="panel"><h3>Issuers <span class="tag">' + num(p.cds_matched_issuers, 0) + " joined to the CDS desk</span></h3>" + issuerTable(euro) + "</div>" +
      '<div class="panel"><h3>Every listed euro bond <span class="tag">' + esc(euro.source_url || "ECB CSPP/PEPP") + "</span></h3>" + bondTable(euro) + "</div>" +
      '<div class="panel"><h3>What this engine covers — and what it refuses to invent</h3>' + coverage(p) + "</div>";
    bind(p);
  }

  function bind(p) {
    Array.prototype.forEach.call(document.querySelectorAll("[data-ccy]"), function (btn) {
      btn.onclick = function () { state.country = btn.getAttribute("data-ccy"); state.show = 250; render(p); };
    });
    Array.prototype.forEach.call(document.querySelectorAll("[data-tab]"), function (btn) {
      btn.onclick = function () { state.tab = btn.getAttribute("data-tab"); render(p); };
    });
    var q = document.getElementById("q");
    if (q) q.oninput = function () { state.q = q.value; state.show = 250; render(p); q.focus(); };
    var more = document.getElementById("more");
    if (more) more.onclick = function () { state.show = 100000; render(p); };
    Array.prototype.forEach.call(document.querySelectorAll("[data-sort]"), function (btn) {
      btn.onclick = function () {
        state.sort = btn.getAttribute("data-sort");
        state.dir = btn.getAttribute("data-dir") || "desc";
        state.onlyChg = btn.getAttribute("data-chg") === "1";
        state.ishow = 200;
        render(p);
      };
    });
    Array.prototype.forEach.call(document.querySelectorAll("[data-col]"), function (th) {
      th.onclick = function () {
        var key = th.getAttribute("data-col");
        if (state.sort === key) state.dir = state.dir === "asc" ? "desc" : "asc";
        else { state.sort = key; state.dir = key === "issuer" || key === "sector" || key === "country" || key === "maturity" || key === "book" || key === "ccy" ? "asc" : "desc"; }
        state.onlyChg = false;
        state.ishow = 200;
        render(p);
      };
    });
    var sec = document.getElementById("sec");
    if (sec) sec.onchange = function () { state.sector = sec.value; state.ishow = 200; render(p); };
    var book = document.getElementById("book");
    if (book) book.onchange = function () { state.book = book.value; state.ishow = 200; render(p); };
    var iq = document.getElementById("iq");
    if (iq) iq.oninput = function () { state.iq = iq.value; state.ishow = 200; render(p); var again = document.getElementById("iq"); if (again) { again.focus(); again.selectionStart = again.selectionEnd = again.value.length; } };
    var imore = document.getElementById("imore");
    if (imore) imore.onclick = function () { state.ishow += 400; render(p); };
  }

  function fail(msg) {
    root.className = "";
    root.innerHTML = '<div class="panel"><h3>Corp bonds packet is not up yet</h3><p class="note">' + esc(msg) + "</p><p class='note'>The engine writes data/corp-bonds.json on its schedule. This page does not invent a tape while that file is missing.</p></div>";
  }

  fetch("/data/corp-bonds.json?t=" + Date.now(), { cache: "no-store" })
    .then(function (r) { if (!r.ok) throw new Error("HTTP " + r.status); return r.json(); })
    .then(function (p) {
      if (!p || p.engine !== "justhodl-corp-bonds") throw new Error("unexpected packet");
      root.className = "";
      render(p);
    })
    .catch(function (err) { fail(err && err.message ? err.message : "fetch failed"); });
})();
