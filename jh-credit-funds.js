/* jh-credit-funds.js - Credit Before Equity, held by funds.

   Khalid 2026-11-26: "go" - who holds each bond, how far apart funds mark it, and how funds are positioned in each
   company's credit default swaps.

   What it draws, all of it from what justhodl-credit-before-equity publishes (doc.funds in the board's document, and the
   fund document it names, asked for once):

     the section "Held by funds"   five tiles, protection sold and bought company by company, the securities funds mark
                                   furthest apart, and every list whole: marks apart, flagged, swap names, swap indices,
                                   dealers, how names were tied. Every list sorts on every column, both ways, and saves as a file.
     a company's sheet             what funds hold of it bond by bond and loan by loan, how each is marked, and the credit
                                   default swaps funds hold on its name.

   It measures nothing. Fund holdings are as of each fund's own report date, months before the SEC posts them; a mark is a
   fund's own valuation, not a trade. A figure the engine did not publish is shown as not published, never as zero. */
(function (root) {
  "use strict";
  var B = root.JHCreditBoard;
  if (!B) return;
  var esc = B.esc, fixed = B.fixed, day = B.day, sortRows = B.sortRows;
  function S() { return root.JHCreditSheet || null; }
  function isNum(v) { return typeof v === "number" && isFinite(v); }
  function isObj(v) { return !!v && typeof v === "object" && !Array.isArray(v); }
  function int(v) { return isNum(v) ? Math.round(v).toLocaleString("en-US") : "–"; }
  function num(v, d) { return isNum(v) ? fixed(v, d == null ? 2 : d).replace("-", "−") : "–"; }
  /* Dollars, short: $2.50T, $3.7B, $552M, $6K. signed: a plus on what is above zero. */
  function usd(v, signed) {
    if (!isNum(v)) return "–";
    var a = Math.abs(v), s = a >= 1e12 ? fixed(a / 1e12, 2) + "T" : a >= 1e9 ? fixed(a / 1e9, 1) + "B" : a >= 1e6 ? fixed(a / 1e6, 0) + "M" : a >= 1e3 ? fixed(a / 1e3, 0) + "K" : fixed(a, 0);
    return (v < 0 ? "−" : signed && v > 0 ? "+" : "") + "$" + s;
  }
  function coupon(v) { return isNum(v) ? String(Math.round(v * 1000) / 1000) + "%" : "–"; }
  function plural(n, one, many) { return int(n) + " " + (n === 1 ? one : many); }

  var KIND_WORDS = { DBT: "Bond", LON: "Loan" };
  var HOW_WORDS = { "short name": "Abbreviations spelled out", "co-issuer": "First of two co-issuers", "name cut": "The fund's name was cut short", exact: "The listed company's own name",
                    alias: "A reviewed alias", core: "The same name without its legal tail", subsidiary: "A named subsidiary", truncated: "A name the fund file cut short" };
  var HELD = [[0, "Any size"], [10e6, "$10M or more"], [50e6, "$50M or more"], [250e6, "$250M or more"], [1e9, "$1B or more"]];
  var VIEWS = ["gaps", "flagged", "names", "indices", "dealers", "ties"];
  var FIRST = 25, STEP = 200;

  // ------------------------------------------------------------------------------------------------ the columns of each list
  /* [key, label, kind]. kind: text | co | int | usd | susd | mark | pc | date | year | how | kind */
  var LISTS = {
    gaps: { title: "Marked apart", cols: [["co", "Company", "co"], ["title", "Security", "text"], ["cpn", "Coupon", "pc"], ["mat", "Matures", "date"], ["funds", "Funds holding", "int"], ["n_px", "Funds marking", "int"],
                                          ["px", "Middle mark", "mark"], ["px_lo", "10th percentile", "mark"], ["px_hi", "90th percentile", "mark"], ["gap", "Gap, points", "mark"], ["value", "Value held", "usd"]],
            sort: { key: "gap", dir: "desc" } },
    flagged: { title: "Flagged", cols: [["co", "Company", "co"], ["title", "Security", "text"], ["cpn", "Coupon", "pc"], ["mat", "Matures", "date"], ["funds", "Funds holding", "int"], ["rows", "Positions", "int"],
                                        ["n_default", "In default", "int"], ["n_pik", "Paid in kind", "int"], ["px", "Middle mark", "mark"], ["value", "Value held", "usd"]],
               sort: { key: "value", dir: "desc" } },
    names: { title: "Swap names", cols: [["reference", "Reference name, as funds write it", "text"], ["co", "Company on the board", "co"], ["positions", "Positions", "int"], ["funds", "Funds", "int"],
                                         ["sold", "Protection sold", "usd"], ["bought", "Protection bought", "usd"], ["net_sold", "Net sold", "susd"], ["unclear", "Side unclear", "usd"],
                                         ["gross", "Notional", "usd"], ["not_in_dollars", "Not in dollars", "int"], ["usual_end_year", "Usual end", "year"]],
             sort: { key: "gross", dir: "desc" } },
    indices: { title: "Swap indices", cols: [["reference", "Index", "text"], ["positions", "Positions", "int"], ["funds", "Funds", "int"], ["sold", "Protection sold", "usd"], ["bought", "Protection bought", "usd"],
                                             ["net_sold", "Net sold", "susd"], ["unclear", "Side unclear", "usd"], ["gross", "Notional", "usd"], ["not_in_dollars", "Not in dollars", "int"], ["usual_end_year", "Usual end", "year"]],
               sort: { key: "gross", dir: "desc" } },
    dealers: { title: "Dealers", cols: [["name", "Counterparty, as funds write it", "text"], ["positions", "Credit derivative positions", "int"]], sort: { key: "positions", dir: "desc" } },
    ties_by_rule: { title: "Tied by a naming rule", cols: [["issuer", "Issuer, as funds write it", "text"], ["how", "Rule", "how"], ["co", "Company on the board", "co"], ["securities", "Securities", "int"], ["value", "Value held", "usd"]],
                    sort: { key: "value", dir: "desc" } },
    not_on_board: { title: "Listed, not on the board", cols: [["ticker", "Ticker", "text"], ["issuer", "Issuer, as funds write it", "text"], ["securities", "Securities", "int"], ["value", "Value held", "usd"], ["funds", "Most funds in one", "int"]],
                    sort: { key: "value", dir: "desc" } },
    untied: { title: "Left untied", cols: [["issuer", "Issuer, as funds write it", "text"], ["securities", "Securities", "int"], ["value", "Value held", "usd"], ["funds", "Most funds in one", "int"]], sort: { key: "value", dir: "desc" } }
  };
  /* The columns of a company's own fund-held securities, in its sheet. */
  var ROW_FIELDS = [["title", "Security", "text"], ["issuer", "Issuer, as funds write it", "text"], ["cls", "Kind", "kind"], ["cpn", "Coupon", "pc"], ["mat", "Matures", "date"], ["funds", "Funds holding", "int"],
                    ["rows", "Positions", "int"], ["face", "Face", "usd"], ["value", "Value", "usd"], ["px", "Middle mark", "mark"], ["px_lo", "10th percentile", "mark"], ["px_hi", "90th percentile", "mark"],
                    ["gap", "Gap, points", "mark"], ["n_px", "Funds marking", "int"], ["n_default", "In default", "int"], ["n_arrears", "In arrears", "int"], ["n_pik", "Paid in kind", "int"],
                    ["n_restricted", "Restricted", "int"], ["n_l3", "Priced by model", "int"], ["d", "Marks dated", "date"]];

  // ------------------------------------------------------------------------------------------------ reading the documents
  function block(doc) { return isObj(doc) && isObj(doc.funds) ? doc.funds : null; }
  function whole(fd) { return isObj(fd) && Array.isArray(fd.cols) && Array.isArray(fd.rows); }
  function objects(cols, rows) {
    if (!Array.isArray(cols) || !Array.isArray(rows)) return [];
    return rows.filter(Array.isArray).map(function (r) { var o = {}; cols.forEach(function (c, i) { o[c] = r[i]; }); return o; });
  }
  function minMarks(doc, fd) { var b = block(doc); return isNum((fd || {}).min_marks) ? fd.min_marks : b && isNum(b.min_marks) ? b.min_marks : 5; }
  /* A security's gap between marks: its 90th percentile mark less its 10th, when enough funds mark it. */
  function gapOf(x, min) { return isNum(x.px_hi) && isNum(x.px_lo) && isNum(x.n_px) && x.n_px >= min ? Math.round((x.px_hi - x.px_lo) * 100) / 100 : null; }
  /* The company rows of the board, by id: its ticker or name, for a list that names a company. */
  function namesOf(doc) {
    if (doc && doc.__fuNames) return doc.__fuNames;
    var out = {};
    (B.isV2(doc) ? B.table(doc) : []).forEach(function (r) { out[r.id] = r; });
    if (doc) { try { Object.defineProperty(doc, "__fuNames", { value: out, enumerable: false }); } catch (e) { /* a frozen document: built again next time */ } }
    return out;
  }
  function label(doc, id) { var r = namesOf(doc)[id]; return r ? (r.ticker || r.name || id) : String(id || "").replace(/^[TI]:/, ""); }

  /* The fund document's rows of one company, with each security's gap. */
  function rowsOf(fd, id, doc) {
    if (!whole(fd)) return null;
    var by = fd.__by;
    if (!by) {
      by = {};
      var at = fd.cols.indexOf("co");
      fd.rows.forEach(function (r) { if (Array.isArray(r)) (by[r[at]] = by[r[at]] || []).push(r); });
      try { Object.defineProperty(fd, "__by", { value: by, enumerable: false }); } catch (e) { /* kept for this call only */ }
    }
    var min = minMarks(doc, fd);
    return objects(fd.cols, by[id] || []).map(function (x) { x.gap = gapOf(x, min); return x; });
  }
  /* Every security funds mark apart: the whole list from the fund document, or the first hundred the board's document carries. */
  function gapRows(doc, fd) {
    var b = block(doc);
    if (!b) return { rows: [], total: 0, whole: true };
    if (whole(fd)) {
      if (!fd.__gaps) {
        var min = minMarks(doc, fd), all = [];
        objects(fd.cols, fd.rows).forEach(function (x) { var g = gapOf(x, min); if (g !== null) { x.gap = g; all.push(x); } });
        try { Object.defineProperty(fd, "__gaps", { value: all, enumerable: false }); } catch (e) { return { rows: all, total: all.length, whole: true }; }
      }
      return { rows: fd.__gaps, total: fd.__gaps.length, whole: true };
    }
    return { rows: objects(b.gap_cols, b.gaps), total: isNum(b.n_gaps) ? b.n_gaps : (b.gaps || []).length, whole: !isNum(b.n_gaps) || b.n_gaps <= (b.gaps || []).length };
  }
  function swapRows(cols, rows) {
    return objects(cols, rows).map(function (x) { x.gross = (isNum(x.sold) ? x.sold : 0) + (isNum(x.bought) ? x.bought : 0) + (isNum(x.unclear) ? x.unclear : 0); return x; });
  }
  /* The rows of a list: {rows, total, whole}. whole is false while only the first hundred are in hand. */
  function listRows(id, doc, fd, st) {
    var b = block(doc), cds = b && isObj(b.cds) ? b.cds : {};
    if (!b) return { rows: [], total: 0, whole: true };
    if (id === "gaps") {
      var g = gapRows(doc, fd), held = (st && st.held) || 0;
      return { rows: held ? g.rows.filter(function (x) { return isNum(x.value) && x.value >= held; }) : g.rows, total: g.total, whole: g.whole, filtered: !!held };
    }
    if (id === "flagged") return { rows: objects(b.flag_cols, b.flagged), total: isNum(b.n_flagged) ? b.n_flagged : (b.flagged || []).length, whole: true };
    if (id === "names") {
      var nm = swapRows(cds.cols, cds.names);
      return { rows: st && st.tied ? nm.filter(function (x) { return x.co; }) : nm, total: nm.length, whole: true, filtered: !!(st && st.tied) };
    }
    if (id === "indices") return { rows: swapRows(cds.index_cols, cds.indices), total: (cds.indices || []).length, whole: true };
    if (id === "dealers") return { rows: (cds.counterparties || []).filter(Array.isArray).map(function (c) { return { name: c[0], positions: c[1] }; }), total: (cds.counterparties || []).length, whole: true };
    var colsKey = { ties_by_rule: "tie_cols", not_on_board: "absent_cols", untied: "untied_cols" }[id], nKey = { ties_by_rule: "n_ties_by_rule", not_on_board: "n_not_on_board", untied: "n_untied" }[id];
    if (!colsKey) return { rows: [], total: 0, whole: true };
    var have = whole(fd) && Array.isArray(fd[id]), src = have ? fd[id] : b[id] || [], total = have ? src.length : isNum(b[nKey]) ? b[nKey] : src.length;
    return { rows: objects(b[colsKey] || (fd || {})[colsKey], src), total: total, whole: have || total <= src.length };
  }

  // ------------------------------------------------------------------------------------------------ the state of the section
  function fu(state) {
    if (!isObj(state.fu)) state.fu = { view: "gaps", sorts: {}, shown: {}, held: 0, tied: false };
    var f = state.fu;
    if (VIEWS.indexOf(f.view) < 0) f.view = "gaps";
    if (!isObj(f.sorts)) f.sorts = {};
    if (!isObj(f.shown)) f.shown = {};
    return f;
  }
  function sortOf(f, id) { return f.sorts[id] || (f.sorts[id] = { key: LISTS[id].sort.key, dir: LISTS[id].sort.dir }); }

  // ------------------------------------------------------------------------------------------------ cells and tables
  function cellText(kind, v) {
    if (v == null || v === "") return "–";
    switch (kind) {
      case "int": return int(v);
      case "usd": return usd(v);
      case "susd": return usd(v, true);
      case "mark": return num(v, 2);
      case "pc": return coupon(v);
      case "date": return typeof v === "string" && v.length >= 10 ? day(v) : "–";
      case "year": return String(v);
      case "how": return HOW_WORDS[v] || String(v);
      case "kind": return KIND_WORDS[v] || String(v);
      default: return String(v);
    }
  }
  function alignOf(kind) { return ["text", "co", "how", "kind", "date", "year"].indexOf(kind) >= 0 ? "l" : "r"; }
  function cellHtml(c, x, doc) {
    var v = x[c[0]];
    if (c[2] === "co") {
      if (!v) return '<td class="l"><span class="cb-dim">' + (x.dealer ? "also a dealer's name: tied to none" : x.ticker ? "listed as " + esc(x.ticker) + ", not on the board" : "–") + "</span></td>";
      var sh = S(), text = esc(label(doc, v));
      return '<td class="l">' + (sh ? sh.nameButton({ id: v }, "<b>" + text + "</b>") : "<b>" + text + "</b>") + "</td>";
    }
    return '<td class="' + alignOf(c[2]) + '">' + esc(cellText(c[2], v)) + "</td>";
  }
  /* A list as a table: every column sorts both ways; it opens whole. */
  function listHtml(id, doc, fd, state, opt) {
    var f = fu(state), L = LISTS[id], got = listRows(id, doc, fd, f), sort = sortOf(f, id), shown = f.shown[id] || FIRST;
    var byCo = sort.key === "co";                          // a company column sorts by the name shown, not by the id under it
    var rows = sortRows(byCo ? got.rows.map(function (x) { return Object.assign({ __co: x.co ? label(doc, x.co) : null }, x); }) : got.rows, byCo ? "__co" : sort.key, sort.dir);
    var n = rows.length, cut = rows.slice(0, shown);
    var head = "<thead><tr>" + L.cols.map(function (c) {
      var on = sort.key === c[0];
      return '<th class="' + alignOf(c[2]) + '" aria-sort="' + (on ? (sort.dir === "asc" ? "ascending" : "descending") : "none") + '"><button type="button" data-fu-sort="' + id + ":" + esc(c[0]) + '">' + esc(c[1]) +
        (on ? (sort.dir === "asc" ? " ▲" : " ▼") : "") + "</button></th>";
    }).join("") + "</tr></thead>";
    var body = cut.map(function (x) { return "<tr>" + L.cols.map(function (c) { return cellHtml(c, x, doc); }).join("") + "</tr>"; }).join("") ||
      '<tr><td colspan="' + L.cols.length + '" class="cb-empty">' + (got.filtered ? "Nothing matches the choice above." : "The engine published none.") + "</td></tr>";
    var pager = n > cut.length ? '<div class="cb-pager"><button type="button" data-fu-more="' + id + '">Show ' + Math.min(STEP, n - cut.length).toLocaleString("en-US") + ' more</button><button type="button" data-fu-all="' + id + '">Show all ' +
      n.toLocaleString("en-US") + "</button></div>" : "";
    var rest = got.whole ? "" : fd === false ? ' <span class="cb-warn">The fund document could not be loaded: these are the first ' + got.rows.length + " of " + got.total.toLocaleString("en-US") + ".</span>" :
      " These are the first " + got.rows.length + " of " + got.total.toLocaleString("en-US") + '; the rest is loading. <button type="button" class="cb-link" data-fu-load>Load it now</button>';
    var note = '<p class="cb-note">' + cut.length.toLocaleString("en-US") + " of " + n.toLocaleString("en-US") + " shown" + (got.filtered ? " (" + got.total.toLocaleString("en-US") + " before the choice above)" : "") + "." + rest +
      ' <button type="button" class="cb-link" data-fu-csv="' + id + '">Download ' + (n === 1 ? "it" : "all " + n.toLocaleString("en-US")) + " as a file</button>" + ((opt || {}).note ? " " + opt.note : "") + "</p>";
    return '<div class="cb-scroll" tabindex="0" role="region" aria-label="' + esc(L.title) + '; scroll sideways for all columns"><table class="cb-table cb-fu-t" data-jh-nosort>' + head + "<tbody>" + body + "</tbody></table></div>" + pager + note;
  }
  function csvOf(id, doc, fd, state) {
    var f = fu(state), L = LISTS[id], got = listRows(id, doc, fd, f), sort = sortOf(f, id), sh = S();
    var keys = L.cols.map(function (c) { return c[0]; }).concat(id === "names" ? ["ticker", "dealer"] : []);
    return sh ? sh.csvText(keys, sortRows(got.rows, sort.key, sort.dir)) : "";
  }

  // ------------------------------------------------------------------------------------------------ the two pictures
  /* Protection funds have sold and bought, company by company: bought grows to the left of the line, sold to the right. */
  function butterflyHtml(doc, state) {
    var p = B.PALETTES[state.palette] || B.PALETTES.rg;
    var rows = (B.isV2(doc) ? B.table(doc) : []).filter(function (r) { return (isNum(r.fc_sold) && r.fc_sold > 0) || (isNum(r.fc_bought) && r.fc_bought > 0); })
      .map(function (r) { return { id: r.id, name: r.ticker || r.name, full: r.name, sold: r.fc_sold || 0, bought: r.fc_bought || 0, net: r.fc_net, funds: r.fc_funds }; })
      .sort(function (a, b) { return (b.sold + b.bought) - (a.sold + a.bought); });
    if (!rows.length) return '<p class="cb-empty">No credit default swap in the fund file is tied to a company on the board.</p>';
    var top = rows.slice(0, 14), max = top.reduce(function (m, x) { return Math.max(m, x.sold, x.bought); }, 0) || 1;
    var nSell = rows.filter(function (x) { return x.net > 0; }).length, nBuy = rows.filter(function (x) { return x.net < 0; }).length;
    return '<p class="cb-cap">' + plural(rows.length, "company has", "companies have") + " a fund swap in dollars on their name: funds are net sellers of protection on " + int(nSell) + " and net buyers on " + int(nBuy) +
      ". The fourteen with the most written, either way.</p>" +
      '<div class="cb-fu-key"><span><i style="background:' + p.bad + '"></i>Bought: the fund pays to be protected</span><span><i style="background:' + p.good + '"></i>Sold: the fund carries the credit risk</span></div>' +
      '<div class="cb-fu-rows" role="list">' + top.map(function (x) {
        var tip = x.full + ": funds sold " + usd(x.sold) + " of protection and bought " + usd(x.bought) + "; net " + (x.net > 0 ? "sold " + usd(x.net) : x.net < 0 ? "bought " + usd(-x.net) : "nothing") +
          (isNum(x.funds) ? "; " + plural(x.funds, "fund", "funds") + " at most in one name" : "") + ". Open its sheet.";
        return '<button type="button" class="cb-fu-row cb-fu-bf" role="listitem" data-sheet="' + esc(x.id) + '" title="' + esc(tip) + '"><span class="cb-fu-n">' + esc(x.name) + "</span>" +
          '<span class="cb-fu-v cb-fu-vl">' + (x.bought ? esc(usd(x.bought)) : "") + '</span><span class="cb-fu-l"><i style="width:' + fixed(100 * x.bought / max, 1) + "%;background:" + p.bad + '"></i></span>' +
          '<span class="cb-fu-r"><i style="width:' + fixed(100 * x.sold / max, 1) + "%;background:" + p.good + '"></i></span><span class="cb-fu-v">' + (x.sold ? esc(usd(x.sold)) : "") + "</span></button>";
      }).join("") + "</div>";
  }
  /* A security's marks as a range: the 10th to the 90th percentile mark, set against its own middle mark. */
  function rangeRow(x, span, name, sub, attrs, tip) {
    var lo = x.px_lo - x.px, hi = x.px_hi - x.px, left = 50 + 50 * lo / span, width = Math.max(50 * (hi - lo) / span, 0.8);
    return "<" + (attrs ? 'button type="button" ' + attrs : "div") + ' class="cb-fu-row cb-fu-rg" role="listitem" title="' + esc(tip) + '"><span class="cb-fu-n">' + name + (sub ? '<span class="cb-fu-s">' + sub + "</span>" : "") + "</span>" +
      '<span class="cb-fu-range"><i style="left:' + fixed(Math.max(left, 0), 1) + "%;width:" + fixed(Math.min(width, 100), 1) + '%"></i><b></b></span><span class="cb-fu-v">' + esc(num(x.gap, 2)) + "</span>" + (attrs ? "</button>" : "</div>");
  }
  function rangesHtml(rows, doc, opt) {
    opt = opt || {};
    if (!rows.length) return '<p class="cb-empty">' + esc(opt.empty || "No security is marked by enough funds to read a gap.") + "</p>";
    var span = rows.reduce(function (m, x) { return Math.max(m, Math.abs(x.px_lo - x.px), Math.abs(x.px_hi - x.px)); }, 0) || 1;
    var edge = num(span, span < 10 ? 1 : 0);
    return '<div class="cb-fu-axis" aria-hidden="true"><span></span><span class="cb-fu-ax"><i>−' + esc(edge) + " pts</i><i>middle mark</i><i>+" + esc(edge) + ' pts</i></span><span>Gap</span></div><div class="cb-fu-rows" role="list">' + rows.map(function (x) {
      var what = (x.title || "") + (isNum(x.cpn) ? " " + coupon(x.cpn) : "") + (typeof x.mat === "string" ? " " + x.mat.slice(0, 4) : "");
      var tip = (opt.sheet ? "" : label(doc, x.co) + ": ") + what + ". " + plural(x.n_px, "fund marks", "funds mark") + " it: middle " + num(x.px, 2) + ", one in ten at or under " + num(x.px_lo, 2) + ", one in ten at or over " + num(x.px_hi, 2) +
        "; " + num(x.gap, 2) + " points apart. " + usd(x.value) + " held." + (opt.sheet ? "" : " Open its sheet.");
      return opt.sheet ? rangeRow(x, span, esc(what), esc(plural(x.n_px, "fund", "funds") + " · " + usd(x.value)), "", tip) :
        rangeRow(x, span, esc(label(doc, x.co)), esc(what), 'data-sheet="' + esc(x.co) + '"', tip);
    }).join("") + "</div>";
  }

  // ------------------------------------------------------------------------------------------------ the section on the board
  function tile(labelText, big, sub) { return '<div class="cb-tile-h"><div class="cb-lbl">' + esc(labelText) + '</div><div class="cb-big">' + big + '</div><div class="cb-sub">' + sub + "</div></div>"; }
  function sectionHtml(doc, state, fd) {
    var b = block(doc);
    if (!b) {
      var why = (isObj(doc) && Array.isArray(doc.degraded) ? doc.degraded : []).filter(function (d) { return typeof d === "string" && /fund/i.test(d); })[0];
      return '<p class="cb-empty">' + esc(why || "The engine published no fund layer on this run. Nothing is shown rather than something stale.") + "</p>";
    }
    var f = fu(state), cds = isObj(b.cds) ? b.cds : {}, tot = isObj(cds.totals) ? cds.totals : {}, tied = isObj(b.tied) ? b.tied : {}, read = isObj(b.read) ? b.read : {}, q = isObj(b.quarter) ? b.quarter : {}, sum = isObj(q.summary) ? q.summary : {};
    var cov = isObj(doc.coverage) ? doc.coverage : {}, gaps = gapRows(doc, fd), top = gaps.rows.slice().sort(function (x, y) { return y.gap - x.gap; })[0];
    var five = gaps.rows.filter(function (x) { return x.gap >= 5; }).length, flagged = objects(b.flag_cols, b.flagged);
    var nDef = flagged.filter(function (x) { return x.n_default * 2 >= x.rows && x.n_default > 0; }).length, nPik = flagged.filter(function (x) { return x.n_pik * 2 >= x.rows && x.n_pik > 0; }).length;
    /* Three blocks, each with a heading of its own as its first child: the site numbers every large block of a section and
       hangs a loose badge over one that has no heading (jh-sections.js). */
    var h = '<div class="cb-fu-block" id="funds-file"><h3>The fund file at a glance</h3><p class="cb-dates">SEC Form N-PORT, file <b>' + esc(String(b.file || "").replace(/\.zip$/, "")) + "</b> · holdings as of <b>" + esc(day(b.as_of)) + "</b>, each fund's own report date" +
      (isNum(sum.funds) ? " · " + int(sum.funds) + " funds" : "") + (typeof q.bank_generated_at === "string" ? " · the SEC bank's document of " + esc(day(q.bank_generated_at)) : "") + " · the board's own bonds are of " + esc(day(doc.as_of)) + "</p>";
    h += '<div class="cb-hero">' +
      tile("Bonds and loans tied to a company", int(tied.securities), esc(usd(tied.value)) + " at funds' own marks" + (isNum(tied.value_pct) ? " · " + esc(fixed(tied.value_pct, 1)) + "% of the " + esc(usd(read.value)) + " of company debt in the file" : "")) +
      tile("Companies on the board funds hold", int(tied.companies), (isNum(cov.companies) ? "of " + int(cov.companies) + " · " : "") + int(tied.listed) + " with a listed stock") +
      tile("Marked 5 points or more apart", (gaps.whole || five < gaps.rows.length ? int(five) : int(five) + " or more"), top ? "widest: " + esc(label(doc, top.co)) + ", " + esc(num(top.gap, 1)) + " points among " + esc(plural(top.n_px, "fund", "funds")) : "no gap read") +
      tile("Flagged by most of their holders", int(b.n_flagged), int(nDef) + " in default · " + int(nPik) + " paid in kind") +
      tile("Credit default swaps funds hold", esc(usd(tot.notional_usd)), int(tot.positions) + " positions · " + int(tot.reference_names) + " names, " + int(cds.tied) + " tied to a company · " + int(tot.indices) + " indices") + "</div></div>";
    var heldRows = listRows("gaps", doc, fd, f).rows.slice().sort(function (x, y) { return y.gap - x.gap || (y.value || 0) - (x.value || 0); }).slice(0, 12);
    h += '<div class="cb-fu-block" id="funds-pictures"><h3>In pictures</h3><div class="cb-fu-grid"><div class="cb-fu-card"><h3>Protection funds have sold and bought, company by company</h3>' + butterflyHtml(doc, state) + "</div>" +
      '<div class="cb-fu-card"><h3>The securities funds mark furthest apart</h3><p class="cb-cap">Each line is one bond or loan: from the mark one fund in ten is at or under to the mark one in ten is at or over, set against the middle mark, per 100 of face' +
      (f.held ? ", among those worth " + esc((HELD.filter(function (o) { return o[0] === f.held; })[0] || [0, ""])[1].toLowerCase()) : "") + ". The twelve widest.</p>" +
      rangesHtml(heldRows, doc, { empty: "No security of this size is marked by enough funds to read a gap." }) + "</div></div></div>";
    var counts = { gaps: gaps.total, flagged: b.n_flagged, names: (cds.names || []).length, indices: (cds.indices || []).length, dealers: (cds.counterparties || []).length };
    h += '<div class="cb-fu-block" id="funds-lists"><h3>The lists, each one whole</h3><div class="cb-tctl">' + B.seg("fu-view", VIEWS.map(function (v) {
      return [v, v === "ties" ? "How names were tied" : LISTS[v].title + (isNum(counts[v]) ? " (" + counts[v].toLocaleString("en-US") + ")" : "")]; }), f.view, "List") +
      (f.view === "gaps" ? B.seg("fu-held", HELD.map(function (o) { return [String(o[0]), o[1]]; }), String(f.held || 0), "Value held") : "") +
      (f.view === "names" ? '<button type="button" class="cb-dir" data-fu-tied aria-pressed="' + (f.tied ? "true" : "false") + '">' + (f.tied ? "Showing names tied to a company" : "Show only names tied to a company") + "</button>" : "") + "</div>";
    if (f.view === "ties") {
      var how = isObj(tied.how) ? tied.how : {};
      h += '<p class="cb-note">' + Object.keys(how).map(function (k) {
        return "<b>" + esc(k === "issuer" ? "An issuer on the board is written this way" : k === "cut name" ? "The board's own name was cut short" : HOW_WORDS[k] || k) + "</b> " + int(how[k][0]) + " securities, " + esc(usd(how[k][1])); }).join(" · ") + ".</p>" +
        "<h4>Tied by a naming rule — " + int(b.n_ties_by_rule) + " issuer names</h4>" + listHtml("ties_by_rule", doc, fd, state, { note: "A name more than one company answers to is tied to none." }) +
        "<h4>Listed companies funds hold bonds of that the board does not cover — " + int(b.n_not_on_board) + "</h4>" + listHtml("not_on_board", doc, fd, state) +
        "<h4>Issuers left untied — " + int(b.n_untied) + "</h4>" + listHtml("untied", doc, fd, state, { note: "Governments, agencies, supranational lenders and issuers none of the board's funds hold are here by design." });
    } else {
      var notes = { gaps: "A gap is read only where " + minMarks(doc, fd) + " funds or more mark the security on its commonest date; it is the 90th percentile mark less the 10th.",
                    flagged: "Securities where at least half of the positions funds report are flagged in default, or as paid in kind.",
                    names: "Sold: the fund receives the fixed payment and carries the credit risk. Bought: it pays it. A name funds also write as a dealer is tied to no company: the file cannot tell a reference from a counterparty written in its place. " +
                           "Positions not in dollars are counted and not added up.",
                    indices: "Index families as the SEC bank reads them from the funds' own descriptions.", dealers: "The forty counterparties funds name most on their credit derivatives, exchanges and clearing houses included." };
      h += listHtml(f.view, doc, fd, state, { note: notes[f.view] });
    }
    return h + '</div><p class="cb-note">' + esc(typeof b.reading === "string" ? b.reading : "") + (typeof b.source === "string" ? " Source: " + esc(b.source) + "." : "") + "</p>";
  }

  // ------------------------------------------------------------------------------------------------ a company's sheet
  function kpi(labelText, big, sub) { return '<div class="cb-kpi"><div class="cb-lbl">' + esc(labelText) + '</div><div class="cb-big">' + big + '</div><div class="cb-sub">' + sub + "</div></div>"; }
  function sheetHtml(doc, r, ctx) {
    ctx = ctx || {};
    var b = block(doc);
    if (!b) return '<p class="cb-empty">The engine published no fund layer on this run.</p>';
    var cds = isObj(b.cds) ? b.cds : {}, mine = swapRows(cds.cols, cds.names).filter(function (x) { return x.co === r.id; });
    var has = isNum(r.f_n) && r.f_n > 0;
    var h = '<p class="cb-note">From the SEC\'s Form N-PORT file ' + esc(String(b.file || "").replace(/\.zip$/, "")) + ": what registered funds reported holding as of " + esc(day(b.as_of)) +
      ". A mark is a fund's own valuation per 100 of face, not a trade. None of it enters the spread, the stress rank or the chances above.</p>";
    if (!has && !mine.length) return h + '<p class="cb-empty">No bond or loan in the fund file is tied to this company, and no credit default swap is written on its name.</p>';
    if (has) {
      h += '<div class="cb-kpis">' +
        kpi("Bonds and loans funds hold", int(r.f_n), (r.f_loans ? plural(r.f_loans, "loan", "loans") + " among them · " : "") + plural(r.f_funds, "fund", "funds") + " in the most widely held") +
        kpi("Held, at funds' own marks", esc(usd(r.f_value)), "face " + esc(usd(r.f_face))) +
        kpi("Middle mark", esc(num(r.f_px, 2)), "per 100 of face, weighted by value") +
        kpi("Widest gap between marks", isNum(r.f_gap) ? esc(num(r.f_gap, 2)) + " pts" : "–", isNum(r.f_gap) ? "10th to 90th percentile mark of one security" : "no security is marked by " + minMarks(doc) + " funds") +
        kpi("Flagged by most holders", int((r.f_def || 0) + (r.f_pik || 0)), int(r.f_def) + " in default · " + int(r.f_pik) + " paid in kind") + "</div>";
      var rows = ctx.funds;
      if (rows === "loading") h += '<p class="cb-empty">Loading what funds hold of it.</p>';
      else if (rows === "error" || rows == null || rows === "unpublished") h += '<p class="cb-empty">The fund document could not be loaded. The figures above are from the board\'s own document.</p>';
      else if (!rows.length) h += '<p class="cb-empty">The fund document lists nothing under this company.</p>';
      else {
        var sort = ctx.fundSort || { key: "value", dir: "desc" }, sorted = sortRows(rows, sort.key, sort.dir);
        var marked = rows.filter(function (x) { return x.gap !== null; }).sort(function (x, y) { return (y.value || 0) - (x.value || 0); }).slice(0, 10);
        h += '<div class="cb-fu-card cb-fu-in"><h3>How far apart funds mark it</h3><p class="cb-cap">Its ' + (marked.length === 1 ? "one security" : marked.length + " largest securities") + " that " + minMarks(doc) +
          " funds or more mark: from the mark one fund in ten is at or under to the mark one in ten is at or over, set against the middle mark.</p>" + rangesHtml(marked, doc, { sheet: true, empty: "None of its securities is marked by enough funds to read a gap." }) + "</div>";
        h += '<p class="cb-note">' + plural(rows.length, "security", "securities") + ", " + esc(usd(rows.reduce(function (s, x) { return s + (isNum(x.value) ? x.value : 0); }, 0))) +
          ' held. The identifiers are not republished. <button type="button" class="cb-link" data-csv="funds">Download these as a file</button></p>' +
          '<div class="cb-scroll" tabindex="0" role="region" aria-label="What funds hold of it; scroll sideways for all columns"><table class="cb-table cb-bonds" data-jh-nosort><thead><tr>' + ROW_FIELDS.map(function (c) {
            var on = sort.key === c[0];
            return '<th class="' + alignOf(c[2]) + '" aria-sort="' + (on ? (sort.dir === "asc" ? "ascending" : "descending") : "none") + '"><button type="button" data-fund-sort="' + esc(c[0]) + '">' + esc(c[1]) + (on ? (sort.dir === "asc" ? " ▲" : " ▼") : "") + "</button></th>";
          }).join("") + "</tr></thead><tbody>" + sorted.map(function (x) { return "<tr>" + ROW_FIELDS.map(function (c) { return '<td class="' + alignOf(c[2]) + '">' + esc(cellText(c[2], x[c[0]])) + "</td>"; }).join("") + "</tr>"; }).join("") + "</tbody></table></div>";
      }
    } else h += '<p class="cb-empty">No bond or loan in the fund file is tied to this company.</p>';
    h += '<h4 id="sheet-fund-swaps">Credit default swaps funds hold on its name</h4>';
    if (!mine.length) return h + '<p class="cb-empty">None in the fund file.</p>';
    var sold = mine.reduce(function (s, x) { return s + (x.sold || 0); }, 0), bought = mine.reduce(function (s, x) { return s + (x.bought || 0); }, 0), other = mine.reduce(function (s, x) { return s + (x.not_in_dollars || 0); }, 0);
    var unclear = mine.reduce(function (s, x) { return s + (x.unclear || 0); }, 0);
    var words = (sold + bought === 0 ? (unclear ? "The file does not make clear which side funds are on for the " + usd(unclear) + " written on it, so nothing is counted as sold or bought." : "No dollar amount is sold or bought on it.") :
      "Funds have sold " + usd(sold) + " of protection on it and bought " + usd(bought) + ": " + (sold > bought * 1.1 ? "on balance they carry its credit risk." : bought > sold * 1.1 ? "on balance they are paying to be protected against it." : "the two sides are close to even.") +
      (unclear ? " A further " + usd(unclear) + " is written with the side unclear." : "")) + (other ? " " + plural(other, "position is", "positions are") + " in another currency and not added up." : "");
    var SW = LISTS.names.cols.filter(function (c) { return c[0] !== "co"; });
    return h + '<p class="cb-verdict">' + esc(words) + '</p><div class="cb-scroll" tabindex="0" role="region" aria-label="Credit default swaps funds hold on its name"><table class="cb-table" data-jh-nosort><thead><tr>' +
      SW.map(function (c) { return '<th class="' + alignOf(c[2]) + '">' + esc(c[1]) + "</th>"; }).join("") + "</tr></thead><tbody>" + mine.map(function (x) {
        return "<tr>" + SW.map(function (c) { return '<td class="' + alignOf(c[2]) + '">' + esc(cellText(c[2], x[c[0]])) + "</td>"; }).join("") + "</tr>"; }).join("") + "</tbody></table></div>" +
      '<p class="cb-note">Sold: the fund receives the fixed payment and carries the credit risk. Bought: it pays it. These are the funds\' own positions, as of their report dates; the traded CDS level, where one prints, is in its traded CDS row below.</p>';
  }

  // ------------------------------------------------------------------------------------------------ clicks
  /* Clicks that belong to what this file draws. api: {doc, funds, need, draw, drawSheet, download}. True when it took the click. */
  function click(d, state, api) {
    var f = fu(state), fd = api.funds;
    if (d.fuView) { f.view = VIEWS.indexOf(d.fuView) >= 0 ? d.fuView : "gaps"; api.need("funds"); api.draw(); return true; }
    if (d.fuHeld != null) { f.held = Number(d.fuHeld) || 0; api.need("funds"); api.draw(); return true; }
    if ("fuTied" in d) { f.tied = !f.tied; api.draw(); return true; }
    if ("fuLoad" in d) { api.need("funds"); api.draw(); return true; }
    if (d.fuSort) {
      var at = d.fuSort.indexOf(":"), id = d.fuSort.slice(0, at), key = d.fuSort.slice(at + 1);
      if (!LISTS[id]) return true;
      var s = sortOf(f, id), col = LISTS[id].cols.filter(function (c) { return c[0] === key; })[0];
      if (s.key === key) s.dir = s.dir === "asc" ? "desc" : "asc";
      else { s.key = key; s.dir = col && alignOf(col[2]) === "l" ? "asc" : "desc"; }
      api.draw(d.fuSort);
      return true;
    }
    if (d.fuMore && LISTS[d.fuMore]) { f.shown[d.fuMore] = (f.shown[d.fuMore] || FIRST) + STEP; api.need("funds"); api.draw(); return true; }
    if (d.fuAll && LISTS[d.fuAll]) { f.shown[d.fuAll] = 1e9; api.need("funds"); api.draw(); return true; }
    if (d.fuCsv && LISTS[d.fuCsv]) {
      api.download("credit-before-equity-funds-" + d.fuCsv.replace(/_/g, "-") + "-" + String((block(api.doc) || {}).as_of || "") + ".csv", csvOf(d.fuCsv, api.doc, fd, state));
      return true;
    }
    if (d.fundSort) {
      var fs = state.fundSort || (state.fundSort = { key: "value", dir: "desc" }), fc = ROW_FIELDS.filter(function (c) { return c[0] === d.fundSort; })[0];
      if (fs.key === d.fundSort) fs.dir = fs.dir === "asc" ? "desc" : "asc";
      else { fs.key = d.fundSort; fs.dir = fc && alignOf(fc[2]) === "l" ? "asc" : "desc"; }
      api.drawSheet(d.fundSort);
      return true;
    }
    if (d.csv === "funds" && whole(fd) && state.sheet) {
      var sh = S(), keys = ["co"].concat(ROW_FIELDS.map(function (c) { return c[0]; }));
      api.download("credit-before-equity-funds-" + String(state.sheet).replace(/[^A-Za-z0-9]+/g, "-") + "-" + String(fd.as_of || "") + ".csv", sh ? sh.csvText(keys, rowsOf(fd, state.sheet, api.doc) || []) : "");
      return true;
    }
    return false;
  }

  root.JHCreditFunds = { LISTS: LISTS, ROW_FIELDS: ROW_FIELDS, VIEWS: VIEWS, HELD: HELD, HOW_WORDS: HOW_WORDS, FIRST: FIRST, STEP: STEP, usd: usd, coupon: coupon, cellText: cellText, block: block, whole: whole,
                         rowsOf: rowsOf, gapRows: gapRows, gapOf: gapOf, listRows: listRows, listHtml: listHtml, csvOf: csvOf, butterflyHtml: butterflyHtml, rangesHtml: rangesHtml, sectionHtml: sectionHtml, sheetHtml: sheetHtml,
                         click: click, state: fu };
})(typeof window !== "undefined" ? window : globalThis);
