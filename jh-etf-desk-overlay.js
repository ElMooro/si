/* jh-reskin-skip */
/* ETF Desk — class heatmaps, 303-name census, weekly technicals, sleeves. */
(function () {
  function esc(s) {
    return String(s == null ? "" : s).replace(/[&<>"]/g, function (c) {
      return c === "&" ? "&" + "amp;" : c === "<" ? "&" + "lt;" : c === ">" ? "&" + "gt;" : "&" + "quot;";
    });
  }
  function num(x) {
    if (x == null || x === "") return null;
    if (typeof x === "object") {
      if (x.value_decimal != null && x.value_decimal !== "") x = x.value_decimal;
      else if (x.raw_decimal != null && x.raw_decimal !== "") x = x.raw_decimal;
      else return null;
    }
    var n = typeof x === "number" ? x : parseFloat(x);
    return n === n ? n : null;
  }
  function money(n) {
    if (n == null || n !== n) return "—";
    var a = Math.abs(n);
    var b = a >= 1e12 ? (a / 1e12).toFixed(2) + "T" : a >= 1e9 ? (a / 1e9).toFixed(2) + "B" : a >= 1e6 ? (a / 1e6).toFixed(1) + "M" : a >= 1e3 ? (a / 1e3).toFixed(0) + "K" : a.toFixed(0);
    return (n > 0 ? "+" : n < 0 ? "−" : "") + b;
  }
  function plain(n) {
    var s = money(n);
    if (s === "—") return s;
    return s.charAt(0) === "+" || s.charAt(0) === "−" ? s.slice(1) : s;
  }
  function pct(n) {
    if (n == null || n !== n) return "—";
    var a = Math.abs(n);
    var d = a !== 0 && a < 0.1 ? 3 : a < 10 ? 2 : 1;
    return (n > 0 ? "+" : n < 0 ? "−" : "") + a.toFixed(d) + "%";
  }
  function flag(v) {
    if (v == null || v !== v) return "—";
    return v ? "Yes" : "No";
  }
  function paint(v, scale) {
    if (v == null || !scale) return ["#1e222d", "#787b86"];
    var t = Math.max(-1, Math.min(1, v / scale));
    var u = Math.pow(Math.abs(t), 0.55);
    var src = t < 0 ? [242, 54, 69] : [8, 153, 129];
    var mid = [19, 23, 34];
    return ["rgb(" + Math.round(mid[0] + (src[0] - mid[0]) * u) + "," + Math.round(mid[1] + (src[1] - mid[1]) * u) + "," + Math.round(mid[2] + (src[2] - mid[2]) * u) + ")", u > 0.42 ? "#fff" : "#d1d4dc"];
  }
  function pScale(vals) {
    var a = [], i;
    for (i = 0; i < vals.length; i++) if (vals[i] != null && vals[i] === vals[i]) a.push(Math.abs(vals[i]));
    if (!a.length) return 1;
    a.sort(function (x, y) { return x - y; });
    return a[Math.min(a.length - 1, Math.floor(a.length * 0.82))] || a[a.length - 1] || 1;
  }
  function worst(row, w) {
    var s = 0, mx = 0, mn = 1e99, i;
    for (i = 0; i < row.length; i++) { s += row[i].a; if (row[i].a > mx) mx = row[i].a; if (row[i].a < mn) mn = row[i].a; }
    if (!s || !mn) return 1e9;
    var s2 = s * s, w2 = w * w;
    return Math.max(w2 * mx / s2, s2 / (w2 * mn));
  }
  function tiles(items, W, H) {
    var out = [], row = [], x = 0, y = 0, w = W, h = H;
    function place(r) {
      var s = 0, i, off = 0, horiz = w >= h, thick, side;
      for (i = 0; i < r.length; i++) s += r[i].a;
      thick = s / (horiz ? h : w) || 1;
      for (i = 0; i < r.length; i++) {
        side = r[i].a / thick;
        out.push(horiz ? { it: r[i], x: x, y: y + off, w: thick, h: side } : { it: r[i], x: x + off, y: y, w: side, h: thick });
        off += side;
      }
      if (horiz) { x += thick; w -= thick; } else { y += thick; h -= thick; }
    }
    var i;
    for (i = 0; i < items.length; i++) {
      var side = Math.min(w, h) || 1;
      if (!row.length || worst(row.concat([items[i]]), side) <= worst(row, side)) row.push(items[i]);
      else { place(row); row = [items[i]]; }
    }
    if (row.length) place(row);
    return out;
  }
  function textOf(f, key) {
    var node = (((((f.profiles || {}).current || {}).summary || {}).text || {})[key]) || null;
    if (!node || node.value == null) return "";
    return String(node.value);
  }
  function flowOf(f, win) {
    var w = (((f.flows || {}).aligned_windows || {})[win]) || null;
    if (!w) return null;
    return { usd: num(w.flow_usd_decimal), pctA: num(w.flow_to_end_assets_pct_decimal) };
  }
  function rowsOf(packet) {
    return Object.keys(packet.funds || {}).map(function (t) {
      var f = packet.funds[t];
      var p = ((((f.profiles || {}).current || {}).summary || {}).numeric) || {};
      var h = (f.holdings || {}).current || {};
      var q = h.quality || {};
      var st = (f.holdings || {}).comparison_status_counts || {};
      var c = (f.flows || {}).classification || {};
      var f1 = flowOf(f, "1"), f5 = flowOf(f, "5"), f21 = flowOf(f, "21");
      return {
        t: t,
        aum: num(p.aum),
        nHold: num(p.num_holdings) || num(q.returned_rows),
        both: num(st.observed_in_both),
        added: num(st.observed_only_in_current),
        deleted: num(st.observed_only_in_prior),
        asof: h.processed_date || "",
        spread: num(p.bid_ask_spread),
        exp: num(p.net_expenses),
        vol: num(p.avg_daily_trading_volume),
        si: num(p.short_interest),
        disc: num(p.discount_premium),
        asset: textOf(f, "asset_class") || "—",
        issuer: textOf(f, "issuer") || "—",
        lev: textOf(f, "leverage_style") || "—",
        bench: textOf(f, "primary_benchmark") || "",
        category: c.category || "—",
        region: c.region || "—",
        sub: c.subcategory || "",
        sector: c.ref_sector || "",
        f1: f1 && f1.usd, p1: f1 && f1.pctA,
        f5: f5 && f5.usd, p5: f5 && f5.pctA,
        f21: f21 && f21.usd, p21: f21 && f21.pctA
      };
    });
  }
  function matrixRows(m) {
    if (!m || !m.tickers) return [];
    var cols = m.cols || {}, keys = Object.keys(cols), out = [], i, k;
    for (i = 0; i < m.tickers.length; i++) {
      var r = { t: m.tickers[i], asset: m.asset_classes[i] || "—", category: m.categories[i] || "—", name: m.names[i] || "" };
      for (k = 0; k < keys.length; k++) r[keys[k]] = num(cols[keys[k]][i]);
      out.push(r);
    }
    return out;
  }
  function sortRows(rows, key, dir) {
    rows.sort(function (a, b) {
      var av = a[key], bv = b[key];
      if (av == null && bv == null) return a.t < b.t ? -1 : 1;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === "string") return dir * String(av).localeCompare(String(bv));
      return (av - bv) * dir;
    });
  }
  function table(body, headers, rows, key, dir, cell) {
    var h = "<table><thead><tr>";
    headers.forEach(function (c) {
      h += "<th data-k='" + c[0] + "'>" + esc(c[1]) + (key === c[0] ? (dir < 0 ? " ↓" : " ↑") : "") + "</th>";
    });
    h += "</tr></thead><tbody>";
    rows.forEach(function (r) { h += "<tr>" + cell(r) + "</tr>"; });
    h += "</tbody></table>";
    body.innerHTML = h;
  }
  function bindSort(body, keyRef, draw) {
    body.querySelectorAll("th").forEach(function (th) {
      th.onclick = function () {
        var k = th.getAttribute("data-k");
        if (keyRef.k === k) keyRef.d = -keyRef.d;
        else { keyRef.k = k; keyRef.d = (k === "t" || k === "asof" || k === "asset" || k === "category" || k === "region" || k === "issuer" || k === "lev" || k === "name") ? 1 : -1; }
        draw();
      };
    });
  }
  function searched(rows, q) {
    if (!q) return rows;
    return rows.filter(function (r) { return String(r.t || "").indexOf(q) >= 0 || String(r.name || "").toUpperCase().indexOf(q) >= 0; });
  }
  function countsOf(rows, field) {
    var m = {}, i;
    for (i = 0; i < rows.length; i++) {
      var k = rows[i][field] || "—";
      m[k] = (m[k] || 0) + 1;
    }
    return Object.keys(m).sort(function (a, b) { return m[b] - m[a] || (a < b ? -1 : 1); }).map(function (k) { return [k, m[k]]; });
  }
  function buckets(rows, field, valKey) {
    var m = {}, order = [], i;
    for (i = 0; i < rows.length; i++) {
      var r = rows[i], k = r[field] || "—";
      if (!m[k]) { m[k] = { name: k, n: 0, net: 0, rows: [] }; order.push(m[k]); }
      m[k].rows.push(r);
      m[k].n++;
      if (r[valKey] != null) m[k].net += r[valKey];
    }
    order.sort(function (a, b) { return Math.abs(b.net) - Math.abs(a.net); });
    return order;
  }

  var SIGNED = { mom_12_1_pct: 1, mom_6m_pct: 1, dist_52w_high_pct: 1, dist_52w_low_pct: 1, rs_13w_pct: 1, prem_disc_pct: 1, variance_drag_pct_ann: 1, f1: 1, f5: 1, f21: 1, net: 1, ytd: 1 };
  var FLAGS = { above_ma40w: 1, golden_cross_10_40w: 1, double_top: 1, double_bottom: 1, breakout_20w: 1, leveraged: 1 };
  var METRICS = {
    tech: [["above_ma40w", "Above 40w"], ["golden_cross_10_40w", "Gold cross"], ["mom_12_1_pct", "Mom 12−1"], ["mom_6m_pct", "Mom 6m"], ["rsi_14w", "RSI"], ["dist_52w_high_pct", "vs 52w high"], ["dist_52w_low_pct", "vs 52w low"], ["beta_spy", "Beta"], ["vol_52w_pct", "Vol 52w"], ["tech_score", "Tech"], ["risk_score", "Risk"]],
    structure: [["double_top", "Dbl top"], ["double_bottom", "Dbl bot"], ["breakout_20w", "Breakout"], ["corr_spy_52w", "Corr SPY"], ["rs_13w_pct", "RS 13w"], ["up_capture_pct", "Up capture"], ["down_capture_pct", "Down capture"], ["prem_disc_pct", "Prem/disc"]],
    risk: [["risk_score", "Risk"], ["tech_score", "Tech"], ["upside_pct", "Upside"], ["downside_pct", "Downside"], ["rr_ratio", "Reward/risk"], ["variance_drag_pct_ann", "Var drag"], ["vol_52w_pct", "Vol"], ["beta_spy", "Beta"]],
    profile: [["aum_usd_m", "AUM $m"], ["holdings_n", "Holdings"], ["expense_pct", "Expense"], ["avg_volume", "Avg vol"], ["dollar_vol_usd_m", "$ vol m"], ["leveraged", "Levered"], ["prem_disc_pct", "Prem/disc"]]
  };

  var CSS = ".jh-etf{font:12px/1.25 'Trebuchet MS',system-ui,sans-serif;color:#d1d4dc;background:#131722;height:100%;display:flex;flex-direction:column}"
    + ".jh-etf .top{display:flex;align-items:center;gap:6px;padding:6px 10px;background:#1e222d;border-bottom:1px solid #2a2e39;flex:none;flex-wrap:wrap}"
    + ".jh-etf .brand{font-weight:700;letter-spacing:.08em;font-size:11px;color:#fff;margin-right:4px}"
    + ".jh-etf button{padding:6px 8px;color:#787b86;background:transparent;border:0;border-bottom:2px solid transparent;cursor:pointer;font:600 11px system-ui}"
    + ".jh-etf button.on{color:#fff;border-bottom-color:#2962ff}"
    + ".jh-etf select,.jh-etf input{background:#131722;color:#d1d4dc;border:1px solid #2a2e39;border-radius:4px;padding:4px 6px;font:11px system-ui}"
    + ".jh-etf input{width:88px}"
    + ".jh-etf .meta{margin-left:auto;color:#b2b5be;font-size:11px;font-variant-numeric:tabular-nums}"
    + ".jh-etf .body{flex:1;min-height:0;overflow:auto;position:relative;background:#0c1016}"
    + ".jh-etf .stack{display:flex;flex-direction:column;height:100%;min-height:480px}"
    + ".jh-etf .strip{display:flex;gap:6px;padding:8px;overflow:auto;flex:none}"
    + ".jh-etf .pill{min-width:112px;padding:6px 8px;border-radius:4px;cursor:pointer;border:1px solid rgba(0,0,0,.35);display:flex;flex-direction:column;gap:2px}"
    + ".jh-etf .pill.on{outline:1px solid #d1d4dc}"
    + ".jh-etf .pill b{font-size:10px;letter-spacing:.04em;font-weight:700}"
    + ".jh-etf .pill span{font-size:13px;font-weight:700;font-variant-numeric:tabular-nums}"
    + ".jh-etf .map{position:relative;flex:1;min-height:420px;background:#0c1016}"
    + ".jh-etf .map.short{min-height:150px;height:168px;flex:none;margin:0 8px 8px}"
    + ".jh-etf .tile{position:absolute;box-sizing:border-box;overflow:hidden;display:flex;flex-direction:column;justify-content:center;padding:2px 5px;border-radius:2px}"
    + ".jh-etf .tile b{font-weight:700;letter-spacing:.02em;line-height:1.05}"
    + ".jh-etf .tile span{font-variant-numeric:tabular-nums;line-height:1.15}"
    + ".jh-etf table{width:100%;border-collapse:collapse}"
    + ".jh-etf th{text-align:right;padding:7px 8px;background:#1e222d;position:sticky;top:0;cursor:pointer;color:#787b86;font-weight:600;white-space:nowrap;z-index:1}"
    + ".jh-etf th:first-child,.jh-etf td:first-child{text-align:left}"
    + ".jh-etf td{padding:5px 8px;border-bottom:1px solid #2a2e39;text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}"
    + ".jh-etf tr:hover td{filter:brightness(1.12)}"
    + ".jh-etf .note{padding:8px 12px;color:#787b86;font-size:11px}"
    + ".jh-etf .sech{display:flex;justify-content:space-between;gap:8px;padding:7px 10px;background:#1e222d;font-weight:700;letter-spacing:.04em}"
    + ".jh-etf .boards{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:8px;padding:8px}"
    + ".jh-etf .board{background:#1e222d;border:1px solid #2a2e39;border-radius:6px;padding:8px 10px;max-height:520px;overflow:auto}"
    + ".jh-etf .board h3{margin:0 0 6px;font-size:11px;letter-spacing:.06em;color:#787b86;font-weight:700;position:sticky;top:0;background:#1e222d}"
    + ".jh-etf .board .row{display:flex;justify-content:space-between;gap:8px;padding:3px 0;border-bottom:1px solid #2a2e39}";

  function render(host, packet) {
    var all = rowsOf(packet);
    var extra = { matrix: null, finviz: null, boards: null };
    var tech = {};
    var state = {
      tab: "flows", win: "1", mode: "heat", src: "funds", group: "asset", cls: "All", q: "",
      hold: "book", censusView: "all", metric: "tech", techMode: "weekly",
      flowSort: { k: "f1", d: -1 }, holdSort: { k: "nHold", d: -1 }, cenSort: { k: "tech_score", d: -1 },
      techSort: { k: "mom_12_1_pct", d: -1 }, topSort: { k: "net1", d: -1 }
    };
    host.innerHTML = "<style>" + CSS + "</style><div class=jh-etf>"
      + "<div class=top><span class=brand>ETF DESK</span><div class=tabs id=etf-tabs></div><div class=win id=etf-win></div>"
      + "<input id=etf-q placeholder='Ticker' autocomplete=off><span class=meta id=etf-meta></span></div>"
      + "<div class=body id=etf-body></div></div>";
    var body = host.querySelector("#etf-body");
    var tabs = [["flows", "Flows"], ["holdings", "Holdings"], ["census", "Census"], ["tech", "Technicals"], ["tops", "Top Held/Added/Deleted"]];
    host.querySelector("#etf-tabs").innerHTML = tabs.map(function (t) {
      return "<button type=button data-tab='" + t[0] + "'" + (t[0] === state.tab ? " class=on" : "") + ">" + t[1] + "</button>";
    }).join("");
    host.querySelector("#etf-q").oninput = function () {
      state.q = host.querySelector("#etf-q").value.toUpperCase().trim();
      draw();
    };
    function meta(s) { host.querySelector("#etf-meta").textContent = s; }
    function flowKeyName() { return state.win === "1" ? "f1" : state.win === "5" ? "f5" : "f21"; }
    function pctKey() { return state.win === "1" ? "p1" : state.win === "5" ? "p5" : "p21"; }
    function pull(key, url) {
      fetch(url, { credentials: "omit" }).then(function (r) {
        if (!r.ok) throw new Error(String(r.status));
        return r.json();
      }).then(function (j) {
        extra[key] = j;
        if (host.isConnected) draw();
      }).catch(function (e) {
        extra[key] = { error: e.message };
        if (host.isConnected) draw();
      });
    }
    pull("matrix", "/data/etf-census-matrix.json");
    pull("finviz", "/data/finviz-etf-flows.json");
    pull("boards", "/data/etf-census.json");

    function clsSelect(rows, field) {
      var opts = countsOf(rows, field);
      var names = opts.map(function (x) { return x[0]; });
      if (state.cls !== "All" && names.indexOf(state.cls) < 0) state.cls = "All";
      var h = "<select id=etf-class><option value='All'" + (state.cls === "All" ? " selected" : "") + ">All classes · " + rows.length + "</option>";
      opts.forEach(function (o) {
        h += "<option value='" + esc(o[0]) + "'" + (state.cls === o[0] ? " selected" : "") + ">" + esc(o[0]) + " · " + o[1] + "</option>";
      });
      return h + "</select>";
    }
    function bindCls() {
      var sel = host.querySelector("#etf-class");
      if (!sel) return;
      sel.onchange = function () { state.cls = sel.value; draw(); };
    }
    function passCls(r, field) {
      return state.cls === "All" || (r[field] || "—") === state.cls;
    }
    function setWin(html) {
      host.querySelector("#etf-win").innerHTML = html;
      host.querySelectorAll("#etf-win button").forEach(function (b) {
        b.onclick = function () {
          if (b.getAttribute("data-w")) state.win = b.getAttribute("data-w");
          if (b.getAttribute("data-mode")) state.mode = b.getAttribute("data-mode");
          if (b.getAttribute("data-src")) { state.src = b.getAttribute("data-src"); state.cls = "All"; }
          if (b.getAttribute("data-group")) { state.group = b.getAttribute("data-group"); state.cls = "All"; }
          if (b.getAttribute("data-hold")) state.hold = b.getAttribute("data-hold");
          if (b.getAttribute("data-cv")) state.censusView = b.getAttribute("data-cv");
          if (b.getAttribute("data-metric")) state.metric = b.getAttribute("data-metric");
          if (b.getAttribute("data-tech")) state.techMode = b.getAttribute("data-tech");
          draw();
        };
      });
      bindCls();
    }
    function btn(attr, val, label, on) {
      return "<button type=button " + attr + "='" + val + "'" + (on ? " class=on" : "") + ">" + label + "</button>";
    }
    function cellSigned(r, key, scale) {
      var v = r[key];
      if (FLAGS[key]) return "<td>" + flag(v) + "</td>";
      if (v == null) return "<td>—</td>";
      if (SIGNED[key]) {
        var c = paint(v, scale[key] || 1);
        return "<td style='background:" + c[0] + ";color:" + c[1] + "'>" + pct(v) + "</td>";
      }
      if (key === "expense_pct" || key === "vol_52w_pct" || key === "upside_pct" || key === "downside_pct" || key === "up_capture_pct" || key === "down_capture_pct") return "<td>" + (v).toFixed(2) + "%</td>";
      if (key === "avg_volume") return "<td>" + plain(v) + "</td>";
      if (key === "aum_usd_m" || key === "dollar_vol_usd_m" || key === "holdings_n") return "<td>" + (Math.abs(v) >= 100 ? plain(v) : v.toFixed(1)) + "</td>";
      return "<td>" + (Math.abs(v) >= 100 ? v.toFixed(0) : v.toFixed(2)) + "</td>";
    }
    function scalesFor(rows, keys) {
      var s = {}, i;
      keys.forEach(function (k) {
        if (!SIGNED[k]) return;
        s[k] = pScale(rows.map(function (r) { return r[k]; }));
      });
      return s;
    }
    function drawTiles(box, list, k, labelOf, subOf) {
      var W = box.clientWidth || host.clientWidth || 960;
      var H = box.clientHeight || 420;
      if (W < 80) W = host.clientWidth || 960;
      if (H < 80) H = 420;
      var scale = pScale(list.map(function (r) { return r[k]; }));
      var area = 0;
      list.forEach(function (r) { r.a = Math.sqrt(Math.abs(r[k]) || 0) || 0.001; area += r.a; });
      list.sort(function (a, b) { return b.a - a.a; });
      var unit = (W * H) / (area || 1);
      list.forEach(function (r) { r.a *= unit; });
      tiles(list, W, H).forEach(function (t) {
        var c = paint(t.it[k], scale);
        var el = document.createElement("div");
        el.className = "tile";
        var label = labelOf(t.it);
        var sub = subOf ? subOf(t.it) : "";
        el.title = label + "  " + money(t.it[k]) + (sub ? "  " + sub : "");
        el.style.cssText = "left:" + (t.x + 1).toFixed(1) + "px;top:" + (t.y + 1).toFixed(1) + "px;width:" + Math.max(0, t.w - 2).toFixed(1) + "px;height:" + Math.max(0, t.h - 2).toFixed(1) + "px;background:" + c[0] + ";color:" + c[1];
        var fs = Math.max(10, Math.min(16, Math.floor(Math.min(t.w, t.h) / 4)));
        el.innerHTML = "<b style='font-size:" + fs + "px'>" + esc(label) + "</b>"
          + (t.h > 32 && t.w > 48 ? "<span>" + money(t.it[k]) + "</span>" : "")
          + (sub && t.h > 50 && t.w > 70 ? "<span>" + esc(sub) + "</span>" : "");
        box.appendChild(el);
      });
    }
    function stripHtml(groups) {
      var scale = pScale(groups.map(function (g) { return g.net; }));
      var nAll = 0, gi;
      for (gi = 0; gi < groups.length; gi++) nAll += groups[gi].n;
      var h = "<div class=strip><div class='pill" + (state.cls === "All" ? " on" : "") + "' data-cls='All' style='background:#1e222d;color:#d1d4dc'><b>ALL</b><span>" + nAll + "</span></div>";
      groups.forEach(function (g) {
        var c = paint(g.net, scale);
        h += "<div class='pill" + (state.cls === g.name ? " on" : "") + "' data-cls='" + esc(g.name) + "' style='background:" + c[0] + ";color:" + c[1] + "'><b>" + esc(g.name) + " · " + g.n + "</b><span>" + money(g.net) + "</span></div>";
      });
      return h + "</div>";
    }
    function bindStrip() {
      body.querySelectorAll(".pill").forEach(function (p) {
        p.onclick = function () {
          var v = p.getAttribute("data-cls");
          state.cls = state.cls === v ? "All" : v;
          draw();
        };
      });
    }

    function showFlows() {
      var k = flowKeyName(), pk = pctKey();
      if (state.flowSort.k === "p1" || state.flowSort.k === "p5" || state.flowSort.k === "p21") state.flowSort.k = pk;
      else if (state.flowSort.k === "f1" || state.flowSort.k === "f5" || state.flowSort.k === "f21") state.flowSort.k = k;
      var groups = [["asset", "Asset"], ["category", "Category"], ["region", "Region"], ["issuer", "Issuer"], ["lev", "Leverage"]];
      var html = btn("data-src", "funds", "Funds", state.src === "funds")
        + btn("data-src", "classes", "Asset classes", state.src === "classes")
        + btn("data-src", "sleeves", "Sleeves", state.src === "sleeves")
        + btn("data-src", "leaders", "1M leaders", state.src === "leaders")
        + btn("data-mode", "heat", "Heat", state.mode === "heat")
        + btn("data-mode", "table", "Table", state.mode === "table");
      if (state.src === "funds") {
        html += ["1", "5", "21"].map(function (w) { return btn("data-w", w, w + "D", state.win === w); }).join("");
        groups.forEach(function (g) { html += btn("data-group", g[0], g[1], state.group === g[0]); });
      }
      setWin(html);
      if (state.src === "classes") return showClasses();
      if (state.src === "sleeves") return showSleeves();
      if (state.src === "leaders") return showLeaders();
      var base = searched(all, state.q).filter(function (r) { return r[k] != null; });
      var bucks = buckets(base, state.group, k);
      var allowed = { t: 1, issuer: 1, region: 1, aum: 1, exp: 1, asset: 1, category: 1, lev: 1 };
      allowed[k] = 1; allowed[pk] = 1; allowed[state.group] = 1;
      if (!allowed[state.flowSort.k]) state.flowSort.k = k;
      var list = state.cls === "All" ? base : base.filter(function (r) { return (r[state.group] || "—") === state.cls; });
      var net = 0, inn = 0, out = 0, i;
      for (i = 0; i < list.length; i++) {
        net += list[i][k] || 0;
        if (list[i][k] > 0) inn++; else if (list[i][k] < 0) out++;
      }
      meta(list.length + " funds · " + state.win + "D net " + money(net) + " · " + inn + " in · " + out + " out · " + bucks.length + " " + state.group + " groups");
      if (state.mode === "table") {
        sortRows(list, state.flowSort.k, state.flowSort.d);
        var scale = {}; scale[k] = pScale(list.map(function (r) { return r[k]; }));
        table(body, [["t", "Ticker"], [state.group, state.group], ["issuer", "Issuer"], ["region", "Region"], ["aum", "AUM"], [k, state.win + "D flow"], [pk, "% AUM"], ["exp", "Expense"]], list, state.flowSort.k, state.flowSort.d, function (r) {
          var c = paint(r[k], scale[k]);
          return "<td>" + esc(r.t) + "</td><td>" + esc(r[state.group] || "—") + "</td><td>" + esc(r.issuer) + "</td><td>" + esc(r.region) + "</td><td>" + plain(r.aum) + "</td><td style='background:" + c[0] + ";color:" + c[1] + "'>" + money(r[k]) + "</td><td>" + pct(r[pk]) + "</td><td>" + (r.exp == null ? "—" : r.exp.toFixed(2) + "%") + "</td>";
        });
        bindSort(body, state.flowSort, draw);
        return;
      }
      var heat = list.filter(function (r) { return r[k]; });
      body.innerHTML = "<div class=stack>" + stripHtml(bucks) + "<div class=map id=etf-map></div></div>";
      bindStrip();
      var box = body.querySelector("#etf-map");
      if (!heat.length) { box.innerHTML = "<div class=note>No " + state.win + "D prints in this slice.</div>"; return; }
      drawTiles(box, heat, k, function (r) { return r.t; }, function (r) { return pct(r[pk]) + " · " + (r[state.group] || ""); });
    }
    function showClasses() {
      var fv = extra.finviz;
      if (!fv || fv.error || !fv.type_flows) { meta("Asset-class rollup"); body.innerHTML = "<div class=note>" + (fv && fv.error ? "Class file failed: " + esc(fv.error) : "Loading 5,775-fund asset classes…") + "</div>"; return; }
      var rows = fv.type_flows.map(function (t) {
        return { t: t.type, n: t.n, aum: num(t.aum), net: num(t.net_1m), ytd: num(t.net_ytd), inn: num(t.gross_in_1m), out: num(t.gross_out_1m), asset: t.type };
      });
      var list = searched(rows, state.q).filter(function (r) { return passCls(r, "asset"); });
      if (["t", "n", "aum", "net", "ytd", "inn", "out"].indexOf(state.flowSort.k) < 0) state.flowSort.k = "net";
      var net = 0, i; for (i = 0; i < list.length; i++) net += list[i].net || 0;
      meta((fv.n_etfs || "") + " funds in " + rows.length + " asset classes · 1M net " + money(net) + " · finviz " + String(fv.generated_at || "").slice(0, 10));
      if (state.mode === "table") {
        sortRows(list, state.flowSort.k, state.flowSort.d);
        var scale = { net: pScale(list.map(function (r) { return r.net; })), ytd: pScale(list.map(function (r) { return r.ytd; })) };
        table(body, [["t", "Asset class"], ["n", "Funds"], ["aum", "AUM"], ["net", "1M net"], ["inn", "1M in"], ["out", "1M out"], ["ytd", "YTD net"]], list, state.flowSort.k, state.flowSort.d, function (r) {
          var c = paint(r.net, scale.net), c2 = paint(r.ytd, scale.ytd);
          return "<td>" + esc(r.t) + "</td><td>" + r.n + "</td><td>" + plain(r.aum) + "</td><td style='background:" + c[0] + ";color:" + c[1] + "'>" + money(r.net) + "</td><td>" + plain(r.inn) + "</td><td>" + plain(r.out) + "</td><td style='background:" + c2[0] + ";color:" + c2[1] + "'>" + money(r.ytd) + "</td>";
        });
        bindSort(body, state.flowSort, draw);
        return;
      }
      body.innerHTML = "<div class=stack><div class=map id=etf-map></div></div>";
      var box = body.querySelector("#etf-map");
      drawTiles(box, list.filter(function (r) { return r.net; }), "net", function (r) { return r.t; }, function (r) { return r.n + " funds"; });
    }
    function showSleeves() {
      var fv = extra.finviz;
      if (!fv || fv.error || !fv.complex_nets) { body.innerHTML = "<div class=note>Loading sleeves…</div>"; return; }
      meta((fv.complex_nets.length) + " sleeves · member 1M flow · " + String(fv.generated_at || "").slice(0, 10));
      if (state.mode === "table") {
        var flat = [];
        fv.complex_nets.forEach(function (c) {
          (c.members || []).forEach(function (m) {
            flat.push({ t: m.ticker, asset: c.complex, net: num(m.f1m), n: c.n });
          });
        });
        flat = searched(flat, state.q).filter(function (r) { return passCls(r, "asset"); });
        sortRows(flat, "net", -1);
        var scale = { net: pScale(flat.map(function (r) { return r.net; })) };
        table(body, [["t", "Ticker"], ["asset", "Sleeve"], ["net", "1M flow"]], flat, "net", -1, function (r) {
          var c = paint(r.net, scale.net);
          return "<td>" + esc(r.t) + "</td><td>" + esc(r.asset) + "</td><td style='background:" + c[0] + ";color:" + c[1] + "'>" + money(r.net) + "</td>";
        });
        return;
      }
      var html = "";
      fv.complex_nets.forEach(function (c, idx) {
        if (state.cls !== "All" && c.complex !== state.cls) return;
        html += "<div class=sech><span>" + esc(c.complex) + " · " + c.n + "</span><span>" + money(num(c.net_1m)) + " 1M · YTD " + money(num(c.net_ytd)) + "</span></div><div class='map short' id='sl-" + idx + "'></div>";
      });
      body.innerHTML = html || "<div class=note>No sleeve in this filter.</div>";
      fv.complex_nets.forEach(function (c, idx) {
        var box = body.querySelector("#sl-" + idx);
        if (!box) return;
        var rows = (c.members || []).map(function (m) { return { t: m.ticker, net: num(m.f1m) }; }).filter(function (r) { return r.net && (!state.q || r.t.indexOf(state.q) >= 0); });
        if (rows.length) drawTiles(box, rows, "net", function (r) { return r.t; }, null);
      });
    }
    function showLeaders() {
      var fv = extra.finviz;
      if (!fv || fv.error) { body.innerHTML = "<div class=note>Loading 1M leaders…</div>"; return; }
      var seen = {}, rows = [];
      function add(list, side) {
        (list || []).forEach(function (x) {
          if (seen[x.ticker]) return;
          seen[x.ticker] = 1;
          rows.push({
            t: x.ticker, name: x.name || "", asset: x.etf_type || "—", sector: x.sector || "",
            aum: num(x.aum), net: num(x.flows_1m), p: num(x.flows_1m_pct), m3: num(x.flows_3m),
            ytd: num(x.flows_ytd), y1: num(x.flows_1y), exp: num(x.expense), nHold: num(x.n_holdings),
            ret: num(x.ret_1y), pm: num(x.perf_m), side: side
          });
        });
      }
      add(fv.top_inflows, "in"); add(fv.top_outflows, "out");
      var base = searched(rows, state.q);
      var bucks = buckets(base, "asset", "net");
      var list = state.cls === "All" ? base : base.filter(function (r) { return r.asset === state.cls; });
      meta(list.length + " leaders · 1M creation estimate · classes in the strip · " + String(fv.generated_at || "").slice(0, 10));
      if (["t", "asset", "name", "aum", "net", "p", "m3", "ytd", "y1", "exp", "nHold", "ret"].indexOf(state.flowSort.k) < 0) state.flowSort.k = "net";
      if (state.mode === "table") {
        sortRows(list, state.flowSort.k, state.flowSort.d);
        var scale = { net: pScale(list.map(function (r) { return r.net; })) };
        table(body, [["t", "Ticker"], ["asset", "Asset class"], ["name", "Name"], ["aum", "AUM"], ["net", "1M"], ["p", "1M %"], ["m3", "3M"], ["ytd", "YTD"], ["y1", "1Y"], ["exp", "Fee"], ["nHold", "Holdings"], ["ret", "Ret 1Y"]], list, state.flowSort.k, state.flowSort.d, function (r) {
          var c = paint(r.net, scale.net);
          return "<td>" + esc(r.t) + "</td><td>" + esc(r.asset) + "</td><td>" + esc(r.name) + "</td><td>" + plain(r.aum) + "</td><td style='background:" + c[0] + ";color:" + c[1] + "'>" + money(r.net) + "</td><td>" + pct(r.p) + "</td><td>" + money(r.m3) + "</td><td>" + money(r.ytd) + "</td><td>" + money(r.y1) + "</td><td>" + (r.exp == null ? "—" : r.exp.toFixed(2) + "%") + "</td><td>" + (r.nHold == null ? "—" : Math.round(r.nHold)) + "</td><td>" + pct(r.ret) + "</td>";
        });
        bindSort(body, state.flowSort, draw);
        return;
      }
      body.innerHTML = "<div class=stack>" + stripHtml(bucks) + "<div class=map id=etf-map></div></div>";
      bindStrip();
      var heat = list.filter(function (r) { return r.net; });
      drawTiles(body.querySelector("#etf-map"), heat, "net", function (r) { return r.t; }, function (r) { return pct(r.p); });
    }

    function showHold() {
      var cen = matrixRows(extra.matrix);
      var html = btn("data-hold", "book", "Book " + all.length, state.hold === "book")
        + btn("data-hold", "census", "Census " + (cen.length || 303), state.hold === "census")
        + clsSelect(state.hold === "book" ? all : cen, "asset");
      setWin(html);
      if (state.hold === "census") {
        if (!cen.length) { meta("Census holdings"); body.innerHTML = "<div class=note>" + (extra.matrix && extra.matrix.error ? esc(extra.matrix.error) : "Loading 303-name census…") + "</div>"; return; }
        var list = searched(cen, state.q).filter(function (r) { return passCls(r, "asset"); });
        if (["t", "asset", "category", "holdings_n", "aum_usd_m", "expense_pct", "prem_disc_pct", "dollar_vol_usd_m", "leveraged"].indexOf(state.holdSort.k) < 0) state.holdSort.k = "holdings_n";
        sortRows(list, state.holdSort.k, state.holdSort.d);
        meta(list.length + " names · holdings count, fee, premium · census " + String((extra.matrix || {}).generated_at || "").slice(0, 10));
        table(body, [["t", "Ticker"], ["asset", "Asset class"], ["category", "Category"], ["holdings_n", "Holdings"], ["aum_usd_m", "AUM $m"], ["expense_pct", "Expense"], ["prem_disc_pct", "Prem/disc"], ["dollar_vol_usd_m", "$ vol m"], ["leveraged", "Levered"]], list, state.holdSort.k, state.holdSort.d, function (r) {
          return "<td>" + esc(r.t) + "</td><td>" + esc(r.asset) + "</td><td>" + esc(r.category) + "</td><td>" + (r.holdings_n == null ? "—" : Math.round(r.holdings_n)) + "</td><td>" + (r.aum_usd_m == null ? "—" : r.aum_usd_m.toFixed(1)) + "</td><td>" + (r.expense_pct == null ? "—" : r.expense_pct.toFixed(2) + "%") + "</td><td>" + pct(r.prem_disc_pct) + "</td><td>" + (r.dollar_vol_usd_m == null ? "—" : r.dollar_vol_usd_m.toFixed(1)) + "</td><td>" + flag(r.leveraged) + "</td>";
        });
        bindSort(body, state.holdSort, draw);
        return;
      }
      var list = searched(all, state.q).filter(function (r) { return passCls(r, "asset"); });
      if (["t", "asset", "category", "region", "issuer", "lev", "nHold", "both", "added", "deleted", "asof", "aum", "exp", "spread", "vol", "si", "disc"].indexOf(state.holdSort.k) < 0) state.holdSort.k = "nHold";
      sortRows(list, state.holdSort.k, state.holdSort.d);
      meta(list.length + " funds · book vs prior · " + countsOf(all, "asset").length + " asset classes · " + countsOf(all, "issuer").length + " issuers");
      table(body, [["t", "Ticker"], ["asset", "Class"], ["category", "Category"], ["region", "Region"], ["issuer", "Issuer"], ["lev", "Leverage"], ["nHold", "Holdings"], ["both", "In both"], ["added", "Added"], ["deleted", "Deleted"], ["asof", "As of"], ["aum", "AUM"], ["exp", "Expense"], ["spread", "Spread bp"], ["vol", "Volume"], ["si", "Short sh"], ["disc", "Prem"]], list, state.holdSort.k, state.holdSort.d, function (r) {
        return "<td>" + esc(r.t) + "</td><td>" + esc(r.asset) + "</td><td>" + esc(r.category) + "</td><td>" + esc(r.region) + "</td><td>" + esc(r.issuer) + "</td><td>" + esc(r.lev) + "</td><td>" + (r.nHold == null ? "—" : Math.round(r.nHold)) + "</td><td>" + (r.both == null ? "—" : r.both) + "</td><td>" + (r.added == null ? "—" : r.added) + "</td><td>" + (r.deleted == null ? "—" : r.deleted) + "</td><td>" + esc(r.asof || "—") + "</td><td>" + plain(r.aum) + "</td><td>" + (r.exp == null ? "—" : r.exp.toFixed(2) + "%") + "</td><td>" + (r.spread == null ? "—" : (r.spread * 10000).toFixed(2)) + "</td><td>" + plain(r.vol) + "</td><td>" + plain(r.si) + "</td><td>" + pct(r.disc) + "</td>";
      });
      bindSort(body, state.holdSort, draw);
    }

    function showCensus() {
      var cen = matrixRows(extra.matrix);
      var html = btn("data-cv", "all", "All names", state.censusView === "all")
        + btn("data-cv", "boards", "Boards", state.censusView === "boards")
        + (state.censusView === "all"
          ? btn("data-metric", "tech", "Trend", state.metric === "tech") + btn("data-metric", "structure", "Structure", state.metric === "structure") + btn("data-metric", "risk", "Risk", state.metric === "risk") + btn("data-metric", "profile", "Profile", state.metric === "profile")
          : "")
        + (cen.length ? clsSelect(cen, "asset") : "");
      setWin(html);
      if (state.censusView === "boards") return showBoards(cen);
      if (!cen.length) { meta("Census"); body.innerHTML = "<div class=note>" + (extra.matrix && extra.matrix.error ? esc(extra.matrix.error) : "Loading census matrix…") + "</div>"; return; }
      var cols = METRICS[state.metric];
      var list = searched(cen, state.q).filter(function (r) { return passCls(r, "asset"); });
      if (cols.every(function (c) { return c[0] !== state.cenSort.k; }) && state.cenSort.k !== "t" && state.cenSort.k !== "asset" && state.cenSort.k !== "category") state.cenSort.k = cols[0][0];
      sortRows(list, state.cenSort.k, state.cenSort.d);
      var scale = scalesFor(list, cols.map(function (c) { return c[0]; }));
      meta(list.length + " of " + cen.length + " · " + countsOf(cen, "asset").length + " asset classes · " + countsOf(cen, "category").length + " categories · " + String(extra.matrix.generated_at || "").slice(0, 10));
      var headers = [["t", "Ticker"], ["asset", "Class"], ["category", "Category"]].concat(cols);
      table(body, headers, list, state.cenSort.k, state.cenSort.d, function (r) {
        var h = "<td>" + esc(r.t) + "</td><td>" + esc(r.asset) + "</td><td>" + esc(r.category) + "</td>";
        cols.forEach(function (c) { h += cellSigned(r, c[0], scale); });
        return h;
      });
      bindSort(body, state.cenSort, draw);
      var pairs = (extra.boards && extra.boards.pairs) || [];
      if (pairs.length && state.cls === "All" && !state.q) {
        var foot = "<table><thead><tr><th>Pair</th><th>Z</th><th>Ratio</th><th>Weeks</th><th>Note</th></tr></thead><tbody>";
        pairs.forEach(function (p) {
          foot += "<tr><td>" + esc(p.pair) + "</td><td>" + esc(p.z) + "</td><td>" + esc(p.ratio) + "</td><td>" + esc(p.n_weeks) + "</td><td>" + esc(p.note || "") + "</td></tr>";
        });
        body.insertAdjacentHTML("beforeend", foot + "</tbody></table><div class=note>Pairs are 3-year ratio z-scores from the census file.</div>");
      }
    }
    function showBoards(cen) {
      var b = extra.boards;
      if (!b || b.error || !b.boards) { body.innerHTML = "<div class=note>Loading boards…</div>"; return; }
      var byT = {};
      cen.forEach(function (r) { byT[r.t] = r; });
      meta("Full boards · " + (b.n || "") + " names · " + String(b.generated_at || "").slice(0, 10));
      var html = "<div class=boards>";
      Object.keys(b.boards).forEach(function (name) {
        var rows = b.boards[name] || [];
        html += "<div class=board><h3>" + esc(name.replace(/_/g, " ").toUpperCase()) + " · " + rows.length + "</h3>";
        rows.forEach(function (row, i) {
          var sym = Array.isArray(row) ? row[0] : row;
          var sc = Array.isArray(row) ? row[1] : "";
          if (state.q && String(sym).indexOf(state.q) < 0) return;
          var info = byT[sym];
          if (state.cls !== "All" && info && info.asset !== state.cls) return;
          html += "<div class=row><span>" + (i + 1) + "  <b>" + esc(sym) + "</b> " + esc(info ? info.asset : "") + "</span><span>" + (sc === "" || sc == null ? "" : esc(sc)) + "</span></div>";
        });
        html += "</div>";
      });
      body.innerHTML = html + "</div>";
    }

    function showTech() {
      var cen = matrixRows(extra.matrix);
      var html = btn("data-tech", "weekly", "Weekly " + (cen.length || 303), state.techMode === "weekly")
        + btn("data-tech", "daily", "Daily MAs", state.techMode === "daily")
        + btn("data-tech", "sectors", "Sector breadth", state.techMode === "sectors");
      if (state.techMode === "weekly") html += clsSelect(cen, "asset");
      if (state.techMode === "daily") html += clsSelect(all, "asset");
      setWin(html);
      if (state.techMode === "sectors") return showSectors();
      if (state.techMode === "daily") return showDaily();
      if (!cen.length) { body.innerHTML = "<div class=note>Loading weekly technicals…</div>"; return; }
      var keys = [["above_ma40w", "Above 40w"], ["golden_cross_10_40w", "10/40 cross"], ["mom_12_1_pct", "Mom 12−1"], ["mom_6m_pct", "Mom 6m"], ["rsi_14w", "RSI"], ["dist_52w_high_pct", "vs high"], ["beta_spy", "Beta"], ["corr_spy_52w", "Corr"], ["rs_13w_pct", "RS 13w"], ["vol_52w_pct", "Vol"], ["tech_score", "Tech"], ["risk_score", "Risk"]];
      var list = searched(cen, state.q).filter(function (r) { return passCls(r, "asset"); });
      sortRows(list, state.techSort.k, state.techSort.d);
      var scale = scalesFor(list, keys.map(function (k) { return k[0]; }));
      var above = list.filter(function (r) { return r.above_ma40w; }).length;
      meta(list.length + " names · " + above + " above the 40-week · weekly census, not the daily 9/20/50 tape");
      table(body, [["t", "Ticker"], ["asset", "Class"], ["category", "Category"]].concat(keys), list, state.techSort.k, state.techSort.d, function (r) {
        var h = "<td>" + esc(r.t) + "</td><td>" + esc(r.asset) + "</td><td>" + esc(r.category) + "</td>";
        keys.forEach(function (c) { h += cellSigned(r, c[0], scale); });
        return h;
      });
      bindSort(body, state.techSort, draw);
    }
    function showSectors() {
      var fv = extra.finviz;
      var rows = (fv && fv.sector_breadth) || null;
      if (!rows) { body.innerHTML = "<div class=note>Loading sector breadth…</div>"; return; }
      meta("Stock sectors · share above the 50- and 200-day · median month · " + String(fv.generated_at || "").slice(0, 10));
      var items = rows.map(function (s) {
        return { t: s.sector, n: s.n, net: num(s.median_perf_m), a200: num(s.pct_above_200dma), a50: num(s.pct_above_50dma) };
      });
      if (state.mode === "table" || true) {
        body.innerHTML = "<div class=map id=etf-map style='height:280px;min-height:280px'></div>";
        drawTiles(body.querySelector("#etf-map"), items, "net", function (r) { return r.t; }, function (r) { return r.a200.toFixed(0) + "% >200d · n=" + r.n; });
        var scale = { net: pScale(items.map(function (r) { return r.net; })) };
        var h = "<table><thead><tr><th>Sector</th><th>Names</th><th>% above 200d</th><th>% above 50d</th><th>Median month</th></tr></thead><tbody>";
        items.sort(function (a, b) { return b.a200 - a.a200; }).forEach(function (r) {
          var c = paint(r.net, scale.net);
          h += "<tr><td>" + esc(r.t) + "</td><td>" + r.n + "</td><td>" + r.a200.toFixed(1) + "%</td><td>" + r.a50.toFixed(1) + "%</td><td style='background:" + c[0] + ";color:" + c[1] + "'>" + pct(r.net) + "</td></tr>";
        });
        body.insertAdjacentHTML("beforeend", h + "</tbody></table><div class=note>Breadth is the stock universe inside each sector, not an ETF holdings count.</div>");
      }
    }
    function showDaily() {
      var list = searched(all, state.q).filter(function (r) { return passCls(r, "asset"); }).map(function (r) {
        return Object.assign({}, r, tech[r.t] || {});
      });
      if (["t", "asset", "spread", "rng", "ma9", "ma20", "ma50", "ma100", "ma200", "ma250", "ret", "eff", "evr"].indexOf(state.techSort.k) < 0) state.techSort.k = "t";
      sortRows(list, state.techSort.k, state.techSort.d);
      var done = Object.keys(tech).length;
      meta("Daily distance to 9/20/50/100/200/250 · spread in bp · " + done + "/" + all.length + " bars · " + list.length + " shown");
      function cellMA(r, key) {
        var v = r[key];
        if (v == null) return "<td>—</td>";
        var c = paint(v, 8);
        return "<td style='background:" + c[0] + ";color:" + c[1] + "'>" + pct(v) + "</td>";
      }
      table(body, [["t", "Ticker"], ["asset", "Class"], ["spread", "Spread bp"], ["rng", "Day range"], ["ma9", "9"], ["ma20", "20"], ["ma50", "50"], ["ma100", "100"], ["ma200", "200"], ["ma250", "250"], ["ret", "Result"], ["eff", "Effort"], ["evr", "EVR"]], list, state.techSort.k, state.techSort.d, function (r) {
        return "<td>" + esc(r.t) + "</td><td>" + esc(r.asset) + "</td>"
          + "<td>" + (r.spread == null ? "—" : (r.spread * 10000).toFixed(2)) + "</td>"
          + (r.rng == null ? "<td>—</td>" : "<td>" + r.rng.toFixed(2) + "%</td>")
          + cellMA(r, "ma9") + cellMA(r, "ma20") + cellMA(r, "ma50") + cellMA(r, "ma100") + cellMA(r, "ma200") + cellMA(r, "ma250")
          + (r.ret == null ? "<td>—</td>" : "<td>" + pct(r.ret) + "</td>")
          + (r.eff == null ? "<td>—</td>" : "<td>" + r.eff.toFixed(2) + "×</td>")
          + (r.evr == null ? "<td>—</td>" : "<td>" + r.evr.toFixed(2) + "</td>");
      });
      bindSort(body, state.techSort, draw);
      if (host._techStarted) return;
      host._techStarted = true;
      var q = all.map(function (r) { return r.t; }), i = 0;
      function step() {
        if (i >= q.length) return;
        var batch = q.slice(i, i + 4); i += 4;
        Promise.all(batch.map(loadTech)).then(function () { if (state.tab === "tech" && state.techMode === "daily") showDaily(); step(); });
      }
      step();
    }
    function loadTech(sym) {
      if (tech[sym]) return Promise.resolve();
      return fetch("/data/warm/tv-bars/universe/US__" + encodeURIComponent(sym) + ".json.gz", { credentials: "omit" }).then(function (res) {
        if (!res.ok) throw new Error(String(res.status));
        return res.arrayBuffer();
      }).then(function (buf) {
        var u8 = new Uint8Array(buf);
        if (u8[0] === 0x7b) return JSON.parse(new TextDecoder().decode(u8));
        if (!window.DecompressionStream) throw new Error("gzip");
        return new Response(new Blob([buf]).stream().pipeThrough(new DecompressionStream("gzip"))).json();
      }).then(function (j) {
        var bars = (j && j.bars) || [], n = bars.length;
        if (n < 10) { tech[sym] = {}; return; }
        function sma(len) {
          if (n < len) return null;
          var s = 0, k; for (k = n - len; k < n; k++) s += +bars[k][4];
          var m = s / len; return m ? (bars[n - 1][4] / m - 1) * 100 : null;
        }
        var last = bars[n - 1], prev = bars[n - 2];
        var ret = prev && prev[4] ? (last[4] / prev[4] - 1) * 100 : null;
        var rng = last[4] ? (last[2] - last[3]) / last[4] * 100 : null;
        var from = Math.max(0, n - 21), sV = 0, c = 0, k;
        for (k = from; k < n - 1; k++) { sV += +bars[k][5] || 0; c++; }
        var avg = c ? sV / c : null, eff = avg ? (+last[5] || 0) / avg : null;
        tech[sym] = { ma9: sma(9), ma20: sma(20), ma50: sma(50), ma100: sma(100), ma200: sma(200), ma250: sma(250), ret: ret, rng: rng, eff: eff, evr: eff ? ret / eff : null };
      }).catch(function () { tech[sym] = {}; });
    }

    function showTops() {
      setWin("");
      var cen = matrixRows(extra.matrix);
      var fv = extra.finviz && !extra.finviz.error ? extra.finviz : null;
      function col(title, rows, key, fmt) {
        var list = rows.filter(function (r) { return r[key] != null; }).slice();
        sortRows(list, key, -1);
        var h = "<div class=board><h3>" + title + " · " + list.length + "</h3>";
        list.slice(0, 40).forEach(function (r, i) {
          if (state.q && r.t.indexOf(state.q) < 0) return;
          h += "<div class=row><span>" + (i + 1) + "  <b>" + esc(r.t) + "</b> " + esc(r.asset || "") + "</span><span>" + fmt(r) + "</span></div>";
        });
        return h + "</div>";
      }
      var bucks = buckets(all, "asset", "f1");
      var scale = pScale(bucks.map(function (g) { return g.net; }));
      var classTable = "<table><thead><tr><th>Asset class</th><th>Funds</th><th>Holdings sum</th><th>Added</th><th>Deleted</th><th>1D net</th><th>21D net</th></tr></thead><tbody>";
      buckets(all, "asset", "f21").sort(function (a, b) { return Math.abs(b.net) - Math.abs(a.net); });
      countsOf(all, "asset").forEach(function (pair) {
        var name = pair[0];
        var rs = all.filter(function (r) { return r.asset === name; });
        var hold = 0, add = 0, del = 0, n1 = 0, n21 = 0, i;
        for (i = 0; i < rs.length; i++) {
          hold += rs[i].nHold || 0; add += rs[i].added || 0; del += rs[i].deleted || 0; n1 += rs[i].f1 || 0; n21 += rs[i].f21 || 0;
        }
        var c = paint(n1, scale);
        classTable += "<tr><td>" + esc(name) + "</td><td>" + rs.length + "</td><td>" + Math.round(hold) + "</td><td>" + add + "</td><td>" + del + "</td><td style='background:" + c[0] + ";color:" + c[1] + "'>" + money(n1) + "</td><td>" + money(n21) + "</td></tr>";
      });
      classTable += "</tbody></table>";
      var html = classTable + "<div class=boards>"
        + col("MOST NAMES HELD · BOOK", all, "nHold", function (r) { return Math.round(r.nHold); })
        + col("MOST NAMES ADDED · BOOK", all, "added", function (r) { return r.added; })
        + col("MOST NAMES DELETED · BOOK", all, "deleted", function (r) { return r.deleted; });
      if (cen.length) html += col("MOST HOLDINGS · CENSUS 303", cen, "holdings_n", function (r) { return Math.round(r.holdings_n); });
      if (fv) {
        html += col("1M INFLOWS", (fv.top_inflows || []).map(function (x) { return { t: x.ticker, asset: x.etf_type || "", v: num(x.flows_1m) }; }), "v", function (r) { return money(r.v); });
        html += col("1M OUTFLOWS", (fv.top_outflows || []).map(function (x) { var v = num(x.flows_1m); return { t: x.ticker, asset: x.etf_type || "", v: v, abs: v == null ? null : Math.abs(v) }; }), "abs", function (r) { return money(r.v); });
      }
      html += "</div><div class=note>Added and deleted are identities only in the current or prior book. Those rows are hashes, not tickers, so this ranks funds. The class table is the book of " + all.length + ". Census holdings are a wider " + (cen.length || "…") + "-name count. 1M leaders are the finviz extremes, not the full 5,775.</div>";
      body.innerHTML = html;
      meta("Book " + all.length + " · census " + (cen.length || "…") + " · " + countsOf(all, "asset").length + " asset classes · " + countsOf(all, "category").length + " flow categories");
    }

    function draw() {
      if (state.tab === "flows") showFlows();
      else if (state.tab === "holdings") showHold();
      else if (state.tab === "census") showCensus();
      else if (state.tab === "tech") showTech();
      else showTops();
    }
    host.querySelectorAll("#etf-tabs button").forEach(function (b) {
      b.onclick = function () {
        state.tab = b.getAttribute("data-tab");
        state.cls = "All";
        host.querySelectorAll("#etf-tabs button").forEach(function (x) { x.className = x === b ? "on" : ""; });
        draw();
      };
    });
    window.addEventListener("resize", function () {
      if (state.tab === "flows" && state.mode === "heat") showFlows();
    });
    draw();
  }

  window.jhEtfDeskOverlay = function (host) {
    if (!host) return;
    host.innerHTML = "<div style='padding:16px;color:#787b86;background:#131722;height:100%;font:13px system-ui'>Loading ETF desk…</div>";
    fetch("/data/etf-desk-research.json", { credentials: "omit" }).then(function (r) {
      if (!r.ok) throw new Error(String(r.status));
      return r.json();
    }).then(function (packet) {
      if (host.isConnected) render(host, packet);
    }).catch(function (err) {
      host.innerHTML = "<div style='padding:16px;color:#f23645;background:#131722;height:100%'>Could not load the desk: " + esc(err && err.message) + "</div>";
    });
  };
})();
