/* jh-reskin-skip */
/* ETF Desk — TradingView-style flow heatmap. Holdings, census, MAs, tops are live tabs. */
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
    var b = a >= 1e9 ? (a / 1e9).toFixed(2) + "B" : a >= 1e6 ? (a / 1e6).toFixed(1) + "M" : a >= 1e3 ? (a / 1e3).toFixed(0) + "K" : a.toFixed(0);
    return (n > 0 ? "+" : n < 0 ? "−" : "") + b;
  }
  function pct(n) {
    if (n == null || n !== n) return "—";
    var a = Math.abs(n);
    var d = a !== 0 && a < 0.1 ? 3 : 2;
    return (n > 0 ? "+" : n < 0 ? "−" : "") + a.toFixed(d) + "%";
  }
  function paint(v, scale) {
    if (v == null || !scale) return ["#1e222d", "#787b86"];
    var t = Math.max(-1, Math.min(1, v / scale));
    var u = Math.pow(Math.abs(t), 0.55);
    var src = t < 0 ? [242, 54, 69] : [8, 153, 129];
    var mid = [19, 23, 34];
    var r = Math.round(mid[0] + (src[0] - mid[0]) * u);
    var g = Math.round(mid[1] + (src[1] - mid[1]) * u);
    var b = Math.round(mid[2] + (src[2] - mid[2]) * u);
    return ["rgb(" + r + "," + g + "," + b + ")", u > 0.42 ? "#fff" : "#d1d4dc"];
  }
  function pScale(vals) {
    var a = [];
    for (var i = 0; i < vals.length; i++) if (vals[i] != null) a.push(Math.abs(vals[i]));
    if (!a.length) return 1;
    a.sort(function (x, y) { return x - y; });
    return a[Math.min(a.length - 1, Math.floor(a.length * 0.82))] || a[a.length - 1] || 1;
  }
  function worst(row, w) {
    var s = 0, mx = 0, mn = 1e99, i;
    for (i = 0; i < row.length; i++) { s += row[i].a; if (row[i].a > mx) mx = row[i].a; if (row[i].a < mn) mn = row[i].a; }
    var s2 = s * s, w2 = w * w;
    return Math.max(w2 * mx / s2, s2 / (w2 * mn));
  }
  function tiles(items, W, H) {
    var out = [], row = [], x = 0, y = 0, w = W, h = H;
    function place(r) {
      var s = 0, i, off = 0, horiz = w >= h, thick, side;
      for (i = 0; i < r.length; i++) s += r[i].a;
      thick = s / (horiz ? h : w);
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
  function prof(f) {
    return ((((f.profiles || {}).current || {}).summary || {}).numeric) || {};
  }
  function flowOf(f, win) {
    var w = (((f.flows || {}).aligned_windows || {})[win]) || null;
    if (!w) return null;
    return { usd: num(w.flow_usd_decimal), pctA: num(w.flow_to_end_assets_pct_decimal) };
  }
  function rowsOf(packet) {
    return Object.keys(packet.funds || {}).map(function (t) {
      var f = packet.funds[t], p = prof(f);
      var h = (f.holdings || {}).current || {};
      var q = h.quality || {};
      var st = (f.holdings || {}).comparison_status_counts || {};
      var f1 = flowOf(f, "1"), f5 = flowOf(f, "5"), f21 = flowOf(f, "21");
      return {
        t: t, aum: num(p.aum),
        nHold: num(p.num_holdings) || num(q.returned_rows),
        both: num(st.observed_in_both),
        added: num(st.observed_only_in_current),
        deleted: num(st.observed_only_in_prior),
        asof: h.processed_date || "",
        spread: num(p.bid_ask_spread),
        f1: f1 && f1.usd, p1: f1 && f1.pctA,
        f5: f5 && f5.usd, p5: f5 && f5.pctA,
        f21: f21 && f21.usd, p21: f21 && f21.pctA
      };
    });
  }
  function sortRows(rows, key, dir) {
    rows.sort(function (a, b) {
      var av = a[key], bv = b[key];
      if (av == null && bv == null) return a.t < b.t ? -1 : 1;
      if (av == null) return 1;
      if (bv == null) return -1;
      if (typeof av === "string") return dir * av.localeCompare(bv);
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
        else { keyRef.k = k; keyRef.d = (k === "t" || k === "asof") ? 1 : -1; }
        draw();
      };
    });
  }

  var CSS = ".jh-etf{font:12px/1.25 'Trebuchet MS',system-ui,sans-serif;color:#d1d4dc;background:#131722;height:100%;display:flex;flex-direction:column}"
    + ".jh-etf .top{display:flex;align-items:center;gap:6px;padding:6px 10px;background:#1e222d;border-bottom:1px solid #2a2e39;flex:none;flex-wrap:wrap}"
    + ".jh-etf .brand{font-weight:700;letter-spacing:.08em;font-size:11px;color:#fff;margin-right:4px}"
    + ".jh-etf .tabs button,.jh-etf .win button{padding:6px 9px;color:#787b86;background:transparent;border:0;border-bottom:2px solid transparent;cursor:pointer;font:600 11px system-ui}"
    + ".jh-etf .tabs button.on,.jh-etf .win button.on{color:#fff;border-bottom-color:#2962ff}"
    + ".jh-etf .meta{margin-left:auto;color:#b2b5be;font-size:11px;font-variant-numeric:tabular-nums}"
    + ".jh-etf .body{flex:1;min-height:0;overflow:auto;position:relative;background:#0c1016}"
    + ".jh-etf table{width:100%;border-collapse:collapse}"
    + ".jh-etf th{text-align:right;padding:7px 8px;background:#1e222d;position:sticky;top:0;cursor:pointer;color:#787b86;font-weight:600;white-space:nowrap}"
    + ".jh-etf th:first-child,.jh-etf td:first-child{text-align:left}"
    + ".jh-etf td{padding:5px 8px;border-bottom:1px solid #2a2e39;text-align:right;font-variant-numeric:tabular-nums}"
    + ".jh-etf tr:hover td{filter:brightness(1.12)}"
    + ".jh-etf .map{position:relative;height:100%;min-height:480px;background:#0c1016}"
    + ".jh-etf .tile{position:absolute;box-sizing:border-box;overflow:hidden;display:flex;flex-direction:column;justify-content:center;padding:2px 5px;border-radius:2px}"
    + ".jh-etf .tile b{font-weight:700;letter-spacing:.02em;line-height:1.05}"
    + ".jh-etf .tile span{font-variant-numeric:tabular-nums;line-height:1.15}"
    + ".jh-etf .note{padding:10px 12px;color:#787b86;font-size:11px}"
    + ".jh-etf .boards{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:8px;padding:8px}"
    + ".jh-etf .board{background:#1e222d;border:1px solid #2a2e39;border-radius:6px;padding:8px 10px}"
    + ".jh-etf .board h3{margin:0 0 6px;font-size:11px;letter-spacing:.06em;color:#787b86;font-weight:700}"
    + ".jh-etf .board .row{display:flex;justify-content:space-between;gap:8px;padding:3px 0;border-bottom:1px solid #2a2e39}";

  var BOARD_NAME = {
    tech_leaders: "TECH LEADERS",
    momentum_12_1: "12−1 MOMENTUM",
    double_bottoms: "DOUBLE BOTTOMS",
    double_tops: "DOUBLE TOPS",
    lowest_risk: "LOWEST RISK",
    decay_careful: "DECAY · CAREFUL"
  };

  function render(host, packet) {
    var all = rowsOf(packet);
    var win = "1", mode = "heat", tab = "flows";
    host.innerHTML = "<style>" + CSS + "</style><div class=jh-etf>"
      + "<div class=top><span class=brand>ETF DESK</span><div class=tabs id=etf-tabs></div><div class=win id=etf-win></div>"
      + "<span class=meta id=etf-meta></span></div><div class=body id=etf-body></div></div>";
    var body = host.querySelector("#etf-body");
    var tabs = [["flows", "Flows"], ["holdings", "Holdings"], ["census", "Census"], ["tech", "Technicals"], ["tops", "Top Held/Added/Deleted"]];
    host.querySelector("#etf-tabs").innerHTML = tabs.map(function (t) {
      return "<button type=button data-tab='" + t[0] + "'" + (t[0] === tab ? " class=on" : "") + ">" + t[1] + "</button>";
    }).join("");
    var flowKey = { k: "f1", d: -1 }, holdKey = { k: "nHold", d: -1 }, techKey = { k: "t", d: 1 };
    var tech = {};
    function flowKeyName() { return win === "1" ? "f1" : win === "5" ? "f5" : "f21"; }
    function pctKey() { return win === "1" ? "p1" : win === "5" ? "p5" : "p21"; }
    function setWin() {
      host.querySelector("#etf-win").innerHTML = tab === "flows"
        ? ["1", "5", "21"].map(function (w) {
            return "<button type=button data-w='" + w + "'" + (w === win ? " class=on" : "") + ">" + w + "D</button>";
          }).join("") + "<button type=button data-mode=heat" + (mode === "heat" ? " class=on" : "") + ">Heat</button><button type=button data-mode=table" + (mode === "table" ? " class=on" : "") + ">Table</button>"
        : "";
      host.querySelectorAll("#etf-win button").forEach(function (b) {
        b.onclick = function () {
          if (b.getAttribute("data-w")) win = b.getAttribute("data-w");
          if (b.getAttribute("data-mode")) mode = b.getAttribute("data-mode");
          draw();
        };
      });
    }
    function flowMeta(list) {
      var net = 0, inn = 0, out = 0, i;
      for (i = 0; i < list.length; i++) {
        net += list[i][flowKeyName()];
        if (list[i][flowKeyName()] > 0) inn++;
        else if (list[i][flowKeyName()] < 0) out++;
      }
      var quiet = all.length - list.length;
      host.querySelector("#etf-meta").textContent = list.length + " prints · " + win + "D net " + money(net) + " · " + inn + " in · " + out + " out" + (quiet ? " · " + quiet + " no print" : "");
    }
    function showFlows() {
      var k = flowKeyName(), pk = pctKey();
      var list = all.filter(function (r) { return r[k] != null; });
      flowMeta(list);
      if (!list.length) {
        body.innerHTML = "<div class=note>No " + win + "D creation/redemption prints in this packet.</div>";
        return;
      }
      var scale = pScale(list.map(function (r) { return r[k]; }));
      if (mode === "table") {
        if (flowKey.k === "p1" || flowKey.k === "p5" || flowKey.k === "p21") flowKey.k = pk;
        else if (flowKey.k !== "t" && flowKey.k !== "aum") flowKey.k = k;
        sortRows(list, flowKey.k, flowKey.d);
        table(body, [["t", "Ticker"], ["aum", "AUM"], [k, win + "D flow"], [pk, "% of AUM"]], list, flowKey.k, flowKey.d, function (r) {
          var c = paint(r[k], scale);
          return "<td>" + esc(r.t) + "</td><td>" + (r.aum ? money(r.aum).replace(/^\+/, "") : "—") + "</td><td style='background:" + c[0] + ";color:" + c[1] + "'>" + money(r[k]) + "</td><td>" + pct(r[pk]) + "</td>";
        });
        bindSort(body, flowKey, draw);
        return;
      }
      var box = document.createElement("div");
      box.className = "map";
      body.innerHTML = "";
      body.appendChild(box);
      var W = box.clientWidth || host.clientWidth || 960;
      var H = box.clientHeight || 560;
      if (H < 200) H = 560;
      var area = 0;
      list.forEach(function (r) { r.a = Math.sqrt(Math.abs(r[k]) || 0); area += r.a; });
      list.sort(function (a, b) { return b.a - a.a; });
      var unit = (W * H) / (area || 1);
      list.forEach(function (r) { r.a *= unit; });
      tiles(list, W, H).forEach(function (t) {
        var c = paint(t.it[k], scale);
        var el = document.createElement("div");
        el.className = "tile";
        el.title = t.it.t + "  " + money(t.it[k]) + "  " + pct(t.it[pk]) + " of AUM";
        el.style.cssText = "left:" + (t.x + 1).toFixed(1) + "px;top:" + (t.y + 1).toFixed(1) + "px;width:" + Math.max(0, t.w - 2).toFixed(1) + "px;height:" + Math.max(0, t.h - 2).toFixed(1) + "px;background:" + c[0] + ";color:" + c[1];
        var fs = Math.max(10, Math.min(18, Math.floor(Math.min(t.w, t.h) / 3.6)));
        var showMoney = t.h > 34 && t.w > 46;
        var showPct = t.h > 52 && t.w > 64;
        el.innerHTML = "<b style='font-size:" + fs + "px'>" + esc(t.it.t) + "</b>"
          + (showMoney ? "<span style='font-size:" + Math.max(10, fs - 2) + "px'>" + money(t.it[k]) + "</span>" : "")
          + (showPct ? "<span style='font-size:" + Math.max(10, fs - 3) + "px'>" + pct(t.it[pk]) + "</span>" : "");
        box.appendChild(el);
      });
    }
    function showHold() {
      var list = all.slice();
      sortRows(list, holdKey.k, holdKey.d);
      host.querySelector("#etf-meta").textContent = list.length + " funds · names vs prior book · click a header to sort";
      table(body, [["t", "Ticker"], ["nHold", "Holdings"], ["both", "In both"], ["added", "Added"], ["deleted", "Deleted"], ["asof", "As of"], ["aum", "AUM"]], list, holdKey.k, holdKey.d, function (r) {
        return "<td>" + esc(r.t) + "</td><td>" + (r.nHold == null ? "—" : Math.round(r.nHold)) + "</td><td>" + (r.both == null ? "—" : r.both) + "</td><td>" + (r.added == null ? "—" : r.added) + "</td><td>" + (r.deleted == null ? "—" : r.deleted) + "</td><td>" + esc(r.asof || "—") + "</td><td>" + (r.aum ? money(r.aum).replace(/^\+/, "") : "—") + "</td>";
      });
      bindSort(body, holdKey, draw);
    }
    function showTops() {
      host.querySelector("#etf-meta").textContent = "By fund. Comparison rows do not carry stock tickers.";
      function col(title, key) {
        var list = all.filter(function (r) { return r[key] != null; }).slice();
        sortRows(list, key, -1);
        var h = "<div class=board><h3>" + title + "</h3>";
        list.slice(0, 20).forEach(function (r, i) {
          h += "<div class=row><span>" + (i + 1) + "  <b>" + esc(r.t) + "</b></span><span>" + Math.round(r[key]) + "</span></div>";
        });
        return h + "</div>";
      }
      body.innerHTML = "<div class=boards>" + col("MOST NAMES HELD", "nHold") + col("MOST NAMES ADDED", "added") + col("MOST NAMES DELETED", "deleted") + "</div>"
        + "<div class=note>Added = identities only in the current book. Deleted = identities only in the prior book. The tape stores those rows as hashes, not tickers, so this ranks funds — not “AAPL held by N ETFs.”</div>";
    }
    function showCensus() {
      host.querySelector("#etf-meta").textContent = "ETF census";
      body.innerHTML = "<div class=note>Loading census…</div>";
      fetch("/data/etf-census.json", { credentials: "omit" }).then(function (r) {
        if (!r.ok) throw new Error(String(r.status));
        return r.json();
      }).then(function (c) {
        if (tab !== "census") return;
        var boards = c.boards || {};
        var html = "<div class=boards>";
        Object.keys(boards).forEach(function (name) {
          var rows = boards[name] || [];
          var label = BOARD_NAME[name] || name.replace(/_/g, " ").toUpperCase();
          html += "<div class=board><h3>" + esc(label) + " · " + rows.length + "</h3>";
          rows.slice(0, 16).forEach(function (row, i) {
            var sym = Array.isArray(row) ? row[0] : row;
            var sc = Array.isArray(row) ? row[1] : "";
            html += "<div class=row><span>" + (i + 1) + "  " + esc(sym) + "</span><span>" + (sc === "" || sc == null ? "" : esc(sc)) + "</span></div>";
          });
          html += "</div>";
        });
        html += "</div><table><thead><tr><th>Pair</th><th>Z</th><th>Ratio</th><th>Weeks</th><th>Note</th></tr></thead><tbody>";
        (c.pairs || []).forEach(function (p) {
          html += "<tr><td>" + esc(p.pair) + "</td><td>" + esc(p.z) + "</td><td>" + esc(p.ratio) + "</td><td>" + esc(p.n_weeks) + "</td><td>" + esc(p.note || "") + "</td></tr>";
        });
        html += "</tbody></table><div class=note>Census " + esc(c.generated_at || "") + " · " + esc(c.n) + " names.</div>";
        body.innerHTML = html;
      }).catch(function (e) {
        body.innerHTML = "<div class=note>Census failed: " + esc(e.message) + "</div>";
      });
    }
    function showTech() {
      var list = all.map(function (r) { return Object.assign({}, r, tech[r.t] || {}); });
      sortRows(list, techKey.k, techKey.d);
      var done = Object.keys(tech).length;
      host.querySelector("#etf-meta").textContent = "Distance to MA · spread in bp · effort = today volume / prior 20d avg · " + done + "/" + all.length + " bars";
      function cellMA(r, key) {
        var v = r[key];
        if (v == null) return "<td>—</td>";
        var c = paint(v, 8);
        return "<td style='background:" + c[0] + ";color:" + c[1] + "'>" + pct(v) + "</td>";
      }
      table(body, [["t", "Ticker"], ["spread", "Spread bp"], ["rng", "Day range"], ["ma9", "9"], ["ma20", "20"], ["ma50", "50"], ["ma100", "100"], ["ma200", "200"], ["ma250", "250"], ["ret", "Result"], ["eff", "Effort"], ["evr", "EVR"]], list, techKey.k, techKey.d, function (r) {
        return "<td>" + esc(r.t) + "</td><td>" + (r.spread == null ? "—" : (r.spread * 10000).toFixed(2)) + "</td>"
          + (r.rng == null ? "<td>—</td>" : "<td>" + r.rng.toFixed(2) + "%</td>")
          + cellMA(r, "ma9") + cellMA(r, "ma20") + cellMA(r, "ma50") + cellMA(r, "ma100") + cellMA(r, "ma200") + cellMA(r, "ma250")
          + (r.ret == null ? "<td>—</td>" : "<td>" + pct(r.ret) + "</td>")
          + (r.eff == null ? "<td>—</td>" : "<td>" + r.eff.toFixed(2) + "×</td>")
          + (r.evr == null ? "<td>—</td>" : "<td>" + r.evr.toFixed(2) + "</td>");
      });
      bindSort(body, techKey, draw);
      if (host._techStarted) return;
      host._techStarted = true;
      var q = all.map(function (r) { return r.t; });
      var i = 0;
      function step() {
        if (i >= q.length) return;
        var batch = q.slice(i, i + 4);
        i += 4;
        Promise.all(batch.map(loadTech)).then(function () {
          if (tab === "tech") showTech();
          step();
        });
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
        var bars = (j && j.bars) || [];
        var n = bars.length;
        if (n < 10) { tech[sym] = {}; return; }
        function sma(len) {
          if (n < len) return null;
          var s = 0, k;
          for (k = n - len; k < n; k++) s += +bars[k][4];
          var m = s / len;
          return m ? (bars[n - 1][4] / m - 1) * 100 : null;
        }
        var last = bars[n - 1], prev = bars[n - 2];
        var ret = prev && prev[4] ? (last[4] / prev[4] - 1) * 100 : null;
        var rng = last[4] ? (last[2] - last[3]) / last[4] * 100 : null;
        var from = Math.max(0, n - 21), sV = 0, c = 0, k;
        for (k = from; k < n - 1; k++) { sV += +bars[k][5] || 0; c++; }
        var avg = c ? sV / c : null;
        var eff = avg ? (+last[5] || 0) / avg : null;
        tech[sym] = { ma9: sma(9), ma20: sma(20), ma50: sma(50), ma100: sma(100), ma200: sma(200), ma250: sma(250), ret: ret, rng: rng, eff: eff, evr: eff ? ret / eff : null };
      }).catch(function () { tech[sym] = {}; });
    }
    function draw() {
      setWin();
      if (tab === "flows") showFlows();
      else if (tab === "holdings") showHold();
      else if (tab === "census") showCensus();
      else if (tab === "tech") showTech();
      else showTops();
    }
    host.querySelectorAll("#etf-tabs button").forEach(function (b) {
      b.onclick = function () {
        tab = b.getAttribute("data-tab");
        host.querySelectorAll("#etf-tabs button").forEach(function (x) { x.className = x === b ? "on" : ""; });
        draw();
      };
    });
    window.addEventListener("resize", function () { if (tab === "flows" && mode === "heat") showFlows(); });
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
