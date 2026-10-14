/* jh-reskin-skip */
/* ETF Desk — regime dates, breadth lines, country tiles, spread maps, chart handoff, slim book. */
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
  function mix(a, b, u) {
    return "rgb(" + [0, 1, 2].map(function (i) { return Math.round(a[i] + (b[i] - a[i]) * u); }).join(",") + ")";
  }
  function tightPaint(v, scale) {
    if (v == null || v !== v || !scale) return ["#1e222d", "#787b86"];
    var t = Math.max(0, Math.min(1, v / scale));
    var a = t < 0.55 ? [0, 188, 212] : [255, 167, 38];
    var b = t < 0.55 ? [255, 167, 38] : [242, 54, 69];
    var u = t < 0.55 ? t / 0.55 : (t - 0.55) / 0.45;
    return [mix(a, b, u), t > 0.72 ? "#fff" : "#10141c"];
  }
  function pricePaint(v, scale) {
    if (v == null || v !== v || !scale) return ["#1e222d", "#787b86"];
    var t = Math.max(-1, Math.min(1, v / scale));
    var u = Math.pow(Math.abs(t), 0.55);
    var src = t < 0 ? [255, 152, 0] : [41, 98, 255];
    return [mix([19, 23, 34], src, u), u > 0.55 ? "#fff" : "#d1d4dc"];
  }
  function countryColor(v, scale) {
    if (v == null || v !== v) return ["#3a3f4b", "#d1d4dc"];
    if (v === 0) return ["#1e222d", "#b2b5be"];
    return paint(v, scale);
  }
  function openChart(sym) {
    sym = String(sym || "").toUpperCase();
    if (!/^[A-Z][A-Z0-9.\-]{0,9}$/.test(sym)) return;
    try {
      var p = window.parent;
      if (p && p !== window && typeof p.jhGoSymbol === "function") {
        p.jhGoSymbol(sym);
        var btn = p.document && p.document.getElementById("wsx");
        if (btn) btn.click();
        else {
          var ov = p.document && p.document.getElementById("ws-overlay");
          if (ov) ov.className = "";
        }
        return;
      }
    } catch (err) {}
    window.location.href = "/chart.html?s=" + encodeURIComponent(sym);
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
  var EM_T = { EEM:1, VWO:1, IEMG:1, EWZ:1, FXI:1, MCHI:1, INDA:1, EWY:1, EWT:1, EIDO:1, EZA:1, EWW:1, THD:1, TUR:1, ECH:1, EPU:1, EPOL:1, KWEB:1, EWX:1, FNDE:1, ASHR:1, GXC:1 };
  var FR_T = { FM:1, VNM:1, AFK:1, FRN:1, FMQQ:1, VNAM:1, KPHO:1, ARGT:1 };
  var IN_T = { EWJ:1, EFA:1, IEFA:1, VEA:1, EFNL:1, EWG:1, EWU:1, EWQ:1, EWL:1, EWI:1, EWP:1, EWC:1, EWA:1, EWS:1, EWH:1, EWN:1, EWD:1, EZU:1, VGK:1, EPP:1, VXUS:1, ACWX:1 };
  var EM_C = { China:1, Brazil:1, Taiwan:1, "South Korea":1, India:1, Mexico:1, "South Africa":1, Malaysia:1, Thailand:1, Indonesia:1, Turkey:1, Chile:1, Colombia:1, Peru:1, Poland:1, Greece:1, Philippines:1, Hungary:1, Egypt:1, "United Arab Emirates":1, Qatar:1, Kuwait:1 };
  var FR_C = { Argentina:1, Uruguay:1, Vietnam:1, Nigeria:1, Kenya:1, Morocco:1, Romania:1, Kazakhstan:1, Pakistan:1, Bangladesh:1, "Sri Lanka":1, Jordan:1, Tunisia:1, Croatia:1, Bahrain:1, Oman:1 };
  var DEV_C = { Canada:1, Israel:1, "Hong Kong":1, "United Kingdom":1, Singapore:1, Switzerland:1, Japan:1, Australia:1, Ireland:1, Netherlands:1, France:1, Germany:1, Sweden:1, Belgium:1, Italy:1, Denmark:1, Spain:1, Norway:1, Finland:1, Austria:1, "New Zealand":1, Portugal:1, Luxembourg:1 };
  function frontierName(name, ticker) {
    if (FR_T[ticker]) return true;
    return /vietnam|nigeria|kenya|morocco|pakistan|romania|kazakhstan|argentina|africa index|frontier markets|msci frontier/.test(String(name || "").toLowerCase());
  }
  function deskMkt(ticker, region, category, sub) {
    if (FR_T[ticker] || frontierName("", ticker)) return "Frontier";
    if (EM_T[ticker]) return "Emerging";
    if (IN_T[ticker]) return "International";
    var s = String(sub || "").toLowerCase();
    if (/vietnam|msci frontier|frontier markets/.test(s)) return "Frontier";
    if (region === "EM" || /emerging|china|india|brazil|korea|taiwan|mexico/.test(s)) return "Emerging";
    if (region === "Intl" || /japan|eafe|developed|europe/.test(s)) return "International";
    return "US";
  }
  function etfAsset(name) {
    var s = String(name || "").toLowerCase();
    if (/bitcoin|ethereum|crypto/.test(s)) return "Crypto";
    if (/gold|silver|oil|commodity|copper|agriculture|platinum|palladium|natural gas/.test(s)) return "Commodities";
    if (/bond|treasury|fixed income|municipal|tips|high yield|corporate bond|mortgage|aggregate/.test(s)) return "Fixed Income";
    if (/real estate|reit/.test(s)) return "Real Estate";
    if (/currency|\bdollar\b|yen|euro/.test(s)) return "Currency";
    return "Equity";
  }
  function etfMkt(name, ticker) {
    if (frontierName(name, ticker)) return "Frontier";
    if (EM_T[ticker]) return "Emerging";
    if (IN_T[ticker]) return "International";
    var s = String(name || "").toLowerCase();
    if (/emerging|\bchina\b|india|brazil|korea|taiwan|mexico|latin america|south africa|indonesia|thailand|turkey|\bchile\b|poland|malaysia|philippines|saudi/.test(s)) return "Emerging";
    if (/eafe|international|ex-us|ex us|\bjapan\b|europe|germany|united kingdom|\bfrance\b|switzerland|australia|hong kong|developed/.test(s)) return "International";
    return "US";
  }
  function stockMkt(country) {
    if (!country || country === "USA") return "US";
    if (FR_C[country]) return "Frontier";
    if (EM_C[country]) return "Emerging";
    if (DEV_C[country]) return "International";
    return "Other";
  }
  function maStats(bars) {
    var n = bars.length, ps = [0], i;
    for (i = 0; i < n; i++) ps.push(ps[i] + (+bars[i][4] || 0));
    function sma(i, len) { return i + 1 < len ? null : (ps[i + 1] - ps[i + 1 - len]) / len; }
    function dist(len) { var m = sma(n - 1, len); return m ? (bars[n - 1][4] / m - 1) * 100 : null; }
    function occ(len, win) {
      var start = Math.max(len - 1, n - win), c = 0, t = 0, k, m;
      for (k = start; k < n; k++) { m = sma(k, len); if (!m) continue; t++; if (bars[k][4] < m) c++; }
      return t ? (c / t) * 100 : null;
    }
    function streak(len) {
      var d = 0, k, m;
      for (k = n - 1; k >= len - 1; k--) { m = sma(k, len); if (m && bars[k][4] < m) d++; else break; }
      return d;
    }
    return { ma250: dist(250), belowDays: streak(250), b21: occ(250, 21), b63: occ(250, 63), b250: occ(250, 250) };
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
        kind: "etf",
        mkt: deskMkt(t, c.region, c.category, c.subcategory),
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
      var r = { t: m.tickers[i], asset: m.asset_classes[i] || "—", category: m.categories[i] || "—", name: m.names[i] || "", kind: "etf", mkt: deskMkt(m.tickers[i], "", m.categories[i], "") };
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
    + ".jh-etf .map.flow{position:relative;height:280px;min-height:280px;flex:none;margin:0 8px 8px;background:#0c1016}"
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
    + ".jh-etf .board .row{display:flex;justify-content:space-between;gap:8px;padding:3px 0;border-bottom:1px solid #2a2e39}"+ ".jh-etf .regime{display:flex;gap:0;background:#0e1420;border-bottom:1px solid #2a2e39;flex:none;overflow:auto}"+ ".jh-etf .rc{min-width:156px;padding:7px 10px 8px;border-right:1px solid #2a2e39}"+ ".jh-etf .rc .asof{font-size:15px;font-weight:800;color:#fff;letter-spacing:.01em;font-variant-numeric:tabular-nums}"+ ".jh-etf .rc .lab{font-size:10px;letter-spacing:.04em;color:#9aa0ac;margin-top:2px}"+ ".jh-etf .rc .val{font-size:12px;font-weight:700;margin-top:3px;font-variant-numeric:tabular-nums;color:#d1d4dc}"+ ".jh-etf .lag{padding:0 10px 7px;background:#0e1420;color:#fff;font-size:12px;font-weight:700;border-bottom:1px solid #2a2e39}"+ ".jh-etf .breadth{height:220px;margin:8px 8px 0;background:#0c1016;border:1px solid #2a2e39;border-radius:4px}"+ ".jh-etf .breadth svg{width:100%;height:100%;display:block}"+ ".jh-etf [data-sym]{cursor:pointer}"+ ".jh-etf [data-sym]:hover{outline:1px solid #d1d4dc}";

  function render(host, packet) {
    var all = rowsOf(packet);
    var extra = { matrix: null, finviz: null, boards: null };
    var tech = {};
    var state = {
      tab: "flows", win: "1", mode: "heat", src: "funds", group: "asset", cls: "All", q: "", scope: "all", belowWin: "63",
      hold: "names", censusView: "all", metric: "tech", techMode: "weekly",
      flowSort: { k: "f1", d: -1 }, holdSort: { k: "nHold", d: -1 }, cenSort: { k: "tech_score", d: -1 },
      techSort: { k: "mom_12_1_pct", d: -1 }, topSort: { k: "net1", d: -1 }
    };
    host.innerHTML = "<style>" + CSS + "</style><div class=jh-etf>"
      + "<div class=top><span class=brand>ETF DESK</span><div class=tabs id=etf-tabs></div><div class=scope id=etf-scope></div><div class=win id=etf-win></div>"
      + "<input id=etf-q placeholder='Ticker' autocomplete=off><span class=meta id=etf-meta></span></div>"
      + "<div id=etf-regime></div>"
      + "<div class=body id=etf-body></div></div>";
    var body = host.querySelector("#etf-body");
    var flowAsOf = (function () {
      var t0 = packet.funds.SPY || packet.funds[Object.keys(packet.funds)[0]];
      var w = ((((t0 || {}).flows || {}).aligned_windows || {})["1"]) || {};
      return w.end_date || "";
    })();
    var barAsOf = "";
    var techSeries = {};
    var techPending = {};
    var byT = {};
    all.forEach(function (r) { byT[r.t] = r; });
    body.addEventListener("click", function (e) {
      var el = e.target && e.target.closest ? e.target.closest("[data-sym]") : null;
      if (!el || !body.contains(el)) return;
      openChart(el.getAttribute("data-sym"));
    });
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
          if (b.getAttribute("data-bw")) state.belowWin = b.getAttribute("data-bw");
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
    function drawTiles(box, list, k, labelOf, subOf, opt) {
      opt = opt || {};
      var colorOf = opt.color || paint;
      var W = box.clientWidth || host.clientWidth || 960;
      var H = box.clientHeight || 420;
      if (W < 80) W = host.clientWidth || 960;
      if (H < 80) H = 420;
      var scale = pScale(list.map(function (r) { return r[k]; }));
      var area = 0;
      list.forEach(function (r) {
        var mag = opt.area ? Math.abs(r[opt.area] || 0) : Math.abs(r[k] || 0);
        r.a = Math.sqrt(mag) || 0.35;
        area += r.a;
      });
      list.sort(function (a, b) { return b.a - a.a; });
      var unit = (W * H) / (area || 1);
      list.forEach(function (r) { r.a *= unit; });
      tiles(list, W, H).forEach(function (t) {
        var c = colorOf(t.it[k], scale);
        var el = document.createElement("div");
        el.className = "tile";
        var label = labelOf(t.it);
        var sub = subOf ? subOf(t.it) : "";
        var shown = opt.fmt ? opt.fmt(t.it[k]) : money(t.it[k]);
        var sym = opt.symOf ? opt.symOf(t.it) : (/^[A-Z][A-Z0-9.\-]{0,9}$/.test(label) ? label : "");
        if (sym) el.setAttribute("data-sym", sym);
        el.title = label + "  " + shown + (sub ? "  " + sub : "") + (sym ? "  · open chart" : "");
        el.style.cssText = "left:" + (t.x + 1).toFixed(1) + "px;top:" + (t.y + 1).toFixed(1) + "px;width:" + Math.max(0, t.w - 2).toFixed(1) + "px;height:" + Math.max(0, t.h - 2).toFixed(1) + "px;background:" + c[0] + ";color:" + c[1];
        var fs = Math.max(10, Math.min(16, Math.floor(Math.min(t.w, t.h) / 4)));
        el.innerHTML = "<b style='font-size:" + fs + "px'>" + esc(label) + "</b>"
          + (t.h > 32 && t.w > 48 ? "<span>" + esc(shown) + "</span>" : "")
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


    function isEquityEtf(r) {
      if (r.kind === "stock" || r.kind === "cef") return false;
      var a = String(r.asset || "");
      if (/fixed|bond|commod|currenc|crypto|real estate|gold|multi|alternative/i.test(a)) return false;
      return a === "Equity" || /equit|leverag|sector|large cap|intl|emerging/i.test(a);
    }
    var _uniIdxN = -1, _uniIdx = null;
    function uniIndex() {
      var n = extra.uni ? extra.uni.length : 0;
      if (_uniIdx && _uniIdxN === n) return _uniIdx;
      _uniIdx = {};
      _uniIdxN = n;
      (extra.uni || []).forEach(function (r) { _uniIdx[r.t] = r; });
      return _uniIdx;
    }
    function mktOf(r) {
      if (r.mkt && r.mkt !== "US") return r.mkt;
      var u = uniIndex()[r.t];
      if (u && u.mkt) return u.mkt;
      return r.mkt || "US";
    }
    function passScope(r) {
      var s = state.scope;
      if (!s || s === "all") return true;
      if (s === "eqEtf") return isEquityEtf(r);
      if (s === "eqStock") return r.kind === "stock";
      var want = s === "intl" ? "International" : s === "em" ? "Emerging" : s === "frontier" ? "Frontier" : "";
      if (want) return mktOf(r) === want;
      return true;
    }
    function applySlim(j) {
      if (!j || !j.t) throw new Error("slim");
      var rows = [], i, n = j.t.length;
      var M = { U: "US", I: "International", E: "Emerging", F: "Frontier", O: "Other" };
      var K = { s: "stock", e: "etf", c: "cef" };
      for (i = 0; i < n; i++) {
        rows.push({
          t: j.t[i],
          name: (j.nm && j.nm[i]) || "",
          kind: K[j.k.charAt(i)] || "stock",
          mkt: M[j.m.charAt(i)] || "US",
          country: (j.countries || [])[j.co[i]] || "",
          sector: (j.sectors || [])[j.se[i]] || "",
          asset: (j.assets || [])[j.as[i]] || "Equity",
          mcap: j.mc[i], price: j.px[i],
          sma20: j.a20[i], sma50: j.a50[i], sma200: j.a200[i],
          perfM: j.pm[i], atrPct: j.atr[i]
        });
      }
      extra.uni = rows;
      extra.uniAt = j.generated_at || "";
      if (host.isConnected) draw();
    }
    function ensureUni() {
      if (extra.uni || host._slimStarted) return;
      host._slimStarted = true;
      if (window.JHEtfSlim && window.JHEtfSlim.t) { try { applySlim(window.JHEtfSlim); } catch (e) { extra.uni = []; extra.uniErr = e.message; if (host.isConnected) draw(); } return; }
      var el = document.createElement("script");
      el.src = "/etf-desk-slim.js";
      el.onload = function () {
        try { applySlim(window.JHEtfSlim); }
        catch (e) { extra.uni = []; extra.uniErr = e.message; if (host.isConnected) draw(); }
      };
      el.onerror = function () { extra.uni = []; extra.uniErr = "slim"; if (host.isConnected) draw(); };
      (document.head || document.documentElement).appendChild(el);
    }
    function ensureIssuer() {
      if (extra.issuer || host._issStarted) return;
      host._issStarted = true;
      fetch("/data/etf-true-flows.json", { credentials: "omit" }).then(function (r) {
        if (!r.ok) throw new Error(String(r.status));
        return r.json();
      }).then(function (j) {
        extra.true = j;
        var out = [], by = j.by_etf || {};
        Object.keys(by).forEach(function (sym) {
          var fw = by[sym].flow_windows || {};
          function val(w) {
            var d = fw[w];
            if (!d || d.status !== "complete_descriptive_estimate") return null;
            return num(d.value_decimal);
          }
          var f1 = val("1d"), f5 = val("5d"), f21 = val("20d");
          var end1 = (fw["1d"] || {}).end_date || "";
          if (!extra.issMap) extra.issMap = {};
          if (f1 != null) extra.issMap[sym] = { f1: f1, end: end1 };
          if (f1 == null && f5 == null && f21 == null) return;
          var mkt = deskMkt(sym, "", by[sym].category, "");
          if (all.some(function (a) { return a.t === sym; })) return;
          out.push({ t: sym, kind: "etf", asset: "Equity", category: String(by[sym].category || "").toLowerCase(), issuer: "issuer tape", lev: "—", region: mkt, mkt: mkt, f1: f1, p1: null, f5: f5, p5: null, f21: f21, p21: null, aum: num(by[sym].aum_est_b) != null ? num(by[sym].aum_est_b) * 1e9 : null, source: "issuer" });
        });
        extra.issuer = out;
        extra.issEnd = (j.aggregation_period || {}).end_date || "";
        if (!extra.issMap) extra.issMap = {};
        if (host.isConnected) draw();
      }).catch(function () { extra.issuer = []; extra.issMap = {}; extra.true = extra.true || { error: "issuer" }; });
    }
    function uniRows() { return extra.uni || []; }
    function flowJoin() {
      var m = {};
      all.forEach(function (r) { m[r.t] = r; });
      (extra.issuer || []).forEach(function (r) { if (!m[r.t]) m[r.t] = r; });
      return m;
    }
    function scopeWant() {
      if (state.scope === "intl") return "International";
      if (state.scope === "em") return "Emerging";
      if (state.scope === "frontier") return "Frontier";
      return "";
    }
    function classMatches(type) {
      var s = state.scope, t = String(type || "").toLowerCase();
      if (!s || s === "all") return true;
      if (s === "eqStock") return false;
      if (s === "eqEtf") return /equit/.test(t);
      if (s === "intl") return /global|ex[\.\- ]\s*us|international/.test(t);
      if (s === "em") return /emerg/.test(t);
      if (s === "frontier") return /frontier market/.test(t);
      return true;
    }
    function sleeveMatches(name) {
      var s = state.scope, n = String(name || "").toLowerCase();
      if (!s || s === "all") return true;
      if (s === "eqStock") return false;
      if (s === "eqEtf") return /s&p|total us|small cap|nasdaq|developed|emerging/.test(n);
      if (s === "intl") return /developed|intl|global|ex-us|ex us/.test(n);
      if (s === "em") return /emerg/.test(n);
      if (s === "frontier") return false;
      return true;
    }
    function bookPlusIssuer() {
      var seen = {}, out = [];
      all.forEach(function (r) { seen[r.t] = 1; out.push(r); });
      (extra.issuer || []).forEach(function (r) { if (!seen[r.t]) out.push(r); });
      return out;
    }
    function foreignRosterHtml(want) {
      ensureUni();
      if (!extra.uni) return "<div class=note>Loading foreign ETFs and stocks…</div>";
      var etfs = uniRows().filter(function (r) { return r.kind === "etf" && r.mkt === want; });
      var stocks = uniRows().filter(function (r) { return r.kind === "stock" && r.mkt === want; });
      var join = flowJoin();
      var html = "<div class=note>" + etfs.length + " ETFs and " + stocks.length + " stocks in " + want + ". Creation dollars show only when the evidence tape printed them. A dash is no print, not zero. Frontier-brand US allocation funds are not treated as frontier markets.</div>";
      html += "<table><thead><tr><th>Ticker</th><th>Kind</th><th>Class</th><th>Name</th><th>1D flow</th><th>200d</th><th>Month</th><th>ATR %</th></tr></thead><tbody>";
      etfs.concat(stocks).sort(function (a, b) { return (b.mcap || 0) - (a.mcap || 0); }).slice(0, 80).forEach(function (r) {
        var f = join[r.t];
        html += "<tr><td>" + esc(r.t) + "</td><td>" + esc(r.kind) + "</td><td>" + esc(r.asset) + "</td><td>" + esc(r.name) + "</td><td>" + (f && f.f1 != null ? money(f.f1) : "—") + "</td><td>" + pct(r.sma200) + "</td><td>" + pct(r.perfM) + "</td><td>" + (r.atrPct == null ? "—" : r.atrPct.toFixed(2)) + "</td></tr>";
      });
      return html + "</tbody></table>";
    }
    function showFileMA() {
      ensureUni();
      var rows = uniRows().filter(function (r) { return passScope(r) && (!state.q || r.t.indexOf(state.q) >= 0 || String(r.name || "").toUpperCase().indexOf(state.q) >= 0); });
      if (!extra.uni) { meta("Loading the stock and ETF universe…"); body.innerHTML = "<div class=note>Loading names…</div>"; return; }
      var key = state.techSort.k;
      if (["t", "mkt", "asset", "country", "sector", "mcap", "atrPct", "sma200", "sma50", "sma20", "perfM"].indexOf(key) < 0) key = "sma200";
      state.techSort.k = key;
      sortRows(rows, key, state.techSort.d);
      var shown = rows.slice(0, 250);
      var scale = { sma200: pScale(shown.map(function (r) { return r.sma200; })), perfM: pScale(shown.map(function (r) { return r.perfM; })) };
      meta(shown.length + " of " + rows.length + " · equity stocks are not mixed with equity ETFs · 20/50/200 from the file · " + String(extra.uniAt || "").slice(0, 10));
      table(body, [["t", "Ticker"], ["kind", "Kind"], ["mkt", "Market"], ["asset", "Class"], ["country", "Country"], ["sector", "Sector"], ["mcap", "Mkt cap"], ["atrPct", "ATR %"], ["sma20", "20d"], ["sma50", "50d"], ["sma200", "200d"], ["perfM", "Month"]], shown, key, state.techSort.d, function (r) {
        var c = pricePaint(r.sma200, scale.sma200), p = pricePaint(r.perfM, scale.perfM);
        return "<td>" + esc(r.t) + "</td><td>" + esc(r.kind) + "</td><td>" + esc(r.mkt) + "</td><td>" + esc(r.asset) + "</td><td>" + esc(r.country) + "</td><td>" + esc(r.sector) + "</td><td>" + (r.mcap == null ? "—" : r.mcap.toFixed(0)) + "</td><td>" + (r.atrPct == null ? "—" : r.atrPct.toFixed(2)) + "</td><td>" + pct(r.sma20) + "</td><td>" + pct(r.sma50) + "</td><td style='background:" + c[0] + ";color:" + c[1] + "'>" + pct(r.sma200) + "</td><td style='background:" + p[0] + ";color:" + p[1] + "'>" + pct(r.perfM) + "</td>";
      });
      bindSort(body, state.techSort, draw);
      body.insertAdjacentHTML("beforeend", "<div class=note>Showing " + shown.length + " of " + rows.length + ". Sort and the ticker box cover this scope. Quote spreads for the evidence ETFs are on Technicals → Spreads. The 250-day bar pass is Technicals → Below 250. Stock file has 20/50/200, not 250. Click a ticker to open it on the chart. Blue is above, amber is below. That is price, not creations.</div>");
    }
    function sectorTiles(stocks) {
      var g = {};
      stocks.forEach(function (r) {
        var k = r.sector || "—";
        if (!g[k]) g[k] = { t: k, n: 0, perfs: [], above200: 0, above50: 0, mcap: 0 };
        g[k].n++;
        if (r.perfM != null) g[k].perfs.push(r.perfM);
        if (r.sma200 != null && r.sma200 > 0) g[k].above200++;
        if (r.sma50 != null && r.sma50 > 0) g[k].above50++;
        g[k].mcap += r.mcap || 0;
      });
      return Object.keys(g).map(function (k) {
        var per = g[k].perfs.slice().sort(function (a, b) { return a - b; });
        var mid = per.length ? per[Math.floor(per.length / 2)] : 0;
        g[k].net = mid;
        g[k].a200 = g[k].n ? g[k].above200 / g[k].n * 100 : 0;
        g[k].a50 = g[k].n ? g[k].above50 / g[k].n * 100 : 0;
        g[k].sub = g[k].a200.toFixed(0) + "% >200d · n=" + g[k].n;
        return g[k];
      });
    }
    function sumKnown(rows, key) {
      var s = 0, n = 0, i, v;
      for (i = 0; i < rows.length; i++) {
        v = rows[i][key];
        if (v == null || v !== v) continue;
        s += v; n++;
      }
      return n ? s : null;
    }
    function disagreeRows() {
      var map = extra.issMap || {}, out = [], i, r, iss;
      for (i = 0; i < all.length; i++) {
        r = all[i]; iss = map[r.t];
        if (!iss || r.f1 == null || iss.f1 == null) continue;
        if (Math.abs(r.f1) < 5e6 || Math.abs(iss.f1) < 5e6) continue;
        if ((r.f1 > 0 ? 1 : -1) !== (iss.f1 > 0 ? 1 : -1)) out.push({ t: r.t, cre: r.f1, iss: iss.f1, asset: r.asset });
      }
      out.sort(function (a, b) { return Math.abs(b.cre - b.iss) - Math.abs(a.cre - a.iss); });
      return out;
    }
    function usUnder200() {
      var rows = uniRows().filter(function (r) { return r.kind === "stock" && r.mkt === "US" && r.sma200 != null; });
      var u = 0, i;
      for (i = 0; i < rows.length; i++) if (rows[i].sma200 < 0) u++;
      return { n: rows.length, u: u, pct: rows.length ? u / rows.length * 100 : null };
    }
    function equityUnder250() {
      var rows = all.filter(isEquityEtf), have = 0, under = 0, i, st;
      for (i = 0; i < rows.length; i++) {
        st = tech[rows[i].t];
        if (!st || st.ma250 == null) continue;
        have++;
        if (st.ma250 < 0) under++;
      }
      return { n: rows.length, have: have, under: under, pct: have ? under / have * 100 : null };
    }
    function paintRegime() {
      var el = host.querySelector("#etf-regime");
      if (!el) return;
      var net = sumKnown(all, "f1");
      var eq = equityUnder250();
      var st = usUnder200();
      var emRows = all.filter(function (r) { return r.mkt === "Emerging"; });
      var em = sumKnown(emRows, "f1");
      var names = ["LQD", "HYG", "EMB"], bits = [], csum = 0, cany = false, i, row, dv;
      for (i = 0; i < names.length; i++) {
        row = byT[names[i]];
        dv = row ? row.f1 : null;
        if (dv != null) { csum += dv; cany = true; }
        bits.push(names[i] + " " + (dv == null ? "—" : money(dv)));
      }
      var dis = extra.issMap ? disagreeRows() : null;
      var stockDate = String(extra.uniAt || "").slice(0, 10);
      var issDate = extra.issEnd || "";
      function cell(date, lab, val, color) {
        return "<div class=rc><div class=asof>" + esc(date || "—") + "</div><div class=lab>" + lab + "</div><div class=val" + (color ? " style='color:" + color + "'" : "") + ">" + val + "</div></div>";
      }
      var eqVal = eq.pct == null ? "—" : eq.pct.toFixed(0) + "% under";
      if (eq.n) eqVal += " · " + eq.have + "/" + eq.n + " bars";
      var stVal = extra.uniErr ? ("file " + extra.uniErr) : (st.pct == null ? (extra.uni ? "—" : "…") : st.pct.toFixed(0) + "% under · " + st.u + "/" + st.n);
      var html = "<div class=regime>"
        + cell(flowAsOf, "CREATIONS 1D · EVIDENCE BOOK", net == null ? "—" : money(net))
        + cell(barAsOf || "BARS LOADING", "EQUITY ETFs UNDER 250-DAY", eqVal)
        + cell(stockDate || "STOCK FILE", "US STOCKS UNDER 200-DAY", stVal)
        + cell(flowAsOf, "EMERGING 1D CREATIONS", (em == null ? "—" : money(em)) + " · " + emRows.length + " ETFs")
        + cell(flowAsOf, "CREDIT 1D · LQD HYG EMB", (cany ? money(csum) + " · " : "") + bits.join(" · "))
        + cell(issDate && flowAsOf ? issDate : "ISSUER", "SIGN DISAGREE · NOT A FLOW", dis == null ? "…" : String(dis.length) + " funds", "#ffb020")
        + "</div><div class=lag>" + esc("Creations " + (flowAsOf || "—") + " are not the " + (barAsOf || "bar") + " session. Issuer share-change ends " + (issDate || "—") + ". Stocks use the 200-day file dated " + (stockDate || "—") + ". A dash is a missing print, not zero.") + "</div>";
      if (el.innerHTML !== html) el.innerHTML = html;
    }
    function dayOf(ts) {
      var d = new Date(ts * 1000), y = d.getUTCFullYear(), m = d.getUTCMonth() + 1, da = d.getUTCDate();
      return y + "-" + (m < 10 ? "0" : "") + m + "-" + (da < 10 ? "0" : "") + da;
    }
    function belowFlags(bars) {
      var n = bars.length, i;
      if (n < 250) return null;
      var ps = [0];
      for (i = 0; i < n; i++) ps.push(ps[i] + (+bars[i][4] || 0));
      var start = Math.max(249, n - 250), out = [], m;
      for (i = start; i < n; i++) {
        m = (ps[i + 1] - ps[i + 1 - 250]) / 250;
        if (!m) continue;
        out.push([dayOf(bars[i][0]), +bars[i][4] < m ? 1 : 0]);
      }
      return out;
    }
    function stockSample() {
      var rows = uniRows().filter(function (r) { return r.kind === "stock" && r.mkt === "US" && r.mcap && /^[A-Z]{1,5}$/.test(r.t); });
      rows.sort(function (a, b) { return b.mcap - a.mcap; });
      var out = rows.slice(0, 24).map(function (r) { return r.t; });
      if (out.length >= 12) return out;
      return ["AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "AVGO", "TSLA", "JPM", "LLY", "XOM", "UNH", "V", "MA", "WMT", "JNJ", "PG", "HD", "COST", "BAC", "ORCL", "CVX", "KO", "PEP"];
    }
    function breadthGroups() {
      return {
        eq: all.filter(isEquityEtf).map(function (r) { return r.t; }),
        oth: all.filter(function (r) { return !isEquityEtf(r); }).map(function (r) { return r.t; }),
        stk: stockSample()
      };
    }
    function cohortLine(symbols, win) {
      var acc = {}, order = [], loaded = 0, i, k, ser, slice, a;
      for (i = 0; i < symbols.length; i++) {
        ser = techSeries[symbols[i]];
        if (!ser || !ser.length) continue;
        loaded++;
        slice = ser.slice(-win);
        for (k = 0; k < slice.length; k++) {
          a = acc[slice[k][0]];
          if (!a) { a = [0, 0]; acc[slice[k][0]] = a; order.push(slice[k][0]); }
          a[0] += slice[k][1]; a[1] += 1;
        }
      }
      order.sort();
      var need = Math.max(4, Math.ceil(loaded * 0.6)), pts = [];
      for (i = 0; i < order.length; i++) {
        a = acc[order[i]];
        if (a[1] < need) continue;
        pts.push([order[i], a[0] / a[1] * 100]);
      }
      if (pts.length > win) pts = pts.slice(-win);
      return { pts: pts, loaded: loaded, n: symbols.length };
    }
    function breadthSvg(win) {
      var g = breadthGroups();
      var lines = [
        ["Equity ETFs", cohortLine(g.eq, win), "#5b8cff"],
        ["Other ETF classes", cohortLine(g.oth, win), "#ffb020"],
        ["US stock sample", cohortLine(g.stk, win), "#e6e8ee"]
      ];
      var dates = {}, i, k;
      for (i = 0; i < lines.length; i++) for (k = 0; k < lines[i][1].pts.length; k++) dates[lines[i][1].pts[k][0]] = 1;
      var axis = Object.keys(dates).sort();
      if (axis.length > win) axis = axis.slice(-win);
      var W = 800, H = 210, L = 36, R = 12, T = 16, B = 22;
      var svg = "<svg viewBox='0 0 " + W + " " + H + "' preserveAspectRatio='xMidYMid meet'>";
      var y0 = T, y1 = H - B, x0 = L, x1 = W - R;
      [0, 25, 50, 75, 100].forEach(function (gline) {
        var y = y1 - (gline / 100) * (y1 - y0);
        svg += "<line x1='" + x0 + "' y1='" + y.toFixed(1) + "' x2='" + x1 + "' y2='" + y.toFixed(1) + "' stroke='#2a2e39'/>";
        svg += "<text x='2' y='" + (y + 3).toFixed(1) + "' fill='#787b86' font-size='10'>" + gline + "</text>";
      });
      function xOf(d) {
        var ix = axis.indexOf(d);
        if (axis.length < 2) return x0;
        return x0 + (ix / (axis.length - 1)) * (x1 - x0);
      }
      function yOf(pv) { return y1 - (Math.max(0, Math.min(100, pv)) / 100) * (y1 - y0); }
      var legend = [];
      lines.forEach(function (ln) {
        var pts = ln[1].pts.filter(function (pt) { return axis.indexOf(pt[0]) >= 0; });
        if (pts.length < 2) { legend.push(ln[0] + " " + ln[1].loaded + "/" + ln[1].n); return; }
        var d = "";
        pts.forEach(function (pt, idx) { d += (idx ? "L" : "M") + xOf(pt[0]).toFixed(1) + " " + yOf(pt[1]).toFixed(1) + " "; });
        svg += "<path d='" + d + "' fill='none' stroke='" + ln[2] + "' stroke-width='2'/>";
        legend.push(ln[0] + " " + pts[pts.length - 1][1].toFixed(0) + "% · " + ln[1].loaded + "/" + ln[1].n);
      });
      if (axis.length) {
        svg += "<text x='" + x0 + "' y='" + (H - 6) + "' fill='#787b86' font-size='10'>" + esc(axis[0]) + "</text>";
        svg += "<text x='" + (x1 - 78) + "' y='" + (H - 6) + "' fill='#787b86' font-size='10'>" + esc(axis[axis.length - 1]) + "</text>";
      }
      svg += "</svg>";
      var stockN = uniRows().filter(function (r) { return r.kind === "stock"; }).length;
      return "<div class=breadth>" + svg + "</div><div class=note>Percent under the 250-day, one line each, last " + win + " sessions. Blue is equity ETFs. Amber is every other class in the evidence book. White is a liquid US stock sample (" + esc(g.stk.slice(0, 6).join(" ")) + "…), not the " + (stockN || "full") + "-name file. That file has 20, 50 and 200 only. " + esc(legend.join(" · ")) + "</div>";
    }
    var COUNTRY_DEF = [
      { name: "Japan", mkt: "International", tickers: { EWJ: 1, DXJ: 1, BBJP: 1, HEWJ: 1, FLJP: 1, JPXN: 1 }, re: /japan/ },
      { name: "Korea", mkt: "Emerging", tickers: { EWY: 1, FLKR: 1 }, re: /korea/ },
      { name: "Taiwan", mkt: "Emerging", tickers: { EWT: 1, FLTW: 1 }, re: /taiwan/ },
      { name: "India", mkt: "Emerging", tickers: { INDA: 1, PIN: 1, INDY: 1, SMIN: 1, EPI: 1 }, re: /\bindia\b/ },
      { name: "China", mkt: "Emerging", tickers: { FXI: 1, MCHI: 1, KWEB: 1, ASHR: 1, GXC: 1, CQQQ: 1, CXSE: 1 }, re: /\bchina\b|chinese/ },
      { name: "Brazil", mkt: "Emerging", tickers: { EWZ: 1, FLBR: 1, EWZS: 1 }, re: /brazil/ },
      { name: "Chile", mkt: "Emerging", tickers: { ECH: 1 }, re: /\bchile\b/ },
      { name: "Peru", mkt: "Emerging", tickers: { EPU: 1 }, re: /\bperu\b/ },
      { name: "Finland", mkt: "International", tickers: { EFNL: 1 }, re: /finland/ }
    ];
    function countryHit(r, d) {
      if (d.tickers[r.t]) return true;
      return d.re.test((r.t + " " + (r.sub || "") + " " + (r.category || "") + " " + (r.bench || "") + " " + (r.name || "")).toLowerCase());
    }
    function countryTiles() {
      var out = [];
      var showCountries = state.scope !== "frontier" && state.scope !== "eqStock";
      if (showCountries) COUNTRY_DEF.forEach(function (d) {
        if (state.scope === "intl" && d.mkt !== "International") return;
        if (state.scope === "em" && d.mkt !== "Emerging") return;
        var rows = all.filter(function (r) { return countryHit(r, d); });
        var net = sumKnown(rows, "f21"), d1 = sumKnown(rows, "f1"), prints = 0, i;
        for (i = 0; i < rows.length; i++) if (rows[i].f21 != null) prints++;
        out.push({
          t: d.name, net: net, sz: net == null ? 8e7 : (Math.abs(net) || 8e7),
          sub: (net == null ? "21D —" : prints + " with 21D") + (d1 == null ? "" : " · 1D " + money(d1)),
          sym: ""
        });
      });
      if (state.scope === "all" || state.scope === "frontier" || state.scope === "eqEtf") {
        var seen = {}, fr = [];
        all.forEach(function (r) { if (r.mkt === "Frontier") { seen[r.t] = 1; fr.push(r); } });
        uniRows().forEach(function (r) {
          if (r.kind === "etf" && r.mkt === "Frontier" && !seen[r.t]) { seen[r.t] = 1; fr.push({ t: r.t, f21: null, aum: r.mcap }); }
        });
        fr.sort(function (a, b) { return (b.aum || 0) - (a.aum || 0); });
        fr.slice(0, 12).forEach(function (r) {
          out.push({ t: r.t, net: r.f21 == null ? null : r.f21, sz: r.f21 == null ? 5e7 : (Math.abs(r.f21) || 5e7), sub: r.f21 == null ? "21D —" : "21D", sym: r.t });
        });
      }
      return out;
    }
    function fillExtra() {
      ensureUni();
      var html = "";
      var mkts = ["International", "Emerging", "Frontier"];
      html += "<div class=sech><span>FOREIGN ETF FLOWS · evidence-book creations</span><span>1D on the pills · 21D on the map</span></div><div class=strip>";
      mkts.forEach(function (m) {
        var rs = all.filter(function (r) { return r.mkt === m && passScope(r); });
        var n1 = sumKnown(rs, "f1"), n5 = sumKnown(rs, "f5"), n21 = sumKnown(rs, "f21");
        var c = n1 == null || n1 === 0 ? ["#1e222d", "#d1d4dc"] : paint(n1, 2e8);
        html += "<div class=pill data-scope='" + (m === "International" ? "intl" : m === "Emerging" ? "em" : "frontier") + "' style='background:" + c[0] + ";color:" + c[1] + "'><b>" + m.toUpperCase() + " · " + rs.length + " ETFs</b><span>" + money(n1) + "</span><span>5D " + money(n5) + " · 21D " + money(n21) + "</span></div>";
      });
      html += "</div><div class=note>Country tiles are sized and colored by 21-day creation dollars. Gray and a dash mean that window is incomplete. The 1D figure is labeled and is not the color. Zero stays zero. Frontier names are their own tiles, and a thin book stays thin.</div>";
      html += "<div class='map short' id=country-map style='height:230px;min-height:230px'></div>";
      if (extra.issMap) {
        var dis = disagreeRows();
        html += "<div class=note>Sign disagree: creation dollars and issuer share-change point opposite ways, and both are at least $5M. " + dis.length + " funds. Issuer session " + esc(extra.issEnd || "—") + ", creations " + esc(flowAsOf) + ". Not a third flow number.</div>";
        if (dis.length) {
          html += "<div class=boards><div class=board><h3>SIGN DISAGREE</h3>";
          dis.slice(0, 14).forEach(function (r) {
            html += "<div class=row><span><b data-sym='" + esc(r.t) + "'>" + esc(r.t) + "</b> " + esc(r.asset || "") + "</span><span>creations " + money(r.cre) + " · issuer " + money(r.iss) + "</span></div>";
          });
          html += "</div></div>";
        }
      }
      if (state.scope === "all") {
        var stocks = uniRows().filter(function (r) { return r.kind === "stock" && r.mkt === "US"; });
        html += "<div class=sech><span>US EQUITY STOCKS · month return, not creations</span><span>" + (stocks.length ? stocks.length + " names" : "loading…") + "</span></div>";
        html += "<div class='map short' id=stock-map></div>";
      }
      if (state.scope === "intl" || state.scope === "em" || state.scope === "frontier") html += foreignRosterHtml(scopeWant());
      body.insertAdjacentHTML("beforeend", html);
      var cm = body.querySelector("#country-map");
      if (cm) {
        var ct = countryTiles();
        if (ct.length) drawTiles(cm, ct, "net", function (r) { return r.t; }, function (r) { return r.sub; }, { color: countryColor, area: "sz", fmt: function (v) { return v == null ? "—" : money(v); }, symOf: function (r) { return r.sym || ""; } });
      }
      var sm = body.querySelector("#stock-map");
      if (sm) {
        var sec = sectorTiles(uniRows().filter(function (r) { return r.kind === "stock" && r.mkt === "US"; }));
        if (sec.length) drawTiles(sm, sec, "net", function (r) { return r.t; }, function (r) { return r.sub; }, { color: pricePaint, fmt: function (v) { return pct(v); }, symOf: function () { return ""; } });
      }
      body.querySelectorAll(".pill[data-scope]").forEach(function (p) {
        p.onclick = function () { state.scope = p.getAttribute("data-scope"); markScope(); draw(); };
      });
    }
    function showSpreads() {
      ensureUni();
      function quoteRows(list) {
        return list.filter(function (r) { return r.spread != null && passScope(r); }).map(function (r) {
          return { t: r.t, v: r.spread * 10000, sz: Math.abs(r.aum || 0) || 1 };
        });
      }
      var eq = quoteRows(all.filter(isEquityEtf));
      var oth = quoteRows(all.filter(function (r) { return !isEquityEtf(r); }));
      var stocks = uniRows().filter(function (r) { return r.kind === "stock" && r.atrPct != null && r.price >= 5 && passScope(r); })
        .sort(function (a, b) { return (b.mcap || 0) - (a.mcap || 0); }).slice(0, 70)
        .map(function (r) { return { t: r.t, v: r.atrPct, sz: Math.abs(r.mcap || 0) || 1 }; });
      body.innerHTML = "<div class=note>Size is assets. Color is tightness: cyan is tight, red is wide. This is not the green and red used for creations. Equity ETFs and the other classes are the bid/ask in basis points. Stocks are ATR as a percent of price, because that file has no quote. Stock tiles are the largest names in this scope, capped at 70.</div>"
        + "<div class=sech><span>EQUITY ETFs · QUOTE BP</span><span>" + eq.length + "</span></div><div class='map short' id=sp-eq></div>"
        + "<div class=sech><span>OTHER CLASSES · QUOTE BP</span><span>" + oth.length + "</span></div><div class='map short' id=sp-ot></div>"
        + "<div class=sech><span>EQUITY STOCKS · ATR % OF PRICE</span><span>" + stocks.length + "</span></div><div class='map short' id=sp-st></div>"
        + (extra.uni ? "" : "<div class=note>Loading the slim book for stock ranges…</div>");
      function go(id, list, unit) {
        var box = body.querySelector(id);
        if (!box) return;
        if (!list.length) { box.innerHTML = "<div class=note>No names in this scope.</div>"; return; }
        drawTiles(box, list, "v", function (r) { return r.t; }, null, { color: tightPaint, area: "sz", fmt: function (v) { return v == null ? "—" : v.toFixed(2) + unit; } });
      }
      go("#sp-eq", eq, " bp");
      go("#sp-ot", oth, " bp");
      go("#sp-st", stocks, "%");
      meta("Spreads · " + eq.length + " equity ETF quotes · " + oth.length + " other quotes · " + stocks.length + " stock ranges · color is tightness, not flow");
    }
    function pushPick(list, n, picks, metaMap) {
      var rows = list.filter(function (r) { return r.sma200 != null && r.sma200 < 0; }).sort(function (a, b) { return a.sma200 - b.sma200; });
      var i, c = 0;
      for (i = 0; i < rows.length && c < n; i++) {
        if (metaMap[rows[i].t]) continue;
        picks.push(rows[i].t);
        metaMap[rows[i].t] = rows[i];
        c++;
      }
    }
    function showBelow() {
      ensureUni();
      var winKey = state.belowWin === "21" ? "b21" : state.belowWin === "250" ? "b250" : "b63";
      var uniReady = extra.uni ? extra.uni.length : 0;
      var book = all.filter(passScope);
      var uni = uniRows().filter(passScope);
      var picks = [], metaMap = {};
      book.forEach(function (r) { picks.push(r.t); metaMap[r.t] = r; });
      if (state.scope === "all") {
        pushPick(uni.filter(function (r) { return r.kind === "etf" && r.asset === "Equity"; }), 16, picks, metaMap);
        pushPick(uni.filter(function (r) { return r.kind === "stock"; }), 24, picks, metaMap);
        pushPick(uni.filter(function (r) { return r.kind === "etf" && r.asset !== "Equity"; }), 16, picks, metaMap);
      } else {
        pushPick(uni, 48, picks, metaMap);
      }
      var groups = breadthGroups(), need = {};
      picks.concat(groups.eq, groups.oth, groups.stk).forEach(function (sym) { need[sym] = 1; });
      var qAll = Object.keys(need);
      var gen = host._belowGen || 0;
      var sig = state.scope + ":" + gen + ":" + uniReady + ":" + qAll.length;
      if (host._belowSig !== sig) {
        host._belowSig = sig;
        var i = 0, q = qAll.filter(function (sym) { return !tech[sym]; });
        (function step() {
          if (host._belowSig !== sig) return;
          if (i >= q.length) return;
          var batch = q.slice(i, i + 3); i += 3;
          Promise.all(batch.map(loadTech)).then(function () {
            if (host._belowSig !== sig) return;
            if (state.tab === "tech" && state.techMode === "below") draw();
            step();
          });
        })();
      }
      var done = picks.filter(function (s) { return tech[s]; }).length;
      var under = [];
      picks.forEach(function (s) {
        var st = tech[s];
        if (!st || st.ma250 == null || !(st.ma250 < 0)) return;
        var base = metaMap[s] || { t: s, kind: "etf" };
        under.push(Object.assign({}, base, st));
      });
      under.sort(function (a, b) { return (b[winKey] || 0) - (a[winKey] || 0); });
      meta("Below the 250-day · " + under.length + " under it now · " + done + "/" + picks.length + " bars · window " + state.belowWin + "d");
      var winN = state.belowWin === "21" ? 21 : state.belowWin === "250" ? 250 : 63;
      var html = breadthSvg(winN) + "<div class=note>The lines are the share under the 250-day over time. The table is who is under it on the last bar. Days under is the current streak. The stock file’s own average, in the boards, is the 200-day. The full stock history is not on the line.</div>";
      html += "<table><thead><tr><th>Ticker</th><th>Kind</th><th>Class</th><th>Market</th><th>250d</th><th>Days under</th><th>21d under</th><th>63d under</th><th>250d under</th></tr></thead><tbody>";
      under.forEach(function (r) {
        var c = paint(r.ma250, 8);
        html += "<tr><td>" + esc(r.t) + "</td><td>" + esc(r.kind || "etf") + "</td><td>" + esc(r.asset || "") + "</td><td>" + esc(r.mkt || "") + "</td><td style='background:" + c[0] + ";color:" + c[1] + "'>" + pct(r.ma250) + "</td><td>" + (r.belowDays == null ? "—" : r.belowDays) + "</td><td>" + (r.b21 == null ? "—" : r.b21.toFixed(0) + "%") + "</td><td>" + (r.b63 == null ? "—" : r.b63.toFixed(0) + "%") + "</td><td>" + (r.b250 == null ? "—" : r.b250.toFixed(0) + "%") + "</td></tr>";
      });
      html += "</tbody></table>";
      var fileUnder = uni.filter(function (r) { return r.sma200 != null && r.sma200 < 0; }).sort(function (a, b) { return a.sma200 - b.sma200; });
      function uBoard(title, list) {
        html += "<div class=board><h3>" + esc(title) + " · 200d file</h3>";
        list.slice(0, 15).forEach(function (r, i) {
          var st = tech[r.t] || {};
          var extraD = st.ma250 != null ? " · 250d " + pct(st.ma250) : "";
          html += "<div class=row><span>" + (i + 1) + "  <b>" + esc(r.t) + "</b> " + esc(r.mkt) + "</span><span>" + pct(r.sma200) + extraD + "</span></div>";
        });
        html += "</div>";
      }
      html += "<div class=boards>";
      uBoard("EQUITY ETFs UNDER 200d", fileUnder.filter(function (r) { return r.kind === "etf" && r.asset === "Equity"; }));
      uBoard("EQUITY STOCKS UNDER 200d", fileUnder.filter(function (r) { return r.kind === "stock"; }));
      uBoard("OTHER CLASSES UNDER 200d", fileUnder.filter(function (r) { return r.kind === "etf" && r.asset !== "Equity"; }));
      html += "</div>";
      if (!extra.uni) html += "<div class=note>Loading the universe so stocks and the wider ETF book can join this board…</div>";
      body.innerHTML = html;
    }
    function spreadBoards(book) {
      var rows = book.filter(function (r) { return r.spread != null; }).slice().sort(function (a, b) { return a.spread - b.spread; });
      var h = "<div class=board><h3>TIGHTEST QUOTE SPREADS · ETFs</h3>";
      rows.slice(0, 20).forEach(function (r, i) {
        h += "<div class=row><span>" + (i + 1) + "  <b>" + esc(r.t) + "</b> " + esc(r.asset) + " " + esc(r.mkt) + "</span><span>" + (r.spread * 10000).toFixed(2) + " bp</span></div>";
      });
      var uni = uniRows().filter(function (r) { return r.kind === "stock" && r.atrPct != null && r.price >= 5 && passScope(r); }).sort(function (a, b) { return a.atrPct - b.atrPct; });
      h += "</div><div class=board><h3>TIGHTEST RANGE · EQUITY STOCKS</h3>";
      uni.slice(0, 15).forEach(function (r, i) {
        h += "<div class=row><span>" + (i + 1) + "  <b>" + esc(r.t) + "</b> " + esc(r.country) + "</span><span>" + r.atrPct.toFixed(2) + "% ATR</span></div>";
      });
      return h + "</div>";
    }
    function foreignFlowBoard() {
      var h = "<div class=board><h3>FOREIGN ETF FLOWS</h3>";
      ["International", "Emerging", "Frontier"].forEach(function (m) {
        var rs = all.filter(function (r) { return r.mkt === m && passScope(r); });
        var n1 = sumKnown(rs, "f1"), n21 = sumKnown(rs, "f21");
        h += "<div class=row><span><b>" + m + "</b> · " + rs.length + "</span><span>" + money(n1) + " 1D · " + money(n21) + " 21D</span></div>";
        rs.filter(function (r) { return r.f1 != null; }).slice().sort(function (a, b) { return Math.abs(b.f1) - Math.abs(a.f1); }).slice(0, 8).forEach(function (r) {
          h += "<div class=row><span><b data-sym='" + esc(r.t) + "'>" + esc(r.t) + "</b> " + esc(r.sub || r.category || "") + "</span><span>" + money(r.f1) + "</span></div>";
        });
      });
      return h + "</div>";
    }
    function markScope() {
      host.querySelectorAll("#etf-scope button").forEach(function (b) {
        b.className = b.getAttribute("data-scope") === state.scope ? "on" : "";
      });
    }
    function flowWinField() {
      if (state.win === "5") return ["net_flow_5d_usd", "5D"];
      if (state.win === "21") return ["net_flow_20d_usd", "20D"];
      return ["net_flow_1d_usd", "1D"];
    }
    function catScope(cat, ticker) {
      var s = state.scope, c = String(cat || "").toUpperCase();
      if (!s || s === "all") return true;
      if (s === "eqStock") return false;
      if (s === "eqEtf") return /EQUITY|SECTOR|THEMATIC|FACTOR|GROWTH|DIVIDEND|INTERNATIONAL|COUNTRY|LEVERED/.test(c) || IN_T[ticker] || EM_T[ticker] || FR_T[ticker];
      if (s === "intl") return c === "INTERNATIONAL" || IN_T[ticker];
      if (s === "em") return EM_T[ticker];
      if (s === "frontier") return FR_T[ticker];
      return true;
    }
    function issuerLists() {
      var j = extra.true;
      if (!j || j.error || !j.inflows) return null;
      var field = flowWinField();
      function rows(list) {
        return (list || []).map(function (r) {
          return { t: r.ticker, net: num(r[field[0]]), cat: String(r.category || "").replace(/_/g, " "), sym: r.ticker };
        }).filter(function (r) {
          if (!r.t || r.net == null || !r.net) return false;
          if (state.q && r.t.indexOf(state.q) < 0) return false;
          return catScope(r.cat, r.t);
        });
      }
      return { label: field[1], inn: rows(j.inflows), out: rows(j.outflows), end: (j.aggregation_period || {}).end_date || "" };
    }
    function industryPairs() {
      var secIn = [], secOut = [], catIn = [], catOut = [];
      var fv = extra.finviz && !extra.finviz.error ? extra.finviz : null;
      (fv && fv.sector_etfs || []).forEach(function (x) {
        var v = num(x.flows_1m);
        if (v == null || !v) return;
        if (state.q && String(x.ticker).indexOf(state.q) < 0 && String(x.sector || "").toUpperCase().indexOf(state.q) < 0) return;
        var row = { t: x.sector || x.ticker, sym: x.ticker, net: v, sub: x.ticker };
        if (v > 0) secIn.push(row); else secOut.push(row);
      });
      ((extra.true && extra.true.category_rotation) || []).forEach(function (c) {
        if (c.status === "unavailable") return;
        if (state.scope === "eqStock") return;
        if (state.scope === "intl" && c.category !== "INTERNATIONAL" && c.category !== "COUNTRY") return;
        if (state.scope === "em" && c.category !== "COUNTRY" && c.category !== "INTERNATIONAL") return;
        var v = num(c.net_flow_5d_usd);
        if (v == null || !v) return;
        var row = { t: String(c.category || "").replace(/_/g, " "), net: v, sub: (c.n_etfs || 0) + " funds · 5D", sym: "" };
        if (v > 0) catIn.push(row); else catOut.push(row);
      });
      return { secIn: secIn, secOut: secOut, catIn: catIn, catOut: catOut };
    }
    function goMap(id, rows, opt) {
      opt = opt || {};
      var box = body.querySelector(id);
      if (!box) return;
      if (!rows || !rows.length) { box.innerHTML = "<div class=note>" + esc(opt.empty || "No prints in this slice.") + "</div>"; return; }
      drawTiles(box, rows, "net", function (r) { return r.t; }, opt.sub || null, {
        color: opt.color || paint,
        fmt: opt.fmt || money,
        symOf: function (r) { return r.sym || ""; },
        area: opt.area || null
      });
    }
    function paintIssuerAndIndustry(where) {
      var iss = issuerLists();
      var ind = industryPairs();
      var old = body.querySelector("#flow-lead");
      if (old) old.remove();
      var lab = iss ? (iss.label + " · ends " + (iss.end || "—") + " · " + iss.inn.length + " in · " + iss.out.length + " out") : "loading the issuer tape";
      var html = "<div id=flow-lead>"
        + "<div class=sech><span>ETF INFLOWS</span><span>" + esc(lab) + "</span></div><div class='map flow' id=map-in></div>"
        + "<div class=sech><span>ETF OUTFLOWS</span><span>issuer share change · not a creation print</span></div><div class='map flow' id=map-out></div>"
        + "<div class=sech><span>INDUSTRIES · BIGGEST INFLOWS</span><span>sector ETFs · 1M</span></div><div class='map flow' id=map-sec-in style='height:200px;min-height:200px'></div>"
        + "<div class=sech><span>INDUSTRIES · BIGGEST OUTFLOWS</span><span>sector ETFs · 1M</span></div><div class='map flow' id=map-sec-out style='height:200px;min-height:200px'></div>"
        + "<div class=sech><span>CATEGORIES · INFLOWS AND OUTFLOWS</span><span>5D issuer share change</span></div><div class='map flow' id=map-cat style='height:220px;min-height:220px'></div>"
        + "</div>";
      body.insertAdjacentHTML(where || "afterbegin", html);
      if (!iss) {
        body.querySelector("#map-in").innerHTML = "<div class=note>Loading ETF inflows and outflows…</div>";
        body.querySelector("#map-out").innerHTML = "<div class=note>Same tape. It has not arrived yet.</div>";
      } else {
        goMap("#map-in", iss.inn, { sub: function (r) { return r.cat; }, empty: "No inflows in this scope." });
        goMap("#map-out", iss.out, { sub: function (r) { return r.cat; }, empty: "No outflows in this scope." });
      }
      goMap("#map-sec-in", ind.secIn, { sub: function (r) { return r.sub; }, empty: extra.finviz ? "No sector inflows." : "Loading sector ETFs…" });
      goMap("#map-sec-out", ind.secOut, { sub: function (r) { return r.sub; }, empty: extra.finviz ? "No sector outflows." : "Loading sector ETFs…" });
      goMap("#map-cat", ind.catIn.concat(ind.catOut), { sub: function (r) { return r.sub; }, empty: extra.true ? "No category print in this scope." : "Loading categories…", symOf: function () { return ""; } });
    }
    function ensureMoves() {
      if (extra.moves || host._mvStarted) return;
      host._mvStarted = true;
      if (window.JHEtfMoves && window.JHEtfMoves.added) { extra.moves = window.JHEtfMoves; return; }
      var el = document.createElement("script");
      el.src = "/etf-holdings-moves.js";
      el.onload = function () {
        extra.moves = window.JHEtfMoves || { error: "empty" };
        if (host.isConnected) draw();
      };
      el.onerror = function () { extra.moves = { error: "moves" }; if (host.isConnected) draw(); };
      (document.head || document.documentElement).appendChild(el);
    }
    function passName(r) {
      var s = state.scope;
      if (!s || s === "all" || s === "eqEtf" || s === "eqStock") return true;
      var u = uniIndex()[r.t];
      if (!u) return false;
      return passScope(u);
    }
    function showNames() {
      ensureMoves();
      ensureUni();
      var mv = extra.moves;
      if (!mv) { meta("Holdings · stocks ETFs added and left"); body.innerHTML = "<div class=note>Loading the stock-level book. Entries and exits, not a blended flow.</div>"; return; }
      if (mv.error) { body.innerHTML = "<div class=note>The holdings move file did not load. Nothing invented.</div>"; return; }
      function take(list) {
        return (list || []).filter(function (r) {
          if (!passName(r)) return false;
          if (state.q && r.t.indexOf(state.q) < 0 && String(r.name || "").toUpperCase().indexOf(state.q) < 0) return false;
          return true;
        });
      }
      var added = take(mv.added).slice(0, 100);
      var left = take(mv.left).slice(0, 100);
      var held = take(mv.held).slice(0, 100);
      var emerg = take(mv.emergers).slice(0, 100);
      function tilesOf(list, sign) {
        return list.slice(0, 36).map(function (r) { return { t: r.t, net: sign * (r.n || 0), sym: r.t, sub: r.n + " ETFs" }; });
      }
      function board(title, rows) {
        var h = "<div class=board><h3>" + esc(title) + " · " + rows.length + "</h3>";
        rows.forEach(function (r, i) {
          h += "<div class=row><span>" + (i + 1) + "  <b data-sym='" + esc(r.t) + "'>" + esc(r.t) + "</b> " + esc(r.name || "") + "</span><span>" + r.n + "</span></div>";
          if (i < 12 && r.etfs) h += "<div class=row><span style='color:#9aa0ac'>" + esc(r.etfs) + "</span><span></span></div>";
        });
        return h + "</div>";
      }
      meta("Holdings · top " + added.length + " added · " + left.length + " left · " + emerg.length + " first-time · book " + (mv.asof || "—"));
      var note = "<div class=note>Top 100 stocks by how many ETFs in this " + (mv.n_books || mv.n_funds || "") + "-fund book newly report them, and how many dropped them, as of " + esc(mv.asof || "—") + ". "
        + "Added means the name was not in that fund’s prior snapshot. Left means it was, and now it is not. "
        + "A trim that stayed inside the fund is not in this file, so this is entries and exits. "
        + "New emergers had zero holders in the prior book and at least one fund reports them now. That is a first print here, not a corporate merger. "
        + (mv.n_added_names || 0) + " names were added somewhere, " + (mv.n_left_names || 0) + " left somewhere, " + (mv.n_emergers || 0) + " were new to the whole book. "
        + "Look-through is the fund list under each name, capped at 16. A name that was added and left by the same funds was dropped as an identity remap, not a new position. The printed name is the snapshot’s name, so a ticker used in two countries can look wrong. Click a ticker for the chart.</div>";
      body.innerHTML = note
        + "<div class=sech><span>STOCKS ADDED BY ETFs</span><span>top " + added.length + "</span></div><div class='map flow' id=mv-add></div>"
        + "<div class=sech><span>STOCKS LEFT BY ETFs</span><span>top " + left.length + "</span></div><div class='map flow' id=mv-left></div>"
        + "<div class=sech><span>NEW EMERGERS · FIRST TIME IN THE BOOK</span><span>" + emerg.length + "</span></div><div class='map flow' id=mv-em style='height:220px;min-height:220px'></div>"
        + "<div class=boards>"
        + board("TOP 100 ADDED", added)
        + board("TOP 100 LEFT", left)
        + board("NEW EMERGERS", emerg)
        + board("MOST HELD · LOOK-THROUGH", held)
        + spreadBoards(all.filter(passScope))
        + "</div>";
      goMap("#mv-add", tilesOf(added, 1), { sub: function (r) { return r.sub; }, fmt: function (v) { return (v == null ? "—" : Math.abs(v) + " ETFs"); }, empty: "No adds in this scope." });
      goMap("#mv-left", tilesOf(left, -1), { sub: function (r) { return r.sub; }, fmt: function (v) { return (v == null ? "—" : Math.abs(v) + " ETFs"); }, empty: "No exits in this scope." });
      goMap("#mv-em", tilesOf(emerg, 1), { sub: function (r) { return r.sub; }, fmt: function (v) { return (v == null ? "—" : Math.abs(v) + " ETFs"); }, empty: "No first-time names in this scope." });
      paintIssuerAndIndustry("beforeend");
    }

    function showFlows() {
      var k = flowKeyName(), pk = pctKey();
      if (state.flowSort.k === "p1" || state.flowSort.k === "p5" || state.flowSort.k === "p21") state.flowSort.k = pk;
      else if (state.flowSort.k === "f1" || state.flowSort.k === "f5" || state.flowSort.k === "f21") state.flowSort.k = k;
      var groups = [["asset", "Asset"], ["mkt", "Market"], ["category", "Category"], ["region", "Region"], ["issuer", "Issuer"], ["lev", "Leverage"]];
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
      if (state.scope === "eqStock") return showFileMA();
      if (state.src === "classes") return showClasses();
      if (state.src === "sleeves") return showSleeves();
      if (state.src === "leaders") return showLeaders();
      var scoped = all.filter(passScope).concat((extra.issuer || []).filter(function (r) { return passScope(r) && !all.some(function (a) { return a.t === r.t; }); }));
      var base = searched(scoped, state.q).filter(function (r) { return r[k] != null; });
      var bucks = buckets(base, state.group, k);
      var allowed = { t: 1, issuer: 1, region: 1, aum: 1, exp: 1, asset: 1, category: 1, lev: 1, mkt: 1 };
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
        body.insertAdjacentHTML("afterbegin", "<div class=sech><span>EVIDENCE BOOK · CREATION DOLLARS " + state.win + "D</span><span>" + list.length + " funds · this is the creation tape, under the issuer inflows</span></div>");
        paintIssuerAndIndustry("afterbegin");
        fillExtra();
        return;
      }
      var heat = list.filter(function (r) { return r[k]; });
      body.innerHTML = "<div class=sech><span>EVIDENCE BOOK · CREATION DOLLARS " + state.win + "D</span><span>" + list.length + " funds with a print</span></div><div class=stack>" + stripHtml(bucks) + "<div class=map id=etf-map></div></div>";
      bindStrip();
      var box = body.querySelector("#etf-map");
      if (!heat.length) box.innerHTML = "<div class=note>No " + state.win + "D creation prints in this slice.</div>";
      else drawTiles(box, heat, k, function (r) { return r.t; }, function (r) { return pct(r[pk]) + " · " + (r[state.group] || ""); });
      paintIssuerAndIndustry("afterbegin");
      fillExtra();
    }
    function showClasses() {
      var fv = extra.finviz;
      if (!fv || fv.error || !fv.type_flows) { meta("Asset-class rollup"); body.innerHTML = "<div class=note>" + (fv && fv.error ? "Class file failed: " + esc(fv.error) : "Loading 5,775-fund asset classes…") + "</div>"; return; }
      var rows = fv.type_flows.map(function (t) {
        return { t: t.type, n: t.n, aum: num(t.aum), net: num(t.net_1m), ytd: num(t.net_ytd), inn: num(t.gross_in_1m), out: num(t.gross_out_1m), asset: t.type };
      });
      var list = searched(rows, state.q).filter(function (r) { return passCls(r, "asset") && classMatches(r.t); });
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
        if (!sleeveMatches(c.complex)) return;
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
      var html = btn("data-hold", "names", "Added / left", state.hold === "names")
        + btn("data-hold", "book", "Book " + all.length, state.hold === "book")
        + btn("data-hold", "census", "Census " + (cen.length || 303), state.hold === "census")
        + (state.hold === "names" ? "" : clsSelect(state.hold === "book" ? all : cen, "asset"));
      setWin(html);
      if (state.hold === "names" || state.hold == null) return showNames();
      if (state.scope === "eqStock") return showFileMA();
      if (state.hold === "census") {
        if (!cen.length) { meta("Census holdings"); body.innerHTML = "<div class=note>" + (extra.matrix && extra.matrix.error ? esc(extra.matrix.error) : "Loading 303-name census…") + "</div>"; return; }
        var list = searched(cen, state.q).filter(function (r) { return passCls(r, "asset") && passScope(r); });
        if (["t", "asset", "category", "holdings_n", "aum_usd_m", "expense_pct", "prem_disc_pct", "dollar_vol_usd_m", "leveraged", "mkt"].indexOf(state.holdSort.k) < 0) state.holdSort.k = "holdings_n";
        sortRows(list, state.holdSort.k, state.holdSort.d);
        meta(list.length + " names · holdings count, fee, premium · census " + String((extra.matrix || {}).generated_at || "").slice(0, 10));
        table(body, [["t", "Ticker"], ["asset", "Asset class"], ["category", "Category"], ["holdings_n", "Holdings"], ["aum_usd_m", "AUM $m"], ["expense_pct", "Expense"], ["prem_disc_pct", "Prem/disc"], ["dollar_vol_usd_m", "$ vol m"], ["leveraged", "Levered"]], list, state.holdSort.k, state.holdSort.d, function (r) {
          return "<td>" + esc(r.t) + "</td><td>" + esc(r.asset) + "</td><td>" + esc(r.category) + "</td><td>" + (r.holdings_n == null ? "—" : Math.round(r.holdings_n)) + "</td><td>" + (r.aum_usd_m == null ? "—" : r.aum_usd_m.toFixed(1)) + "</td><td>" + (r.expense_pct == null ? "—" : r.expense_pct.toFixed(2) + "%") + "</td><td>" + pct(r.prem_disc_pct) + "</td><td>" + (r.dollar_vol_usd_m == null ? "—" : r.dollar_vol_usd_m.toFixed(1)) + "</td><td>" + flag(r.leveraged) + "</td>";
        });
        bindSort(body, state.holdSort, draw);
        return;
      }
      if (state.scope === "eqStock") return showFileMA();
      var list = searched(all, state.q).filter(function (r) { return passCls(r, "asset") && passScope(r); });
      if (["t", "asset", "category", "region", "issuer", "lev", "nHold", "both", "added", "deleted", "asof", "aum", "exp", "spread", "vol", "si", "disc", "mkt"].indexOf(state.holdSort.k) < 0) state.holdSort.k = "nHold";
      sortRows(list, state.holdSort.k, state.holdSort.d);
      meta(list.length + " funds · book vs prior · " + countsOf(all, "asset").length + " asset classes · " + countsOf(all, "issuer").length + " issuers");
      table(body, [["t", "Ticker"], ["asset", "Class"], ["category", "Category"], ["region", "Region"], ["issuer", "Issuer"], ["lev", "Leverage"], ["nHold", "Holdings"], ["both", "In both"], ["added", "Added"], ["deleted", "Deleted"], ["asof", "As of"], ["aum", "AUM"], ["exp", "Expense"], ["spread", "Spread bp"], ["vol", "Volume"], ["si", "Short sh"], ["disc", "Prem"]], list, state.holdSort.k, state.holdSort.d, function (r) {
        return "<td>" + esc(r.t) + "</td><td>" + esc(r.asset) + "</td><td>" + esc(r.category) + "</td><td>" + esc(r.region) + "</td><td>" + esc(r.issuer) + "</td><td>" + esc(r.lev) + "</td><td>" + (r.nHold == null ? "—" : Math.round(r.nHold)) + "</td><td>" + (r.both == null ? "—" : r.both) + "</td><td>" + (r.added == null ? "—" : r.added) + "</td><td>" + (r.deleted == null ? "—" : r.deleted) + "</td><td>" + esc(r.asof || "—") + "</td><td>" + plain(r.aum) + "</td><td>" + (r.exp == null ? "—" : r.exp.toFixed(2) + "%") + "</td><td>" + (r.spread == null ? "—" : (r.spread * 10000).toFixed(2)) + "</td><td>" + plain(r.vol) + "</td><td>" + plain(r.si) + "</td><td>" + pct(r.disc) + "</td>";
      });
      bindSort(body, state.holdSort, draw);
      body.insertAdjacentHTML("beforeend", "<div class=note>Fund book only. The Added and deleted columns are line counts, not the stock. The stocks themselves are on Added / left. Click a ticker to open that fund.</div>");
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
      if (state.scope === "eqStock") return showFileMA();
      if (!cen.length) { meta("Census"); body.innerHTML = "<div class=note>" + (extra.matrix && extra.matrix.error ? esc(extra.matrix.error) : "Loading census matrix…") + "</div>"; return; }
      var cols = METRICS[state.metric];
      var list = searched(cen, state.q).filter(function (r) { return passCls(r, "asset") && passScope(r); });
      if (cols.every(function (c) { return c[0] !== state.cenSort.k; }) && state.cenSort.k !== "t" && state.cenSort.k !== "asset" && state.cenSort.k !== "category" && state.cenSort.k !== "mkt") state.cenSort.k = cols[0][0];
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
          if (info && !passScope(info)) return;
          if (!info && state.scope !== "all") return;
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
        + btn("data-tech", "sectors", "Stock sectors", state.techMode === "sectors")
        + btn("data-tech", "spreads", "Spreads", state.techMode === "spreads")
        + btn("data-tech", "below", "Below 250", state.techMode === "below")
        + (state.techMode === "below" ? btn("data-bw", "21", "21d", state.belowWin === "21") + btn("data-bw", "63", "63d", state.belowWin === "63") + btn("data-bw", "250", "250d", state.belowWin === "250") : "");
      if (state.techMode === "weekly") html += clsSelect(cen, "asset");
      if (state.techMode === "daily") html += clsSelect(all, "asset");
      setWin(html);
      if (state.techMode === "sectors") return showSectors();
      if (state.techMode === "spreads") return showSpreads();
      if (state.techMode === "below") return showBelow();
      if (state.techMode === "daily") return showDaily();
      if (state.scope === "eqStock") return showFileMA();
      if (!cen.length) { body.innerHTML = "<div class=note>Loading weekly technicals…</div>"; return; }
      var keys = [["above_ma40w", "Above 40w"], ["golden_cross_10_40w", "10/40 cross"], ["mom_12_1_pct", "Mom 12−1"], ["mom_6m_pct", "Mom 6m"], ["rsi_14w", "RSI"], ["dist_52w_high_pct", "vs high"], ["beta_spy", "Beta"], ["corr_spy_52w", "Corr"], ["rs_13w_pct", "RS 13w"], ["vol_52w_pct", "Vol"], ["tech_score", "Tech"], ["risk_score", "Risk"]];
      var list = searched(cen, state.q).filter(function (r) { return passCls(r, "asset") && passScope(r); });
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
      meta("EQUITY STOCKS only · share above the 50- and 200-day · median month · not ETFs · " + String(fv.generated_at || "").slice(0, 10));
      var items = rows.map(function (s) {
        return { t: s.sector, n: s.n, net: num(s.median_perf_m), a200: num(s.pct_above_200dma), a50: num(s.pct_above_50dma) };
      });
      if (state.mode === "table" || true) {
        body.innerHTML = "<div class=map id=etf-map style='height:280px;min-height:280px'></div>";
        drawTiles(body.querySelector("#etf-map"), items, "net", function (r) { return r.t; }, function (r) { return r.a200.toFixed(0) + "% >200d · n=" + r.n; }, { color: pricePaint, fmt: function (v) { return pct(v); }, symOf: function () { return ""; } });
        var scale = { net: pScale(items.map(function (r) { return r.net; })) };
        var h = "<table><thead><tr><th>Sector</th><th>Names</th><th>% above 200d</th><th>% above 50d</th><th>Median month</th></tr></thead><tbody>";
        items.sort(function (a, b) { return b.a200 - a.a200; }).forEach(function (r) {
          var c = pricePaint(r.net, scale.net);
          h += "<tr><td>" + esc(r.t) + "</td><td>" + r.n + "</td><td>" + r.a200.toFixed(1) + "%</td><td>" + r.a50.toFixed(1) + "%</td><td style='background:" + c[0] + ";color:" + c[1] + "'>" + pct(r.net) + "</td></tr>";
        });
        body.insertAdjacentHTML("beforeend", h + "</tbody></table><div class=note>These are stocks. Equity ETFs stay on the Flows and Weekly tapes so the two books are not mixed.</div>");
      }
    }
    function showDaily() {
      if (state.scope === "eqStock") return showFileMA();
      var list = searched(all, state.q).filter(function (r) { return passCls(r, "asset") && passScope(r); }).map(function (r) {
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
      if (techPending[sym]) return techPending[sym];
      techPending[sym] = fetch("/data/warm/tv-bars/universe/US__" + encodeURIComponent(sym) + ".json.gz", { credentials: "omit" }).then(function (res) {
        if (!res.ok) throw new Error(String(res.status));
        return res.arrayBuffer();
      }).then(function (buf) {
        var u8 = new Uint8Array(buf);
        if (u8[0] === 0x7b) return JSON.parse(new TextDecoder().decode(u8));
        if (!window.DecompressionStream) throw new Error("gzip");
        return new Response(new Blob([buf]).stream().pipeThrough(new DecompressionStream("gzip"))).json();
      }).then(function (j) {
        var bars = (j && j.bars) || [], n = bars.length;
        var stamp = (j && j.last_date) || (n ? dayOf(bars[n - 1][0]) : "");
        if (stamp && (!barAsOf || stamp > barAsOf)) barAsOf = stamp;
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
        var st = n >= 250 ? maStats(bars) : { ma250: null, belowDays: null, b21: null, b63: null, b250: null };
        var flags = belowFlags(bars);
        if (flags) techSeries[sym] = flags;
        tech[sym] = { ma9: sma(9), ma20: sma(20), ma50: sma(50), ma100: sma(100), ma200: sma(200), ma250: st.ma250, belowDays: st.belowDays, b21: st.b21, b63: st.b63, b250: st.b250, ret: ret, rng: rng, eff: eff, evr: eff ? ret / eff : null };
      }).catch(function () { tech[sym] = {}; });
      return techPending[sym];
    }

    function showTops() {
      setWin("");
      ensureUni();
      ensureMoves();
      var book = all.filter(passScope);
      var cen = matrixRows(extra.matrix).filter(passScope);
      var fv = extra.finviz && !extra.finviz.error ? extra.finviz : null;
      function col(title, rows, key, fmt) {
        var list = rows.filter(function (r) { return r[key] != null; }).slice();
        sortRows(list, key, -1);
        var h = "<div class=board><h3>" + title + " · " + list.length + "</h3>";
        list.slice(0, 40).forEach(function (r, i) {
          if (state.q && r.t.indexOf(state.q) < 0) return;
          h += "<div class=row><span>" + (i + 1) + "  <b data-sym='" + esc(r.t) + "'>" + esc(r.t) + "</b> " + esc(r.asset || "") + "</span><span>" + fmt(r) + "</span></div>";
        });
        return h + "</div>";
      }
      var bucks = buckets(book, "asset", "f1");
      var scale = pScale(bucks.map(function (g) { return g.net; }));
      var classTable = "<table><thead><tr><th>Asset class</th><th>Funds</th><th>Holdings sum</th><th>Added</th><th>Deleted</th><th>1D net</th><th>21D net</th></tr></thead><tbody>";
      buckets(all, "asset", "f21").sort(function (a, b) { return Math.abs(b.net) - Math.abs(a.net); });
      countsOf(book, "asset").forEach(function (pair) {
        var name = pair[0];
        var rs = book.filter(function (r) { return r.asset === name; });
        var hold = 0, add = 0, del = 0, i;
        for (i = 0; i < rs.length; i++) { hold += rs[i].nHold || 0; add += rs[i].added || 0; del += rs[i].deleted || 0; }
        var n1 = sumKnown(rs, "f1"), n21 = sumKnown(rs, "f21");
        var c = paint(n1, scale);
        classTable += "<tr><td>" + esc(name) + "</td><td>" + rs.length + "</td><td>" + Math.round(hold) + "</td><td>" + add + "</td><td>" + del + "</td><td style='background:" + c[0] + ";color:" + c[1] + "'>" + money(n1) + "</td><td>" + money(n21) + "</td></tr>";
      });
      classTable += "</tbody></table>";
      function stockCol(title, rows) {
        var h = "<div class=board><h3>" + esc(title) + " · " + rows.length + "</h3>";
        rows.forEach(function (r, i) {
          if (state.q && r.t.indexOf(state.q) < 0 && String(r.name || "").toUpperCase().indexOf(state.q) < 0) return;
          h += "<div class=row><span>" + (i + 1) + "  <b data-sym='" + esc(r.t) + "'>" + esc(r.t) + "</b> " + esc(r.name || "") + "</span><span>" + r.n + " ETFs</span></div>";
        });
        return h + "</div>";
      }
      var mv = extra.moves;
      var stockHtml = "";
      if (mv && mv.added) {
        function takeMv(list) { return (list || []).filter(passName).slice(0, 40); }
        stockHtml = "<div class=boards>"
          + stockCol("MOST STOCKS HELD", takeMv(mv.held))
          + stockCol("STOCKS ADDED", takeMv(mv.added))
          + stockCol("STOCKS LEFT", takeMv(mv.left))
          + stockCol("NEW EMERGERS", takeMv(mv.emergers))
          + "</div>";
      } else if (!mv) stockHtml = "<div class=note>Loading which stocks the book holds, added, and left…</div>";
      var html = stockHtml + classTable + "<div class=boards>"
        + col("FUND LINE COUNT", book, "nHold", function (r) { return Math.round(r.nHold); })
        + col("FUND LINES ADDED", book, "added", function (r) { return r.added; })
        + col("FUND LINES DELETED", book, "deleted", function (r) { return r.deleted; })
        + spreadBoards(book)
        + foreignFlowBoard();
      if (cen.length) html += col("MOST HOLDINGS · CENSUS 303", cen, "holdings_n", function (r) { return Math.round(r.holdings_n); });
      if (fv) {
        html += col("1M INFLOWS", (fv.top_inflows || []).map(function (x) { return { t: x.ticker, asset: x.etf_type || "", v: num(x.flows_1m) }; }), "v", function (r) { return money(r.v); });
        html += col("1M OUTFLOWS", (fv.top_outflows || []).map(function (x) { var v = num(x.flows_1m); return { t: x.ticker, asset: x.etf_type || "", v: v, abs: v == null ? null : Math.abs(v) }; }), "abs", function (r) { return money(r.v); });
      }
      if (extra.issMap) {
        var dis = disagreeRows();
        html += "<div class=board><h3>SIGN DISAGREE · CREATIONS VS ISSUER SHARES · " + dis.length + "</h3>";
        dis.slice(0, 20).forEach(function (r) {
          html += "<div class=row><span><b data-sym='" + esc(r.t) + "'>" + esc(r.t) + "</b></span><span>creations " + money(r.cre) + " · issuer " + money(r.iss) + "</span></div>";
        });
        html += "</div>";
      }
      html += "</div><div class=note>Fund-level added and deleted are line counts, not stock names. The stock boards above are entries and exits. Click a ticker to open it. Sign disagree compares creation dollars ending " + esc(flowAsOf) + " with issuer share-change ending " + esc(extra.issEnd || "—") + ". It is a flag, not a blended flow. 21D cells are a dash when that window did not print.</div>";
      body.innerHTML = html;
      meta("Scope " + state.scope + " · book " + book.length + " · census " + (cen.length || "…") + " · equity ETFs and equity stocks are separate");
    }

    function bindTickers(root) {
      root.querySelectorAll("td:first-child").forEach(function (td) {
        if (td.getAttribute("data-sym")) return;
        var sym = String(td.textContent || "").replace(/\s+/g, "");
        if (!/^[A-Z][A-Z0-9.\-]{0,9}$/.test(sym)) return;
        td.setAttribute("data-sym", sym);
        td.title = "Open " + sym + " on the chart";
      });
    }
    function armEquityBars() {
      if (host._eqArmed) return;
      host._eqArmed = true;
      var q = all.filter(isEquityEtf).map(function (r) { return r.t; }), i = 0;
      function step() {
        if (!host.isConnected) return;
        var batch = [];
        while (i < q.length && batch.length < 2) { if (!tech[q[i]]) batch.push(q[i]); i++; }
        if (!batch.length) { paintRegime(); return; }
        Promise.all(batch.map(loadTech)).then(function () { paintRegime(); setTimeout(step, 40); });
      }
      setTimeout(step, 400);
    }
    function draw() {
      if (state.tab === "flows") showFlows();
      else if (state.tab === "holdings") showHold();
      else if (state.tab === "census") showCensus();
      else if (state.tab === "tech") showTech();
      else showTops();
      bindTickers(body);
      paintRegime();
    }
    var scopes = [["all", "All"], ["eqEtf", "Equity ETFs"], ["eqStock", "Equity stocks"], ["intl", "International"], ["em", "Emerging"], ["frontier", "Frontier"]];
    host.querySelector("#etf-scope").innerHTML = scopes.map(function (s) {
      return "<button type=button data-scope='" + s[0] + "'" + (s[0] === state.scope ? " class=on" : "") + ">" + s[1] + "</button>";
    }).join("");
    host.querySelectorAll("#etf-scope button").forEach(function (b) {
      b.onclick = function () {
        state.scope = b.getAttribute("data-scope");
        state.cls = "All";
        host._belowGen = (host._belowGen || 0) + 1;
        markScope();
        draw();
      };
    });
    ensureIssuer();
    ensureUni();
    armEquityBars();
    host.querySelectorAll("#etf-tabs button").forEach(function (b) {
      b.onclick = function () {
        state.tab = b.getAttribute("data-tab");
        state.cls = "All";
        host.querySelectorAll("#etf-tabs button").forEach(function (x) { x.className = x === b ? "on" : ""; });
        draw();
      };
    });
    window.addEventListener("resize", function () {
      if (!host.isConnected) return;
      if (state.tab === "flows" && state.mode === "heat") showFlows();
      else if (state.tab === "tech" && (state.techMode === "spreads" || state.techMode === "below" || state.techMode === "sectors")) draw();
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
