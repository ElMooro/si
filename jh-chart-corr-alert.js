/* Correlation Matrix · ALERT
   Separate heatmap per asset class.
   Color is compounded excess vs SPY on SPY-down sessions only.
   Green = held up. Red = dumped harder. Scale is inside the class. */
(function () {
  var BOOKS = [
    ["Stocks", "AAPL,MSFT,NVDA,AMZN,GOOGL,META,TSLA,AVGO,AMD,JPM,XOM,LLY,COST"],
    ["Sector ETFs", "XLK,XLF,XLE,XLV,XLY,XLP,XLI,XLB,XLU,XLRE,XLC,SMH,KRE,XBI"],
    ["Index ETFs", "QQQ,IWM,DIA,MDY,RSP,VTI"],
    ["Factor ETFs", "SPLV,MTUM,QUAL,VLUE,USMV,ARKK"],
    ["Country ETFs", "EFA,EEM,VXUS,EWJ,EWZ,FXI,INDA,EWY,EWT,EWG,EWU,KWEB"],
    ["Bonds", "TLT,IEF,LQD,HYG,AGG,BND,SHY,TIP,BIL,EMB"],
    ["Commodities", "GLD,SLV,USO,UNG,DBC,DBA,CPER,WEAT,CORN,GDX,COPX,URA,TAN"],
    ["Crypto", "IBIT,BITO,GBTC,ETHA,ETHE,MSTR,COIN"],
    ["FX", "UUP,FXE,FXY,FXB,FXA,FXC,FXF"],
    ["Inverse", "SH,PSQ,SQQQ"]
  ];
  var WINS = [5, 21, 63];
  var COLS = ["5D", "21D", "63D", "Drop"];

  function dayKey(ts) { return Math.floor(+ts / 86400); }
  function ymd(day) {
    var d = new Date(day * 86400 * 1000);
    var m = d.getUTCMonth() + 1, dd = d.getUTCDate();
    return d.getUTCFullYear() + "-" + (m < 10 ? "0" : "") + m + "-" + (dd < 10 ? "0" : "") + dd;
  }
  function fmt(v) {
    if (v == null || v !== v) return "—";
    return (v >= 0 ? "+" : "") + (v * 100).toFixed(1) + "%";
  }
  function needFor(n) { return n >= 63 ? 40 : n >= 21 ? 14 : 4; }
  function closes(bars) {
    var map = Object.create(null), lastDay = 0;
    for (var i = 0; i < bars.length; i++) {
      var c = +bars[i][4];
      if (!(c > 0)) continue;
      var d = dayKey(bars[i][0]);
      map[d] = c;
      if (d > lastDay) lastDay = d;
    }
    return { map: map, lastDay: lastDay };
  }
  function spyDays(bars) {
    var map = Object.create(null), days = [];
    for (var i = 0; i < bars.length; i++) {
      var c = +bars[i][4];
      if (!(c > 0)) continue;
      var d = dayKey(bars[i][0]);
      if (map[d] == null) days.push(d);
      map[d] = c;
    }
    var downs = [];
    for (var j = 1; j < days.length; j++) {
      var prev = map[days[j - 1]], cur = map[days[j]];
      var r = cur / prev - 1;
      if (r < 0) downs.push({ day: days[j], prev: days[j - 1], sret: r });
    }
    return { map: map, days: days, downs: downs };
  }
  function compound(slice, amap) {
    var ps = 1, pa = 1, k = 0, sum = 0;
    for (var i = 0; i < slice.length; i++) {
      var a0 = amap[slice[i].prev], a1 = amap[slice[i].day];
      if (!(a0 > 0 && a1 > 0)) continue;
      var ar = a1 / a0 - 1;
      ps *= 1 + slice[i].sret;
      pa *= 1 + ar;
      sum += ar - slice[i].sret;
      k++;
    }
    return { k: k, ps: ps, pa: pa, v: k ? sum : null };
  }
  function spyRet(slice) {
    var p = 1;
    for (var i = 0; i < slice.length; i++) p *= 1 + slice[i].sret;
    return p - 1;
  }
  function scoreOne(spy, amap) {
    var out = [];
    for (var w = 0; w < WINS.length; w++) {
      var slice = spy.downs.slice(-WINS[w]);
      var c = compound(slice, amap);
      var need = needFor(WINS[w]);
      var fresh = slice.length && amap[slice[slice.length - 1].day] > 0;
      out.push(c.k >= need && fresh ? { v: c.v, k: c.k, a: c.pa - 1, s: c.ps - 1 } : { v: null, k: c.k, a: null, s: null });
    }
    var tail = spy.days.slice(-63);
    var last = spy.days[spy.days.length - 1];
    var peak = 0, i;
    for (i = 0; i < tail.length; i++) if (spy.map[tail[i]] > peak) peak = spy.map[tail[i]];
    var spyDd = peak > 0 ? spy.map[last] / peak - 1 : null;
    var aPeak = 0, ak = 0;
    for (i = 0; i < tail.length; i++) {
      var px = amap[tail[i]];
      if (px > 0) { ak++; if (px > aPeak) aPeak = px; }
    }
    var drop = null, aDd = null;
    if (ak >= 40 && amap[last] > 0 && aPeak > 0 && spyDd != null) {
      aDd = amap[last] / aPeak - 1;
      drop = { v: aDd - spyDd, a: aDd, s: spyDd, k: ak };
    } else drop = { v: null, a: null, s: spyDd, k: ak };
    out.push(drop);
    return out;
  }
  function scaleOf(rows, col) {
    var m = 0.005;
    for (var i = 0; i < rows.length; i++) {
      var v = rows[i].cells[col].v;
      if (v != null) m = Math.max(m, Math.abs(v));
    }
    return m;
  }
  function paint(v, scale) {
    if (v == null || v !== v) return ["#1e222d", "#787b86"];
    var t = Math.max(-1, Math.min(1, v / scale));
    var src = t < 0 ? [242, 54, 69] : [8, 153, 129];
    var u = Math.abs(t), mid = [30, 34, 45];
    var r = Math.round(mid[0] + (src[0] - mid[0]) * u);
    var g = Math.round(mid[1] + (src[1] - mid[1]) * u);
    var b = Math.round(mid[2] + (src[2] - mid[2]) * u);
    return ["rgb(" + r + "," + g + "," + b + ")", u > 0.62 ? "#fff" : "#d1d4dc"];
  }
  function tip(cell, label) {
    if (cell.v == null) return label + ": not enough overlapping SPY-down sessions (" + cell.k + ")";
    if (label === "Drop") return "From the high of the last 63 SPY sessions: this name " + fmt(cell.a) + ", SPY " + fmt(cell.s) + ". Green means it gave back less.";
    return label + " SPY-down sessions: this name " + fmt(cell.a) + " compounded, SPY " + fmt(cell.s) + ". Cell is the sum of daily excess (" + cell.k + " days).";
  }
  function esc(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      if (c === "&") return "&" + "amp;";
      if (c === "<") return "&" + "lt;";
      if (c === ">") return "&" + "gt;";
      return "&" + "quot;";
    });
  }

  window.jhCorrScore = function (spyBars, books) {
    var spy = spyDays(spyBars);
    var last = spy.days[spy.days.length - 1];
    var peak = 0, i;
    var tail = spy.days.slice(-63);
    for (i = 0; i < tail.length; i++) if (spy.map[tail[i]] > peak) peak = spy.map[tail[i]];
    var base = [];
    for (i = 0; i < WINS.length; i++) {
      var slice = spy.downs.slice(-WINS[i]);
      base.push({ v: spyRet(slice), k: slice.length, from: slice.length ? ymd(slice[0].day) : "", to: slice.length ? ymd(slice[slice.length - 1].day) : "" });
    }
    var classes = [];
    for (i = 0; i < books.length; i++) {
      var rows = [];
      var list = books[i].names;
      for (var n = 0; n < list.length; n++) {
        var book = list[n];
        if (!book || !book.bars) continue;
        var cl = closes(book.bars);
        rows.push({
          sym: book.sym,
          asof: cl.lastDay && cl.lastDay < last ? ymd(cl.lastDay) : "",
          cells: scoreOne(spy, cl.map)
        });
      }
      rows.sort(function (a, b) {
        var av = a.cells[1].v, bv = b.cells[1].v;
        if (av == null && bv == null) return a.sym < b.sym ? -1 : 1;
        if (av == null) return 1;
        if (bv == null) return -1;
        return bv - av;
      });
      classes.push({ name: books[i].name, rows: rows, scales: [0, 1, 2, 3].map(function (c) { return scaleOf(rows, c); }) });
    }
    return {
      last: ymd(last),
      px: spy.map[last],
      dd: peak > 0 ? spy.map[last] / peak - 1 : null,
      base: base,
      classes: classes,
      missed: books.missed || []
    };
  };

  function render(host, model) {
    var html = "<style>"
      + ".jhca{font:12px/1.35 ui-sans-serif,system-ui,sans-serif;color:#d1d4dc;padding:14px 16px 28px}"
      + ".jhca h2{margin:0 0 4px;font-size:13px;letter-spacing:.04em;text-transform:uppercase;color:#d1d4dc}"
      + ".jhca .sub{color:#787b86;margin:0 0 12px;max-width:820px}"
      + ".jhca .spy{display:flex;flex-wrap:wrap;gap:8px;margin:0 0 14px}"
      + ".jhca .pill{background:#1e222d;border:1px solid #2a2e39;border-radius:6px;padding:8px 10px;min-width:120px}"
      + ".jhca .pill b{display:block;color:#f23645;font-size:14px}"
      + ".jhca .pill span{color:#787b86;font-size:11px}"
      + ".jhca .leg{display:flex;align-items:center;gap:8px;margin:0 0 16px;color:#787b86}"
      + ".jhca .bar{width:140px;height:10px;border-radius:5px;background:linear-gradient(90deg,#f23645,#1e222d 50%,#089981)}"
      + ".jhca section{margin:0 0 18px}"
      + ".jhca h3{margin:0 0 6px;font-size:13px;color:#d1d4dc}"
      + ".jhca h3 em{font-style:normal;color:#787b86;font-weight:500}"
      + ".jhca table{border-collapse:separate;border-spacing:3px;width:100%}"
      + ".jhca th{text-align:right;color:#787b86;font-weight:600;font-size:11px;padding:0 6px}"
      + ".jhca th:first-child,.jhca td.nm{text-align:left}"
      + ".jhca td.nm{padding:0 8px;white-space:nowrap}"
      + ".jhca td.nm small{color:#787b86;margin-left:6px}"
      + ".jhca td.c{text-align:right;border-radius:4px;padding:5px 8px;font-variant-numeric:tabular-nums;min-width:72px}"
      + ".jhca .note{color:#787b86;font-size:11px;margin-top:8px}"
      + "</style><div class=jhca>";
    html += "<h2>Held up vs dumped · SPY down days</h2>";
    html += "<p class=sub>Not a correlation. Each cell adds up how much the name beat or lagged SPY on sessions SPY fell. Green held up. Red dumped harder. Each asset class has its own scale, so a bond is not washed out by crypto.</p>";
    html += "<div class=spy>";
    html += "<div class=pill><b>" + (model.px != null ? model.px.toFixed(2) : "—") + "</b><span>SPY close · " + esc(model.last) + "</span></div>";
    var labels = ["Last 5 down days", "Last 21 down days", "Last 63 down days"];
    for (var i = 0; i < model.base.length; i++) {
      var b = model.base[i];
      html += "<div class=pill><b>" + fmt(b.v) + "</b><span>" + labels[i] + (b.from ? " · " + b.from + " → " + b.to : "") + "</span></div>";
    }
    html += "<div class=pill><b>" + fmt(model.dd) + "</b><span>SPY drop from the 63-session high</span></div>";
    html += "</div>";
    html += "<div class=leg><span>Dumped more</span><i class=bar></i><span>Held up</span></div>";
    for (i = 0; i < model.classes.length; i++) {
      var cls = model.classes[i];
      var hint = cls.name === "Crypto" ? " — spot ETFs and proxies, not a coin tape" : cls.name === "Inverse" ? " — these are built to rise when SPY falls" : "";
      html += "<section><h3>" + esc(cls.name) + " <em>" + cls.rows.length + hint + "</em></h3><table><thead><tr><th>Name</th>";
      for (var c = 0; c < COLS.length; c++) html += "<th>" + COLS[c] + "</th>";
      html += "</tr></thead><tbody>";
      for (var r = 0; r < cls.rows.length; r++) {
        var row = cls.rows[r];
        html += "<tr><td class=nm>" + esc(row.sym) + (row.asof ? "<small>as of " + esc(row.asof) + "</small>" : "") + "</td>";
        for (c = 0; c < 4; c++) {
          var cell = row.cells[c];
          var bg = paint(cell.v, cls.scales[c]);
          html += "<td class=c style=\"background:" + bg[0] + ";color:" + bg[1] + "\" title=\"" + esc(tip(cell, COLS[c])) + "\">" + fmt(cell.v) + "</td>";
        }
        html += "</tr>";
      }
      html += "</tbody></table></section>";
    }
    if (model.missed.length) html += "<p class=note>No warehouse tape, left blank: " + esc(model.missed.join(", ")) + "</p>";
    html += "<p class=note>5D / 21D / 63D are SPY-down sessions, not calendar days. The number is the sum of (name return minus SPY return) on those days. Drop is this name's decline from its high over the last 63 SPY sessions, minus SPY's own decline. A name that missed the latest down day is left blank. Sorted by 21D. Warehouse closes only.</p></div>";
    host.innerHTML = html;
  }

  function parseList(spec) {
    return spec.split(",").map(function (sym) {
      return { sym: sym, key: "US__" + sym };
    });
  }

  async function loadBank(key) {
    var res = await fetch("/data/warm/tv-bars/universe/" + key + ".json.gz", { credentials: "omit" });
    if (!res.ok) throw new Error(key + " " + res.status);
    var buf = await res.arrayBuffer();
    var stream = new Blob([buf]).stream().pipeThrough(new DecompressionStream("gzip"));
    var text = await new Response(stream).text();
    var data = JSON.parse(text);
    if (!data || !data.bars || !data.bars.length) throw new Error(key + " empty");
    return data.bars;
  }

  window.jhCorrAlert = function (host) {
    if (!host) return;
    if (window.__jhCorrHeat) { render(host, window.__jhCorrHeat); return; }
    var jobs = [];
    BOOKS.forEach(function (b) { parseList(b[1]).forEach(function (x) { jobs.push(x); }); });
    host.innerHTML = "<div style='padding:16px;color:#787b86;font:13px ui-sans-serif,system-ui,sans-serif'>Loading warehouse closes… <b id=jhca-n>0</b>/" + (jobs.length + 1) + "</div>";
    var n = 0;
    function tick() {
      n++;
      var el = document.getElementById("jhca-n");
      if (el) el.textContent = String(n);
    }
    function pool(items, limit, fn) {
      var i = 0, active = 0, out = new Array(items.length);
      return new Promise(function (resolve) {
        function next() {
          if (i >= items.length && active === 0) return resolve(out);
          while (active < limit && i < items.length) {
            (function (idx) {
              active++;
              i++;
              fn(items[idx]).then(function (v) { out[idx] = v; }, function () { out[idx] = null; }).then(function () {
                active--;
                tick();
                next();
              });
            })(i);
          }
        }
        next();
      });
    }
    loadBank("US__SPY").then(function (spyBars) {
      tick();
      return pool(jobs, 6, function (job) {
        return loadBank(job.key).then(function (bars) { return { sym: job.sym, bars: bars }; });
      }).then(function (loaded) {
        if (!host.isConnected) return;
        var by = Object.create(null), missed = [];
        for (var i = 0; i < jobs.length; i++) {
          if (loaded[i] && loaded[i].bars) by[jobs[i].sym] = loaded[i];
          else missed.push(jobs[i].sym);
        }
        var books = BOOKS.map(function (b) {
          return {
            name: b[0],
            names: parseList(b[1]).map(function (x) { return by[x.sym] || null; })
          };
        });
        books.missed = missed;
        var model = window.jhCorrScore(spyBars, books);
        window.__jhCorrHeat = model;
        if (host.isConnected) render(host, model);
      });
    }).catch(function (err) {
      if (host.isConnected) host.innerHTML = "<div style='padding:16px;color:#f23645'>SPY tape did not load. " + esc(err && err.message || err) + "</div>";
    });
  };
})();
