/* jh-reskin-skip */
/* tv-heat-1
   Correlation Matrix · ALERT
   TradingView-style treemap, one group per asset class.
   Color is compounded excess vs SPY on SPY-down sessions only.
   Green = held up. Red = dumped harder. Scale is inside the class.
   Tile area grows with how far the name sat from SPY (mono-weighted). */
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
    var n = v * 100;
    var body = Math.abs(n) >= 10 ? Math.abs(n).toFixed(1) : Math.abs(n).toFixed(2);
    if (n > 0) return "+" + body + "%";
    if (n < 0) return "−" + body + "%";
    return "0.00%";
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
    if (v == null || v !== v) return ["#2a2e39", "#d1d4dc"];
    var t = Math.max(-1, Math.min(1, v / (scale || 0.005)));
    var u = Math.pow(Math.abs(t), 0.62);
    var src = t < 0 ? [242, 54, 69] : [8, 153, 129];
    var mid = [32, 35, 46];
    var r = Math.round(mid[0] + (src[0] - mid[0]) * u);
    var g = Math.round(mid[1] + (src[1] - mid[1]) * u);
    var b = Math.round(mid[2] + (src[2] - mid[2]) * u);
    return ["rgb(" + r + "," + g + "," + b + ")", u > 0.42 ? "#fff" : "#d1d4dc"];
  }
  function tip(cell, label) {
    if (cell.v == null) return label + ": not enough overlapping SPY-down sessions (" + cell.k + ")";
    if (label === "Drop") return "From the high of the last 63 SPY sessions: this name " + fmt(cell.a) + ", SPY " + fmt(cell.s) + ". Green means it gave back less.";
    return label + " SPY-down sessions: this name " + fmt(cell.a) + " compounded, SPY " + fmt(cell.s) + ". Tile is the sum of daily excess (" + cell.k + " days).";
  }
  function esc(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      if (c === "&") return "&" + "amp;";
      if (c === "<") return "&" + "lt;";
      if (c === ">") return "&" + "gt;";
      return "&" + "quot;";
    });
  }

  function worst(row, side) {
    var sum = 0, max = 0, min = Infinity, k;
    for (k = 0; k < row.length; k++) {
      sum += row[k];
      if (row[k] > max) max = row[k];
      if (row[k] < min) min = row[k];
    }
    var s2 = sum * sum, w2 = side * side;
    return Math.max(w2 * max / s2, s2 / (w2 * min));
  }
  function treemap(weights, W, H) {
    var n = weights.length, i, total = 0, areas = [];
    for (i = 0; i < n; i++) total += weights[i];
    if (!(total > 0) || !(W > 2) || !(H > 2)) return [];
    var space = W * H;
    for (i = 0; i < n; i++) areas.push(weights[i] / total * space);
    var rects = new Array(n);
    var cursor = 0;
    function place(x, y, w, h, count) {
      if (count <= 0 || w <= 0 || h <= 0) return;
      if (count === 1) {
        rects[cursor] = { x: x, y: y, w: w, h: h };
        cursor++;
        return;
      }
      var side = Math.min(w, h);
      var row = [areas[cursor]];
      var best = worst(row, side);
      var take = 1;
      while (take < count) {
        var next = row.concat([areas[cursor + take]]);
        var score = worst(next, side);
        if (score > best) break;
        row = next;
        best = score;
        take++;
      }
      var rowArea = 0, k;
      for (k = 0; k < row.length; k++) rowArea += row[k];
      if (w >= h) {
        var rowW = rowArea / h;
        var yy = y;
        for (k = 0; k < row.length; k++) {
          var rh = row[k] / rowW;
          rects[cursor] = { x: x, y: yy, w: rowW, h: rh };
          cursor++;
          yy += rh;
        }
        place(x + rowW, y, w - rowW, h, count - take);
      } else {
        var rowH = rowArea / w;
        var xx = x;
        for (k = 0; k < row.length; k++) {
          var rw = row[k] / rowH;
          rects[cursor] = { x: xx, y: y, w: rw, h: rowH };
          cursor++;
          xx += rw;
        }
        place(x, y + rowH, w, h - rowH, count - take);
      }
    }
    place(0, 0, W, H, n);
    return rects;
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

  function colNow() {
    var c = window.__jhCorrCol;
    if (c == null || c < 0 || c > 3) return 1;
    return c | 0;
  }

  function render(host, model) {
    var col = colNow();
    var W = host.clientWidth || (host.parentElement && host.parentElement.clientWidth) || 0;
    if (W < 280) W = 960;
    var html = "<style>"
      + ".jhca{font:12px/1.25 -apple-system,BlinkMacSystemFont,'Trebuchet MS',Roboto,Ubuntu,sans-serif;color:#d1d4dc;background:#000;min-height:100%;padding:0 0 28px}"
      + ".jhca .top{display:flex;align-items:center;gap:14px;min-height:44px;padding:6px 12px;background:#131722;border-bottom:1px solid #2a2e39;position:sticky;top:0;z-index:3;flex-wrap:wrap}"
      + ".jhca .top .ttl{font-size:13px;font-weight:600;letter-spacing:.04em;text-transform:uppercase;color:#d1d4dc}"
      + ".jhca .top .meta{color:#787b86;font-size:12px;min-width:0}"
      + ".jhca .modes{margin-left:auto;display:flex;gap:1px;background:#0d1118;border:1px solid #2a2e39;border-radius:6px;padding:2px}"
      + ".jhca .modes button{padding:5px 10px;border-radius:4px;color:#787b86;font-size:12px;font-weight:600;letter-spacing:.02em}"
      + ".jhca .modes button.on{background:#2a2e39;color:#fff}"
      + ".jhca .grp{background:#000}"
      + ".jhca .gh{display:flex;align-items:center;gap:6px;height:26px;padding:0 10px;background:#131722;color:#787b86;font-size:12px;border-top:1px solid #000}"
      + ".jhca .gh b{color:#d1d4dc;font-weight:600}"
      + ".jhca .gh i{font-style:normal;color:#787b86}"
      + ".jhca .gh em{font-style:normal;margin-left:auto;color:#787b86}"
      + ".jhca .map{position:relative;background:#000;overflow:hidden}"
      + ".jhca .tile{position:absolute;box-sizing:border-box;display:flex;align-items:center;justify-content:center;overflow:hidden;text-align:center}"
      + ".jhca .tile .in{max-width:96%;line-height:1.05}"
      + ".jhca .tile b{display:block;font-weight:600;letter-spacing:.01em}"
      + ".jhca .tile em{display:block;font-style:normal;font-variant-numeric:tabular-nums;margin-top:3px;font-weight:500}"
      + ".jhca .lg{display:flex;align-items:center;gap:8px;padding:4px 10px 8px;background:#131722;color:#787b86;font-size:11px}"
      + ".jhca .sw{display:flex;flex:1;height:8px;max-width:420px;border-radius:1px;overflow:hidden}"
      + ".jhca .sw i{flex:1}"
      + ".jhca .note{color:#787b86;font-size:11px;padding:10px 12px 0;max-width:880px}"
      + "</style><div class=jhca>";
    var labels = ["Last 5 down days", "Last 21 down days", "Last 63 down days"];
    var spyBit = col < 3
      ? (labels[col] + " " + fmt(model.base[col] && model.base[col].v) + (model.base[col] && model.base[col].from ? " · " + model.base[col].from + " → " + model.base[col].to : ""))
      : ("Drop from the 63-session high " + fmt(model.dd));
    html += "<div class=top><span class=ttl>Held up vs dumped</span><span class=meta>SPY " + (model.px != null ? model.px.toFixed(2) : "—") + " · " + esc(model.last) + " · " + esc(spyBit) + "</span><div class=modes>";
    for (var m = 0; m < COLS.length; m++) {
      html += "<button type=button data-col=" + m + (m === col ? " class=on" : "") + ">" + COLS[m] + "</button>";
    }
    html += "</div></div>";
    for (var i = 0; i < model.classes.length; i++) {
      var cls = model.classes[i];
      if (!cls.rows.length) continue;
      var hint = cls.name === "Crypto" ? "spot ETFs" : cls.name === "Inverse" ? "rise when SPY falls" : "";
      var scale = cls.scales[col] || 0.005;
      var items = cls.rows.map(function (row) {
        var cell = row.cells[col];
        var v = cell.v;
        var weight = v == null ? 0.38 : Math.abs(v) / scale + 0.48;
        return { row: row, cell: cell, weight: weight };
      });
      items.sort(function (a, b) { return b.weight - a.weight || ((b.cell.v || 0) - (a.cell.v || 0)); });
      var n = items.length;
      var H = Math.round(Math.min(480, Math.max(156, Math.sqrt(n) * 112)));
      var rects = treemap(items.map(function (it) { return it.weight; }), W, H);
      html += "<section class=grp><div class=gh><b>" + esc(cls.name) + "</b><i>›</i>";
      if (hint) html += "<span>" + esc(hint) + "</span>";
      html += "<em>" + n + " · " + COLS[col] + "</em></div>";
      html += "<div class=map style=\"height:" + H + "px\">";
      for (var r = 0; r < n; r++) {
        var it = items[r];
        var rc = rects[r] || { x: 0, y: 0, w: 0, h: 0 };
        var gap = 1;
        var x = rc.x + gap, y = rc.y + gap;
        var tw = Math.max(0, rc.w - gap * 2), th = Math.max(0, rc.h - gap * 2);
        var bg = paint(it.cell.v, scale);
        var side = Math.min(tw, th);
        var symPx = side < 48 ? 11 : Math.max(12, Math.min(40, Math.floor(side * 0.30)));
        var pctPx = side < 52 || th < 40 ? 0 : Math.max(10, Math.min(16, Math.floor(side * 0.16)));
        var title = tip(it.cell, COLS[col]) + (it.row.asof ? " Tape as of " + it.row.asof + "." : "");
        html += "<div class=tile title=\"" + esc(title) + "\" style=\"left:" + x.toFixed(1) + "px;top:" + y.toFixed(1) + "px;width:" + tw.toFixed(1) + "px;height:" + th.toFixed(1) + "px;background:" + bg[0] + ";color:" + bg[1] + "\">";
        html += "<div class=in><b style=\"font-size:" + symPx + "px\">" + esc(it.row.sym) + "</b>";
        if (pctPx) html += "<em style=\"font-size:" + pctPx + "px\">" + fmt(it.cell.v) + "</em>";
        html += "</div></div>";
      }
      html += "</div><div class=lg><span>" + fmt(-scale) + "</span><span class=sw>";
      for (var s = 0; s < 7; s++) {
        var sw = paint((s - 3) / 3 * scale, scale);
        html += "<i style=\"background:" + sw[0] + "\"></i>";
      }
      html += "</span><span>" + fmt(scale) + "</span></div></section>";
    }
    if (model.missed && model.missed.length) html += "<p class=note>No warehouse tape, left blank: " + esc(model.missed.join(", ")) + "</p>";
    html += "<p class=note>Same read as a TradingView heatmap: tile color is the move, tile size is how far it was from unchanged. Here the move is excess versus SPY on sessions SPY fell — green held up, red dumped harder. Each asset class has its own scale. 5D / 21D / 63D are SPY-down sessions, not calendar days. Drop is this name's decline from its high over the last 63 SPY sessions, minus SPY's own decline. Warehouse closes only.</p></div>";
    host.innerHTML = html;
    host.onclick = function (ev) {
      var b = ev.target && ev.target.closest ? ev.target.closest("[data-col]") : null;
      if (!b || !host.contains(b)) return;
      window.__jhCorrCol = +b.getAttribute("data-col");
      render(host, model);
    };
    if (!host._jhcaRz) {
      host._jhcaRz = 1;
      var timer;
      window.addEventListener("resize", function () {
        clearTimeout(timer);
        timer = setTimeout(function () {
          if (host.isConnected && window.__jhCorrHeat) render(host, window.__jhCorrHeat);
        }, 140);
      });
    }
  }

  function parseList(spec) {
    return spec.split(",").map(function (sym) {
      return { sym: sym, key: "US__" + sym };
    });
  }

  async function loadBank(key) {
    var ctrl = new AbortController();
    var timer = setTimeout(function () { ctrl.abort(); }, 10000);
    try {
      var res = await fetch("/data/warm/tv-bars/universe/" + key + ".json.gz", { credentials: "omit", signal: ctrl.signal });
      if (!res.ok) throw new Error(key + " " + res.status);
      var buf = await res.arrayBuffer();
      var stream = new Blob([buf]).stream().pipeThrough(new DecompressionStream("gzip"));
      var text = await new Response(stream).text();
      var data = JSON.parse(text);
      if (!data || !data.bars || !data.bars.length) throw new Error(key + " empty");
      return data.bars;
    } finally {
      clearTimeout(timer);
    }
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
