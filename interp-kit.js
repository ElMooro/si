/* ──────────────────────────────────────────────────────────────────────
   JustHodl interp-kit.js — shared "you are here vs history" engine.
   Any page can mount the crisis/tops/bottoms historical-context block in
   one line:  JHKit.mountHistContext('elementId', ['VIXCLS', ...])

   Reads data/episode-reference.json (16 indicators × 13 episodes × 35y
   monthly history + NBER recession bands). Self-injects its own CSS.
   ────────────────────────────────────────────────────────────────────── */
(function (w) {
  "use strict";
  var EP_URL = "https://justhodl-data-proxy.raafouis.workers.dev/episode-reference.json";
  var _cache = null, _inflight = null;

  var esc = function (s) { return String(s == null ? "" : s).replace(/[&<>]/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]; }); };

  function injectCSS() {
    if (document.getElementById("jhk-css")) return;
    var css = `
    .jhk-ts{background:var(--panel,#0f131a);border:1px solid var(--line,#1c2433);border-radius:8px;padding:16px 16px 10px;margin-bottom:12px}
    .jhk-ts .jhk-tt{font-family:var(--mono,monospace);font-size:11.5px;font-weight:700;color:var(--txt,#e6eaf2);margin-bottom:2px}
    .jhk-ts .jhk-tl{font-family:var(--mono,monospace);font-size:10px;color:var(--dim,#6f7b91);margin-bottom:8px}
    .jhk-row{background:var(--panel,#0f131a);border:1px solid var(--line,#1c2433);border-radius:8px;padding:14px 16px;margin-bottom:12px}
    .jhk-h{display:flex;align-items:baseline;justify-content:space-between;flex-wrap:wrap;gap:8px;margin-bottom:4px}
    .jhk-hl{font-family:var(--mono,monospace);font-size:12px;font-weight:700;color:var(--txt,#e6eaf2)}
    .jhk-hc{font-family:var(--mono,monospace);font-size:11px;color:var(--mute,#a8b3c7)}
    .jhk-near{font-family:var(--mono,monospace);font-size:11px;color:var(--mute,#a8b3c7);margin:2px 0 2px}
    .jhk-near b{color:var(--txt,#e6eaf2)}
    .jhk-badge{font-family:var(--mono,monospace);font-size:10px;font-weight:700;padding:2px 7px;border-radius:4px}
    .jhk-tbl{display:grid;grid-template-columns:1fr auto auto;gap:0;font-family:var(--mono,monospace);font-size:11px;margin-top:8px;border:1px solid var(--line,#1c2433);border-radius:6px;overflow:hidden}
    .jhk-tbl>div{padding:6px 11px}
    .jhk-load{text-align:center;padding:30px;color:var(--dim,#6f7b91);font-family:var(--mono,monospace);font-size:12px;letter-spacing:1.5px}`;
    var st = document.createElement("style"); st.id = "jhk-css"; st.textContent = css;
    document.head.appendChild(st);
  }

  function loadRef() {
    if (_cache) return Promise.resolve(_cache);
    if (_inflight) return _inflight;
    _inflight = fetch(EP_URL + "?t=" + Date.now())
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (j) { _cache = j; return j; })
      .catch(function () { return null; });
    return _inflight;
  }

  function pctColor(p) { if (p == null) return "var(--mute,#a8b3c7)"; if (p >= 80) return "var(--red,#ff5577)"; if (p >= 60) return "var(--orange,#ff7a18)"; if (p >= 40) return "var(--yellow,#ffd266)"; if (p >= 20) return "var(--cyan,#00d4ff)"; return "var(--green,#26ffaf)"; }
  function epTypeColor(t) { t = t || ""; return /TOP/.test(t) ? "var(--red,#ff5577)" : /BOTTOM/.test(t) ? "var(--green,#26ffaf)" : "var(--orange,#ff7a18)"; }

  function ymToX(ym, ym0, ymN, x0, x1) {
    var toM = function (s) { var p = s.split("-"); return (+p[0]) * 12 + (+p[1] - 1); };
    var a = toM(ym0), b = toM(ymN), v = toM(ym);
    return x0 + ((v - a) / (b - a)) * (x1 - x0);
  }

  function drawHistorySeries(ind, recessions, episodes) {
    var mh = ind.monthly_history; if (!mh || mh.length < 2) return "";
    var W = 1080, H = 240, pad = { l: 46, r: 18, t: 14, b: 26 };
    var ym0 = mh[0][0], ymN = mh[mh.length - 1][0];
    var vals = mh.map(function (p) { return p[1]; });
    var lo = Math.min.apply(null, vals), hi = Math.max.apply(null, vals);
    var pd = (hi - lo) * 0.08 || 0.1; lo -= pd; hi += pd;
    var sx = function (ym) { return ymToX(ym, ym0, ymN, pad.l, W - pad.r); };
    var sy = function (v) { return pad.t + (1 - (v - lo) / (hi - lo)) * (H - pad.t - pad.b); };
    var svg = "";
    (recessions || []).forEach(function (rc) { var x1 = sx(rc.start), x2 = sx(rc.end); if (x2 > pad.l && x1 < W - pad.r) { svg += '<rect x="' + Math.max(pad.l, x1) + '" y="' + pad.t + '" width="' + (Math.min(W - pad.r, x2) - Math.max(pad.l, x1)) + '" height="' + (H - pad.t - pad.b) + '" fill="rgba(255,85,119,.07)"/>'; } });
    for (var i = 0; i <= 4; i++) { var yv = lo + (hi - lo) * i / 4; var yy = sy(yv); svg += '<line x1="' + pad.l + '" y1="' + yy + '" x2="' + (W - pad.r) + '" y2="' + yy + '" stroke="var(--line,#1c2433)" stroke-width="1"/><text x="' + (pad.l - 5) + '" y="' + (yy + 3) + '" fill="var(--dim,#6f7b91)" font-size="9.5" font-family="monospace" text-anchor="end">' + yv.toFixed(1) + '</text>'; }
    if (lo < 0 && hi > 0) { var zy = sy(0); svg += '<line x1="' + pad.l + '" y1="' + zy + '" x2="' + (W - pad.r) + '" y2="' + zy + '" stroke="var(--mute,#a8b3c7)" stroke-width="1" stroke-dasharray="3 3" opacity="0.5"/>'; }
    var y0 = +ym0.slice(0, 4), yN = +ymN.slice(0, 4);
    for (var y = Math.ceil(y0 / 5) * 5; y <= yN; y += 5) { var x = sx(y + "-01"); svg += '<text x="' + x + '" y="' + (H - pad.b + 16) + '" fill="var(--dim,#6f7b91)" font-size="9.5" font-family="monospace" text-anchor="middle">' + y + '</text>'; }
    var path = mh.map(function (p, i) { return (i ? "L" : "M") + sx(p[0]).toFixed(1) + " " + sy(p[1]).toFixed(1); }).join(" ");
    svg += '<path d="' + path + '" fill="none" stroke="var(--cyan,#00d4ff)" stroke-width="1.6"/>';
    (episodes || []).forEach(function (e) { var v = (ind.at_episodes || {})[e.id]; if (v == null) return; var x = sx(e.date.slice(0, 7)); if (x < pad.l || x > W - pad.r) return; svg += '<circle cx="' + x + '" cy="' + sy(v) + '" r="3.5" fill="' + epTypeColor(e.type) + '" stroke="var(--bg,#0a0e14)" stroke-width="1"/>'; });
    var nx = sx(ymN), ny = sy(ind.current);
    svg += '<circle cx="' + nx + '" cy="' + ny + '" r="4.5" fill="var(--txt,#e6eaf2)"/><text x="' + (nx - 4) + '" y="' + (ny - 9) + '" fill="var(--txt,#e6eaf2)" font-size="10" font-weight="700" font-family="monospace" text-anchor="end">NOW ' + ind.current + '</text>';
    return '<svg viewBox="0 0 ' + W + ' ' + H + '" style="width:100%;height:auto;display:block">' + svg + '</svg>';
  }

  function episodeRank(ind, episodes) {
    var epMap = {}; (episodes || []).forEach(function (e) { epMap[e.id] = e; });
    var rows = Object.keys(ind.at_episodes || {}).map(function (id) { return { v: ind.at_episodes[id], name: (epMap[id] || {}).name || id, type: (epMap[id] || {}).type || "", isNow: false }; });
    rows.push({ v: ind.current, name: "YOU ARE HERE", type: "NOW", isNow: true });
    rows.sort(function (a, b) { return b.v - a.v; });
    var u = ind.unit || "";
    return '<div class="jhk-tbl">' + rows.map(function (r) {
      var c = r.isNow ? "var(--cyan,#00d4ff)" : epTypeColor(r.type);
      var bg = r.isNow ? "background:rgba(0,212,255,.10)" : "";
      var badge = r.isNow ? "NOW" : (r.type || "").replace("BOTTOM", "BTM").replace("CRISIS", "CRI").replace("TOP", "TOP");
      var w = r.isNow ? "700" : "400"; var fc = r.isNow ? "var(--cyan,#00d4ff)" : "var(--mute,#a8b3c7)";
      return '<div style="' + bg + ';color:' + fc + ';font-weight:' + w + '">' + (r.isNow ? "► " : "") + esc(r.name) + '</div>' +
        '<div style="' + bg + ';text-align:right"><span style="color:' + c + ';font-size:9px;font-weight:700;letter-spacing:.5px">' + badge + '</span></div>' +
        '<div style="' + bg + ';text-align:right;color:' + (r.isNow ? "var(--cyan,#00d4ff)" : "var(--txt,#e6eaf2)") + ';font-weight:' + w + '">' + r.v + u + '</div>';
    }).join("") + '</div>';
  }

  function renderInto(el, ref, ids, opts) {
    opts = opts || {};
    if (!ref || !ref.indicators) { el.innerHTML = '<div class="jhk-load">historical reference unavailable</div>'; return; }
    var html = "";
    var primary = ids[0], pind = ref.indicators[primary];
    if (pind && pind.monthly_history && pind.monthly_history.length && opts.chart !== false) {
      html += '<div class="jhk-ts"><div class="jhk-tt">' + esc(pind.label) + ' · 1990–today</div><div class="jhk-tl">Recession bands shaded · ●tops ●bottoms ●crises · the full journey through every cycle</div>' + drawHistorySeries(pind, ref.recessions, ref.episodes) + '</div>';
    }
    ids.forEach(function (id) {
      var ind = ref.indicators[id]; if (!ind) return;
      var ne = ind.nearest_episode || {};
      var tword = /TOP/.test(ne.type || "") ? "a market top" : /BOTTOM/.test(ne.type || "") ? "a market bottom" : "a crisis";
      html += '<div class="jhk-row"><div class="jhk-h"><span class="jhk-hl">' + esc(ind.label) + '</span>' +
        '<span class="jhk-hc">now <b style="color:var(--txt,#e6eaf2)">' + ind.current + (ind.unit || "") + '</b> · <span class="jhk-badge" style="background:rgba(255,255,255,.05);color:' + pctColor(ind.percentile) + '">' + ind.percentile + 'th pctile</span></span></div>' +
        '<div class="jhk-near">Closest analog: <b style="color:' + epTypeColor(ne.type || "") + '">' + esc(ne.name || "—") + '</b> — ' + tword + ' (was ' + ne.value + (ind.unit || "") + ').</div>' +
        episodeRank(ind, ref.episodes) + '</div>';
    });
    el.innerHTML = html || '<div class="jhk-load">no matching indicators</div>';
  }

  var JHKit = {
    loadRef: loadRef,
    mountHistContext: function (elId, ids, opts) {
      injectCSS();
      var el = document.getElementById(elId); if (!el) return;
      el.innerHTML = '<div class="jhk-load">BENCHMARKING vs HISTORY…</div>';
      return loadRef().then(function (ref) { renderInto(el, ref, ids, opts); return ref; });
    },
    drawHistorySeries: drawHistorySeries,
    episodeRank: episodeRank,
    pctColor: pctColor,
    epTypeColor: epTypeColor
  };

  /* ── Reusable Asset Matrix (regime → SPX/credit/crypto/gold/USD/duration) ── */
  var DS = { BULLISH: 2, MIXED: 0.5, NEUTRAL: 0, CAUTION: -1, BEARISH: -2 };
  function toDir(s) { return s >= 1.5 ? "BULLISH" : s >= 0.4 ? "MIXED" : s > -0.4 ? "NEUTRAL" : s > -1.5 ? "CAUTION" : "BEARISH"; }
  function dirStyle(d) {
    return { BULLISH: "background:rgba(38,255,175,.12);color:#26ffaf",
             BEARISH: "background:rgba(255,85,119,.12);color:#ff5577",
             CAUTION: "background:rgba(255,122,24,.12);color:#ff7a18",
             MIXED:   "background:rgba(255,210,102,.12);color:#ffd266",
             NEUTRAL: "background:rgba(168,179,199,.10);color:#a8b3c7" }[d] || "";
  }
  function computeMods(ref) {
    var mods = { eq:0, credit:0, crypto:0, gold:0, usd:0, dur:0 };
    var notes = [], adj = {};
    var I = (ref && ref.indicators) || {};
    var real = I.DFII10, ten = I.DGS10, curve = I.T10Y2Y, be = I.T10YIE;
    if (real && real.percentile != null && real.percentile >= 75) {
      mods.eq -= 1.5; adj.eq = "real 10Y yield at the " + real.percentile + "th pctile (" + real.current + "%) is restrictive — a valuation headwind";
      mods.credit -= 1; adj.credit = "restrictive real rates raise refinancing/default risk as the cycle ages";
      mods.crypto -= 2; adj.crypto = "real yields at the " + real.percentile + "th pctile are a direct headwind — crypto is the most rate-sensitive risk asset";
      mods.gold -= 1.5; adj.gold = "high real yields (" + real.current + "%) are gold's primary headwind";
      mods.usd += 1.5; adj.usd = "high real yields support the dollar";
      mods.dur += 0.5; adj.dur = "the " + real.percentile + "th-pctile real yield makes the income/entry attractive";
      notes.push("real 10Y yields are <b>restrictive</b> (" + real.percentile + "th pctile)");
    } else if (real && real.percentile != null && real.percentile <= 25) {
      mods.eq += 1; mods.crypto += 1.5; mods.gold += 1.5; mods.usd -= 1;
      notes.push("real yields are accommodative (" + real.percentile + "th pctile) — a tailwind for risk");
    }
    var topish = (curve && curve.nearest_episode && /TOP/.test(curve.nearest_episode.type || "")) ||
                 (ten && ten.nearest_episode && /TOP/.test(ten.nearest_episode.type || ""));
    if (topish) {
      mods.eq -= 0.5; mods.credit -= 0.5; mods.crypto -= 0.5;
      notes.push("yields sit at <b>market-top</b> historical analogs (late-cycle)");
    }
    if (be && be.percentile != null && be.percentile >= 75) {
      mods.dur -= 0.5; notes.push("breakevens elevated (" + be.percentile + "th pctile) keep the Fed cautious");
    }
    return { mods: mods, notes: notes, adj: adj };
  }
  function renderAssetMatrix(elId, opts) {
    injectCSS();
    var el = document.getElementById(elId); if (!el) return;
    var regime = (opts.regime || "").toUpperCase();
    var base = opts.baseMatrix && (opts.baseMatrix[regime] || opts.baseMatrix[Object.keys(opts.baseMatrix)[0]]) || null;
    if (!base) { el.innerHTML = '<div class="jhk-load">no regime mapping available</div>'; return; }
    var mods = computeMods(opts.ref);
    var rows = [["US Equities (SPX/NDX)","eq"],["Credit (HY / IG)","credit"],["Crypto (BTC/ETH)","crypto"],
                ["Gold","gold"],["US Dollar (DXY)","usd"],["Duration / Bonds","dur"]];
    var leadExtra = "";
    if (mods.notes.length) leadExtra = ' <b style="color:#ff7a18">Cross-checks adjust these from the raw regime template:</b> ' + mods.notes.join("; ") + " — so the reads below are not the textbook \"" + regime + "\" call.";
    var html = '<div style="background:#0f131a;border:1px solid #1c2433;border-left:4px solid #a78bfa;border-radius:8px;padding:18px 20px">' +
      '<div style="font-family:ui-monospace,monospace;font-size:13px;font-weight:700;color:#a78bfa;margin-bottom:4px">⚡ ' + esc(opts.title || ("Regime → Risk-Asset Read · " + regime)) + '</div>' +
      '<div style="color:#a8b3c7;font-size:12.5px;line-height:1.6;margin-bottom:14px">' + (base.lead || "") + leadExtra + '</div>' +
      '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:10px">' +
      rows.map(function (r) {
        var nm = r[0], k = r[1]; var b = base[k] || ["NEUTRAL", ""];
        var score = (DS[b[0]] != null ? DS[b[0]] : 0) + (mods.mods[k] || 0);
        var dir = toDir(score);
        var note = (dir !== b[0] && mods.adj[k]) ? ' <span style="color:#ff7a18">Adjusted: ' + mods.adj[k] + '.</span>'
                 : (mods.adj[k] ? ' <span style="color:#6f7b91">' + mods.adj[k] + '.</span>' : '');
        return '<div style="background:#11161f;border:1px solid #1c2433;border-radius:7px;padding:12px 14px">' +
               '<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:5px">' +
               '<span style="font-family:ui-monospace,monospace;font-size:12px;font-weight:700;color:#e6eaf2">' + nm + '</span>' +
               '<span style="font-family:ui-monospace,monospace;font-size:10px;font-weight:700;padding:3px 8px;border-radius:4px;' + dirStyle(dir) + '">' + dir + '</span></div>' +
               '<div style="color:#a8b3c7;font-size:11.5px;line-height:1.5">' + (b[1] || "") + note + '</div></div>';
      }).join("") + '</div></div>';
    el.innerHTML = html;
  }

  /* ── Reusable Bottom Line synthesis ── */
  function renderBottomLine(elId, opts) {
    injectCSS();
    var el = document.getElementById(elId); if (!el) return;
    var ref = opts.ref || {}; var I = ref.indicators || {};
    var pind = I[opts.primary] || {};
    var ne = pind.nearest_episode || {};
    var parts = [];
    if (pind.current != null) {
      var tword = /TOP/.test(ne.type || "") ? "a market TOP" : /BOTTOM/.test(ne.type || "") ? "a market BOTTOM" : "a CRISIS";
      parts.push((opts.primaryLabel || pind.label || opts.primary) + " is at <b>" + pind.current + (pind.unit || "") + "</b> (" + pind.percentile + "th pctile), closest to <b>" + esc(ne.name || "—") + "</b> — " + tword + ".");
    }
    if (opts.regimeRead) parts.push(opts.regimeRead);
    var mods = computeMods(ref);
    if (mods.notes.length) parts.push("Cross-checks: " + mods.notes.join("; ") + ".");
    if (opts.net) parts.push(opts.net);
    el.innerHTML = '<div style="background:linear-gradient(135deg,rgba(0,212,255,.07),rgba(167,139,250,.05));border:1px solid rgba(0,212,255,.22);border-left:4px solid #00d4ff;border-radius:8px;padding:16px 20px;margin-bottom:16px">' +
      '<div style="font-family:ui-monospace,monospace;font-size:10px;letter-spacing:1.5px;text-transform:uppercase;color:#00d4ff;margin-bottom:6px">⚡ Bottom Line</div>' +
      '<div style="font-size:14.5px;line-height:1.6;color:#e6eaf2">' + parts.join(" ") + '</div></div>';
  }

  JHKit.renderAssetMatrix = renderAssetMatrix;
  JHKit.renderBottomLine = renderBottomLine;
  JHKit.computeMods = computeMods;
  w.JHKit = JHKit;
})(window);
