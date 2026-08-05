/* jh-institutional.js — shared institutional-grade UI components for
   crisis.html, plumbing.html, liquidity.html, and any future desk page
   built to the Google/MS bar per master directive 2026-08-05.

   Provides:
   - JHI.fetchAll(paths) -> Promise<{path: json | null}>
   - JHI.fmt(v, kind)     safe formatter (never renders "undefined%" or NaN)
   - JHI.ageChip(iso)      color-coded freshness pill
   - JHI.gauge(el, opts)   0-100 barometer with color threshold
   - JHI.regimeStrip(el, strip, bandsOpts?)   1996+ regime strip w/ crisis bands
   - JHI.percentileBar(el, current, distribution, opts)
   - JHI.components(el, rows)  weighted-component decomp bar
   - JHI.sparkline(el, arr)   inline mini chart
   - JHI.provenance(el, feeds)  data-provenance footer w/ live ages

   Intent: every page can import a barometer, regime strip, percentile bar,
   component decomp, and provenance footer in <200 LOC of page code.
*/
(function(){
  var JHI = {};

  // 8 canonical crisis bands (per master directive Part 2 visual standard #2)
  JHI.CRISIS_BANDS = [
    {name:"Asian",   start:"1997-07", end:"1998-01"},
    {name:"LTCM",    start:"1998-08", end:"1998-10"},
    {name:"Dotcom",  start:"2000-03", end:"2002-10"},
    {name:"GFC",     start:"2007-10", end:"2009-03"},
    {name:"EU-Sov",  start:"2011-05", end:"2012-07"},
    {name:"CNY",     start:"2015-08", end:"2016-02"},
    {name:"Q4-18",   start:"2018-10", end:"2018-12"},
    {name:"COVID",   start:"2020-02", end:"2020-04"},
    {name:"SVB",     start:"2023-03", end:"2023-05"}
  ];

  // ---- fetchers ----
  JHI.fetchAll = function(paths){
    var cb = "?cb=" + Date.now();
    return Promise.all(paths.map(function(p){
      return fetch(p + cb).then(function(r){ return r.ok ? r.json() : null; }).catch(function(){ return null; });
    })).then(function(arr){
      var out = {};
      paths.forEach(function(p,i){ out[p] = arr[i]; });
      return out;
    });
  };

  // ---- safe formatting ----
  JHI.fmt = function(v, kind){
    kind = kind || "n";
    if (v === null || v === undefined || v === "" || (typeof v === "number" && !isFinite(v))) return "—";
    if (kind === "pct")   return (typeof v === "number") ? v.toFixed(2) + "%" : "—";
    if (kind === "pct0")  return (typeof v === "number") ? v.toFixed(0) + "%" : "—";
    if (kind === "bp")    return (typeof v === "number") ? Math.round(v) + " bp" : "—";
    if (kind === "z")     return (typeof v === "number") ? (v>=0?"+":"") + v.toFixed(2) + "σ" : "—";
    if (kind === "num2")  return (typeof v === "number") ? v.toFixed(2) : String(v);
    if (kind === "num1")  return (typeof v === "number") ? v.toFixed(1) : String(v);
    if (kind === "int")   return (typeof v === "number") ? Math.round(v).toString() : String(v);
    if (kind === "usd_tn"){
      // Auto-detect scale: if abs(v) > 1000, assume input is billions (convert to T);
      // else assume input is already trillions.
      if (typeof v !== "number") return "—";
      var scaled = Math.abs(v) > 1000 ? (v/1000) : v;
      return "$" + scaled.toFixed(2) + "T";
    }
    if (kind === "usd_tn_from_bn"){
      if (typeof v !== "number") return "—";
      return "$" + (v/1000).toFixed(2) + "T";
    }
    if (kind === "usd_bn"){
      if (typeof v !== "number") return "—";
      return "$" + v.toFixed(0) + "B";
    }
    return (typeof v === "number") ? v.toFixed(2) : String(v);
  };

  JHI.ageHours = function(iso){
    if (!iso) return null;
    try {
      var t = new Date(iso).getTime();
      if (!isFinite(t)) return null;
      return (Date.now() - t) / 3600000;
    } catch(e){ return null; }
  };

  JHI.ageChip = function(iso){
    var h = JHI.ageHours(iso);
    if (h === null) return '<span class="chip stale">no ts</span>';
    var cls = h < 6 ? "chip ok" : (h < 26 ? "chip warn" : (h < 72 ? "chip stale" : "chip missing"));
    var label = h < 1 ? Math.round(h*60)+"m" : (h < 48 ? h.toFixed(1)+"h" : Math.round(h/24)+"d");
    return '<span class="'+cls+'">'+label+'</span>';
  };

  // ---- 0-100 barometer ----
  // opts: { value, label, verdict, verdictColor, thresholds: [green,amber,red,severe] }
  JHI.gauge = function(el, opts){
    var v = opts.value, thresh = opts.thresholds || [30, 55, 75, 90];
    var pct = (typeof v === "number") ? Math.max(0, Math.min(100, v)) : 0;
    var color;
    var palette = opts.invertColor
      ? ["var(--sev,#ff5a5a)","var(--red,#E07A6A)","var(--amber,#F0B429)","var(--blue,#5bb0ea)","var(--green,#6fce8a)"]
      : ["var(--green,#6fce8a)","var(--blue,#5bb0ea)","var(--amber,#F0B429)","var(--red,#E07A6A)","var(--sev,#ff5a5a)"];
    if (v === null || v === undefined || !isFinite(v)) color = "var(--dim,#666)";
    else if (v < thresh[0]) color = palette[0];
    else if (v < thresh[1]) color = palette[1];
    else if (v < thresh[2]) color = palette[2];
    else if (v < thresh[3]) color = palette[3];
    else color = palette[4];
    var W=220, H=132, cx=W/2, cy=H-10, r=90;
    var startA = Math.PI, endA = 0;
    // arc points
    function pt(a){ return [cx + r*Math.cos(a), cy - r*Math.sin(a)]; }
    var trackPath = "M " + pt(startA).join(" ") + " A " + r + " " + r + " 0 0 1 " + pt(endA).join(" ");
    var vAngle = startA + (endA - startA) * (pct/100);
    var valPath = "M " + pt(startA).join(" ") + " A " + r + " " + r + " 0 0 1 " + pt(vAngle).join(" ");
    var needle = pt(vAngle);
    var verdictHTML = opts.verdict ? '<div class="gauge-verdict" style="color:'+(opts.verdictColor||color)+'">'+opts.verdict+'</div>' : "";
    el.innerHTML =
      '<div class="gauge-wrap">' +
        '<svg viewBox="0 0 '+W+' '+H+'" width="100%" style="max-width:'+W+'px">' +
          '<path d="'+trackPath+'" fill="none" stroke="#2a2f3a" stroke-width="14" stroke-linecap="round"/>' +
          '<path d="'+valPath+'" fill="none" stroke="'+color+'" stroke-width="14" stroke-linecap="round"/>' +
          '<circle cx="'+cx+'" cy="'+cy+'" r="6" fill="'+color+'"/>' +
          '<line x1="'+cx+'" y1="'+cy+'" x2="'+needle[0]+'" y2="'+needle[1]+'" stroke="'+color+'" stroke-width="3" stroke-linecap="round"/>' +
          '<text x="'+cx+'" y="'+(cy-30)+'" text-anchor="middle" fill="var(--tx,#e8e8e8)" font-size="30" font-weight="800">'+JHI.fmt(v,"num1")+'</text>' +
          '<text x="12" y="'+(cy+18)+'" fill="var(--dim,#9aa4b2)" font-size="10">0</text>' +
          '<text x="'+(W-12)+'" y="'+(cy+18)+'" text-anchor="end" fill="var(--dim,#9aa4b2)" font-size="10">100</text>' +
        '</svg>' +
        (opts.label ? '<div class="gauge-label">'+opts.label+'</div>' : '') +
        verdictHTML +
      '</div>';
  };

  // ---- 1996+ regime strip with crisis bands ----
  // strip: [[YYYY-MM, regimeName], ...] chronological
  // We render a horizontal strip with regime colors + optional crisis bands overlay
  JHI.regimeStrip = function(el, strip, opts){
    opts = opts || {};
    if (!Array.isArray(strip) || !strip.length){
      el.innerHTML = '<div class="dim" style="padding:10px">Regime history unavailable.</div>';
      return;
    }
    var colors = {
      "GOLDILOCKS":       "#6fce8a",
      "REFLATION":        "#38bdf8",
      "STAGFLATION":      "#F0B429",
      "DEFLATION-BUST":   "#ff5a5a",
      "DEFLATION":        "#ff5a5a",
      "UNKNOWN":          "#3a3f4a"
    };
    var W = 1200, H = 44, padL = 40, padR = 20, padT = 4, padB = 18;
    var n = strip.length;
    var innerW = W - padL - padR;
    var cellW = innerW / n;

    function ymToIdx(ym){
      for (var i=0;i<n;i++){ if (strip[i][0] === ym) return i; }
      // approximate by parse
      var [y,m] = ym.split("-").map(Number);
      var t = y*12 + (m-1);
      var [y0,m0] = strip[0][0].split("-").map(Number);
      var t0 = y0*12 + (m0-1);
      return t - t0;
    }

    var cellsSvg = strip.map(function(row, i){
      var name = row[1] || "UNKNOWN";
      var c = colors[name] || "#3a3f4a";
      return '<rect x="'+(padL + i*cellW)+'" y="'+padT+'" width="'+(cellW+0.4)+'" height="'+(H - padT - padB)+'" fill="'+c+'" opacity="0.85"/>';
    }).join("");

    var bandsSvg = "";
    if (opts.showBands !== false){
      JHI.CRISIS_BANDS.forEach(function(b){
        var i0 = ymToIdx(b.start), i1 = ymToIdx(b.end);
        if (i0 < 0 || i1 < 0) return;
        var x0 = padL + i0*cellW, x1 = padL + (i1+1)*cellW;
        bandsSvg += '<rect x="'+x0+'" y="'+padT+'" width="'+(x1-x0)+'" height="'+(H - padT - padB)+'" fill="none" stroke="#0d1117" stroke-width="1.5"/>';
        bandsSvg += '<text x="'+((x0+x1)/2)+'" y="'+(H - padB + 12)+'" text-anchor="middle" fill="#e8e8e8" font-size="9" font-weight="600">'+b.name+'</text>';
      });
    }

    // year ticks every 4 years
    var y0 = parseInt(strip[0][0].split("-")[0], 10);
    var y1 = parseInt(strip[strip.length-1][0].split("-")[0], 10);
    var yearTicks = "";
    for (var yy = y0; yy <= y1; yy += 4){
      var idx = ymToIdx(yy + "-01");
      if (idx < 0) continue;
      var x = padL + idx*cellW;
      yearTicks += '<text x="'+x+'" y="'+(padT-2)+'" text-anchor="middle" fill="#9aa4b2" font-size="9">'+yy+'</text>';
    }

    // "TODAY" marker
    var lastX = padL + (n-1)*cellW + cellW/2;
    var todayMarker =
      '<line x1="'+lastX+'" y1="'+padT+'" x2="'+lastX+'" y2="'+(H-padB)+'" stroke="#fff" stroke-width="2"/>' +
      '<circle cx="'+lastX+'" cy="'+padT+'" r="3" fill="#fff"/>';

    el.innerHTML =
      '<svg viewBox="0 0 '+W+' '+H+'" width="100%" preserveAspectRatio="none" style="display:block">' +
        yearTicks + cellsSvg + bandsSvg + todayMarker +
      '</svg>' +
      '<div class="regime-legend">' +
        '<span><i style="background:#6fce8a"></i>GOLDILOCKS</span>' +
        '<span><i style="background:#38bdf8"></i>REFLATION</span>' +
        '<span><i style="background:#F0B429"></i>STAGFLATION</span>' +
        '<span><i style="background:#ff5a5a"></i>DEFLATION-BUST</span>' +
        '<span class="dim">shaded outlines: 8 crisis periods 1997-2023</span>' +
      '</div>';
  };

  // ---- historical percentile bar ----
  // opts: { current, samples: [numbers], label, invertColor?:bool }
  JHI.percentileBar = function(el, opts){
    var arr = (opts.samples||[]).slice().sort(function(a,b){ return a-b; });
    var v = opts.current;
    var pct = null;
    if (typeof v === "number" && arr.length){
      var below = 0;
      for (var i=0;i<arr.length;i++){ if (arr[i] < v) below++; }
      pct = 100 * below / arr.length;
    }
    var mn = arr.length ? arr[0] : null, mx = arr.length ? arr[arr.length-1] : null;
    var W=600, H=48, padL=40, padR=40;
    var innerW = W-padL-padR;
    var xForV = function(x){
      if (mn === null || mx === null || mx === mn) return padL + innerW/2;
      return padL + innerW * (x - mn)/(mx - mn);
    };
    // color scale
    var barColor = "linear-gradient(90deg, #6fce8a 0%, #5bb0ea 30%, #F0B429 60%, #E07A6A 85%, #ff5a5a 100%)";
    if (opts.invertColor) barColor = "linear-gradient(90deg, #ff5a5a 0%, #E07A6A 15%, #F0B429 40%, #5bb0ea 70%, #6fce8a 100%)";

    var markerX = (v!==null && v!==undefined && isFinite(v)) ? xForV(v) : null;
    var pctLabel = pct === null ? "—" : Math.round(pct)+"th pctile";

    el.innerHTML =
      '<div class="pct-wrap">' +
        '<div class="pct-track" style="background:'+barColor+'"></div>' +
        (markerX !== null
          ? '<div class="pct-marker" style="left:calc('+((markerX-padL)/innerW*100)+'% + '+(padL-6)+'px)"></div>'
          : '') +
        '<div class="pct-axis">' +
          '<span>' + (mn===null ? "—" : JHI.fmt(mn,"num2")) + '</span>' +
          '<span class="pct-label">'+ pctLabel +'</span>' +
          '<span>' + (mx===null ? "—" : JHI.fmt(mx,"num2")) + '</span>' +
        '</div>' +
      '</div>';
  };

  // ---- component decomposition bar ----
  // rows: [{label, weight, contribution, color?, available?}]
  JHI.components = function(el, rows, opts){
    opts = opts || {};
    var totW = rows.reduce(function(s,r){ return s + (r.weight||0); }, 0) || 1;
    var html = '<table class="comp-table"><thead><tr>' +
      '<th>Component</th><th class="r">Weight</th><th class="r">Stress</th><th>Bar</th>' +
    '</tr></thead><tbody>';
    rows.forEach(function(r){
      var contrib = (typeof r.contribution === "number") ? r.contribution : null;
      var barW = contrib !== null ? Math.min(100, Math.max(0, contrib)) : 0;
      var color = r.color || (contrib >= 75 ? "var(--red)" : contrib >= 50 ? "var(--amber)" : "var(--green)");
      var avail = r.available === false ? '<span class="chip missing">missing</span>' : '';
      html += '<tr><td>'+r.label+' '+avail+'</td>' +
              '<td class="r">'+ ((r.weight*100).toFixed(0) + "%") +'</td>' +
              '<td class="r">'+ (contrib===null?"—":contrib.toFixed(1)) +'</td>' +
              '<td><div class="comp-bar"><div class="comp-bar-fill" style="width:'+barW+'%;background:'+color+'"></div></div></td></tr>';
    });
    html += '</tbody></table>';
    el.innerHTML = html;
  };

  // ---- provenance footer ----
  // feeds: [{path, name, iso?}]
  JHI.provenance = function(el, feeds){
    var rows = feeds.map(function(f){
      return '<tr><td><a href="/'+f.path+'" target="_blank" rel="noopener">'+f.path+'</a></td>' +
             '<td>'+f.name+'</td>' +
             '<td>'+JHI.ageChip(f.iso)+'</td>' +
             '<td class="mono dim">'+ (f.iso? new Date(f.iso).toISOString().slice(0,16).replace("T"," ") : "—") +'</td></tr>';
    }).join("");
    el.innerHTML = '<table class="prov-table"><thead><tr><th>Feed</th><th>Engine / description</th><th>Age</th><th>Timestamp (UTC)</th></tr></thead><tbody>'+rows+'</tbody></table>';
  };

  // ---- inline sparkline ----
  JHI.sparkline = function(el, arr, opts){
    opts = opts || {};
    if (!Array.isArray(arr) || arr.length < 2){ el.innerHTML=''; return; }
    var W = opts.w || 220, H = opts.h || 36;
    var min = Math.min.apply(null, arr), max = Math.max.apply(null, arr);
    if (min === max){ min -= 1; max += 1; }
    var pts = arr.map(function(v,i){
      var x = i * W/(arr.length-1);
      var y = H - 2 - (H-4) * (v - min)/(max - min);
      return x.toFixed(1) + "," + y.toFixed(1);
    }).join(" ");
    var color = opts.color || "var(--amber, #F0B429)";
    el.innerHTML = '<svg viewBox="0 0 '+W+' '+H+'" width="100%" height="'+H+'" preserveAspectRatio="none"><polyline points="'+pts+'" fill="none" stroke="'+color+'" stroke-width="1.5"/></svg>';
  };

  window.JHI = JHI;
})();
