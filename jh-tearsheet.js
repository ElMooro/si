/* jh-tearsheet.js - the tearsheet kit: one sheet at the top of a desk that says what the desk holds and what changed.

   Khalid 2026-11-17 (after the 13F and the Inflows & outflows tearsheets): "apply these concepts from the picture I
   attached and that donut everywhere in my system where you see it's necessary." The picture is a one-manager 13F
   tearsheet: a headline, a six-tile scoreboard, the most notable sale and buy, three notes, a donut of the positions
   beside their list, columns of what was opened / added / cut / closed, and a source line.

   A desk gets the sheet when it has the two things the picture needs: a population that adds up to a whole (the
   donut) and items that can be ranked or compared with their own prior print (the lists). A page opts in with
       <div data-jts="ark"></div>  +  <script src="/jh-tearsheet.js" defer></script>
   and the adapter of that name below reads the page's own published feed - nothing is estimated here that the feed
   does not carry; where a feed has no prior period the sheet says so and shows no change lists.

   Colours are the site's tokens only (jh-theme.css) with opacity steps, because the deploy reskin rewrites colour
   literals. A composition ring is one hue stepped by rank; a ring with two sides (long / short, in / out) is solid on
   one side and hatched on the other, because the site's green and red are too close for a colour-blind reader
   (validate_palette: 3.3 protan) - every slice also carries its sign. */
(function () {
  'use strict';
  var D = '\u2014', MINUS = '\u2212', DOT = ' \u00b7 ', seq = 0;

  function esc(v) { return String(v == null ? '' : v).replace(/[&<>"']/g, function (c) { return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]; }); }
  function fin(v) { return typeof v === 'number' && isFinite(v); }
  function sign(v, signed) { return v < 0 ? MINUS : (signed && v > 0 ? '+' : ''); }
  function money(v, signed) {
    if (!fin(v)) return D;
    var a = Math.abs(v);
    return sign(v, signed) + '$' + (a >= 1e12 ? (a / 1e12).toFixed(2) + 'T' : a >= 1e9 ? (a / 1e9).toFixed(2) + 'B' : a >= 1e6 ? (a / 1e6).toFixed(1) + 'M' : Math.round(a).toLocaleString('en-US'));
  }
  function pct(v, dp, signed) { return fin(v) ? sign(v, signed) + Math.abs(v).toFixed(dp == null ? 1 : dp) + '%' : D; }
  function num(v, dp) { return fin(v) ? v.toLocaleString('en-US', { minimumFractionDigits: dp || 0, maximumFractionDigits: dp || 0 }) : D; }
  function fix(v, dp) { return fin(v) ? sign(v) + Math.abs(v).toFixed(dp == null ? 2 : dp) : D; }
  function tone(v) { return v > 0 ? 'up' : v < 0 ? 'down' : ''; }
  function day(s) { return String(s || '').slice(0, 10); }
  var MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
  function month(s) { var m = /^(\d{4})-(\d{2})/.exec(String(s || '')); return m ? MONTHS[+m[2] - 1] + ' ' + m[1] : D; }
  function sum(list, f) { return list.reduce(function (acc, x) { var v = f(x); return acc + (fin(v) ? v : 0); }, 0); }
  function by(f, desc) { return function (a, b) { return desc ? f(b) - f(a) : f(a) - f(b); }; }

  var CSS = '.jts{--jb:var(--jh-bg,#0C0B09);--jp:var(--jh-panel,#12110C);--jp2:var(--jh-panel-2,#17150E);--ji:var(--jh-ink,#e8e2d4);--jd:var(--jh-dim,#8a836f);--ja:var(--jh-amber,#F0B429);--jg:var(--jh-green,#6fce8a);--jr:var(--jh-red,#E07A6A);--jl:var(--jh-line,#2B2820);'
    + '--jm:var(--jh-mono,ui-monospace,SFMono-Regular,Menlo,monospace);--js:var(--jh-sans,system-ui,-apple-system,sans-serif);box-sizing:border-box;max-width:1180px;margin:14px auto 22px;padding:0 20px;color:var(--ji);font-family:var(--js);text-align:left;scroll-margin-top:76px}'
    + '.jts[data-jts-flush]{max-width:none;padding:0;margin:0 0 22px}'
    + '.jts *{box-sizing:border-box}.jts-in{border:1px solid var(--jl);border-top:2px solid var(--ja);border-radius:8px;background:var(--jp);padding:14px 16px 12px;min-width:0}'
    + '.jts .up{color:var(--jg)}.jts .down{color:var(--jr)}.jts .am{color:var(--ja)}'
    + '.jts-top{display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap}'
    + '.jts-eye{font:700 9.5px/1.3 var(--jm);letter-spacing:1.1px;color:var(--ja);text-transform:uppercase}'
    + '.jts .jts-h{margin:8px 0 4px;padding:0;border:0;background:none;font:800 19px/1.3 var(--js);color:var(--ji);max-width:1000px;letter-spacing:0;text-transform:none}'
    + '.jts-sub{font:10.5px/1.5 var(--jm);color:var(--jd)}'
    + '.jts-act{display:flex;gap:6px;flex-wrap:wrap}.jts .jts-btn{width:auto;margin:0;font:700 9.5px/1 var(--jm);letter-spacing:.6px;text-transform:none;padding:6px 9px;border:1px solid var(--jl);border-radius:4px;background:var(--jb);color:var(--jd);box-shadow:none;cursor:pointer}'
    + '.jts .jts-btn:hover,.jts .jts-btn:focus-visible{color:var(--ja);border-color:var(--ja);outline:none}.jts .jts-btn[aria-pressed="true"]{color:var(--jb);background:var(--ja);border-color:var(--ja)}'
    + '.jts-ctl{display:flex;gap:6px;flex-wrap:wrap;margin-top:10px}'
    + '.jts-score{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px;margin-top:12px}'
    + '.jts-kpi{border:1px solid var(--jl);border-radius:6px;background:var(--jb);padding:9px 10px;min-width:0}'
    + '.jts-kpi .k{font:700 8.5px/1.25 var(--jm);letter-spacing:.8px;color:var(--jd);text-transform:uppercase}.jts-kpi .v{font:800 19px/1.15 var(--jm);color:var(--ji);margin-top:5px;overflow-wrap:anywhere}'
    + '.jts-kpi .v small{font-size:11px;color:var(--jd);font-weight:600}.jts-kpi .s{font:9.5px/1.4 var(--jm);color:var(--jd);margin-top:4px}'
    + '.jts-cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:8px;margin-top:8px}'
    + '.jts-card{display:flex;gap:14px;align-items:center;border:1px solid var(--jl);border-left:3px solid var(--jd);border-radius:6px;background:var(--jb);padding:11px 13px;min-width:0}'
    + '.jts-card.up{border-left-color:var(--jg)}.jts-card.down{border-left-color:var(--jr)}.jts-card .big{font:800 26px/1 var(--jm);white-space:nowrap;color:var(--ji)}'
    + '.jts-card .e{font:700 9px/1.3 var(--jm);letter-spacing:.9px;color:var(--jd);text-transform:uppercase}.jts-card .t{margin:3px 0;font:700 13.5px/1.3 var(--js);color:var(--ji);letter-spacing:0}'
    + '.jts .jts-card p{margin:0;padding:0;font:11.5px/1.5 var(--js);color:var(--ji);opacity:.86}'
    + '.jts-notes{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:10px;margin-top:10px}'
    + '.jts .jts-notes p{margin:0;padding:0 0 0 9px;border-left:2px solid var(--ja);font:11.5px/1.5 var(--js);color:var(--ji);opacity:.9}.jts-notes b{opacity:1}'
    + '.jts-cols{display:grid;grid-template-columns:repeat(auto-fit,minmax(270px,1fr));gap:8px;margin-top:12px;align-items:start}'
    + '.jts-col{border:1px solid var(--jl);border-radius:6px;background:var(--jb);min-width:0;overflow:hidden}'
    + '.jts-ch{display:flex;justify-content:space-between;gap:8px;align-items:baseline;padding:7px 9px;border-bottom:1px solid var(--jl);font:700 9.5px/1.3 var(--jm);letter-spacing:.7px;color:var(--ji);background:var(--jp2);text-transform:uppercase}'
    + '.jts-ch i{font-style:normal;font-weight:500;letter-spacing:0;color:var(--jd);text-transform:none}.jts-col.up .jts-ch b{color:var(--jg)}.jts-col.down .jts-ch b{color:var(--jr)}.jts-n{color:var(--jd)}'
    + '.jts .jts-t{width:100%;margin:0;border:0;border-collapse:collapse;background:none;font:11px var(--jm);font-variant-numeric:tabular-nums}.jts .jts-t tr{background:none}.jts .jts-t td{padding:6px 9px;border:0;border-bottom:1px solid var(--jl);text-align:left;vertical-align:top;background:none;color:var(--ji);font-size:11px}'
    + '.jts .jts-t .n{text-align:right;white-space:nowrap}.jts-nm{display:block;font:600 11.5px/1.3 var(--js);color:var(--ji);overflow-wrap:anywhere}'
    + '.jts-s{display:block;font:9.5px/1.45 var(--jm);color:var(--jd);overflow-wrap:anywhere;white-space:normal}.jts .jts-t .n .jts-s{white-space:normal;max-width:16em;margin-left:auto}.jts-big{display:block;font:800 13px/1.2 var(--jm);color:var(--ji)}'
    + '.jts .jts-none,.jts .jts-rest{margin:0;padding:7px 9px;font:10px/1.5 var(--jm);color:var(--jd)}'
    + '.jts-ring{display:grid;place-items:center;padding:12px 10px 4px}.jts-ring svg{display:block;width:100%;max-width:240px;height:auto;overflow:visible}'
    + '.jts-arc{stroke:var(--jb);stroke-width:2}.jts-arc.pos{fill:var(--ja)}.jts-arc.up{fill:var(--jg)}.jts-arc.oth{fill:var(--jd)}.jts-arc:hover{stroke:var(--ji)}'
    + '.jts-hbg{fill:var(--jr)}.jts-hln{stroke:var(--jb);stroke-width:2.2}'
    + '.jts-rl{font:700 10.5px var(--jm);fill:var(--ji);stroke:var(--jb);stroke-width:3px;paint-order:stroke;text-anchor:middle;pointer-events:none}'
    + '.jts-rc{font:800 19px var(--jm);fill:var(--ji);text-anchor:middle}.jts-rs{font:600 8.5px var(--jm);fill:var(--jd);text-anchor:middle;letter-spacing:.6px}'
    + '.jts-sw{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:7px;vertical-align:-1px;background:var(--ja)}.jts-sw.up{background:var(--jg)}.jts-sw.oth{background:var(--jd)}'
    + '.jts-sw.down{background:repeating-linear-gradient(45deg,var(--jr) 0 3px,var(--jb) 3px 5px)}.jts-leg .jts-nm{display:inline}'
    + '.jts .jts-foot{margin:10px 0 0;padding:0;font:10.5px/1.6 var(--js);color:var(--jd);max-width:1100px}.jts-wait{font:11px var(--jm);color:var(--jd);padding:10px 0}'
    + '@media (max-width:700px){.jts{padding:0 12px}.jts[data-jts-flush]{padding:0}.jts-in{padding:12px}.jts .jts-h{font-size:16px}.jts-score{grid-template-columns:repeat(2,minmax(0,1fr))}.jts-cols,.jts-cards,.jts-notes{grid-template-columns:minmax(0,1fr)}.jts-card .big{font-size:21px}}'
    /* the full-list view */
    + '.jts-chr{display:inline-flex;gap:9px;align-items:baseline;white-space:nowrap}.jts-go{color:var(--ja);font-weight:700;letter-spacing:.6px}'
    + '.jts-ch[data-jts-open]{cursor:pointer}.jts-ch[data-jts-open]:hover,.jts-ch[data-jts-open]:focus-visible{background:var(--jb);box-shadow:inset 0 0 0 1px var(--ja);outline:none}'
    + '.jts .jts-lnk{display:inline;width:auto;min-width:0;margin:0;padding:0;border:0;border-radius:0;background:none;box-shadow:none;font:inherit;letter-spacing:0;text-transform:none;color:var(--ja);cursor:pointer;text-decoration:underline;text-underline-offset:2px}'
    + '.jts .jts-lnk:hover,.jts .jts-lnk:focus-visible{color:var(--ji);outline:none}'
    + '.jts-leg tr[data-jts-open]{cursor:pointer}.jts .jts-leg tr[data-jts-open]:hover td,.jts .jts-leg tr[data-jts-open]:focus-visible td{background:var(--jp2)}.jts-leg tr[data-jts-open]:focus-visible{outline:none}'
    + '.jts-full{margin-top:12px;border:1px solid var(--jl);border-radius:6px;background:var(--jb);min-width:0;overflow:hidden;scroll-margin-top:62px}'
    + '.jts-fnav{display:flex;gap:6px;flex-wrap:wrap;padding:9px;border-bottom:1px solid var(--jl);background:var(--jp2)}.jts-fnav .jts-btn i{font-style:normal;font-weight:500;margin-left:6px;opacity:.75}'
    + '.jts-fhd{display:flex;justify-content:space-between;gap:10px 16px;align-items:center;flex-wrap:wrap;padding:11px 10px 4px}'
    + '.jts-ft1{font:800 15px/1.3 var(--js);color:var(--ji)}.jts-ft1 i{font:500 11px var(--jm);font-style:normal;color:var(--jd);margin-left:8px}'
    + '.jts-fn{padding:0 10px 9px;font:10.5px/1.5 var(--jm);color:var(--jd)}.jts-ftools{display:flex;gap:6px;flex-wrap:wrap;align-items:center}'
    + '.jts .jts-find{width:300px;max-width:100%;height:auto;margin:0;padding:6px 9px;border:1px solid var(--jl);border-radius:4px;background:var(--jp);color:var(--ji);font:11px var(--jm);box-shadow:none}.jts .jts-find:focus{outline:none;border-color:var(--ja)}'
    + '.jts-fwrap{max-height:76vh;overflow:auto;border-top:1px solid var(--jl)}'
    + '.jts .jts-ft{width:100%;margin:0;border:0;border-collapse:separate;border-spacing:0;background:none;font:11.5px var(--jm);font-variant-numeric:tabular-nums}.jts .jts-ft tr{background:none}'
    + '.jts .jts-ft th{position:sticky;top:0;z-index:2;padding:8px 7px;border:0;border-bottom:1px solid var(--jl);background:var(--jp2);color:var(--jd);font:700 9px/1.3 var(--jm);letter-spacing:.7px;text-transform:uppercase;text-align:right;white-space:nowrap;cursor:pointer;user-select:none}'
    + '.jts .jts-ft th:hover,.jts .jts-ft th:focus-visible{color:var(--ja);outline:none}.jts .jts-ft th[aria-sort]{color:var(--ja)}'
    + '.jts .jts-ft td{padding:6px 7px;border:0;border-bottom:1px solid var(--jl);background:var(--jb);color:var(--ji);text-align:right;white-space:nowrap}.jts .jts-ft tbody tr:hover td{background:var(--jp)}'
    + '.jts .jts-ft .l{text-align:left}.jts .jts-ft .jts-fi{width:1%;color:var(--jd);cursor:default}.jts .jts-ft td.up{color:var(--jg)}.jts .jts-ft td.down{color:var(--jr)}'
    + '.jts .jts-ft .jts-fk{position:sticky;left:0;font-weight:700}.jts .jts-ft td.jts-fk{z-index:1}.jts .jts-ft th.jts-fk{z-index:3}.jts .jts-ft td.jts-fk a{color:var(--ja);text-decoration:none}.jts .jts-ft td.jts-fk a:hover{text-decoration:underline}'
    + '.jts-fx{display:inline-block;max-width:176px;overflow:hidden;text-overflow:ellipsis;vertical-align:bottom}.jts-fmore{display:flex;justify-content:center;padding:9px}'
    + '@media (max-width:700px){.jts .jts-find{width:100%}.jts-ftools{width:100%}.jts-fx{max-width:130px}.jts-fwrap{max-height:70vh}.jts-fnav{flex-wrap:nowrap;overflow-x:auto;scrollbar-width:thin}.jts-fnav .jts-btn{flex:none}}'
    + '@media print{html.jts-print .jts-fwrap{max-height:none;overflow:visible}html.jts-print .jts-ftools,html.jts-print .jts-fnav,html.jts-print .jts-go{display:none!important}}'
    /* end of the full-list view */
    + '@media print{html.jts-print body *{visibility:hidden!important}html.jts-print .jts,html.jts-print .jts *{visibility:visible!important}html.jts-print .jts{position:absolute;left:0;top:0;width:100%;max-width:none;margin:0;padding:0}html.jts-print .jts-act,html.jts-print .jts-ctl{display:none}}';

  function style() {
    if (document.getElementById('jts-style')) return;
    var el = document.createElement('style');
    el.id = 'jts-style';
    el.textContent = CSS;
    document.head.appendChild(el);
  }

  /* -- the ring --
     slices: [{name, value (>0), tone: 'pos' | 'up' | 'down' | 'oth', big, small, tip, label}] in drawing order.
     'pos' = one hue stepped by rank (a composition); 'up' solid / 'down' hatched (two sides); 'oth' = the remainder. */
  function ring(r) {
    var slices = (r.slices || []).filter(function (s) { return fin(s.value) && s.value > 0; });
    var total = sum(slices, function (s) { return s.value; });
    if (!slices.length || !(total > 0)) return '';
    var id = 'jts-hatch-' + (++seq), R = 104, RI = 64, ang = -Math.PI / 2, rank = { pos: 0, up: 0, down: 0, oth: 0 };
    var steps = { pos: [1, 0.8, 0.62, 0.48, 0.37, 0.28, 0.22, 0.18], up: [1, 0.74, 0.54, 0.4], down: [1, 0.74, 0.54, 0.4], oth: [0.34, 0.24] };
    function pt(rad, a) { return [(110 + rad * Math.cos(a)).toFixed(2), (110 + rad * Math.sin(a)).toFixed(2)]; }
    var arcs = [], labels = [], legend = [];
    slices.forEach(function (s) {
      var t = steps[s.tone] ? s.tone : 'pos', list = steps[t], op = fin(s.op) ? s.op : s.rest && t !== 'oth' ? 0.3 : list[Math.min(rank[t]++, list.length - 1)];
      var share = s.value / total * 100, sweep = Math.min(s.value / total * Math.PI * 2, Math.PI * 2 - 0.001), a0 = ang, a1 = ang + sweep, large = sweep > Math.PI ? 1 : 0;
      ang = a1;
      var o0 = pt(R, a0), o1 = pt(R, a1), i1 = pt(RI, a1), i0 = pt(RI, a0), mid = pt((R + RI) / 2, (a0 + a1) / 2);
      var shareTxt = (t === 'up' ? '+' : t === 'down' ? MINUS : '') + (share >= 9.95 ? share.toFixed(0) : share.toFixed(1)) + '%';
      arcs.push('<path class="jts-arc ' + t + '" d="M' + o0[0] + ' ' + o0[1] + 'A' + R + ' ' + R + ' 0 ' + large + ' 1 ' + o1[0] + ' ' + o1[1] + 'L' + i1[0] + ' ' + i1[1] + 'A' + RI + ' ' + RI + ' 0 ' + large + ' 0 ' + i0[0] + ' ' + i0[1] + 'Z"'
        + (t === 'down' ? ' fill="url(#' + id + ')"' : '') + ' fill-opacity="' + op + '" data-share="' + share.toFixed(3) + '"><title>' + esc(s.tip || (s.name + ': ' + (s.big || '') + DOT + share.toFixed(1) + '% of the whole')) + '</title></path>');
      if (share >= 6.5) labels.push('<text class="jts-rl" x="' + mid[0] + '" y="' + mid[1] + '"><tspan x="' + mid[0] + '" dy="-1">' + esc(s.label || s.name) + '</tspan><tspan x="' + mid[0] + '" dy="12">' + shareTxt + '</tspan></text>');
      legend.push('<tr' + (s.open ? openAttr(s.open) : '') + '><td><i class="jts-sw ' + t + '" style="opacity:' + op + '"></i><b class="jts-nm">' + esc(s.name) + '</b></td><td class="n"><span class="jts-big ' + (t === 'up' ? 'up' : t === 'down' ? 'down' : '') + '">' + esc(s.big || '') + '</span><span class="jts-s">'
        + esc(s.small || (shareTxt + ' of ' + (r.whole || 'the whole'))) + '</span></td></tr>');
    });
    return '<div class="jts-col jts-ringcol" data-jts-list="ring" data-total="' + total + '"><div class="jts-ch"' + (r.open ? openAttr(r.open) : '') + '><span><b>' + esc(r.title) + '</b> <i>' + esc(r.hint || '') + '</i></span>'
      + (r.open ? '<span class="jts-chr"><span class="jts-n">' + esc(r.n == null ? '' : r.n) + '</span><span class="jts-go">VIEW ALL \u203a</span></span>' : '<span class="jts-n">' + esc(r.n == null ? '' : r.n) + '</span>') + '</div>'
      + '<div class="jts-ring"><svg viewBox="0 0 220 220" role="img" aria-label="' + esc(r.aria || r.title) + '"><defs><pattern id="' + id + '" patternUnits="userSpaceOnUse" width="6" height="6" patternTransform="rotate(45)"><rect class="jts-hbg" width="6" height="6"></rect>'
      + '<line class="jts-hln" x1="0" y1="0" x2="0" y2="6"></line></pattern></defs>' + arcs.join('') + labels.join('') + '<text class="jts-rc" x="110" y="106">' + esc(r.center || '') + '</text>'
      + (r.subs || []).slice(0, 2).map(function (t, i) { return '<text class="jts-rs" x="110" y="' + (121 + i * 13) + '">' + esc(t) + '</text>'; }).join('') + '</svg></div>'
      + '<table class="jts-t jts-leg"><tbody>' + legend.join('') + '</tbody></table>' + (r.caption ? '<p class="jts-rest">' + esc(r.caption) + '</p>' : '') + '</div>';
  }

  /* -- a list opened in full --
     Khalid 2026-11-20: "you are only displaying the first six of each category - when I click on that category I should
     be able to view the entire list and sort them by market cap and by industry, ascending and descending."

     A list whose adapter gives it `full` opens in place of the sheet's body when its heading is clicked:
       full: { cols: [{id, t, type: 's' | 'n', get(row), fmt(row), tone(row), tip, find: 'start', csv(row)}], rows: [...],
               sort: {k, d}, units: ['stock', 'stocks'], href(row), pick(row), file }
     rows arrive in the list's own order (full.sort names it); a click on a column heading ranks by that column, a second
     click reverses it; blanks are last either way and ties keep the list's own order. A model's `more` lists have no
     card and still open (a ring slice's or the ring's own `open`, or a category button; model.order orders the buttons).
     A desk hears a click on a group row (full.pick) as the bubbling event jts:pick. Everything here is pure; mount()
     does the wiring. */
  function fullLists(m) { return (m.lists || []).concat(m.more || []); }
  var FULL_PAGE = 200;
  function fullBlank(v) { return v == null || v === '' || (typeof v === 'number' && !isFinite(v)); }
  function fullKey(c, r) { var v = c.get(r); return fullBlank(v) ? null : c.type === 's' ? String(v).toLowerCase() : (fin(v) ? v : null); }
  function fullSortOf(full, open) { return (open && open.sort) || full.sort || null; }
  function fullRows(full, open) {
    var rows = full.rows || [], q = String((open && open.q) || '').trim().toLowerCase(), s = open && open.sort ? open.sort : null;   /* unranked: the list's own order */
    if (q) {
      var text = full.cols.filter(function (c) { return c.type === 's'; });
      rows = rows.filter(function (r) {
        return text.some(function (c) { var v = c.get(r); if (fullBlank(v)) return false; v = String(v).toLowerCase(); return c.find === 'start' ? v.indexOf(q) === 0 : v.indexOf(q) >= 0; });
      });
    }
    var c = s ? full.cols.filter(function (x) { return x.id === s.k; })[0] : null;
    if (!c) return rows;
    var dec = rows.map(function (r, i) { return { r: r, i: i, v: fullKey(c, r) }; });
    dec.sort(function (a, b) {
      if (a.v === null || b.v === null) return a.v === null && b.v === null ? a.i - b.i : a.v === null ? 1 : -1;
      var cmp = a.v < b.v ? -1 : a.v > b.v ? 1 : 0;
      return (s.d < 0 ? -cmp : cmp) || a.i - b.i;
    });
    return dec.map(function (x) { return x.r; });
  }
  function fullCsv(full, rows) {
    var cell = function (v) { v = v == null ? '' : typeof v === 'number' ? (isFinite(v) ? String(Math.round(v * 1e4) / 1e4) : '') : String(v); return /[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v; };
    return [full.cols.map(function (c) { return cell(c.t); }).join(',')].concat(rows.map(function (r) {
      return full.cols.map(function (c) { return cell(c.csv ? c.csv(r) : c.get(r)); }).join(',');
    })).join('\n');
  }
  function fullWord(full, open) {
    var s = fullSortOf(full, open), c = s ? full.cols.filter(function (x) { return x.id === s.k; })[0] : null;
    return c ? 'ranked by ' + c.t + ', ' + (c.type === 's' ? (s.d < 0 ? 'Z to A' : 'A to Z') : (s.d < 0 ? 'highest first' : 'lowest first')) : 'in the list\u2019s own order';
  }
  /* the part a click on a heading or a keystroke in the find box redraws: the count line, the table, the show-all button */
  /* on a phone the columns come in the adapter's `phone` order (the answer beside the ticker, the long text last) */
  function fullCols(f, narrow) {
    if (!narrow || !f.phone) return f.cols;
    var at = function (c) { var i = f.phone.indexOf(c.id); return i < 0 ? f.phone.length : i; };
    return f.cols.map(function (c, i) { return [c, i]; }).sort(function (a, b) { return (at(a[0]) - at(b[0])) || (a[1] - b[1]); }).map(function (x) { return x[0]; });
  }
  function fullBody(l, open, narrow) {
    var f = l.full, cols = fullCols(f, narrow), rows = fullRows(f, open), s = fullSortOf(f, open) || {}, cap = open.all ? rows.length : Math.min(rows.length, FULL_PAGE), u = f.units || ['row', 'rows'], total = (f.rows || []).length;
    var count = num(rows.length) + (rows.length !== total ? ' of ' + num(total) : '') + ' ' + (total === 1 ? u[0] : u[1]) + DOT + fullWord(f, open) + DOT + 'click a column heading to rank by it, again to reverse';
    var head = '<th class="jts-fi">#</th>' + cols.map(function (c, j) {
      var on = s.k === c.id;
      return '<th class="' + (c.type === 's' ? 'l' : '') + (j === 0 ? ' jts-fk' : '') + '" data-jts-sort="' + esc(c.id) + '" tabindex="0"' + (on ? ' aria-sort="' + (s.d < 0 ? 'descending' : 'ascending') + '"' : '')
        + ' title="' + esc((c.tip ? c.tip + DOT : '') + 'click to rank by this column, again to reverse') + '">' + esc(c.t) + '<span class="jts-ar">' + (on ? (s.d < 0 ? ' \u25be' : ' \u25b4') : '') + '</span></th>';
    }).join('');
    var body = rows.slice(0, cap).map(function (r, i) {
      return '<tr><td class="jts-fi">' + (i + 1) + '</td>' + cols.map(function (c, j) {
        var txt = esc(c.fmt ? c.fmt(r) : c.get(r)), t = c.tone ? c.tone(r) : '', inner = txt;
        var to = j === 0 && f.href ? f.href(r) : '';
        if (to) inner = '<a href="' + esc(to) + '" target="_top">' + txt + '</a>';
        else if (j === 0 && f.pick) inner = '<button type="button" class="jts-lnk" data-jts-pick="' + i + '" title="' + esc(f.pickTip || 'Show these names') + '">' + txt + '</button>';
        else if (c.type === 's') inner = '<span class="jts-fx" title="' + txt + '">' + txt + '</span>';
        return '<td class="' + (c.type === 's' ? 'l' : '') + (j === 0 ? ' jts-fk' : '') + (t ? ' ' + t : '') + '">' + inner + '</td>';
      }).join('') + '</tr>';
    }).join('');
    return '<div class="jts-fn" data-jts-count>' + esc(count) + '</div>'
      + (rows.length ? '<div class="jts-fwrap"><table class="jts-ft"><thead><tr>' + head + '</tr></thead><tbody>' + body + '</tbody></table></div>'
        : '<p class="jts-none">' + (total ? 'Nothing in this list matches \u201c' + esc(open.q) + '\u201d.' : esc(l.none || 'none')) + '</p>')
      + (rows.length > cap ? '<div class="jts-fmore"><button type="button" class="jts-btn" data-jts-act="all">SHOW ALL ' + num(rows.length) + ' ROWS</button></div>' : '')
      + (l.note ? '<p class="jts-rest">' + esc(l.note) + '</p>' : '');
  }
  function fullHtml(m, l, open, narrow) {
    var order = m.order || [], at = function (x) { var i = order.indexOf(x.key); return i < 0 ? order.length : i; };
    var can = fullLists(m).filter(function (x) { return x.full; }).map(function (x, i) { return [x, i]; }).sort(function (a, b) { return (at(a[0]) - at(b[0])) || (a[1] - b[1]); }).map(function (x) { return x[0]; });
    return '<div class="jts-full" data-jts-full="' + esc(l.key) + '"><div class="jts-fnav">' + can.map(function (x) {
      return '<button type="button" class="jts-btn" data-jts-open="' + esc(x.key) + '" aria-pressed="' + (x.key === l.key ? 'true' : 'false') + '">' + esc(x.short || x.title) + '<i>' + num((x.full.rows || []).length) + '</i></button>';
    }).join('') + '</div><div class="jts-fhd"><div class="jts-ft1">' + esc(l.title) + ' <i>' + esc(l.hint || '') + '</i></div><div class="jts-ftools">'
      + '<input type="search" class="jts-find" data-jts-find placeholder="' + esc(l.full.find || 'Find in this list') + '" aria-label="Find in this list" value="' + esc(open.q || '') + '">'
      + '<button type="button" class="jts-btn" data-jts-act="csv">DOWNLOAD CSV</button><button type="button" class="jts-btn" data-jts-act="close">\u2039 BACK TO THE TEARSHEET</button></div></div>'
      + '<div data-jts-fbody>' + fullBody(l, open, narrow) + '</div></div>';
  }
  function openAttr(key) { return ' data-jts-open="' + esc(key) + '" role="button" tabindex="0" title="Open the whole list: rank it by any column, either way"'; }

  /* -- every list opens in full (kit 1.7.0) --
     Khalid 2026-11-20: "now do the same with Volatility - most and least volatile stocks, price spread - and all the
     other engines that aren't showing the whole list."

     An adapter writes a card's rows as FL(list, row, S, k, d): list.map(row) for the card, and the whole list for the
     full view - S is a sheet() of typed columns over the adapter's own records (tx text, nx number), k / d the column
     and direction the list already stands in. hidden() is a list with no card (a ring's whole, a ring slice's members).
     finish() runs on every model an adapter builds: it moves each list's whole onto the list as `full`, and a list
     written without columns still opens, on the four things every card row carries (name, detail, figure, note) with
     the figure ranked by its value. Nothing here adds a row the feed does not carry. */
  function getter(g) { return typeof g === 'function' ? g : function (r) { return r == null ? null : r[g]; }; }
  function tx(id, t, g, o) { return Object.assign({ id: id, t: t, type: 's', get: getter(g) }, o || {}); }
  function nx(id, t, g, f, o) {
    var get = getter(g), c = Object.assign({ id: id, t: t, type: 'n', get: get, fmt: function (r) { var v = get(r); return fin(v) ? f(v) : D; } }, o || {});
    if (c.tone === true) c.tone = function (r) { return tone(get(r)); };
    return c;
  }
  function fP(dp, signed) { return function (v) { return pct(+v.toFixed(dp == null ? 1 : dp) || 0, dp, signed); }; }   /* a move that rounds to nothing carries no sign */
  function fN(dp) { return function (v) { return num(v, dp); }; }
  function fS(dp, unit) { return function (v) { var z = +v.toFixed(dp == null ? 1 : dp) || 0; return sign(z, true) + Math.abs(z).toFixed(dp == null ? 1 : dp) + (unit || ''); }; }
  function fNs(dp) { return function (v) { return sign(v, true) + num(Math.abs(v), dp); }; }
  var fM = function (v) { return money(v); }, fMs = function (v) { return money(v, true); }, fX = function (v) { return v.toFixed(1) + '\u00d7'; }, fPx = function (v) { return num(v, Math.abs(v) < 10 ? 3 : 2); };
  var chartOf = function (t) { return t ? '/chart.html?s=' + encodeURIComponent(t) : ''; };
  function sheet(cols, units, o) { return Object.assign({ cols: cols, units: units }, o || {}); }
  function wholeOf(list, S, k, d, o) { return Object.assign({}, S, { rows: list, sort: k ? { k: k, d: d || -1 } : null }, o || {}); }
  function FL(list, row, S, k, d, o) {
    var out = list.map(function (r, i) {
      var x = row(r, i, list);
      if (x && typeof x === 'object' && !x.__r) { try { Object.defineProperty(x, '__r', { value: r }); } catch (e) { /* a frozen row keeps the list's own place */ } }
      return x;
    });
    Object.defineProperty(out, 'full', { value: wholeOf((o && o.rows) || list, S, k, d, o) });
    return out;
  }
  /* a copy of the list ranked by a figure, blanks last, ties in the list's own order */
  function sorted(list, f, desc) {
    return list.map(function (r, i) { var v = f(r); return { r: r, i: i, v: fin(v) ? v : null }; }).sort(function (a, b) {
      if (a.v === null || b.v === null) return a.v === null && b.v === null ? a.i - b.i : a.v === null ? 1 : -1;
      return (desc ? b.v - a.v : a.v - b.v) || a.i - b.i;
    }).map(function (x) { return x.r; });
  }
  function hidden(key, title, hint, list, S, k, d, o) { return { key: key, title: title, hint: hint, card: false, rows: [], full: wholeOf(list, S, k, d, o) }; }
  /* the value of a figure as a card prints it: $1.2B, \u22123.4%, 2.1\u00d7, +3.4 pts, 1,234 */
  function figure(s) {
    var m = /^([+\u2212-]?)\s*\$?\s*(\d+(?:\.\d+)?)\s*([KMBT]?)\s*(%|\u00d7|x|pts|pp|bp)?\+?$/.exec(String(s == null ? '' : s).replace(/,/g, '').trim());
    if (!m) return null;
    var v = parseFloat(m[2]) * ({ K: 1e3, M: 1e6, B: 1e9, T: 1e12 }[m[3]] || 1);
    return m[1] === '\u2212' || m[1] === '-' ? -v : v;
  }
  function autoFull(l) {
    var rows = l.rows || [], has = function (k) { return rows.some(function (x) { return !fullBlank(x[k]) && x[k] !== D; }); }, H = l.heads || [];
    var said = rows.filter(function (x) { return !fullBlank(x.big) && x.big !== D; }), numeric = said.length > 0 && said.every(function (x) { return figure(x.big) !== null; });
    var cols = [tx('name', H[0] || 'Name', 'name')];
    if (has('sub')) cols.push(tx('sub', H[1] || 'Detail', 'sub'));
    if (said.length) cols.push(numeric ? { id: 'big', t: H[2] || 'Figure', type: 'n', get: function (x) { return figure(x.big); }, fmt: function (x) { return x.big; }, tone: function (x) { return x.tone || ''; }, csv: function (x) { return x.big; } }
      : tx('big', H[2] || 'Figure', 'big', { tone: function (x) { return x.tone || ''; } }));
    if (has('small')) cols.push(tx('small', H[3] || 'Note', 'small'));
    return { cols: cols, rows: rows, sort: null, units: l.units || ['row', 'rows'] };
  }
  function finish(m) {
    if (!m || m.hide) return m;
    fullLists(m).forEach(function (l) {
      if (!l.full && l.rows && l.rows.full) l.full = l.rows.full;
      if (!l.full && (l.rows || []).length) l.full = autoFull(l);
      /* a card drawn row for row from its whole list (the strength sheet) learns which record each row came from, as FL teaches the rest */
      var fr = l.full && l.full.rows, cr = l.rows || [];
      if (fr && cr.length && cr.length === fr.length) cr.forEach(function (x, i) {
        if (x && typeof x === 'object' && !x.__r) { try { Object.defineProperty(x, '__r', { value: fr[i] }); } catch (e) { /* a frozen row keeps its place */ } }
      });
    });
    return m;
  }

  /* -- which names come first (kit 1.8.0). Khalid 2026-11-23: "the leaders and largest market caps should show up first at the
     top of the engines they get first priority. everything stay im talking view mode and how different stocks are arranged in
     view". A list of stocks is arranged with the industry leaders first (the Leaders desk's own set: the five largest companies
     of each industry), largest first, then every other name by market value, largest first; a name with no market value keeps
     its place after them, in the desk's own order. Nothing leaves a list and no figure changes. The desk's own order is one
     button away and the choice is kept in the browser. -- */
  var PRIO_KEY = 'jh-stock-order', PRIO_CACHE = 'jh-leaders-index-v1', PRIO_DESKS = { leaders: 1, wyckoff: 1, capitulation: 1, volatility: 1, strength: 1 }, PRIO_HEAD = 15, PRIO_RANKED = /^(newest|latest|strongest|weakest|deepest|heaviest|longest|largest|most|least|widest|narrowest|best|worst|highest|lowest|biggest|top)\b/i, prioIndex = null, prioAsked = false;
  function prioStore() { try { return typeof window !== 'undefined' ? window.localStorage : null; } catch (e) { return null; } }
  function prioMode() { var s = prioStore(), v = null; try { v = s && s.getItem(PRIO_KEY); } catch (e) { v = null; } return v === 'engine' ? 'engine' : 'priority'; }
  function prioEvent(name) { try { window.dispatchEvent(new CustomEvent(name)); } catch (e) { /* no window: nothing listens */ } }
  function prioSet(mode) {
    var s = prioStore();
    try { if (s) s.setItem(PRIO_KEY, mode === 'engine' ? 'engine' : 'priority'); } catch (e) { /* private mode: the choice lasts for the page */ }
    prioEvent('jh:stock-order');
  }
  function prioFromDoc(doc) {
    var out = { as_of: (doc && doc.as_of) || null, cap: {} }, n = 0;
    ((doc && doc.rows) || []).forEach(function (r) { if (r && r.ticker) { out.cap[String(r.ticker).toUpperCase()] = fin(r.mcap) ? r.mcap : 0; n++; } });
    return n ? out : null;
  }
  /* the leaders: from the Leaders desk's feed when the page already holds it, else from the copy this browser kept (twenty hours at
     most), else read once in the background; lists are arranged again when it lands */
  function prioLeaders() {
    if (typeof window === 'undefined') return prioIndex;
    if (window.JHLeadersDoc && (!prioIndex || prioIndex.doc !== window.JHLeadersDoc)) { var live = prioFromDoc(window.JHLeadersDoc); if (live) { live.doc = window.JHLeadersDoc; prioIndex = live; } }
    if (prioIndex) return prioIndex;
    var s = prioStore(), kept = null;
    try { kept = s && JSON.parse(s.getItem(PRIO_CACHE) || 'null'); } catch (e) { kept = null; }
    var fresh = !!(kept && kept.cap && kept.saved && (Date.now() - kept.saved) < 72e6);
    if (kept && kept.cap) prioIndex = kept;
    if (!fresh && !prioAsked && typeof fetch === 'function') {
      prioAsked = true;
      fetch('/data/leaders.json').then(function (r) { return r.ok ? r.json() : null; }).then(function (doc) {
        var idx = prioFromDoc(doc);
        if (!idx) return;
        idx.saved = Date.now(); prioIndex = idx;
        try { if (s) s.setItem(PRIO_CACHE, JSON.stringify(idx)); } catch (e) { /* no room: read again on the next visit */ }
        prioEvent('jh:leaders-index');
      }).catch(function () { /* until it can be read the lists stand by market value alone */ });
    }
    return prioIndex;
  }
  function prioTicker(r) { var t = r && (r.ticker || r.t || r.symbol || r.sym); return typeof t === 'string' && t ? t.toUpperCase() : null; }
  function prioCap(r) { var v = r ? (r.mcap != null ? r.mcap : r.market_cap != null ? r.market_cap : r.mc != null ? r.mc : r.cap) : null; return fin(v) && v > 0 ? v : null; }
  function prioIsLeader(t, idx) { return !!(t && idx && idx.cap && Object.prototype.hasOwnProperty.call(idx.cap, t)); }
  /* rows arranged: leaders (largest first), then the rest by market value, then names with none in their own order.
     get = { ticker(row), cap(row) } for a table with its own record shape; idx = the leaders (a test passes its own) */
  function prioOrder(rows, get, idx) {
    idx = idx === undefined ? prioLeaders() : idx; get = get || {};
    var tk = get.ticker || prioTicker, cp = get.cap || prioCap;
    return (rows || []).map(function (r, i) {
      var t = tk(r), c = cp(r), lead = prioIsLeader(t, idx);
      if (c == null && lead && idx.cap[t] > 0) c = idx.cap[t];
      return { r: r, i: i, g: lead ? 0 : c != null ? 1 : 2, c: c || 0 };
    }).sort(function (a, b) { return (a.g - b.g) || (a.g < 2 ? b.c - a.c : 0) || (a.i - b.i); }).map(function (x) { return x.r; });
  }
  /* a model's lists of stocks arranged (mode 'priority') or put back as the desk built them (mode 'engine'); m.stockOrder says
     which, or stays unset when the sheet lists no stock */
  function prioModel(m, mode, idx) {
    if (!m || m.hide) return m;
    idx = idx === undefined ? prioLeaders() : idx;
    var any = false, stock = function (r) { var t = prioTicker(r); return !!t && (prioCap(r) != null || prioIsLeader(t, idx)); };
    var own = function (l) { return l.full ? (l.full.own || l.full) : null; }, seen = {};
    /* a card whose title is a ranking (Strongest, Newest, Heaviest ...), or one of two lists that hold the same names, keeps what
       it is about: its first fifteen by the desk's own ranking, and those fifteen are arranged - else every such card would show
       the same largest names. A list that is a set of its own (under accumulation, volatility rising) is arranged whole. Opened
       in full, every list is arranged whole; a click on a column ranks it by that column as before. */
    var sig = function (l) { var f = own(l); return f && (f.rows || []).length > PRIO_HEAD ? (f.rows || []).map(function (r) { return prioTicker(r) || ''; }).sort().join(',') : null; };
    fullLists(m).forEach(function (l) { var k = sig(l); if (k) seen[k] = (seen[k] || 0) + 1; });
    var arrange = function (rows) {
      var at = new Map();
      prioOrder(rows.map(function (x) { return x.__r; }), null, idx).forEach(function (r, i) { if (!at.has(r)) at.set(r, i); });
      return rows.map(function (x, i) { return { x: x, i: i, p: at.get(x.__r) }; }).sort(function (a, b) { return (a.p - b.p) || (a.i - b.i); }).map(function (o) { return o.x; });
    };
    fullLists(m).forEach(function (l) {
      var full = own(l), card = l.ownRows || l.rows || [];
      if (!full || !(full.rows || []).some(stock)) return;
      any = true;
      if (mode !== 'priority') { l.full = full; l.rows = card; return; }
      l.full = Object.assign({}, full, { rows: prioOrder(full.rows, null, idx), sort: null, own: full });
      if (card.length && card.every(function (x) { return x && x.__r; })) {
        var k = sig(l), ranking = PRIO_RANKED.test(String(l.title || '')) || !!(k && seen[k] > 1 && card.length === (full.rows || []).length);
        l.ownRows = card;
        l.rows = ranking ? arrange(card.slice(0, PRIO_HEAD)).concat(card.slice(PRIO_HEAD)) : arrange(card);
      }
    });
    if (any) m.stockOrder = mode === 'priority' ? 'priority' : 'engine';
    return m;
  }

  function render(el, m) {
    style();
    el.__model = m;
    var limit = el.__long ? 15 : (el.clientWidth && el.clientWidth < 520 ? 3 : 6);   /* a phone stacks the lists in one column: three rows each, MORE ROWS for the rest */
    var lists = fullLists(m), cards = (m.lists || []).filter(function (l) { return l.card !== false; });
    var open = el.__open || null, ol = open ? lists.filter(function (l) { return l.key === open.key && l.full; })[0] : null;
    if (open && !ol) el.__open = open = null;                                         /* this scope has no such list: back to the sheet */
    function col(l) {
      var rows = l.rows || [], shown = rows.slice(0, limit), rest = rows.slice(limit), can = !!l.full, all = can ? (l.full.rows || []).length : 0, count = l.n != null ? esc(l.n) : can ? all : rows.length;
      return '<div class="jts-col ' + (l.tone || '') + '" data-jts-list="' + esc(l.key || '') + '"><div class="jts-ch"' + (can ? openAttr(l.key) : '') + '><span><b>' + esc(l.title) + '</b> <i>' + esc(l.hint || '') + '</i></span>'
        + (can ? '<span class="jts-chr"><span class="jts-n">' + count + '</span><span class="jts-go">VIEW ALL \u203a</span></span>' : '<span class="jts-n">' + count + '</span>') + '</div>'
        + (shown.length ? '<table class="jts-t"><tbody>' + shown.map(function (x) {
          return '<tr><td><b class="jts-nm">' + esc(x.name) + '</b><span class="jts-s">' + esc(x.sub || '') + '</span></td><td class="n"><span class="jts-big ' + (x.tone || '') + '">' + esc(x.big == null ? '' : x.big) + '</span><span class="jts-s">' + esc(x.small || '') + '</span></td></tr>';
        }).join('') + '</tbody></table>' : '<p class="jts-none">' + esc(l.none || 'none') + '</p>')
        + (rest.length ? '<p class="jts-rest">The other ' + rest.length + ': ' + rest.slice(0, 12).map(function (x) { return esc(String(x.name).split(DOT)[0]) + (x.big ? ' (' + esc(x.big) + ')' : ''); }).join(', ') + (rest.length > 12 ? ' and ' + (rest.length - 12) + ' more' : '')
          + (can ? '. <button type="button" class="jts-lnk" data-jts-open="' + esc(l.key) + '">View all ' + num(all) + '</button>' : '') + '</p>'
          : (can && all > shown.length ? '<p class="jts-rest"><button type="button" class="jts-lnk" data-jts-open="' + esc(l.key) + '">View all ' + num(all) + '</button></p>' : ''))
        + (l.note ? '<p class="jts-rest">' + esc(l.note) + '</p>' : '') + '</div>';
    }
    var long = !ol && cards.some(function (l) { return (l.rows || []).length > Math.min(limit, 6); });
    /* a repaint (the desk hands a fresh feed every minute) must not take the reader's place in an open list */
    var was = ol ? el.querySelector('.jts-fwrap') : null, scroll = was ? [was.scrollTop, was.scrollLeft] : null, act = typeof document !== 'undefined' ? document.activeElement : null;
    var caret = act && act.hasAttribute && act.hasAttribute('data-jts-find') && el.contains(act) ? [act.selectionStart, act.selectionEnd] : null;
    el.classList.add('jts');
    el.innerHTML = '<div class="jts-in"><div class="jts-top"><div class="jts-eye">' + esc(m.eyebrow) + '</div><div class="jts-act">' + (long ? '<button type="button" class="jts-btn" data-jts-act="more" aria-pressed="' + (el.__long ? 'true' : 'false') + '">' + (el.__long ? 'FEWER ROWS' : 'MORE ROWS') + '</button>' : '')
      + '<button type="button" class="jts-btn" data-jts-act="link">COPY LINK</button><button type="button" class="jts-btn" data-jts-act="print">PRINT' + DOT + 'SAVE PDF</button></div></div>'
      + '<div class="jts-h" role="heading" aria-level="2">' + esc(m.title) + '</div><div class="jts-sub">' + esc(m.sub || '') + '</div>'
      + ((m.controls || []).length ? '<div class="jts-ctl">' + m.controls.map(function (c) { return '<button type="button" class="jts-btn" data-jts-ctl="' + esc(c.id) + '" aria-pressed="' + (c.on ? 'true' : 'false') + '">' + esc(c.label) + '</button>'; }).join('') + '</div>' : '')
      + (m.stockOrder ? '<div class="jts-ctl jts-ord" data-jts-ord="' + m.stockOrder + '"><button type="button" class="jts-btn" data-jts-order="priority" aria-pressed="' + (m.stockOrder === 'priority' ? 'true' : 'false') + '" title="Every list: the industry leaders first, largest first, then every other name by market value. Nothing is left out.">LEADERS' + DOT + 'LARGEST FIRST</button>'
        + '<button type="button" class="jts-btn" data-jts-order="engine" aria-pressed="' + (m.stockOrder === 'engine' ? 'true' : 'false') + '" title="Every list in the order this desk ranks it.">THE DESK\u2019S OWN ORDER</button><span class="jts-s" style="align-self:center">' + (m.stockOrder === 'priority' ? 'Each list of names starts with its largest. Nothing is left out; every figure is the desk\u2019s own.' : 'Each list in the order this desk ranks it.') + '</span></div>' : '')
      + '<div class="jts-score">' + (m.tiles || []).map(function (t) { return '<div class="jts-kpi"><div class="k">' + esc(t.k) + '</div><div class="v">' + t.v + '</div><div class="s">' + esc(t.s || '') + '</div></div>'; }).join('') + '</div>'
      + (ol ? fullHtml(m, ol, open, !!(el.clientWidth && el.clientWidth < 520))
        : ((m.cards || []).length ? '<div class="jts-cards">' + m.cards.map(function (c) {
          return '<div class="jts-card ' + (c.tone || '') + '"><div class="big ' + (c.tone || '') + '">' + esc(c.big) + '</div><div><div class="e">' + esc(c.eyebrow) + '</div><div class="t">' + esc(c.title) + '</div><p>' + esc(c.text) + '</p></div></div>';
        }).join('') + '</div>' : '')
        + ((m.notes || []).length ? '<div class="jts-notes">' + m.notes.map(function (n) { return '<p><b>' + esc(n[0]) + '</b> ' + esc(n[1]) + '</p>'; }).join('') + '</div>' : '')
        + '<div class="jts-cols">' + (m.ring ? ring(m.ring) : '') + cards.map(col).join('') + '</div>')
      + '<p class="jts-foot">' + esc(m.foot || '') + '</p></div>';
    /* a phone keeps the lists' buttons on one row that slides: the open one stands at its start */
    var nav = ol ? el.querySelector('.jts-fnav') : null, on = nav ? nav.querySelector('[aria-pressed="true"]') : null;
    if (on && nav.scrollWidth > nav.clientWidth) nav.scrollLeft = Math.max(0, on.offsetLeft - nav.offsetLeft - 9);
    if (scroll) { var now = el.querySelector('.jts-fwrap'); if (now) { now.scrollTop = scroll[0]; now.scrollLeft = scroll[1]; } }
    if (caret) { var box = el.querySelector('[data-jts-find]'); if (box) { box.focus(); try { box.setSelectionRange(caret[0], caret[1]); } catch (e) { /* a search box may refuse a range */ } } }
    el.dataset.state = 'ready';
    el.dataset.open = ol ? ol.key : '';
  }

  /* -- adapters: one per desk, each reading that desk's own published feed -- */
  var ADAPTERS = {};

  ADAPTERS.ark = {
    feeds: ['/data/ark-holdings.json'],
    build: function (docs, st) {
      /* Engine 2.1.0 publishes every move (diff_vs_prev.all), the holdings dates on both sides and whether the diff
         is carried (no newer holdings since). An older feed has only its 15 largest moves; the sheet then says so. */
      var d = docs[0], by_fund = d.holdings_by_fund || {}, funds = (d.funds_tracked || Object.keys(by_fund)).filter(function (f) { return (by_fund[f] || []).length; });
      var fund = st.fund && by_fund[st.fund] ? st.fund : 'ALL', diff = d.diff_vs_prev || {}, hi = diff.highlights || {}, all = diff.all || null, complete = !!all;
      var pos;
      if (fund === 'ALL') {
        var agg = {};
        funds.forEach(function (f) { by_fund[f].forEach(function (h) { var a = agg[h.ticker || h.company] || (agg[h.ticker || h.company] = { ticker: h.ticker, company: h.company, value: 0, funds: [] }); a.value += h.market_value || 0; a.funds.push(f); }); });
        pos = Object.keys(agg).map(function (k) { return agg[k]; });
      } else pos = by_fund[fund].map(function (h) { return { ticker: h.ticker, company: h.company, value: h.market_value || 0, funds: [fund] }; });
      var total = sum(pos, function (p) { return p.value; });
      pos.sort(by(function (p) { return p.value; }, true));
      pos.forEach(function (p) { p.w = total ? p.value / total * 100 : 0; });
      var mine = function (rows) { return (rows || []).filter(function (x) { return fund === 'ALL' || x.fund === fund; }); };
      var px = function (x) { return x.current_shares ? (x.current_value || 0) / x.current_shares : 0; };
      var adds = mine(complete ? all.position_adds : hi.biggest_adds).map(function (x) { return { x: x, usd: (x.shares_added || 0) * px(x) }; }).sort(by(function (r) { return r.usd; }, true));
      var trims = mine(complete ? all.position_trims : hi.biggest_trims).map(function (x) { return { x: x, usd: (x.shares_sold || 0) * px(x) }; }).sort(by(function (r) { return r.usd; }, true));
      var opened = mine(complete ? all.new_positions : hi.new_positions).slice().sort(by(function (x) { return x.market_value || 0; }, true)), closed = mine(complete ? all.closed_positions : hi.closed_positions).slice().sort(by(function (x) { return x.prev_value || 0; }, true));
      var label = function (x) { return (x.ticker || '?') + (x.company ? DOT + x.company : ''); };
      var nAll = d.n_positions_total || sum(funds, function (f) { return by_fund[f].length; }), scoped = complete || fund === 'ALL';
      /* counts: the whole family from the feed; one fund from the complete lists (only an engine that publishes them can say) */
      var n = fund === 'ALL' ? { opened: diff.n_new_positions || 0, adds: diff.n_position_adds || 0, trims: diff.n_position_trims || 0, closed: diff.n_closed_positions || 0, lines: nAll }
        : complete ? { opened: opened.length, adds: adds.length, trims: trims.length, closed: closed.length, lines: pos.length }
          : { opened: diff.n_new_positions || 0, adds: diff.n_position_adds || 0, trims: diff.n_position_trims || 0, closed: diff.n_closed_positions || 0, lines: nAll };
      var unchanged = n.lines - n.opened - n.adds - n.trims, where = fund === 'ALL' || !complete ? 'fund-lines, all funds' : 'lines in ' + fund;
      var top10 = sum(pos.slice(0, 10), function (p) { return p.w; }), multi = pos.filter(function (p) { return p.funds.length > 1; }).sort(by(function (p) { return p.funds.length; }, true))[0];
      var prev = day(d.prev_snapshot_at), now = day(d.generated_at), fundDate = function (f) { return (by_fund[f] || []).reduce(function (m, h) { var x = day(h.date); return x > m ? x : m; }, ''); };
      var held = day(d.holdings_date) || funds.reduce(function (m, f) { var x = fundDate(f); return x > m ? x : m; }, ''), base = day(d.prev_holdings_date), since = base ? 'the ' + base + ' holdings' : 'the last snapshot';
      var stale = funds.filter(function (f) { var x = fundDate(f); return x && held && x < held; }), listed = adds.length + trims.length + opened.length + closed.length;
      var who = fund === 'ALL' ? 'ARK Invest, all ' + funds.length + ' funds' : fund + (d.fund_descriptions && d.fund_descriptions[fund] ? ' (' + String(d.fund_descriptions[fund]).replace(/\s*\(.*$/, '') + ')' : '');
      var top6 = pos.slice(0, 6), restV = total - sum(top6, function (p) { return p.value; });
      var card = function (r, kind) {
        if (!r) return { tone: kind, big: D, eyebrow: 'Most notable ' + (kind === 'up' ? 'buy' : 'sale'), title: complete ? 'None' : 'None in the day\u2019s largest moves', text: 'No ' + (kind === 'up' ? 'addition' : 'cut') + (complete ? ' of 1% or more' : ' is listed') + ' for ' + (fund === 'ALL' ? 'any fund' : fund) + ' against ' + since + '.' };
        var x = r.x, sh = kind === 'up' ? x.shares_added : x.shares_sold;
        return { tone: kind, big: pct(x.pct_change, Math.abs(x.pct_change) >= 100 ? 0 : 1, true), eyebrow: 'Most notable ' + (kind === 'up' ? 'buy' : 'sale') + DOT + x.fund, title: label(x),
          text: x.fund + (kind === 'up' ? ' added ' : ' sold ') + num(sh) + ' shares, about ' + money(r.usd) + ' at the reported price; it now holds ' + num(x.current_shares) + ' shares worth ' + money(x.current_value) + ', ' + pct(x.weight, 2) + ' of the fund.' };
      };
      var moveRow = function (r, kind) { var x = r.x; return { name: label(x), sub: x.fund + DOT + (kind === 'up' ? '+' : MINUS) + num(kind === 'up' ? x.shares_added : x.shares_sold) + ' sh' + DOT + 'now ' + pct(x.weight, 2) + ' of the fund', big: (kind === 'up' ? '+' : MINUS) + money(r.usd), tone: kind, small: pct(x.pct_change, 1, true) + ' shares' }; };
      var plain = function (x, kind) {
        var v = x.current_value != null ? x.current_value : x.market_value != null ? x.market_value : x.prev_value, sh = x.current_shares != null ? x.current_shares : x.shares != null ? x.shares : x.prev_shares;
        return { name: label(x), sub: (x.fund || '') + (x.weight != null ? DOT + pct(x.weight, 2) + ' of the fund' : kind === 'down' ? DOT + 'held before' : ''), big: money(v), tone: kind, small: num(sh) + ' sh' };
      };
      /* the full view's columns: a line of a fund, a move in one, a position */
      var valOf = function (x) { return x.current_value != null ? x.current_value : x.market_value != null ? x.market_value : x.prev_value; }, shOf = function (x) { return x.current_shares != null ? x.current_shares : x.shares != null ? x.shares : x.prev_shares; };
      var LINE = sheet([tx('t', 'Ticker', 'ticker', { find: 'start' }), tx('co', 'Company', 'company'), tx('fund', 'Fund', 'fund'), nx('val', 'Value', valOf, fM), nx('sh', 'Shares', shOf, fN(0)), nx('w', '% of the fund', 'weight', fP(2))],
        ['line', 'lines'], { href: function (x) { return chartOf(x.ticker); }, find: 'Find a ticker, company or fund' });
      var MOVE = function (s) {
        return sheet([tx('t', 'Ticker', function (r) { return r.x.ticker; }, { find: 'start' }), tx('co', 'Company', function (r) { return r.x.company; }), tx('fund', 'Fund', function (r) { return r.x.fund; }),
          nx('usd', 'Dollars moved', function (r) { return s * r.usd; }, fMs, { tone: true, tip: 'shares added or sold x the position\u2019s reported price that day' }),
          nx('dsh', 'Shares moved', function (r) { var v = s > 0 ? r.x.shares_added : r.x.shares_sold; return fin(v) ? s * v : null; }, fNs(0), { tone: true }), nx('chg', 'Change in shares', function (r) { return r.x.pct_change; }, fP(1, true), { tone: true }),
          nx('sh', 'Shares now', function (r) { return r.x.current_shares; }, fN(0)), nx('val', 'Value now', function (r) { return r.x.current_value; }, fM), nx('w', '% of the fund', function (r) { return r.x.weight; }, fP(2))],
          ['line', 'lines'], { href: function (r) { return chartOf(r.x.ticker); }, find: 'Find a ticker, company or fund', phone: ['t', 'usd', 'chg', 'fund', 'w', 'co'] });
      };
      var POSN = sheet([tx('t', 'Ticker', 'ticker', { find: 'start' }), tx('co', 'Company', 'company'), nx('val', 'Value', 'value', fM), nx('w', '% of the value', 'w', fP(2)), nx('nf', 'Funds', function (p) { return p.funds.length; }, fN(0)), tx('in', 'Held in', function (p) { return p.funds.join(', '); })],
        ['position', 'positions'], { href: function (p) { return chartOf(p.ticker); }, find: 'Find a ticker, company or fund' });
      var counts = n.adds + ' added to, ' + n.trims + ' cut, ' + n.opened + ' opened, ' + n.closed + ' closed since ' + since;
      return {
        eyebrow: 'ARK tearsheet' + DOT + (held && base ? 'holdings ' + held + ' vs ' + base : 'snapshot ' + now + ' vs ' + prev), title: who + ': ' + money(total) + ' across ' + pos.length + ' positions' + DOT + (scoped ? counts : listed + ' of the day\u2019s largest moves are in this fund'),
        sub: 'Daily holdings as ARK publishes them' + (held ? ', dated ' + held : '') + DOT + 'positions for ' + (fund === 'ALL' ? 'all funds, one row per ticker' : fund) + DOT + (fund === 'ALL' || !complete ? 'changes counted across all ' + funds.length + ' funds' : 'changes in ' + fund + ' only')
          + (d.diff_is_carried ? DOT + 'nothing newer has been published since, so these are still that day\u2019s moves' : ''),
        controls: [{ id: 'ALL', label: 'ALL FUNDS', on: fund === 'ALL' }].concat(funds.map(function (f) { return { id: f, label: f, on: fund === f }; })),
        tiles: [{ k: 'Reported value', v: money(total), s: fund === 'ALL' ? funds.length + ' funds' : fund }, { k: 'Line items', v: num(pos.length), s: fund === 'ALL' ? num(nAll) + ' fund-lines, ' + pos.length + ' tickers' : 'positions in the fund' },
          { k: 'In top 10', v: pct(top10, 0), s: 'of the reported value' }, { k: 'New / increased', v: '<span class="up">' + n.opened + '</span> / <span class="up">' + n.adds + '</span>', s: where },
          { k: 'Reduced / closed', v: '<span class="down">' + n.trims + '</span> / <span class="down">' + n.closed + '</span>', s: where }, { k: 'Unchanged', v: num(Math.max(unchanged, 0)), s: 'share count within 1% of ' + (base || prev) }],
        cards: [card(trims[0], 'down'), card(adds[0], 'up')],
        notes: [['Largest holding:', pos[0] ? label(pos[0]) + ', ' + money(pos[0].value) + ' (' + pct(pos[0].w) + ')' + (pos[0].funds.length > 1 ? ' across ' + pos[0].funds.join(', ') : '') + '.' : D],
          stale.length ? ['Stale at the source:', stale.map(function (f) { return f + '\u2019s holdings are dated ' + fundDate(f); }).join('; ') + ', not ' + held + ': the source has not updated ' + (stale.length > 1 ? 'them' : 'it') + ', so ' + (stale.length > 1 ? 'their' : 'its') + ' lines cannot show a change.']
            : ['Held by the most funds:', multi ? label(multi) + ' sits in ' + multi.funds.length + ' funds (' + multi.funds.join(', ') + ').' : 'no ticker is held by more than one fund in this view.'],
          ['Caveat:', 'a share count that rises on every line of a fund on the same day is usually new money entering the fund (creation units), not a fresh call on each name; the percentage beside each move is the change in shares.']],
        ring: { open: 'positions', title: 'Current positions', hint: 'top 6 of ' + pos.length, n: pos.length, center: money(total), subs: ['REPORTED VALUE', 'TOP 10 = ' + pct(top10, 0)], whole: 'the value', aria: 'Positions by reported value: ' + top6.map(function (p) { return p.ticker + ' ' + pct(p.w, 0); }).join(', '),
          slices: top6.map(function (p) { return { name: label(p), label: p.ticker, value: p.value, tone: 'pos', big: pct(p.w), small: money(p.value) + (p.funds.length > 1 ? DOT + p.funds.length + ' funds' : '') }; })
            .concat(restV > 0 ? [{ name: (pos.length - 6) + ' other positions', label: 'other', value: restV, tone: 'oth', big: pct(restV / total * 100), small: money(restV) }] : []),
          caption: 'Slices are shares of the reported value' + (fund === 'ALL' ? ' of all funds together' : ' of ' + fund) + '.' },
        lists: [{ key: 'new', title: 'Initiations', hint: 'new lines', tone: 'up', n: scoped ? n.opened : opened.length, rows: FL(opened, function (x) { return plain(x, 'up'); }, LINE, 'val', -1), none: 'no position opened since ' + since },
          { key: 'adds', title: 'Increases', hint: 'largest by dollars', tone: 'up', n: scoped ? n.adds : adds.length, rows: FL(adds, function (r) { return moveRow(r, 'up'); }, MOVE(1), 'usd', -1), none: (complete ? 'no line added to in ' : 'none listed for ') + (fund === 'ALL' ? 'any fund' : fund),
            note: !complete && fund === 'ALL' && (diff.n_position_adds || 0) > adds.length ? 'The feed lists the ' + adds.length + ' largest of ' + diff.n_position_adds + ' additions (largest by percent).' : '' },
          { key: 'trims', title: 'Reductions', hint: 'largest by dollars', tone: 'down', n: scoped ? n.trims : trims.length, rows: FL(trims, function (r) { return moveRow(r, 'down'); }, MOVE(-1), 'usd', 1), none: (complete ? 'no line cut in ' : 'none listed for ') + (fund === 'ALL' ? 'any fund' : fund),
            note: !complete && fund === 'ALL' && (diff.n_position_trims || 0) > trims.length ? 'The feed lists the ' + trims.length + ' largest of ' + diff.n_position_trims + ' cuts (largest by percent).' : '' },
          { key: 'closed', title: 'Terminations', hint: 'lines gone', tone: 'down', n: scoped ? n.closed : closed.length, rows: FL(closed, function (x) { return plain(x, 'down'); }, LINE, 'val', -1), none: 'no position closed since ' + since }],
        more: [hidden('positions', 'Current positions', fund === 'ALL' ? 'all funds, one row a ticker' : 'every line of ' + fund, pos, POSN, 'val', -1)],
        foot: 'Source: ARK Invest daily fund holdings via arkfunds.io' + (held && base ? ', holdings dated ' + held + ' against ' + base : '') + ' (snapshot ' + now + ' against ' + prev + '). Dollar size of a move = shares added or sold x the position\u2019s reported price that day. Percent changes are changes in share count; a line counts as changed when its shares move 1% or more. '
          + (complete ? 'Every such line is listed.' : 'The feed publishes the 15 largest additions and cuts by percent.') + ' ARK ETFs are actively managed; a daily file shows holdings, not intent.'
      };
    }
  };

  ADAPTERS.stablecoin = {
    feeds: ['/data/stablecoin-flow.json'],
    build: function (docs) {
      var d = docs[0], a = d.aggregate || {}, coins = (d.top_stablecoins_by_mcap || []).slice().sort(by(function (c) { return c.circulating_usd || 0; }, true)), total = a.total_usd || sum(coins, function (c) { return c.circulating_usd; });
      var nm = function (c) { return c.symbol + DOT + c.name; }, w = function (c) { return total ? (c.circulating_usd || 0) / total * 100 : 0; };
      var minters = (d.top_5_minters_30d || []).slice().sort(by(function (c) { return c.delta_30d || 0; }, true)), burners = (d.top_3_burners_30d || []).slice().sort(by(function (c) { return c.delta_30d || 0; }));
      var up30 = coins.filter(function (c) { return c.delta_30d > 0; }).length, dn30 = coins.filter(function (c) { return c.delta_30d < 0; }).length, top3 = sum(coins.slice(0, 3), w);
      var top6 = coins.slice(0, 6), restV = total - sum(top6, function (c) { return c.circulating_usd; });
      var row = function (c) { return { name: nm(c), sub: 'supply ' + money(c.circulating_usd) + DOT + '7d ' + money(c.delta_7d, true) + DOT + (c.mechanism || ''), big: money(c.delta_30d, true), tone: tone(c.delta_30d), small: pct(c.delta_30d_pct, 1, true) + ' in 30d' }; };
      var card = function (c, kind) {
        if (!c) return { tone: kind, big: D, eyebrow: 'Most notable ' + (kind === 'up' ? 'minting' : 'redemption'), title: 'None', text: 'No coin in the feed ' + (kind === 'up' ? 'grew' : 'shrank') + ' over 30 days.' };
        return { tone: kind, big: money(c.delta_30d, true), eyebrow: 'Most notable ' + (kind === 'up' ? 'minting' : 'redemption') + DOT + '30 days', title: nm(c),
          text: 'Supply went from ' + money(c.prev_month) + ' to ' + money(c.circulating_usd) + ' (' + pct(c.delta_30d_pct, 1, true) + '); ' + money(c.delta_7d, true) + ' of it in the last 7 days and ' + money(c.delta_24h, true) + ' in the last 24 hours.' };
      };
      /* the whole of the minting and redeeming lists: every coin the feed carries, by its 30-day change */
      var known = [], seenC = {};
      coins.concat(minters, burners).forEach(function (c) { if (c && !seenC[c.symbol]) { seenC[c.symbol] = 1; known.push(c); } });
      var d30 = function (c) { return c.delta_30d || 0; }, grew = known.filter(function (c) { return c.delta_30d > 0; }).sort(by(d30, true)), shrank = known.filter(function (c) { return c.delta_30d < 0; }).sort(by(d30));
      var COIN = sheet([tx('t', 'Symbol', 'symbol', { find: 'start' }), tx('name', 'Coin', 'name'), nx('sup', 'Supply', 'circulating_usd', fM), nx('w', '% of supply', w, fP(1)), nx('d1', '24 hours', 'delta_24h', fMs, { tone: true }), nx('d7', '7 days', 'delta_7d', fMs, { tone: true }),
        nx('d30', '30 days', 'delta_30d', fMs, { tone: true }), nx('p30', '30 days, %', 'delta_30d_pct', fP(1, true), { tone: true }), tx('mech', 'Backing', 'mechanism')], ['coin', 'coins'], { find: 'Find a coin', phone: ['t', 'd30', 'p30', 'sup', 'name'] });
      var CHAIN = sheet([tx('name', 'Chain', 'chain'), nx('sup', 'Supply', 'circulating_usd', fM), nx('w', '% of supply', function (c) { return total ? c.circulating_usd / total * 100 : null; }, fP(1))], ['chain', 'chains']);
      var chains = (a.top_chains || []).slice(0, 10);
      return {
        eyebrow: 'Stablecoin tearsheet' + DOT + day(d.as_of) + ' vs 30 days ago', title: 'Stablecoins: ' + money(total) + ' outstanding, ' + money(a.delta_30d_usd, true) + ' in 30 days (' + pct(a.delta_30d_pct, 1, true) + ')' + DOT + 'state ' + (d.state || D),
        sub: num(a.n_stablecoins) + ' USD-pegged coins' + DOT + 'supply is dollars waiting on-chain: minting adds dry powder, redemption removes it' + (d.previous_state && d.previous_state !== d.state ? DOT + 'was ' + d.previous_state : ''),
        tiles: [{ k: 'Total supply', v: money(total), s: 'all USD-pegged coins' }, { k: 'Line items', v: num(a.n_stablecoins), s: 'coins tracked' }, { k: 'In top 3', v: pct(top3, 0), s: coins.slice(0, 3).map(function (c) { return c.symbol; }).join(', ') },
          { k: 'Minting / redeeming', v: '<span class="up">' + up30 + '</span> / <span class="down">' + dn30 + '</span>', s: 'of the ' + coins.length + ' largest, 30 days' },
          { k: '7-day change', v: '<span class="' + tone(a.delta_7d_usd) + '">' + money(a.delta_7d_usd, true) + '</span>', s: pct(a.delta_7d_pct, 2, true) }, { k: '24-hour change', v: '<span class="' + tone(a.delta_24h_usd) + '">' + money(a.delta_24h_usd, true) + '</span>', s: pct(a.delta_24h_pct, 2, true) }],
        cards: [card(burners[0] || coins.slice().sort(by(function (c) { return c.delta_30d || 0; }))[0], 'down'), card(minters[0], 'up')],
        notes: [['Regime:', (d.state_description || d.state || D) + (fin(d.signal_strength) ? ' Signal strength ' + d.signal_strength + '/100.' : '')],
          ['Where it lives:', chains.slice(0, 3).map(function (c) { return c.chain + ' ' + money(c.circulating_usd) + ' (' + pct(total ? c.circulating_usd / total * 100 : 0, 0) + ')'; }).join(', ') + '.'],
          ['Caveat:', 'supply is what issuers have minted, not what is being spent; a coin can shrink because holders redeem for dollars or because it moves to another issuer.']],
        ring: { open: 'coins', title: 'Supply by coin', hint: 'top 6 of ' + num(a.n_stablecoins), n: a.n_stablecoins, center: money(total), subs: ['OUTSTANDING', '30D ' + pct(a.delta_30d_pct, 1, true)], whole: 'supply', aria: 'Stablecoin supply by coin',
          slices: top6.map(function (c) { return { name: nm(c), label: c.symbol, value: c.circulating_usd, tone: 'pos', big: pct(w(c)), small: money(c.circulating_usd) + DOT + '30d ' + pct(c.delta_30d_pct, 1, true) }; })
            .concat(restV > 0 ? [{ name: ((a.n_stablecoins || coins.length) - 6) + ' other coins', label: 'other', value: restV, tone: 'oth', big: pct(restV / total * 100), small: money(restV) }] : []),
          caption: 'Slices are shares of all USD-pegged supply.' },
        lists: [{ key: 'mint', title: 'Minting', hint: 'largest 30-day growth', tone: 'up', rows: FL(minters, row, COIN, 'd30', -1, { rows: grew })}, { key: 'burn', title: 'Redeeming', hint: 'largest 30-day shrink', tone: 'down', rows: FL(burners, row, COIN, 'd30', 1, { rows: shrank })},
          { key: 'coins', title: 'Largest coins', hint: 'supply and 30-day change', rows: FL(coins, function (c) { return { name: nm(c), sub: pct(w(c)) + ' of supply' + DOT + '7d ' + money(c.delta_7d, true), big: money(c.circulating_usd), small: '30d ' + pct(c.delta_30d_pct, 1, true) }; }, COIN, 'sup', -1)},
          { key: 'chains', title: 'By chain', hint: 'where the supply sits', rows: FL(chains, function (c) { return { name: c.chain, sub: '', big: money(c.circulating_usd), small: pct(total ? c.circulating_usd / total * 100 : 0) + ' of supply' }; }, CHAIN, null, 0, { rows: a.top_chains || [] })}],
        foot: 'Source: DefiLlama stablecoin supply, as of ' + day(d.as_of) + '. Changes are supply now against supply 24 hours, 7 days and 30 days ago. Minting and redeeming lists are the feed\u2019s own largest movers over 30 days.'
      };
    }
  };

  ADAPTERS.firmbook = {
    feeds: ['/data/firm-book.json'],
    build: function (docs) {
      var d = docs[0], f = d.firm || {}, book = (d.equity_book || []).slice(), longs = book.filter(function (p) { return p.net_pct > 0; }).sort(by(function (p) { return p.net_pct; }, true)), shorts = book.filter(function (p) { return p.net_pct < 0; }).sort(by(function (p) { return p.net_pct; }));
      var g = function (p) { return Math.abs(p.gross_pct != null ? p.gross_pct : p.net_pct); }, lt = sum(longs, g), stt = sum(shorts, g), gross = lt + stt, stated = fin(f.gross_exposure_pct) && Math.abs(f.gross_exposure_pct - gross) < 0.05 ? f.gross_exposure_pct : gross;
      var nm = function (p) { return p.symbol + DOT + (p.name || ''); };
      var side = function (list, t) { var top = list.slice(0, 3), rest = list.slice(3), out = top.map(function (p) { return { name: nm(p), label: p.symbol, value: g(p), tone: t, big: pct(p.net_pct, 2, true), small: (p.sector || '') }; });
        if (rest.length) out.push({ name: rest.length + ' other ' + (t === 'up' ? 'longs' : 'shorts'), label: 'other', value: sum(rest, g), tone: t, rest: true, big: pct((t === 'up' ? 1 : -1) * sum(rest, g), 2, true), small: 'of capital' }); return out; };
      var row = function (p) { return { name: nm(p), sub: (p.sector && p.sector !== 'Unknown' ? p.sector : 'sector not mapped') + DOT + (p.n_desks || 1) + (p.n_desks > 1 ? ' desks' : ' desk') + (p.price ? DOT + '$' + num(p.price, 2) : ''), big: pct(p.net_pct, 2, true), tone: tone(p.net_pct), small: 'of capital' }; };
      var desks = Object.keys(d.desk_contributions || {}).map(function (k) { return d.desk_contributions[k]; }).sort(by(function (x) { return x.gross_contributed_pct || 0; }, true));
      var sectors = (d.sector_exposure || []).slice().sort(by(function (s) { return s.gross_pct || 0; }, true));
      var POSN = sheet([tx('t', 'Symbol', 'symbol', { find: 'start' }), tx('name', 'Name', 'name'), tx('sec', 'Sector', function (p) { return p.sector && p.sector !== 'Unknown' ? p.sector : 'Sector not mapped'; }), tx('side', 'Side', function (p) { return p.net_pct > 0 ? 'Long' : p.net_pct < 0 ? 'Short' : 'Flat'; }),
        nx('net', 'Net % of capital', 'net_pct', fP(2, true), { tone: true }), nx('gross', 'Gross % of capital', g, fP(2)), nx('nd', 'Desks', function (p) { return p.n_desks || 1; }, fN(0)), tx('by', 'Held by', function (p) { return Object.keys(p.desks || {}).join(', '); }), nx('px', 'Price', 'price', fN(2))],
        ['position', 'positions'], { href: function (p) { return chartOf(p.symbol); }, find: 'Find a symbol, name, sector or desk', phone: ['t', 'net', 'side', 'sec', 'nd', 'name'] });
      var SECT = sheet([tx('name', 'Sector', function (s) { return s.sector === 'Unknown' ? 'Sector not mapped' : s.sector; }), nx('n', 'Names', 'n', fN(0)), nx('gross', 'Gross % of capital', 'gross_pct', fP(2)), nx('net', 'Net % of capital', 'net_pct', fP(2, true), { tone: true })], ['sector', 'sectors']);
      var DESK = sheet([tx('name', 'Desk', 'name'), tx('status', 'Status', 'status'), tx('sleeve', 'Sleeve', 'sleeve'), nx('n', 'Positions', function (x) { return x.n_positions || 0; }, fN(0)), nx('budget', 'Budget, % of capital', 'capital_pct', fP(1)),
        nx('gross', 'Gross in the book', function (x) { return x.gross_contributed_pct || 0; }, fP(1))], ['desk', 'desks']);
      var bl = longs[0], bs = shorts[0];
      return {
        eyebrow: 'Firm book tearsheet' + DOT + day(d.generated_at), title: 'Firm book: ' + pct(f.gross_exposure_pct, 1) + ' gross, ' + pct(f.net_exposure_pct, 1, true) + ' net' + DOT + num(f.n_longs) + ' longs against ' + num(f.n_shorts) + ' shorts',
        sub: 'Every desk\u2019s positions netted into one book' + DOT + 'percent of capital' + DOT + 'this feed is today\u2019s book only, so there is no opened / closed list',
        tiles: [{ k: 'Gross exposure', v: pct(f.gross_exposure_pct, 1), s: 'long ' + pct(f.equity_long_pct, 1) + ' + short ' + pct(Math.abs(f.equity_short_pct), 1) }, { k: 'Line items', v: num(f.n_equity_names), s: 'net equity names' + (f.n_macro_positions ? ' + ' + f.n_macro_positions + ' macro' : '') },
          { k: 'In top 10', v: pct(f.top10_concentration_pct, 0), s: 'of gross' }, { k: 'Longs / shorts', v: '<span class="up">' + num(f.n_longs) + '</span> / <span class="down">' + num(f.n_shorts) + '</span>', s: 'names' },
          { k: 'Net exposure', v: '<span class="' + tone(f.net_exposure_pct) + '">' + pct(f.net_exposure_pct, 1, true) + '</span>', s: 'long minus short' }, { k: 'Long / short ratio', v: fix(f.long_short_ratio), s: 'dollars long per dollar short' }],
        cards: [bs ? { tone: 'down', big: pct(bs.net_pct, 2, true), eyebrow: 'Largest short', title: nm(bs), text: 'The biggest single short in the book: ' + pct(Math.abs(bs.net_pct), 2) + ' of capital' + (bs.sector ? ', ' + bs.sector : '') + ', carried by ' + (bs.n_desks || 1) + (bs.n_desks > 1 ? ' desks.' : ' desk.') } : null,
          bl ? { tone: 'up', big: pct(bl.net_pct, 2, true), eyebrow: 'Largest long', title: nm(bl), text: 'The biggest single long in the book: ' + pct(bl.net_pct, 2) + ' of capital' + (bl.sector ? ', ' + bl.sector : '') + ', carried by ' + (bl.n_desks || 1) + (bl.n_desks > 1 ? ' desks.' : ' desk.') } : null].filter(Boolean),
        notes: [['The book in one line:', d.headline || D], ['Desks firing:', (desks.filter(function (x) { return x.n_positions; }).map(function (x) { return x.name + ' ' + pct(x.gross_contributed_pct, 1) + ' (' + x.n_positions + ')'; }).join(', ') || 'none') + '.'],
          ['Overlaps and conflicts:', (d.conviction_overlap || []).length + ' names held by more than one desk on the same side, ' + (d.desk_conflicts || []).length + ' where desks disagree.']],
        ring: { open: 'book', title: 'Gross exposure', hint: 'long solid, short hatched', n: book.length, center: pct(stated, 1), subs: ['OF CAPITAL, GROSS', 'LONG ' + pct(gross ? lt / gross * 100 : 0, 0) + DOT + 'SHORT ' + pct(gross ? stt / gross * 100 : 0, 0)], whole: 'gross', aria: 'Gross exposure by position, longs and shorts',
          slices: side(longs, 'up').concat(side(shorts, 'down')), caption: 'Solid = long, hatched = short. Each slice is a share of gross exposure (' + pct(stated, 1) + ' of capital).' },
        lists: [{ key: 'longs', title: 'Largest longs', hint: 'net % of capital', tone: 'up', n: longs.length, rows: FL(longs, row, POSN, 'net', -1)}, { key: 'shorts', title: 'Largest shorts', hint: 'net % of capital', tone: 'down', n: shorts.length, rows: FL(shorts, row, POSN, 'net', 1)},
          { key: 'sectors', title: 'By sector', hint: 'gross and net', rows: FL(sectors, function (s) { return { name: s.sector === 'Unknown' ? 'Sector not mapped' : s.sector, sub: s.n + ' names' + DOT + 'net ' + pct(s.net_pct, 2, true), big: pct(s.gross_pct, 2), small: 'gross' }; }, SECT, 'gross', -1)},
          { key: 'desks', title: 'By desk', hint: 'capital and status', rows: FL(desks, function (x) { return { name: x.name, sub: (x.status || '') + DOT + (x.n_positions || 0) + ' positions' + DOT + 'budget ' + pct(x.capital_pct, 1), big: pct(x.gross_contributed_pct, 1), small: 'gross in the book' }; }, DESK, 'gross', -1)}],
        more: [hidden('book', 'The whole book', 'every position, largest gross first', book.slice().sort(by(g, true)), POSN, 'gross', -1)],
        foot: 'Source: justhodl-firm-book, ' + day(d.generated_at) + '. Sizes are percent of firm capital after netting every desk. ' + (d.disclaimer ? String(d.disclaimer).slice(0, 220) : '')
      };
    }
  };

  ADAPTERS.paperbook = {
    feeds: ['/data/paper-book.json'],
    build: function (docs) {
      var d = docs[0], pos = (d.positions || []).slice().sort(by(function (p) { return p.weight_pct || 0; }, true)), trades = d.recent_trades || [], last = d.last_rebalance;
      var at = trades.filter(function (t) { return t.date === last; }), buys = at.filter(function (t) { return /buy/i.test(t.side); }).sort(by(function (t) { return t.notional || 0; }, true)), sells = at.filter(function (t) { return /sell/i.test(t.side); }).sort(by(function (t) { return t.notional || 0; }, true));
      var earlier = trades.filter(function (t) { return t.date !== last; }), nav = d.nav, held = {};
      pos.forEach(function (p) { held[p.ticker] = 1; });
      var isBuy = function (t) { return /buy/i.test(t.side); };
      var TRADE = sheet([tx('t', 'Ticker', 'ticker', { find: 'start' }), tx('date', 'Date', 'date'), tx('side', 'Side', function (t) { return isBuy(t) ? 'Bought' : 'Sold'; }), nx('usd', 'Notional', function (t) { return fin(t.notional) ? (isBuy(t) ? 1 : -1) * t.notional : null; }, fMs, { tone: true }),
        nx('px', 'Price', 'px', fN(2)), nx('slip', 'Slippage, $', 'slippage', fN(2))], ['trade', 'trades'], { href: function (t) { return chartOf(t.ticker); }, find: 'Find a ticker or a date' });
      var HELD = sheet([tx('t', 'Ticker', 'ticker', { find: 'start' }), nx('w', '% of the book', 'weight_pct', fP(1)), nx('sh', 'Shares', 'shares', fN(1)), nx('px', 'Price', 'px', fN(2))], ['position', 'positions'], { href: function (p) { return chartOf(p.ticker); } });
      var row = function (t) { return { name: t.ticker, sub: t.date + DOT + 'at $' + num(t.px, 2) + DOT + 'slippage $' + num(t.slippage, 2), big: (/buy/i.test(t.side) ? '+' : MINUS) + money(t.notional), tone: /buy/i.test(t.side) ? 'up' : 'down', small: /sell/i.test(t.side) && !held[t.ticker] ? 'closed' : /buy/i.test(t.side) ? 'bought' : 'sold' }; };
      var card = function (t, kind) {
        if (!t) return { tone: kind, big: D, eyebrow: 'Most notable ' + (kind === 'up' ? 'buy' : 'sale') + DOT + 'last rebalance', title: 'None', text: 'Nothing was ' + (kind === 'up' ? 'bought' : 'sold') + ' at the ' + (last || 'last') + ' rebalance.' };
        return { tone: kind, big: (kind === 'up' ? '+' : MINUS) + money(t.notional), eyebrow: 'Most notable ' + (kind === 'up' ? 'buy' : 'sale') + DOT + t.date, title: t.ticker,
          text: (kind === 'up' ? 'Bought ' : 'Sold ') + money(t.notional) + ' of ' + t.ticker + ' at $' + num(t.px, 2) + (fin(nav) ? ', ' + pct(t.notional / nav * 100) + ' of the book' : '') + (kind === 'down' && !held[t.ticker] ? '; the position is closed.' : '.') };
      };
      return {
        eyebrow: 'Paper book tearsheet' + DOT + day(d.generated_at) + DOT + 'last rebalance ' + (last || D), title: 'Paper book: ' + money(nav) + ', ' + pct(d.total_return_pct, 2, true) + ' since ' + (d.inception || 'inception') + ' against ' + pct(d.benchmark_return_pct, 2, true) + ' for the S&P 500',
        sub: 'A live systematic track record on paper capital' + DOT + num(d.n_days) + ' days' + DOT + 'regime at the last rebalance: ' + (d.regime_at_last_rebalance || D),
        tiles: [{ k: 'Net asset value', v: money(nav), s: 'benchmark ' + money(d.benchmark_nav) }, { k: 'Line items', v: num(pos.length), s: 'positions + ' + pct(d.cash_pct, 1) + ' cash' },
          { k: 'Excess vs S&P 500', v: '<span class="' + tone(d.excess_vs_spy_pct) + '">' + pct(d.excess_vs_spy_pct, 2, true) + '</span>', s: 'percentage points' }, { k: 'Bought / sold', v: '<span class="up">' + buys.length + '</span> / <span class="down">' + sells.length + '</span>', s: 'at the ' + (last || 'last') + ' rebalance' },
          { k: 'Sharpe', v: fix(d.sharpe), s: 'since inception' }, { k: 'Max drawdown', v: '<span class="down">' + pct(d.max_drawdown_pct, 1, true) + '</span>', s: 'peak to trough' }],
        cards: [card(sells[0], 'down'), card(buys[0], 'up')],
        notes: [['Largest holding:', pos[0] ? pos[0].ticker + ', ' + pct(pos[0].weight_pct, 1) + ' of the book (' + num(pos[0].shares, 1) + ' shares at $' + num(pos[0].px, 2) + ').' : 'the book is all cash.'],
          ['Rebalance turnover:', at.length ? money(sum(at, function (t) { return t.notional; })) + ' traded across ' + at.length + ' orders on ' + last + (fin(nav) ? ', ' + pct(sum(at, function (t) { return t.notional; }) / nav * 100, 0) + ' of the book.' : '.') : 'no trades at the last rebalance.'],
          ['Caveat:', 'paper capital, not money at risk; fills are modelled with slippage and the record is ' + num(d.n_days) + ' days long.']],
        ring: { open: 'positions', title: 'Current positions', hint: 'share of the book', n: pos.length, center: money(nav), subs: ['NET ASSET VALUE', 'CASH ' + pct(d.cash_pct, 1)], whole: 'the book', aria: 'Paper book by position and cash',
          slices: pos.map(function (p) { return { name: p.ticker, value: p.weight_pct, tone: 'pos', big: pct(p.weight_pct, 1), small: num(p.shares, 1) + ' sh at $' + num(p.px, 2) }; }).concat(d.cash_pct > 0 ? [{ name: 'Cash', label: 'cash', value: d.cash_pct, tone: 'oth', big: pct(d.cash_pct, 1), small: 'uninvested' }] : []),
          caption: 'Slices are shares of net asset value; cash is the grey slice.' },
        lists: [{ key: 'buys', title: 'Bought', hint: 'last rebalance', tone: 'up', rows: FL(buys, row, TRADE, 'usd', -1), none: 'nothing bought on ' + (last || D) }, { key: 'sells', title: 'Sold', hint: 'last rebalance', tone: 'down', rows: FL(sells, row, TRADE, 'usd', 1), none: 'nothing sold on ' + (last || D) },
          { key: 'earlier', title: 'Earlier trades', hint: 'most recent first', rows: FL(earlier, row, TRADE), none: 'no earlier trades in the feed' }],
        more: [hidden('positions', 'Current positions', 'share of the book', pos, HELD, 'w', -1)],
        foot: 'Source: justhodl-paper-book, ' + day(d.generated_at) + '. ' + (d.note ? String(d.note).slice(0, 260) : '')
      };
    }
  };

  ADAPTERS.sp500 = {
    feeds: ['/data/sp500.json'],
    build: function (docs) {
      var d = docs[0], ix = d.index || {}, F = d.member_fields || [], iChg = F.indexOf('px_chg_since_census_pct'), iSec = F.indexOf('sector'), iCap = F.indexOf('mcap'), iPe = F.indexOf('pe_ttm'), iMom = F.indexOf('mom_6m_pct'), fld = function (r, name) { var i = F.indexOf(name); return i < 0 ? null : r[i]; };
      var mem = Object.keys(d.members || {}).map(function (t) { var r = d.members[t]; return { t: t, chg: r[iChg], sec: r[iSec], cap: r[iCap], pe: r[iPe], mom: r[iMom], fwd: fld(r, 'pe_fwd'), dy: fld(r, 'div_yield_pct'), fcf: fld(r, 'fcf_yield_pct'), roe: fld(r, 'roe_pct'), rev: fld(r, 'revenue_yoy_pct'), eps: fld(r, 'eps_yoy_pct'), m12: fld(r, 'mom_12_1_pct'), beta: fld(r, 'beta_2y') }; }).filter(function (m) { return fin(m.cap); });
      var total = ix.total_mcap || sum(mem, function (m) { return m.cap; });
      mem.forEach(function (m) { m.w = total ? m.cap / total * 100 : 0; m.contrib = fin(m.chg) ? m.w * m.chg / 100 : null; });
      var heavy = mem.slice().sort(by(function (m) { return m.cap; }, true)), top10 = sum(heavy.slice(0, 10), function (m) { return m.w; });
      var moved = mem.filter(function (m) { return fin(m.contrib); }), lift = moved.slice().sort(by(function (m) { return m.contrib; }, true)), drag = moved.slice().sort(by(function (m) { return m.contrib; }));
      var upN = moved.filter(function (m) { return m.chg > 0; }).length, dnN = moved.filter(function (m) { return m.chg < 0; }).length, flatN = moved.length - upN - dnN;
      var secs = Object.keys(d.sectors || {}).map(function (k) { var s = d.sectors[k]; return { name: k, w: s.weight_pct, n: s.n, cap: s.cap_t, pe: s.pe_ttm, fwd: s.pe_fwd }; }).sort(by(function (s) { return s.w || 0; }, true));
      var SHORT = { 'Technology': 'Tech', 'Communication Services': 'Comm', 'Financial Services': 'Financials', 'Consumer Cyclical': 'Cyclical', 'Consumer Defensive': 'Defensive', 'Healthcare': 'Health', 'Industrials': 'Industrial', 'Basic Materials': 'Materials', 'Real Estate': 'Real est.' };
      var top6 = secs.slice(0, 6), restW = sum(secs.slice(6), function (s) { return s.w; }), v = d.valuation || {}, pe = (v.pe_ttm || {}).agg, ey = (v.earnings_yield_pct || {}).agg;
      var MEMB = sheet([tx('t', 'Ticker', 't', { find: 'start' }), tx('sec', 'Sector', 'sec'), nx('cap', 'Mkt value', 'cap', fM), nx('w', '% of the index', 'w', fP(2)), nx('chg', 'Since the census', 'chg', fP(1, true), { tone: true, tip: 'price change since the last fundamentals census' }),
        nx('contrib', 'Index points', 'contrib', fS(3), { tone: true, tip: 'the member\u2019s contribution to the index\u2019s move since the census, in percentage points' }), nx('pe', 'P/E', 'pe', fN(1)), nx('fwd', 'Forward P/E', 'fwd', fN(1)), nx('mom', '6 months', 'mom', fP(0, true), { tone: true }),
        nx('m12', '12-1 momentum', 'm12', fP(0, true), { tone: true }), nx('dy', 'Dividend yield', 'dy', fP(2)), nx('fcf', 'FCF yield', 'fcf', fP(1)), nx('roe', 'ROE', 'roe', fP(1)), nx('rev', 'Revenue, 1 year', 'rev', fP(1, true), { tone: true }), nx('eps', 'EPS, 1 year', 'eps', fP(1, true), { tone: true }),
        nx('beta', 'Beta', 'beta', fN(2))], ['member', 'members'], { href: function (m) { return chartOf(m.t); }, find: 'Find a ticker or a sector', phone: ['t', 'chg', 'contrib', 'w', 'cap', 'sec'] });
      var SECS = sheet([tx('name', 'Sector', 'name'), nx('w', '% of the index', 'w', fP(1)), nx('n', 'Members', 'n', fN(0)), nx('cap', 'Mkt value', 'cap', fM), nx('pe', 'P/E', 'pe', fN(1)), nx('fwd', 'Forward P/E', 'fwd', fN(1))], ['sector', 'sectors']);
      var row = function (m) { return { name: m.t, sub: (m.sec || '') + DOT + pct(m.w, 2) + ' of the index' + (fin(m.pe) ? DOT + 'P/E ' + m.pe.toFixed(1) : ''), big: pct(m.chg, 1, true), tone: tone(m.chg), small: (m.contrib >= 0 ? '+' : MINUS) + Math.abs(m.contrib).toFixed(3) + ' pts of index' }; };
      var card = function (m, kind) { return !m ? null : { tone: kind, big: pct(m.chg, 1, true), eyebrow: 'Largest ' + (kind === 'up' ? 'lift' : 'drag') + ' on the index' + DOT + 'since the last fundamentals census', title: m.t + DOT + (m.sec || ''),
        text: m.t + ' is ' + pct(m.w, 2) + ' of the index and has moved ' + pct(m.chg, 1, true) + ' since ' + (ix.fundamentals_asof || 'the census') + ': ' + (m.contrib >= 0 ? '+' : MINUS) + Math.abs(m.contrib).toFixed(2) + ' percentage points of the index on its own.' }; };
      return {
        eyebrow: 'S&P 500 tearsheet' + DOT + 'prices ' + (ix.px_date || day(d.as_of)) + DOT + 'fundamentals ' + (ix.fundamentals_asof || D), title: 'The S&P 500 as one stock: ' + num(ix.level, 2) + ', ' + money(total) + ' of market value in ' + num(ix.members) + ' members' + DOT + 'regime ' + (ix.regime || D),
        sub: 'Weights are market value' + DOT + 'moves are each member\u2019s price change since the last fundamentals census (' + (ix.fundamentals_asof || D) + ')' + DOT + upN + ' up, ' + dnN + ' down',
        tiles: [{ k: 'Market value', v: money(total), s: 'index level ' + num(ix.level, 2) }, { k: 'Line items', v: num(ix.members || mem.length), s: 'members' }, { k: 'In top 10', v: pct(top10, 0), s: heavy.slice(0, 3).map(function (m) { return m.t; }).join(', ') + ' lead' },
          { k: 'Up / down', v: '<span class="up">' + upN + '</span> / <span class="down">' + dnN + '</span>', s: 'members since the census' + (flatN ? DOT + flatN + ' flat' : '') }, { k: 'P/E, trailing', v: fin(pe) ? pe.toFixed(1) + 'x' : D, s: 'earnings yield ' + pct(ey, 2) },
          { k: 'Largest sector', v: secs[0] ? pct(secs[0].w, 0) : D, s: secs[0] ? secs[0].name : '' }],
        cards: [card(drag[0], 'down'), card(lift[0], 'up')].filter(Boolean),
        notes: [['Largest member:', heavy[0] ? heavy[0].t + ', ' + money(heavy[0].cap) + ' (' + pct(heavy[0].w, 1) + ' of the index).' : D], ['Concentration:', 'the ten largest members are ' + pct(top10, 1) + ' of the index; ' + (secs[0] ? secs[0].name + ' alone is ' + pct(secs[0].w, 1) + '.' : '')],
          ['Caveat:', 'weights here are full market value, not the float-adjusted weights of the official index, so they differ slightly from S&P\u2019s own.']],
        ring: { open: 'sectors', title: 'Index by sector', hint: 'share of market value', n: secs.length, center: money(total), subs: ['MARKET VALUE', num(ix.members) + ' MEMBERS'], whole: 'the index', aria: 'S&P 500 market value by sector',
          slices: top6.map(function (s) { return { name: s.name, label: SHORT[s.name] || s.name, value: s.w, tone: 'pos', big: pct(s.w, 1), small: s.n + ' members' + (fin(s.pe) ? DOT + 'P/E ' + s.pe.toFixed(1) : '') }; })
            .concat(restW > 0 ? [{ name: (secs.length - 6) + ' other sectors', label: 'other', value: restW, tone: 'oth', big: pct(restW, 1), small: secs.slice(6).map(function (s) { return s.name; }).join(', ') }] : []), caption: 'Slices are shares of total market value.' },
        lists: [{ key: 'lift', title: 'Lifting the index', hint: 'largest contributions', tone: 'up', n: upN, rows: FL(lift.filter(function (m) { return m.contrib > 0; }), row, MEMB, 'contrib', -1)}, { key: 'drag', title: 'Dragging the index', hint: 'largest contributions', tone: 'down', n: dnN, rows: FL(drag.filter(function (m) { return m.contrib < 0; }), row, MEMB, 'contrib', 1)},
          { key: 'heavy', title: 'Heaviest members', hint: 'share of the index', n: mem.length, rows: FL(heavy, function (m) { return { name: m.t, sub: (m.sec || '') + (fin(m.pe) ? DOT + 'P/E ' + m.pe.toFixed(1) : '') + (fin(m.mom) ? DOT + '6m ' + pct(m.mom, 0, true) : ''), big: pct(m.w, 2), small: money(m.cap) }; }, MEMB, 'cap', -1)},
          { key: 'sectors', title: 'Sectors', hint: 'weight and valuation', rows: FL(secs, function (s) { return { name: s.name, sub: s.n + ' members' + (fin(s.fwd) ? DOT + 'forward P/E ' + s.fwd.toFixed(1) : ''), big: pct(s.w, 1), small: fin(s.pe) ? 'P/E ' + s.pe.toFixed(1) : '' }; }, SECS, 'w', -1)}],
        foot: 'Source: justhodl-sp500 ' + (d.engine_v || '') + ', prices ' + (ix.px_date || D) + ', fundamentals ' + (ix.fundamentals_asof || D) + '. A member\u2019s contribution = its weight x its price change since the census, in percentage points of the index.'
      };
    }
  };

  ADAPTERS.insiders = {
    feeds: ['/data/insider-clusters.json'],
    build: function (docs) {
      /* Engine 2.2.0 cleans the purchase list itself (hygiene: one row per filing line, one filing kept once across an
         issuer's tickers, an impossible price set aside), so the dollars are every company with a purchase. An older
         feed's raw list is not safe for dollars - on 2026-11-17 it carried one record priced at shares / 1000 ($9.9B
         for one holder) and one company under three tickers - so for it the sheet stays on the engine's scored set. */
      var d = docs[0], s = d.stats || {}, th = d.thresholds || {}, hy = d.hygiene && d.hygiene.applied ? d.hygiene : null, strongAt = th.score_strong_threshold || 70, seen = {}, raw = (d.clusters || []).slice().sort(by(function (c) { return c.score || 0; }, true));
      var scored = raw.filter(function (c) { var k = c.cik || c.ticker; if (seen[k]) return false; seen[k] = 1; return fin(c.total_value); });
      var byTicker = {}, seenB = {};
      scored.forEach(function (c) { byTicker[c.ticker] = c; });
      var pop = hy ? (d.all_ticker_buys || []).filter(function (b) { var k = b.cik || b.ticker; if (seenB[k] || !fin(b.total_value)) return false; seenB[k] = 1; return true; }) : scored;
      var KIND = { smart_money_dual: 'CEO and CFO together', ceo_conviction: 'CEO conviction', executive_cluster: 'executive cluster', exec_pair: 'executive pair', cluster_buy: 'cluster buy', lone_buy: 'ordinary purchase' };
      var kind = function (c) { return KIND[c.signal_type] || String(c.signal_type || '').replace(/_/g, ' '); };
      var total = sum(pop, function (c) { return c.total_value; }), dollars = pop.slice().sort(by(function (c) { return c.total_value; }, true)), top6 = dollars.slice(0, 6), restV = total - sum(top6, function (c) { return c.total_value; });
      var multi = scored.filter(function (c) { return c.n_insiders >= 2; }), strong = scored.filter(function (c) { return c.score >= strongAt; }), ceo = scored.filter(function (c) { return c.has_ceo; }), crowd = scored.slice().sort(by(function (c) { return c.n_insiders || 0; }, true));
      var types = {}, days = d.lookback_days || 30, share = function (c) { return total ? c.total_value / total * 100 : 0; };
      scored.forEach(function (c) { var t = types[c.signal_type] || (types[c.signal_type] = { name: kind(c), n: 0, usd: 0, top: c }); t.n++; t.usd += c.total_value; if (c.total_value > t.top.total_value) t.top = c; });
      var typeRows = Object.keys(types).map(function (k) { return types[k]; }).sort(by(function (t) { return t.usd; }, true)), cnt = function (k) { return types[k] ? types[k].n : 0; };
      var nm = function (c) { var co = c.company || (byTicker[c.ticker] || {}).company; return c.ticker + (co ? DOT + co : ''); }, people = function (n) { return n + (n === 1 ? ' insider' : ' insiders'); };
      var role = function (c) { return c.highest_role || (c.roles || [])[0] || ''; }, scoreOf = function (c) { var x = c.score != null ? c : byTicker[c.ticker]; return x && fin(x.score) ? x.score.toFixed(0) : null; };
      var also = function (c) { return (c.also_filed_as || []).length ? DOT + 'also filed as ' + c.also_filed_as.join(', ') : ''; }, off = function (c) { return c.n_off_tape ? DOT + c.n_off_tape + (c.n_off_tape === 1 ? ' line' : ' lines') + ' in another class of security' : ''; };
      var crow = function (c) { return { name: nm(c), sub: people(c.n_insiders) + DOT + money(c.total_value) + ' at $' + num(c.avg_price, 2) + DOT + (c.highest_role || ''), big: fin(c.score) ? c.score.toFixed(0) : D, tone: c.score >= strongAt ? 'up' : '', small: kind(c) }; };
      var drow = function (c) { var sc = scoreOf(c); return { name: nm(c), sub: people(c.n_insiders) + DOT + c.n_transactions + (c.n_transactions === 1 ? ' buy' : ' buys') + DOT + day(c.first_buy) + (c.last_buy !== c.first_buy ? ' to ' + day(c.last_buy) : '') + (role(c) ? DOT + role(c) : '') + also(c) + off(c), big: money(c.total_value), small: pct(share(c)) + ' of the dollars' + (sc ? DOT + 'score ' + sc : '') }; };
      var rich = dollars[0], best = scored[0], nTick = s.n_unique_tickers || pop.length, left = Math.max(nTick - scored.length, 0), aside = hy ? (hy.set_aside || []) : [];
      var whoSet = hy ? 'companies' : 'scored companies', avg = function (c) { return fin(c.avg_price) ? c.avg_price : c.total_shares ? c.total_value / c.total_shares : null; };
      var coOf = function (c) { return c.company || (byTicker[c.ticker] || {}).company; }, scOf = function (c) { var x = c.score != null ? c : byTicker[c.ticker]; return x && fin(x.score) ? x.score : null; };
      var kindOf = function (c) { var x = c.signal_type ? c : byTicker[c.ticker]; return x && x.signal_type ? kind(x) : null; };
      var CLUS = sheet([tx('t', 'Ticker', 'ticker', { find: 'start' }), tx('co', 'Company', coOf), nx('score', 'Score', scOf, fN(0)), nx('usd', 'Dollars bought', 'total_value', fM), nx('share', '% of the dollars', share, fP(1)), nx('n', 'Insiders', 'n_insiders', fN(0)),
        nx('buys', 'Buys', 'n_transactions', fN(0)), nx('px', 'Average price', avg, fN(2)), tx('first', 'First buy', function (c) { return c.first_buy ? day(c.first_buy) : null; }), tx('last', 'Last buy', function (c) { return c.last_buy ? day(c.last_buy) : null; }),
        tx('role', 'Most senior buyer', role), tx('kind', 'Kind of buying', kindOf)], ['company', 'companies'], { href: function (c) { return chartOf(c.ticker); }, find: 'Find a ticker, company or role', phone: ['t', 'score', 'usd', 'n', 'co'] });
      var KINDS = sheet([tx('name', 'Kind of buying', 'name'), nx('n', 'Companies', 'n', fN(0)), nx('usd', 'Dollars bought', 'usd', fM), nx('share', '% of the dollars', function (t) { return total ? t.usd / total * 100 : null; }, fP(1)), tx('top', 'Largest', function (t) { return (t.top || {}).ticker; })], ['kind', 'kinds']);
      var ASIDE = sheet([tx('t', 'Ticker', 'ticker', { find: 'start' }), tx('co', 'Company', 'company'), tx('who', 'Insider', 'insider_name'), nx('usd', 'Value as filed', 'value', fM), nx('sh', 'Shares', 'shares', fN(0)), nx('px', 'Price', 'price', fN(2)), tx('why', 'Why it is set aside', 'reason')], ['filing', 'filings']);
      var lists = [{ key: 'clusters', title: 'Strongest scores', hint: 'by score', tone: 'up', n: scored.length, rows: FL(scored, crow, CLUS, 'score', -1)}, { key: 'dollars', title: 'Largest dollars', hint: 'by company', n: pop.length, rows: FL(dollars, drow, CLUS, 'usd', -1)},
        { key: 'ceo', title: 'The CEO is buying', hint: 'by score', tone: 'up', n: ceo.length, rows: FL(ceo, crow, CLUS, 'score', -1), none: 'no chief executive bought in the window' }, { key: 'crowd', title: 'Most insiders', hint: 'breadth of the buying', n: multi.length, rows: FL(crowd.filter(function (c) { return c.n_insiders >= 2; }), function (c) { var r = crow(c); r.big = String(c.n_insiders); r.tone = ''; r.small = 'insiders' + DOT + 'score ' + (fin(c.score) ? c.score.toFixed(0) : D); return r; }, CLUS, 'n', -1), none: 'no company has two insiders buying' },
        { key: 'types', title: 'By kind of buying', hint: 'scored companies and dollars', rows: FL(typeRows, function (t) { return { name: t.name, sub: t.n + (t.n === 1 ? ' company' : ' companies') + DOT + 'largest ' + t.top.ticker, big: money(t.usd), small: pct(total ? t.usd / total * 100 : 0) + ' of the dollars' }; }, KINDS, 'usd', -1)}];
      if (aside.length) lists.push({ key: 'aside', title: 'Set aside', hint: 'not counted', tone: 'down', n: aside.length, rows: FL(aside, function (a) { return { name: a.ticker + (a.company ? DOT + a.company : ''), sub: (a.insider_name || '') + DOT + num(a.shares) + ' sh at $' + num(a.price, 2) + DOT + (a.reason || ''), big: money(a.value), tone: 'down', small: 'as filed' + DOT + 'not counted' }; }, ASIDE),
        note: 'A purchase is set aside only when its price is far off the stock\u2019s tape and it is worth more than the whole company.' });
      return {
        eyebrow: 'Insider buying tearsheet' + DOT + 'last ' + days + ' days to ' + day(d.generated_at), title: 'Insider buying: ' + money(total) + ' across ' + num(pop.length) + ' ' + whoSet + ' in ' + days + ' days' + DOT + multi.length + ' with two or more insiders, ' + strong.length + ' strong',
        sub: 'Open-market purchases only' + DOT + 'one row per company' + DOT + num(s.n_buy_transactions) + (hy ? ' purchase lines' : ' buy transactions read') + ', ' + num(nTick) + ' tickers with a buy' + (aside.length ? DOT + aside.length + ' impossible ' + (aside.length === 1 ? 'record' : 'records') + ' set aside' : ''),
        tiles: [{ k: 'Dollars bought', v: money(total), s: whoSet + ', ' + days + ' days' }, { k: 'Line items', v: num(pop.length), s: hy ? 'companies with a purchase' + DOT + scored.length + ' scored' : 'scored, of ' + num(nTick) + ' tickers with a buy' },
          { k: 'In top 10', v: pct(total ? sum(dollars.slice(0, 10), function (c) { return c.total_value; }) / total * 100 : 0, 0), s: 'of the dollars' },
          { k: 'Clusters / strong', v: '<span class="up">' + multi.length + '</span> / <span class="up">' + strong.length + '</span>', s: 'two or more insiders / score ' + strongAt + ' or more' },
          { k: 'CEO and CFO together', v: num(cnt('smart_money_dual')), s: 'both top officers buying' }, { k: 'The CEO is buying', v: num(ceo.length), s: 'companies where the chief executive bought' }],
        cards: [rich ? { tone: 'up', big: money(rich.total_value), eyebrow: 'Largest dollars' + (byTicker[rich.ticker] ? DOT + kind(byTicker[rich.ticker]) : ''), title: nm(rich), text: people(rich.n_insiders) + ' (' + (role(rich) || 'role not given') + ') bought ' + money(rich.total_value) + ', ' + num(rich.total_shares) + ' shares' + (avg(rich) ? ' at $' + num(avg(rich), 2) + ' on average' : '') + ', between ' + day(rich.first_buy) + ' and ' + day(rich.last_buy) + ': ' + pct(share(rich)) + ' of the dollars.' } : null,
          best ? { tone: 'up', big: fin(best.score) ? best.score.toFixed(0) : D, eyebrow: 'Strongest score' + DOT + kind(best), title: nm(best), text: best.rationale || '' } : null].filter(Boolean),
        notes: [['Who is buying:', typeRows.slice().sort(by(function (t) { return t.n; }, true)).map(function (t) { return t.name + ' ' + t.n; }).join(', ') + ' (scored companies).'], ['Most insiders in one name:', crowd[0] ? nm(crowd[0]) + ', ' + people(crowd[0].n_insiders) + ', ' + money(crowd[0].total_value) + '.' : D],
          ['Caveat:', 'the largest dollar lines are usually 10% holders, who are funds and parent companies rather than management; the score, not the dollars, is what weighs who bought.' + (hy && hy.n_off_tape ? ' ' + hy.n_off_tape + ' lines are for another class of security (preferred units, class A, a local line) and are counted at the price filed.' : '') + ' Sales are not on this sheet.']],
        ring: { open: 'dollars', title: 'Insider dollars', hint: 'top 6 of ' + num(pop.length), n: pop.length, center: money(total), subs: ['BOUGHT IN ' + days + ' DAYS', num(pop.length) + ' COMPANIES'], whole: 'the dollars', aria: 'Insider buying dollars by company',
          slices: top6.map(function (c) { return { name: nm(c), label: c.ticker, value: c.total_value, tone: 'pos', big: money(c.total_value), small: pct(share(c)) + ' of the dollars' + DOT + people(c.n_insiders) }; })
            .concat(restV > 0 ? [{ name: (pop.length - top6.length) + ' other companies', label: 'other', value: restV, tone: 'oth', big: money(restV), small: pct(restV / total * 100) + ' of the dollars' }] : []), caption: 'Slices are shares of the purchase dollars' + (hy ? ' of every company with a purchase.' : ' in the scored set.') },
        lists: lists,
        foot: 'Source: SEC Form 4 open-market purchases (' + (d.source_used || 'insider bank') + '), ' + days + ' days to ' + day(d.generated_at) + '. '
          + (hy ? 'Dollars are every purchase line the engine read, one row per filing line and per company (' + num(hy.n_feed_purchase_rows) + ' feed rows were ' + num(hy.n_filing_lines) + ' lines)' + (aside.length ? '; ' + aside.length + ' impossible ' + (aside.length === 1 ? 'record' : 'records') + ' (' + money(hy.usd_set_aside) + ' as filed) set aside and listed' : '') + '. Prices are checked against the factor book\u2019s tape.'
            : 'Dollars and counts are the engine\u2019s scored set, one row per company by SEC CIK' + (left ? '; ' + num(left) + ' other tickers with a purchase record were not scored and are left out' : '') + '.')
          + ' Purchases under $' + num(th.min_buy_value_usd || 5000) + ' are left out. The score is the engine\u2019s own, 0 to 100.'
      };
    }
  };

  ADAPTERS.tic = {
    feeds: ['/data/tic-flows.json'],
    build: function (docs) {
      var d = docs[0], T = d.total_foreign_holdings || {}, B = 1e9, total = T.current_b, np = d.net_purchases || {};
      var rows = Object.keys(d.individual || {}).map(function (k) { return d.individual[k]; }).filter(function (r) { return fin(r.current_b); }).sort(by(function (r) { return r.current_b; }, true));
      var c3 = function (r) { return r['3mo_change_b']; }, w = function (r) { return total ? r.current_b / total * 100 : 0; }, named = sum(rows, function (r) { return r.current_b; });
      var up = rows.filter(function (r) { return c3(r) > 0; }).sort(by(c3, true)), dn = rows.filter(function (r) { return c3(r) < 0; }).sort(by(c3));
      var yoy = rows.filter(function (r) { return fin(r.yoy_change_b); }).sort(by(function (r) { return Math.abs(r.yoy_change_b); }, true));
      var top6 = rows.slice(0, 6), otherNamed = named - sum(top6, function (r) { return r.current_b; }), rest = total - named, when = month(T.as_of || d.observation_date);
      var HOLD = sheet([tx('name', 'Jurisdiction', 'country'), nx('now', 'Holdings', function (r) { return r.current_b * B; }, fM), nx('w', '% of the total', w, fP(1)), nx('c3', '3 months', function (r) { return fin(c3(r)) ? c3(r) * B : null; }, fMs, { tone: true }),
        nx('c12', '1 year', function (r) { return fin(r.yoy_change_b) ? r.yoy_change_b * B : null; }, fMs, { tone: true }), nx('p12', '1 year, %', 'yoy_pct', fP(1, true), { tone: true })], ['jurisdiction', 'jurisdictions'], { find: 'Find a jurisdiction' });
      var row3 = function (r) { return { name: r.country, sub: 'holds ' + money(r.current_b * B) + DOT + pct(w(r)) + ' of all foreign holdings', big: money(c3(r) * B, true), tone: tone(c3(r)), small: 'in 3 months' }; };
      var card = function (r, kind) {
        if (!r) return null;
        return { tone: kind, big: money(c3(r) * B, true), eyebrow: 'Largest 3-month ' + (kind === 'up' ? 'rise' : 'fall') + ' in holdings', title: r.country,
          text: r.country + '\u2019s reported holdings went ' + (kind === 'up' ? 'up' : 'down') + ' ' + money(Math.abs(c3(r)) * B) + ' in three months to ' + money(r.current_b * B) + (fin(r.yoy_change_b) ? '; ' + money(r.yoy_change_b * B, true) + ' (' + pct(r.yoy_pct, 1, true) + ') over a year.' : '.') };
      };
      return {
        eyebrow: 'Foreign holders tearsheet' + DOT + when + DOT + 'Treasury TIC', title: 'Foreign holders of US Treasuries: ' + money(total * B) + DOT + money(T['3mo_change_b'] * B, true) + ' in three months, ' + money(T.yoy_change_b * B, true) + ' over a year',
        sub: 'Reported holdings by where the securities are held' + DOT + rows.length + ' named jurisdictions' + DOT + 'a change in holdings includes price moves, it is not the same as buying or selling',
        tiles: [{ k: 'Foreign holdings', v: money(total * B), s: 'all holders, ' + when }, { k: 'Line items', v: num(rows.length), s: 'named jurisdictions' }, { k: 'In top 3', v: pct(sum(rows.slice(0, 3), w), 0), s: rows.slice(0, 3).map(function (r) { return r.country; }).join(', ') },
          { k: 'Holdings up / down', v: '<span class="up">' + up.length + '</span> / <span class="down">' + dn.length + '</span>', s: 'jurisdictions, 3 months' },
          { k: 'Change over a year', v: '<span class="' + tone(T.yoy_change_b) + '">' + money(T.yoy_change_b * B, true) + '</span>', s: pct(T.yoy_pct, 1, true) + ', all holders' },
          { k: 'Net purchases, ' + month(np.latest_date), v: '<span class="' + tone(np.latest_month_m) + '">' + money(np.latest_month_m * 1e6, true) + '</span>', s: 'transactions' + DOT + '12 months ' + money(np.trailing_12mo_m * 1e6, true) }],
        cards: [card(dn[0], 'down'), card(up[0], 'up')].filter(Boolean),
        notes: [['Largest holder:', rows[0] ? rows[0].country + ', ' + money(rows[0].current_b * B) + ' (' + pct(w(rows[0])) + ' of all foreign holdings).' : D],
          ['Holdings are not transactions:', d.interpretation || 'holdings changes include valuation and other adjustments.'], ['Coverage:', (d.holder_coverage || '') + ' The ' + rows.length + ' named here hold ' + pct(total ? named / total * 100 : 0, 0) + ' of the total.']],
        ring: { open: 'holders', title: 'Who holds them', hint: 'top 6 of ' + rows.length + ' named', n: rows.length, center: money(total * B), subs: ['FOREIGN HOLDINGS', when.toUpperCase()], whole: 'foreign holdings', aria: 'Foreign holdings of US Treasuries by jurisdiction',
          slices: top6.map(function (r) { return { name: r.country, label: r.country.split(',')[0].replace('United Kingdom', 'UK').slice(0, 10), value: r.current_b, tone: 'pos', big: pct(w(r)), small: money(r.current_b * B) + DOT + '3m ' + money(c3(r) * B, true) }; })
            .concat(otherNamed > 0 ? [{ name: (rows.length - 6) + ' other named jurisdictions', label: (rows.length - 6) + ' more', value: otherNamed, tone: 'oth', big: pct(otherNamed / total * 100), small: money(otherNamed * B) }] : [])
            .concat(rest > 0 ? [{ name: 'All other holders', label: 'unnamed', value: rest, tone: 'oth', big: pct(rest / total * 100), small: money(rest * B) + DOT + 'not named in the feed' }] : []),
          caption: 'Slices are shares of all foreign holdings of US Treasuries. Custody and residence, not ultimate ownership.' },
        lists: [{ key: 'up', title: 'Holdings up', hint: '3-month change', tone: 'up', rows: FL(up, row3, HOLD, 'c3', -1), none: 'no named jurisdiction rose' }, { key: 'down', title: 'Holdings down', hint: '3-month change', tone: 'down', rows: FL(dn, row3, HOLD, 'c3', 1), none: 'no named jurisdiction fell' },
          { key: 'holders', title: 'Largest holders', hint: 'share of the total', rows: FL(rows, function (r) { return { name: r.country, sub: '3m ' + money(c3(r) * B, true) + (fin(r.yoy_change_b) ? DOT + '1y ' + money(r.yoy_change_b * B, true) : ''), big: money(r.current_b * B), small: pct(w(r)) + ' of the total' }; }, HOLD, 'now', -1)},
          { key: 'yoy', title: 'Over a year', hint: 'largest changes either way', rows: FL(yoy, function (r) { return { name: r.country, sub: 'holds ' + money(r.current_b * B), big: money(r.yoy_change_b * B, true), tone: tone(r.yoy_change_b), small: pct(r.yoy_pct, 1, true) }; }, HOLD)}],
        foot: 'Source: US Treasury International Capital (TIC) system, major foreign holders of Treasury securities, ' + when + ' (read ' + day((d.quality || {}).acquired_at || d.generated_at) + '). ' + (T.scope || '') + ' Research measurements, not a forecast or a trade signal.'
      };
    }
  };

  ADAPTERS.capital = {
    feeds: ['/data/capital-inflows.json'],
    build: function (docs) {
      var d = docs[0], h = d.headline || {}, B = 1e9, names = { treasuries: 'Treasury bonds and notes', agency_bonds: 'Agency bonds', corporate_bonds: 'Corporate bonds', equities: 'Equities' }, short = { treasuries: 'Treasuries', agency_bonds: 'Agency', corporate_bonds: 'Corporate', equities: 'Equities' };
      var cls = Object.keys(d.by_asset_class || {}).map(function (k) { var v = d.by_asset_class[k]; return { key: k, name: names[k] || k, label: short[k] || k, m: v.latest_month_b, y: v.rolling_12mo_b, id: v.series_id }; }).filter(function (c) { return fin(c.m); });
      var buys = cls.filter(function (c) { return c.m > 0; }).sort(by(function (c) { return c.m; }, true)), sells = cls.filter(function (c) { return c.m < 0; }).sort(by(function (c) { return c.m; }));
      var inn = sum(buys, function (c) { return c.m; }), out = -sum(sells, function (c) { return c.m; }), gross = inn + out, net = fin(h.latest_month_b) ? h.latest_month_b : inn - out, when = month(d.data_asof);
      var lt = (d.holder_splits || {}).lt_total || {}, off = lt.official || {}, prv = lt.private || {}, hist = (d.history_12mo_rolling_b || []).slice(0, 12);
      var TYPE = sheet([tx('name', 'Security type', 'name'), nx('m', when, function (c) { return c.m * B; }, fMs, { tone: true }), nx('share', '% of the month\u2019s gross', function (c) { return gross ? Math.abs(c.m) / gross * 100 : null; }, fP(1)),
        nx('y', '12 months', function (c) { return fin(c.y) ? c.y * B : null; }, fMs, { tone: true }), tx('id', 'Series', 'id')], ['security type', 'security types']);
      var prints = (d.history_12mo_rolling_b || []).map(function (x, i, list) { var p = list[i + 1]; return { asof: x.asof, v: x.rolling_12mo_b, chg: p && fin(x.rolling_12mo_b) && fin(p.rolling_12mo_b) ? x.rolling_12mo_b - p.rolling_12mo_b : null }; });
      var PRINT = sheet([tx('asof', '12 months to', 'asof', { fmt: function (x) { return month(x.asof); } }), nx('v', '12-month total', function (x) { return fin(x.v) ? x.v * B : null; }, fMs, { tone: true }),
        nx('chg', 'Against the print before', function (x) { return fin(x.chg) ? x.chg * B : null; }, fMs, { tone: true })], ['print', 'prints']);
      var row = function (c) { return { name: c.name, sub: '12 months ' + money(c.y * B, true) + DOT + pct(gross ? Math.abs(c.m) / gross * 100 : 0) + ' of the month\u2019s gross', big: money(c.m * B, true), tone: tone(c.m), small: when }; };
      var card = function (c, kind) {
        if (!c) return { tone: kind, big: D, eyebrow: 'Most notable ' + (kind === 'up' ? 'buying' : 'selling') + DOT + when, title: 'None', text: 'Foreign investors were not net ' + (kind === 'up' ? 'buyers' : 'sellers') + ' of any of the four security types in ' + when + '.' };
        return { tone: kind, big: money(c.m * B, true), eyebrow: 'Most notable ' + (kind === 'up' ? 'buying' : 'selling') + DOT + when, title: c.name,
          text: 'Foreign investors were net ' + (kind === 'up' ? 'buyers' : 'sellers') + ' of ' + money(Math.abs(c.m) * B) + ' of ' + c.name.toLowerCase() + ' in ' + when + '; ' + money(c.y * B, true) + ' over the twelve months to ' + when + '.' };
      };
      var side = function (list, t) { return list.map(function (c) { return { name: c.name, label: c.label, value: Math.abs(c.m), tone: t, big: money(c.m * B, true), small: '12 months ' + money(c.y * B, true) }; }); };
      return {
        eyebrow: 'Cross-border tearsheet' + DOT + when + DOT + 'Treasury TIC', title: 'Foreign investors were net ' + (net >= 0 ? 'buyers' : 'sellers') + ' of ' + money(Math.abs(net) * B) + ' of US long-term securities in ' + when + DOT + money(h.foreign_net_into_us_lt_12mo_b * B, true) + ' over twelve months',
        sub: 'Reported monthly transactions by security type' + DOT + 'net purchases by foreign residents' + DOT + 'dated ' + when + ', published with a lag of about ten weeks',
        tiles: [{ k: 'Net, ' + when, v: '<span class="' + tone(net) + '">' + money(net * B, true) + '</span>', s: 'US long-term securities' }, { k: 'Line items', v: num(cls.length), s: 'security types' },
          { k: 'Twelve months', v: '<span class="' + tone(h.foreign_net_into_us_lt_12mo_b) + '">' + money(h.foreign_net_into_us_lt_12mo_b * B, true) + '</span>', s: money(h.yoy_change_12mo_b * B, true) + ' against a year earlier' },
          { k: 'Bought / sold', v: '<span class="up">' + buys.length + '</span> / <span class="down">' + sells.length + '</span>', s: 'security types, ' + when },
          { k: 'Private buyers', v: '<span class="' + tone(prv.latest) + '">' + money(prv.latest * B, true) + '</span>', s: 'official holders ' + money(off.latest * B, true) },
          { k: 'Net of US buying abroad', v: '<span class="' + tone(h.net_cross_border_lt_12mo_b) + '">' + money(h.net_cross_border_lt_12mo_b * B, true) + '</span>', s: 'twelve months, both directions' }],
        cards: [card(sells[0], 'down'), card(buys[0], 'up')],
        notes: [['Who bought:', 'private investors ' + money(prv.latest * B, true) + ' and official holders ' + money(off.latest * B, true) + ' in ' + when + '; ' + money(prv.sum_12m * B, true) + ' and ' + money(off.sum_12m * B, true) + ' over twelve months.'],
          ['Run rate:', 'the last three months annualise to ' + money(h.run_rate_3mo_annualized_b * B, true) + '. ' + ((d.exact_headline || {}).annualization_note || '')], ['Caveat:', (d.regime_interpretation || '') + ' ' + (d.vintage_note || '')]],
        ring: { open: 'year', title: 'The month by security type', hint: 'bought solid, sold hatched', n: cls.length, center: money(gross * B), subs: ['GROSS, ' + when.toUpperCase(), 'IN ' + pct(gross ? inn / gross * 100 : 0, 0) + DOT + 'OUT ' + pct(gross ? out / gross * 100 : 0, 0)], whole: 'the gross', aria: 'Net foreign transactions by security type',
          slices: side(buys, 'up').concat(side(sells, 'down')), caption: 'Solid = net bought, hatched = net sold. Each slice is a share of the gross (' + money(gross * B) + '); the net is ' + money(net * B, true) + '.' },
        lists: [{ key: 'bought', title: 'Bought', hint: 'net purchases, ' + when, tone: 'up', rows: FL(buys, row, TYPE, 'm', -1), none: 'nothing was net bought' }, { key: 'sold', title: 'Sold', hint: 'net sales, ' + when, tone: 'down', rows: FL(sells, row, TYPE, 'm', 1), none: 'nothing was net sold' },
          { key: 'year', title: 'Twelve months by type', hint: 'to ' + when, rows: FL(cls.slice().sort(by(function (c) { return c.y; }, true)), function (c) { return { name: c.name, sub: when + ' ' + money(c.m * B, true) + DOT + c.id, big: money(c.y * B, true), tone: tone(c.y), small: '12 months' }; }, TYPE, 'y', -1)},
          { key: 'history', title: 'Twelve-month total', hint: 'each print, newest first', rows: FL(hist, function (x, i) { var p = hist[i + 1]; return { name: month(x.asof), sub: p ? money((x.rolling_12mo_b - p.rolling_12mo_b) * B, true) + ' against ' + month(p.asof) : '', big: money(x.rolling_12mo_b * B, true), small: '12 months to date' }; }, PRINT, 'asof', -1, { rows: prints })}],
        foot: 'Source: US Treasury International Capital (TIC) system, transactions in long-term securities, ' + when + ' (read ' + day(d.generated_at) + '). ' + (d.disclaimer || '')
      };
    }
  };

  /* Leaders desk: the five largest companies of each industry against the S&P 500 (data/leaders.json, justhodl-leaders).
     The ring is the leaders' market cap by the tape label the engine gave each one - every leader once, one unit, one date.
     Population = the engine's own "ranked" set (a row with a read against SPY that is not stale), so every count on the sheet
     is the count the desk's verdict tiles print. The window buttons move the horizon of the lists that have one. */
  ADAPTERS.leaders = {
    feeds: ['/data/leaders.json'],
    share: { global: 'JHLeadersDoc', event: 'jh:leaders-doc', ms: 8000 },   /* the desk has already read this 3 MB feed: use its copy */
    build: function (docs, st) {
      var d = docs[0], S = d.summary || {}, bench = (d.benchmark || {}).h || {}, f13 = d.f13 || {}, ch = d.changes || {};
      var WIN = { w: { label: '1 WEEK', word: 'the week', flow: 'w5', obs: 5, span: 9 }, m: { label: '1 MONTH', word: 'the month', flow: 'w21', obs: 21, span: 33 }, q: { label: '1 QUARTER', word: 'the quarter', flow: 'w63', obs: 63, span: 95 } };
      var k = WIN[st.fund] ? st.fund : 'm', W = WIN[k];
      var live = (d.rows || []).filter(function (r) { return r.vs && !r.stale; }), funds = (d.etfs || []).filter(function (f) { return !f.stale; });
      var cap = function (r) { return fin(r.mcap) ? r.mcap : 0; }, lab = function (r) { return (r.ad || {}).label || 'NEUTRAL'; }, low = function (s) { return String(s || '').toLowerCase(); };
      var sg = function (v, dp) { return fin(v) ? sign(v, true) + Math.abs(v).toFixed(dp == null ? 1 : dp) : D; }, pts = function (v) { return fin(v) ? sg(v, 1) + ' pts' : D; };
      var nm = function (r) { return r.ticker + (r.name ? DOT + r.name : ''); }, sentence = function (s) { s = low(s); return s.charAt(0).toUpperCase() + s.slice(1); };
      /* -- breadth against the S&P 500 on the chosen window -- */
      var read = live.filter(function (r) { return fin(r.vs[k]); }), beat = read.filter(function (r) { return r.vs[k] > 0; }).length, beatPct = read.length ? beat / read.length * 100 : null;
      var strong = read.slice().sort(by(function (r) { return r.vs[k]; }, true)), weak = read.slice().sort(by(function (r) { return r.vs[k]; }));
      var vsRow = function (r) { return { name: nm(r), sub: (r.industry || '') + DOT + low(lab(r)), big: pts(r.vs[k]), tone: tone(r.vs[k]), small: 'own ' + pct((r.h || {})[k], 1, true) }; };
      /* -- the tape: five labels, market cap and count -- */
      var GRADES = [['STRONG ACCUMULATION', 'Strong accumulation', 'Strong acc.', 'up', 1], ['ACCUMULATION', 'Accumulation', 'Accum.', 'up', 0.6], ['NEUTRAL', 'Neutral', 'Neutral', 'oth', 0.34],
        ['DISTRIBUTION', 'Distribution', 'Distrib.', 'down', 0.6], ['STRONG DISTRIBUTION', 'Strong distribution', 'Strong dist.', 'down', 1]];
      var total = sum(live, cap), tier = {};
      GRADES.forEach(function (g) { var list = live.filter(function (r) { return lab(r) === g[0]; }).sort(by(cap, true)); tier[g[0]] = { list: list, cap: sum(list, cap) }; });
      var accN = tier['STRONG ACCUMULATION'].list.length + tier.ACCUMULATION.list.length, distN = tier['STRONG DISTRIBUTION'].list.length + tier.DISTRIBUTION.list.length;
      var accCap = tier['STRONG ACCUMULATION'].cap + tier.ACCUMULATION.cap, distCap = tier['STRONG DISTRIBUTION'].cap + tier.DISTRIBUTION.cap;
      var share = function (v) { return total ? v / total * 100 : 0; };
      var mf = function (r) { return ((r.mf || {})[k] || {}).net; };
      var mfIn = sum(live, function (r) { var v = mf(r); return v > 0 ? v : 0; }), mfOut = -sum(live, function (r) { var v = mf(r); return v < 0 ? v : 0; }), mfNet = mfIn - mfOut;
      var scored = live.filter(function (r) { return fin((r.ad || {}).score); });
      var accList = scored.filter(function (r) { return r.ad.score >= 20; }).sort(by(function (r) { return r.ad.score; }, true)), distList = scored.filter(function (r) { return r.ad.score <= -20; }).sort(by(function (r) { return r.ad.score; }));
      var tapeRow = function (r) { return { name: nm(r), sub: (r.industry || '') + DOT + 'signed dollar volume ' + money(mf(r), true), big: sg(r.ad.score, 0), tone: tone(r.ad.score), small: low(r.ad.label) }; };
      var notable = function (key, kind) {
        var r = tier[key].list[0], when = day(d.as_of);
        if (!r) return { tone: kind, big: D, eyebrow: 'Most notable ' + (kind === 'up' ? 'accumulation' : 'distribution') + DOT + when, title: 'None', text: 'No leader carries the label ' + low(key) + ' on ' + when + '.' };
        var a = r.ad || {}, sm = (r.smart || {}).verdict;
        return { tone: kind, big: sg(a.score, 0), eyebrow: 'Most notable ' + (kind === 'up' ? 'accumulation' : 'distribution') + DOT + 'the largest leader under ' + low(key), title: nm(r),
          text: money(cap(r)) + ' of market cap, ' + (r.industry || '') + '. Tape score ' + sg(a.score, 1) + ' (' + low(a.label) + '): ' + (fin(a.acc_days) ? a.acc_days + ' accumulation and ' + a.dist_days + ' distribution sessions in the last 25, ' : '')
            + 'signed dollar volume ' + money(mf(r), true) + ' over ' + W.word + '. ' + pts(r.vs[k]) + ' against the S&P 500 over ' + W.word + ' (own ' + pct((r.h || {})[k], 1, true) + ').' + (sm ? ' Smart money: ' + low(sm) + '.' : '') };
      };
      /* -- smart money: the 13F cohort and Form 4 -- */
      var net13 = function (r) { return (r.f13 || {}).net_usd; }, verdict = function (r) { return (r.smart || {}).verdict; };
      var in13 = sum(live, function (r) { var v = net13(r); return v > 0 ? v : 0; }), out13 = -sum(live, function (r) { var v = net13(r); return v < 0 ? v : 0; });
      var buying = live.filter(function (r) { return verdict(r) === 'BUYING'; }).sort(by(function (r) { return fin(net13(r)) ? net13(r) : -Infinity; }, true));
      var selling = live.filter(function (r) { return verdict(r) === 'SELLING'; }).sort(by(function (r) { return fin(net13(r)) ? net13(r) : Infinity; }));
      var smartRow = function (r) {
        var f = r.f13 || {}, i = r.insider || {}, bits = [r.industry || ''];
        if (fin(f.n_holders)) bits.push(f.n_holders + ' of the cohort hold it' + (f.holder_delta ? ' (' + sg(f.holder_delta, 0) + ')' : ''));
        if (i.buy_usd > 0) bits.push('insiders bought ' + money(i.buy_usd));
        else if (i.sell_usd > 0 && verdict(r) === 'SELLING') bits.push('insiders sold ' + money(i.sell_usd));
        return { name: nm(r), sub: bits.join(DOT), big: money(net13(r), true), tone: tone(net13(r)), small: '13F net' };
      };
      /* -- industries -- */
      var inds = (d.industries || []).filter(function (x) { return x.vs && fin(x.vs[k]); });
      var indRow = function (x) { return { name: x.industry, sub: (x.leaders || []).join(', ') + DOT + money(x.mcap_usd), big: pts(x.vs[k]), tone: tone(x.vs[k]), small: low((x.ad || {}).label) }; };
      /* -- industry funds: reported creations and redemptions. A window that holds a share split is not money in or out:
            the vendor's own flow for the split day is the jump in the share count at the old price (ops 6839: SOXX and ITA,
            3-for-1 on 2026-11-05, reported +$89.5B and +$24.5B on that one day; their other days are ordinary). Redemptions
            beyond a small fund's remaining assets (IYZ, IDRV) are real share-count falls at an unchanged NAV and stay. -- */
      var flowOf = function (f) { return ((f.flow || {})[W.flow]) || {}; };
      var aside = function (f) {
        var fl = f.flow || {}, w = flowOf(f), end = Date.parse(w.end || fl.effective || ''), start = w.start ? Date.parse(w.start) : end - W.span * 864e5;   /* a window without its first date: the calendar span its reports cover, generously */
        var sp = (f.splits || []).filter(function (s) { var t = Date.parse(s.date); return t >= start && t <= end + 864e5; })[0];
        if (sp) return (sp.split_to && sp.split_from ? sp.split_to + '-for-' + sp.split_from + ' ' : '') + 'share split on ' + day(sp.date) + ' inside the window';
        if (w.usd > 0 && w.pct_aum > 100) return 'creations larger than the fund\u2019s own assets';
        return null;
      };
      var flowing = funds.filter(function (f) { return (f.flow || {}).current && fin(flowOf(f).usd); }), kept = [], out = [];
      flowing.forEach(function (f) { var why = aside(f); if (why) out.push({ f: f, why: why }); else kept.push(f); });
      var fin_ = kept.filter(function (f) { return flowOf(f).usd > 0; }).sort(by(function (f) { return flowOf(f).usd; }, true)), fout = kept.filter(function (f) { return flowOf(f).usd < 0; }).sort(by(function (f) { return flowOf(f).usd; }));
      var fundIn = sum(fin_, function (f) { return flowOf(f).usd; }), fundOut = -sum(fout, function (f) { return flowOf(f).usd; });
      var fundRow = function (f) { var w = flowOf(f), cov = f.industries || []; return { name: f.ticker, sub: (f.role || 'fund') + ' fund' + DOT + (cov.length > 2 ? cov.slice(0, 2).join(', ') + ' and ' + (cov.length - 2) + ' more' : cov.join(', ')), big: money(w.usd, true), tone: tone(w.usd), small: fin(w.pct_aum) ? sg(w.pct_aum, 2) + '% of assets' : '' }; };
      var asideNote = out.length ? 'Set aside, not counted: ' + out.map(function (o) { return o.f.ticker + ' ' + money(flowOf(o.f).usd, true) + ' (' + o.why + ')'; }).join('; ') + '. A split multiplies the reported share count, so that window\u2019s figure is arithmetic, not money.' : '';
      /* -- what was printed since the prior session -- */
      var ORDER = ['GOLDEN CROSS', 'DEATH CROSS', 'GOLDEN NEAR', 'DEATH NEAR', 'DOUBLE BOTTOM CONFIRMED', 'DOUBLE TOP CONFIRMED', 'DOUBLE BOTTOM FORMING', 'DOUBLE TOP FORMING', 'BUYING CLIMAX', 'SELLING CLIMAX', 'STRONG ACCUMULATION', 'STRONG DISTRIBUTION',
        'ACCUMULATION', 'DISTRIBUTION', 'SMART BUYING', 'SMART SELLING', 'RS HIGH', 'RS LOW', 'BULL STACK', 'BEAR STACK', 'VERY TIGHT', 'TIGHT'];
      var UP = { 'GOLDEN CROSS': 1, 'GOLDEN NEAR': 1, 'DOUBLE BOTTOM CONFIRMED': 1, 'DOUBLE BOTTOM FORMING': 1, 'STRONG ACCUMULATION': 1, 'ACCUMULATION': 1, 'SMART BUYING': 1, 'RS HIGH': 1, 'BULL STACK': 1 };
      var DOWN = { 'DEATH CROSS': 1, 'DEATH NEAR': 1, 'DOUBLE TOP CONFIRMED': 1, 'DOUBLE TOP FORMING': 1, 'STRONG DISTRIBUTION': 1, 'DISTRIBUTION': 1, 'SMART SELLING': 1, 'RS LOW': 1, 'BEAR STACK': 1 };
      var group = function (list) { var g = {}; (list || []).forEach(function (x) { (g[x.signal] = g[x.signal] || []).push(x.ticker); }); return g; };
      var gNew = group(ch['new']), gGone = group(ch.gone), nNew = fin(ch.n_new) ? ch.n_new : (ch['new'] || []).length, nGone = fin(ch.n_gone) ? ch.n_gone : (ch.gone || []).length;
      var kinds = Object.keys(gNew).sort(function (a, b) { var ia = ORDER.indexOf(a), ib = ORDER.indexOf(b); return (ia < 0 ? 99 : ia) - (ib < 0 ? 99 : ib) || gNew[b].length - gNew[a].length; });
      var newRows = kinds.map(function (s) { var t = gNew[s]; return { name: sentence(s), sub: t.slice(0, 8).join(', ') + (t.length > 8 ? ' and ' + (t.length - 8) + ' more' : ''), big: num(t.length), tone: UP[s] ? 'up' : DOWN[s] ? 'down' : '', small: 'new' }; });
      var goneTop = Object.keys(gGone).sort(function (a, b) { return gGone[b].length - gGone[a].length; }).slice(0, 4).map(function (s) { return gGone[s].length + ' ' + low(s); });
      /* the full view's columns: a leader, a signal printed, an industry, a fund */
      var byTk = {}, span = W.word.replace('the ', '');
      (d.rows || []).forEach(function (r) { byTk[r.ticker] = r; });
      var LEAD = sheet([tx('t', 'Ticker', 'ticker', { find: 'start' }), tx('name', 'Name', 'name'), tx('ind', 'Industry', 'industry'), nx('mcap', 'Mkt cap', function (r) { return fin(r.mcap) ? r.mcap : null; }, fM),
        nx('vs', 'vs S&P 500, ' + span, function (r) { return (r.vs || {})[k]; }, fS(1, ' pts'), { tone: true, tip: 'the stock\u2019s return less the S&P 500\u2019s over ' + W.word + ', in percentage points' }),
        nx('own', 'Own return, ' + span, function (r) { return (r.h || {})[k]; }, fP(1, true), { tone: true }), nx('score', 'Tape score', function (r) { return (r.ad || {}).score; }, fS(0), { tone: true, tip: 'accumulation above zero, distribution below' }),
        tx('tape', 'Tape', function (r) { return sentence(lab(r)); }), nx('mf', 'Signed $ volume', mf, fMs, { tone: true, tip: 'dollar volume on up days less dollar volume on down days over ' + W.word }),
        tx('smart', 'Smart money', function (r) { var v = verdict(r); return v ? sentence(v) : null; }), nx('f13', '13F net', net13, fMs, { tone: true }), nx('hold', '13F holders', function (r) { return (r.f13 || {}).n_holders; }, fN(0)),
        nx('dh', 'Holders, change', function (r) { return (r.f13 || {}).holder_delta; }, fNs(0), { tone: true }), nx('ib', 'Insiders bought', function (r) { return (r.insider || {}).buy_usd; }, fM), nx('is', 'Insiders sold', function (r) { return (r.insider || {}).sell_usd; }, fM)],
        ['leader', 'leaders'], { href: function (r) { return chartOf(r.ticker); }, find: 'Find a ticker, name or industry', phone: ['t', 'vs', 'own', 'score', 'mcap', 'tape', 'ind', 'name'] });
      var rankOf = function (sg) { var i = ORDER.indexOf(sg); return i < 0 ? 99 : i; };
      var prints = (ch['new'] || []).map(function (x) { return { ticker: x.ticker, signal: x.signal, r: byTk[x.ticker] || {} }; })
        .sort(function (a, b) { return rankOf(a.signal) - rankOf(b.signal) || String(a.signal).localeCompare(String(b.signal)) || String(a.ticker).localeCompare(String(b.ticker)); });
      var PRINTED = sheet([tx('t', 'Ticker', 'ticker', { find: 'start' }), tx('name', 'Name', function (x) { return x.r.name; }), tx('sig', 'Signal', function (x) { return sentence(x.signal); }, { tone: function (x) { return UP[x.signal] ? 'up' : DOWN[x.signal] ? 'down' : ''; } }),
        tx('ind', 'Industry', function (x) { return x.r.industry; }), nx('mcap', 'Mkt cap', function (x) { return fin(x.r.mcap) ? x.r.mcap : null; }, fM), nx('vs', 'vs S&P 500, ' + span, function (x) { return (x.r.vs || {})[k]; }, fS(1, ' pts'), { tone: true }),
        nx('score', 'Tape score', function (x) { return (x.r.ad || {}).score; }, fS(0), { tone: true })], ['print', 'prints'], { href: function (x) { return chartOf(x.ticker); }, find: 'Find a ticker, name, signal or industry', phone: ['t', 'sig', 'vs', 'mcap', 'name'] });
      var INDS = sheet([tx('name', 'Industry', 'industry'), nx('vs', 'vs S&P 500, ' + span, function (x) { return x.vs[k]; }, fS(1, ' pts'), { tone: true }), nx('mcap', 'Mkt cap', 'mcap_usd', fM), nx('n', 'Leaders', function (x) { return (x.leaders || []).length; }, fN(0)),
        tx('who', 'Its leaders', function (x) { return (x.leaders || []).join(', '); }), tx('tape', 'Tape', function (x) { var l = (x.ad || {}).label; return l ? sentence(l) : null; })], ['industry', 'industries'], { find: 'Find an industry or a ticker' });
      var FUND = sheet([tx('t', 'Ticker', 'ticker', { find: 'start' }), tx('role', 'Kind', function (f) { return (f.role || 'fund') + ' fund'; }), nx('usd', 'Net flow', function (f) { return flowOf(f).usd; }, fMs, { tone: true }), nx('aum', '% of assets', function (f) { return flowOf(f).pct_aum; }, fS(2, '%'), { tone: true }),
        nx('n', 'Industries', function (f) { return (f.industries || []).length; }, fN(0)), tx('cov', 'Covers', function (f) { return (f.industries || []).join(', '); }), tx('end', 'Reports to', function (f) { var e = flowOf(f).end; return e ? day(e) : null; })],
        ['fund', 'funds'], { href: function (f) { return chartOf(f.ticker); }, find: 'Find a fund or an industry' });
      var bl = (d.boards || {}).leaders || {}, n = function (key) { return (bl[key] || []).length; };
      var conf = function (key) { return (d.rows || []).filter(function (r) { return r[key] && r[key].state === 'CONFIRMED'; }).length; };
      return {
        eyebrow: 'Leaders tearsheet' + DOT + 'bars ' + day(d.as_of) + DOT + num(live.length) + ' leaders ranked in ' + num((d.industries || []).length) + ' industries',
        title: pct(beatPct, 0) + ' of the leaders are ahead of the S&P 500 over ' + W.word + DOT + money(accCap) + ' of their market cap is under accumulation, ' + money(distCap) + ' under distribution',
        sub: 'The five largest companies of each industry against SPY' + DOT + 'tape labels read from price and volume' + DOT + '13F cohort of ' + num(f13.n_cohort) + ' as of ' + day(f13.period) + DOT + 'fund flows through ' + day((flowing[0] && (flowing[0].flow || {}).effective) || '') + DOT + 'published ' + day(d.generated_at),
        controls: ['w', 'm', 'q'].map(function (id) { return { id: id, label: WIN[id].label, on: id === k }; }),
        tiles: [{ k: 'Ahead of the S&P 500', v: pct(beatPct, 0), s: num(beat) + ' of ' + num(read.length) + ' over ' + W.word + DOT + 'SPY ' + pct(bench[k], 1, true) },
          { k: 'Market cap by tape', v: '<span class="up">' + pct(share(accCap), 0) + '</span> / <span class="down">' + pct(share(distCap), 0) + '</span>', s: 'under accumulation / distribution, of ' + money(total) },
          { k: 'Leaders by tape', v: '<span class="up">' + num(accN) + '</span> / <span class="down">' + num(distN) + '</span>', s: num(tier['STRONG ACCUMULATION'].list.length) + ' strong' + DOT + num(tier['STRONG DISTRIBUTION'].list.length) + ' strong' + DOT + num(tier.NEUTRAL.list.length) + ' neutral' },
          { k: 'Signed dollar volume', v: '<span class="' + tone(mfNet) + '">' + money(mfNet, true) + '</span>', s: W.word + DOT + 'in ' + money(mfIn) + DOT + 'out ' + money(mfOut) },
          { k: 'Smart money', v: '<span class="up">' + num(buying.length) + '</span> / <span class="down">' + num(selling.length) + '</span>', s: 'buying / selling' + DOT + '13F net ' + money(in13 - out13, true) },
          { k: 'Industry funds', v: '<span class="' + tone(fundIn - fundOut) + '">' + money(fundIn - fundOut, true) + '</span>', s: W.obs + ' reports' + DOT + fin_.length + ' in' + DOT + fout.length + ' out' + (out.length ? DOT + out.length + ' set aside' : '') }],
        cards: [notable('STRONG DISTRIBUTION', 'down'), notable('STRONG ACCUMULATION', 'up')],
        notes: [['Breadth:', 'ahead of the S&P 500 over a week ' + pct(S.pct_beating_w, 0) + ', a month ' + pct(S.pct_beating_m, 0) + ', a quarter ' + pct(S.pct_beating_q, 0) + ', six months ' + pct(S.pct_beating_h, 0) + ' and a year ' + pct(S.pct_beating_y, 0) + '. Closing above the 20-day average '
            + pct(S.pct_above_20, 0) + ', the 50-day ' + pct(S.pct_above_50, 0) + ', the 200-day ' + pct(S.pct_above_200, 0) + '; ' + n('golden_cross') + ' golden and ' + n('death_cross') + ' death crosses inside 20 sessions.'],
          ['Patterns:', n('double_top') + ' double tops (' + conf('dtop') + ' confirmed) and ' + n('double_bottom') + ' double bottoms (' + conf('dbot') + ' confirmed); ' + n('buying_climax') + ' buying and ' + n('selling_climax') + ' selling climaxes inside 30 sessions; ' + n('tight') + ' tight bases; '
            + n('rs_new_highs') + ' new highs and ' + n('rs_new_lows') + ' new lows of relative strength.'],
          ['Caveat:', 'the tape label and signed dollar volume are read from price and volume \u2014 an estimate of pressure, not a reported flow. The 13F cohort is ' + num(f13.n_cohort) + ' managers as of ' + day(f13.period) + ' and arrives 45 days or more after the quarter; newly disclosed is not bought. Fund flows are reported creations and redemptions.']],
        ring: { open: 'all', title: 'Market cap by tape', hint: 'accumulation solid, distribution hatched', n: live.length, center: money(total), subs: [num(live.length) + ' LEADERS', 'ACC ' + pct(share(accCap), 0) + DOT + 'DIST ' + pct(share(distCap), 0)], whole: 'the leaders\u2019 market cap', aria: 'The leaders\u2019 market cap by tape label',
          slices: GRADES.map(function (g, gi) { var t = tier[g[0]]; return { open: t.list.length ? 'tape-' + gi : '', name: g[1], label: g[2], value: t.cap, tone: g[3], op: g[4], big: money(t.cap), small: num(t.list.length) + ' leaders' + (t.list.length ? DOT + t.list.slice(0, 3).map(function (r) { return r.ticker; }).join(', ') : '') }; }),
          caption: 'Each leader\u2019s market cap is counted once, under the label the engine read from its price and volume on ' + day(d.as_of) + ' (score at or beyond \u00b120, strong at \u00b160). Solid = accumulation, hatched = distribution, plain = neutral.' },
        lists: [{ key: 'strongest', title: 'Strongest against the S&P 500', hint: W.word + ', percentage points', tone: 'up', n: beat, rows: FL(strong.filter(function (r) { return r.vs[k] > 0; }), vsRow, LEAD, 'vs', -1), none: 'no leader is ahead' },
          { key: 'weakest', title: 'Weakest against the S&P 500', hint: W.word + ', percentage points', tone: 'down', n: read.length - beat, rows: FL(weak.filter(function (r) { return r.vs[k] <= 0; }), vsRow, LEAD, 'vs', 1), none: 'no leader is behind' },
          { key: 'printed', title: 'Printed since ' + (ch.prev_as_of ? day(ch.prev_as_of) : 'the prior session'), hint: 'signals that appeared', n: nNew, rows: newRows, full: wholeOf(prints, PRINTED), none: ch.note || 'nothing new was printed',
            note: nGone ? num(nGone) + ' signals went away' + (goneTop.length ? ': ' + goneTop.join(', ') + (Object.keys(gGone).length > 4 ? ' and others' : '') : '') + '.' : '' },
          { key: 'accumulation', title: 'Under accumulation', hint: 'highest tape scores', tone: 'up', rows: FL(accList, tapeRow, LEAD, 'score', -1), none: 'no leader is under accumulation' },
          { key: 'distribution', title: 'Under distribution', hint: 'lowest tape scores', tone: 'down', rows: FL(distList, tapeRow, LEAD, 'score', 1), none: 'no leader is under distribution' },
          { key: 'smart-buying', title: 'Smart money buying', hint: '13F cohort and insiders', tone: 'up', rows: FL(buying, smartRow, LEAD, 'f13', -1), none: 'no leader reads as buying' },
          { key: 'smart-selling', title: 'Smart money selling', hint: '13F cohort and insiders', tone: 'down', rows: FL(selling, smartRow, LEAD, 'f13', 1), none: 'no leader reads as selling' },
          { key: 'industries-leading', title: 'Industries leading', hint: 'cap-weighted, ' + W.word, tone: 'up', rows: FL(inds.filter(function (x) { return x.vs[k] > 0; }).sort(by(function (x) { return x.vs[k]; }, true)), indRow, INDS, 'vs', -1), none: 'no industry is ahead' },
          { key: 'industries-lagging', title: 'Industries lagging', hint: 'cap-weighted, ' + W.word, tone: 'down', rows: FL(inds.filter(function (x) { return x.vs[k] <= 0; }).sort(by(function (x) { return x.vs[k]; })), indRow, INDS, 'vs', 1), none: 'no industry is behind' },
          { key: 'fund-inflows', title: 'Fund inflows', hint: 'reported creations, ' + W.obs + ' reports', tone: 'up', rows: FL(fin_, fundRow, FUND, 'usd', -1), none: 'no fund took money in', note: asideNote },
          { key: 'fund-outflows', title: 'Fund outflows', hint: 'reported redemptions, ' + W.obs + ' reports', tone: 'down', rows: FL(fout, fundRow, FUND, 'usd', 1), none: 'no fund lost money' }],
        more: [hidden('all', 'Every leader', 'largest first', live.slice().sort(by(cap, true)), LEAD, 'mcap', -1)]
          .concat(GRADES.map(function (g, gi) { return hidden('tape-' + gi, g[1], 'leaders the tape reads so, largest first', tier[g[0]].list, LEAD, 'mcap', -1); })),
        foot: 'Source: justhodl-leaders ' + (d.version || '') + ' (data/leaders.json), published ' + String(d.generated_at || '').slice(0, 16).replace('T', ' ') + ' UTC from the Polygon end-of-day bank, bars as of ' + day(d.as_of) + '; 13F period ' + day(f13.period) + '; Form 4 purchases and sales over '
          + ((d.insiders || {}).window_days || 90) + ' days to ' + day((d.insiders || {}).newest_day) + '. A leader is one of the five largest companies of its industry by market cap; excess return is against SPY in percentage points.'
      };
    }
  };

  /* -- the Wyckoff desks (wyckoff-desk.html, one sheet a tab) and the capitulation desk (capitulation-desk.html) --
     Selling climax, Capitulation, Buying climax, Candle spread and Spring read the feeds their tables already read;
     Accumulation, Bottom and Distribution read data/campaign-scan.json - the chart's own marks over every stock. */
  function med(list) { var a = list.filter(fin).sort(function (x, y) { return x - y; }); return a.length ? (a.length % 2 ? a[(a.length - 1) / 2] : (a[a.length / 2 - 1] + a[a.length / 2]) / 2) : null; }
  function groupBy(list, f) {
    var m = {}, order = [];
    list.forEach(function (x) { var k = f(x); if (!m[k]) { m[k] = []; order.push(k); } m[k].push(x); });
    return order.map(function (k) { return { key: k, list: m[k] }; }).sort(function (a, b) { return b.list.length - a.list.length || String(a.key).localeCompare(String(b.key)); });
  }
  function tick(r) { var n = r.name || r.company; return r.ticker + (n ? DOT + n : ''); }
  function agoWord(n) { return !fin(n) ? D : n === 0 ? 'last session' : n === 1 ? '1 session ago' : n + ' sessions ago'; }
  function plural(n, word) { return num(n) + ' ' + word + (n === 1 ? '' : 's'); }
  function zpct(v, dp, signed) { return fin(v) ? pct(+v.toFixed(dp == null ? 1 : dp) || 0, dp, signed) : D; }      /* a move that rounds to nothing carries no sign */
  var HARVEST = { ST_CONFIRMED: 'test confirmed', NO_RALLY: 'no rally', NO_TEST_BREAKOUT: 'breakout without a test' };
  function harvestWord(sv) { return HARVEST[sv] || lower(String(sv || '').replace(/_/g, ' ')); }
  function daysBetween(a, b) { var x = Date.parse(day(a) + 'T00:00:00Z'), y = Date.parse(day(b) + 'T00:00:00Z'); return isFinite(x) && isFinite(y) ? Math.round((y - x) / 864e5) : null; }
  function daysWord(n) { return !fin(n) ? D : n <= 0 ? 'today' : n === 1 ? '1 day ago' : n + ' days ago'; }
  function times(v, dp) { return fin(v) ? v.toFixed(dp == null ? 1 : dp) + '\u00d7' : D; }
  function capped(v) { return v === 12 ? '12\u00d7+' : times(v); }      /* the chart's scan stops counting climax volume at 12 times */
  function cap(r) { return fin(r.mcap) ? r.mcap : 0; }
  function share(n, of) { return of ? n / of * 100 : null; }
  function lower(s) { return String(s || '').toLowerCase(); }
  function compose(groups, nameOf, smallOf, keep) {
    var top = groups.slice(0, keep || 6), rest = groups.slice(keep || 6);
    var out = top.map(function (g) { return { name: nameOf(g.key), value: g.list.length, tone: 'pos', big: num(g.list.length), small: smallOf(g) }; });
    if (rest.length) {
      var n = sum(rest, function (g) { return g.list.length; });
      out.push({ name: 'Other (' + rest.length + ')', label: 'Other', value: n, tone: 'oth', rest: true, big: num(n), small: rest.slice(0, 4).map(function (g) { return nameOf(g.key) + ' ' + g.list.length; }).join(', ') });
    }
    return out;
  }
  function tickers(list, n) { return list.slice(0, n || 3).map(function (r) { return r.ticker; }).join(', '); }
  function busiest(rows, dateOf) { var g = groupBy(rows.filter(function (r) { return dateOf(r); }), function (r) { return day(dateOf(r)); })[0]; return g ? { date: g.key, n: g.list.length } : null; }
  var WY = {};

  /* Accumulation, Bottom, Distribution: every stock (or ETF) the chart's icon would mark */
  /* the full view of a scan board: what every row carries, then the tab's own columns */
  function scanSheet(p, cols) {
    var head = [tx('t', 'Ticker', 'ticker', { find: 'start' }), tx('name', 'Name', 'name')].concat(p.kind === 'etf' ? [] : [tx('sec', 'Sector', function (r) { return r.sector || 'Unclassified'; })],
      [nx('mcap', p.kind === 'etf' ? 'Assets' : 'Mkt cap', function (r) { return fin(r.mcap) ? r.mcap : null; }, fM)]);
    return sheet(head.concat(cols), [p.one, p.word], { href: function (r) { return chartOf(r.ticker); }, find: p.kind === 'etf' ? 'Find a ticker or name' : 'Find a ticker, name or sector' });
  }
  function scanSectors(d) {
    var scanned = function (g) { return ((d.by_sector || {})[g.key] || {}).scanned; };
    return sheet([tx('name', 'Sector', 'key'), nx('n', 'Marked', function (g) { return g.list.length; }, fN(0)), nx('of', 'Scanned', scanned, fN(0)), nx('share', '% of the scanned', function (g) { return scanned(g) ? share(g.list.length, scanned(g)) : null; }, fP(0)),
      nx('mcap', 'Mkt cap marked', function (g) { return sum(g.list, cap); }, fM), tx('top', 'Largest', function (g) { return tickers(g.list.slice().sort(by(cap, true)), 4); })], ['sector', 'sectors']);
  }
  var statusWord = function (r) { return r.status ? lower(r.status) : null; }, dayOf = function (k) { return function (r) { return r[k] ? day(r[k]) : null; }; };
  function scanParts(D_, st, board) {
    var d = D_.scan;
    if (!d || !d.boards || !d.boards[board]) throw new Error('the all-stock scan (data/campaign-scan.json) is not published yet');
    var kind = st.fund === 'etf' ? 'etf' : 'stock', u = d.universe || {};
    var rows = d.boards[board].filter(function (r) { return r.kind === kind; });
    /* Rankings by percentage are read among names large enough to act on, and without the rows whose tape holds a
       one-session move beyond 3-to-1 that no split explains (the engine flags them; they stay on the board and in the ring). */
    var floor = kind === 'etf' ? 1e8 : 1e9, clear = rows.filter(function (r) { return !r.jump; }), sized = clear.filter(function (r) { return cap(r) >= floor; }), odd = rows.length - clear.length;
    return { d: d, kind: kind, word: kind === 'etf' ? 'ETFs' : 'stocks', one: kind === 'etf' ? 'ETF' : 'stock', rows: rows, scanned: kind === 'etf' ? u.scanned_etf : u.scanned_stock, u: u, w: d.windows || {},
      clear: clear, sized: sized, odd: odd, floorWord: kind === 'etf' ? '$100M of assets and larger' : '$1B and larger', largest: kind === 'etf' ? 'the largest fund' : 'the largest company', capWord: kind === 'etf' ? ' of assets' : ' of market cap',
      sector: function (r) { return r.kind === 'etf' || !r.sector ? '' : ', ' + r.sector; },
      oddNote: odd ? plural(odd, 'row') + ' on this board carr' + (odd === 1 ? 'ies' : 'y') + ' a one-session move beyond 3-to-1 that no split explains; ' + (odd === 1 ? 'it is' : 'they are') + ' listed in the table and left out of the medians and rankings here. ' : '',
      controls: [{ id: 'stock', label: 'STOCKS', on: kind === 'stock' }, { id: 'etf', label: 'ETFS', on: kind === 'etf' }],
      where: function (r) { return r.kind === 'etf' ? 'ETF' : (r.sector || 'Unclassified'); },
      fresh: rows.filter(function (r) { return r.ago <= 4; }),
      sectors: function () {
        return FL(groupBy(rows, function (r) { return r.sector || 'Unclassified'; }), function (g) {
          var s = (d.by_sector || {})[g.key] || {};
          return { name: g.key, sub: tickers(g.list.slice().sort(by(cap, true)), 4), big: num(g.list.length), small: s.scanned ? pct(share(g.list.length, s.scanned), 0) + ' of ' + num(s.scanned) + ' scanned' : '' };
        }, scanSectors(d), 'n', -1);
      },
      foot: function (what) {
        return 'Source: justhodl-campaign-scan ' + (d.version || '') + ' (data/campaign-scan.json), published ' + String(d.generated_at || '').slice(0, 16).replace('T', ' ') + ' UTC, bars as of ' + day(d.as_of) + ' from the Polygon end-of-day bank (since '
          + day(u.bank_first_session) + '). ' + what + ' Scanned: ' + num(u.scanned_stock) + ' listed common stocks and ' + num(u.scanned_etf) + ' plain ETFs trading $' + num((u.min_dollar_vol || 0) / 1e3) + 'K a day or more with '
          + num(u.min_sessions) + ' sessions of history; ' + num(u.short_history) + ' had less. ' + this.oddNote + 'A mark is where the tape printed a pattern, not a forecast.';
      } };
  }
  function kind_cap(p) { return p.kind === 'etf' ? 'Assets marked' : 'Market cap marked'; }
  function statusRing(p, title, hint, STAT, whole) {
    var groups = STAT.map(function (s) { return { s: s, list: p.rows.filter(function (r) { return r.status === s[0]; }).sort(by(cap, true)) }; });
    return { groups: groups, more: function (S) { return groups.map(function (g, gi) { return hidden('status-' + gi, g.s[1], 'largest first', g.list, S, 'mcap', -1); }); }, ring: { open: 'largest', title: title, hint: hint, n: p.rows.length, center: num(p.rows.length), subs: [p.word.toUpperCase(), STAT.map(function (s, i) { return s[2] + ' ' + groups[i].list.length; }).slice(0, 2).join(DOT)], whole: whole,
      aria: title + ': ' + groups.map(function (g) { return g.s[1] + ' ' + g.list.length; }).join(', '),
      slices: groups.map(function (g, gi) { return { open: g.list.length ? 'status-' + gi : '', name: g.s[1], label: g.s[2], value: g.list.length, tone: g.s[3], op: g.s[4], big: num(g.list.length), small: pct(share(g.list.length, p.rows.length), 0) + (g.list.length ? DOT + tickers(g.list) : '') }; }) } };
  }

  WY.bottom = function (D_, st) {
    var p = scanParts(D_, st, 'bottom'), rows = p.rows, d = p.d;
    var cl = rows.filter(function (r) { return r.mark === 'CLIMAX'; }), lo = rows.filter(function (r) { return r.mark === 'BOTTOM'; });
    var SR = statusRing(p, 'Where the marked lows stand', 'by the last close', [['HELD', 'Low holding', 'HELD', 'pos', 1], ['UNDERCUT', 'Undercut, closed back above', 'UNDERCUT', 'pos', 0.5], ['BROKEN', 'Closing under the low', 'BROKEN', 'oth', 0.34]], 'the marked ' + p.word);
    var held = SR.groups[0].list.length, under = SR.groups[1].list.length, broke = SR.groups[2].list.length, peak = busiest(cl, function (r) { return r.date; });
    var S = scanSheet(p, [tx('mark', 'Mark', function (r) { return r.mark === 'CLIMAX' ? 'Selling climax' : r.mark === 'BOTTOM' ? 'Confirmed low' : r.mark; }), tx('date', 'Date', dayOf('date')), nx('ago', 'Sessions ago', 'ago', fN(0)), tx('status', 'Status', statusWord),
      nx('since', 'Since the mark', 'since_pct', fP(1, true), { tone: true }), nx('decline', 'Decline into the low', 'decline_pct', fP(0)), nx('vol', 'Volume', 'vol_x', capped, { tip: 'the bar\u2019s volume against its average; the scan stops counting at 12 times' }), nx('range', 'Range', 'range_x', fX),
      nx('score', 'Score', 'score', fN(2), { tip: 'volume x range, the scan\u2019s score of a climax bar' }), nx('low', 'Low', 'low', fPx), nx('last', 'Last', 'last', fPx), nx('off', 'Against the low', 'off_low_pct', fP(1, true), { tone: true })]);
    var facts = function (r) { return [p.where(r), fin(r.decline_pct) ? pct(r.decline_pct, 0) + ' into the low' : '', fin(r.vol_x) ? 'volume ' + capped(r.vol_x) : ''].filter(Boolean).join(DOT); };
    var newest = function (r) { return { name: tick(r), sub: facts(r), big: zpct(r.since_pct, 1, true), tone: tone(r.since_pct), small: day(r.date) + DOT + lower(r.status) }; };
    var card = function (list, label, none) {
      var r = list.slice().sort(by(cap, true))[0];
      if (!r) return { tone: '', big: D, eyebrow: label, title: 'None', text: none };
      return { tone: tone(r.since_pct), big: pct(r.since_pct, 1, true), eyebrow: label + DOT + p.largest, title: tick(r),
        text: money(cap(r)) + p.capWord + p.sector(r) + '. ' + (r.mark === 'CLIMAX' ? 'Selling climax' : 'Cycle low') + ' on ' + day(r.date) + ' (' + agoWord(r.ago) + ')'
          + (fin(r.decline_pct) ? ', ' + pct(Math.abs(r.decline_pct), 0) + ' under the high before it' : '') + (fin(r.vol_x) ? ', on ' + capped(r.vol_x) + ' the usual volume and ' + times(r.range_x) + ' the usual range' : '')
          + (r.climax_date ? '; its climax printed ' + day(r.climax_date) : '') + (r.rev_up_date ? '; reversal confirmed ' + day(r.rev_up_date) : '') + '. Marked low ' + fix(r.low) + ', last ' + fix(r.last) + ' (' + pct(r.off_low_pct, 1, true)
          + ' against the low): ' + (r.status === 'HELD' ? 'the low has held' : r.status === 'UNDERCUT' ? 'the low was undercut and price closed back above it' : 'price is closing under the low') + '.'
          + (r.accum_date ? ' The base that followed ended on ' + day(r.accum_date) + '.' : '') + (r.harvest_state ? ' Bottom desk: ' + harvestWord(r.harvest_state) + ', grade ' + (r.harvest_grade || D) + '.' : '') };
    };
    var mSince = med(p.clear.map(function (r) { return r.since_pct; }));
    return {
      eyebrow: 'Bottom tearsheet' + DOT + 'bars ' + day(d.as_of) + DOT + num(p.scanned) + ' ' + p.word + ' scanned',
      title: num(rows.length) + ' ' + p.word + ' carry the Bot icon\u2019s mark' + DOT + num(cl.length) + ' live selling climaxes and ' + num(lo.length) + ' confirmed lows' + DOT + num(held) + ' lows are holding',
      sub: 'Every ' + p.one + ' in the warehouse, scanned with the marks the chart prints' + DOT + 'a climax stays live for ' + (p.w.climax_sessions || 40) + ' sessions, a confirmed low is listed for ' + (p.w.bottom_sessions || 126) + DOT + 'published ' + day(d.generated_at),
      controls: p.controls,
      tiles: [{ k: 'On the board', v: num(rows.length), s: pct(share(rows.length, p.scanned), 0) + ' of ' + num(p.scanned) + ' ' + p.word + ' scanned' },
        { k: 'Live climaxes', v: num(cl.length), s: num(cl.filter(function (r) { return r.ago <= 4; }).length) + ' in the last 5 sessions' + (peak ? DOT + 'most on ' + peak.date + ' (' + num(peak.n) + ')' : '') },
        { k: 'Confirmed lows', v: num(lo.length), s: 'readable 40 sessions after the low' + DOT + num(lo.filter(function (r) { return r.accum_date; }).length) + ' already ended their base' },
        { k: 'Low holding', v: pct(share(held, rows.length), 0), s: num(held) + ' held' + DOT + num(under) + ' undercut and back' + DOT + num(broke) + ' closing under' },
        { k: 'Median since the mark', v: '<span class="' + tone(mSince) + '">' + zpct(mSince, 1, true) + '</span>', s: 'last close against the marked bar\u2019s close' },
        { k: kind_cap(p), v: money(sum(rows, cap)), s: 'sum of the marked ' + p.word }],
      cards: [card(cl, 'Live selling climax', 'No ' + p.one + ' carries a live selling climax.'), card(lo, 'Confirmed low', 'No ' + p.one + ' carries a confirmed low inside the window.')],
      notes: [['Climax:', 'after a markdown, a bar at the 20, 60 or 126-session low that sold off on climactic volume and range and found demand (a close off the low, or a rally inside 8 sessions). It is the location of the selling, not a buy.'],
        ['Confirmed low:', 'a swing low at the 126 or 252-session low, 12% or more under the year\u2019s high, that bounced 4% inside 12 sessions. It can only be read 40 sessions later; the bottom desk\u2019s secondary test counts as the same mark.'],
        ['Caveat:', 'a third state matters as much as the mark: whether the low still holds. A climax whose low is closing under is a failed climax, and it stays on the board saying so.']],
      ring: Object.assign(SR.ring, { caption: 'Each ' + p.one + ' once, under where its last close stands against the low of its mark on ' + day(d.as_of) + '. Held = no later low under it; undercut = a later low went under and the close is back above.' }),
      lists: [{ key: 'new-climax', title: 'Newest selling climaxes', hint: 'largest first on each day', n: cl.length, rows: FL(cl.slice().sort(function (a, b) { return a.ago - b.ago || cap(b) - cap(a); }), newest, S, 'ago', 1), none: 'no live climax' },
        { key: 'new-low', title: 'Newest confirmed lows', hint: 'by the date of the low', n: lo.length, rows: FL(lo.slice().sort(function (a, b) { return a.ago - b.ago || cap(b) - cap(a); }), function (r) {
          return { name: tick(r), sub: [p.where(r), 'low ' + fix(r.low), r.rev_up_date ? 'reversal ' + day(r.rev_up_date) : '', r.source !== 'scan' ? 'bottom desk ' + harvestWord(r.harvest_state) : ''].filter(Boolean).join(DOT), big: zpct(r.off_low_pct, 1, true), tone: tone(r.off_low_pct), small: day(r.date) + DOT + 'vs the low' }; }, S, 'ago', 1), none: 'no confirmed low' },
        { key: 'largest', title: 'Largest ' + (p.kind === 'etf' ? 'funds' : 'companies') + ' marked', hint: p.kind === 'etf' ? 'assets' : 'market cap', rows: FL(rows.slice().sort(by(cap, true)), function (r) { return { name: tick(r), sub: p.where(r) + DOT + lower(r.mark) + ' ' + day(r.date) + DOT + lower(r.status), big: money(cap(r)), small: zpct(r.since_pct, 1, true) + ' since' }; }, S, 'mcap', -1), none: 'none' },
        { key: 'volume', title: 'Strongest climax bars', hint: 'volume \u00d7 range, the scan\u2019s score' + DOT + p.floorWord, rows: FL(p.sized.filter(function (r) { return r.mark === 'CLIMAX' && fin(r.score); }).sort(by(function (r) { return r.score; }, true)), function (r) {
          return { name: tick(r), sub: p.where(r) + DOT + 'volume ' + capped(r.vol_x) + DOT + 'range ' + times(r.range_x) + DOT + pct(r.decline_pct, 0) + ' into the low', big: num(r.score, 1), small: day(r.date) + DOT + lower(r.status) }; }, S, 'score', -1), none: 'no climax that size carries a score' },
        { key: 'deepest', title: 'Deepest declines into the low', hint: p.floorWord, tone: 'down', rows: FL(p.sized.filter(function (r) { return fin(r.decline_pct); }).sort(by(function (r) { return r.decline_pct; })), function (r) {
          return { name: tick(r), sub: p.where(r) + DOT + lower(r.mark) + ' ' + day(r.date) + DOT + lower(r.status), big: pct(r.decline_pct, 0), tone: 'down', small: pct(r.off_low_pct, 1, true) + ' vs the low' }; }, S, 'decline', 1), none: 'none' },
        { key: 'strongest', title: 'Strongest since the mark', hint: p.floorWord, tone: 'up', rows: FL(p.sized.filter(function (r) { return r.since_pct > 0; }).sort(by(function (r) { return r.since_pct; }, true)), newest, S, 'since', -1), none: 'nothing is up since its mark' },
        { key: 'broken', title: 'Lows that are breaking', hint: 'closing under the marked low' + DOT + p.floorWord, tone: 'down', n: broke, rows: FL(p.sized.filter(function (r) { return r.status === 'BROKEN'; }).sort(by(function (r) { return r.off_low_pct; })), function (r) {
          return { name: tick(r), sub: p.where(r) + DOT + lower(r.mark) + ' ' + day(r.date) + DOT + 'low ' + fix(r.low), big: pct(r.off_low_pct, 1, true), tone: 'down', small: 'vs the low' }; }, S, 'off', 1, { rows: sorted(rows.filter(function (r) { return r.status === 'BROKEN'; }), function (r) { return r.off_low_pct; }) }), none: 'no marked low is closing under' }]
        .concat(p.kind === 'stock' ? [{ key: 'sectors', title: 'By sector', hint: 'share of the sector\u2019s scanned stocks', rows: p.sectors(), none: 'none' }] : []),
      more: SR.more(S),
      foot: p.foot('CLIMAX and BOTTOM are the chart\u2019s own scans (sellingClimaxScan, structureScan) plus the bottom desk\u2019s climax and secondary-test dates, exactly as the Bot icon marks them.')
    };
  };

  WY.accum = function (D_, st) {
    var p = scanParts(D_, st, 'accumulation'), rows = p.rows, d = p.d;
    var SR = statusRing(p, 'Where the breakouts stand', 'by the last close', [['ABOVE', 'Above the ACCUM bar', 'ABOVE', 'pos', 1], ['BASE', 'Back in the base', 'BASE', 'pos', 0.5], ['LOST', 'Under the base\u2019s low', 'LOST', 'oth', 0.34]], 'the marked ' + p.word);
    var above = SR.groups[0].list, base = SR.groups[1].list, lost = SR.groups[2].list, mSince = med(p.clear.map(function (r) { return r.since_pct; })), mBase = med(rows.map(function (r) { return r.base_sessions; }));
    var S = scanSheet(p, [tx('date', 'ACCUM bar', dayOf('date')), nx('ago', 'Sessions ago', 'ago', fN(0)), tx('status', 'Status', statusWord), nx('since', 'Since the bar', 'since_pct', fP(1, true), { tone: true }), nx('base', 'Base, sessions', 'base_sessions', fN(0)),
      nx('gain', 'Off the low at the bar', 'gain_from_low_pct', fP(0, true)), nx('low', 'Base low', 'low', fPx), tx('lowd', 'Low on', dayOf('low_date')), nx('px', 'Close on the bar', 'px', fPx), nx('last', 'Last', 'last', fPx),
      nx('off', 'Against the low', 'off_low_pct', fP(1, true), { tone: true }), tx('src', 'Read by', function (r) { return r.source === 'scan' ? 'chart scan' : r.source ? 'bottom desk trigger' : null; }), tx('grade', 'Desk grade', 'harvest_grade')]);
    var row = function (r) { return { name: tick(r), sub: [p.where(r), fin(r.base_sessions) ? 'base ' + plural(r.base_sessions, 'session') : '', fin(r.gain_from_low_pct) ? pct(r.gain_from_low_pct, 0, true) + ' off the low at the bar' : '', r.source !== 'scan' ? 'bottom desk trigger' : ''].filter(Boolean).join(DOT),
      big: zpct(r.since_pct, 1, true), tone: tone(r.since_pct), small: day(r.date) + DOT + lower(r.status) }; };
    var card = function (list, label, none) {
      var r = list[0];
      if (!r) return { tone: '', big: D, eyebrow: label, title: 'None', text: none };
      return { tone: tone(r.since_pct), big: zpct(r.since_pct, 1, true), eyebrow: label + DOT + p.largest, title: tick(r),
        text: money(cap(r)) + p.capWord + p.sector(r) + '. ACCUM printed on ' + day(r.date) + ' (' + agoWord(r.ago) + ') at ' + fix(r.px) + (r.low_date ? ', ' + plural(r.base_sessions, 'session') + ' after the low of ' + fix(r.low) + ' on ' + day(r.low_date)
          + ' and ' + pct(r.gain_from_low_pct, 0, true) + ' above it' : '') + '. Last ' + fix(r.last) + ': ' + zpct(r.since_pct, 1, true) + ' since the bar' + (fin(r.off_low_pct) ? ', ' + pct(r.off_low_pct, 1, true) + ' against the low' : '') + '.'
          + (r.climax_after ? ' A new selling climax printed on ' + day(r.climax_after) + '.' : '') + (r.harvest_state ? ' Bottom desk: ' + harvestWord(r.harvest_state) + ', grade ' + (r.harvest_grade || D) + '.' : '') };
    };
    return {
      eyebrow: 'Accumulation tearsheet' + DOT + 'bars ' + day(d.as_of) + DOT + num(p.scanned) + ' ' + p.word + ' scanned',
      title: num(rows.length) + ' ' + p.word + ' printed ACCUM in the last ' + (p.w.accumulation_sessions || 63) + ' sessions' + DOT + num(above.length) + ' are above the bar, ' + num(base.length) + ' back in the base, ' + num(lost.length) + ' under its low',
      sub: 'Every ' + p.one + ' in the warehouse, scanned with the mark the chart\u2019s Acc icon prints' + DOT + 'ACCUM is the bar that ends the base after a confirmed low' + DOT + 'published ' + day(d.generated_at),
      controls: p.controls,
      tiles: [{ k: 'On the board', v: num(rows.length), s: pct(share(rows.length, p.scanned), 1) + ' of ' + num(p.scanned) + ' ' + p.word + ' scanned' },
        { k: 'New in 5 sessions', v: num(p.fresh.length), s: p.fresh.length ? tickers(p.fresh.slice().sort(by(cap, true)), 4) : 'no new print' },
        { k: 'Above the bar', v: pct(share(above.length, rows.length), 0), s: num(above.length) + ' above' + DOT + num(base.length) + ' in the base' + DOT + num(lost.length) + ' lost the low' },
        { k: 'Median since the bar', v: '<span class="' + tone(mSince) + '">' + zpct(mSince, 1, true) + '</span>', s: 'last close against the ACCUM bar\u2019s close' },
        { k: 'Median base', v: fin(mBase) ? num(mBase) + ' sessions' : D, s: 'from the low to the bar that ended it' },
        { k: kind_cap(p), v: money(sum(rows, cap)), s: 'sum of the marked ' + p.word }],
      cards: [card(above, 'Holding above the bar', 'No ' + p.one + ' is above its ACCUM bar.'), card(lost, 'Lost the low', 'No ' + p.one + ' has closed under the low its base stands on.')],
      notes: [['The mark:', '8 to 50 sessions after a confirmed low, the first up-close bar that makes the highest high since the low, closes within 0.2% of it and at least 6% above the low. The bottom desk\u2019s trigger is marked the same way.'],
        ['Read it late:', 'a low is only confirmed 40 sessions after it prints, so an ACCUM bar can appear on the chart weeks after its date. The date shown is the bar\u2019s, not the day it became readable.'],
        ['Caveat:', 'the bar ends a base; it does not promise a markup. The ring shows how many have since fallen back into the base or under its low.']],
      ring: Object.assign(SR.ring, { caption: 'Each ' + p.one + ' once, under where its last close stands on ' + day(d.as_of) + ': at or above the ACCUM bar\u2019s close, under it but above the low the base stands on, or under that low.' }),
      lists: [{ key: 'newest', title: 'Newest ACCUM bars', hint: 'largest first on each day', rows: FL(rows.slice().sort(function (a, b) { return a.ago - b.ago || cap(b) - cap(a); }), row, S, 'ago', 1), none: 'no ACCUM inside the window' },
        { key: 'largest', title: 'Largest ' + (p.kind === 'etf' ? 'funds' : 'companies') + ' marked', hint: p.kind === 'etf' ? 'assets' : 'market cap', rows: FL(rows.slice().sort(by(cap, true)), function (r) { return { name: tick(r), sub: p.where(r) + DOT + day(r.date) + DOT + lower(r.status), big: money(cap(r)), small: zpct(r.since_pct, 1, true) + ' since' }; }, S, 'mcap', -1), none: 'none' },
        { key: 'strongest', title: 'Strongest since the bar', hint: p.floorWord, tone: 'up', rows: FL(p.sized.filter(function (r) { return r.since_pct > 0; }).sort(by(function (r) { return r.since_pct; }, true)), row, S, 'since', -1), none: 'nothing that size is up since its bar' },
        { key: 'weakest', title: 'Weakest since the bar', hint: p.floorWord, tone: 'down', rows: FL(p.sized.filter(function (r) { return r.since_pct <= 0; }).sort(by(function (r) { return r.since_pct; })), row, S, 'since', 1), none: 'nothing that size is down since its bar' },
        { key: 'bases', title: 'Longest bases', hint: 'sessions from the low to the bar', rows: FL(rows.filter(function (r) { return fin(r.base_sessions); }).sort(by(function (r) { return r.base_sessions; }, true)), function (r) {
          return { name: tick(r), sub: p.where(r) + DOT + 'low ' + fix(r.low) + ' on ' + day(r.low_date) + DOT + lower(r.status), big: num(r.base_sessions), small: 'sessions' + DOT + day(r.date) }; }, S, 'base', -1), none: 'none' }]
        .concat(p.kind === 'stock' ? [{ key: 'sectors', title: 'By sector', hint: 'share of the sector\u2019s scanned stocks', rows: p.sectors(), none: 'none' }] : []),
      more: SR.more(S),
      foot: p.foot('ACCUM is the chart\u2019s end-of-accumulation bar (structureScan) plus the bottom desk\u2019s trigger date, exactly as the Acc icon marks them.')
    };
  };

  WY.dist = function (D_, st) {
    var p = scanParts(D_, st, 'distribution'), rows = p.rows, d = p.d;
    var SR = statusRing(p, 'Where the failed rallies stand', 'by the last close', [['BELOW', 'Under the DIST bar', 'BELOW', 'pos', 1], ['RANGE', 'Between the bar and the high', 'RANGE', 'pos', 0.5], ['RECLAIMED', 'Back above the rally high', 'RECLAIMED', 'oth', 0.34]], 'the marked ' + p.word);
    var below = SR.groups[0].list, range = SR.groups[1].list, back = SR.groups[2].list, mHigh = med(p.clear.map(function (r) { return r.below_high_pct; }));
    var S = scanSheet(p, [tx('date', 'DIST bar', dayOf('date')), nx('ago', 'Sessions ago', 'ago', fN(0)), tx('status', 'Status', statusWord), nx('below', 'Against the rally high', 'below_high_pct', fP(1, true), { tone: true }),
      nx('since', 'Since the bar', 'since_pct', fP(1, true), { tone: true }), nx('high', 'Rally high', 'high', fPx), tx('highd', 'High on', dayOf('high_date')), nx('push', 'Last push', 'push_pct', fP(0, true)), nx('prev', 'Push before', 'prev_push_pct', fP(0, true)),
      nx('react', 'Reaction', 'reaction_pct', fP(0, true)), nx('effort', 'Volume, push on push', 'effort_x', fX, { tip: 'the last push\u2019s volume against the push before it' }), tx('why', 'Why', 'why'), nx('px', 'Close on the bar', 'px', fPx), nx('last', 'Last', 'last', fPx)]);
    var effort = rows.filter(function (r) { return r.why === 'more volume bought less'; }).length;
    var row = function (r) { return { name: tick(r), sub: [p.where(r), 'rally high ' + fix(r.high) + ' on ' + day(r.high_date), fin(r.push_pct) ? 'push ' + pct(r.push_pct, 0, true) + ' after ' + pct(r.prev_push_pct, 0, true) : ''].filter(Boolean).join(DOT),
      big: zpct(r.below_high_pct, 1, true), tone: tone(r.below_high_pct), small: day(r.date) + DOT + lower(r.status) }; };
    var card = function (list, label, none) {
      var r = list[0];
      if (!r) return { tone: '', big: D, eyebrow: label, title: 'None', text: none };
      return { tone: tone(r.since_pct), big: zpct(r.since_pct, 1, true), eyebrow: label + DOT + p.largest, title: tick(r),
        text: money(cap(r)) + p.capWord + p.sector(r) + '. DIST printed on ' + day(r.date) + ' (' + agoWord(r.ago) + ') at ' + fix(r.px) + ', after the rally high of ' + fix(r.high) + ' on ' + day(r.high_date) + ': the last push gained '
          + pct(r.push_pct, 1) + ' where the one before gained ' + pct(r.prev_push_pct, 1) + ' (' + r.why + '), and the reaction took ' + pct(Math.abs(r.reaction_pct), 1) + ' off the high.' + (r.d_top_date ? ' A structure top stands at ' + day(r.d_top_date) + '.' : '')
          + ' Last ' + fix(r.last) + ': ' + pct(r.since_pct, 1, true) + ' since the bar, ' + pct(r.below_high_pct, 1, true) + ' against the high.' };
    };
    return {
      eyebrow: 'Distribution tearsheet' + DOT + 'bars ' + day(d.as_of) + DOT + num(p.scanned) + ' ' + p.word + ' scanned',
      title: num(rows.length) + ' ' + p.word + ' printed DIST in the last ' + (p.w.distribution_sessions || 63) + ' sessions' + DOT + num(below.length) + ' are under the bar, ' + num(range.length) + ' between it and the high, ' + num(back.length) + ' took the high back',
      sub: 'Every ' + p.one + ' in the warehouse, scanned with the mark the chart\u2019s Dist icon prints' + DOT + 'a rally is not the verdict: DIST prints only after it fails its holding tests' + DOT + 'published ' + day(d.generated_at),
      controls: p.controls,
      tiles: [{ k: 'On the board', v: num(rows.length), s: pct(share(rows.length, p.scanned), 1) + ' of ' + num(p.scanned) + ' ' + p.word + ' scanned' },
        { k: 'New in 5 sessions', v: num(p.fresh.length), s: p.fresh.length ? tickers(p.fresh.slice().sort(by(cap, true)), 4) : 'no new print' },
        { k: 'Under the bar', v: pct(share(below.length, rows.length), 0), s: num(below.length) + ' under' + DOT + num(range.length) + ' in the range' + DOT + num(back.length) + ' back above the high' },
        { k: 'Median against the high', v: '<span class="' + tone(mHigh) + '">' + zpct(mHigh, 1, true) + '</span>', s: 'last close against the rally high' },
        { k: 'Why it printed', v: num(effort) + ' / ' + num(rows.length - effort), s: 'more volume bought less / the push barely cleared the last high' },
        { k: kind_cap(p), v: money(sum(rows, cap)), s: 'sum of the marked ' + p.word }],
      cards: [card(below, 'Under the bar', 'No ' + p.one + ' is under its DIST bar.'), card(back, 'Took the high back', 'No ' + p.one + ' has closed back above its rally high.')],
      notes: [['The mark:', 'a swing high within 3% of the 189-session high after an advance of 15% off the year\u2019s low, where the last push bought less than the one before, gave back half of itself inside 18 sessions, was not reclaimed in the next 6 and reacted worse than the reaction before.'],
        ['Where it prints:', 'on the bar that completes those tests, days after the rally high. The row shows both dates, and the push sizes the verdict rests on.'],
        ['Caveat:', 'a distribution that is reclaimed was wrong. It stays on the board for the window, in the ring\u2019s third slice, so the failures are counted beside the successes.']],
      ring: Object.assign(SR.ring, { caption: 'Each ' + p.one + ' once, under where its last close stands on ' + day(d.as_of) + ': under the DIST bar\u2019s close, between it and the rally high, or back at or above that high.' }),
      lists: [{ key: 'newest', title: 'Newest DIST bars', hint: 'largest first on each day', rows: FL(rows.slice().sort(function (a, b) { return a.ago - b.ago || cap(b) - cap(a); }), row, S, 'ago', 1), none: 'no DIST inside the window' },
        { key: 'largest', title: 'Largest ' + (p.kind === 'etf' ? 'funds' : 'companies') + ' marked', hint: p.kind === 'etf' ? 'assets' : 'market cap', rows: FL(rows.slice().sort(by(cap, true)), function (r) { return { name: tick(r), sub: p.where(r) + DOT + day(r.date) + DOT + lower(r.status), big: money(cap(r)), small: zpct(r.below_high_pct, 1, true) + ' vs high' }; }, S, 'mcap', -1), none: 'none' },
        { key: 'furthest', title: 'Furthest under the rally high', hint: p.floorWord, tone: 'down', rows: FL(p.sized.filter(function (r) { return r.below_high_pct < 0; }).sort(by(function (r) { return r.below_high_pct; })), row, S, 'below', 1), none: 'nothing is under its high' },
        { key: 'reclaimed', title: 'Took the high back', hint: 'the distribution failed' + DOT + p.floorWord, tone: 'up', n: back.length, rows: FL(p.sized.filter(function (r) { return r.status === 'RECLAIMED'; }).sort(by(function (r) { return r.below_high_pct; }, true)), row, S, 'below', -1, { rows: sorted(rows.filter(function (r) { return r.status === 'RECLAIMED'; }), function (r) { return r.below_high_pct; }, true) }), none: 'no rally high that size was reclaimed' },
        { key: 'effort', title: 'Most volume for the least progress', hint: 'last push against the one before' + DOT + p.floorWord, rows: FL(p.sized.filter(function (r) { return fin(r.effort_x) && r.why === 'more volume bought less'; }).sort(by(function (r) { return r.effort_x; }, true)), function (r) {
          return { name: tick(r), sub: p.where(r) + DOT + 'push ' + pct(r.push_pct, 0, true) + ' after ' + pct(r.prev_push_pct, 0, true) + DOT + lower(r.status), big: times(r.effort_x), small: day(r.date) }; }, S, 'effort', -1), none: 'none' }]
        .concat(p.kind === 'stock' ? [{ key: 'sectors', title: 'By sector', hint: 'share of the sector\u2019s scanned stocks', rows: p.sectors(), none: 'none' }] : []),
      more: SR.more(S),
      foot: p.foot('DIST is the chart\u2019s distribution scan (distributionScan), exactly as the Dist icon marks it; D-TOP is the structure top within 40 sessions of the rally high.')
    };
  };

  /* Selling climax: the bottom desk's daily rows in CLIMAX or TESTING (the table's own rule) */
  WY.sc = function (D_) {
    var d = D_.bottom;
    if (!d || !d.board) throw new Error('the bottom desk\u2019s board (data/bottom.json) is not readable');
    var rows = d.board.filter(function (r) { return r.frame === 'D' && (r.state === 'CLIMAX' || r.state === 'TESTING'); }), ref = d.session || d.generated_at, labels = d.desk_labels || {};
    var where = function (r) { return r.desk === 'stocks' ? (r.sector || 'Unclassified') : (labels[r.desk] || r.desk || 'Other'); };
    var cl = rows.filter(function (r) { return r.state === 'CLIMAX'; }), te = rows.filter(function (r) { return r.state === 'TESTING'; });
    var age = function (r) { return daysBetween(r.sc_date, ref); }, fresh = rows.filter(function (r) { var a = age(r); return fin(a) && a <= 7; }), good = rows.filter(function (r) { return r.grade === 'A' || r.grade === 'B'; });
    var groups = groupBy(rows, where), u = d.universe || {};
    var S = sheet([tx('t', 'Ticker', 'ticker', { find: 'start' }), tx('name', 'Name', function (r) { return r.name || r.company; }), tx('where', 'Sector or desk', where), nx('mcap', 'Mkt cap', function (r) { return fin(r.mcap) ? r.mcap : null; }, fM),
      tx('state', 'State', function (r) { return r.state ? lower(r.state) : null; }), tx('grade', 'Grade', 'grade'), nx('score', 'Score', 'score', fN(1)), tx('date', 'Climax', dayOf('sc_date')), nx('vol', 'Volume', 'sc_vol_x', fX, { tip: 'the climax bar against the 50-day average volume' }),
      nx('range', 'Range', 'sc_range_x', fX), nx('decline', 'Decline into it', 'sc_decline_pct', fP(0)), nx('low', 'Climax low', 'sc_low', fPx), nx('last', 'Last', 'last', fPx), nx('off', 'Against the low', 'dist_sc_low_pct', fP(1, true), { tone: true }),
      nx('ar', 'Automatic rally', 'ar_rally_pct', fP(0))], ['name', 'names'], { href: function (r) { return r.desk === 'crypto' || r.desk === 'currencies' ? '' : chartOf(r.ticker); }, find: 'Find a ticker, name, sector or desk', phone: ['t', 'vol', 'date', 'decline', 'state', 'grade', 'where'] });
    var row = function (r) { return { name: tick(r), sub: [where(r), pct(r.sc_decline_pct, 0) + ' into the climax', 'range ' + times(r.sc_range_x), 'grade ' + (r.grade || D)].join(DOT), big: times(r.sc_vol_x), small: day(r.sc_date) + DOT + lower(r.state) }; };
    var card = function (r, label) {
      if (!r) return { tone: '', big: D, eyebrow: label, title: 'None', text: 'No daily row is in a climax or testing one.' };
      return { tone: '', big: times(r.sc_vol_x), eyebrow: label, title: tick(r), text: (fin(r.mcap) ? money(r.mcap) + ' of market cap, ' : '') + where(r) + '. Selling climax on ' + day(r.sc_date) + ' (' + daysWord(age(r)) + '): ' + pct(Math.abs(r.sc_decline_pct), 0)
        + ' under the high before it, on ' + times(r.sc_vol_x) + ' the 50-day volume and ' + times(r.sc_range_x) + ' the average range. Climax low ' + fix(r.sc_low) + ', last ' + fix(r.last) + ' (' + pct(r.dist_sc_low_pct, 1, true) + ' against it). State '
        + lower(r.state) + ', grade ' + (r.grade || D) + (fin(r.ar_rally_pct) ? '; the automatic rally ran ' + pct(r.ar_rally_pct, 0) : '') + '.' };
    };
    return {
      eyebrow: 'Selling climax tearsheet' + DOT + 'session ' + day(ref) + DOT + num(u.scored) + ' instruments scored by the bottom desk',
      title: num(rows.length) + ' names are in a selling climax or testing one' + DOT + num(cl.length) + ' at the climax, ' + num(te.length) + ' testing the low' + DOT + num(fresh.length) + ' printed in the last week',
      sub: 'Daily bars' + DOT + 'the bottom desk\u2019s board, rows in CLIMAX or TESTING' + DOT + 'this is where the selling climaxed, not a buy' + DOT + 'published ' + day(d.generated_at),
      tiles: [{ k: 'On the board', v: num(rows.length), s: num(cl.length) + ' climax' + DOT + num(te.length) + ' testing' },
        { k: 'Printed in the last week', v: num(fresh.length), s: fresh.length ? tickers(fresh.slice().sort(by(function (r) { return r.sc_vol_x || 0; }, true)), 4) : 'no new climax' },
        { k: 'Median climax volume', v: times(med(rows.map(function (r) { return r.sc_vol_x; }))), s: 'against the 50-day average' },
        { k: 'Median decline', v: pct(med(rows.map(function (r) { return r.sc_decline_pct; })), 0), s: 'into the climax low' },
        { k: 'Graded A or B', v: num(good.length), s: good.length ? tickers(good.slice().sort(by(function (r) { return r.score || 0; }, true)), 4) : 'none yet' },
        { k: 'Most names', v: groups[0] ? groups[0].key : D, s: groups[0] ? num(groups[0].list.length) + ' of ' + num(rows.length) : '' }],
      cards: [card(rows.slice().sort(by(function (r) { return r.sc_vol_x || 0; }, true))[0], 'Heaviest climax volume'), card(rows.filter(function (r) { return fin(r.mcap); }).sort(by(cap, true))[0], 'Largest company on the board')],
      notes: [['The climax:', 'climactic volume at a new low of a real decline, a wide range, and a close off the low. CLIMAX = the bar has printed; TESTING = price rallied and is back testing that low.'],
        ['What comes next:', 'the proof is the secondary test on shrunken volume. The Bottom tab follows the names that pass it.'],
        ['Caveat:', 'the board is the bottom desk\u2019s top ' + num((d.board || []).length) + ' rows by score, not every climax in the market; the Bottom tab lists every stock the chart marks.']],
      ring: { open: 'newest', title: 'Where the climaxes are', hint: 'names by sector or desk', n: rows.length, center: num(rows.length), subs: ['NAMES', 'CLIMAX ' + cl.length + DOT + 'TESTING ' + te.length], whole: 'the board', aria: 'Selling-climax names by sector',
        slices: compose(groups, function (k) { return k; }, function (g) { return pct(share(g.list.length, rows.length), 0) + DOT + tickers(g.list.slice().sort(by(function (r) { return r.sc_vol_x || 0; }, true))); }),
        caption: 'Each name once, under its sector (stocks) or its desk (funds, bonds, countries, crypto).' },
      lists: [{ key: 'newest', title: 'Newest climaxes', hint: 'by the date of the bar', rows: FL(rows.slice().sort(function (a, b) { return String(b.sc_date).localeCompare(String(a.sc_date)) || (b.sc_vol_x || 0) - (a.sc_vol_x || 0); }), row, S, 'date', -1), none: 'none' },
        { key: 'volume', title: 'Heaviest volume', hint: 'climax bar against the 50-day average', rows: FL(rows.slice().sort(by(function (r) { return r.sc_vol_x || 0; }, true)), row, S, 'vol', -1), none: 'none' },
        { key: 'deepest', title: 'Deepest declines', hint: 'into the climax low', tone: 'down', rows: FL(rows.filter(function (r) { return fin(r.sc_decline_pct); }).sort(by(function (r) { return r.sc_decline_pct; })), function (r) {
          return { name: tick(r), sub: where(r) + DOT + 'volume ' + times(r.sc_vol_x) + DOT + lower(r.state), big: pct(r.sc_decline_pct, 0), tone: 'down', small: day(r.sc_date) }; }, S, 'decline', 1), none: 'none' },
        { key: 'largest', title: 'Largest companies', hint: 'market cap', rows: FL(rows.filter(function (r) { return fin(r.mcap); }).sort(by(cap, true)), function (r) { return { name: tick(r), sub: where(r) + DOT + lower(r.state) + DOT + 'volume ' + times(r.sc_vol_x), big: money(r.mcap), small: day(r.sc_date) }; }, S, 'mcap', -1), none: 'none' },
        { key: 'graded', title: 'Best graded', hint: 'the bottom desk\u2019s score', tone: 'up', rows: FL(rows.filter(function (r) { return fin(r.score); }).sort(by(function (r) { return r.score; }, true)), function (r) {
          return { name: tick(r), sub: where(r) + DOT + lower(r.state) + DOT + 'volume ' + times(r.sc_vol_x), big: (r.grade || D) + ' ' + num(r.score), small: day(r.sc_date) }; }, S, 'score', -1), none: 'none' },
        { key: 'testing', title: 'Testing the low', hint: 'closest to the climax low', n: te.length, rows: FL(te.filter(function (r) { return fin(r.dist_sc_low_pct); }).sort(by(function (r) { return Math.abs(r.dist_sc_low_pct); })), function (r) {
          return { name: tick(r), sub: where(r) + DOT + 'climax ' + day(r.sc_date) + DOT + 'low ' + fix(r.sc_low), big: pct(r.dist_sc_low_pct, 1, true), tone: tone(r.dist_sc_low_pct), small: 'vs the low' }; }, S), none: 'nothing is testing' }],
      foot: 'Source: justhodl-bottom ' + (d.version || '') + ' (data/bottom.json), session ' + day(ref) + ', published ' + String(d.generated_at || '').slice(0, 16).replace('T', ' ') + ' UTC. The board carries the desk\u2019s ' + num((d.board || []).length) + ' highest-scored rows of '
        + num(u.scored) + ' instruments scored; this tab reads the daily rows in CLIMAX or TESTING.'
    };
  };

  /* Capitulation: panic-volume prints at the lows, all desks (capitulation-desk.json) */
  var CAP_DESK = { stocks: 'Stocks', equity_etfs: 'Equity & sector ETFs', countries: 'Countries', bonds: 'Bonds', gold_metals: 'Gold & metals', commodities: 'Commodities', currencies: 'Currencies', crypto: 'Crypto' };
  WY.capit = function (D_) {
    var d = D_.capit;
    if (!d || !d.boards || !d.breadth) throw new Error('the capitulation desk\u2019s feed (data/capitulation-desk.json) is not readable');
    var B = d.breadth, C = d.counts || {}, P = d.params || {}, order = d.desk_order || Object.keys(d.boards), all = [];
    order.forEach(function (k) { (d.boards[k] || []).forEach(function (r) { all.push(r); }); });
    var latest = (d.latest || []).slice().sort(by(function (r) { return r.score || 0; }, true)), keep = P.keep_sessions || 60, win = P.latest_sessions || 5;
    var total = fin(d.n_prints_60) ? d.n_prints_60 : sum(order, function (k) { return (C[k] || {}).prints_60; });
    var desks = order.map(function (k) { return { key: k, n: (C[k] || {}).prints_60 || 0, five: (C[k] || {}).prints_5 || 0, scored: (C[k] || {}).scored }; }).filter(function (x) { return x.n > 0; }).sort(by(function (x) { return x.n; }, true));
    var after = all.filter(function (r) { return r.after && r.after.sessions_since >= 1 && typeof r.after.held === 'boolean'; }), heldN = after.filter(function (r) { return r.after.held; }).length;
    var sess = (B.sessions || []).slice().sort(by(function (s) { return s.total || 0; }, true)), last = B.last_session || {};
    var desk = function (r) { return CAP_DESK[r.desk] || r.desk || ''; };
    var row = function (r, big, small) { return { name: tick(r), sub: [desk(r), pct(r.ret_pct, 1, true) + ' on the day', 'volume ' + times(r.vol_x), 'range ' + times(r.range_x)].join(DOT), big: big, tone: 'down', small: small || day(r.date) }; };
    var aft = function (k) { return function (r) { return (r.after || {})[k]; }; }, newestFirst = function (a, b) { return String(b.date).localeCompare(String(a.date)) || (b.score || 0) - (a.score || 0); };
    var S = sheet([tx('t', 'Ticker', 'ticker', { find: 'start' }), tx('name', 'Name', 'name'), tx('desk', 'Desk', desk), tx('date', 'Print', dayOf('date')), nx('score', 'Score', 'score', fN(0)), nx('ret', 'On the day', 'ret_pct', fP(1, true), { tone: true }),
      nx('vol', 'Volume', 'vol_x', fX, { tip: 'the print bar against the 50-session average volume' }), nx('range', 'Range', 'range_x', fX), nx('dd20', 'Fall, 20 sessions', 'dd20_pct', fP(0)), nx('dd60', 'Fall, 60 sessions', 'dd60_pct', fP(0)),
      nx('since', 'Sessions since', aft('sessions_since'), fN(0)), nx('bounce', 'Bounce since', aft('bounce_pct'), fP(1, true), { tone: true, tip: 'the highest close after the print, against it' }),
      nx('under', 'Undercut since', aft('undercut_pct'), fP(1, true), { tone: true, tip: 'the lowest low after the print, against its low' }), tx('held', 'Its low', function (r) { var h = (r.after || {}).held; return h === true ? 'held' : h === false ? 'undercut' : null; }),
      nx('now', 'Now against the print', aft('now_vs_print_pct'), fP(1, true), { tone: true }), tx('ctx', 'Context', function (r) { return r.context ? lower(r.context) : null; }),
      tx('wy', 'Bottom desk', function (r) { var y = r.wyckoff || {}; return y.state ? lower(y.state) + (y.grade ? ', grade ' + y.grade : '') : null; })],
      ['print', 'prints'], { href: function (r) { return r.desk === 'crypto' || r.desk === 'currencies' ? '' : chartOf(r.ticker); }, find: 'Find a ticker, name or desk', phone: ['t', 'score', 'ret', 'vol', 'date', 'desk'] });
    var DAYS = sheet([tx('date', 'Session', dayOf('date')), nx('n', 'Prints', 'total', fN(0)), nx('share', '% of the scored', 'pct_of_scored', fP(2)), nx('s5', 'Five-session sum', 'sum_5', fN(0))].concat(order.map(function (k) {
      return nx('d_' + k, CAP_DESK[k] || k, function (s) { return (s.by_desk || {})[k] || 0; }, fN(0)); })), ['session', 'sessions']);
    var top = latest[0], cluster = sess[0];
    return {
      eyebrow: 'Capitulation tearsheet' + DOT + 'session ' + day(d.session) + DOT + num(B.n_scored) + ' instruments scored across ' + order.length + ' desks',
      title: num(B.sum_5) + ' capitulation prints in the last ' + win + ' sessions' + DOT + lower(B.regime) + ' breadth, ' + (fin(B.sum_5_percentile) ? 'the ' + num(B.sum_5_percentile) + 'th percentile of the window' : D) + DOT + num(total) + ' prints in ' + keep + ' sessions',
      sub: 'One session of panic: a down close on 2.5\u00d7 volume or more, a range 1.5\u00d7 the ATR, the close in the bottom 30% of the bar, at the 20-session low' + DOT + 'every desk on its own floor' + DOT + 'published ' + day(d.generated_at),
      tiles: [{ k: 'Last ' + win + ' sessions', v: num(B.sum_5), s: lower(B.regime) + DOT + (fin(B.sum_5_percentile) ? num(B.sum_5_percentile) + 'th percentile' : '') + DOT + 'median ' + num(B.window_median_sum_5) },
        { k: 'Last session', v: num(last.total), s: day(last.date) + (last.by_desk ? DOT + Object.keys(last.by_desk).map(function (k) { return (CAP_DESK[k] || k) + ' ' + last.by_desk[k]; }).join(', ') : '') },
        { k: 'Last ' + keep + ' sessions', v: num(total), s: desks.slice(0, 3).map(function (x) { return (CAP_DESK[x.key] || x.key) + ' ' + num(x.n); }).join(DOT) },
        { k: 'Share of the scored', v: pct(B.sum_5_pct_of_scored, 2), s: 'printed in the last ' + win + ' sessions, of ' + num(B.n_scored) },
        { k: 'Heaviest session', v: cluster ? num(cluster.total) : D, s: cluster ? day(cluster.date) + DOT + pct(cluster.pct_of_scored, 1) + ' of the scored' : '' },
        { k: 'Low held since', v: after.length ? pct(share(heldN, after.length), 0) : D, s: num(heldN) + ' of ' + num(after.length) + ' prints with a session after them' }],
      cards: [top ? { tone: 'down', big: pct(top.ret_pct, 0, true), eyebrow: 'Highest-scored print of the last ' + win + ' sessions', title: tick(top),
        text: desk(top) + '. ' + day(top.date) + ': ' + pct(top.ret_pct, 1, true) + ' on the day on ' + times(top.vol_x) + ' the 50-session volume and ' + times(top.range_x) + ' the ATR, closing ' + pct((top.loc || 0) * 100, 0) + ' up the bar at '
          + fix(top.close) + '. That left it ' + pct(top.dd20_pct, 0) + ' under its 20-session high and ' + pct(top.dd60_pct, 0) + ' under its 60-session high. Score ' + num(top.score, 1) + (top.wyckoff && top.wyckoff.state ? '; bottom desk: ' + lower(top.wyckoff.state) + ', grade ' + (top.wyckoff.grade || D) : '') + '.' }
        : { tone: '', big: D, eyebrow: 'Latest prints', title: 'None', text: 'No capitulation printed in the last ' + win + ' sessions.' },
        cluster ? { tone: 'down', big: num(cluster.total), eyebrow: 'Heaviest session of the last ' + (B.sessions || []).length + DOT + day(cluster.date), title: pct(cluster.pct_of_scored, 1) + ' of everything scored printed on one day',
          text: Object.keys(cluster.by_desk || {}).sort(function (a, b) { return cluster.by_desk[b] - cluster.by_desk[a]; }).map(function (k) { return (CAP_DESK[k] || k) + ' ' + cluster.by_desk[k]; }).join(', ') + '. A cluster is a session where at least '
            + (P.cluster_share_pct || 2.5) + '% of the scored universe prints together; ' + num(B.n_clusters_window) + ' such sessions stand in the ' + num(B.window_sessions) + '-session window.' }
          : { tone: '', big: D, eyebrow: 'Heaviest session', title: 'None', text: 'No session in the window carries a print.' }],
      notes: [['Read:', String(d.read || '') + '.'],
        ['Not a low by itself:', 'a print is one bar of panic. The follow-through columns on the desk below show whether the low held, how far it bounced and whether it was undercut.'],
        ['Caveat:', 'the stock board keeps the ' + num(P.board_rows || 500) + ' highest-scored prints of the ' + keep + ' sessions; the ring counts every print, the lists rank what the boards hold (' + num(all.length) + ').']],
      ring: { open: 'all', title: 'Prints by desk', hint: 'last ' + keep + ' sessions', n: total, center: num(total), subs: ['PRINTS', 'LAST ' + win + ': ' + num(B.sum_5)], whole: 'the prints', aria: 'Capitulation prints by desk over ' + keep + ' sessions',
        slices: desks.map(function (x, i) { return { open: (d.boards[x.key] || []).length ? 'desk-' + x.key : '', name: CAP_DESK[x.key] || x.key, value: x.n, tone: i < 5 ? 'pos' : 'oth', big: num(x.n), small: pct(share(x.n, total), 0) + DOT + pct(share(x.n, x.scored), 0) + ' of ' + num(x.scored) + ' scored' + DOT + x.five + ' in the last ' + win }; }),
        caption: 'Every print of the last ' + keep + ' sessions once, under the desk that scored it. A desk\u2019s floor is its own: stocks \u22123%, equity ETFs \u22122%, bonds \u22121%, crypto \u22125%.' },
      lists: [{ key: 'latest', title: 'Latest prints', hint: 'last ' + win + ' sessions, by score', tone: 'down', n: latest.length, rows: FL(latest, function (r) { return row(r, num(r.score, 0), day(r.date) + DOT + 'score'); }, S, 'score', -1), none: 'no print in the last ' + win + ' sessions' },
        { key: 'volume', title: 'Heaviest volume', hint: 'against the 50-session average', rows: FL(all.filter(function (r) { return fin(r.vol_x); }).sort(by(function (r) { return r.vol_x; }, true)), function (r) { return row(r, times(r.vol_x)); }, S, 'vol', -1), none: 'none' },
        { key: 'falls', title: 'Deepest one-day falls', hint: 'the print bar', tone: 'down', rows: FL(all.filter(function (r) { return fin(r.ret_pct); }).sort(by(function (r) { return r.ret_pct; })), function (r) { return row(r, pct(r.ret_pct, 1, true)); }, S, 'ret', 1), none: 'none' },
        { key: 'bounce', title: 'Best bounce since the print', hint: 'highest close after it', tone: 'up', rows: FL(all.filter(function (r) { return r.after && fin(r.after.bounce_pct) && r.after.bounce_pct >= 0.05; }).sort(by(function (r) { return r.after.bounce_pct; }, true)), function (r) {
          return { name: tick(r), sub: [desk(r), 'printed ' + day(r.date), r.after.held ? 'low held' : 'low undercut'].join(DOT), big: zpct(r.after.bounce_pct, 1, true), tone: 'up', small: plural(r.after.sessions_since, 'session') }; }, S, 'bounce', -1), none: 'no print has bounced yet' },
        { key: 'undercut', title: 'Undercut since the print', hint: 'lowest low after it', tone: 'down', rows: FL(all.filter(function (r) { return r.after && fin(r.after.undercut_pct) && r.after.undercut_pct <= -0.05; }).sort(by(function (r) { return r.after.undercut_pct; })), function (r) {
          return { name: tick(r), sub: [desk(r), 'printed ' + day(r.date), fin(r.after.now_vs_print_pct) ? 'now ' + zpct(r.after.now_vs_print_pct, 1, true) + ' vs the print' : ''].filter(Boolean).join(DOT), big: zpct(r.after.undercut_pct, 1, true), tone: 'down', small: plural(r.after.sessions_since, 'session') }; }, S, 'under', 1), none: 'no print has been undercut' },
        { key: 'sessions', title: 'Heaviest sessions', hint: 'prints in one day', rows: FL(sess.filter(function (s) { return s.total > 0; }), function (s) {
          return { name: day(s.date), sub: Object.keys(s.by_desk || {}).sort(function (a, b) { return s.by_desk[b] - s.by_desk[a]; }).slice(0, 4).map(function (k) { return (CAP_DESK[k] || k) + ' ' + s.by_desk[k]; }).join(DOT), big: num(s.total), small: pct(s.pct_of_scored, 2) + ' of scored' }; }, DAYS, 'n', -1), none: 'none' }],
      more: [hidden('all', 'Every print', 'last ' + keep + ' sessions, newest first', all.slice().sort(newestFirst), S, 'date', -1)].concat(order.filter(function (k) { return (d.boards[k] || []).length; }).map(function (k) {
        return hidden('desk-' + k, CAP_DESK[k] || k, 'its prints, newest first', d.boards[k].slice().sort(newestFirst), S, 'date', -1); })),
      foot: 'Source: justhodl-bottom ' + (d.engine_version || '') + ' (data/capitulation-desk.json, contract ' + (d.contract || '') + '), session ' + day(d.session) + ', published ' + String(d.generated_at || '').slice(0, 16).replace('T', ' ')
        + ' UTC. Breadth percentile is against ' + num(B.window_sessions) + ' sessions. A print is detected on the tape alone; nothing here is a forecast.'
    };
  };

  /* Buying climax and Spring: the phase detector's book */
  function phaseBook(D_) {
    var d = D_.phase;
    if (!d || !d.tickers) throw new Error('the phase detector\u2019s feed (data/phase-detector.json) is not readable');
    return d;
  }
  var PHASE = { ACCUMULATION: 'Accumulation', DISTRIBUTION: 'Distribution', MARKUP: 'Markup', MARKDOWN: 'Markdown', NEUTRAL_RANGE: 'Neutral range', NEUTRAL: 'Neutral' };
  WY.bc = function (D_) {
    var d = phaseBook(D_), ref = d.generated_at, rows = [], B = d.boards || {}, pc = d.phase_counts || {};
    Object.keys(d.tickers).forEach(function (t) {
      var v = d.tickers[t] || {}, ev = (v.events || []).filter(function (e) { return e && e.type === 'BC'; }).sort(function (a, b) { return String(b.date).localeCompare(String(a.date)); })[0];
      if (ev) rows.push({ ticker: t, date: ev.date, phase: v.phase, price: v.price, days: v.days_in_phase, pressure: v.pressure, lo: v.range_low, hi: v.range_high, whales: (v.confirmations || {}).whales_flow_usd, age: daysBetween(ev.date, ref) });
    });
    rows.sort(function (a, b) { return String(b.date).localeCompare(String(a.date)) || a.ticker.localeCompare(b.ticker); });
    var groups = groupBy(rows, function (r) { return r.phase || 'NEUTRAL'; }), inDist = rows.filter(function (r) { return r.phase === 'DISTRIBUTION'; }), fresh = rows.filter(function (r) { return fin(r.age) && r.age <= 7; });
    var pos = function (r) { return fin(r.lo) && fin(r.hi) && r.hi > r.lo && fin(r.price) ? (r.price - r.lo) / (r.hi - r.lo) * 100 : null; };
    var row = function (r) { return { name: r.ticker, sub: [PHASE[r.phase] || r.phase || '', fin(r.days) ? plural(r.days, 'day') + ' in phase' : '', fin(pos(r)) ? pct(pos(r), 0) + ' up its range' : '', fin(r.whales) ? '13F ' + money(r.whales, true) : ''].filter(Boolean).join(DOT),
      big: fin(r.pressure) ? fix(r.pressure) : D, tone: tone(r.pressure), small: day(r.date) + DOT + 'pressure' }; };
    var book = function (list) { return FL((list || []), function (x) { return { name: x.ticker, sub: [PHASE[x.phase] || x.phase, 'since ' + day(x.begin), fin(x.price) ? 'last ' + fix(x.price) : ''].filter(Boolean).join(DOT), big: fin(x.pressure) ? fix(x.pressure) : D, tone: tone(x.pressure), small: fin(x.days) ? plural(x.days, 'day') : '' }; }, BOOK); };
    var phaseOf = function (r) { return PHASE[r.phase] || r.phase; }, link = { href: function (r) { return chartOf(r.ticker); }, find: 'Find a ticker or a phase' };
    var S = sheet([tx('t', 'Ticker', 'ticker', { find: 'start' }), tx('date', 'Buying climax', dayOf('date')), tx('phase', 'Phase now', phaseOf), nx('days', 'Days in phase', 'days', fN(0)), nx('press', 'Pressure', 'pressure', fS(2), { tone: true }),
      nx('pos', 'Up its range', pos, fP(0)), nx('last', 'Last', 'price', fPx), nx('lo', 'Range low', 'lo', fPx), nx('hi', 'Range high', 'hi', fPx), nx('whales', '13F flow', 'whales', fMs, { tone: true })], ['name', 'names'], link);
    var BOOK = sheet([tx('t', 'Ticker', 'ticker', { find: 'start' }), tx('phase', 'Phase', phaseOf), tx('begin', 'Since', dayOf('begin')), nx('days', 'Days', 'days', fN(0)), nx('press', 'Pressure', 'pressure', fS(2), { tone: true }), nx('last', 'Last', 'price', fPx)], ['name', 'names'], link);
    var newest = rows[0];
    return {
      eyebrow: 'Buying climax tearsheet' + DOT + 'read ' + day(ref) + DOT + num(d.analyzed_n) + ' names analysed by the phase detector',
      title: num(rows.length) + ' names carry a live buying climax' + DOT + num(inDist.length) + ' of them are in distribution now' + DOT + num(fresh.length) + ' printed in the last week',
      sub: 'Daily bars' + DOT + 'climactic volume at or near a high, a wide range, a close off the extreme' + DOT + 'not a short by itself: the proof is weakness on the reaction' + DOT + 'published ' + day(ref),
      tiles: [{ k: 'Live buying climaxes', v: num(rows.length), s: 'of ' + num(d.analyzed_n) + ' names analysed' },
        { k: 'Printed in the last week', v: num(fresh.length), s: fresh.length ? tickers(fresh, 5) : 'no new climax' },
        { k: 'In distribution now', v: num(inDist.length), s: inDist.length ? tickers(inDist, 5) : 'none of them' },
        { k: 'Newest', v: newest ? newest.ticker : D, s: newest ? day(newest.date) + DOT + (PHASE[newest.phase] || newest.phase || '') : '' },
        { k: 'Distribution book', v: num(pc.DISTRIBUTION), s: num((B.distribution_beginning || []).length) + ' beginning' + DOT + num((B.distribution_mature || []).length) + ' mature' },
        { k: 'Ended in markdown', v: num((B.distribution_ended_markdown || []).length), s: 'distributions that resolved down' }],
      cards: [newest ? { tone: '', big: day(newest.date).slice(5), eyebrow: 'Newest buying climax', title: newest.ticker, text: 'Printed ' + day(newest.date) + ' (' + daysWord(newest.age) + '). Phase now: ' + lower(PHASE[newest.phase] || newest.phase) + (fin(newest.days) ? ', ' + plural(newest.days, 'day') + ' in it' : '')
        + (fin(newest.lo) ? '; range ' + fix(newest.lo) + ' to ' + fix(newest.hi) + ', last ' + fix(newest.price) : '') + (fin(newest.pressure) ? '; pressure ' + fix(newest.pressure) : '') + '.' }
        : { tone: '', big: D, eyebrow: 'Newest buying climax', title: 'None', text: 'No name in the book carries a live buying climax.' },
        (B.distribution_beginning || [])[0] ? { tone: 'down', big: num((B.distribution_beginning || []).length), eyebrow: 'Distribution beginning' + DOT + 'context, not the signal', title: (B.distribution_beginning || []).slice(0, 6).map(function (x) { return x.ticker; }).join(', '),
          text: 'Names whose distribution phase began most recently: ' + (B.distribution_beginning || []).map(function (x) { return x.ticker + ' (' + day(x.begin) + ')'; }).join(', ') + '.' }
          : { tone: '', big: D, eyebrow: 'Distribution beginning', title: 'None', text: 'No distribution began recently.' }],
      notes: [['The climax:', 'buyers rush a high on climactic volume and the bar closes off its extreme. Supply met them there.'],
        ['What comes next:', 'weakness on the reaction, or an upthrust that fails. Until one prints the trend has not been broken.'],
        ['Caveat:', 'the phase detector reads a book of ' + num(d.universe_n) + ' liquid names, not the whole warehouse. Pressure is its own reading of effort against result, shown as published.']],
      ring: { open: 'newest', title: 'Where they stand now', hint: 'names by phase', n: rows.length, center: num(rows.length), subs: ['NAMES', 'DISTRIBUTION ' + inDist.length], whole: 'the names', aria: 'Names with a live buying climax by phase',
        slices: compose(groups, function (k) { return PHASE[k] || k; }, function (g) { return pct(share(g.list.length, rows.length), 0) + DOT + tickers(g.list, 4); }), caption: 'Each name once, under the phase the detector reads for it on ' + day(ref) + '.' },
      lists: [{ key: 'newest', title: 'Newest buying climaxes', hint: 'by the date of the bar', rows: FL(rows, row, S, 'date', -1), none: 'no live buying climax' },
        { key: 'dist-begin', title: 'Distribution beginning', hint: 'context', tone: 'down', rows: book(B.distribution_beginning), none: 'none' },
        { key: 'dist-mature', title: 'Distribution mature', hint: 'context', tone: 'down', rows: book(B.distribution_mature), none: 'none' },
        { key: 'dist-ended', title: 'Ended in markdown', hint: 'distributions that resolved', tone: 'down', rows: book(B.distribution_ended_markdown), none: 'none' }],
      foot: 'Source: justhodl-phase-detector (data/phase-detector.json), published ' + String(ref || '').slice(0, 16).replace('T', ' ') + ' UTC; ' + num(d.analyzed_n) + ' of ' + num(d.universe_n) + ' names analysed. Phases in the book: '
        + Object.keys(pc).map(function (k) { return lower(PHASE[k] || k) + ' ' + pc[k]; }).join(', ') + '.'
    };
  };

  WY.spring = function (D_) {
    var d = phaseBook(D_), ref = d.generated_at, rows = [], pc = d.phase_counts || {};
    Object.keys(d.tickers).forEach(function (t) {
      var v = d.tickers[t] || {}, fired = null;
      (v.events || []).forEach(function (e) { if (e && e.type === 'SPRING') fired = e; });
      var dist = v.range_low && v.price ? (v.price - v.range_low) / v.range_low * 100 : null, base = { ticker: t, phase: v.phase, price: v.price, lo: v.range_low, hi: v.range_high, days: v.days_in_phase || 0, pressure: v.pressure, dist: dist };
      if (v.phase === 'ACCUMULATION' && v.range_low && v.price) {
        if (fired) rows.push(Object.assign(base, { kind: 'FIRED', date: fired.date, age: daysBetween(fired.date, ref) }));
        else if ((v.days_in_phase || 0) >= 10 && dist <= 3) rows.push(Object.assign(base, { kind: 'FORMING' }));
      } else if (fired && v.phase === 'DISTRIBUTION') rows.push(Object.assign(base, { kind: 'FAILED', date: fired.date, age: daysBetween(fired.date, ref) }));
    });
    var K = [['FIRED', 'Fired', 'pos', 1], ['FORMING', 'Forming', 'pos', 0.5], ['FAILED', 'Failed', 'oth', 0.34]];
    var G = K.map(function (k) { return rows.filter(function (r) { return r.kind === k[0]; }); }), fired = G[0].sort(function (a, b) { return String(b.date).localeCompare(String(a.date)); }), forming = G[1].sort(by(function (r) { return r.dist; })), failed = G[2];
    var row = function (r) { return { name: r.ticker, sub: ['range ' + fix(r.lo) + ' to ' + fix(r.hi), plural(r.days, 'day') + ' in the range', fin(r.pressure) ? 'pressure ' + fix(r.pressure) : ''].filter(Boolean).join(DOT), big: zpct(r.dist, 1, true), tone: tone(r.dist), small: (r.date ? day(r.date) + DOT : '') + 'vs range low' }; };
    var S = sheet([tx('t', 'Ticker', 'ticker', { find: 'start' }), tx('state', 'State', function (r) { return r.kind ? lower(r.kind) : null; }), tx('date', 'Spring', dayOf('date')), nx('dist', 'Against the range low', 'dist', fP(1, true), { tone: true }),
      nx('lo', 'Range low', 'lo', fPx), nx('hi', 'Range high', 'hi', fPx), nx('last', 'Last', 'price', fPx), nx('days', 'Days in the range', 'days', fN(0)), nx('press', 'Pressure', 'pressure', fS(2), { tone: true })],
      ['name', 'names'], { href: function (r) { return chartOf(r.ticker); }, find: 'Find a ticker' });
    var card = function (r, label, none, how) {
      if (!r) return { tone: '', big: D, eyebrow: label, title: 'None', text: none };
      return { tone: tone(r.dist), big: zpct(r.dist, 1, true), eyebrow: label, title: r.ticker, text: how(r) + ' Range ' + fix(r.lo) + ' to ' + fix(r.hi) + ', ' + plural(r.days, 'day') + ' old; last ' + fix(r.price) + ', ' + zpct(r.dist, 1, true) + ' against the range low' + (fin(r.pressure) ? '; pressure ' + fix(r.pressure) : '') + '.' };
    };
    return {
      eyebrow: 'Spring tearsheet' + DOT + 'read ' + day(ref) + DOT + num(pc.ACCUMULATION) + ' names in accumulation of ' + num(d.analyzed_n) + ' analysed',
      title: num(rows.length) + ' names are at a spring' + DOT + num(fired.length) + ' fired, ' + num(forming.length) + ' forming, ' + num(failed.length) + ' failed',
      sub: 'A range under accumulation, then an undercut of support that closes back inside' + DOT + 'forming = 10 days or more in the range and within 3% of its low' + DOT + 'published ' + day(ref),
      tiles: [{ k: 'At a spring', v: num(rows.length), s: 'of ' + num(pc.ACCUMULATION) + ' in accumulation and ' + num(pc.DISTRIBUTION) + ' in distribution' },
        { k: 'Fired', v: num(fired.length), s: fired.length ? tickers(fired, 5) : 'no spring has printed' },
        { k: 'Forming', v: num(forming.length), s: forming.length ? tickers(forming, 5) : 'nothing sits at its range low' },
        { k: 'Failed', v: num(failed.length), s: failed.length ? tickers(failed, 5) + DOT + 'a spring inside distribution' : 'none' },
        { k: 'Closest to the low', v: forming[0] ? forming[0].ticker : D, s: forming[0] ? zpct(forming[0].dist, 1, true) + ' against its range low' : '' },
        { k: 'Median range age', v: fin(med(rows.map(function (r) { return r.days; }))) ? num(med(rows.map(function (r) { return r.days; }))) + ' days' : D, s: 'days in the phase' }],
      cards: [card(fired[0], 'Newest fired spring', 'No spring has fired in a name still under accumulation.', function (r) { return 'Spring on ' + day(r.date) + ' (' + daysWord(r.age) + '), the name still in accumulation.'; }),
        card(forming[0], 'Closest to its range low', 'No accumulation range is within 3% of its low.', function () { return 'No spring has printed yet: the undercut is what to watch for.'; })],
      notes: [['Fired:', 'a spring event while the phase is still accumulation. Compare its volume with the climax before trusting it.'],
        ['Failed:', 'a spring event in a name whose phase is distribution. That is an upthrust\u2019s cousin, not a buy.'],
        ['Caveat:', 'the phase detector reads a book of ' + num(d.universe_n) + ' liquid names, not the whole warehouse.']],
      ring: { title: 'Springs by state', hint: 'names', n: rows.length, center: num(rows.length), subs: ['NAMES', 'FIRED ' + fired.length + DOT + 'FORMING ' + forming.length], whole: 'the names', aria: 'Springs by state',
        slices: K.map(function (k, i) { return { open: G[i].length ? ['fired', 'forming', 'failed'][i] : '', name: k[1], value: G[i].length, tone: k[2], op: k[3], big: num(G[i].length), small: pct(share(G[i].length, rows.length), 0) + (G[i].length ? DOT + tickers(G[i], 4) : '') }; }),
        caption: 'Each name once: fired (the spring printed, still in accumulation), forming (at the range low, no spring yet) or failed (a spring inside distribution).' },
      lists: [{ key: 'fired', title: 'Fired', hint: 'newest first', tone: 'up', rows: FL(fired, row, S, 'date', -1), none: 'no spring has fired' },
        { key: 'forming', title: 'Forming', hint: 'closest to the range low', rows: FL(forming, row, S, 'dist', 1), none: 'nothing is forming' },
        { key: 'failed', title: 'Failed', hint: 'a spring inside distribution', tone: 'down', rows: FL(failed, row, S), none: 'no failed spring' }],
      foot: 'Source: justhodl-phase-detector (data/phase-detector.json), published ' + String(ref || '').slice(0, 16).replace('T', ' ') + ' UTC; ' + num(d.analyzed_n) + ' of ' + num(d.universe_n) + ' names analysed.'
    };
  };

  /* Candle spread: Katlin's picks whose band is tight (the table's own rule) */
  WY.spread = function (D_) {
    var d = D_.katlin;
    if (!d || !d.picks) throw new Error('Katlin\u2019s feed (data/katlin.json) is not readable');
    var rows = d.picks.filter(function (x) { return x && x.bbw_pctile != null && x.bbw_pctile <= 20 && (x.squeeze || (x.accum_legs && x.accum_legs.bb_tight >= 70)); }).sort(by(function (r) { return r.bbw_pctile; }));
    var where = function (r) { return r.asset_class === 'stock' || !r.asset_class ? (r.sector || 'Unclassified') : r.asset_class === 'etf' ? 'ETFs' : r.asset_class === 'crypto' ? 'Crypto' : r.asset_class; };
    var sq = rows.filter(function (r) { return r.squeeze; }), legs = function (r) { return r.accum_legs || {}; }, acc = function (r) { return (r.pillars || {}).accumulation; }, groups = groupBy(rows, where), u = d.universe || {};
    var S = sheet([tx('t', 'Ticker', 'ticker', { find: 'start' }), tx('name', 'Name', 'name'), tx('where', 'Sector or class', where), nx('mcap', 'Mkt cap', function (r) { return fin(r.mcap) ? r.mcap : null; }, fM),
      nx('bbw', 'Band width, percentile', 'bbw_pctile', fN(0), { tip: 'Bollinger band width against the pick\u2019s own history: low is tight' }), tx('sq', 'Coil', function (r) { return r.squeeze ? 'squeeze' : 'tight'; }),
      nx('rc', 'Range contraction', function (r) { return legs(r).range_contraction; }, fN(0)), nx('dry', 'Volume dry-up', function (r) { return legs(r).vol_dryup; }, fN(0)), nx('vr', '20-day over 120-day volume', 'vol_ratio_20_120', fN(2)),
      nx('acc', 'Accumulation pillar', acc, fN(0)), tx('tier', 'Tier', function (r) { return r.tier ? lower(String(r.tier).replace(/_/g, ' ')) : null; }), nx('weeks', 'Base, weeks', 'base_weeks', fN(0)), nx('last', 'Last', 'last', fPx)],
      ['pick', 'picks'], { href: function (r) { return chartOf(r.asset_class === 'crypto' ? r.ticker + '-USD' : r.ticker); }, find: 'Find a ticker, name, sector or tier', phone: ['t', 'bbw', 'sq', 'acc', 'mcap', 'where'] });
    var row = function (r) { return { name: tick(r), sub: [where(r), r.squeeze ? 'squeeze' : 'tight', 'range contraction ' + num(legs(r).range_contraction), 'volume ' + (fin(r.vol_ratio_20_120) ? r.vol_ratio_20_120.toFixed(2) : D) + ' of its 120-day'].join(DOT), big: 'p' + num(r.bbw_pctile), small: 'band width' }; };
    var card = function (r, label) {
      if (!r) return { tone: '', big: D, eyebrow: label, title: 'None', text: 'No pick is coiled.' };
      return { tone: '', big: 'p' + num(r.bbw_pctile), eyebrow: label, title: tick(r), text: (fin(r.mcap) ? money(r.mcap) + ' of market cap, ' : '') + where(r) + '. Bollinger width at the ' + num(r.bbw_pctile) + 'th percentile of its own history' + (r.squeeze ? ', in a squeeze' : '')
        + '; range contraction ' + num(legs(r).range_contraction) + ' of 100, volume dry-up ' + num(legs(r).vol_dryup) + ', 20-day volume ' + (fin(r.vol_ratio_20_120) ? r.vol_ratio_20_120.toFixed(2) : D) + ' of the 120-day. Accumulation pillar ' + num(acc(r))
        + (r.tier ? ', tier ' + lower(String(r.tier).replace(/_/g, ' ')) : '') + '. Last ' + fix(r.last) + '.' };
    };
    return {
      eyebrow: 'Candle spread tearsheet' + DOT + 'session ' + day(d.session || d.as_of) + DOT + num(d.picks.length) + ' Katlin picks of ' + num(u.scored) + ' scored',
      title: num(rows.length) + ' picks are coiled' + DOT + num(sq.length) + ' in a squeeze, ' + num(rows.length - sq.length) + ' tight' + DOT + 'tightest: ' + (rows[0] ? rows[0].ticker + ' at the ' + num(rows[0].bbw_pctile) + 'th percentile' : D),
      sub: 'Daily bars' + DOT + 'Bollinger width at or under the 20th percentile, with a squeeze or a tight-band leg of 70 or more' + DOT + 'this is coiling, not a trigger' + DOT + 'published ' + day(d.generated_at),
      tiles: [{ k: 'Coiled', v: num(rows.length), s: 'of ' + num(d.picks.length) + ' picks' },
        { k: 'Squeeze / tight', v: num(sq.length) + ' / ' + num(rows.length - sq.length), s: 'band inside the channel / tight-band leg 70+' },
        { k: 'Median band width', v: 'p' + num(med(rows.map(function (r) { return r.bbw_pctile; }))), s: 'percentile of each name\u2019s own history' },
        { k: 'Median volume', v: fin(med(rows.map(function (r) { return r.vol_ratio_20_120; }))) ? med(rows.map(function (r) { return r.vol_ratio_20_120; })).toFixed(2) : D, s: '20-day against 120-day' },
        { k: 'Accumulation 65+', v: num(rows.filter(function (r) { return acc(r) >= 65; }).length), s: 'coiled with the accumulation pillar behind it' },
        { k: 'Most names', v: groups[0] ? groups[0].key : D, s: groups[0] ? num(groups[0].list.length) + ' of ' + num(rows.length) : '' }],
      cards: [card(rows[0], 'Tightest band'), card(rows.filter(function (r) { return fin(r.mcap); }).sort(by(cap, true))[0], 'Largest company coiled')],
      notes: [['The coil:', 'a narrow daily range inside a narrow Bollinger band. Energy is stored; direction is not given.'],
        ['What comes next:', 'expansion. Which way is read from the structure around it: a base after a low, or a ledge under a high.'],
        ['Caveat:', 'Katlin\u2019s permission panel below governs entries; a coiled row is research, not a signal.']],
      ring: { open: 'tightest', title: 'Where the coils are', hint: 'names by sector or class', n: rows.length, center: num(rows.length), subs: ['NAMES', 'SQUEEZE ' + sq.length], whole: 'the coiled picks', aria: 'Coiled picks by sector',
        slices: compose(groups, function (k) { return k; }, function (g) { return pct(share(g.list.length, rows.length), 0) + DOT + tickers(g.list); }), caption: 'Each coiled pick once, under its sector (stocks) or its class (ETFs, crypto).' },
      lists: [{ key: 'tightest', title: 'Tightest bands', hint: 'Bollinger width percentile', rows: FL(rows, row, S, 'bbw', 1), none: 'no pick is coiled' },
        { key: 'contraction', title: 'Most range contraction', hint: 'the leg, 0 to 100', rows: FL(rows.filter(function (r) { return fin(legs(r).range_contraction); }).sort(by(function (r) { return legs(r).range_contraction; }, true)), function (r) {
          return { name: tick(r), sub: where(r) + DOT + 'band p' + num(r.bbw_pctile) + DOT + (r.squeeze ? 'squeeze' : 'tight'), big: num(legs(r).range_contraction), small: 'of 100' }; }, S, 'rc', -1), none: 'none' },
        { key: 'dry', title: 'Driest volume', hint: '20-day against 120-day', rows: FL(rows.filter(function (r) { return fin(r.vol_ratio_20_120); }).sort(by(function (r) { return r.vol_ratio_20_120; })), function (r) {
          return { name: tick(r), sub: where(r) + DOT + 'band p' + num(r.bbw_pctile), big: r.vol_ratio_20_120.toFixed(2), small: 'of its 120-day' }; }, S, 'vr', 1), none: 'none' },
        { key: 'accum', title: 'Most accumulation behind it', hint: 'Katlin\u2019s pillar, 0 to 100', tone: 'up', rows: FL(rows.filter(function (r) { return fin(acc(r)); }).sort(by(acc, true)), function (r) {
          return { name: tick(r), sub: where(r) + DOT + 'band p' + num(r.bbw_pctile) + (r.tier ? DOT + lower(String(r.tier).replace(/_/g, ' ')) : ''), big: num(acc(r)), small: 'pillar' }; }, S, 'acc', -1), none: 'none' },
        { key: 'largest', title: 'Largest companies coiled', hint: 'market cap', rows: FL(rows.filter(function (r) { return fin(r.mcap); }).sort(by(cap, true)), function (r) { return { name: tick(r), sub: where(r) + DOT + 'band p' + num(r.bbw_pctile), big: money(r.mcap), small: r.squeeze ? 'squeeze' : 'tight' }; }, S, 'mcap', -1), none: 'none' }],
      foot: 'Source: justhodl-katlin ' + (d.version || '') + ' (data/katlin.json), session ' + day(d.session || d.as_of) + ', published ' + String(d.generated_at || '').slice(0, 16).replace('T', ' ') + ' UTC; ' + num(d.picks.length) + ' picks published of ' + num(u.scored) + ' instruments scored.'
    };
  };

  var WY_FEEDS = ['/data/bottom.json', '/data/phase-detector.json', '/data/katlin.json', '/data/accumulation-radar.json', '/data/capitulation-desk.json', '/data/campaign-scan.json'];
  function wyDocs(docs) {
    var one = docs && docs.length === 1 ? docs[0] : null;
    if (one && one.__wyckoff) return one;
    return { bottom: docs[0], phase: docs[1], katlin: docs[2], radar: docs[3], capit: docs[4], scan: docs[5] };
  }
  function wyTab(st) {
    if (st && st.tab) return st.tab;
    try { return (new URLSearchParams(location.search).get('tab') || 'sc').toLowerCase(); } catch (e) { return 'sc'; }
  }
  ADAPTERS.wyckoff = {
    feeds: WY_FEEDS, soft: true,
    share: { global: 'JHWyckoffDocs', event: 'jh:wyckoff-docs', ms: 9000 },   /* the desk has already read these feeds: use its copies */
    tabs: WY,
    build: function (docs, st) {
      var f = WY[wyTab(st)];
      return f ? f(wyDocs(docs), st || {}) : { hide: true };
    }
  };
  ADAPTERS.capitulation = {
    feeds: ['/data/capitulation-desk.json'],
    share: { global: 'JHCapitDoc', event: 'jh:capit-doc', ms: 8000 },
    build: function (docs) { return WY.capit({ capit: docs[0] }); }
  };

  /* -- the Volatility desk (stock-volatility.html): data/stock-volatility.json (justhodl-stock-volatility). The whole is
        the market value of the ranked common stocks, cut by how volatile each one is now; a card is the engine's
        own board and its heading opens the whole ranking that board is the end of, so every name on the sheet is a name on
        the desk. Volatility is neither good nor bad: no list, figure or column is coloured up or down. The four buttons choose the window of the price-spread lists. -- */
  ADAPTERS.volatility = {
    feeds: ['/data/stock-volatility.json'],
    share: { global: 'JHStockVolDoc', event: 'jh:stock-vol-doc', ms: 8000 },   /* the desk has already read this 2.7 MB feed: use its copy */
    build: function (docs, st) {
      var d = docs[0], cols = d.cols || [], U = d.universe || {}, mk = d.market || {}, M = mk.stock || {}, md = M.med || {}, B = mk.benchmark || {}, boards = d.boards || {};
      var WIN = { d: { label: '1 DAY', word: 'the day', of: 'day' }, w: { label: '1 WEEK', word: 'the week', of: 'week' }, m: { label: '1 MONTH', word: 'the month', of: 'month' }, q: { label: '1 QUARTER', word: 'the quarter', of: 'quarter' } };
      var k = WIN[st.fund] ? st.fund : 'm', W = WIN[k], sp = 'sp_' + k;
      var byT = {}, every = [];
      (d.rows || []).forEach(function (r) { var o = {}, i; for (i = 0; i < cols.length; i++) o[cols[i]] = r[i]; byT[o.t] = o; every.push(o); });
      var rowsOf = function (list) { return (list || []).map(function (t) { return byT[t]; }).filter(Boolean); };
      var nm = function (r) { return r.t + (r.name ? DOT + r.name : ''); };
      var pp = function (v, dp) { return fin(v) ? num(v, dp == null ? (Math.abs(v) < 10 ? 2 : 1) : dp) + '%' : D; };
      var vp = function (v) { return pp(v, 1); };
      var pts = function (v) { return fin(v) ? sign(v, true) + Math.abs(v).toFixed(1) + ' pts' : D; };
      var px = function (v) { return fin(v) ? num(v, Math.abs(v) < 10 ? 3 : 2) : D; };
      var chg = function (r) { return fin(r.hv21) && fin(r.hv21p) ? r.hv21 - r.hv21p : null; };
      var where = function (r) { return (r.sector || 'Unclassified') + DOT + money(r.mcap); };
      var inYear = function (v) { return !fin(v) ? '' : v >= 99.5 ? 'its highest in a year' : v <= 0.5 ? 'its lowest in a year' : 'above ' + num(v, 0) + '% of its year'; };
      /* -- the whole: ranked common stocks by volatility band -- */
      var bands = (d.bands || []).filter(function (b) { return fin(b.mcap); }), total = sum(bands, function (b) { return b.mcap; }), nRanked = sum(bands, function (b) { return b.n; });
      var hot = sum(bands.filter(function (b) { return b.from >= 40; }), function (b) { return b.mcap; }), calm = sum(bands.filter(function (b) { return b.to <= 25; }), function (b) { return b.mcap; });
      var share = function (v) { return total ? v / total * 100 : null; };
      var OPS = [0.2, 0.34, 0.5, 0.66, 0.82, 1];
      /* -- counts -- */
      var aside = (U.set_aside || {}).stock || 0, pinnedN = (U.pinned || {}).stock || 0, moved = (M.rising || 0) + (M.falling || 0), risingPct = moved ? M.rising / moved * 100 : null;
      var dir = !fin(md.hv21) || !fin(md.hv21p) ? '' : md.hv21 > md.hv21p ? ', up from ' + vp(md.hv21p) + ' a month ago' : md.hv21 < md.hv21p ? ', down from ' + vp(md.hv21p) + ' a month ago' : ', where it stood a month ago';
      /* -- rows -- */
      var volRow = function (r) { return { name: nm(r), sub: where(r) + DOT + vp(r.hv21p) + ' a month ago', big: vp(r.hv21), small: inYear(r.hvp) }; };
      var spreadRow = function (r) { return { name: nm(r), sub: where(r) + DOT + 'low ' + px(r['lo_' + k]) + ' to high ' + px(r['hi_' + k]), big: pp(r[sp]), small: 'volatility ' + vp(r.hv21) }; };
      var moveRow = function (r) { return { name: nm(r), sub: where(r) + DOT + 'was ' + vp(r.hv21p), big: pts(chg(r)), small: 'now ' + vp(r.hv21) }; };
      var pinRow = function (r) { return { name: nm(r), sub: where(r) + DOT + 'last ' + px(r.px), big: pp(r.sp_m), small: 'month spread' }; };
      var sectors = (d.by_sector || []).filter(function (g) { return g.med && fin(g.med.hv21); }).sort(by(function (g) { return g.med.hv21; }, true));
      var sectorRow = function (g) { return { name: g.sector, sub: plural(g.n, 'stock') + DOT + money(g.mcap) + DOT + num(g.rising || 0) + ' rising, ' + num(g.falling || 0) + ' falling', big: vp(g.med.hv21), small: W.of + ' spread ' + pp(g.med[sp]) }; };
      var sizes = (d.by_size || []).filter(function (g) { return g.med && fin(g.med.hv21); });
      var notable = function (r, end) {
        var when = day(d.as_of);
        if (!r) return { tone: '', big: D, eyebrow: end + ' volatile' + DOT + when, title: 'None', text: 'No company worth $1B or more is ranked on ' + when + '.' };
        return { tone: '', big: vp(r.hv21), eyebrow: end + ' volatile' + DOT + 'companies worth $1B or more', title: nm(r),
          text: money(r.mcap) + ' of market value, ' + (r.sector || 'unclassified') + (r.industry ? ' (' + r.industry + ')' : '') + '. ' + vp(r.hv21) + ' a year on its last 21 sessions, ' + vp(r.hv21p) + ' a month earlier, ' + vp(r.hv252) + ' over a year'
            + (fin(r.hvp) ? '; ' + (r.hvp >= 99.5 ? 'the highest reading of its last year' : r.hvp <= 0.5 ? 'the lowest reading of its last year' : 'above ' + num(r.hvp, 0) + '% of its own readings of the last year') : '') + '. Largest one-day move of the quarter ' + pct(r.mx, 1, true) + ' on ' + day(r.mxd) + '. Price spread ' + pp(r.sp_d) + ' for the day, ' + pp(r.sp_w) + ' the week, '
            + pp(r.sp_m) + ' the month, ' + pp(r.sp_q) + ' the quarter.' };
      };
      var most = rowsOf(boards.most_volatile), least = rowsOf(boards.least_volatile), wide = rowsOf(boards['widest_' + k]), narrow = rowsOf(boards['narrowest_' + k]);
      var rising = rowsOf(boards.vol_rising), falling = rowsOf(boards.vol_falling), pinned = rowsOf(d.pinned).sort(by(function (r) { return r.mcap || 0; }, true));
      /* -- the whole of every list. A board is the 25 names at one end of a ranking; the whole list is that ranking: common
            stocks at or above the board's size floor that the engine ranked for the measure (its rk_ column), so the first
            25 rows of a list opened in full are the card's own. Rising / falling is the engine's rule too: a reading 21
            sessions ago, and no flagged jump inside the two windows being compared. -- */
      var BR = d.board_rule || {}, floor = fin(BR.min_mcap) ? BR.min_mcap : 1e9, prior = (d.windows || {}).prior || 21, RK = { hv21: 'rk_hv21', sp_d: 'rk_d', sp_w: 'rk_w', sp_m: 'rk_m', sp_q: 'rk_q' };
      var rankedBy = function (m) { return every.filter(function (r) { return r.kind === 'stock' && (r.mcap || 0) >= floor && fin(r[RK[m]]); }).sort(by(function (r) { return r[RK[m]]; })); };
      var volAll = rankedBy('hv21'), spAll = rankedBy(sp), back = function (list) { return list.slice().reverse(); };
      var moving = volAll.filter(function (r) { return fin(r.hv21p) && (r.ja == null || r.ja > 21 + prior); }).sort(function (a, b) { return chg(b) - chg(a) || (a.t < b.t ? -1 : a.t > b.t ? 1 : 0); });
      var upAll = moving.filter(function (r) { return chg(r) > 0; }), downAll = back(moving).filter(function (r) { return chg(r) < 0; });
      var rankedAll = every.filter(function (r) { return r.kind === 'stock' && fin(r.rk_hv21); }).sort(by(function (r) { return r.mcap || 0; }, true));
      var asideAll = every.filter(function (r) { return r.kind === 'stock' && r.aside; }).sort(by(function (r) { return r.mcap || 0; }, true)), etfAll = every.filter(function (r) { return r.kind === 'etf'; }).sort(by(function (r) { return r.mcap || 0; }, true));
      var WHY = { jump: 'an unexplained one-day jump', pinned: 'pinned to a deal price', thin: 'thin trading', print: 'a price print no trade supports' };
      var COL = {
        t: tx('t', 'Ticker', 't', { find: 'start' }), name: tx('name', 'Name', 'name'), sector: tx('sector', 'Sector', function (r) { return r.kind === 'stock' ? (r.sector || 'Unclassified') : null; }), industry: tx('industry', 'Industry', 'industry'),
        mcap: nx('mcap', 'Mkt value', 'mcap', fM), px: nx('px', 'Last', 'px', px),
        hv21: nx('hv21', 'Volatility, 21 sessions', 'hv21', vp, { tip: 'realized volatility of the last 21 daily closes, in percent a year' }), hv21p: nx('hv21p', 'A month ago', 'hv21p', vp, { tip: 'the same reading 21 sessions ago' }),
        dv: nx('dv', 'Change', chg, pts, { tip: 'today\u2019s 21-session volatility less the reading 21 sessions ago, in points' }), hvp: nx('hvp', 'In its own year', 'hvp', fN(0), { tip: 'where today\u2019s reading stands among the stock\u2019s own last 252 readings, 0 to 100' }),
        hv63: nx('hv63', '63 sessions', 'hv63', vp), hv252: nx('hv252', 'A year', 'hv252', vp), yz21: nx('yz21', 'Yang-Zhang, 21', 'yz21', vp, { tip: 'volatility read from the whole bar: the gap, the open-to-close move and the range' }),
        sp_d: nx('sp_d', 'Spread, day', 'sp_d', pp, { tip: 'the high over the low of the last session' }), sp_w: nx('sp_w', 'Spread, week', 'sp_w', pp, { tip: 'the highest high over the lowest low of the last 5 sessions' }),
        sp_m: nx('sp_m', 'Spread, month', 'sp_m', pp, { tip: 'the highest high over the lowest low of the last 21 sessions' }), sp_q: nx('sp_q', 'Spread, quarter', 'sp_q', pp, { tip: 'the highest high over the lowest low of the last 63 sessions' }),
        lo: nx('lo', 'Low of ' + W.word, 'lo_' + k, px), hi: nx('hi', 'High of ' + W.word, 'hi_' + k, px), pos: nx('pos', 'Close in the spread', 'pos_' + k, fN(0), { tip: 'where the last close sits inside the spread of ' + W.word + ': 0 at the low, 100 at the high' }),
        own: nx('own', 'Spread in its own year', 'own_' + k, fN(0), { tip: 'where the spread of ' + W.word + ' stands among the stock\u2019s own last 252 windows of that length, 0 to 100' }),
        atr: nx('atr', 'ATR %', 'atr', pp, { tip: 'average true range of the last 14 sessions as a percent of the last close' }), mx: nx('mx', 'Largest move', 'mx', fP(1, true), { tip: 'the largest one-session move of the last 63 sessions' }), mxd: tx('mxd', 'On', function (r) { return r.mxd ? day(r.mxd) : null; }),
        adv: nx('adv', '$ traded a day', 'adv', fM, { tip: '20-session average dollar volume' }), why: tx('why', 'Why it is set aside', function (r) { return WHY[r.why] || r.why; })
      };
      var ORDER = ['t', 'name', 'sector', 'industry', 'mcap', 'px', 'hv21', 'hv21p', 'dv', 'hvp', 'hv63', 'hv252', 'sp_d', 'sp_w', 'sp_m', 'sp_q', 'lo', 'hi', 'pos', 'own', 'yz21', 'atr', 'mx', 'mxd', 'adv'];
      /* one sheet a kind of list: the columns it leads with stand right after the name, so the ranked figure is on screen */
      var VS = function (lead, o) {
        o = o || {};
        var ids = ['t', 'name'].concat(lead, ORDER.filter(function (id) { return id !== 't' && id !== 'name' && lead.indexOf(id) < 0; })).filter(function (id) { return !(o.etf && (id === 'sector' || id === 'industry')); });
        if (o.why) ids.splice(2, 0, 'why');
        return sheet(ids.map(function (id) { return o.etf && id === 'mcap' ? nx('mcap', 'Assets', 'mcap', fM) : COL[id]; }), o.etf ? ['ETF', 'ETFs'] : ['stock', 'stocks'],
          { href: function (r) { return chartOf(r.t); }, find: o.etf ? 'Find a ticker or name' : 'Find a ticker, name, sector or industry', phone: ['t'].concat(lead, ['mcap', 'sector', 'name'].filter(function (id) { return lead.indexOf(id) < 0; })) });
      };
      var S_VOL = VS(['hv21', 'hv21p', 'dv', 'hvp']), S_SP = VS([sp, 'lo', 'hi', 'pos', 'own', 'hv21']), S_MOVE = VS(['dv', 'hv21', 'hv21p', 'hvp']), S_PIN = VS(['sp_m', 'mcap', 'px']), S_SIZE = VS(['mcap', 'hv21', 'hv21p', 'dv', sp]);
      var medOf = function (m) { return function (g) { return (g.med || {})[m]; }; };
      var S_SECT = sheet([tx('name', 'Sector', 'sector'), nx('n', 'Stocks', 'n', fN(0)), nx('mcap', 'Mkt value', 'mcap', fM), nx('hv21', 'Median volatility, 21', medOf('hv21'), vp), nx('hv21p', 'A month ago', medOf('hv21p'), vp), nx('hv63', '63 sessions', medOf('hv63'), vp),
        nx('hv252', 'A year', medOf('hv252'), vp), nx('up', 'Rising', function (g) { return g.rising || 0; }, fN(0)), nx('dn', 'Falling', function (g) { return g.falling || 0; }, fN(0)), nx('sp_d', 'Median spread, day', medOf('sp_d'), pp), nx('sp_w', 'Week', medOf('sp_w'), pp),
        nx('sp_m', 'Month', medOf('sp_m'), pp), nx('sp_q', 'Quarter', medOf('sp_q'), pp), nx('atr', 'Median ATR %', medOf('atr'), pp)], ['sector', 'sectors'], { phone: ['name', 'hv21', sp, 'n', 'mcap'] });
      var inBand = function (b) { return rankedAll.filter(function (r) { return r.hv21 >= b.from && (b.to == null || r.hv21 < b.to); }); };
      var pop = ((d.board_rule || {}).population || {}), others = ['d', 'w', 'm', 'q'].filter(function (x) { return x !== k; }).map(function (x) { return WIN[x].of + ' ' + pp(md['sp_' + x]); }).join(DOT);
      return {
        eyebrow: 'Volatility tearsheet' + DOT + 'bars ' + day(d.as_of) + DOT + num(U.scanned_stock) + ' stocks and ' + num(U.scanned_etf) + ' ETFs measured',
        title: 'The median stock is moving at ' + vp(md.hv21) + ' a year' + dir + DOT + pct(share(hot), 0) + ' of market value sits in stocks above 40%',
        sub: 'Realized volatility of daily closes over 21 sessions, in percent a year' + DOT + 'price spread = the highest high over the lowest low of the window' + DOT + 'lists from companies worth $1B or more' + DOT + 'published ' + day(d.generated_at),
        controls: ['d', 'w', 'm', 'q'].map(function (id) { return { id: id, label: WIN[id].label, on: id === k }; }),
        tiles: [{ k: 'Median stock volatility', v: vp(md.hv21), s: vp(md.hv21p) + ' a month ago' + DOT + '63 sessions ' + vp(md.hv63) + DOT + 'a year ' + vp(md.hv252) },
          { k: 'More volatile than a month ago', v: pct(risingPct, 0), s: num(M.rising || 0) + ' rising' + DOT + num(M.falling || 0) + ' falling' },
          { k: 'S&P 500 fund (' + (B.t || 'SPY') + ')', v: vp(B.hv21), s: vp(B.hv21p) + ' a month ago' + (fin(B.hvp) ? DOT + inYear(B.hvp) : '') },
          { k: 'Median price spread', v: pp(md[sp]), s: W.word + DOT + others },
          { k: 'Market value above 40%', v: pct(share(hot), 0), s: money(hot) + ' of ' + money(total) + DOT + 'under 25%: ' + pct(share(calm), 0) },
          { k: 'Measured, not ranked', v: num(aside), s: num(pinnedN) + ' pinned to a deal price' + DOT + num(aside - pinnedN) + ' jump, print or thin' }],
        cards: [notable(most[0], 'Most'), notable(least[0], 'Least')],
        notes: [['Spread:', 'the median stock traded a range of ' + pp(md.sp_d) + ' on the day, ' + pp(md.sp_w) + ' over the week, ' + pp(md.sp_m) + ' over the month and ' + pp(md.sp_q) + ' over the quarter, the highest high over the lowest low of each window.'
            + (B.t ? ' ' + B.t + ': ' + pp(B.sp_d) + ', ' + pp(B.sp_w) + ', ' + pp(B.sp_m) + ' and ' + pp(B.sp_q) + '.' : '')],
          ['Where:', (sectors.length ? 'by sector the median runs from ' + vp(sectors[0].med.hv21) + ' in ' + sectors[0].sector + ' to ' + vp(sectors[sectors.length - 1].med.hv21) + ' in ' + sectors[sectors.length - 1].sector + '. ' : '')
            + (sizes.length ? 'By size: ' + sizes.map(function (g) { return (g.label || g.size) + ' ' + vp(g.med.hv21); }).join(', ') + '.' : '')],
          ['Caveat:', 'this is realized volatility, what the closes did, not what options price. A stock that collapsed prints a wide spread, because the spread is the high over the low. ' + num(aside) + ' stocks are measured but not ranked or counted here: '
            + num(pinnedN) + ' pinned to a deal price, the rest for an unexplained one-day jump, a price print no trade supports, or thin trading.']],
        ring: { open: 'ranked', title: 'Market value by volatility', hint: 'fainter is calmer', n: nRanked, center: money(total), subs: [num(nRanked) + ' STOCKS', 'ABOVE 40% ' + pct(share(hot), 0)], whole: 'ranked market value',
          aria: 'Market value of the ranked common stocks by volatility band',
          slices: bands.map(function (b, i) { return { open: b.n ? 'band-' + i : '', name: b.band + ' a year', label: String(b.band).replace(/%/g, '').replace(' and over', '+'), value: b.mcap, tone: 'pos', op: OPS[Math.min(i, OPS.length - 1)], big: money(b.mcap), small: plural(b.n, 'stock') }; }),
          caption: 'Each ranked common stock is counted once, at its market value, in the band its 21-session volatility falls in on ' + day(d.as_of) + '. Fainter is calmer, brighter more volatile. Stocks set aside are not in the ring.' },
        lists: [{ key: 'most-volatile', title: 'Most volatile', hint: '21 sessions, a year', n: pop.hv21, rows: FL(most, volRow, S_VOL, 'hv21', -1, { rows: volAll }), none: 'no company is ranked' },
          { key: 'least-volatile', title: 'Least volatile', hint: '21 sessions, a year', n: pop.hv21, rows: FL(least, volRow, S_VOL, 'hv21', 1, { rows: back(volAll) }), none: 'no company is ranked' },
          { key: 'widest', title: 'Widest price spread', hint: W.word + ', high over low', n: pop[sp], rows: FL(wide, spreadRow, S_SP, sp, -1, { rows: spAll }), none: 'no company is ranked' },
          { key: 'narrowest', title: 'Narrowest price spread', hint: W.word + ', high over low', n: pop[sp], rows: FL(narrow, spreadRow, S_SP, sp, 1, { rows: back(spAll) }), none: 'no company is ranked' },
          { key: 'rising', title: 'Volatility rising', hint: 'against 21 sessions ago', rows: FL(rising, moveRow, S_MOVE, 'dv', -1, { rows: upAll }), none: 'no company is more volatile' },
          { key: 'falling', title: 'Volatility falling', hint: 'against 21 sessions ago', rows: FL(falling, moveRow, S_MOVE, 'dv', 1, { rows: downAll }), none: 'no company is calmer' },
          { key: 'sectors', title: 'Sectors', hint: 'median stock, most volatile first', rows: FL(sectors, sectorRow, S_SECT, 'hv21', -1), none: 'no sector is measured' },
          { key: 'pinned', title: 'Pinned to a deal price', hint: 'measured, not ranked', n: pinnedN, rows: FL(pinned, pinRow, S_PIN, 'mcap', -1), none: 'no stock is pinned',
            note: 'A stock whose last month fits inside ' + num(((d.windows || {}).pin_spread_m) || 3.5, 1) + '% is waiting for a takeover to close: its calm is the deal, not the company.' }],
        more: [hidden('ranked', 'Every ranked stock', 'common stocks of any size, largest first', rankedAll, S_SIZE, 'mcap', -1)]
          .concat(bands.map(function (b, i) { return hidden('band-' + i, b.band + ' a year', 'ranked stocks in the band, largest first', inBand(b), S_SIZE, 'mcap', -1); }))
          .concat([hidden('aside', 'Measured, not ranked', 'stocks set aside, largest first', asideAll, VS(['mcap', 'hv21', 'sp_m'], { why: true }), 'mcap', -1),
            hidden('etfs', 'Every ETF measured', 'largest first', etfAll, VS(['mcap', 'hv21', 'hv21p', 'dv', sp], { etf: true }), 'mcap', -1)]),
        order: ['most-volatile', 'least-volatile', 'widest', 'narrowest', 'rising', 'falling', 'sectors', 'ranked'].concat(bands.map(function (b, i) { return 'band-' + i; }), ['pinned', 'aside', 'etfs']),
        foot: 'Source: justhodl-stock-volatility ' + (d.version || '') + ' (data/stock-volatility.json), published ' + String(d.generated_at || '').slice(0, 16).replace('T', ' ') + ' UTC from the Polygon end-of-day bank, bars as of ' + day(d.as_of)
          + '. Volatility = the standard deviation of daily log returns, annualized. A card shows the names at one end of a ranking among common stocks worth $1B or more, and its heading opens the whole ranking; the count beside it is the size of that ranking.'
      };
    }
  };

  /* -- strength: every listed stock and ETF against the S&P 500 fund, today's forming bars or the last session --
     The desk (jh-strong-engine.js) lays the nightly whole-market table under today's forming bars and hands this sheet
     the view on every refresh; nothing is recomputed here but the sums of the scope on screen. */
  function strengthStats(list, spy) {
    var n = list.length, c = { up: 0, flat: 0, down_less: 0, down_more: 0 }, beat = 0;
    list.forEach(function (r) { c[r.out]++; if (r.ret > spy) beat++; });
    var held = n - c.down_more;
    return { n: n, up: c.up, flat: c.flat, down_less: c.down_less, down_more: c.down_more, held: held, held_pct: share(held, n), up_pct: share(c.up, n), beat: beat, beat_pct: share(beat, n),
      median_ret: med(list.map(function (r) { return r.ret; })), mcap: sum(list, cap) };
  }
  ADAPTERS.strength = {
    feeds: ['/data/strength-wide.json'],
    share: { global: 'JHStrengthDoc', event: 'jh:strength-doc', ms: 15000 },
    build: function (docs, st) {
      var v = docs[0];
      if (v && v.contract === 'strength-wide.v1') {      /* the desk handed nothing over: the table alone, read as its own last session */
        var E = typeof window !== 'undefined' ? window.JHStrong : null;
        if (!E || !E.model) throw new Error('the desk has not built its view');
        v = E.model(v, null, {});
      }
      var R = v.rules || {}, rd = v.read || {}, G = rd.groups || {}, spy = v.spy || {}, live = v.mode === 'live', red = rd.regime === 'dump' || rd.regime === 'down';
      var counted = (v.rows || []).filter(function (r) { return r.counted; });
      var stocks = counted.filter(function (r) { return r.kind === 'stock'; });
      var etfs = counted.filter(function (r) { return r.kind === 'etf' && (r.adv || 0) >= R.etf_min_adv; });
      var SC = { b1: { label: '$1B+ STOCKS', word: 'stocks worth $1B or more', unit: 'stock', rows: stocks.filter(function (r) { return (r.mcap || 0) >= R.board_min_mcap; }) },
        sp: { label: 'S&P 500', word: 'S&P 500 members', unit: 'member', rows: stocks.filter(function (r) { return r.sp; }) },
        all: { label: 'EVERY STOCK', word: 'listed stocks', unit: 'stock', rows: stocks },
        etf: { label: 'ETFS', word: 'ETFs trading $5M or more a day', unit: 'fund', rows: etfs } };
      var k = SC[st.fund] ? st.fund : 'b1', S = SC[k], rows = S.rows, isEtf = k === 'etf', g = strengthStats(rows, spy.ret);
      var hm = function (iso) { try { return new Date(iso).toLocaleTimeString('en-US', { timeZone: 'America/New_York', hour: 'numeric', minute: '2-digit' }) + ' ET'; } catch (e) { return String(iso || '').slice(11, 16) + ' UTC'; } };
      var when = live ? 'today ' + day(v.session) + (v.bars_as_of ? ', bars as of ' + hm(v.bars_as_of) : '') + (v.delay_minutes ? ', ' + v.delay_minutes + ' minutes delayed' : '') : 'last session ' + day(v.session) + ' (final)';
      var mv = function (x) { return zpct(x, 2, true); }, pts = function (x) { return fin(x) ? sign(+x.toFixed(2) || 0, true) + Math.abs(x).toFixed(2) + ' pts' : D; }, p0 = function (x) { return pct(x, 0); };
      var nm = function (r) { return r.t + (r.name ? DOT + r.name : ''); };
      var cls = function (r) { return (v.class_labels || {})[r.cls] || r.cls || 'ETF'; };
      var where = function (r) { return (r.kind === 'etf' ? cls(r) : (r.sector || 'Unclassified')) + DOT + money(r.mcap); };
      var carried = function (r) { return (fin(r.rank) ? 'rank ' + num(r.rank, 0) : 'not ranked') + (r.rec_n ? DOT + 'held ' + r.rec_h + ' of ' + r.rec_n + ' dump days' : ''); };
      var row = function (r) { return { name: nm(r), sub: where(r) + DOT + carried(r), big: mv(r.ret), small: pts(r.x) + ' vs SPY' }; };
      var top = function (list, f, desc) { return list.slice().sort(function (a, b) { return (desc === false ? f(a) - f(b) : f(b) - f(a)) || (a.t < b.t ? -1 : 1); }); };
      var ret = function (r) { return r.ret; }, x = function (r) { return r.x; };
      /* the engine keeps the funds that hold volatility itself off its ETF boards (rules.board_skip_cls): they rise when SPY falls by
         construction. They are counted in every figure of the sheet and left off its lists of names. */
      var skip = isEtf ? (R.board_skip_cls || []) : [], named = skip.length ? rows.filter(function (r) { return skip.indexOf(r.cls) < 0; }) : rows, unlisted = rows.length - named.length;
      var up = top(named.filter(function (r) { return r.out === 'up'; }), ret), less = top(named.filter(function (r) { return r.out === 'down_less'; }), x), more = top(named.filter(function (r) { return r.out === 'down_more'; }), ret, false);
      var strong = named.filter(function (r) { return (r.rank || 0) >= 80; }), weak = named.filter(function (r) { return fin(r.rank) && r.rank <= 20; });
      var sHold = top(strong.filter(function (r) { return r.out !== 'down_more'; }), x), sBreak = top(strong.filter(function (r) { return r.out === 'down_more'; }), x, false);
      var wBid = top(weak.filter(function (r) { return r.out === 'up'; }), ret), repeat = top(named.filter(function (r) { return r.bucket === 'all' && r.out !== 'down_more'; }), x);
      var every = rows.filter(function (r) { return r.bucket === 'all'; }), few = rows.filter(function (r) { return r.bucket === 'few'; });
      var gEvery = strengthStats(every, spy.ret), gFew = strengthStats(few, spy.ret);
      var ranked = top(rows.filter(function (r) { return fin(r.rank); }), function (r) { return r.rank; }, false), tenth = Math.floor(ranked.length / 10);
      var gTop = tenth >= 5 ? strengthStats(ranked.slice(-tenth), spy.ret) : null, gBot = tenth >= 5 ? strengthStats(ranked.slice(0, tenth), spy.ret) : null;
      /* a finished session is told in the past tense */
      var key = red ? 'held_pct' : 'beat_pct', keyWord = red ? (live ? 'holding up' : 'held up') : 'ahead of SPY', shareWord = red && !live ? 'that held up' : keyWord;
      var isWord = red ? (live ? 'are holding up' : 'held up') : (live ? 'are ahead of it' : 'were ahead of it'), nowWord = red ? (live ? 'are holding up now' : 'held up in the session') : (live ? 'are ahead of SPY now' : 'were ahead of SPY in the session');
      /* -- groups inside the scope -- */
      var grp = function (f, min) {
        return groupBy(rows, f).filter(function (q) { return q.key && q.list.length >= (min || 1); }).map(function (q) { var s = strengthStats(q.list, spy.ret); s.name = q.key; s.sector = q.list[0].sector || ''; return s; })
          .sort(function (a, b) { return (b[key] - a[key]) || (b.median_ret - a.median_ret) || (a.name < b.name ? -1 : 1); });
      };
      var sectors = isEtf ? grp(cls) : grp(function (r) { return r.sector || 'Unclassified'; }), inds = isEtf ? [] : grp(function (r) { return r.industry; }, R.industry_min || 5);
      var grow = function (q) { return { name: q.name, sub: plural(q.n, S.unit) + DOT + 'up ' + num(q.up) + ', flat ' + num(q.flat) + ', down less ' + num(q.down_less) + ', down more ' + num(q.down_more), big: p0(q[key]), small: 'median ' + mv(q.median_ret) }; };
      /* -- the whole: market value by how it is doing against SPY -- */
      var OUT = [['up', 'Up', 'pos', 1], ['flat', 'Flat', 'pos', 0.62], ['down_less', 'Down less than SPY', 'pos', 0.4], ['down_more', 'Down more than SPY', 'oth', null]];
      /* a slice opens the list of its names: the card of that outcome where the sheet shows one, a list kept off the cards where it does not */
      var OPEN = { up: 'up', flat: 'flat', down_less: 'down-less', down_more: red ? (isEtf ? 'down-most-etf' : 'down-more') : 'down-most' };
      var total = sum(rows, cap), heldCap = sum(rows.filter(function (r) { return r.out !== 'down_more'; }), cap);
      var slices = OUT.map(function (o) {
        var list = rows.filter(function (r) { return r.out === o[0]; }), val = sum(list, cap), s = { name: o[1], label: o[1].replace(' than SPY', ''), value: val, tone: o[2], big: money(val), small: plural(list.length, S.unit) + DOT + p0(share(list.length, rows.length)) + ' of them', open: OPEN[o[0]] };
        if (o[3] != null) s.op = o[3];
        return s;
      });
      /* -- the two cards -- */
      var bigOf = function (list) { return list.slice().sort(by(cap, true))[0]; };
      var good = bigOf(red ? up : up.filter(function (r) { return r.ret > spy.ret; }));
      var bad = bigOf(sBreak.length ? sBreak : more);
      var facts = function (r) {
        var out = [];
        if (fin(r.fo)) out.push(r.fo > 0 ? 'above its open' : 'below its open');
        if (fin(r.vwx)) out.push(r.vwx > 0 ? 'above the day\'s VWAP' : 'below the day\'s VWAP');
        if (fin(r.pos)) out.push('at ' + num(r.pos, 0) + '% of the day\'s range');
        if (fin(r.rv)) out.push('volume ' + times(r.rv) + ' the market\'s pace');
        return out.length ? ' It is ' + out.join(', ') + '.' : '';
      };
      var card = function (r, eyebrow, none) {
        if (!r) return { tone: '', big: D, eyebrow: eyebrow, title: 'None', text: none };
        return { tone: '', big: mv(r.ret), eyebrow: eyebrow, title: nm(r),
          text: money(r.mcap) + (r.kind === 'etf' ? ' of assets, ' + cls(r) : ' of market value, ' + (r.sector || 'unclassified') + (r.industry ? ' (' + r.industry + ')' : '')) + '. ' + mv(r.ret) + ' against SPY\'s ' + mv(spy.ret) + ': ' + pts(r.x) + '. '
            + (fin(r.rank) ? 'It carried a rank of ' + num(r.rank, 0) + ' into the session' : 'It carried no rank into the session') + (r.rec_n ? ' and had held up on ' + r.rec_h + ' of the quarter\'s ' + r.rec_n + ' dump days' : '') + '.' + facts(r) };
      };
      var title = (live ? 'SPY ' + mv(spy.ret) + ' today' : 'SPY ' + mv(spy.ret) + ' on ' + day(v.session)) + DOT + p0(g[key]) + ' of ' + S.word + ' ' + isWord
        + (gTop ? DOT + 'the strongest tenth coming in: ' + p0(gTop[key]) : '');
      var u = v.universe || {}, sa = u.set_aside || {}, aside = sum(Object.keys(sa), function (q) { return sa[q]; });
      /* -- every list opens in full (the kit draws it): all of its rows, ranked by any column either way --
         Khalid 2026-11-20: "when I click on that category I should be able to view the entire list and sort them by market cap and by industry". */
      var OUTW = { up: 'Up', flat: 'Flat', down_less: 'Down less', down_more: 'Down more' }, OUTN = { up: 3, flat: 2, down_less: 1, down_more: 0 };
      var fld = function (id) { return function (r) { return r[id]; }; }, tn = function (f) { return function (r) { var q = f(r); return q > 0 ? 'up' : q < 0 ? 'down' : ''; }; };
      var NC = [{ id: 't', t: 'Ticker', type: 's', find: 'start', get: fld('t') }, { id: 'name', t: 'Name', type: 's', get: function (r) { return r.name || ''; } },
        isEtf ? { id: 'sector', t: 'Class', type: 's', get: cls } : { id: 'sector', t: 'Sector', type: 's', get: function (r) { return r.sector || 'Unclassified'; } },
        isEtf ? { id: 'adv', t: 'Traded a day', type: 'n', tip: 'dollars traded on an average day', get: fld('adv'), fmt: function (r) { return money(r.adv); } }
          : { id: 'industry', t: 'Industry', type: 's', get: function (r) { return r.industry || ''; } },
        { id: 'mcap', t: isEtf ? 'Assets' : 'Mkt value', type: 'n', tip: isEtf ? 'the fund\'s assets' : 'the company\'s market value', get: fld('mcap'), fmt: function (r) { return money(r.mcap); } },
        { id: 'px', t: 'Last', type: 'n', get: fld('px'), fmt: function (r) { return num(r.px, 2); } },
        { id: 'ret', t: 'Move', type: 'n', tip: 'the last price over the close before', get: ret, fmt: function (r) { return mv(r.ret); }, tone: tn(ret) },
        { id: 'x', t: 'vs SPY', type: 'n', tip: 'the move less SPY\'s, in points', get: x, fmt: function (r) { return fin(r.x) ? sign(+r.x.toFixed(2) || 0, true) + Math.abs(r.x).toFixed(2) : D; }, tone: tn(x) },
        { id: 'out', t: 'Against SPY', type: 'n', tip: 'up, flat, down less than SPY, or down more', get: function (r) { return OUTN[r.out]; }, fmt: function (r) { return OUTW[r.out] || D; }, csv: function (r) { return OUTW[r.out] || ''; } },
        { id: 'rank', t: 'Rank in', type: 'n', tip: 'the rank carried into the session, 0 to 100', get: fld('rank'), fmt: function (r) { return num(r.rank, 0); } },
        { id: 'dump', t: 'Dump days', type: 'n', tip: 'dump days of the quarter it held up on, before this session', get: function (r) { return r.rec_n ? r.rec_h / r.rec_n + r.rec_n / 1e4 : null; },
          fmt: function (r) { return r.rec_n ? r.rec_h + '/' + r.rec_n : D; }, csv: function (r) { return r.rec_n ? r.rec_h + ' of ' + r.rec_n : ''; } },
        { id: 'fo', t: 'From open', type: 'n', tip: 'the last price over the day\'s open', get: fld('fo'), fmt: function (r) { return mv(r.fo); }, tone: tn(fld('fo')) },
        { id: 'pos', t: 'Day range', type: 'n', tip: 'where the last price sits in the day\'s range: 0% the low, 100% the high', get: fld('pos'), fmt: function (r) { return fin(r.pos) ? num(r.pos, 0) + '%' : D; } },
        { id: 'vwx', t: 'vs VWAP', type: 'n', tip: 'the last price against the day\'s volume-weighted average price', get: fld('vwx'), fmt: function (r) { return mv(r.vwx); }, tone: tn(fld('vwx')) },
        { id: 'rv', t: 'Vol pace', type: 'n', tip: 'volume against its own average, over the market\'s pace at this hour', get: fld('rv'), fmt: function (r) { return times(r.rv); } }];
      var fileOf = function (key) { return 'strength-' + key + '-' + k + '-' + day(v.session) + (live ? '-live' : ''); };
      var all = function (key, list, id, d) {
        return { cols: NC, rows: list, sort: { k: id, d: d }, units: [S.unit, S.unit + 's'], href: function (r) { return '/chart.html?s=' + encodeURIComponent(r.t); },
          phone: ['t', 'ret', 'x', 'mcap', 'industry', 'adv', 'sector', 'out', 'rank', 'dump', 'name'],
          find: isEtf ? 'Find a ticker, name or class' : 'Find a ticker, name, sector or industry', file: fileOf(key) };
      };
      var GC = function (label, sectorToo) {
        return [{ id: 'name', t: label, type: 's', get: fld('name') }].concat(sectorToo ? [{ id: 'sector', t: 'Sector', type: 's', get: fld('sector') }] : []).concat([
          { id: 'n', t: 'Counted', type: 'n', get: fld('n'), fmt: function (q) { return num(q.n); } },
          { id: 'held', t: live ? 'Holding up' : 'Held up', type: 'n', tip: 'share up, flat, or down less than SPY', get: fld('held_pct'), fmt: function (q) { return p0(q.held_pct); } },
          { id: 'up', t: 'Up', type: 'n', tip: 'share up more than the flat band', get: fld('up_pct'), fmt: function (q) { return p0(q.up_pct); } },
          { id: 'beat', t: 'Ahead of SPY', type: 'n', tip: 'share whose move is above SPY\'s', get: fld('beat_pct'), fmt: function (q) { return p0(q.beat_pct); } },
          { id: 'more', t: 'Down more', type: 'n', tip: 'share down at least as much as SPY', get: function (q) { return share(q.down_more, q.n); }, fmt: function (q) { return p0(share(q.down_more, q.n)); } },
          { id: 'med', t: 'Median move', type: 'n', get: fld('median_ret'), fmt: function (q) { return mv(q.median_ret); }, tone: tn(fld('median_ret')) },
          { id: 'mcap', t: 'Mkt value', type: 'n', get: fld('mcap'), fmt: function (q) { return money(q.mcap); } }]);
      };
      var gid = key === 'held_pct' ? 'held' : 'beat', scopeOf = isEtf ? 'b1' : k;
      var grpFull = function (keyName, list, label, sectorToo, d, units, field) {
        return { cols: GC(label, sectorToo), rows: list, sort: { k: gid, d: d }, units: units, phone: ['name', 'held', 'n', 'med', 'mcap', 'up', 'beat', 'more', 'sector'], find: 'Find ' + (/^[aeiou]/.test(units[0]) ? 'an ' : 'a ') + units[0], file: fileOf(keyName), pickTip: 'Show these names in the table below',
          pick: function (q) { var o = { desk: 'strength', kind: isEtf ? 'etf' : 'stock', scope: scopeOf }; o[field] = q.name; return o; } };
      };
      var flat = top(named.filter(function (r) { return r.out === 'flat'; }), x);
      var indUnits = ['industry', 'industries'], indsUp = inds.slice().reverse();
      var lists = [
        { key: 'all', card: false, short: 'ALL', title: 'All ' + S.word, hint: 'every counted ' + S.unit + ' in this scope', n: rows.length, rows: [], full: all('all', top(rows, x), 'x', -1) },
        { key: 'up', short: 'UP', full: all('up', up, 'ret', -1), title: red ? (live ? 'Up while SPY falls' : 'Up while SPY fell') : 'Up the most', hint: 'by the move', n: up.length, rows: up.map(row), none: 'no ' + S.unit + (live ? ' is up' : ' was up'),
          note: unlisted ? plural(unlisted, 'fund') + ' that hold' + (unlisted === 1 ? 's' : '') + ' volatility itself (VIX futures, volatility-premium funds) ' + (unlisted === 1 ? 'is' : 'are') + ' counted in every figure above and left off these lists: how such a fund moves against SPY is its design, not strength.' : '' },
        { key: 'flat', card: false, short: 'FLAT', title: 'Flat', hint: 'within ' + R.flat_pct + '% either way', n: flat.length, rows: [], none: 'none', full: all('flat', flat, 'x', -1) },
        red ? { key: 'down-less', short: 'DOWN LESS', full: all('down-less', less, 'x', -1), title: 'Down less than SPY', hint: 'closest to flat first', n: less.length, rows: less.map(row), none: 'none' }
          : { key: 'down-less', card: false, short: 'DOWN LESS', full: all('down-less', less, 'x', -1), title: 'Down less than SPY', hint: 'closest to flat first', n: less.length, rows: [], none: 'none' },
        red ? (isEtf ? null : { key: 'down-more', card: false, short: 'DOWN MORE', full: all('down-more', more, 'ret', 1), title: 'Down more than SPY', hint: 'the largest fall first', n: more.length, rows: [], none: 'none' })
          : { key: 'down-most', short: 'DOWN MORE', full: all('down-most', more, 'ret', 1), title: 'Down the most', hint: live ? 'while SPY is not falling' : 'while SPY did not fall', n: more.length, rows: more.map(row), none: 'no ' + S.unit + ' is down more than SPY' },
        { key: 'strong-holding', short: 'STRONG, ' + (red ? (live ? 'HOLDING' : 'HELD') : 'AHEAD'), full: all('strong-holding', sHold, 'x', -1), title: 'Strong coming in, ' + (red ? keyWord : 'still ahead'), hint: 'rank 80 or higher', n: sHold.length, rows: sHold.map(row), none: 'none' },
        { key: 'strong-breaking', short: 'STRONG, ' + (live ? 'BREAKING' : 'BROKE'), full: all('strong-breaking', sBreak, 'x', 1), title: 'Strong coming in, ' + (live ? 'breaking' : 'broke'), hint: 'rank 80 or higher, down more than SPY', n: sBreak.length, rows: sBreak.map(row), none: 'no strong name ' + (live ? 'is' : 'was') + ' down more than SPY' },
        { key: 'weak-bid', short: 'WEAK, BID', full: all('weak-bid', wBid, 'ret', -1), title: 'Weak coming in, bid', hint: 'rank 20 or lower, up', n: wBid.length, rows: wBid.map(row), none: 'none' },
        { key: 'repeat', short: 'HELD EVERY DUMP DAY', full: all('repeat', repeat, 'x', -1), title: 'Held every dump day, ' + (live ? 'holding again' : 'held again'), hint: 'of the quarter before this session', n: repeat.length, rows: repeat.map(row), none: 'none',
          note: 'A dump day is a session SPY fell ' + Math.abs(R.dump_pct) + '% or more. These held up on every one of the quarter (three or more) and ' + (live ? 'are holding up in this session too.' : 'held up in this session too.') },
        { key: 'groups', short: isEtf ? 'ASSET CLASSES' : 'SECTORS', full: grpFull(isEtf ? 'classes' : 'sectors', sectors, isEtf ? 'Asset class' : 'Sector', false, -1, isEtf ? ['asset class', 'asset classes'] : ['sector', 'sectors'], 'sector'),
          title: isEtf ? 'Asset classes' : 'Sectors', hint: 'share ' + shareWord, n: sectors.length, rows: sectors.map(grow), none: 'none' }
      ].filter(function (l) { return l; });
      if (!isEtf) {
        lists.push({ key: 'industries-holding', short: 'INDUSTRIES, BEST FIRST', full: grpFull('industries', inds, 'Industry', true, -1, indUnits, 'industry'),
          title: 'Industries ' + (red ? shareWord : 'ahead'), hint: (R.industry_min || 5) + ' or more ' + S.unit + 's', n: inds.length, rows: inds.slice(0, 15).map(grow), none: 'no industry has enough names' });
        lists.push({ key: 'industries-breaking', short: 'INDUSTRIES, WORST FIRST', full: grpFull('industries-worst-first', indsUp, 'Industry', true, 1, indUnits, 'industry'),
          title: live ? 'Industries breaking' : 'Industries that broke', hint: 'lowest share ' + shareWord, n: inds.length, rows: inds.slice(-15).reverse().map(grow), none: 'no industry has enough names' });
      } else if (red) lists.push({ key: 'down-most-etf', short: 'DOWN MORE', full: all('down-most-etf', more, 'ret', 1), title: 'Down the most', hint: 'by the move', n: more.length, rows: more.map(row), none: 'none' });
      return {
        eyebrow: 'Strength tearsheet' + DOT + when + DOT + num(counted.length) + ' stocks and ETFs counted',
        title: title,
        sub: 'Up = more than +' + R.flat_pct + '%' + DOT + 'flat = within ' + R.flat_pct + '% either way' + DOT + 'down less = down, but less than SPY' + DOT + 'down more = down at least as much' + DOT + 'holding up = the first three' + DOT + 'ranks and records as of ' + day(v.rank_as_of),
        controls: ['b1', 'sp', 'all', 'etf'].map(function (id) { return { id: id, label: SC[id].label, on: id === k }; }),
        tiles: [{ k: 'S&P 500 fund (SPY)', v: mv(spy.ret), s: 'from the open ' + mv(spy.fo) + (fin(spy.pos) ? DOT + num(spy.pos, 0) + '% of the day\'s range' : '') + (fin(spy.vwx) ? DOT + (spy.vwx >= 0 ? 'above' : 'below') + ' the day\'s VWAP' : '') },
          { k: red ? (live ? 'Holding up' : 'Held up') : 'Ahead of SPY', v: p0(g[key]), s: num(g.n) + ' ' + S.unit + 's' + DOT + 'up ' + num(g.up) + DOT + 'flat ' + num(g.flat) + DOT + 'down less ' + num(g.down_less) },
          { k: 'Down more than SPY', v: p0(share(g.down_more, g.n)), s: num(g.down_more) + ' of ' + num(g.n) + DOT + 'median move ' + mv(g.median_ret) },
          { k: live ? 'Market value holding up' : 'Market value that held up', v: p0(share(heldCap, total)), s: money(heldCap) + ' of ' + money(total) },
          { k: 'Strongest tenth coming in', v: gTop ? p0(gTop[key]) : D, s: gTop ? keyWord + DOT + 'median ' + mv(gTop.median_ret) + DOT + 'weakest tenth ' + p0(gBot[key]) + ', median ' + mv(gBot.median_ret) : 'too few ranked names have printed' },
          { k: 'Held every dump day before', v: gEvery.n ? p0(gEvery[key]) : D, s: gEvery.n ? num(gEvery.n) + ' ' + S.unit + 's' + DOT + keyWord + (gFew.n ? DOT + 'held fewer than a third: ' + p0(gFew[key]) + ' of ' + num(gFew.n) : '') : 'no record of three dump days' }],
        cards: [card(good, 'Largest ' + (isEtf ? 'fund' : 'company') + (red ? (live ? ' up while SPY falls' : ' up while SPY fell') : ' ahead of SPY'), 'No ' + S.unit + ' in this scope ' + (live ? 'is' : 'was') + ' up.'),
          card(bad, sBreak.length ? (live ? 'Largest strong name breaking' : 'Largest strong name that broke') : 'Largest name down more than SPY', 'No ' + S.unit + ' in this scope ' + (live ? 'is' : 'was') + ' down more than SPY.')],
        notes: [['Where:', sectors.length > 1 ? 'by ' + (isEtf ? 'asset class' : 'sector') + ', the share ' + shareWord + ' runs from ' + p0(sectors[0][key]) + ' in ' + sectors[0].name + ' (' + plural(sectors[0].n, S.unit) + ') to ' + p0(sectors[sectors.length - 1][key]) + ' in ' + sectors[sectors.length - 1].name + ' (' + plural(sectors[sectors.length - 1].n, S.unit) + ').'
            + ' By market value ' + p0(share(heldCap, total)) + ' of the scope ' + (live ? 'is holding up' : 'held up') + ', against ' + p0(g.held_pct) + ' by count: the larger names ' + (live ? 'are doing' : 'did') + (share(heldCap, total) < g.held_pct ? ' worse than the typical one.' : ' no worse than the typical one.') : 'one group in this scope.'],
          ['Carried in:', (gTop ? 'the strongest tenth by rank: ' + p0(gTop[key]) + ' ' + keyWord + ', median ' + mv(gTop.median_ret) + '; the weakest tenth: ' + p0(gBot[key]) + ', median ' + mv(gBot.median_ret) + '. ' : '')
            + (gEvery.n ? num(gEvery.n) + ' ' + S.unit + 's had held up on every dump day of the quarter: ' + p0(gEvery[key]) + ' ' + nowWord + (gFew.n ? ', against ' + p0(gFew[key]) + ' of the ' + num(gFew.n) + ' that had held on fewer than a third' : '') + '.' : '')
            + ' Ranks and records are as of ' + day(v.rank_as_of) + ', before this session.'],
          ['Caveat:', (live ? 'these are forming bars, ' + (v.delay_minutes || 15) + ' minutes behind the market, and the session is not over. ' : 'this is the last finished session. ')
            + num(aside) + ' names are measured and set aside from every count (pinned to a deal price, leveraged or inverse funds, an unexplained jump, thin trading)'
            + ((v.unconfirmed || []).length ? '; ' + num(v.unconfirmed.length) + ' moves the size of a split with none on record are shown as unconfirmed and not counted' : '') + '. Holding up on a down day is a fact about one session, not a forecast.']],
        ring: { title: 'Market value by outcome', hint: 'against SPY', open: 'all', n: rows.length, center: money(total), subs: [num(rows.length) + ' ' + S.unit.toUpperCase() + 'S', (red ? (live ? 'HOLDING UP ' : 'HELD UP ') : 'UP OR FLAT ') + p0(red ? share(heldCap, total) : share(sum(rows.filter(function (r) { return r.out === 'up' || r.out === 'flat'; }), cap), total))],
          whole: 'the scope\'s market value', aria: 'Market value of ' + S.word + ' by how each is doing against SPY', slices: slices,
          caption: 'Each counted ' + S.unit + ' once, at its market value' + (isEtf ? ' (a fund\'s assets)' : '') + ', by how it ' + (live ? 'is doing against SPY today' : 'did against SPY on ' + day(v.session)) + '. The count beside each slice is its share of the names, which can differ widely from its share of the value.' },
        /* the cards of the sheet; `more` are lists that open in full without a card (a ring slice or a category button opens them); `order` is
           the order of the category buttons */
        lists: lists.filter(function (l) { return l.card !== false; }), more: lists.filter(function (l) { return l.card === false; }),
        order: ['all', 'up', 'flat', 'down-less', 'down-more', 'down-most', 'down-most-etf', 'strong-holding', 'strong-breaking', 'weak-bid', 'repeat', 'groups', 'industries-holding', 'industries-breaking'],
        foot: 'Source: justhodl-strength ' + ((v.table || {}).version || '') + ' (data/strength-wide.json, published ' + String((v.table || {}).generated_at || '').slice(0, 16).replace('T', ' ') + ' UTC, closes of ' + day(v.table_as_of) + ')'
          + (live ? ' under justhodl-polygon-today\'s forming day bars (data/warm/polygon-full/today.json.gz, every five minutes in the session, ' + (v.delay_minutes || 15) + ' minutes delayed)' : '')
          + '. The universe is every listed common stock and plain ETF trading $250k a day or more. The move is the last price over the close before.'
      };
    }
  };

  /* every model leaves through finish(): each of its lists opens in full */
  Object.keys(WY).forEach(function (tab) { var f = WY[tab]; WY[tab] = function (D_, st) { return finish(f(D_, st)); }; });
  Object.keys(ADAPTERS).forEach(function (name) { var build = ADAPTERS[name].build; ADAPTERS[name].build = function (docs, st) { return finish(build.call(ADAPTERS[name], docs, st)); }; });

  /* -- boot -- */
  function get(url) { return fetch(url + (url.indexOf('?') < 0 ? '?' : '&') + 'v=' + Math.floor(Date.now() / 300000), { cache: 'no-store' }).then(function (r) { if (!r.ok) throw new Error(url + ' HTTP ' + r.status); return r.json(); }); }

  function mount(el) {
    var name = el.getAttribute('data-jts'), ad = ADAPTERS[name];
    if (!ad || el.__jts) return;
    el.__jts = true;
    style();
    el.classList.add('jts');
    if (!el.id) el.id = 'jh-tearsheet';
    el.setAttribute('aria-label', 'Tearsheet');
    el.setAttribute('data-jh-nosort', '');
    /* Kept out of the site's section numbering (jh-sections.js): as a numbered block it changed which container the
       numbering treats as the page's axis and renumbered existing sections on some desks (firm-book). Addresses that
       already exist must not move, so the sheet is marked as chrome; its own anchor is #jh-tearsheet. */
    el.setAttribute('data-jh-chrome', '');
    el.innerHTML = '<div class="jts-in"><div class="jts-wait">Reading the tearsheet\u2026</div></div>';
    var state = {}, docs = null;
    function paint() {
      try {
        var model = ad.build(docs, state);
        if (PRIO_DESKS[name]) model = prioModel(model, prioMode());
        el.hidden = !!(model && model.hide);                 /* a tab with no sheet: the mount steps aside */
        if (el.hidden) { el.dataset.state = 'hidden'; return; }
        render(el, model);
      }
      catch (e) { el.hidden = false; el.dataset.state = 'error'; el.innerHTML = '<div class="jts-in"><div class="jts-wait">The tearsheet could not be built from this feed (' + esc(e && e.message || e) + '). The rest of the page is unaffected.</div></div>'; }
    }
    /* -- a list opened in full: which one, how it is ranked, what is being looked for -- */
    function openList() { var o = el.__open, m = el.__model; return o && m ? fullLists(m).filter(function (l) { return l.key === o.key && l.full; })[0] : null; }
    /* an opened list starts where the reader is looking: its own top comes into view when it is above the screen or far down it
       (a card clicked deep in a long sheet); back on the sheet, the sheet's top comes back when it is above the screen */
    function top() {
      try {
        var f = el.querySelector('.jts-full'), y = (f || el).getBoundingClientRect().top;
        if (f ? (y < 0 || y > window.innerHeight * 0.55) : y < 0) (f || el).scrollIntoView({ block: 'start' });
      } catch (e) { /* no layout: nothing to bring into view */ }
    }
    function body() { var l = openList(), host = el.querySelector('[data-jts-fbody]'); if (l && host) host.innerHTML = fullBody(l, el.__open, el.clientWidth < 520); else paint(); }
    function show(key) { el.__open = key ? { key: key, sort: null, q: '', all: false } : null; paint(); top(); }
    function rank(id) {
      var l = openList();
      if (!l) return;
      var c = l.full.cols.filter(function (x) { return x.id === id; })[0], s = fullSortOf(l.full, el.__open);
      if (!c) return;
      el.__open.sort = s && s.k === id ? { k: id, d: -s.d } : { k: id, d: c.type === 's' ? 1 : -1 };
      body();
    }
    /* inside the chart's workspace a ticker loads in the chart behind the desk, as the desk's own tables do */
    function inChart(a, e) {
      try {
        var s = new URL(a.getAttribute('href'), location.href).searchParams.get('s');
        if (s && window.top !== window && typeof window.top.jhOpenSymbol === 'function') {
          e.preventDefault();
          window.top.jhOpenSymbol(s);
          var x = window.top.document.getElementById('wsx');
          if (x) x.click();
        }
      } catch (err) { /* another origin: the link's own target=_top navigation stands */ }
    }
    function press(e) {
      var o = e.target.closest('[data-jts-open]'), h = e.target.closest('[data-jts-sort]'), p = e.target.closest('[data-jts-pick]'), a = e.target.closest('.jts-ft a[href]');
      if (a) { if (e.type === 'click') inChart(a, e); return false; }
      if (p) {
        var l = openList();
        if (l && l.full.pick) el.dispatchEvent(new CustomEvent('jts:pick', { bubbles: true, detail: l.full.pick(fullRows(l.full, el.__open)[+p.getAttribute('data-jts-pick')]) }));
        return true;
      }
      if (o) { show(o.getAttribute('data-jts-open')); return true; }
      if (h) { rank(h.getAttribute('data-jts-sort')); return true; }
      return false;
    }
    el.addEventListener('click', function (e) {
      var c = e.target.closest('[data-jts-ctl]'), a = e.target.closest('[data-jts-act]'), ord = e.target.closest('[data-jts-order]');
      if (ord) { prioSet(ord.getAttribute('data-jts-order')); if (el.__open) el.__open.sort = null; paint(); return; }
      if (c) { state.fund = c.getAttribute('data-jts-ctl'); paint(); return; }
      if (!a) { press(e); return; }
      var act = a.getAttribute('data-jts-act');
      if (act === 'more') { el.__long = !el.__long; paint(); }
      if (act === 'close') show(null);
      if (act === 'all' && el.__open) { el.__open.all = true; body(); }
      if (act === 'csv') {
        var l = openList();
        if (!l) return;
        var link = document.createElement('a');
        link.href = URL.createObjectURL(new Blob([fullCsv(l.full, fullRows(l.full, el.__open))], { type: 'text/csv' }));
        link.download = (l.full.file || name + '-' + l.key) + '.csv';
        document.body.appendChild(link); link.click(); link.remove();
        setTimeout(function () { URL.revokeObjectURL(link.href); }, 4000);
      }
      if (act === 'print') {
        document.documentElement.classList.add('jts-print');
        var done = function () { document.documentElement.classList.remove('jts-print'); window.removeEventListener('afterprint', done); };
        window.addEventListener('afterprint', done);
        window.print();
      }
      if (act === 'link') {
        var url = location.origin + location.pathname + '#' + el.id, ok = function () { var t = a.textContent; a.textContent = 'COPIED'; setTimeout(function () { a.textContent = t; }, 1400); };
        try { navigator.clipboard.writeText(url).then(ok, function () { window.prompt('Copy this link', url); }); } catch (err) { window.prompt('Copy this link', url); }
      }
    });
    el.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && el.__open) { show(null); return; }
      if ((e.key === 'Enter' || e.key === ' ') && e.target.matches && e.target.matches('[data-jts-open]:not(button),[data-jts-sort]') && press(e)) e.preventDefault();
    });
    var typing = null;
    el.addEventListener('input', function (e) {
      if (!e.target.hasAttribute || !e.target.hasAttribute('data-jts-find') || !el.__open) return;
      el.__open.q = e.target.value; el.__open.all = false;
      clearTimeout(typing);
      typing = setTimeout(body, 120);
    });
    /* the leaders landing, or the order changed from another table on the page: the lists are arranged again */
    if (PRIO_DESKS[name]) ['jh:leaders-index', 'jh:stock-order'].forEach(function (evt) { window.addEventListener(evt, function () { if (docs) paint(); }); });
    /* A desk with tabs tells its sheet which one is open: el.dispatchEvent(new CustomEvent('jts:set', { detail: { tab: 'sc' } })) */
    el.addEventListener('jts:set', function (e) {
      var patch = (e && e.detail) || {};
      Object.keys(patch).forEach(function (key) { state[key] = patch[key]; });
      el.__long = false;
      if (docs) paint();
    });
    function read() {
      /* soft: a sheet that stands on several feeds still builds the tabs whose feed arrived */
      Promise.all(ad.feeds.map(function (u) { return ad.soft ? get(u).catch(function () { return null; }) : get(u); })).then(function (list) { docs = list; paint(); }).catch(function (e) {
        if (docs) return;
        el.dataset.state = 'error';
        el.innerHTML = '<div class="jts-in"><div class="jts-wait">The tearsheet\u2019s feed is not available right now (' + esc(e && e.message || e) + '). The rest of the page is unaffected.</div></div>';
      });
    }
    if (!ad.share) { read(); return; }
    /* The desk on this page has already read the same feed: take its copy (window[global], then every later hand-over by
       event) instead of downloading it a second time, and read it here only when no copy arrives. */
    var use = function (doc) { if (doc && typeof doc === 'object') { docs = [doc]; paint(); } };
    window.addEventListener(ad.share.event, function (e) { use(e && e.detail); });
    if (window[ad.share.global]) use(window[ad.share.global]);
    else setTimeout(function () { if (!docs) read(); }, ad.share.ms || 8000);
  }

  function boot() { Array.prototype.forEach.call(document.querySelectorAll('[data-jts]'), mount); }
  var api = { version: '1.8.0', priority: { order: prioOrder, model: prioModel, mode: prioMode, set: prioSet, leaders: prioLeaders, index: prioFromDoc, ticker: prioTicker, cap: prioCap, isLeader: prioIsLeader }, render: render, ring: ring, adapters: ADAPTERS, mount: mount, money: money, pct: pct, full: { rows: fullRows, csv: fullCsv, body: fullBody, html: fullHtml, page: FULL_PAGE, finish: finish, figure: figure } };
  if (typeof module === 'object' && module.exports) module.exports = api;   /* node: the adapters and the ring are pure, tests/tearsheet-kit.test.js */
  if (typeof window === 'undefined' || typeof document === 'undefined') return;
  window.JHTearsheet = api;
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
})();
