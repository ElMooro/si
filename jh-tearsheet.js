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
      var t = steps[s.tone] ? s.tone : 'pos', list = steps[t], op = s.rest && t !== 'oth' ? 0.3 : list[Math.min(rank[t]++, list.length - 1)];
      var share = s.value / total * 100, sweep = Math.min(s.value / total * Math.PI * 2, Math.PI * 2 - 0.001), a0 = ang, a1 = ang + sweep, large = sweep > Math.PI ? 1 : 0;
      ang = a1;
      var o0 = pt(R, a0), o1 = pt(R, a1), i1 = pt(RI, a1), i0 = pt(RI, a0), mid = pt((R + RI) / 2, (a0 + a1) / 2);
      var shareTxt = (t === 'up' ? '+' : t === 'down' ? MINUS : '') + (share >= 9.95 ? share.toFixed(0) : share.toFixed(1)) + '%';
      arcs.push('<path class="jts-arc ' + t + '" d="M' + o0[0] + ' ' + o0[1] + 'A' + R + ' ' + R + ' 0 ' + large + ' 1 ' + o1[0] + ' ' + o1[1] + 'L' + i1[0] + ' ' + i1[1] + 'A' + RI + ' ' + RI + ' 0 ' + large + ' 0 ' + i0[0] + ' ' + i0[1] + 'Z"'
        + (t === 'down' ? ' fill="url(#' + id + ')"' : '') + ' fill-opacity="' + op + '" data-share="' + share.toFixed(3) + '"><title>' + esc(s.tip || (s.name + ': ' + (s.big || '') + DOT + share.toFixed(1) + '% of the whole')) + '</title></path>');
      if (share >= 6.5) labels.push('<text class="jts-rl" x="' + mid[0] + '" y="' + mid[1] + '"><tspan x="' + mid[0] + '" dy="-1">' + esc(s.label || s.name) + '</tspan><tspan x="' + mid[0] + '" dy="12">' + shareTxt + '</tspan></text>');
      legend.push('<tr><td><i class="jts-sw ' + t + '" style="opacity:' + op + '"></i><b class="jts-nm">' + esc(s.name) + '</b></td><td class="n"><span class="jts-big ' + (t === 'up' ? 'up' : t === 'down' ? 'down' : '') + '">' + esc(s.big || '') + '</span><span class="jts-s">'
        + esc(s.small || (shareTxt + ' of ' + (r.whole || 'the whole'))) + '</span></td></tr>');
    });
    return '<div class="jts-col jts-ringcol" data-jts-list="ring" data-total="' + total + '"><div class="jts-ch"><span><b>' + esc(r.title) + '</b> <i>' + esc(r.hint || '') + '</i></span><span class="jts-n">' + esc(r.n == null ? '' : r.n) + '</span></div>'
      + '<div class="jts-ring"><svg viewBox="0 0 220 220" role="img" aria-label="' + esc(r.aria || r.title) + '"><defs><pattern id="' + id + '" patternUnits="userSpaceOnUse" width="6" height="6" patternTransform="rotate(45)"><rect class="jts-hbg" width="6" height="6"></rect>'
      + '<line class="jts-hln" x1="0" y1="0" x2="0" y2="6"></line></pattern></defs>' + arcs.join('') + labels.join('') + '<text class="jts-rc" x="110" y="106">' + esc(r.center || '') + '</text>'
      + (r.subs || []).slice(0, 2).map(function (t, i) { return '<text class="jts-rs" x="110" y="' + (121 + i * 13) + '">' + esc(t) + '</text>'; }).join('') + '</svg></div>'
      + '<table class="jts-t jts-leg"><tbody>' + legend.join('') + '</tbody></table>' + (r.caption ? '<p class="jts-rest">' + esc(r.caption) + '</p>' : '') + '</div>';
  }

  function render(el, m) {
    style();
    var limit = el.__long ? 15 : (el.clientWidth && el.clientWidth < 520 ? 3 : 6);   /* a phone stacks the lists in one column: three rows each, MORE ROWS for the rest */
    function col(l) {
      var rows = l.rows || [], shown = rows.slice(0, limit), rest = rows.slice(limit);
      return '<div class="jts-col ' + (l.tone || '') + '" data-jts-list="' + esc(l.key || '') + '"><div class="jts-ch"><span><b>' + esc(l.title) + '</b> <i>' + esc(l.hint || '') + '</i></span><span class="jts-n">' + (l.n == null ? rows.length : esc(l.n)) + '</span></div>'
        + (shown.length ? '<table class="jts-t"><tbody>' + shown.map(function (x) {
          return '<tr><td><b class="jts-nm">' + esc(x.name) + '</b><span class="jts-s">' + esc(x.sub || '') + '</span></td><td class="n"><span class="jts-big ' + (x.tone || '') + '">' + esc(x.big == null ? '' : x.big) + '</span><span class="jts-s">' + esc(x.small || '') + '</span></td></tr>';
        }).join('') + '</tbody></table>' : '<p class="jts-none">' + esc(l.none || 'none') + '</p>')
        + (rest.length ? '<p class="jts-rest">The other ' + rest.length + ': ' + rest.slice(0, 12).map(function (x) { return esc(String(x.name).split(DOT)[0]) + (x.big ? ' (' + esc(x.big) + ')' : ''); }).join(', ') + (rest.length > 12 ? ' and ' + (rest.length - 12) + ' more' : '') + '</p>' : '')
        + (l.note ? '<p class="jts-rest">' + esc(l.note) + '</p>' : '') + '</div>';
    }
    var long = (m.lists || []).some(function (l) { return (l.rows || []).length > Math.min(limit, 6); });
    el.classList.add('jts');
    el.innerHTML = '<div class="jts-in"><div class="jts-top"><div class="jts-eye">' + esc(m.eyebrow) + '</div><div class="jts-act">' + (long ? '<button type="button" class="jts-btn" data-jts-act="more" aria-pressed="' + (el.__long ? 'true' : 'false') + '">' + (el.__long ? 'FEWER ROWS' : 'MORE ROWS') + '</button>' : '')
      + '<button type="button" class="jts-btn" data-jts-act="link">COPY LINK</button><button type="button" class="jts-btn" data-jts-act="print">PRINT' + DOT + 'SAVE PDF</button></div></div>'
      + '<div class="jts-h" role="heading" aria-level="2">' + esc(m.title) + '</div><div class="jts-sub">' + esc(m.sub || '') + '</div>'
      + ((m.controls || []).length ? '<div class="jts-ctl">' + m.controls.map(function (c) { return '<button type="button" class="jts-btn" data-jts-ctl="' + esc(c.id) + '" aria-pressed="' + (c.on ? 'true' : 'false') + '">' + esc(c.label) + '</button>'; }).join('') + '</div>' : '')
      + '<div class="jts-score">' + (m.tiles || []).map(function (t) { return '<div class="jts-kpi"><div class="k">' + esc(t.k) + '</div><div class="v">' + t.v + '</div><div class="s">' + esc(t.s || '') + '</div></div>'; }).join('') + '</div>'
      + ((m.cards || []).length ? '<div class="jts-cards">' + m.cards.map(function (c) {
        return '<div class="jts-card ' + (c.tone || '') + '"><div class="big ' + (c.tone || '') + '">' + esc(c.big) + '</div><div><div class="e">' + esc(c.eyebrow) + '</div><div class="t">' + esc(c.title) + '</div><p>' + esc(c.text) + '</p></div></div>';
      }).join('') + '</div>' : '')
      + ((m.notes || []).length ? '<div class="jts-notes">' + m.notes.map(function (n) { return '<p><b>' + esc(n[0]) + '</b> ' + esc(n[1]) + '</p>'; }).join('') + '</div>' : '')
      + '<div class="jts-cols">' + (m.ring ? ring(m.ring) : '') + (m.lists || []).map(col).join('') + '</div><p class="jts-foot">' + esc(m.foot || '') + '</p></div>';
    el.dataset.state = 'ready';
  }

  /* -- adapters: one per desk, each reading that desk's own published feed -- */
  var ADAPTERS = {};

  ADAPTERS.ark = {
    feeds: ['/data/ark-holdings.json'],
    build: function (docs, st) {
      var d = docs[0], by_fund = d.holdings_by_fund || {}, funds = (d.funds_tracked || Object.keys(by_fund)).filter(function (f) { return (by_fund[f] || []).length; });
      var fund = st.fund && by_fund[st.fund] ? st.fund : 'ALL', diff = d.diff_vs_prev || {}, hi = diff.highlights || {};
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
      var adds = mine(hi.biggest_adds).map(function (x) { return { x: x, usd: (x.shares_added || 0) * px(x) }; }).sort(by(function (r) { return r.usd; }, true));
      var trims = mine(hi.biggest_trims).map(function (x) { return { x: x, usd: (x.shares_sold || 0) * px(x) }; }).sort(by(function (r) { return r.usd; }, true));
      var opened = mine(hi.new_positions), closed = mine(hi.closed_positions);
      var label = function (x) { return (x.ticker || '?') + DOT + (x.company || ''); };
      var nAll = d.n_positions_total || sum(funds, function (f) { return by_fund[f].length; });
      var unchanged = nAll - (diff.n_new_positions || 0) - (diff.n_position_adds || 0) - (diff.n_position_trims || 0);
      var top10 = sum(pos.slice(0, 10), function (p) { return p.w; }), multi = pos.filter(function (p) { return p.funds.length > 1; }).sort(by(function (p) { return p.funds.length; }, true))[0];
      var prev = day(d.prev_snapshot_at), now = day(d.generated_at), held = day((by_fund[funds[0]][0] || {}).date), listed = adds.length + trims.length + opened.length + closed.length, who = fund === 'ALL' ? 'ARK Invest, all ' + funds.length + ' funds' : fund + (d.fund_descriptions && d.fund_descriptions[fund] ? ' (' + String(d.fund_descriptions[fund]).replace(/\s*\(.*$/, '') + ')' : '');
      var top6 = pos.slice(0, 6), restV = total - sum(top6, function (p) { return p.value; });
      var card = function (r, kind) {
        if (!r) return { tone: kind, big: D, eyebrow: 'Most notable ' + (kind === 'up' ? 'buy' : 'sale'), title: 'None in the day\u2019s largest moves', text: 'No ' + (kind === 'up' ? 'addition' : 'cut') + ' is listed for ' + (fund === 'ALL' ? 'any fund' : fund) + ' against the ' + prev + ' snapshot.' };
        var x = r.x, sh = kind === 'up' ? x.shares_added : x.shares_sold;
        return { tone: kind, big: pct(x.pct_change, Math.abs(x.pct_change) >= 100 ? 0 : 1, true), eyebrow: 'Most notable ' + (kind === 'up' ? 'buy' : 'sale') + DOT + x.fund, title: label(x),
          text: x.fund + (kind === 'up' ? ' added ' : ' sold ') + num(sh) + ' shares, about ' + money(r.usd) + ' at the reported price; it now holds ' + num(x.current_shares) + ' shares worth ' + money(x.current_value) + ', ' + pct(x.weight, 2) + ' of the fund.' };
      };
      var moveRow = function (r, kind) { var x = r.x; return { name: label(x), sub: x.fund + DOT + (kind === 'up' ? '+' : MINUS) + num(kind === 'up' ? x.shares_added : x.shares_sold) + ' sh' + DOT + 'now ' + pct(x.weight, 2) + ' of the fund', big: (kind === 'up' ? '+' : MINUS) + money(r.usd), tone: kind, small: pct(x.pct_change, 1, true) + ' shares' }; };
      var plain = function (x, kind) { return { name: label(x), sub: (x.fund || '') + (x.weight != null ? DOT + pct(x.weight, 2) + ' of the fund' : ''), big: money(x.current_value != null ? x.current_value : x.market_value), tone: kind, small: num(x.current_shares != null ? x.current_shares : x.shares) + ' sh' }; };
      return {
        eyebrow: 'ARK tearsheet' + DOT + 'snapshot ' + now + ' vs ' + prev, title: who + ': ' + money(total) + ' across ' + pos.length + ' positions' + DOT + (fund === 'ALL' ? (diff.n_position_adds || 0) + ' added to, ' + (diff.n_position_trims || 0) + ' cut, ' + (diff.n_new_positions || 0) + ' opened, ' + (diff.n_closed_positions || 0) + ' closed since the last snapshot' : listed + ' of the day\u2019s largest moves are in this fund'),
        sub: 'Daily holdings as ARK publishes them' + (held ? ', dated ' + held : '') + DOT + 'positions for ' + (fund === 'ALL' ? 'all funds, one row per ticker' : fund) + DOT + 'changes counted across all ' + funds.length + ' funds',
        controls: [{ id: 'ALL', label: 'ALL FUNDS', on: fund === 'ALL' }].concat(funds.map(function (f) { return { id: f, label: f, on: fund === f }; })),
        tiles: [{ k: 'Reported value', v: money(total), s: fund === 'ALL' ? funds.length + ' funds' : fund }, { k: 'Line items', v: num(pos.length), s: fund === 'ALL' ? num(nAll) + ' fund-lines, ' + pos.length + ' tickers' : 'positions in the fund' },
          { k: 'In top 10', v: pct(top10, 0), s: 'of the reported value' }, { k: 'New / increased', v: '<span class="up">' + (diff.n_new_positions || 0) + '</span> / <span class="up">' + (diff.n_position_adds || 0) + '</span>', s: 'fund-lines, all funds' },
          { k: 'Reduced / closed', v: '<span class="down">' + (diff.n_position_trims || 0) + '</span> / <span class="down">' + (diff.n_closed_positions || 0) + '</span>', s: 'fund-lines, all funds' }, { k: 'Unchanged', v: num(Math.max(unchanged, 0)), s: 'share count within 1% of ' + prev }],
        cards: [card(trims[0], 'down'), card(adds[0], 'up')],
        notes: [['Largest holding:', pos[0] ? label(pos[0]) + ', ' + money(pos[0].value) + ' (' + pct(pos[0].w) + ')' + (pos[0].funds.length > 1 ? ' across ' + pos[0].funds.join(', ') : '') + '.' : D],
          ['Held by the most funds:', multi ? label(multi) + ' sits in ' + multi.funds.length + ' funds (' + multi.funds.join(', ') + ').' : 'no ticker is held by more than one fund in this view.'],
          ['Caveat:', 'a share count that rises on every line of a fund on the same day is usually new money entering the fund (creation units), not a fresh call on each name; the percentage beside each move is the change in shares.']],
        ring: { title: 'Current positions', hint: 'top 6 of ' + pos.length, n: pos.length, center: money(total), subs: ['REPORTED VALUE', 'TOP 10 = ' + pct(top10, 0)], whole: 'the value', aria: 'Positions by reported value: ' + top6.map(function (p) { return p.ticker + ' ' + pct(p.w, 0); }).join(', '),
          slices: top6.map(function (p) { return { name: label(p), label: p.ticker, value: p.value, tone: 'pos', big: pct(p.w), small: money(p.value) + (p.funds.length > 1 ? DOT + p.funds.length + ' funds' : '') }; })
            .concat(restV > 0 ? [{ name: (pos.length - 6) + ' other positions', label: 'other', value: restV, tone: 'oth', big: pct(restV / total * 100), small: money(restV) }] : []),
          caption: 'Slices are shares of the reported value' + (fund === 'ALL' ? ' of all funds together' : ' of ' + fund) + '.' },
        lists: [{ key: 'new', title: 'Initiations', hint: 'new lines', tone: 'up', n: diff.n_new_positions || 0, rows: opened.map(function (x) { return plain(x, 'up'); }), none: 'no position opened since ' + prev },
          { key: 'adds', title: 'Increases', hint: 'largest by dollars', tone: 'up', n: fund === 'ALL' ? (diff.n_position_adds || adds.length) : adds.length, rows: adds.map(function (r) { return moveRow(r, 'up'); }), none: 'none listed for ' + fund,
            note: fund === 'ALL' && (diff.n_position_adds || 0) > adds.length ? 'The feed lists the ' + adds.length + ' largest of ' + diff.n_position_adds + ' additions (largest by percent).' : '' },
          { key: 'trims', title: 'Reductions', hint: 'largest by dollars', tone: 'down', n: fund === 'ALL' ? (diff.n_position_trims || trims.length) : trims.length, rows: trims.map(function (r) { return moveRow(r, 'down'); }), none: 'none listed for ' + (fund === 'ALL' ? 'any fund' : fund) },
          { key: 'closed', title: 'Terminations', hint: 'lines gone', tone: 'down', n: diff.n_closed_positions || 0, rows: closed.map(function (x) { return plain(x, 'down'); }), none: 'no position closed since ' + prev }],
        foot: 'Source: ARK Invest daily fund holdings via arkfunds.io, snapshot ' + now + ' against ' + prev + '. Dollar size of a move = shares added or sold x the position\u2019s reported price that day. Percent changes are changes in share count; a line counts as changed when its shares move 1% or more. The feed publishes the 15 largest additions and cuts by percent. ARK ETFs are actively managed; a daily file shows holdings, not intent.'
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
        ring: { title: 'Supply by coin', hint: 'top 6 of ' + num(a.n_stablecoins), n: a.n_stablecoins, center: money(total), subs: ['OUTSTANDING', '30D ' + pct(a.delta_30d_pct, 1, true)], whole: 'supply', aria: 'Stablecoin supply by coin',
          slices: top6.map(function (c) { return { name: nm(c), label: c.symbol, value: c.circulating_usd, tone: 'pos', big: pct(w(c)), small: money(c.circulating_usd) + DOT + '30d ' + pct(c.delta_30d_pct, 1, true) }; })
            .concat(restV > 0 ? [{ name: ((a.n_stablecoins || coins.length) - 6) + ' other coins', label: 'other', value: restV, tone: 'oth', big: pct(restV / total * 100), small: money(restV) }] : []),
          caption: 'Slices are shares of all USD-pegged supply.' },
        lists: [{ key: 'mint', title: 'Minting', hint: 'largest 30-day growth', tone: 'up', rows: minters.map(row) }, { key: 'burn', title: 'Redeeming', hint: 'largest 30-day shrink', tone: 'down', rows: burners.map(row) },
          { key: 'coins', title: 'Largest coins', hint: 'supply and 30-day change', rows: coins.map(function (c) { return { name: nm(c), sub: pct(w(c)) + ' of supply' + DOT + '7d ' + money(c.delta_7d, true), big: money(c.circulating_usd), small: '30d ' + pct(c.delta_30d_pct, 1, true) }; }) },
          { key: 'chains', title: 'By chain', hint: 'where the supply sits', rows: chains.map(function (c) { return { name: c.chain, sub: '', big: money(c.circulating_usd), small: pct(total ? c.circulating_usd / total * 100 : 0) + ' of supply' }; }) }],
        foot: 'Source: DefiLlama stablecoin supply, as of ' + day(d.as_of) + '. Changes are supply now against supply 24 hours, 7 days and 30 days ago. Minting and redeeming lists are the feed\u2019s own largest movers over 30 days.'
      };
    }
  };

  ADAPTERS.firmbook = {
    feeds: ['/data/firm-book.json'],
    build: function (docs) {
      var d = docs[0], f = d.firm || {}, book = (d.equity_book || []).slice(), longs = book.filter(function (p) { return p.net_pct > 0; }).sort(by(function (p) { return p.net_pct; }, true)), shorts = book.filter(function (p) { return p.net_pct < 0; }).sort(by(function (p) { return p.net_pct; }));
      var g = function (p) { return Math.abs(p.gross_pct != null ? p.gross_pct : p.net_pct); }, lt = sum(longs, g), stt = sum(shorts, g), gross = lt + stt;
      var nm = function (p) { return p.symbol + DOT + (p.name || ''); };
      var side = function (list, t) { var top = list.slice(0, 3), rest = list.slice(3), out = top.map(function (p) { return { name: nm(p), label: p.symbol, value: g(p), tone: t, big: pct(p.net_pct, 2, true), small: (p.sector || '') }; });
        if (rest.length) out.push({ name: rest.length + ' other ' + (t === 'up' ? 'longs' : 'shorts'), label: 'other', value: sum(rest, g), tone: t, rest: true, big: pct((t === 'up' ? 1 : -1) * sum(rest, g), 2, true), small: 'of capital' }); return out; };
      var row = function (p) { return { name: nm(p), sub: (p.sector && p.sector !== 'Unknown' ? p.sector : 'sector not mapped') + DOT + (p.n_desks || 1) + (p.n_desks > 1 ? ' desks' : ' desk') + (p.price ? DOT + '$' + num(p.price, 2) : ''), big: pct(p.net_pct, 2, true), tone: tone(p.net_pct), small: 'of capital' }; };
      var desks = Object.keys(d.desk_contributions || {}).map(function (k) { return d.desk_contributions[k]; }).sort(by(function (x) { return x.gross_contributed_pct || 0; }, true));
      var sectors = (d.sector_exposure || []).slice().sort(by(function (s) { return s.gross_pct || 0; }, true));
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
        ring: { title: 'Gross exposure', hint: 'long solid, short hatched', n: book.length, center: pct(gross, 1), subs: ['OF CAPITAL, GROSS', 'LONG ' + pct(gross ? lt / gross * 100 : 0, 0) + DOT + 'SHORT ' + pct(gross ? stt / gross * 100 : 0, 0)], whole: 'gross', aria: 'Gross exposure by position, longs and shorts',
          slices: side(longs, 'up').concat(side(shorts, 'down')), caption: 'Solid = long, hatched = short. Each slice is a share of gross exposure (' + pct(gross, 1) + ' of capital).' },
        lists: [{ key: 'longs', title: 'Largest longs', hint: 'net % of capital', tone: 'up', n: longs.length, rows: longs.map(row) }, { key: 'shorts', title: 'Largest shorts', hint: 'net % of capital', tone: 'down', n: shorts.length, rows: shorts.map(row) },
          { key: 'sectors', title: 'By sector', hint: 'gross and net', rows: sectors.map(function (s) { return { name: s.sector === 'Unknown' ? 'Sector not mapped' : s.sector, sub: s.n + ' names' + DOT + 'net ' + pct(s.net_pct, 2, true), big: pct(s.gross_pct, 2), small: 'gross' }; }) },
          { key: 'desks', title: 'By desk', hint: 'capital and status', rows: desks.map(function (x) { return { name: x.name, sub: (x.status || '') + DOT + (x.n_positions || 0) + ' positions' + DOT + 'budget ' + pct(x.capital_pct, 1), big: pct(x.gross_contributed_pct, 1), small: 'gross in the book' }; }) }],
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
        ring: { title: 'Current positions', hint: 'share of the book', n: pos.length, center: money(nav), subs: ['NET ASSET VALUE', 'CASH ' + pct(d.cash_pct, 1)], whole: 'the book', aria: 'Paper book by position and cash',
          slices: pos.map(function (p) { return { name: p.ticker, value: p.weight_pct, tone: 'pos', big: pct(p.weight_pct, 1), small: num(p.shares, 1) + ' sh at $' + num(p.px, 2) }; }).concat(d.cash_pct > 0 ? [{ name: 'Cash', label: 'cash', value: d.cash_pct, tone: 'oth', big: pct(d.cash_pct, 1), small: 'uninvested' }] : []),
          caption: 'Slices are shares of net asset value; cash is the grey slice.' },
        lists: [{ key: 'buys', title: 'Bought', hint: 'last rebalance', tone: 'up', rows: buys.map(row), none: 'nothing bought on ' + (last || D) }, { key: 'sells', title: 'Sold', hint: 'last rebalance', tone: 'down', rows: sells.map(row), none: 'nothing sold on ' + (last || D) },
          { key: 'earlier', title: 'Earlier trades', hint: 'most recent first', rows: earlier.map(row), none: 'no earlier trades in the feed' }],
        foot: 'Source: justhodl-paper-book, ' + day(d.generated_at) + '. ' + (d.note ? String(d.note).slice(0, 260) : '')
      };
    }
  };

  ADAPTERS.sp500 = {
    feeds: ['/data/sp500.json'],
    build: function (docs) {
      var d = docs[0], ix = d.index || {}, F = d.member_fields || [], iChg = F.indexOf('px_chg_since_census_pct'), iSec = F.indexOf('sector'), iCap = F.indexOf('mcap'), iPe = F.indexOf('pe_ttm'), iMom = F.indexOf('mom_6m_pct');
      var mem = Object.keys(d.members || {}).map(function (t) { var r = d.members[t]; return { t: t, chg: r[iChg], sec: r[iSec], cap: r[iCap], pe: r[iPe], mom: r[iMom] }; }).filter(function (m) { return fin(m.cap); });
      var total = ix.total_mcap || sum(mem, function (m) { return m.cap; });
      mem.forEach(function (m) { m.w = total ? m.cap / total * 100 : 0; m.contrib = fin(m.chg) ? m.w * m.chg / 100 : null; });
      var heavy = mem.slice().sort(by(function (m) { return m.cap; }, true)), top10 = sum(heavy.slice(0, 10), function (m) { return m.w; });
      var moved = mem.filter(function (m) { return fin(m.contrib); }), lift = moved.slice().sort(by(function (m) { return m.contrib; }, true)), drag = moved.slice().sort(by(function (m) { return m.contrib; }));
      var upN = moved.filter(function (m) { return m.chg > 0; }).length, dnN = moved.filter(function (m) { return m.chg < 0; }).length, flatN = moved.length - upN - dnN;
      var secs = Object.keys(d.sectors || {}).map(function (k) { var s = d.sectors[k]; return { name: k, w: s.weight_pct, n: s.n, cap: s.cap_t, pe: s.pe_ttm, fwd: s.pe_fwd }; }).sort(by(function (s) { return s.w || 0; }, true));
      var SHORT = { 'Technology': 'Tech', 'Communication Services': 'Comm', 'Financial Services': 'Financials', 'Consumer Cyclical': 'Cyclical', 'Consumer Defensive': 'Defensive', 'Healthcare': 'Health', 'Industrials': 'Industrial', 'Basic Materials': 'Materials', 'Real Estate': 'Real est.' };
      var top6 = secs.slice(0, 6), restW = sum(secs.slice(6), function (s) { return s.w; }), v = d.valuation || {}, pe = (v.pe_ttm || {}).agg, ey = (v.earnings_yield_pct || {}).agg;
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
        ring: { title: 'Index by sector', hint: 'share of market value', n: secs.length, center: money(total), subs: ['MARKET VALUE', num(ix.members) + ' MEMBERS'], whole: 'the index', aria: 'S&P 500 market value by sector',
          slices: top6.map(function (s) { return { name: s.name, label: SHORT[s.name] || s.name, value: s.w, tone: 'pos', big: pct(s.w, 1), small: s.n + ' members' + (fin(s.pe) ? DOT + 'P/E ' + s.pe.toFixed(1) : '') }; })
            .concat(restW > 0 ? [{ name: (secs.length - 6) + ' other sectors', label: 'other', value: restW, tone: 'oth', big: pct(restW, 1), small: secs.slice(6).map(function (s) { return s.name; }).join(', ') }] : []), caption: 'Slices are shares of total market value.' },
        lists: [{ key: 'lift', title: 'Lifting the index', hint: 'largest contributions', tone: 'up', n: upN, rows: lift.filter(function (m) { return m.contrib > 0; }).map(row) }, { key: 'drag', title: 'Dragging the index', hint: 'largest contributions', tone: 'down', n: dnN, rows: drag.filter(function (m) { return m.contrib < 0; }).map(row) },
          { key: 'heavy', title: 'Heaviest members', hint: 'share of the index', n: mem.length, rows: heavy.map(function (m) { return { name: m.t, sub: (m.sec || '') + (fin(m.pe) ? DOT + 'P/E ' + m.pe.toFixed(1) : '') + (fin(m.mom) ? DOT + '6m ' + pct(m.mom, 0, true) : ''), big: pct(m.w, 2), small: money(m.cap) }; }) },
          { key: 'sectors', title: 'Sectors', hint: 'weight and valuation', rows: secs.map(function (s) { return { name: s.name, sub: s.n + ' members' + (fin(s.fwd) ? DOT + 'forward P/E ' + s.fwd.toFixed(1) : ''), big: pct(s.w, 1), small: fin(s.pe) ? 'P/E ' + s.pe.toFixed(1) : '' }; }) }],
        foot: 'Source: justhodl-sp500 ' + (d.engine_v || '') + ', prices ' + (ix.px_date || D) + ', fundamentals ' + (ix.fundamentals_asof || D) + '. A member\u2019s contribution = its weight x its price change since the census, in percentage points of the index.'
      };
    }
  };

  ADAPTERS.insiders = {
    feeds: ['/data/insider-clusters.json'],
    build: function (docs) {
      /* The population is the engine's scored set, one row per company (SEC CIK). The raw purchase list is not used for
         dollars: on 2026-11-17 it carried one record priced at shares / 1000 ($9.9B for one holder) and one company under
         three tickers, and the engine itself scores neither. */
      var d = docs[0], s = d.stats || {}, th = d.thresholds || {}, strongAt = th.score_strong_threshold || 70, seen = {}, raw = (d.clusters || []).slice().sort(by(function (c) { return c.score || 0; }, true));
      var scored = raw.filter(function (c) { var k = c.cik || c.ticker; if (seen[k]) return false; seen[k] = 1; return fin(c.total_value); });
      var KIND = { smart_money_dual: 'CEO and CFO together', ceo_conviction: 'CEO conviction', executive_cluster: 'executive cluster', exec_pair: 'executive pair', cluster_buy: 'cluster buy', lone_buy: 'single insider' };
      var kind = function (c) { return KIND[c.signal_type] || String(c.signal_type || '').replace(/_/g, ' '); };
      var total = sum(scored, function (c) { return c.total_value; }), dollars = scored.slice().sort(by(function (c) { return c.total_value; }, true)), top6 = dollars.slice(0, 6), restV = total - sum(top6, function (c) { return c.total_value; });
      var multi = scored.filter(function (c) { return c.n_insiders >= 2; }), strong = scored.filter(function (c) { return c.score >= strongAt; }), ceo = scored.filter(function (c) { return c.has_ceo; }), crowd = scored.slice().sort(by(function (c) { return c.n_insiders || 0; }, true));
      var types = {}, days = d.lookback_days || 30, share = function (c) { return total ? c.total_value / total * 100 : 0; };
      scored.forEach(function (c) { var t = types[c.signal_type] || (types[c.signal_type] = { name: kind(c), n: 0, usd: 0, top: c }); t.n++; t.usd += c.total_value; if (c.total_value > t.top.total_value) t.top = c; });
      var typeRows = Object.keys(types).map(function (k) { return types[k]; }).sort(by(function (t) { return t.usd; }, true)), cnt = function (k) { return types[k] ? types[k].n : 0; };
      var nm = function (c) { return c.ticker + (c.company ? DOT + c.company : ''); }, people = function (n) { return n + (n === 1 ? ' insider' : ' insiders'); };
      var crow = function (c) { return { name: nm(c), sub: people(c.n_insiders) + DOT + money(c.total_value) + ' at $' + num(c.avg_price, 2) + DOT + (c.highest_role || ''), big: fin(c.score) ? c.score.toFixed(0) : D, tone: c.score >= strongAt ? 'up' : '', small: kind(c) }; };
      var drow = function (c) { return { name: nm(c), sub: people(c.n_insiders) + DOT + c.n_transactions + (c.n_transactions === 1 ? ' buy' : ' buys') + DOT + day(c.first_buy) + (c.last_buy !== c.first_buy ? ' to ' + day(c.last_buy) : '') + DOT + (c.highest_role || ''), big: money(c.total_value), small: pct(share(c)) + ' of the dollars' + DOT + 'score ' + (fin(c.score) ? c.score.toFixed(0) : D) }; };
      var rich = dollars[0], best = scored[0], left = Math.max((s.n_unique_tickers || 0) - scored.length, 0);
      return {
        eyebrow: 'Insider buying tearsheet' + DOT + 'last ' + days + ' days to ' + day(d.generated_at), title: 'Insider buying: ' + money(total) + ' across ' + num(scored.length) + ' scored companies in ' + days + ' days' + DOT + multi.length + ' with two or more insiders, ' + strong.length + ' strong',
        sub: 'Open-market purchases only' + DOT + 'one row per company' + DOT + num(s.n_buy_transactions) + ' buy transactions read, ' + num(s.n_unique_tickers) + ' tickers with a buy',
        tiles: [{ k: 'Dollars bought', v: money(total), s: 'scored companies, ' + days + ' days' }, { k: 'Line items', v: num(scored.length), s: 'scored, of ' + num(s.n_unique_tickers) + ' tickers with a buy' },
          { k: 'In top 10', v: pct(total ? sum(dollars.slice(0, 10), function (c) { return c.total_value; }) / total * 100 : 0, 0), s: 'of the dollars' },
          { k: 'Clusters / strong', v: '<span class="up">' + multi.length + '</span> / <span class="up">' + strong.length + '</span>', s: 'two or more insiders / score ' + strongAt + ' or more' },
          { k: 'CEO and CFO together', v: num(cnt('smart_money_dual')), s: 'both top officers buying' }, { k: 'The CEO is buying', v: num(ceo.length), s: 'companies where the chief executive bought' }],
        cards: [rich ? { tone: 'up', big: money(rich.total_value), eyebrow: 'Largest dollars' + DOT + kind(rich), title: nm(rich), text: people(rich.n_insiders) + ' (' + (rich.highest_role || 'role not given') + ') bought ' + money(rich.total_value) + ', ' + num(rich.total_shares) + ' shares at $' + num(rich.avg_price, 2) + ' on average, between ' + day(rich.first_buy) + ' and ' + day(rich.last_buy) + ': ' + pct(share(rich)) + ' of the scored dollars.' } : null,
          best ? { tone: 'up', big: fin(best.score) ? best.score.toFixed(0) : D, eyebrow: 'Strongest score' + DOT + kind(best), title: nm(best), text: best.rationale || '' } : null].filter(Boolean),
        notes: [['Who is buying:', typeRows.slice().sort(by(function (t) { return t.n; }, true)).map(function (t) { return t.name + ' ' + t.n; }).join(', ') + ' (companies).'], ['Most insiders in one name:', crowd[0] ? nm(crowd[0]) + ', ' + people(crowd[0].n_insiders) + ', ' + money(crowd[0].total_value) + '.' : D],
          ['Caveat:', 'the largest dollar lines are usually 10% holders, who are funds and parent companies rather than management; the score, not the dollars, is what weighs who bought. Sales are not on this sheet.']],
        ring: { title: 'Insider dollars', hint: 'top 6 of ' + num(scored.length), n: scored.length, center: money(total), subs: ['BOUGHT IN ' + days + ' DAYS', num(scored.length) + ' COMPANIES'], whole: 'the dollars', aria: 'Insider buying dollars by company',
          slices: top6.map(function (c) { return { name: nm(c), label: c.ticker, value: c.total_value, tone: 'pos', big: money(c.total_value), small: pct(share(c)) + ' of the dollars' + DOT + people(c.n_insiders) }; })
            .concat(restV > 0 ? [{ name: (scored.length - top6.length) + ' other companies', label: 'other', value: restV, tone: 'oth', big: money(restV), small: pct(restV / total * 100) + ' of the dollars' }] : []), caption: 'Slices are shares of the purchase dollars in the scored set.' },
        lists: [{ key: 'clusters', title: 'Strongest scores', hint: 'by score', tone: 'up', n: scored.length, rows: scored.map(crow) }, { key: 'dollars', title: 'Largest dollars', hint: 'by company', n: scored.length, rows: dollars.map(drow) },
          { key: 'ceo', title: 'The CEO is buying', hint: 'by score', tone: 'up', n: ceo.length, rows: ceo.map(crow), none: 'no chief executive bought in the window' }, { key: 'crowd', title: 'Most insiders', hint: 'breadth of the buying', n: multi.length, rows: crowd.filter(function (c) { return c.n_insiders >= 2; }).map(function (c) { var r = crow(c); r.big = String(c.n_insiders); r.tone = ''; r.small = 'insiders' + DOT + 'score ' + (fin(c.score) ? c.score.toFixed(0) : D); return r; }), none: 'no company has two insiders buying' },
          { key: 'types', title: 'By kind of buying', hint: 'companies and dollars', rows: typeRows.map(function (t) { return { name: t.name, sub: t.n + (t.n === 1 ? ' company' : ' companies') + DOT + 'largest ' + t.top.ticker, big: money(t.usd), small: pct(total ? t.usd / total * 100 : 0) + ' of the dollars' }; }) }],
        foot: 'Source: SEC Form 4 open-market purchases (' + (d.source_used || 'insider bank') + '), ' + days + ' days to ' + day(d.generated_at) + '. Dollars and counts are the engine\u2019s scored set, one row per company by SEC CIK' + (left ? '; ' + num(left) + ' other tickers with a purchase record were not scored and are left out' : '') + '. Purchases under $' + num(th.min_buy_value_usd || 5000) + ' are left out. The score is the engine\u2019s own, 0 to 100.'
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
        ring: { title: 'Who holds them', hint: 'top 6 of ' + rows.length + ' named', n: rows.length, center: money(total * B), subs: ['FOREIGN HOLDINGS', when.toUpperCase()], whole: 'foreign holdings', aria: 'Foreign holdings of US Treasuries by jurisdiction',
          slices: top6.map(function (r) { return { name: r.country, label: r.country.split(',')[0].replace('United Kingdom', 'UK').slice(0, 10), value: r.current_b, tone: 'pos', big: pct(w(r)), small: money(r.current_b * B) + DOT + '3m ' + money(c3(r) * B, true) }; })
            .concat(otherNamed > 0 ? [{ name: (rows.length - 6) + ' other named jurisdictions', label: (rows.length - 6) + ' more', value: otherNamed, tone: 'oth', big: pct(otherNamed / total * 100), small: money(otherNamed * B) }] : [])
            .concat(rest > 0 ? [{ name: 'All other holders', label: 'unnamed', value: rest, tone: 'oth', big: pct(rest / total * 100), small: money(rest * B) + DOT + 'not named in the feed' }] : []),
          caption: 'Slices are shares of all foreign holdings of US Treasuries. Custody and residence, not ultimate ownership.' },
        lists: [{ key: 'up', title: 'Holdings up', hint: '3-month change', tone: 'up', rows: up.map(row3), none: 'no named jurisdiction rose' }, { key: 'down', title: 'Holdings down', hint: '3-month change', tone: 'down', rows: dn.map(row3), none: 'no named jurisdiction fell' },
          { key: 'holders', title: 'Largest holders', hint: 'share of the total', rows: rows.map(function (r) { return { name: r.country, sub: '3m ' + money(c3(r) * B, true) + (fin(r.yoy_change_b) ? DOT + '1y ' + money(r.yoy_change_b * B, true) : ''), big: money(r.current_b * B), small: pct(w(r)) + ' of the total' }; }) },
          { key: 'yoy', title: 'Over a year', hint: 'largest changes either way', rows: yoy.map(function (r) { return { name: r.country, sub: 'holds ' + money(r.current_b * B), big: money(r.yoy_change_b * B, true), tone: tone(r.yoy_change_b), small: pct(r.yoy_pct, 1, true) }; }) }],
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
        ring: { title: 'The month by security type', hint: 'bought solid, sold hatched', n: cls.length, center: money(gross * B), subs: ['GROSS, ' + when.toUpperCase(), 'IN ' + pct(gross ? inn / gross * 100 : 0, 0) + DOT + 'OUT ' + pct(gross ? out / gross * 100 : 0, 0)], whole: 'the gross', aria: 'Net foreign transactions by security type',
          slices: side(buys, 'up').concat(side(sells, 'down')), caption: 'Solid = net bought, hatched = net sold. Each slice is a share of the gross (' + money(gross * B) + '); the net is ' + money(net * B, true) + '.' },
        lists: [{ key: 'bought', title: 'Bought', hint: 'net purchases, ' + when, tone: 'up', rows: buys.map(row), none: 'nothing was net bought' }, { key: 'sold', title: 'Sold', hint: 'net sales, ' + when, tone: 'down', rows: sells.map(row), none: 'nothing was net sold' },
          { key: 'year', title: 'Twelve months by type', hint: 'to ' + when, rows: cls.slice().sort(by(function (c) { return c.y; }, true)).map(function (c) { return { name: c.name, sub: when + ' ' + money(c.m * B, true) + DOT + c.id, big: money(c.y * B, true), tone: tone(c.y), small: '12 months' }; }) },
          { key: 'history', title: 'Twelve-month total', hint: 'each print, newest first', rows: hist.map(function (x, i) { var p = hist[i + 1]; return { name: month(x.asof), sub: p ? money((x.rolling_12mo_b - p.rolling_12mo_b) * B, true) + ' against ' + month(p.asof) : '', big: money(x.rolling_12mo_b * B, true), small: '12 months to date' }; }) }],
        foot: 'Source: US Treasury International Capital (TIC) system, transactions in long-term securities, ' + when + ' (read ' + day(d.generated_at) + '). ' + (d.disclaimer || '')
      };
    }
  };

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
      try { render(el, ad.build(docs, state)); }
      catch (e) { el.dataset.state = 'error'; el.innerHTML = '<div class="jts-in"><div class="jts-wait">The tearsheet could not be built from this feed (' + esc(e && e.message || e) + '). The rest of the page is unaffected.</div></div>'; }
    }
    el.addEventListener('click', function (e) {
      var c = e.target.closest('[data-jts-ctl]'), a = e.target.closest('[data-jts-act]');
      if (c) { state.fund = c.getAttribute('data-jts-ctl'); paint(); return; }
      if (!a) return;
      var act = a.getAttribute('data-jts-act');
      if (act === 'more') { el.__long = !el.__long; paint(); }
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
    Promise.all(ad.feeds.map(get)).then(function (list) { docs = list; paint(); }).catch(function (e) {
      el.dataset.state = 'error';
      el.innerHTML = '<div class="jts-in"><div class="jts-wait">The tearsheet\u2019s feed is not available right now (' + esc(e && e.message || e) + '). The rest of the page is unaffected.</div></div>';
    });
  }

  function boot() { Array.prototype.forEach.call(document.querySelectorAll('[data-jts]'), mount); }
  var api = { version: '1.0.0', render: render, ring: ring, adapters: ADAPTERS, mount: mount, money: money, pct: pct };
  if (typeof module === 'object' && module.exports) module.exports = api;   /* node: the adapters and the ring are pure, tests/tearsheet-kit.test.js */
  if (typeof window === 'undefined' || typeof document === 'undefined') return;
  window.JHTearsheet = api;
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot); else boot();
})();
