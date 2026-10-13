/* jh-reskin-skip */
/* ETF Desk Overlay — dense sortable desk matching heatmap style */
(function () {
  function esc(s) { return String(s||'').replace(/[&<>"]/g, c => ({'&':'&','<':'<','>':'>','"':'"'}[c])); }
  function fmtFlow(n) {
    if (n == null || n !== n) return '—';
    var abs = Math.abs(n);
    var body = abs >= 1e9 ? (abs/1e9).toFixed(1)+'B' : abs >= 1e6 ? (abs/1e6).toFixed(1)+'M' : abs >= 1e3 ? (abs/1e3).toFixed(0)+'K' : abs.toFixed(0);
    return (n > 0 ? '+' : n < 0 ? '−' : '') + body;
  }
  function paintFlow(v) {
    if (v == null || v !== v) return ['#2a2e39','#d1d4dc'];
    var t = Math.max(-1, Math.min(1, v / 5e8)); // rough scale
    var u = Math.pow(Math.abs(t), 0.6);
    var src = t < 0 ? [242,54,69] : [8,153,129];
    var mid = [32,35,46];
    var r = Math.round(mid[0] + (src[0]-mid[0])*u);
    var g = Math.round(mid[1] + (src[1]-mid[1])*u);
    var b = Math.round(mid[2] + (src[2]-mid[2])*u);
    return ['rgb('+r+','+g+','+b+')', u > 0.4 ? '#fff' : '#d1d4dc'];
  }

  async function loadPacket() {
    var res = await fetch('/data/etf-desk-research.json', {credentials:'omit'});
    if (!res.ok) throw new Error(res.status);
    return res.json();
  }

  function render(host, packet) {
    var funds = Object.keys(packet.funds || {}).sort();
    var html = '<style>.jh-etf{font:12px/1.25 system-ui,sans-serif;color:#d1d4dc;background:#000;height:100%;overflow:auto}'
      + '.jh-etf .top{display:flex;align-items:center;gap:12px;padding:8px 12px;background:#131722;border-bottom:1px solid #2a2e39;position:sticky;top:0;z-index:2}'
      + '.jh-etf .tabs button{padding:6px 10px;color:#787b86;background:transparent;border:0}'
      + '.jh-etf .tabs button.on{color:#fff;border-bottom:2px solid #2962ff}'
      + '.jh-etf table{width:100%;border-collapse:collapse}'
      + '.jh-etf th{text-align:left;padding:6px 8px;background:#1e222d;position:sticky;top:36px;cursor:pointer;color:#787b86;font-weight:600}'
      + '.jh-etf td{padding:5px 8px;border-bottom:1px solid #2a2e39}'
      + '.jh-etf .flow{font-variant-numeric:tabular-nums}'
      + '</style><div class=jh-etf>'
      + '<div class=top><span style="font-weight:600">ETF Desk</span>'
      + '<div class=tabs><button class=on data-tab=flows>Flows</button><button data-tab=holdings>Holdings</button><button data-tab=census>Census</button><button data-tab=tech>Technicals</button><button data-tab=tops>Top Held/Added/Deleted</button></div>'
      + '<span style="margin-left:auto;color:#787b86;font-size:11px">' + funds.length + ' funds · evidence packet</span></div>'
      + '<div id=jh-etf-body></div></div>';
    host.innerHTML = html;
    var body = host.querySelector('#jh-etf-body');
    function showFlows() {
      var rows = funds.map(function(t) {
        var f = packet.funds[t];
        var w1 = f.flows && f.flows.aligned_windows && f.flows.aligned_windows['1'];
        var w5 = f.flows && f.flows.aligned_windows && f.flows.aligned_windows['5'];
        var w21 = f.flows && f.flows.aligned_windows && f.flows.aligned_windows['21'];
        return {t:t, f1: w1 && w1.flow_usd_decimal, f5: w5 && w5.flow_usd_decimal, f21: w21 && w21.flow_usd_decimal, aum: f.profiles && f.profiles.summary && f.profiles.summary.numeric && f.profiles.summary.numeric.aum};
      });
      var sortKey = 'f21', dir = -1;
      function draw() {
        rows.sort(function(a,b) {
          var av = a[sortKey], bv = b[sortKey];
          if (av == null && bv == null) return a.t < b.t ? -1 : 1;
          if (av == null) return 1;
          if (bv == null) return -1;
          return (av - bv) * dir;
        });
        var h = '<table><thead><tr><th data-k=t>Ticker</th><th data-k=aum>AUM</th><th data-k=f1>1D Flow</th><th data-k=f5>5D Flow</th><th data-k=f21>21D Flow</th></tr></thead><tbody>';
        rows.forEach(function(r) {
          var c1 = paintFlow(r.f1), c5 = paintFlow(r.f5), c21 = paintFlow(r.f21);
          h += '<tr><td>'+esc(r.t)+'</td><td>'+(r.aum ? (r.aum/1e9).toFixed(1)+'B' : '—')+'</td>'
            + '<td class=flow style="background:'+c1[0]+';color:'+c1[1]+'">'+fmtFlow(r.f1)+'</td>'
            + '<td class=flow style="background:'+c5[0]+';color:'+c5[1]+'">'+fmtFlow(r.f5)+'</td>'
            + '<td class=flow style="background:'+c21[0]+';color:'+c21[1]+'">'+fmtFlow(r.f21)+'</td></tr>';
        });
        h += '</tbody></table>';
        body.innerHTML = h;
        body.querySelectorAll('th').forEach(function(th) {
          th.onclick = function() {
            var k = th.getAttribute('data-k');
            if (sortKey === k) dir = -dir; else { sortKey = k; dir = k === 't' ? 1 : -1; }
            draw();
          };
        });
      }
      draw();
    }
    showFlows();
    host.querySelectorAll('.tabs button').forEach(function(btn) {
      btn.onclick = function() {
        host.querySelectorAll('.tabs button').forEach(function(b){b.className='';});
        btn.className = 'on';
        var tab = btn.getAttribute('data-tab');
        if (tab === 'flows') showFlows();
        else body.innerHTML = '<div style="padding:20px;color:#787b86">'+tab+' section — data from existing packets, expanding next. Sortable tables coming for holdings, census, MAs, spreads, EVR, top held/added/deleted.</div>';
      };
    });
  }

  window.jhEtfDeskOverlay = function(host) {
    if (!host) return;
    host.innerHTML = '<div style="padding:16px;color:#787b86">Loading ETF desk evidence…</div>';
    loadPacket().then(function(packet) {
      if (host.isConnected) render(host, packet);
    }).catch(function(err) {
      host.innerHTML = '<div style="padding:16px;color:#f23645">Could not load etf-desk-research.json: '+esc(err.message)+'</div>';
    });
  };
})();
