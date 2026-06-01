/**
 * ai-portfolio-kit.js  v1.0.0
 * ─────────────────────────────────────────────────────────────────
 * Personalized portfolio brief renderer. Used on portfolio-manager.html.
 *
 *   <script src="/ai-portfolio-kit.js"></script>
 *   <div id="ai-portfolio"></div>
 *   <script>JHAIPortfolio.mount('ai-portfolio');</script>
 *
 * Schema:
 *   {headline, regime_fit, thesis, biggest_strength, biggest_concern,
 *    concentration_flags, out_of_regime_holdings, this_weeks_action,
 *    tripwires_for_this_book}
 */
(function () {
  if (window.JHAIPortfolio) return;

  var S3_BASE = "https://justhodl-dashboard-live.s3.amazonaws.com/data";
  var PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
  function jhFetch(slug, suffix) {
    var ts = Date.now();
    var p = S3_BASE + "/" + slug + ".json" + (suffix || "") + (suffix && suffix.indexOf("?") >= 0 ? "&" : "?") + "t=" + ts;
    var f = PROXY   + "/" + slug + ".json" + (suffix || "") + (suffix && suffix.indexOf("?") >= 0 ? "&" : "?") + "t=" + ts;
    return fetch(p).then(function (r) {
      if (r.ok) return r;
      return fetch(f).then(function (r2) {
        if (!r2.ok) throw new Error("Both endpoints failed (S3=" + r.status + ", proxy=" + r2.status + ")");
        return r2;
      });
    });
  }

  var DEFAULT_KEY = "portfolio-manager-brief";

  function injectCSS() {
    if (document.getElementById("jh-ai-portfolio-css")) return;
    var s = document.createElement("style");
    s.id = "jh-ai-portfolio-css";
    s.textContent = "\n.jhpf-wrap{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#e6eaf2;font-size:14px;line-height:1.6;margin-bottom:22px}\n.jhpf-headline{background:linear-gradient(135deg,rgba(167,139,250,.10),rgba(0,212,255,.04));border:1px solid rgba(167,139,250,.28);border-radius:10px;padding:18px 22px;margin-bottom:12px}\n.jhpf-headline.STRONG_FIT{border-color:rgba(38,255,175,.45);background:linear-gradient(135deg,rgba(38,255,175,.10),rgba(0,212,255,.04))}\n.jhpf-headline.NEUTRAL{border-color:rgba(255,210,102,.32);background:linear-gradient(135deg,rgba(255,210,102,.08),rgba(0,212,255,.04))}\n.jhpf-headline.POOR_FIT{border-color:rgba(255,85,119,.42);background:linear-gradient(135deg,rgba(255,85,119,.10),rgba(255,122,24,.05))}\n\n.jhpf-meta{display:flex;align-items:center;gap:11px;margin-bottom:12px;flex-wrap:wrap}\n.jhpf-badge{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;letter-spacing:1.3px;text-transform:uppercase;color:#a78bfa;font-weight:700}\n.jhpf-fit{font-family:ui-monospace,monospace;font-size:12.5px;font-weight:700;padding:4px 11px;border-radius:4px;letter-spacing:0.5px}\n.jhpf-fit.STRONG_FIT{background:rgba(38,255,175,.18);color:#26ffaf}\n.jhpf-fit.NEUTRAL{background:rgba(255,210,102,.16);color:#ffd266}\n.jhpf-fit.POOR_FIT{background:rgba(255,85,119,.18);color:#ff5577}\n.jhpf-age{font-family:ui-monospace,monospace;font-size:10.5px;color:#6f7b91;margin-left:auto}\n\n.jhpf-h{font-size:16px;font-weight:700;color:#e6eaf2;line-height:1.45;margin-bottom:8px}\n.jhpf-t{font-size:13px;color:#cbd2dc;line-height:1.65}\n\n.jhpf-section-h{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;letter-spacing:1.4px;text-transform:uppercase;color:#00d4ff;margin:14px 0 7px;padding-bottom:5px;border-bottom:1px solid #1c2433}\n\n.jhpf-row2{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:6px}\n@media (max-width:700px){.jhpf-row2{grid-template-columns:1fr}}\n.jhpf-strength,.jhpf-concern{background:#11161f;border:1px solid #1c2433;border-radius:7px;padding:11px 13px;font-size:12.5px}\n.jhpf-strength{border-left:3px solid #26ffaf}\n.jhpf-strength .lbl{font-family:ui-monospace,monospace;font-size:10px;letter-spacing:1px;color:#26ffaf;text-transform:uppercase;margin-bottom:4px;font-weight:700}\n.jhpf-concern{border-left:3px solid #ff5577}\n.jhpf-concern .lbl{font-family:ui-monospace,monospace;font-size:10px;letter-spacing:1px;color:#ff5577;text-transform:uppercase;margin-bottom:4px;font-weight:700}\n\n.jhpf-flag{background:#11161f;border:1px solid #1c2433;border-radius:6px;padding:10px 13px;margin-bottom:6px;font-size:12.5px;border-left:3px solid #ff7a18}\n.jhpf-flag.HIGH{border-left-color:#ff5577}\n.jhpf-flag.MEDIUM{border-left-color:#ff7a18}\n.jhpf-flag.LOW{border-left-color:#ffd266}\n.jhpf-flag .head{display:flex;align-items:center;gap:8px;margin-bottom:5px}\n.jhpf-flag .sev{font-family:ui-monospace,monospace;font-size:10px;font-weight:700;padding:2px 6px;border-radius:3px;letter-spacing:0.5px;background:#1c2433}\n.jhpf-flag.HIGH .sev{color:#ff5577}\n.jhpf-flag.MEDIUM .sev{color:#ff7a18}\n.jhpf-flag.LOW .sev{color:#ffd266}\n.jhpf-flag .type{font-family:ui-monospace,monospace;font-size:10.5px;color:#a8b3c7;text-transform:uppercase;letter-spacing:0.5px}\n.jhpf-flag .what{color:#e6eaf2;margin-bottom:5px;line-height:1.5}\n.jhpf-flag .action{font-size:11.5px;color:#cbd2dc;line-height:1.5;padding-top:5px;border-top:1px dashed #1c2433}\n.jhpf-flag .action b{color:#26ffaf}\n\n.jhpf-action{background:linear-gradient(135deg,rgba(38,255,175,.06),rgba(0,212,255,.03));border:1px solid rgba(38,255,175,.35);border-left:4px solid #26ffaf;border-radius:9px;padding:14px 17px;margin-bottom:10px}\n.jhpf-action .head{font-family:ui-monospace,monospace;font-size:11px;color:#26ffaf;font-weight:700;letter-spacing:1.4px;text-transform:uppercase;margin-bottom:6px}\n.jhpf-action .trade{font-size:14px;color:#e6eaf2;font-weight:600;line-height:1.55;margin-bottom:6px}\n.jhpf-action .rationale{font-size:12px;color:#cbd2dc;line-height:1.55;margin-bottom:6px;font-style:italic}\n.jhpf-action .min{font-size:11.5px;color:#a8b3c7;line-height:1.5;border-top:1px dashed rgba(38,255,175,.18);padding-top:6px}\n.jhpf-action .min b{color:#a78bfa}\n\n.jhpf-oor{background:#11161f;border:1px solid #1c2433;border-radius:6px;padding:10px 13px;margin-bottom:6px;font-size:12.5px;border-left:3px solid #ff5577;display:flex;gap:10px;align-items:baseline}\n.jhpf-oor .tick{font-family:ui-monospace,monospace;font-size:14px;font-weight:800;color:#ff5577;letter-spacing:0.5px}\n.jhpf-oor .why{flex:1;color:#cbd2dc;line-height:1.5}\n\n.jhpf-trip{background:rgba(255,85,119,.05);border:1px solid rgba(255,85,119,.22);border-left:3px solid #ff5577;border-radius:6px;padding:10px 13px;margin-bottom:6px;font-size:12.5px}\n.jhpf-trip .head{display:flex;align-items:center;gap:8px;margin-bottom:4px}\n.jhpf-trip .sev{font-family:ui-monospace,monospace;font-size:10px;color:#ff5577;font-weight:700;padding:2px 6px;background:rgba(255,85,119,.14);border-radius:3px}\n.jhpf-trip .cond{color:#e6eaf2;line-height:1.5;margin-bottom:5px}\n.jhpf-trip .action{font-size:11.5px;color:#cbd2dc;line-height:1.5;padding-top:5px;border-top:1px dashed rgba(255,85,119,.18)}\n.jhpf-trip .action b{color:#ff7a18}\n\n.jhpf-loading{padding:24px;text-align:center;color:#6f7b91;font-family:ui-monospace,monospace;font-size:12px}\n.jhpf-err{padding:14px 17px;background:rgba(255,85,119,.07);border:1px solid rgba(255,85,119,.22);border-left:3px solid #ff5577;border-radius:7px;color:#ff5577;font-size:12.5px;font-family:ui-monospace,monospace}\n";
    document.head.appendChild(s);
  }

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }
  function ageStr(iso) {
    if (!iso) return "";
    var ms = Date.now() - new Date(iso).getTime();
    var m = Math.round(ms / 60000);
    if (m < 1) return "just now";
    if (m < 60) return m + "m ago";
    if (m < 1440) return Math.round(m / 60) + "h ago";
    return Math.round(m / 1440) + "d ago";
  }

  function mount(elId, contextSlug, opts) {
    injectCSS();
    var el = document.getElementById(elId);
    if (!el) return;
    el.classList.add("jhpf-wrap");
    el.innerHTML = '<div class="jhpf-loading">⚡ generating personalized portfolio brief…</div>';

    var url = PROXY + "/" + (contextSlug || DEFAULT_KEY) + ".json?t=" + Date.now();
    return fetch(url).then(function (r) {
      if (!r.ok) throw new Error("HTTP " + r.status);
      return r.json();
    }).then(function (b) {
      var fit = (b.regime_fit || "NEUTRAL").toUpperCase();
      var html = '';

      html += '<div class="jhpf-headline ' + fit + '">' +
                '<div class="jhpf-meta">' +
                  '<span class="jhpf-badge">📓 Personalized PM Brief</span>' +
                  '<span class="jhpf-fit ' + fit + '">' + esc(fit.replace(/_/g, " ")) + '</span>' +
                  '<span class="jhpf-age">' + esc(ageStr(b.generated_at)) + '</span>' +
                '</div>' +
                (b.headline ? '<div class="jhpf-h">' + esc(b.headline) + '</div>' : '') +
                (b.thesis ? '<div class="jhpf-t">' + esc(b.thesis) + '</div>' : '') +
              '</div>';

      // Strength + concern side-by-side
      if (b.biggest_strength || b.biggest_concern) {
        html += '<div class="jhpf-row2">';
        if (b.biggest_strength) {
          html += '<div class="jhpf-strength"><div class="lbl">▲ Biggest Strength</div>' + esc(b.biggest_strength) + '</div>';
        }
        if (b.biggest_concern) {
          html += '<div class="jhpf-concern"><div class="lbl">▼ Biggest Concern</div>' + esc(b.biggest_concern) + '</div>';
        }
        html += '</div>';
      }

      // This week's action
      var ta = b.this_weeks_action;
      if (ta && ta.primary_trade) {
        html += '<div class="jhpf-action">' +
                  '<div class="head">⚡ This Week\'s Primary Action</div>' +
                  '<div class="trade">' + esc(ta.primary_trade) + '</div>' +
                  (ta.rationale ? '<div class="rationale">' + esc(ta.rationale) + '</div>' : '') +
                  (ta.if_can_only_do_one_thing ? '<div class="min"><b>Minimum-viable version:</b> ' + esc(ta.if_can_only_do_one_thing) + '</div>' : '') +
                '</div>';
      }

      // Out-of-regime holdings
      if (b.out_of_regime_holdings && b.out_of_regime_holdings.length) {
        html += '<div class="jhpf-section-h">Out-of-Regime Holdings</div>';
        b.out_of_regime_holdings.forEach(function (h) {
          html += '<div class="jhpf-oor">' +
                    '<span class="tick">' + esc(h.ticker) + '</span>' +
                    '<span class="why">' + esc(h.why || '') + '</span>' +
                  '</div>';
        });
      }

      // Concentration flags
      if (b.concentration_flags && b.concentration_flags.length) {
        html += '<div class="jhpf-section-h">Concentration Flags</div>';
        b.concentration_flags.forEach(function (f) {
          var sev = (f.severity || "MEDIUM").toUpperCase();
          html += '<div class="jhpf-flag ' + sev + '">' +
                    '<div class="head"><span class="sev">[' + esc(sev) + ']</span><span class="type">' + esc(f.type) + '</span></div>' +
                    '<div class="what">' + esc(f.what) + '</div>' +
                    (f.action ? '<div class="action"><b>Action:</b> ' + esc(f.action) + '</div>' : '') +
                  '</div>';
        });
      }

      // Book-specific tripwires
      if (b.tripwires_for_this_book && b.tripwires_for_this_book.length) {
        html += '<div class="jhpf-section-h">Tripwires for This Book</div>';
        b.tripwires_for_this_book.forEach(function (t) {
          html += '<div class="jhpf-trip">' +
                    '<div class="head"><span class="sev">[' + esc(t.severity) + ']</span></div>' +
                    '<div class="cond">' + esc(t.condition) + '</div>' +
                    (t.action ? '<div class="action"><b>If triggered:</b> ' + esc(t.action) + '</div>' : '') +
                  '</div>';
        });
      }

      el.innerHTML = html;
      return b;
    }).catch(function (e) {
      el.innerHTML = '<div class="jhpf-err">Portfolio brief unavailable: ' + esc(e.message || e) + '</div>';
      if (opts && typeof opts.onError === "function") opts.onError(e);
      throw e;
    });
  }

  window.JHAIPortfolio = { mount: mount, version: "1.0.0" };
})();
