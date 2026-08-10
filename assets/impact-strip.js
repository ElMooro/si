/* assets/impact-strip.js — the ONE renderer for the wo4580 impact contract.
   Every engine payload now carries impact_map (schema impact-map/1.0):
   benefiting[]/suffering[] rows in three honest classes —
     measured   : arithmetic on real data (solid badge, no CI needed)
     estimated  : model output — MUST show ±CI and n_obs or it is not shown
     structural : direction only (arrow, no pp asserted)
   plus insufficient[] (the honest non-answers). Namespaced styles injected
   once; degrades silently. Two entry points:
     window.jhImpactStrip(el, impact_map)         — render a map you have
     window.jhImpactStripAuto(elId, payloadUrl)   — fetch payload, render .impact_map
*/
(function () {
  "use strict";
  if (window.jhImpactStrip) return;

  var CSS = ""
    + ".jhim{font-family:ui-monospace,Menlo,Consolas,monospace;background:#10151d;"
    + "border:1px solid #1c2330;border-radius:12px;padding:14px 16px;margin:14px 0;"
    + "color:#e6ecf3;font-size:11.5px;line-height:1.45;max-width:1180px}"
    + ".jhim-h{font-size:11px;text-transform:uppercase;letter-spacing:1.2px;"
    + "color:#5b6479;font-weight:600;margin-bottom:8px}"
    + ".jhim-cols{display:grid;grid-template-columns:1fr 1fr;gap:16px}"
    + "@media(max-width:760px){.jhim-cols{grid-template-columns:1fr}}"
    + ".jhim-side{min-width:0}"
    + ".jhim-st{font-size:10.5px;font-weight:700;margin-bottom:5px;"
    + "letter-spacing:.6px}"
    + ".jhim-row{display:flex;align-items:baseline;gap:7px;margin:3px 0;"
    + "white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
    + ".jhim-nm{overflow:hidden;text-overflow:ellipsis;min-width:0;flex:1}"
    + ".jhim-kind{font-size:9px;color:#5b6479;border:1px solid #2a3242;"
    + "border-radius:4px;padding:0 4px;flex:none}"
    + ".jhim-pp{font-weight:800;flex:none}"
    + ".jhim-est{color:#F0B429;font-weight:600;flex:none}"
    + ".jhim-ci{color:#8b96a8;font-size:9.5px;flex:none}"
    + ".jhim-arrow{font-weight:800;flex:none}"
    + ".jhim-foot{color:#5b6479;font-size:9.8px;margin-top:9px;"
    + "border-top:1px solid #1c2330;padding-top:7px}"
    + ".jhim-insuf{color:#8b96a8;font-size:10px;margin-top:7px}"
    + ".jhim-empty{color:#5b6479}";

  function ensureCss() {
    if (document.getElementById("jhim-css")) return;
    var st = document.createElement("style");
    st.id = "jhim-css"; st.textContent = CSS;
    document.head.appendChild(st);
  }

  function esc(x) {
    return String(x == null ? "" : x).replace(/[<>&"]/g, function (c) {
      return { "<": "&lt;", ">": "&gt;", "&": "&amp;", '"': "&quot;" }[c];
    });
  }

  function fmtPP(v) {
    if (v == null || isNaN(v)) return "";
    return (v > 0 ? "+" : "") + Number(v).toFixed(Math.abs(v) >= 100 ? 0 : 1) + "pp";
  }

  function rowHtml(r, pos) {
    var GRN = "#6fce8a", RED = "#E07A6A";
    var col = pos ? GRN : RED;
    var name = '<span class="jhim-nm" title="' + esc(r.basis || "") + '">'
      + esc(r.name) + '</span>'
      + '<span class="jhim-kind">' + esc(r.kind === "industry" ? "ind" : "co")
      + (r.n_members ? " ×" + r.n_members : "") + '</span>';
    if (r.pp_kind === "measured") {
      return '<div class="jhim-row">' + name
        + '<span class="jhim-pp" style="color:' + col + '" title="'
        + esc(r.unit || "") + '">' + fmtPP(r.pp) + '</span></div>';
    }
    if (r.pp_kind === "estimated") {
      if (r.ci == null || r.n_obs == null) return ""; // contract: never naked
      return '<div class="jhim-row">' + name
        + '<span class="jhim-est" title="' + esc(r.unit || "") + '">'
        + fmtPP(r.pp) + '</span>'
        + '<span class="jhim-ci">[' + fmtPP(r.ci[0]) + "…" + fmtPP(r.ci[1])
        + '] n=' + esc(r.n_obs) + '</span></div>';
    }
    // structural — direction only, no number invented
    var up = (r.direction || (pos ? "benefit" : "suffer")) === "benefit";
    return '<div class="jhim-row">' + name
      + '<span class="jhim-arrow" style="color:' + (up ? GRN : RED)
      + '" title="structural exposure — direction only, no pp asserted">'
      + (up ? "▲" : "▼") + '</span></div>';
  }

  function side(title, rows, pos, color) {
    var body = (rows || []).slice(0, 12).map(function (r) {
      return rowHtml(r, pos);
    }).join("");
    if (!body) body = '<div class="jhim-empty">— none surfaced this run</div>';
    return '<div class="jhim-side"><div class="jhim-st" style="color:'
      + color + '">' + title + '</div>' + body + '</div>';
  }

  function render(el, m) {
    ensureCss();
    if (!el) return;
    if (!m || !m.schema) {
      el.innerHTML = '<div class="jhim"><div class="jhim-h">Impact — who '
        + 'benefits / who suffers</div><div class="jhim-empty">impact layer '
        + 'pending this engine\u2019s first post-wo4580 run</div></div>';
      return;
    }
    var insuf = (m.insufficient || []);
    var insufHtml = insuf.length
      ? '<div class="jhim-insuf" title="'
        + esc(insuf.slice(0, 4).map(function (x) {
            return x.name + ": " + x.reason;
          }).join(" · "))
        + '">' + insuf.length + ' honest non-answer'
        + (insuf.length > 1 ? "s" : "")
        + ' (insufficient history/coverage — hover)</div>'
      : "";
    el.innerHTML = '<div class="jhim">'
      + '<div class="jhim-h">Impact — ' + esc(m.factor || "")
      + ' <span style="text-transform:none;letter-spacing:0">· measured pp are arithmetic on real data; '
      + 'estimated pp always carry ±CI and n; ▲▼ are structural direction only</span></div>'
      + '<div class="jhim-cols">'
      + side("BENEFITING", m.benefiting, true, "#6fce8a")
      + side("SUFFERING", m.suffering, false, "#E07A6A")
      + '</div>' + insufHtml
      + '<div class="jhim-foot">' + esc(m.method || "")
      + (m.basis_note ? " — " + esc(m.basis_note) : "")
      + (m.generated_at ? " · " + esc(String(m.generated_at).slice(0, 16)) : "")
      + '</div></div>';
  }

  window.jhImpactStrip = render;
  window.jhImpactStripAuto = function (elId, url) {
    var el = document.getElementById(elId);
    if (!el) return;
    fetch(url + (url.indexOf("?") < 0 ? "?" : "&") + "t=" + Date.now())
      .then(function (r) { return r.ok ? r.json() : null; })
      .then(function (p) { render(el, p && p.impact_map); })
      .catch(function () { render(el, null); });
  };
})();
