/* JH_VERDICT_HEADER_V1 -- reads data/verdict.json only. Additive banner. */
(function () {
  if (window.JH_VERDICT_HEADER) return;
  window.JH_VERDICT_HEADER = 1;
  var S3 = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com/data/verdict.json?t=" + Date.now();
  function el(tag, css, text) {
    var n = document.createElement(tag);
    if (css) n.style.cssText = css;
    if (text != null) n.textContent = text;
    return n;
  }
  function paint(v) {
    if (!document.body) return;
    if (document.getElementById("jh-verdict-header")) return;
    var bias = (v && v.bias) || "mixed";
    var bg = bias === "risk-on" ? "#052e16" : bias === "risk-off" ? "#3f0d15" : "#172033";
    var bar = el("div");
    bar.id = "jh-verdict-header";
    bar.setAttribute("data-marker", "JH_VERDICT_HEADER_V1");
    bar.style.cssText = "padding:10px 16px;background:" + bg + ";color:#f3f4f6;border-bottom:1px solid #536078;font:13px/1.4 ui-monospace,Menlo,system-ui;z-index:9999;position:relative";
    if (!v || v.writer !== "jh-fusion-projection") {
      bar.textContent = "VERDICT unavailable — missing projection (no fabricated call)";
      document.body.prepend(bar);
      return;
    }
    var miss = (v.missing_families || []).join(" ") || "none";
    bar.innerHTML = "<b>CALL</b> " + bias.toUpperCase() +
      " · score " + Number(v.score).toFixed(3) +
      " · " + (v.horizon || "") + " " + (v.direction || "") +
      " · regime " + (v.regime_label || "") +
      " · coverage " + (v.coverage == null ? "—" : Number(v.coverage).toFixed(2)) +
      (v.shadow_mode ? " · SHADOW" : "") +
      " <span style='opacity:.75'>missing " + miss + "</span>" +
      " <a href='/fusion.html' style='color:#22d3ee;margin-left:8px'>fusion</a>";
    document.body.prepend(bar);
  }
  fetch(S3, {cache: "no-store"}).then(function (r) { return r.ok ? r.json() : null; }).then(paint).catch(function () { paint(null); });
})();
