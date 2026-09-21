/* jh-reskin-skip */
/* Command overlay for ai.html — student truth, not another factory chrome. */
(function () {
  if (window.__jhAiCommand) return;
  window.__jhAiCommand = true;
  function el() {
    var n = document.getElementById("jh-ai-command");
    if (n) return n;
    n = document.createElement("section");
    n.id = "jh-ai-command";
    n.style.cssText = "margin:12px 0 18px;padding:14px 16px;background:#0e1016;border:1px solid #2c3344;border-radius:12px;color:#e8edf5;font:13px/1.45 Inter,system-ui,sans-serif";
    var main = document.getElementById("main") || document.body;
    main.insertBefore(n, main.firstChild);
    return n;
  }
  function pct(x) { return x == null ? "\u2014" : (Number(x) * 100).toFixed(1) + "%"; }
  function row(k, v) {
    return "<div style=\"display:flex;justify-content:space-between;gap:12px;padding:4px 0;border-bottom:1px solid #1c2230\"><span style=\"color:#8b95a8\">" + k + "</span><span>" + v + "</span></div>";
  }
  function list(title, arr) {
    if (!arr || !arr.length) return "";
    return "<div style=\"margin-top:10px\"><div style=\"color:#9ec5ff;font-size:11px;letter-spacing:.06em\">" + title + "</div><ul style=\"margin:6px 0 0 18px;padding:0\">" +
      arr.map(function (x) { return "<li style=\"margin:3px 0\">" + x + "</li>"; }).join("") + "</ul></div>";
  }
  function render(d, ai) {
    var n = el();
    var me = (d && d.market_exam_holdout) || {};
    var ce = (d && d.coding_exam) || {};
    var calls = (d && d.calls) || {};
    var beat = me.beats_prior === true ? "YES — promote path open" : (me.beats_prior === false ? "NO — do not promote weights" : "unknown");
    n.innerHTML =
      "<div style=\"font:12px IBM Plex Mono,monospace;color:#9ec5ff\">JUSTHODL STUDENT</div>" +
      "<div style=\"font-size:22px;margin:4px 0 8px\">" + (d && d.decision_status || "ADVISORY_ONLY") + "</div>" +
      "<div style=\"color:#a8b0be;margin-bottom:10px\">" + ((d && d.voice) || "") + "</div>" +
      row("Understanding (notes)", pct(d && d.understanding_score)) +
      row("Coding exam", (ce.passed || "\u2014") + "  score " + (ce.score != null ? Number(ce.score).toFixed(3) : "\u2014")) +
      row("Market holdout", pct(me.score) + " vs prior " + pct(me.prior_score) + " · beats prior: " + beat) +
      row("Calls graded", (calls.graded || 0) + " / " + (calls.made || 0) + " (hit rate null until graded)") +
      row("Pipeline", (d && d.pipeline) || ((ai && ai.pipeline) || {}).status || "\u2014") +
      "<div style=\"margin-top:12px;color:#d7c58b\">" + ((d && d.next_lesson) || "") + "</div>" +
      list("CAN DO", d && d.can_do) +
      list("CANNOT YET", d && d.cannot_do_yet);
  }
  Promise.all([
    fetch("/data/ai-student-desk.json", { cache: "no-store" }).then(function (r) { return r.ok ? r.json() : null; }),
    fetch("/data/ai.json", { cache: "no-store" }).then(function (r) { return r.ok ? r.json() : null; })
  ]).then(function (a) {
    render(a[0] || {}, a[1] || {});
  }).catch(function () {});
})();
