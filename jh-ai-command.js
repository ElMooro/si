/* jh-reskin-skip */
/* Reported student diagnostics; no forecast or promotion authority. */
(function () {
  if (window.__jhAiCommand) return;
  window.__jhAiCommand = true;
  function esc(value) { return String(value == null ? "" : value).replace(/[&<>"']/g, function (c) { return {"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]; }); }
  function el() {
    var n = document.getElementById("jh-ai-command");
    if (n) return n;
    n = document.createElement("section"); n.id = "jh-ai-command";
    n.style.cssText = "margin:12px 0 18px;padding:14px 16px;background:#0e1016;border:1px solid #2c3344;border-radius:12px;color:#e8edf5;font:13px/1.45 Inter,system-ui,sans-serif;overflow-wrap:anywhere";
    var main = document.getElementById("main") || document.body; main.insertBefore(n, main.firstChild); return n;
  }
  function fraction(x) { return typeof x === "number" && Number.isFinite(x) && x >= 0 && x <= 1; }
  function pct(x) { return fraction(x) ? (x * 100).toFixed(1) + "%" : "\u2014"; }
  function count(x) { return typeof x === "number" && Number.isInteger(x) && x >= 0 ? String(x) : "unavailable"; }
  function row(k, v) {
    return '<div style="display:flex;justify-content:space-between;gap:12px;padding:4px 0;border-bottom:1px solid #1c2230"><span style="color:#8b95a8">' + esc(k) + '</span><span>' + esc(v) + '</span></div>';
  }
  function list(title, arr) {
    if (!Array.isArray(arr) || !arr.length) return "";
    return '<div style="margin-top:10px"><div style="color:#9ec5ff;font-size:11px;letter-spacing:.06em">' + esc(title) + '</div><ul style="margin:6px 0 0 18px;padding:0">' + arr.map(function (x) { return '<li style="margin:3px 0">' + esc(x) + '</li>'; }).join("") + '</ul></div>';
  }
  function render(d, ai) {
    var n = el(), me = (d && d.market_exam_holdout) || {}, ce = (d && d.coding_exam) || {}, calls = (d && d.calls) || {};
    var typed = (calls.qualification || {}).contract === "market-read-reported-outcomes.v1" && calls.graded_unit === "reported_window";
    var beat = me.beats_prior === true ? "YES (reported comparison)" : me.beats_prior === false ? "NO (reported comparison)" : "unavailable";
    n.innerHTML = '<div style="font:12px IBM Plex Mono,monospace;color:#9ec5ff">JUSTHODL STUDENT</div>' +
      '<div style="font-size:22px;margin:4px 0 8px">' + esc(d && d.decision_status || "ADVISORY_ONLY") + '</div>' +
      '<div style="color:#a8b0be;margin-bottom:10px">' + esc(d && d.voice || "") + '</div>' +
      '<div>Reported diagnostics. Exam comparisons and outcome counts do not establish forecasting skill, weight promotion or portfolio authority.</div>' +
      row("Understanding (notes)", pct(d && d.understanding_score)) +
      row("Coding exam", (typeof ce.passed === "string" ? ce.passed : "\u2014") + " score " + (fraction(ce.score) ? ce.score.toFixed(3) : "\u2014")) +
      row("Market exam comparison", pct(me.score) + " vs prior " + pct(me.prior_score) + " · beats prior: " + beat) +
      row("Reported outcome windows", typed ? count(calls.graded) : "validation unavailable") +
      row("Calls recorded", count(calls.made)) +
      row("Pipeline", (d && d.pipeline) || ((ai && ai.pipeline) || {}).status || "\u2014") +
      '<div style="margin-top:12px;color:#d7c58b">' + esc(typed ? (d && d.next_lesson) || "" : "Outcome qualification is unavailable; legacy promotion narratives are withheld.") + '</div>' +
      list("REPORTED CAPABILITIES", d && d.can_do) + list("REPORTED LIMITATIONS", d && d.cannot_do_yet);
  }
  Promise.all([
    fetch("/data/ai-student-desk.json", { cache: "no-store" }).then(function (r) { return r.ok ? r.json() : null; }),
    fetch("/data/ai.json", { cache: "no-store" }).then(function (r) { return r.ok ? r.json() : null; })
  ]).then(function (a) { render(a[0] || {}, a[1] || {}); }).catch(function () {});
})();
