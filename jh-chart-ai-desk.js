/* What the public AI + risk-gate will admit onto the chart.
   Private notes / constitution are NOT read here (401 by design). */
(function () {
  function line(id, text) {
    var q = document.getElementById("quote");
    if (!q) return;
    var n = document.getElementById(id);
    if (!n) {
      n = document.createElement("div");
      n.id = id;
      n.style.cssText = "flex-basis:100%;font-size:10px;color:var(--mut)";
      q.appendChild(n);
    }
    n.textContent = text;
  }
  async function pull() {
    var ai = null, rg = null;
    try { ai = await (await fetch("/data/ai.json", { cache: "no-store" })).json(); } catch (e) {}
    try { rg = await (await fetch("/data/risk-gate.json", { cache: "no-store" })).json(); } catch (e) {}
    var parts = [];
    if (ai && ai.market_read && ai.market_read.stances) {
      var s = ai.market_read.stances;
      parts.push("AI " + (s.stocks || "—") + " eq / " + (s.bonds || "—") + " bd / " + (s.crypto || "—") + " cry");
    }
    if (ai && ai.scoreboard) {
      var sb = ai.scoreboard;
      var learned = (sb.categories_learned || []).join(",");
      var excl = (sb.categories_excluded || []).join(",");
      parts.push("notes " + (sb.notes_studied || 0) + " learned[" + learned + "] excluded[" + excl + "]");
    }
    if (rg) {
      var fund = (rg.legs && rg.legs.funding) || {};
      parts.push("gate " + (rg.posture || "?") + " size " + (rg.sizing_multiplier != null ? rg.sizing_multiplier : "?") +
        " fund " + (fund.score != null ? fund.score : "?"));
      if ((fund.why || [])[0]) parts.push(String(fund.why[0]).slice(0, 90));
    }
    line("jh-ai-desk", parts.join(" | ") || "AI desk feed missing");
    var mk = [];
    if (rg && rg.posture && window.lastBars && window.lastBars.length) {
      var t = window.lastBars[window.lastBars.length - 1].time;
      var bad = String(rg.posture).indexOf("OFF") >= 0 || String(rg.posture).indexOf("SEVERE") >= 0;
      var fundS = rg.legs && rg.legs.funding && rg.legs.funding.score;
      if (fundS != null && fundS <= -1.5)
        mk.push({ time: t, position: "aboveBar", color: "#f23645", shape: "square", text: "GATE-PLUMB" });
      if (bad)
        mk.push({ time: t, position: "aboveBar", color: "#f23645", shape: "arrowDown", text: String(rg.posture) });
    }
    return mk;
  }
  window.jhAiDesk = pull;
  setInterval(pull, 120000);
  window.addEventListener("load", function () { setTimeout(pull, 1200); });
})();
