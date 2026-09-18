/* Identified market observations with visible clocks and inspectable definitions. */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.JHMarketTape = factory();
}(typeof window !== "undefined" ? window : this, function () {
  "use strict";
  function describe(item) {
    return [item.definition || item.label, "Unit: " + (item.unit || "unverified"),
      "Observed: " + (item.observed_at || item.observation_date || "unknown"),
      item.comparison_date ? "Comparison: " + item.comparison_date : "",
      item.seasonal_adjustment || "", "Source: " + (item.src || item.source || item.series_id || "unknown"),
      item.quality && item.quality.status === "delayed" ? "Delayed quote" : "",
      item.evidence ? "Archived source evidence available in /data/market-tape.json" : ""].filter(Boolean).join(" · ");
  }
  function render(doc, target, packet, now) {
    target.replaceChildren();
    now = now == null ? Date.now() : now;
    var stamp = Date.parse(packet && packet.generated_at);
    if (!packet || packet.schema_version !== "2.0" || !Number.isFinite(stamp) || now - stamp > 900000 || stamp - now > 300000) {
      target.textContent = "Market observations unavailable or stale";
      return;
    }
    (packet.items || []).forEach(function (item) {
      if (!item || typeof item.value !== "number" || !Number.isFinite(item.value) || !item.observation_date || !item.unit) return;
      var state = item.quality && item.quality.status;
      if (state !== "fresh" && state !== "delayed") return;
      var sp = doc.createElement("span"); sp.className = "jhc-chip";
      sp.setAttribute("data-sym", item.label || "");
      sp.setAttribute("tabindex", "0"); sp.title = describe(item);
      sp.setAttribute("aria-label", (item.label || "") + " " + (item.display || item.value) + ". " + describe(item));
      var label = doc.createElement("b"); label.textContent = item.label || "";
      var value = doc.createElement("span"); value.textContent = " " + (item.display || item.value);
      if (typeof item.chg_pct === "number" && Number.isFinite(item.chg_pct)) {
        value.className = item.chg_pct >= 0 ? "jhc-up" : "jhc-dn";
        value.textContent += (item.chg_pct >= 0 ? " +" : " ") + item.chg_pct.toFixed(1) + "%";
      }
      var date = doc.createElement("small"); date.className = "jhc-asof";
      date.textContent = " · " + (item.frequency === "quote" && item.observed_at
        ? item.observed_at.slice(5, 16).replace("T", " ") + "Z" : item.observation_date);
      if (state === "delayed") date.textContent += " delayed";
      sp.appendChild(label); sp.appendChild(value); sp.appendChild(date); target.appendChild(sp);
    });
    var evidence = doc.createElement("a"); evidence.href = "/data/market-tape.json";
    evidence.className = "jhc-chip"; evidence.textContent = "Sources";
    evidence.title = "Definitions, observation dates, formulas, archived evidence and unavailable inputs";
    target.appendChild(evidence);
    if (packet.gaps && packet.gaps.length) {
      var gap = doc.createElement("span"); gap.className = "jhc-chip";
      gap.textContent = packet.gaps.length + " unavailable";
      gap.title = packet.gaps.map(function (g) { return g.label + ": " + g.reason; }).join(" · ");
      target.appendChild(gap);
    }
  }
  return {render: render, describe: describe};
}));
