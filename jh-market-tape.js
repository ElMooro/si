/* Identified market observations with visible clocks and inspectable definitions. */
(function (root, factory) {
  if (typeof module === "object" && module.exports) module.exports = factory();
  else root.JHMarketTape = factory();
}(typeof window !== "undefined" ? window : this, function () {
  "use strict";
  // 8/10 quote-freshness compatibility: prefer the new badge vocabulary
  // (LIVE/DELAYED/SESSION/STALE/OFFLINE from aws/shared/quote_meta.py),
  // fall back to legacy quality.status ("fresh"/"delayed") for old packets.
  // Returns one of: fresh, delayed, session, stale, offline, unknown.
  function badgeState(item) {
    var badge = item && item.badge;
    if (badge === "LIVE") return "fresh";
    if (badge === "DELAYED") return "delayed";
    if (badge === "SESSION") return "session";
    if (badge === "STALE") return "stale";
    if (badge === "OFFLINE") return "offline";
    var legacy = item && item.quality && item.quality.status;
    if (legacy === "fresh" || legacy === "delayed") return legacy;
    return "unknown";
  }
  function describe(item) {
    var bs = badgeState(item);
    return [item.definition || item.label, "Unit: " + (item.unit || "unverified"),
      "Observed: " + (item.observed_at || item.observation_date || "unknown"),
      item.comparison_date ? "Comparison: " + item.comparison_date : "",
      item.seasonal_adjustment || "", "Source: " + (item.src || item.source || item.series_id || "unknown"),
      bs === "delayed" ? "Delayed quote" : "",
      bs === "session" ? "Last session close" : "",
      bs === "stale" ? "Stale quote" : "",
      item.evidence ? "Archived source evidence available in /data/market-tape.json" : ""].filter(Boolean).join(" · ");
  }
  function render(doc, target, packet, now) {
    target.replaceChildren();
    now = now == null ? Date.now() : now;
    var stamp = Date.parse(packet && packet.generated_at);
    /* 2026-11-26: the tape's engine has run once a day since the cadence floor of 2026-10-20, and this strip - which
       refused any packet older than fifteen minutes - read "Market observations unavailable or stale" on every page
       for all but fifteen minutes of every day. A sound packet up to four days old is now shown as what it is: a
       first chip says when it was written and how old it is, and a quote reads "last print", never as a live price.
       A missing, malformed, future-dated or older packet still reads unavailable. */
    if (!packet || packet.schema_version !== "2.0" || !Number.isFinite(stamp) || now - stamp > 345600000 || stamp - now > 300000) {
      target.textContent = "Market observations unavailable or stale";
      return;
    }
    var old = now - stamp > 900000;
    if (old) {
      var mins = Math.round((now - stamp) / 60000);
      var asof = doc.createElement("span"); asof.className = "jhc-chip jhc-stale";
      asof.setAttribute("data-asof", packet.generated_at);
      asof.textContent = "As of " + String(packet.generated_at).slice(5, 16).replace("T", " ") + "Z · " + (mins < 90 ? mins + " min" : Math.round(mins / 60) + " h") + " old";
      asof.title = "The market tape is written once a day. Each figure carries its own observation date; a quote is the last print at that time, not a live price.";
      target.appendChild(asof);
    }
    (packet.items || []).forEach(function (item) {
      if (!item || typeof item.value !== "number" || !Number.isFinite(item.value) || !item.observation_date || !item.unit) return;
      var state = badgeState(item);
      if (state === "offline" || state === "unknown") return;
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
      if (old && item.frequency === "quote" && state !== "session" && state !== "stale") date.textContent += " last print";
      else if (state === "delayed") date.textContent += " delayed";
      else if (state === "session") date.textContent += " last close";
      else if (state === "stale") { date.textContent += " stale"; sp.className += " jhc-stale"; }
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
