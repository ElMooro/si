const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const html = fs.readFileSync(path.join(root, "khalid.html"), "utf8");
const js = fs.readFileSync(path.join(root, "khalid.js"), "utf8");
const css = fs.readFileSync(path.join(root, "khalid.css"), "utf8");

test("Khalid page has the required stable sections", () => {
  for (const key of ["command", "opportunities", "assets", "risk", "method"]) {
    assert.match(html, new RegExp(`data-jh-key="${key}"`));
  }
  assert.match(html, /data-jh-axis/);
});

test("Khalid loads the canonical artifact with the production fallback", () => {
  assert.ok(js.includes("/data/khalid.json"));
  assert.match(js, /justhodl-data-proxy/);
});

test("Khalid renders the broad opportunity radar and lifecycle changes", () => {
  assert.match(js, /opportunity_radar/);
  assert.match(js, /opportunity_changes/);
  assert.match(html, /GLOBAL OPPORTUNITY RADAR/);
  assert.match(html, /data-filter="HIGH_CONVICTION"/);
  assert.match(html, /data-filter="EVIDENCE_HOLD"/);
  assert.match(js, /\["DATA HOLD", changeData\.held\]/);
});

test("Dynamic feed values use DOM text nodes rather than HTML interpolation", () => {
  assert.doesNotMatch(js, /innerHTML\s*=/);
  assert.match(js, /textContent/);
});

test("The dashboard exposes responsive mobile layouts", () => {
  assert.ok(css.includes("@media (max-width: 760px)"));
  assert.ok(css.includes("@media (max-width: 390px)"));
});

test("The closed detail drawer is inert and restores focus to its opener", () => {
  assert.match(html, /id="detail"[^>]*aria-hidden="true"[^>]*inert/);
  assert.match(js, /detailReturnFocus/);
  assert.match(js, /detail\.inert = false/);
  assert.match(js, /detail\.inert = true/);
  assert.match(js, /state\.detailReturnFocus\.focus\(\)/);
});

test("Opportunity table sorting is accessible, bidirectional, and null-last", () => {
  assert.match(html, /data-sort="criteria"/);
  assert.match(html, /aria-sort="descending"/);
  assert.match(js, /setAttribute\("aria-sort"/);
  assert.match(js, /state\.sortDirection === "asc" \? "desc" : "asc"/);
  assert.match(js, /aNull \? 1 : -1/);
});

test("Opportunity controls expose requested filters and pagination", () => {
  assert.match(html, /id="asset-class-filter"/);
  assert.match(html, /id="industry-filter"/);
  assert.match(html, /id="cap-filter"/);
  assert.match(html, /id="page-prev"/);
  assert.match(html, /id="page-next"/);
  assert.match(js, /state\.pageSize: 25|pageSize: 25/);
  assert.match(js, /rows\.slice\(0, 9\)/);
});

test("Full risk board and breadth clusters are rendered", () => {
  assert.match(html, /id="risk-board-domains"/);
  assert.match(html, /id="board-conflicts"/);
  assert.match(html, /id="breadth-clusters"/);
  assert.match(js, /board\.domains/);
  assert.match(js, /data\.breadth_clusters/);
  assert.match(html, /Breadth never creates entry readiness/);
});

test("Capital decision is prominent and reconciles exposure cap", () => {
  assert.match(html, /AUTHORITATIVE CAPITAL DECISION/);
  assert.match(html, /id="capital-decision"/);
  assert.match(html, /id="exposure-cap"/);
  assert.match(js, /risk_board\.capital_decision/);
  assert.match(js, /risk_board\.exposure_cap_pct/);
});

test("Opportunity drawer includes full criteria, momentum, dump risk and reward-risk detail", () => {
  assert.match(js, /CLASSIFICATION \+ MOMENTUM/);
  assert.match(js, /CRITERIA \+ GATES/);
  assert.match(js, /STRUCTURAL DUMP-RISK ESTIMATE/);
  assert.match(js, /THESIS RISKS/);
  assert.match(js, /RISK \/ REWARD/);
  assert.match(html, /role="dialog"[^>]*aria-modal="true"/);
});
