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
  assert.doesNotMatch(js, /innerHTML\\s*=/);
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
