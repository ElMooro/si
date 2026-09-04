const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const html = fs.readFileSync(path.join(root, "khalidrisk.html"), "utf8");
const js = fs.readFileSync(path.join(root, "khalidrisk.js"), "utf8");
const css = fs.readFileSync(path.join(root, "khalidrisk.css"), "utf8");

test("risk page loads the canonical artifact with repository proxy fallback", () => {
  assert.match(js, /\/data\/khalid-risk\.json/);
  assert.match(js, /justhodl-data-proxy/);
  assert.match(js, /Malformed risk artifact/);
  assert.match(html, /id="kr-error"/);
  assert.match(html, /id="kr-retry"/);
});

test("capital control is first and includes cap, mode, freshness, vetoes and tighteners", () => {
  const decisionAt = html.indexOf("AUTHORITATIVE CAPITAL DECISION");
  const domainsAt = html.indexOf("INDEPENDENT RISK DOMAINS");
  assert.ok(decisionAt > -1 && decisionAt < domainsAt);
  for (const id of ["kr-decision", "kr-cap", "kr-mode", "kr-vetoes", "kr-tighteners", "kr-coverage"]) {
    assert.match(html, new RegExp(`id="${id}"`));
  }
});

test("Treasury deliver, receive and combined fails expose required statistics", () => {
  assert.match(js, /FAILS TO DELIVER/);
  assert.match(js, /FAILS TO RECEIVE/);
  assert.match(js, /COMBINED FAILS/);
  assert.match(js, /Percentile/);
  assert.match(js, /Z-score/);
  assert.match(js, /Regime/);
  assert.match(js, /As of/);
  assert.match(js, /ftd_bn/);
  assert.match(js, /ftr_bn/);
  assert.match(js, /gross_bn/);
  assert.match(js, /USD_bn_par/);
});

test("risk page includes domains, source health, conflicts and explicit non-advice wording", () => {
  assert.match(html, /id="kr-domains"/);
  assert.match(html, /SOURCE HEALTH/);
  assert.match(html, /CONFLICTS \+ DISAGREEMENTS/);
  assert.match(html, /not personalized investment advice/);
  assert.match(html, /Missing data is not positive evidence/);
});

test("dynamic risk values are written through textContent and mobile CSS targets 390px", () => {
  assert.doesNotMatch(js, /innerHTML\s*=/);
  assert.match(js, /textContent/);
  assert.match(css, /@media \(max-width: 390px\)/);
  assert.match(html, /class="kr-skip"/);
  assert.match(html, /aria-live="polite"/);
});
