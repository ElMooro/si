const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const root = path.resolve(__dirname, "..");
const html = fs.readFileSync(path.join(root, "index.html"), "utf8");
const js = fs.readFileSync(path.join(root, "home.js"), "utf8");
const css = fs.readFileSync(path.join(root, "home.css"), "utf8");

test("homepage builder is a guided four-step composer", () => {
  assert.match(html, /data-kind="engine"/);
  assert.match(html, /data-kind="page"/);
  assert.match(html, /Search and select a source/);
  assert.match(js, /Exact output feed/);
  assert.ok(js.includes("Individual fields / table columns"));
  assert.match(html, /LIVE PREVIEW/);
  assert.match(html, /Block title/);
  assert.match(html, /Width/);
});

test("advanced address input remains available for legacy workflows", () => {
  assert.match(html, /<details class="hd-advanced">/);
  assert.match(html, /id="hd-form"/);
  assert.match(html, /id="hd-input"/);
  assert.match(js, /function parseAddress/);
});

test("schema v3 persists exact fields and columns while accepting schema 1 and 2", () => {
  assert.match(js, /var SCHEMA = 3/);
  assert.match(js, /Array\.isArray\(b\.fields\)/);
  assert.match(js, /Array\.isArray\(b\.columns\)/);
  assert.match(js, /cloud\.schema === 2/);
  assert.match(js, /cloud\.schema === 1 \? migrateV1\(cloud\) : safeState\(cloud\)/);
  assert.match(js, /fields: composer\.fields\.slice\(\)/);
  assert.match(js, /columns: composer\.columns\.slice\(\)/);
  assert.match(js, /titleLocked: b\.titleLocked === true/);
});

test("renderers honor selected fields and columns and keep sort null-last", () => {
  assert.match(js, /genericTable\(v, lim, b\.columns, b\)/);
  assert.match(js, /kpiGrid\(v, b\.fields\)/);
  assert.match(js, /aNull \? 1 : -1/);
  assert.match(js, /data-col-sort/);
  assert.match(js, /aria-sort/);
});

test("authoritative risk and fusion feeds are visible and risk can override local calm", () => {
  assert.match(js, /\/data\/engine-fusion\.json/);
  assert.match(js, /\/data\/khalid-risk\.json/);
  assert.match(html, /AUTHORITATIVE CAPITAL DECISION/);
  assert.match(html, /Fusion coverage/);
  assert.match(html, /Disagreements/);
  assert.match(html, /Stale \/ missing/);
  assert.match(js, /Capital control unavailable/);
  assert.match(js, /validateRiskArtifact/);
  assert.match(js, /validateFusionArtifact/);
  assert.match(js, /Local widget heartbeat cannot override/);
  assert.match(js, /coverage\.freshness\.stale/);
  assert.match(js, /coverage\.freshness\.missing/);
});

test("homepage has explicit auth and Khalid navigation with mobile composer CSS", () => {
  assert.match(html, /id="jh-auth-slot"\s+data-auth-slot/);
  assert.match(html, /href="\/khalid\.html"/);
  assert.match(html, /href="\/khalidrisk\.html"/);
  assert.match(html, /class="hd-skip"/);
  assert.match(css, /@media \(max-width: 560px\)/);
  assert.match(css, /@media \(max-width: 520px\)/);
});
