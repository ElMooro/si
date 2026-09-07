// Regression tests for the QA audit of 2026-09-07 (branch qa/2026-09-07-audit).
// Confirmed live: /config/engine-contracts.json 404 on the homepage, engines.html baked from a registry with no
// writer since 2026-07-07, and the nav-drawer diag beacon failing CORS on every page.
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { execFileSync } = require("node:child_process");

const root = path.resolve(__dirname, "..");
const read = (p) => fs.readFileSync(path.join(root, p), "utf8");

test("pages build ships config/engine-contracts.json so home.js stops 404ing on it", () => {
  assert.match(read("scripts/bake_sections.py"), /"engine-contracts\.json"\)/);
  assert.match(read("home.js"), /\/config\/engine-contracts\.json/);
});

test("home.js reads the deploy-fresh engine registry before the July S3 document", () => {
  const js = read("home.js");
  const i = js.indexOf('"/config/engine-registry.json"'), j = js.indexOf('"/data/engine-registry.json"');
  assert.ok(i > 0 && j > i, "config copy must be tried first");
});

test("engine directory is baked from the deploy-time manifest unioned with the registry (never stale on its own)", () => {
  const py = read("scripts/bake_engine_directory.py");
  assert.match(py, /def load_entries/);
  assert.match(py, /engine-manifest\.json/);
  assert.match(py, /config\/engine-registry\.json/);
  // run the union against the repo's own manifest, no network (registry fetch fails closed to {})
  const script = `
import sys, json; sys.path.insert(0, ${JSON.stringify(path.join(root, "scripts"))})
import bake_engine_directory as b
b.get = lambda url, to=12: (None, b"", {})
entries, asof = b.load_entries(${JSON.stringify(root)})
print(json.dumps({"n": len(entries), "fusion": "justhodl-jh-fusion" in entries, "katlin": "justhodl-katlin" in entries, "asof": asof}))`;
  const out = JSON.parse(execFileSync("python3", ["-c", script], { encoding: "utf8" }).trim().split("\n").pop());
  assert.ok(out.n >= 800, "manifest union must cover the fleet, got " + out.n);
  assert.equal(out.fusion, true);
  assert.equal(out.katlin, true);
  assert.equal(out.asof, null, "registry unreachable -> no as_of, no crash");
});

test("nav-drawer diag beacon is fire-and-forget (no-cors) so it cannot raise a CORS error on every page", () => {
  const js = read("jh-nav-drawer.js");
  const i = js.indexOf("?diag=1");
  assert.ok(i > 0);
  const call = js.slice(i, i + 260);
  assert.match(call, /mode:'no-cors'|mode:"no-cors"/);
  assert.match(call, /keepalive:true/);
});

test("static dependency map builder runs and separates referenced-by-code from loaded/displayed", () => {
  const py = read("scripts/build_dependency_map.py");
  assert.match(py, /REFERENCED BY CODE/);
  assert.match(py, /orphan_page_refs/);
  assert.match(py, /duplicate_writers/);
});
