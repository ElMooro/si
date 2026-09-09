const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { execFileSync } = require("node:child_process");
const root = path.resolve(__dirname, "..");
const html = fs.readFileSync(path.join(root, "risk-gate.html"), "utf8");
const script = [...html.matchAll(/<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/g)].map(match => match[1]).find(code => code.includes("const FEEDS"));
const payload = JSON.parse(execFileSync("python3", ["-c", [
  "import json, runpy, contextlib, io",
  "with contextlib.redirect_stdout(io.StringIO()):",
  " t = runpy.run_path('aws/lambdas/justhodl-risk-gate/tests/run_tests.py')",
  " response, writes, reads = t['_handler_fixture'](t['_load']())",
  "print(json.dumps(writes['data/risk-gate.json'], allow_nan=False))",
].join("\n")], { cwd: root, encoding: "utf8" }));

function harness() {
  const nodes = new Map([...html.matchAll(/id="([^"]+)"/g)].map(match => [match[1], { innerHTML: "", textContent: "" }]));
  const context = vm.createContext({ document: { getElementById(id) { assert.ok(nodes.has(id), id); return nodes.get(id); } },
    fetch: () => new Promise(() => {}), Date, console });
  vm.runInContext(script, context, { filename: "risk-gate.html" });
  return { context, nodes };
}

test("actual Risk Gate output exposes live ACM and scoped Treasury evidence on the page", () => {
  const { context, nodes } = harness();
  context.renderLegs(payload); context.renderIndicators(payload); context.renderFleet(payload); context.renderCompleteEvidence(payload);
  const indicator = nodes.get("indicators").innerHTML;
  for (const text of ["0.70 pp", "data/term-premium.json", "tp5", "rn10", "120", "source"]) assert.ok(indicator.includes(text), text);
  const fleet = nodes.get("fleet").innerHTML;
  for (const text of ["treasury_fails_gross_z", "data/settlement-fails.json", "US_TREASURY_INCLUDING_TIPS", "USD_bn_par", "ftd_bn", "ftr_bn", "gross_bn", "240", "observation_time"]) assert.ok(fleet.includes(text), text);
  assert.ok(nodes.get("legs").innerHTML.includes("Funding &amp; Plumbing"));
  assert.ok(!nodes.get("legs").innerHTML.includes("weight —"));
  const complete = nodes.get("completeEvidence").innerHTML;
  for (const key of Object.keys(payload)) assert.ok(complete.includes(key), key);
});

test("fleet rendering preserves nulls, nested numbers, long notes and rows after 80", () => {
  const { context, nodes } = harness();
  const inputs = Object.fromEntries(Array.from({ length: 120 }, (_, index) => ["row_" + index,
    { value: index === 0 ? { jpy_z: -2.1, eur_z: 0.6 } : index === 1 ? null : index,
      status: "STALE", age_h: 300, max_age_h: 72, delta: 0, feed: "data/test.json",
      note: "a".repeat(160) + "TAIL-NOTE-" + index, extra: index === 119 ? "<script>display as text</script>" : false }]));
  context.renderFleet({ fleet_context: { inputs, method: "fixture" } });
  const rendered = nodes.get("fleet").innerHTML;
  for (const text of ["-2.1", "0.6", "null (not supplied)", "row_119", "TAIL-NOTE-119", "data/test.json", "STALE", "300h", "&lt;script&gt;display as text&lt;/script&gt;"]) assert.ok(rendered.includes(text), text);
  assert.ok(!rendered.includes("[object Object]"));
  assert.ok(!rendered.includes("<script>"));
});
