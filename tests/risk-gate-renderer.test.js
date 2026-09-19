const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { execFileSync } = require("node:child_process");
const root = path.resolve(__dirname, "..");
const html = fs.readFileSync(path.join(root, "docs/archive/risk-gate-v2.html"), "utf8");
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

test("retained legacy Risk Gate output exposes live ACM and scoped Treasury evidence on the page", () => {
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


test("Risk Gate shows heuristic limits and never converts a missing event return to zero", () => {
  const { context, nodes } = harness();
  context.renderPosture({posture:'RISK_OFF',composite:-.5,sizing_multiplier:.45});
  const posture=nodes.get('postureBlock').innerHTML;
  assert.ok(posture.includes('p-RISK_OFF'));
  assert.ok(posture.includes('Legacy heuristic multiplier'));
  assert.ok(posture.includes('unvalidated; no allocation recommendation'));
  context.renderES({event_study:{flips:[{date:'2026-01-01',posture:'RISK_OFF',spx_fwd_21d_pct:null},
    {date:'2026-02-01',posture:'RISK_OFF',spx_fwd_21d_pct:0}],avg_spx_fwd_21d_while_risk_off_pct:null,spx_baseline_fwd_21d_pct:null}});
  assert.ok(nodes.get('esChart').innerHTML.includes('2026-01-01: forward window unavailable'));
  assert.ok(!nodes.get('esChart').innerHTML.includes('2026-01-01 · flip to'));
  assert.ok(nodes.get('esChart').innerHTML.includes('SPX 21 union-calendar rows: 0.00%'));
  const note=nodes.get('esNote').innerHTML;
  assert.ok(note.includes('not point-in-time, out-of-sample'));
  assert.ok(!note.includes('gate adds value'));
  assert.ok(html.includes('jh-ciss-readthrough.js?v='));
  assert.ok(!html.includes('calibrated fleet inputs weighted by historical hit rate'));
});
