const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { execFileSync } = require("node:child_process");

const root = path.resolve(__dirname, "..");
const js = fs.readFileSync(path.join(root, "khalidrisk.js"), "utf8");
const html = fs.readFileSync(path.join(root, "khalidrisk.html"), "utf8");
// Run the dependency-free production producer with its explicit synthetic
// inputs. No AWS/client module is imported, and there are no network calls.
const produced = JSON.parse(execFileSync("python3", ["-c", [
  "import sys, runpy, json",
  "sys.path.insert(0, 'aws/shared')",
  "t = runpy.run_path('aws/lambdas/justhodl-khalid-risk/tests/test_risk_engine.py')",
  "feeds, metas = t['base_inputs']()",
  "out = t['build_output'](t['REGISTRY'], feeds, metas, t['NOW'])",
  "print(json.dumps(out, allow_nan=False))",
].join("\n")], { cwd: root, encoding: "utf8" }));

class Element {
  constructor(tag) { this.tagName = tag; this.children = []; this.hidden = false; this.listeners = {}; this._text = ""; this.classList = { toggle() {} }; }
  append(...children) { this.children.push(...children); }
  get firstChild() { return this.children[0]; }
  removeChild(child) { this.children.splice(this.children.indexOf(child), 1); }
  set textContent(value) { this._text = String(value); this.children = []; }
  get textContent() { return this._text + this.children.map(child => child.textContent).join(" "); }
  addEventListener(event, fn) { this.listeners[event] = fn; }
  closest() { return this; }
}

function harness() {
  const ids = new Map([...html.matchAll(/id="([^"]+)"/g)].map(match => [match[1], new Element("div")]));
  const created = [], events = {}, windowEvents = {}, timers = new Map();
  let now = Date.parse(produced.generated_at), serial = 0;
  class Clock extends Date { static now() { return now; } }
  const document = {
    hidden: false,
    getElementById(id) { assert.ok(ids.has(id), `Unknown page element: ${id}`); return ids.get(id); },
    createElement(tag) { const node = new Element(tag); created.push(node); return node; },
    addEventListener(event, fn) { events[event] = fn; },
  };
  const window = { addEventListener(event, fn) { windowEvents[event] = fn; } };
  const context = vm.createContext({ document, window, Date: Clock,
    fetch: () => new Promise(() => {}),
    setTimeout(fn, delay) { const id = ++serial; timers.set(id, { fn, delay }); return id; },
    clearTimeout(id) { timers.delete(id); },
  });
  vm.runInContext(js, context, { filename: "khalidrisk.js" });
  return { ids, created, events, windowEvents, document, timers, api: window.JustHodlRisk, advance(time) { now = time; } };
}

test("dedicated renderer exposes actual producer domains, provenance and every nested leaf", () => {
  const h = harness(), data = structuredClone(produced);
  data.fusion_context = { status: "OK", summaries: [{ evidence_id: "ev1:lineage-test", source_id: "credit", score: 41.23456789 }], vetoes: [] };
  data.future_optional_contract = { nullable: null, empty: [], nested: [{ value: "<script>must stay text</script>" }], long_note: "a".repeat(500) + "TAIL-END", rows: Array.from({ length: 200 }, (_, index) => ({ index, later_only: index === 199 ? "FINAL-ROW-FIELD" : null })) };
  assert.equal(h.api.render(data), true);
  assert.equal(h.ids.get("kr-board").hidden, false);
  const domainText = h.ids.get("kr-domains").textContent;
  const domain = data.domains.find(row => row.id === "credit_composite");
  for (const key of ["score", "state", "status", "as_of", "age_h", "artifact"]) {
    assert.ok(domainText.includes(key), `Domain metadata ${key}`);
    assert.ok(domainText.includes(String(domain[key])), `Domain value ${key}`);
  }
  for (const metric of domain.metrics) {
    assert.ok(domainText.includes(metric.label));
    assert.ok(domainText.includes(String(metric.value)));
  }
  const sourceText = h.ids.get("kr-sources").textContent;
  for (const key of ["critical", "max_age_h", "error", "freshness_basis", "producer"]) assert.ok(sourceText.includes(key), key);
  const evidence = h.ids.get("kr-evidence-fields").textContent;
  for (const text of ["policy", "critical_failures", "methodology", "fusion_context", "ev1:lineage-test", "41.23456789", "null (not supplied)", "TAIL-END", "FINAL-ROW-FIELD", "<script>must stay text</script>"]) assert.ok(evidence.includes(text), text);
  assert.equal(h.created.some(node => node.tagName === "script"), false);
  assert.deepEqual(JSON.parse(h.ids.get("kr-raw-payload").textContent), data);
  assert.ok(h.ids.get("kr-coverage").textContent.includes("Invalid"));
  assert.ok(h.ids.get("kr-coverage").textContent.includes("Unknown"));
});

test("permission expires on its timer while full evidence stays inspectable", () => {
  const h = harness(), data = structuredClone(produced);
  const expiry = Date.parse(data.generated_at) + 10_000;
  data.expires_at = new Date(expiry).toISOString();
  assert.equal(h.api.render(data), true);
  const timer = [...h.timers.values()][0];
  assert.equal(timer.delay, 10_000);
  h.advance(expiry); timer.fn();
  assert.equal(h.ids.get("kr-board").hidden, true);
  assert.equal(h.ids.get("kr-error").hidden, false);
  assert.equal(h.ids.get("kr-cap").textContent, "—");
  assert.equal(h.ids.get("kr-evidence").hidden, false);
  assert.match(h.ids.get("kr-contract-state").textContent, /UNAVAILABLE.*expired/);
  assert.deepEqual(JSON.parse(h.ids.get("kr-raw-payload").textContent), data);
});

test("render validates directly, and resume invalidates expired cached permission", () => {
  const h = harness(), bad = structuredClone(produced);
  bad.exposure_cap_pct = 100;
  assert.equal(h.api.render(bad), false);
  assert.equal(h.ids.get("kr-board").hidden, true);
  const data = structuredClone(produced);
  assert.equal(h.api.render(data), true);
  h.advance(Date.parse(data.generated_at) + 2 * 60 * 60 * 1000 + 1);
  h.events.visibilitychange();
  assert.equal(h.ids.get("kr-board").hidden, true);
  assert.match(h.ids.get("kr-error-copy").textContent, /stale/);
});
