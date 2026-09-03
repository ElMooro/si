const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const path = require("node:path");

function loadWorkspaceHelpers() {
  const listeners = {};
  const context = {
    console,
    Map,
    Set,
    Date,
    Intl,
    URL,
    CSS: { escape: String },
    fetch: async () => { throw new Error("not used"); },
    localStorage: { getItem() { return null; }, setItem() {} },
    document: { getElementById() { return null; }, addEventListener() {} },
    window: {
      JUSTHODL_AUTH_CONFIG: {},
      matchMedia() { return { matches: false }; },
      addEventListener(name, callback) { listeners[name] = callback; },
    },
  };
  context.window.window = context.window;
  vm.createContext(context);
  const source = fs.readFileSync(path.join(__dirname, "..", "workspace.js"), "utf8");
  vm.runInContext(source, context);
  return context.window.EngineWorkspace;
}

test("joins the 851-engine manifest with richer runtime registry records", () => {
  const helpers = loadWorkspaceHelpers();
  const manifest = {
    engines: [
      { engine: "alpha", keys: ["data/alpha.json"], description: "manifest copy" },
      { engine: "beta", keys: [], description: "second engine" },
    ],
  };
  const registry = {
    engines: {
      alpha: { doc: "registry copy", outs: ["data/alpha-detail.json"] },
      gamma: { doc: "registry only", outs: ["data/gamma.json"] },
    },
  };
  const result = helpers.normalizeEngines(registry, manifest);
  assert.deepEqual(Array.from(result, (engine) => engine.name), ["alpha", "beta", "gamma"]);
  assert.deepEqual(Array.from(result[0].feeds), ["data/alpha-detail.json", "data/alpha.json"]);
  assert.equal(result[0].description, "registry copy");
  assert.match(result[0].search, /alpha detail/);
});

test("workspace schema enforces the explicit 500-card client/server cap", () => {
  const helpers = loadWorkspaceHelpers();
  assert.equal(helpers.validateWorkspace({
    schema: 1,
    cards: [{ id: "one", engine: "alpha", zone: "analysis" }],
  }), true);
  assert.equal(helpers.validateWorkspace({
    schema: 1,
    cards: [{ id: "one", engine: "alpha", zone: "not-a-zone" }],
  }), false);
  assert.equal(helpers.validateWorkspace({
    schema: 1,
    cards: Array.from({ length: 501 }, (_, index) => ({ id: String(index), engine: "alpha", zone: "analysis" })),
  }), false);
  assert.equal(helpers.maxCards, 500);
});

test("field extraction is bounded and value traversal handles nested output", () => {
  const helpers = loadWorkspaceHelpers();
  const data = {
    generated_at: "2026-09-03T18:00:00Z",
    composite: { posture: "DEFENSIVE", score: 61 },
    ranked: [{ ticker: "BIL", score: 72 }],
  };
  assert.deepEqual(Array.from(helpers.dataPaths(data)), [
    "generated_at",
    "composite.posture",
    "composite.score",
    "ranked.ticker",
    "ranked.score",
  ]);
  assert.equal(helpers.valueAt(data, "composite.posture"), "DEFENSIVE");
  assert.deepEqual(Array.from(helpers.valueAt({
    ranked: [{ metrics: { score: 72 } }, { metrics: { score: 65 } }],
  }, "ranked.metrics.score")), [72, 65]);
  assert.deepEqual(Array.from(helpers.dataPaths({
    ranked: [{ ticker: "BIL", metrics: { score: 72, risk: { level: "low" } } }],
  })), ["ranked.ticker", "ranked.metrics.score", "ranked.metrics.risk.level"]);
});

test("nested table discovery retains its base path and reads relative nested columns", () => {
  const helpers = loadWorkspaceHelpers();
  const data = {
    meta: { generated_at: "now" },
    ranked: [
      { ticker: "BIL", metrics: { score: 72 } },
      { ticker: "SGOV", metrics: { score: 68 } },
    ],
  };
  const table = helpers.findTable(data, ["ranked.ticker", "ranked.metrics.score"]);
  assert.equal(table.basePath, "ranked");
  assert.deepEqual(Array.from(helpers.tableColumns(table, ["ranked.ticker", "ranked.metrics.score"])), [
    "ticker",
    "metrics.score",
  ]);
  assert.equal(helpers.valueAt(table.rows[1], "metrics.score"), 68);
});

test("Worker home workspace route is verified, transactionally coordinated, and capped", () => {
  const source = fs.readFileSync(
    path.join(__dirname, "..", "cloudflare", "workers", "justhodl-data-proxy", "src", "index.js"),
    "utf8",
  );
  const route = source.slice(source.indexOf('if (url.pathname === "/workspace/home")'), source.indexOf("// GET /plan/self"));
  assert.match(route, /await verifySupabaseUser\(\)/);
  assert.match(source, /export class WorkspaceCoordinator/);
  assert.match(source, /storage\.transaction/);
  assert.match(route, /WORKSPACE_COORDINATOR\.idFromName\(workspaceUid\)/);
  assert.match(route, /> 500000/);
  assert.match(route, /body\.cards\.length > 500/);
  assert.match(source, /incoming\.baseRevision !== currentRevision/);
  assert.match(source, /incoming\.revision <= currentRevision/);
  assert.match(source, /jsonResp\(\{ error: "revision conflict"/);
  assert.doesNotMatch(route, /searchParams\.get\(["']uid/);
});

test("workspace markup provides the inline auth host and accessible reorder handle", () => {
  const html = fs.readFileSync(path.join(__dirname, "..", "index.html"), "utf8");
  assert.match(html, /id="jh-auth-slot"\s+data-auth-slot/);
  assert.match(html, /aria-label="Reorder card\. Use arrow keys to move, or Space to grab\."/);
});

test("workspace implementation scopes persistence and supports continuation browsing", () => {
  const source = fs.readFileSync(path.join(__dirname, "..", "workspace.js"), "utf8");
  assert.match(source, /STORAGE_PREFIX/);
  assert.match(source, /"account:" \+ String\(user\.id\)/);
  assert.match(source, /"anonymous:" \+ id/);
  assert.match(source, /libraryLimit \+= PAGE_SIZE/);
  assert.match(source, /pointerdown/);
  assert.match(source, /event\.key === "ArrowLeft"/);
});
