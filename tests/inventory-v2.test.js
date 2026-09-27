const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
function load() {
  const ctx = { window: {}, module: { exports: {} }, console };
  ctx.globalThis = ctx.window;
  vm.createContext(ctx);
  vm.runInContext(fs.readFileSync(path.join(__dirname, "..", "jh-inventory-v2.js"), "utf8"), ctx);
  return ctx.window.jhInventoryV2;
}
test("RM/WIP/FG stay null when untagged", () => {
  const c = load().compositionFromRow({ ticker: "MU", dio_chg_pct: -11 });
  assert.equal(c.raw, null); assert.equal(c.wip, null); assert.equal(c.fg, null);
});
test("refiners are COMMODITY not SHORTAGE", () => {
  const fs = load().fourState({ dio_chg_pct: -29, rev_growth_yoy: 50, industry: "Oil & Gas Refining & Marketing" }, { book: "UP", accelerating: true }, { direction: "UP" });
  assert.equal(fs.state, "COMMODITY");
});
test("DIO down + book up is SHORTAGE", () => {
  const fs = load().fourState({ dio_chg_pct: -14, rev_growth_yoy: 27, industry: "Semiconductors" }, { book: "UP", accelerating: true }, {});
  assert.equal(fs.state, "SHORTAGE");
});
test("DIO down + demand down is DESTOCK", () => {
  const fs = load().fourState({ dio_chg_pct: -12, rev_growth_yoy: -4, industry: "Retail" }, { book: "DOWN" }, { direction: "DOWN" });
  assert.equal(fs.state, "DESTOCK");
});
