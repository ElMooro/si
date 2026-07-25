"""
ops_3878 — GATE: call the SERVED page's real load() function end-to-end,
against LIVE data, via a fetch() shim. WRITES NO CODE.

ops 3876 proved renderPressureAndHeatmap() works when called directly with
already-fetched data — but load() itself was modified (added the cpPromise
capture, added the final await+call), and every prior gate in this arc
called the new orchestrator directly rather than going through the real
load(). That leaves one real question unanswered: does the ACTUAL load()
function — the one that fires on every real page visit — still execute the
PRE-EXISTING sections (AI note, macro regime, composite gauges, sector
heatmap, top-10 in/outflows, full universe table) cleanly, now that it also
fires the new orchestrator at the end? A working new feature bolted onto a
broken load() would still be a regression for every existing user.

ops 3878 itself had a gap: FEEDS never included macro/regime.json, so fetchMacroRegime() 404'd against the shim and its own graceful if(!r.ok) return; correctly no-op'd — reporting macro-regime-body as 0 bytes, which looked like a regression but was a harness gap. Fixed here.\n\nThis shims global.fetch to serve the real live JSON (fetched once, held in
memory) for every CDN URL load() requests, then calls load() itself and
checks that EVERY section — old and new — populated.
"""
import json
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

PAGE = "https://justhodl.ai/flows.html"
CDN = "https://justhodl-data-proxy.raafouis.workers.dev"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

FEEDS = {
    "etf-flows/daily.json": "/tmp/l_daily.json",
    "etf-flows/composite.json": "/tmp/l_composite.json",
    "etf-flows/rotation.json": "/tmp/l_rotation.json",
    "etf-flows/ai-analysis.json": "/tmp/l_ai.json",
    "etf-flows/constituent-pressure.json": "/tmp/l_cp.json",
    # ops 3878 forgot this one -> fetchMacroRegime() 404'd against the shim
    # and correctly no-op'd (if(!r.ok) return;), reporting a FALSE regression
    # on macro-regime-body. Added here, not a page-code issue.
    "macro/regime.json": "/tmp/l_macro.json",
}


def get(url, tries=3):
    for i in range(tries):
        try:
            req = urllib.request.Request(f"{url}{'&' if '?' in url else '?'}v={int(time.time())}-{i}",
                                          headers={"User-Agent": UA, "Cache-Control": "no-cache"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read()
        except Exception:
            time.sleep(4)
    return None


HARNESS = r'''
const fs = require('fs');
function makeEl(tag) {
  const el = {
    tag, _html: '', style: {}, dataset: {}, children: [], _listeners: {},
    classList: { _set: new Set(), add(c){this._set.add(c)}, remove(c){this._set.delete(c)}, contains(c){return this._set.has(c)} },
    set innerHTML(v){ this._html = v; }, get innerHTML(){ return this._html; },
    set textContent(v){ this._html = String(v); }, get textContent(){ return this._html; },
    appendChild(c){ this.children.push(c); },
    addEventListener(evt, fn){ (this._listeners[evt] = this._listeners[evt] || []).push(fn); },
    querySelectorAll(){ return []; },
  };
  return el;
}
const store = {};
function el(id){ return store[id] || (store[id] = makeEl('div')); }
global.document = { getElementById: id => el(id), createElement: tag => makeEl(tag), querySelectorAll: () => [] };

const feedMap = JSON.parse(fs.readFileSync('/tmp/feedmap.json', 'utf8'));
global.fetch = async (url) => {
  const hit = Object.keys(feedMap).find(k => url.includes(k));
  if (!hit) return { ok: false, status: 404, json: async () => ({}) };
  const body = JSON.parse(fs.readFileSync(feedMap[hit], 'utf8'));
  return { ok: true, status: 200, json: async () => body };
};

const src = fs.readFileSync('/tmp/live_page_full.js', 'utf8');
let thrown = null;
try {
  eval(src);   // this INCLUDES the real trailing `load();` call — the actual page bootstrap
} catch (e) { thrown = e; }
// load() is async and fires more async work internally; give it a moment
setTimeout(() => {
  console.log('THREW', thrown ? thrown.stack.replace(/\n/g, ' | ') : 'null');
  const ids = ['ai-body', 'macro-regime-body', 'constituent-pressure-body', 'sector-heatmap',
               'top-inflows', 'top-outflows', 'full-table', 'buying-board', 'selling-board',
               'unified-heatmap', 'master-table', 'meta-bar'];
  for (const id of ids) console.log('BYTES_' + id, el(id).innerHTML.length);
  console.log('DONE');
}, 3000);
'''


def main():
    with report("3879_load_end_to_end_gate_recheck") as rep:
        rep.heading("ops 3879 — GATE: real load() end-to-end vs LIVE data (fetch-shimmed)")

        rep.section("1. pull the served page and extract the FULL inline script (load(); included)")
        html = get(PAGE)
        if not html:
            rep.fail("  could not fetch the served page")
            sys.exit(1)
        html_s = html.decode("utf-8", "ignore")
        if "JH_FLOWS_PRESSURE_3873" not in html_s:
            rep.fail("  version marker absent — stale CDN copy")
            sys.exit(1)
        m = re.search(r"<script>(.*?)</script>", html_s, re.S)
        if not m:
            rep.fail("  inline <script> not found")
            sys.exit(1)
        Path("/tmp/live_page_full.js").write_text(m.group(1))
        rep.ok(f"  {len(m.group(1)):,} chars of inline script extracted, load(); call intact")

        rep.section("2. pull every live feed load() actually requests")
        feedmap = {}
        for key, path in FEEDS.items():
            raw = get(f"{CDN}/{key}")
            if not raw:
                rep.fail(f"  {key} unreachable")
                sys.exit(1)
            Path(path).write_bytes(raw)
            feedmap[key] = path
            rep.ok(f"  {key}: {len(raw):,} bytes")
        Path("/tmp/feedmap.json").write_text(json.dumps(feedmap))

        rep.section("3. run the REAL load() through a fetch shim, wait for async completion")
        Path("/tmp/gate3878.js").write_text(HARNESS)
        proc = subprocess.run(["node", "/tmp/gate3878.js"], capture_output=True, text=True, timeout=30)
        rep.log(proc.stdout)
        if proc.returncode != 0:
            rep.fail(f"  node exited {proc.returncode}\n{proc.stderr[-1500:]}")
            sys.exit(1)

        out = {}
        for line in proc.stdout.splitlines():
            parts = line.split(" ", 1)
            if len(parts) == 2:
                out[parts[0]] = parts[1]

        rep.section("4. gate — EVERY section, old and new, must have populated")
        old_sections = ["ai-body", "macro-regime-body", "constituent-pressure-body",
                        "sector-heatmap", "top-inflows", "top-outflows", "full-table", "meta-bar"]
        new_sections = ["buying-board", "selling-board", "unified-heatmap", "master-table"]
        checks = [("no exception thrown by the real load()", out.get("THREW") == "null")]
        for sid in old_sections:
            n = int(out.get(f"BYTES_{sid}", "0"))
            checks.append((f"PRE-EXISTING section '{sid}' populated ({n} bytes)", n > 0))
        for sid in new_sections:
            n = int(out.get(f"BYTES_{sid}", "0"))
            checks.append((f"NEW section '{sid}' populated ({n} bytes)", n > 0))
        for label, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {label}")

        failed = [l for l, ok in checks if not ok]
        if failed:
            rep.fail(f"FAILED {len(failed)}: {failed}")
            sys.exit(1)
        rep.ok("PASS_ALL — the real load() executes every section, pre-existing and new, cleanly against live data")


if __name__ == "__main__":
    main()
