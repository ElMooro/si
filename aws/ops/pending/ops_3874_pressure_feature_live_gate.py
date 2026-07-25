"""
ops_3874 — GATE: run the SERVED flows.html's own inline script, unmodified,
against LIVE etf-flows/daily.json + etf-flows/constituent-pressure.json, inside
a Node+DOM shim. This is the check that matters most: local synthetic tests
(harness_pressure_heatmap.js, 8/8 pass) prove the LOGIC is correct, but only
running the real served bytes against real live data can catch a real-shape
mismatch — the exact class of bug (wrong field name, wrong container, wrong
units) that has hit this fleet repeatedly. Clones the proven pattern from ops
3855/3861 (which caught the nav-drawer scoping bug and the two wrong
constituent-pressure reads respectively).

Runs entirely in run-ops.yml's Node environment — no code is written, this is
a pass/fail proof against production.
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


def get(url, tries=3):
    for i in range(tries):
        try:
            req = urllib.request.Request(
                f"{url}{'&' if '?' in url else '?'}v={int(time.time())}-{i}",
                headers={"User-Agent": UA, "Cache-Control": "no-cache"})
            with urllib.request.urlopen(req, timeout=60) as r:
                return r.read()
        except Exception:
            time.sleep(5)
    return None


HARNESS = r'''
const fs = require('fs');
function makeEl(tag) {
  const el = {
    tag, _html: '', _value: '', style: {}, dataset: {}, children: [], _listeners: {},
    classList: { _set: new Set(), add(c){this._set.add(c)}, remove(c){this._set.delete(c)}, contains(c){return this._set.has(c)} },
    set innerHTML(v){ this._html = v; this._reindexTh(); }, get innerHTML(){ return this._html; },
    set textContent(v){ this._html = String(v); }, get textContent(){ return this._html; },
    get value(){ return this._value; }, set value(v){ this._value = v; },
    appendChild(c){ this.children.push(c); },
    addEventListener(evt, fn){ (this._listeners[evt] = this._listeners[evt] || []).push(fn); },
    click(){ (this._listeners['click']||[]).forEach(fn => fn.call(this)); },
    dispatch(evt){ (this._listeners[evt]||[]).forEach(fn => fn.call(this)); },
    _reindexTh(){
      this._thByKey = {};
      const re = /<th class="sortable[^"]*" data-key="([a-z]+)"/g; let m;
      while ((m = re.exec(this._html))) { if(!this._thByKey[m[1]]) { const th = makeEl('th'); th.dataset.key = m[1]; this._thByKey[m[1]] = th; } }
    },
    querySelectorAll(sel){ return sel === 'th.sortable' ? Object.values(this._thByKey||{}) : []; },
  };
  return el;
}
const store = {};
function el(id){ return store[id] || (store[id] = makeEl('div')); }
function sel(id, v){ const n = makeEl('select'); n._value = v; store[id] = n; return n; }
sel('flt-type','ALL'); sel('flt-sector','ALL'); sel('flt-leveraged','ALL'); sel('flt-region','ALL');
const cadDaily = makeEl('button'); cadDaily.dataset.cad='daily';
const cadWeekly = makeEl('button'); cadWeekly.dataset.cad='weekly'; cadWeekly.classList.add('active');
const cadMonthly = makeEl('button'); cadMonthly.dataset.cad='monthly';
global.document = {
  getElementById: id => el(id), createElement: tag => makeEl(tag),
  querySelectorAll: s => s === '#pressure-cadence button' ? [cadDaily,cadWeekly,cadMonthly] : [],
};

const daily = JSON.parse(fs.readFileSync('/tmp/live_daily.json', 'utf8'));
const cpDoc = JSON.parse(fs.readFileSync('/tmp/live_cp.json', 'utf8'));

const src = fs.readFileSync('/tmp/live_page.js', 'utf8');
const trimmed = src.replace(/\nload\(\);\s*$/, '\n');
let thrown = null;
try {
  eval(trimmed);
  renderPressureAndHeatmap(daily, cpDoc);
} catch (e) { thrown = e; }
console.log('THREW', thrown ? thrown.stack.replace(/\n/g,' | ') : 'null');
if (thrown) process.exit(1);

const nStocks = Object.keys(cpDoc.per_stock_exposure || {}).length;
const nEtfs = (daily.metrics || []).length;
console.log('INPUT_COUNTS', JSON.stringify({ nStocks, nEtfs }));

console.log('BUYING_BYTES', el('buying-board').innerHTML.length);
console.log('SELLING_BYTES', el('selling-board').innerHTML.length);
console.log('HEATMAP_BYTES', el('unified-heatmap').innerHTML.length);
console.log('MASTER_BYTES', el('master-table').innerHTML.length);
for (const id of ['pressure-section','unified-heatmap-section','master-table-section']) {
  console.log('DISPLAY_' + id, JSON.stringify(el(id).style.display));
}
console.log('MASTER_COUNT_TEXT', JSON.stringify(el('master-count').innerHTML));

// real interaction proof against REAL data
const monthlyTh = el('master-table')._thByKey['monthly'];
if (monthlyTh) { monthlyTh.click(); console.log('SORT_CLICK_OK', true); } else { console.log('SORT_CLICK_OK', false); }

const buyBefore = el('buying-board').innerHTML;
cadDaily.click();
console.log('CADENCE_SWITCH_CHANGED_CONTENT', buyBefore !== el('buying-board').innerHTML);

store['flt-type']._value = 'STOCK'; store['flt-type'].dispatch('change');
console.log('TYPE_STOCK_ETF_LEAK', (el('master-table').innerHTML.match(/tag etf/g)||[]).length);
store['flt-type']._value = 'ALL'; store['flt-type'].dispatch('change');

store['flt-leveraged']._value = 'Y'; store['flt-leveraged'].dispatch('change');
console.log('LEV_Y_STOCK_LEAK', (el('master-table').innerHTML.match(/tag stock/g)||[]).length);
store['flt-leveraged']._value = 'ALL'; store['flt-leveraged'].dispatch('change');

const finalHtml = el('master-table').innerHTML + el('buying-board').innerHTML + el('selling-board').innerHTML + el('unified-heatmap').innerHTML;
const badTokens = ['>null<','>NaN<','>undefined<','NaN%','NaNσ'];
console.log('BAD_TOKENS_FOUND', JSON.stringify(badTokens.filter(t => finalHtml.includes(t))));

// sample a few real rows for a human-readable spot check
const rowMatch = el('master-table').innerHTML.match(/<tbody>([\s\S]*)<\/tbody>/);
const firstRows = rowMatch ? (rowMatch[1].match(/<tr>[\s\S]*?<\/tr>/g)||[]).slice(0,3) : [];
console.log('SAMPLE_ROWS_JSON', JSON.stringify(firstRows));
'''


def main():
    with report("3874_pressure_feature_live_gate") as rep:
        rep.heading("ops 3874 — GATE: served flows.html script vs LIVE production data")

        rep.section("1. pull the SERVED page and extract its inline script")
        html = get(PAGE)
        if not html:
            rep.fail("  could not fetch the served page")
            sys.exit(1)
        html_s = html.decode("utf-8", "ignore")
        rep.ok(f"  {len(html_s):,} bytes served")
        if "JH_FLOWS_PRESSURE_3873" not in html_s:
            rep.fail("  version marker absent from the served page — CDN may be serving a stale copy")
            sys.exit(1)
        rep.ok("  version marker JH_FLOWS_PRESSURE_3873 present — CDN matches this deploy")

        m = re.search(r"<script>(.*?)</script>", html_s, re.S)
        if not m:
            rep.fail("  could not locate the inline <script> block")
            sys.exit(1)
        Path("/tmp/live_page.js").write_text(m.group(1))

        rep.section("2. pull LIVE daily.json + constituent-pressure.json")
        d_raw = get(f"{CDN}/etf-flows/daily.json")
        c_raw = get(f"{CDN}/etf-flows/constituent-pressure.json")
        if not d_raw or not c_raw:
            rep.fail("  one or both live feeds unreachable")
            sys.exit(1)
        Path("/tmp/live_daily.json").write_bytes(d_raw)
        Path("/tmp/live_cp.json").write_bytes(c_raw)
        rep.ok(f"  daily.json {len(d_raw):,} bytes · constituent-pressure.json {len(c_raw):,} bytes")

        rep.section("3. execute the SERVED script against LIVE data under Node")
        Path("/tmp/gate_harness.js").write_text(HARNESS)
        proc = subprocess.run(["node", "/tmp/gate_harness.js"], capture_output=True, text=True, timeout=120)
        rep.log(proc.stdout)
        if proc.returncode != 0:
            rep.fail(f"  harness exited {proc.returncode}\n{proc.stderr[-2000:]}")
            sys.exit(1)

        out = {}
        for line in proc.stdout.splitlines():
            parts = line.split(" ", 1)
            if len(parts) == 2:
                out[parts[0]] = parts[1]

        rep.section("4. hard gate on the real results")
        checks = [
            ("no exception thrown", out.get("THREW") == "null"),
            ("buying board rendered content", int(out.get("BUYING_BYTES", "0")) > 500),
            ("selling board rendered content", int(out.get("SELLING_BYTES", "0")) > 500),
            ("heatmap rendered content", int(out.get("HEATMAP_BYTES", "0")) > 1000),
            ("master table rendered content", int(out.get("MASTER_BYTES", "0")) > 5000),
            ("pressure-section unhid", out.get('DISPLAY_pressure-section') == '""'),
            ("unified-heatmap-section unhid", out.get('DISPLAY_unified-heatmap-section') == '""'),
            ("master-table-section unhid", out.get('DISPLAY_master-table-section') == '""'),
            ("sort click wired to a real <th>", out.get("SORT_CLICK_OK") == "true"),
            ("cadence switch changed real content", out.get("CADENCE_SWITCH_CHANGED_CONTENT") == "true"),
            ("type=STOCK filter leaked zero ETF rows on LIVE data", out.get("TYPE_STOCK_ETF_LEAK") == "0"),
            ("leveraged=Y filter leaked zero stock rows on LIVE data", out.get("LEV_Y_STOCK_LEAK") == "0"),
            ("no null/NaN/undefined leaked on LIVE data", out.get("BAD_TOKENS_FOUND") == "[]"),
        ]
        for label, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {label}")

        rep.kv(input_counts=out.get("INPUT_COUNTS"), master_count_text=out.get("MASTER_COUNT_TEXT"))
        rep.section("5. sample real rows (human spot-check)")
        rep.log(f"  {out.get('SAMPLE_ROWS_JSON', '')[:1500]}")

        failed = [l for l, ok in checks if not ok]
        if failed:
            rep.fail(f"FAILED {len(failed)}: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — served page executes cleanly against LIVE production data "
               f"({out.get('INPUT_COUNTS')})")


if __name__ == "__main__":
    main()
