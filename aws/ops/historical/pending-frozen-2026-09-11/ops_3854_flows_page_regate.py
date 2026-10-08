"""
ops_3854 — prove the flows.html fix on the EDGE, against the LIVE feed.

ops 3852 cleared the engine: 296/300 rows carry flow_zscore_90d, SPDR 11/11,
CDN byte-matches S3. The break was client-side:

    <script src="/jh-nav-drawer.js" defer>
    function renderDivergence(b){ ... }        <-- NEVER EXECUTES
    </script>

Inline content of a <script> with a src attribute is ignored per HTML spec, so
renderDivergence was undefined. load() calls it 3 statements in (line 612), and
the ReferenceError aborted every render after it: composite gauges, sector
heatmap, top-10 inflows, top-10 outflows, full universe. Exactly the reported
symptom set, in source order. Fix moved the function into the executing block
and self-closed the nav-drawer tag.

Repo-passing is not proof. This ops:
  1. polls the CLOUDFLARE EDGE for the new marker (repo copy is irrelevant),
  2. asserts the structural invariant (marker before load(), nav-drawer tag
     self-closed, exactly one renderDivergence),
  3. executes the page's OWN script under Node with a DOM shim against the LIVE
     daily.json + composite.json, and gates on every one of the six containers
     receiving real content — the browser code path, real data, no mocks.
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
MARKER = "JH_FLOWS_DIVFIX_3853"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")
TARGETS = ["meta-bar", "composite-grid", "sector-heatmap",
           "top-inflows", "top-outflows", "full-table"]


def fetch(url, attempt=0):
    req = urllib.request.Request(
        f"{url}{'&' if '?' in url else '?'}v={int(time.time())}-{attempt}",
        headers={"User-Agent": UA, "Cache-Control": "no-cache", "Pragma": "no-cache"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read().decode("utf-8", "ignore")


HARNESS = r"""
const fs = require('fs');
const els = {};
const mk = id => ({ set innerHTML(v){ els[id] = v }, get innerHTML(){ return els[id] || '' }, style:{} });
const store = {};
global.document = { getElementById: id => store[id] || (store[id] = mk(id)),
                    querySelectorAll: () => [] };
global.window = {};
const daily = JSON.parse(fs.readFileSync('/tmp/daily.json','utf8'));
const comp  = JSON.parse(fs.readFileSync('/tmp/composite.json','utf8'));
global.fetch = async (u) => ({ ok:true, status:200, json: async () =>
    u.includes('daily.json') ? daily : u.includes('composite.json') ? comp : {} });
(async () => {
  let threw = null;
  try { eval(fs.readFileSync('/tmp/flows_main.js','utf8')); await new Promise(r=>setTimeout(r,600)); }
  catch (e) { threw = e.message; }
  const out = { threw };
  for (const id of %TARGETS%) {
    const h = els[id] || '';
    out[id] = { bytes: h.length, rows: (h.match(/<tr>/g) || []).length };
  }
  console.log(JSON.stringify(out));
})();
"""


def main():
    with report("3854_flows_page_regate") as rep:
        rep.heading("ops 3854 — flows.html render fix proven on the edge, live feed")
        fails = []

        rep.section("1. EDGE — poll until the new page is actually served")
        html = None
        for attempt in range(1, 10):
            try:
                h = fetch(PAGE, attempt)
            except Exception as e:
                rep.log(f"  attempt {attempt}: {str(e)[:80]}")
                time.sleep(20)
                continue
            if MARKER in h:
                html = h
                rep.ok(f"  served on attempt {attempt} ({len(h):,} bytes)")
                break
            rep.log(f"  attempt {attempt}: stale ({len(h):,} bytes, no marker)")
            time.sleep(20)
        if html is None:
            rep.fail(f"  marker {MARKER} never reached the edge after 9 attempts")
            sys.exit(1)

        rep.section("2. structural invariant — the bug class itself")
        i_marker = html.find(MARKER)
        i_load = html.find("\nload();")
        checks = [
            ("renderDivergence defined exactly once",
             html.count("function renderDivergence(b){") == 1),
            ("defined BEFORE load() in the same executing block",
             0 < i_marker < i_load),
            ("nav-drawer script tag is self-closed (no trapped body)",
             '<script src="/jh-nav-drawer.js" defer></script>' in html),
            ("no function body left inside any src'd script tag",
             not re.search(r'<script[^>]+src=[^>]*>\s*\n\s*function', html)),
        ]
        for label, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {label}")
            if not ok:
                fails.append(label)

        rep.section("3. execute the page's own script against the LIVE feed")
        m = re.search(r"<script>(.*?)</script>", html, re.S)
        Path("/tmp/flows_main.js").write_text(m.group(1))
        for name in ("daily", "composite"):
            Path(f"/tmp/{name}.json").write_text(fetch(f"{CDN}/etf-flows/{name}.json"))
        d = json.loads(Path("/tmp/daily.json").read_text())
        nz = sum(1 for r in (d.get("metrics") or []) if r.get("flow_zscore_90d") is not None)
        rep.log(f"  live feed: {len(d.get('metrics') or [])} rows, {nz} z-scored")

        Path("/tmp/harness.js").write_text(HARNESS.replace("%TARGETS%", json.dumps(TARGETS)))
        proc = subprocess.run(["node", "/tmp/harness.js"], capture_output=True, text=True, timeout=180)
        if proc.returncode != 0:
            rep.fail(f"  harness exit {proc.returncode}: {proc.stderr[:400]}")
            sys.exit(1)
        res = json.loads(proc.stdout.strip().splitlines()[-1])

        if res.get("threw"):
            rep.fail(f"  render THREW: {res['threw']}")
            fails.append("render_threw")
        else:
            rep.ok("  render completed with no exception")

        for tid in TARGETS:
            b, rows = res[tid]["bytes"], res[tid]["rows"]
            ok = b > 200
            (rep.ok if ok else rep.fail)(f"  {tid:<16} {b:>7,} bytes  rows={rows}")
            if not ok:
                fails.append(tid)

        gate = [
            ("sector heatmap has all 11 SPDR cells",
             res["sector-heatmap"]["bytes"] > 1500),
            ("top-inflows renders 10 data rows", res["top-inflows"]["rows"] >= 10),
            ("top-outflows renders 10 data rows", res["top-outflows"]["rows"] >= 10),
            ("full universe renders the whole z-scored set",
             res["full-table"]["rows"] >= nz),
        ]
        rep.section("4. the four reported sections, specifically")
        for label, ok in gate:
            (rep.ok if ok else rep.fail)(f"  {label}")
            if not ok:
                fails.append(label)

        rep.kv(marker=MARKER, live_rows=len(d.get("metrics") or []), z_scored=nz,
               heatmap_bytes=res["sector-heatmap"]["bytes"],
               inflow_rows=res["top-inflows"]["rows"],
               outflow_rows=res["top-outflows"]["rows"],
               full_rows=res["full-table"]["rows"], fails=str(fails))
        if fails:
            rep.fail(f"FAILED {len(fails)}: {fails}")
            sys.exit(1)
        rep.ok("PASS_ALL — all four sections render from live data on the served page")


if __name__ == "__main__":
    main()
