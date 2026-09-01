"""ops_5094 -- CSP policy-as-code: the edge header that broke global-cycle.html.

Root cause (ops 5092/5093 forensics + this session's origin audit): the
Cloudflare response-header transform rule `a2a-csp-header` (ops 4386, 2026-08-04)
was hand-written to close an A2A security critique and never audited against
the origins the fleet loads. From that day it blocked cdn.jsdelivr.net (38
pages incl. global-cycle's d3/topojson/world-atlas), Google Fonts (65 pages),
Tailwind (8), Plotly (5), unpkg, and EVERY Lambda function URL in connect-src.
global-cycle.html bundled the blocked topo fetch with the data fetch, so one
CSP block became "Failed to load: Failed to fetch" on a page whose feed was
fine (HTTP 200).

This op makes the policy a derived artefact of the code (scripts/gen_csp.py ->
config/csp-policy.json) and proves the fix in a real browser:

  S1  live header at the edge before the change (HEAD global-cycle.html)
  S2  policy-as-code check: config/csp-policy.json must match the site tree
  S3  upsert the Cloudflare rule (PATCH by id / POST / create entrypoint)
  S4  edge propagation: HEAD apex + www until the new header is served
  S5  Pages deploy: wait until global-cycle.html references /assets/vendor and
      the vendored d3 / topojson / world-atlas answer 200 through the edge
  S6  headless Chromium (runner Google Chrome): global-cycle.html desktop +
      mobile + warm (service-worker controlling), /global-cycle/, index.html,
      chart-pro.html, why.html, fortress.html -- CSP violations, page errors,
      DOM facts, screenshots committed to reports/latest

Gate (sys.exit(1)): header not observed at the edge; global-cycle.html shows
no data (decisive/tiles/ladder/map), or ANY rendered page still reports a
Content-Security-Policy violation. A RED here lists the exact directive and
origin so the next iteration is a one-line change to config/csp-policy.json.
"""
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
from ops_report import report  # noqa: E402
import gen_csp  # noqa: E402

OUT_DIR = ROOT / "aws" / "ops" / "reports" / "latest"
CF_API = "https://api.cloudflare.com/client/v4"
ZONE = "justhodl.ai"
EDGE_PAGES = ["https://justhodl.ai/global-cycle.html", "https://www.justhodl.ai/global-cycle.html"]
VENDOR = ["/assets/vendor/d3.v7.9.0.min.js", "/assets/vendor/topojson-client.v3.1.0.min.js",
          "/assets/vendor/world-atlas-2.0.2-countries-110m.json"]
RENDER = [
    ("gc-desktop", "https://justhodl.ai/global-cycle.html", {"width": 1440, "height": 1000}, "gc"),
    ("gc-mobile", "https://justhodl.ai/global-cycle.html", {"width": 390, "height": 844}, "gc"),
    ("gc-history", "https://justhodl.ai/global-cycle/", {"width": 1440, "height": 1000}, "hist"),
    ("index", "https://justhodl.ai/", {"width": 1440, "height": 1000}, "generic"),
    ("chart-pro", "https://justhodl.ai/chart-pro.html", {"width": 1440, "height": 1000}, "generic"),
    ("why", "https://justhodl.ai/why.html", {"width": 1440, "height": 1000}, "generic"),
    ("fortress", "https://justhodl.ai/fortress.html", {"width": 1440, "height": 1000}, "generic"),
]

GC_PROBE = r"""
() => {
  const q = (id) => { const e = document.getElementById(id); return e ? e.textContent.trim().slice(0, 220) : null; };
  const paths = Array.from(document.querySelectorAll('#world-map svg path'));
  const withData = paths.filter(p => p.classList.contains('has-data')).length;
  const note = document.getElementById('map-note');
  return {
    title: document.title, decisive: q('decisiveText'), genTime: q('genTime'), ageStr: q('ageStr'),
    globalPhase: q('globalPhase'), globalCli: q('globalCli'), pctExpansion: q('pctExpansion'),
    countryCount: q('countryCount'), freshCount: q('freshCount'), freshSub: q('freshSub'),
    ladderCells: document.querySelectorAll('#ladder .ladder-cell').length,
    regions: document.querySelectorAll('#regionGrid .region').length,
    tiles: document.querySelectorAll('#regionGrid .country-tile').length,
    physTags: document.querySelectorAll('#regionGrid .country-tile .fresh-tag').length,
    mapPaths: paths.length, mapWithData: withData,
    mapNote: note && note.style.display !== 'none' ? note.textContent.slice(0, 160) : null,
    d3: typeof window.d3, topojson: typeof window.topojson,
    dataLoaded: !!(window.__gbc && window.__gbc.countries), worldLoaded: !!(window.__gbc && window.__gbc.world),
    engineVersion: window.__gbc ? window.__gbc.engine_version : null,
    swController: !!(navigator.serviceWorker && navigator.serviceWorker.controller),
    firstVisibleText: (document.body.innerText || '').slice(0, 300),
  };
}
"""
HIST_PROBE = r"""
() => ({ title: document.title, svgs: document.querySelectorAll('svg').length,
         paths: document.querySelectorAll('svg path').length,
         genTime: (document.getElementById('genTime')||{}).textContent,
         dataLoaded: !!(window.__gbc && window.__gbc.countries), d3: typeof window.d3,
         firstVisibleText: (document.body.innerText||'').slice(0, 300) })
"""
GENERIC_PROBE = r"""
() => ({ title: document.title, bodyChars: (document.body.innerText||'').length,
         scripts: document.querySelectorAll('script[src]').length,
         fontsLoaded: (document.fonts && document.fonts.size) || 0,
         firstVisibleText: (document.body.innerText||'').slice(0, 200) })
"""


def http(url, method="GET", headers=None, timeout=25):
    req = urllib.request.Request(url, method=method, headers=headers or {"User-Agent": "ops5094"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers or {}), b""
    except Exception as e:  # noqa: BLE001
        return None, {"error": str(e)[:160]}, b""


def cf(path, method="GET", body=None):
    tok = os.environ.get("CLOUDFLARE_API_TOKEN", "")
    req = urllib.request.Request(CF_API + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Authorization": "Bearer " + tok, "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode())
        except Exception:  # noqa: BLE001
            return e.code, {"errors": [{"message": str(e)[:200]}]}


def upsert_rule(r, csp_header, cfg):
    st, z = cf(f"/zones?name={ZONE}")
    if st != 200 or not z.get("result"):
        raise RuntimeError(f"zone lookup failed: {st} {json.dumps(z)[:300]}")
    zid = z["result"][0]["id"]
    r.log(f"zone {zid[:8]}…")
    phase = cfg.get("cloudflare_phase", "http_response_headers_transform")
    ref = cfg.get("cloudflare_rule_ref", "a2a-csp-header")
    rule = {"ref": ref,
            "expression": 'http.host in {"justhodl.ai" "www.justhodl.ai"}',
            "description": "ops5094: CSP derived from config/csp-policy.json (scripts/gen_csp.py) -- a2a-csp-header",
            "action": "rewrite",
            "action_parameters": {"headers": {"Content-Security-Policy": {"operation": "set", "value": csp_header}}},
            "enabled": True}
    st, rs = cf(f"/zones/{zid}/rulesets/phases/{phase}/entrypoint")
    if st == 200 and (rs.get("result") or {}).get("id"):
        rsid = rs["result"]["id"]
        existing = [x for x in (rs["result"].get("rules") or [])
                    if x.get("ref") == ref or "a2a-csp" in (x.get("description") or "")]
        r.log(f"entrypoint ruleset {rsid[:8]}… with {len(rs['result'].get('rules') or [])} rule(s); "
              f"{len(existing)} match ref/description")
        if existing:
            rid = existing[0]["id"]
            st2, res = cf(f"/zones/{zid}/rulesets/{rsid}/rules/{rid}", "PATCH", rule)
            how = f"PATCH rule {rid[:8]}…"
            if st2 != 200:
                # some accounts reject ref on PATCH; retry without it
                rule2 = {k: v for k, v in rule.items() if k != "ref"}
                st2, res = cf(f"/zones/{zid}/rulesets/{rsid}/rules/{rid}", "PATCH", rule2)
                how += " (retry without ref)"
            for extra in existing[1:]:
                cf(f"/zones/{zid}/rulesets/{rsid}/rules/{extra['id']}", "DELETE")
                r.log(f"removed duplicate CSP rule {extra['id'][:8]}…")
        else:
            st2, res = cf(f"/zones/{zid}/rulesets/{rsid}/rules", "POST", rule)
            how = "POST new rule into entrypoint"
    else:
        st2, res = cf(f"/zones/{zid}/rulesets", "POST",
                      {"name": "a2a response headers", "kind": "zone", "phase": phase, "rules": [rule]})
        how = "POST new entrypoint ruleset"
    ok = st2 == 200 and res.get("success", False)
    r.log(f"{how}: HTTP {st2} success={res.get('success')} errors={json.dumps(res.get('errors'))[:300]}")
    if not ok:
        raise RuntimeError(f"Cloudflare upsert failed: {how} {st2} {json.dumps(res)[:400]}")
    return how


def wait_edge(r, needle_all, deadline_s=240):
    t0 = time.time()
    seen = {}
    while time.time() - t0 < deadline_s:
        ok_all = True
        for u in EDGE_PAGES:
            st, h, _ = http(u, "HEAD")
            csp = h.get("Content-Security-Policy") or h.get("content-security-policy") or ""
            seen[u] = (st, csp[:160])
            if not all(n in csp for n in needle_all):
                ok_all = False
        if ok_all:
            r.ok(f"edge serves the new header on {len(EDGE_PAGES)} host(s) after {time.time() - t0:.0f}s")
            return True
        time.sleep(10)
    r.fail(f"edge header not observed within {deadline_s}s: {json.dumps(seen)[:500]}")
    return False


def wait_pages(r, deadline_s=900):
    t0 = time.time()
    last = {}
    while time.time() - t0 < deadline_s:
        st, h, body = http("https://justhodl.ai/global-cycle.html?nocache=" + str(int(time.time())),
                           headers={"User-Agent": "ops5094", "Cache-Control": "no-cache"})
        page_ok = st == 200 and b"/assets/vendor/d3.v7.9.0.min.js" in body and b"cdn.jsdelivr.net/npm/d3" not in body
        vend = {}
        for v in VENDOR:
            s2, h2, b2 = http("https://justhodl.ai" + v)
            vend[v] = (s2, len(b2))
        vend_ok = all(s == 200 and n > 1000 for s, n in vend.values())
        last = {"page": st, "page_has_vendor": page_ok, "vendor": vend}
        if page_ok and vend_ok:
            r.ok(f"Pages deploy live after {time.time() - t0:.0f}s: {json.dumps(last)[:300]}")
            return True
        time.sleep(20)
    r.fail(f"Pages deploy not observed within {deadline_s}s: {json.dumps(last)[:400]}")
    return False


def ensure_playwright(r):
    try:
        import playwright  # noqa: F401
        r.log("playwright already importable")
    except Exception:  # noqa: BLE001
        r.log("pip installing playwright")
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)


def launch(p, r):
    try:
        b = p.chromium.launch(channel="chrome", headless=True)
        r.log("launched runner Google Chrome (channel=chrome)")
        return b
    except Exception as e:  # noqa: BLE001
        r.warn("channel=chrome failed: %s -- installing chromium" % str(e)[:120])
        subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
        b = p.chromium.launch(headless=True)
        r.log("launched downloaded Chromium")
        return b


def is_csp(text):
    t = text.lower()
    return "content security policy" in t or "violates the following" in t or "refused to" in t and "policy" in t


def visit(browser, r, tag, url, viewport, probe_js, settle_ms=9000, context=None):
    own = context is None
    ctx = context or browser.new_context(viewport=viewport, ignore_https_errors=True)
    page = ctx.new_page()
    console, errors, failed = [], [], []
    page.on("console", lambda m: console.append({"type": m.type, "text": m.text[:400]}))
    page.on("pageerror", lambda e: errors.append(str(e)[:400]))
    page.on("requestfailed", lambda rq: failed.append({"url": rq.url[:160], "failure": (rq.failure or "")[:100]}))
    t0 = time.time()
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        try:
            page.wait_for_load_state("networkidle", timeout=20000)
        except Exception:  # noqa: BLE001
            pass
        page.wait_for_timeout(settle_ms)
        dom = page.evaluate(probe_js)
    except Exception as e:  # noqa: BLE001
        dom = {"error": str(e)[:300]}
    load_s = round(time.time() - t0, 1)
    shot = OUT_DIR / ("5094-%s.jpg" % tag)
    try:
        page.screenshot(path=str(shot), full_page=(tag != "index"), type="jpeg", quality=55)
    except Exception as e:  # noqa: BLE001
        r.warn("%s: screenshot failed: %s" % (tag, str(e)[:100]))
    csp_viol = [c["text"] for c in console if is_csp(c["text"])]
    bad_console = [c for c in console if c["type"] in ("error", "warning") and not is_csp(c["text"])]
    r.section("%s -- %s @ %dx%d (%.1fs)" % (tag, url, viewport["width"], viewport["height"], load_s))
    for k, v in dom.items():
        if k != "firstVisibleText":
            r.log("  DOM %s = %s" % (k, json.dumps(v)[:240]))
    r.log("  firstVisibleText = %s" % json.dumps(dom.get("firstVisibleText"))[:320])
    r.log("  CSP violations (%d): %s" % (len(csp_viol), json.dumps(csp_viol[:8])[:1600]))
    r.log("  page errors (%d): %s" % (len(errors), json.dumps(errors)[:800]))
    r.log("  other console errors/warnings (%d): %s" % (len(bad_console), json.dumps(bad_console[:8])[:1200]))
    r.log("  failed requests (%d): %s" % (len(failed), json.dumps(failed[:10])[:1200]))
    r.kv(visit=tag, load_s=load_s, csp_violations=len(csp_viol), page_errors=len(errors),
         failed_req=len(failed), decisive=(dom.get("decisive") or dom.get("genTime") or dom.get("error") or "")[:70],
         tiles=dom.get("tiles"), ladder=dom.get("ladderCells"), map_with_data=dom.get("mapWithData"),
         data_loaded=dom.get("dataLoaded"), engine=dom.get("engineVersion"))
    page.close()
    if own:
        ctx.close()
    return dom, errors, csp_viol


def main():
    with report("5094-csp-policy-as-code") as r:
        r.heading("ops 5094 -- CSP policy-as-code: fix the edge header + prove global-cycle renders")
        r.log("started %s" % datetime.now(timezone.utc).isoformat(timespec="seconds"))
        fails = []

        r.section("S1 live header before")
        for u in EDGE_PAGES:
            st, h, _ = http(u, "HEAD")
            csp = h.get("Content-Security-Policy") or h.get("content-security-policy") or ""
            r.log(f"{u}: HTTP {st} csp_len={len(csp)} jsdelivr_in_script_src={'cdn.jsdelivr.net' in csp.split('connect-src')[0]}")
            r.log(f"  before: {csp[:900]}")

        r.section("S2 policy-as-code")
        cfg = json.loads((ROOT / "config" / "csp-policy.json").read_text())
        res = gen_csp.build(cfg)
        if res["header"] != cfg.get("header"):
            fails.append("config/csp-policy.json is STALE vs the site tree -- run scripts/gen_csp.py --write")
            r.fail(fails[-1])
        header = res["header"]
        r.log(f"generated header ({len(header)} chars, {res['n_files']} files scanned):")
        for part in header.split("; "):
            r.log("  " + part)
        r.kv(visit="policy", header_chars=len(header), n_files=res["n_files"],
             script_src=len(res["generated"]["script-src"]), connect_src=len(res["generated"]["connect-src"]))

        r.section("S3 Cloudflare upsert")
        if not os.environ.get("CLOUDFLARE_API_TOKEN"):
            fails.append("CLOUDFLARE_API_TOKEN not present in the runner env")
            r.fail(fails[-1])
        else:
            try:
                how = upsert_rule(r, header, cfg)
                r.ok(f"rule upserted via {how}")
            except Exception as e:  # noqa: BLE001
                fails.append(f"upsert failed: {str(e)[:300]}")
                r.fail(fails[-1])

        r.section("S4 edge propagation")
        if not wait_edge(r, ["cdn.jsdelivr.net", "lambda-url.us-east-1.on.aws", "fonts.googleapis.com"]):
            fails.append("new CSP not observed at the edge")

        r.section("S5 Pages deploy (vendored assets + patched page)")
        if not wait_pages(r):
            fails.append("Pages deploy with /assets/vendor not observed")

        r.section("S6 headless render")
        ensure_playwright(r)
        from playwright.sync_api import sync_playwright
        results = {}
        with sync_playwright() as p:
            browser = launch(p, r)
            for tag, url, vp, kind in RENDER:
                probe = {"gc": GC_PROBE, "hist": HIST_PROBE}.get(kind, GENERIC_PROBE)
                results[tag] = visit(browser, r, tag, url, vp, probe)
            # warm visit: service worker controlling, same context
            ctx = browser.new_context(viewport={"width": 1440, "height": 1000})
            visit(browser, r, "gc-warm1", RENDER[0][1], {"width": 1440, "height": 1000}, GC_PROBE, settle_ms=4000, context=ctx)
            results["gc-warm2"] = visit(browser, r, "gc-warm2", RENDER[0][1], {"width": 1440, "height": 1000}, GC_PROBE, context=ctx)
            ctx.close()
            browser.close()

        r.section("verdict")
        for tag, (dom, errors, viol) in results.items():
            if viol:
                fails.append(f"{tag}: {len(viol)} CSP violation(s): {viol[0][:200]}")
            if errors and tag.startswith("gc"):
                fails.append(f"{tag}: {len(errors)} page error(s): {errors[0][:160]}")
            if tag in ("gc-desktop", "gc-mobile", "gc-warm2"):
                dec = (dom.get("decisive") or "")
                if not dom.get("dataLoaded") or dec.startswith("Feed unavailable") or dec.startswith("Failed") or dec.startswith("Loading"):
                    fails.append(f"{tag}: data not rendered (decisive='{dec[:80]}')")
                if (dom.get("tiles") or 0) < 30 or (dom.get("ladderCells") or 0) != 10:
                    fails.append(f"{tag}: tiles={dom.get('tiles')} ladder={dom.get('ladderCells')}")
                if (dom.get("mapWithData") or 0) < 25:
                    fails.append(f"{tag}: map has {dom.get('mapWithData')} data paths (note={dom.get('mapNote')})")
            if tag == "gc-history" and not dom.get("dataLoaded"):
                fails.append(f"gc-history: DATA not loaded (genTime={dom.get('genTime')})")
        for f in fails:
            r.fail(f)
        if fails:
            r.log("VERDICT: RED -- %d failure(s)" % len(fails))
            sys.exit(1)
        r.ok("VERDICT: GREEN -- CSP derived from code is live at the edge; global-cycle.html, /global-cycle/, "
             "index, chart-pro, why and fortress render with zero CSP violations")


if __name__ == "__main__":
    main()
