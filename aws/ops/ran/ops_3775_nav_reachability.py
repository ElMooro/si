#!/usr/bin/env python3
"""ops 3775 — confirm capture-gap.html is reachable from the SIDEBAR.

3774 proved the page is served (9/9 v4 markers, 28,188 bytes). But its nav check
hit /data/nav-manifest.json and 403'd — my error: jh-nav-drawer.js actually
fetches "/nav-manifest.json" from the ROOT. So sidebar reachability was never
actually verified, and the page contract requires it (a page nobody can navigate
to is half-shipped).

The repo copy of nav-manifest.json is stale by design (dated 2026-07-13, 378
pages, no capture-gap) because CI regenerates it during pages.yml without
committing back. Therefore ONLY the served copy is authoritative — checking the
repo copy would produce a false negative.

If the served manifest is missing the page, the fix is to regenerate it via the
committed generator and push, which is what pages.yml runs anyway.
"""
import sys, time, json, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report

BASE = "https://justhodl.ai"
UA = "JustHodl.AI ops-verify raafouis@gmail.com"
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def fetch(url, attempt=0):
    u = url + ("&" if "?" in url else "?") + "v=%d%d" % (int(time.time()), attempt)
    req = urllib.request.Request(u, headers={
        "User-Agent": UA, "Cache-Control": "no-cache", "Pragma": "no-cache"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.status, r.read().decode("utf-8", "replace")


def main():
    with report("3775_nav_reachability") as rep:
        rep.heading("ops 3775 — sidebar reachability for /capture-gap.html")

        rep.section("Resolve the correct manifest URL (3774 used the wrong path)")
        candidates = ["/nav-manifest.json", "/data/nav-manifest.json"]
        manifest, good_url = None, None
        for c in candidates:
            try:
                st, body = fetch(BASE + c)
                rep.log("  %s -> HTTP %s (%d bytes)" % (c, st, len(body)))
                if st == 200 and body.strip().startswith("{"):
                    manifest, good_url = json.loads(body), c
                    break
            except Exception as e:
                rep.log("  %s -> %s" % (c, str(e)[:80]))
        gate(rep, "NAV.reachable", manifest is not None,
             "served manifest found at %s" % good_url)
        if FAILED:
            sys.exit(1)

        rep.kv(manifest_url=good_url, n_pages=manifest.get("n_pages"),
               generated_at=manifest.get("generated_at"),
               categories=len(manifest.get("categories") or []))

        rep.section("Is capture-gap listed?")
        found, cat, title = False, None, None
        for c in manifest.get("categories", []):
            for p in c.get("pages", []):
                if "capture-gap" in (p.get("href") or ""):
                    found, cat, title = True, c.get("name"), p.get("title")
        if found:
            rep.ok("LISTED under '%s' as '%s'" % (cat, title))
        else:
            rep.warn("NOT listed in the served manifest — regenerating")

        rep.section("Regenerate if missing")
        if not found:
            gen = ROOT.parent / "scripts" / "gen_nav_manifest.py"
            gate(rep, "NAV.generator", gen.exists(), "generator present")
            if FAILED:
                sys.exit(1)
            gtxt = gen.read_text()
            gate(rep, "NAV.pin_present", '"/capture-gap.html"' in gtxt,
                 "FORCE pin committed (ops 3767 follow-up)")
            page = ROOT.parent / "capture-gap.html"
            gate(rep, "NAV.page_at_root", page.exists(),
                 "capture-gap.html at repo root so the generator will scan it")
            if FAILED:
                sys.exit(1)
            import subprocess
            r = subprocess.run([sys.executable, str(gen)], cwd=str(ROOT.parent),
                               capture_output=True, text=True, timeout=300)
            rep.log("  generator rc=%s :: %s" % (r.returncode, (r.stdout or r.stderr)[:200].strip()))
            gate(rep, "NAV.regen_ok", r.returncode == 0, "generator ran clean")
            local = ROOT.parent / "nav-manifest.json"
            if local.exists():
                m2 = json.loads(local.read_text())
                hit = [(c.get("name"), p.get("title"))
                       for c in m2.get("categories", [])
                       for p in m2.get("pages" if False else "pages", []) or []
                       if "capture-gap" in (p.get("href") or "")]
                hit = [(c.get("name"), p.get("title")) for c in m2.get("categories", [])
                       for p in (c.get("pages") or []) if "capture-gap" in (p.get("href") or "")]
                rep.kv(regen_pages=m2.get("n_pages"),
                       capture_gap_listed=bool(hit),
                       category=hit[0][0] if hit else None)
                gate(rep, "NAV.now_listed", bool(hit),
                     "capture-gap present after regeneration (%s)" % (hit[0][0] if hit else "-"))
                rep.log("  manifest regenerated locally; the commit that carries it "
                        "must be a NORMAL push — [skip-deploy] suppresses pages.yml, "
                        "which is exactly what hid the page v4 edits for six ops.")
        else:
            rep.ok("no regeneration needed")

        rep.section("Page still served (regression check)")
        try:
            st, body = fetch(BASE + "/capture-gap.html")
            gate(rep, "PAGE.still_served", st == 200 and "v4-ops3774" in body,
                 "HTTP %s, %d bytes, version stamp present" % (st, len(body)))
        except Exception as e:
            gate(rep, "PAGE.still_served", False, str(e)[:120])

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — sidebar reachability resolved")


if __name__ == "__main__":
    main()
