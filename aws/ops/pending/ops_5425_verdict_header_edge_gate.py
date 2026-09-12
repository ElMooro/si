"""ops 5425 -- verdict header edge gate (read-only, no AWS writes).

Pages runs 4102/4103 failed the node gate because the home binder in
private-artifacts.js matched an empty pathname; 8a8e075 fixed the matcher
and made jh-verdict-header.js read /data/verdict.json same-origin (never the
bucket URL). Pages 4105 is green on that commit. A green workflow is not a
live page: this op proves the chain at the edge, the way the site is used.

  1. GET /private-artifacts.js -- exact home matcher present, '' branch gone
  2. GET /jh-verdict-header.js -- marker present, no amazonaws.com
  3. GET /data/verdict.json    -- 200 via the zone route, writer is the
                                 fusion projection, as_of age reported
  4. headless Chrome on /      -- #jh-verdict-header painted with the marker,
                                 text is a CALL or an honest "unavailable",
                                 nav links still present, no page errors

RED if any of 1-4 fails. Nothing is written to S3 or Lambda.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

SITE = "https://justhodl.ai"
UA = "justhodl-ops-5425/1.0"


def get(path: str):
    req = urllib.request.Request(SITE + path + ("&" if "?" in path else "?") + "v=%d" % int(time.time()),
                                 headers={"User-Agent": UA, "Accept": "*/*", "Cache-Control": "no-cache"})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, r.read().decode("utf-8", "replace"), dict(r.headers)
    except urllib.error.HTTPError as e:  # type: ignore[attr-defined]
        return e.code, e.read().decode("utf-8", "replace")[:400], dict(e.headers)
    except Exception as e:  # noqa: BLE001
        return None, str(e)[:200], {}


def main() -> int:
    red = []
    with report("ops_5425_verdict_header_edge_gate") as R:
        R.heading("ops 5425 -- verdict header edge gate (read-only)")

        # 1. binder
        st, body, _ = get("/private-artifacts.js")
        has_exact = "p === '/' || p === '/index.html'" in body and "jh-verdict-header.js" in body
        has_empty = "p === ''" in body or "/command/.test(p)" in body
        if st == 200 and has_exact and not has_empty:
            R.ok("private-artifacts.js 200 -- exact home matcher live, empty-path branch gone")
        else:
            red.append("binder"); R.fail("private-artifacts.js status=%s exact=%s empty_branch=%s" % (st, has_exact, has_empty))

        # 2. header script
        st, body, hdr = get("/jh-verdict-header.js")
        ok2 = st == 200 and "JH_VERDICT_HEADER_V1" in body and "amazonaws.com" not in body and '"/data/verdict.json"' in body
        (R.ok if ok2 else R.fail)("jh-verdict-header.js status=%s marker=%s bucket_url=%s same_origin=%s type=%s" % (
            st, "JH_VERDICT_HEADER_V1" in body, "amazonaws.com" in body, '"/data/verdict.json"' in body,
            (hdr.get("Content-Type") or hdr.get("content-type") or "?")[:40]))
        if not ok2:
            red.append("header-js")

        # 3. verdict through the zone route
        st, body, hdr = get("/data/verdict.json")
        v = {}
        try:
            v = json.loads(body) if st == 200 else {}
        except Exception:
            v = {}
        writer = v.get("writer")
        as_of = v.get("as_of") or v.get("generated_at")
        age_h = None
        try:
            age_h = round((datetime.now(timezone.utc) - datetime.fromisoformat(str(as_of).replace("Z", "+00:00"))).total_seconds() / 3600, 2)
        except Exception:
            pass
        ok3 = st == 200 and writer == "jh-fusion-projection"
        (R.ok if ok3 else R.fail)("/data/verdict.json status=%s writer=%s bias=%s score=%s horizon=%s shadow=%s missing=%s as_of=%s age_h=%s cache=%s" % (
            st, writer, v.get("bias"), v.get("score"), v.get("horizon"), v.get("shadow_mode"), v.get("missing_families"),
            as_of, age_h, (hdr.get("cf-cache-status") or hdr.get("CF-Cache-Status") or "-")))
        R.kv(step="verdict", status=st, writer=writer, bias=v.get("bias"), score=v.get("score"), as_of=as_of, age_h=age_h)
        if not ok3:
            red.append("verdict")
        if age_h is not None and age_h > 30:
            R.warn("verdict as_of is %.1fh old -- the projection has no scheduled writer yet (ops 5424 was a one-shot)" % age_h)

        # 4. headless render of /
        try:
            import playwright  # noqa: F401
        except Exception:
            subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
        from playwright.sync_api import sync_playwright  # noqa: E402

        facts = {}
        errors, failed = [], []
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(channel="chrome", headless=True)
            except Exception:
                subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
                browser = p.chromium.launch(headless=True)
            ctx = browser.new_context(viewport={"width": 1440, "height": 1100}, user_agent=UA)
            pg = ctx.new_page()
            pg.on("pageerror", lambda e: errors.append(str(e)[:200]))
            pg.on("requestfailed", lambda rq: failed.append("%s %s" % (rq.url[:140], rq.failure)) if "verdict" in rq.url else None)
            pg.goto(SITE + "/?v=%d" % int(time.time()), wait_until="domcontentloaded", timeout=60000)
            for _ in range(20):  # up to 20s for auth assets + verdict fetch
                pg.wait_for_timeout(1000)
                if pg.evaluate("() => !!document.getElementById('jh-verdict-header')"):
                    break
            facts = pg.evaluate("""() => {
                const h = document.getElementById('jh-verdict-header');
                const nav = Array.from(document.querySelectorAll('a[href]')).map(a => a.getAttribute('href'));
                return {
                  painted: !!h,
                  marker: h ? h.getAttribute('data-marker') : null,
                  text: h ? h.innerText.slice(0, 220) : null,
                  first_child_is_header: !!(document.body && document.body.firstElementChild && document.body.firstElementChild.id === 'jh-verdict-header'),
                  nav_links: nav.length,
                  binder_loaded: !!window.JustHodlPrivateArtifacts,
                  header_loaded: !!window.JH_VERDICT_HEADER,
                  title: document.title
                };
            }""")
            browser.close()

        painted = bool(facts.get("painted")) and facts.get("marker") == "JH_VERDICT_HEADER_V1"
        text = facts.get("text") or ""
        honest = text.startswith("CALL") or "unavailable" in text
        nav_ok = (facts.get("nav_links") or 0) >= 50
        ok4 = painted and honest and nav_ok and not errors
        (R.ok if ok4 else R.fail)("home render painted=%s marker=%s prepended=%s text=%r nav_links=%s binder=%s header=%s pageerrors=%s verdict_req_failed=%s" % (
            painted, facts.get("marker"), facts.get("first_child_is_header"), text[:120], facts.get("nav_links"),
            facts.get("binder_loaded"), facts.get("header_loaded"), errors[:3], failed[:3]))
        R.kv(step="render", **{k: (v if not isinstance(v, str) else v[:120]) for k, v in facts.items()})
        if not ok4:
            red.append("render")
        if text.startswith("CALL") is False and painted:
            R.warn("header painted the honest 'unavailable' state -- /data/verdict.json did not reach the page (see step 3 and verdict_req_failed)")

        if red:
            R.fail("RED -- failed: %s" % ", ".join(red))
            return 1
        R.ok("GREEN -- verdict header live at the edge: binder exact-path, header same-origin, verdict via zone route, banner painted on / with nav intact")
        return 0


if __name__ == "__main__":
    sys.exit(main())
