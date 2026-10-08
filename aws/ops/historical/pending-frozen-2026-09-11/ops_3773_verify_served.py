#!/usr/bin/env python3
"""ops 3773 — prove capture-gap.html is SERVED (edge), not merely committed.

Repo state is not proof of live. pages.yml publishes to GitHub Pages behind
Cloudflare, and page audits read the EDGE — so this ops fetches the live URL
from the runner (never the sandbox, which is CF-403'd) with a cache-buster and
greps for markers UNIQUE TO v4. Reusing a marker that existed pre-v4 would
happily accept a stale copy, which has burned this platform before.

Also confirms the page appears in the SERVED nav manifest (the repo copy is
CI-regenerated without commit-back and is therefore always stale).
"""
import sys, time, json, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report

URL = "https://justhodl.ai/capture-gap.html"
NAV = "https://justhodl.ai/data/nav-manifest.json"
UA = "JustHodl.AI ops-verify raafouis@gmail.com"
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)
    return ok


def fetch(url, attempt):
    u = url + ("&" if "?" in url else "?") + "v=%d%d" % (int(time.time()), attempt)
    req = urllib.request.Request(u, headers={
        "User-Agent": UA, "Cache-Control": "no-cache", "Pragma": "no-cache"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.status, r.read().decode("utf-8", "replace")


def main():
    with report("3773_verify_served") as rep:
        rep.heading("ops 3773 — edge verification of capture-gap.html")

        # markers UNIQUE to v4 — none of these existed in the v3 page
        V4_MARKERS = {
            "leaderboard_div": 'id="leader"',
            "byind_div": 'id="byind"',
            "leaderboard_key": "top_undervalued_all_industries",
            "byind_key": "by_industry",
            "catchup": "catchup_pct",
            "catchup_basis": "catchup_basis",
            "blend_note": "Rank is blended",
            "accordion": "indrow",
        }

        body = ""
        for attempt in range(1, 9):
            try:
                status, body = fetch(URL, attempt)
            except Exception as e:
                rep.warn("attempt %d fetch error: %s" % (attempt, str(e)[:120]))
                time.sleep(25)
                continue
            hits = sum(1 for m in V4_MARKERS.values() if m in body)
            rep.log("attempt %d: HTTP %s · %d bytes · %d/%d v4 markers" % (
                attempt, status, len(body), hits, len(V4_MARKERS)))
            if hits == len(V4_MARKERS):
                break
            time.sleep(25)

        rep.section("Marker audit (edge copy)")
        for name, m in V4_MARKERS.items():
            gate(rep, f"SERVED.{name}", m in body, "present")
        gate(rep, "SERVED.size", len(body) > 20000, "%d bytes" % len(body))
        gate(rep, "SERVED.no_dead_field", "rpo_growth_yoy" not in body,
             "dead v3.0 field absent from served copy")

        rep.section("Nav manifest (SERVED copy — repo copy is always stale)")
        try:
            req = urllib.request.Request(NAV + "?v=%d" % int(time.time()),
                                         headers={"User-Agent": UA, "Cache-Control": "no-cache"})
            with urllib.request.urlopen(req, timeout=45) as r:
                nav = json.loads(r.read())
            found, cat = False, None
            for c in nav.get("categories", []):
                for p in c.get("pages", []):
                    if "capture-gap" in (p.get("href") or ""):
                        found, cat = True, c.get("name")
            gate(rep, "NAV.listed", found, "capture-gap in served manifest under '%s'" % cat)
            rep.kv(nav_pages=nav.get("n_pages"), nav_category=cat)
        except Exception as e:
            rep.warn("nav manifest fetch failed: %s" % str(e)[:140])

        rep.section("Feed sanity from the edge")
        try:
            req = urllib.request.Request(
                "https://justhodl-dashboard-live.s3.amazonaws.com/data/chokepoint.json",
                headers={"User-Agent": UA})
            with urllib.request.urlopen(req, timeout=60) as r:
                d = json.loads(r.read())
            cap = d.get("capture_gap") or {}
            st = cap.get("stats") or {}
            rep.kv(version=d.get("version"), scored=st.get("scored"),
                   industries=st.get("industries_grouped"),
                   with_catchup=st.get("with_catchup"),
                   leaderboard=len(cap.get("top_undervalued_all_industries") or []))
            gate(rep, "FEED.v4", str(d.get("version", "")).startswith("4."), "engine v4 live")
        except Exception as e:
            rep.warn("feed fetch failed: %s" % str(e)[:140])

        rep.section("VERDICT")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — page v4 confirmed SERVED at %s" % URL)


if __name__ == "__main__":
    main()
