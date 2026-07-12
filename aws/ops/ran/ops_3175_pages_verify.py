"""ops 3175 — verify BOTH pages actually render Khalid's data.

He reported: "i dont see none of my watchlists". Two things were wrong:
  1. theses.html — `var b = d.spy_base_rates_pct` SHADOWED the variable
     holding the board element, so the per-thesis table (his 31 scored
     watchlists) wrote into a plain object and the page kept its "…"
     placeholder. Fixed; regime columns added.
  2. watchlists.html — the TradingView mirror. Verify it is served AND
     that its feeds (tv-watchlists.json + symbol-map.json) are reachable
     through the CDN path the page actually uses.
"""

import json
import sys
import time
import urllib.request

from ops_report import report

PROXY = "https://justhodl-data-proxy.raafouis.workers.dev"


def get(url, timeout=20):
    r = urllib.request.urlopen(urllib.request.Request(
        url, headers={"User-Agent": "Mozilla/5.0 ops-3175",
                      "Cache-Control": "no-cache"}), timeout=timeout)
    return r.status, r.read().decode("utf-8", "replace")


with report("3175_pages_verify") as rep:
    fails, warns = [], []
    rep.heading("ops 3175 — do his pages actually show his data?")

    rep.section("1. watchlists.html (the TradingView mirror)")
    try:
        st, html = get(f"https://justhodl.ai/watchlists.html?t={int(time.time())}")
        ok = "My Watchlists" in html and "tv-watchlists.json" in html
        rep.kv(watchlists_http=st, markers=ok)
        if ok:
            rep.ok("page served and wired to the watchlists feed")
        else:
            fails.append("watchlists.html served but markers missing")
    except Exception as e:
        fails.append(f"watchlists.html: {str(e)[:90]}")

    rep.section("2. theses.html (board fix)")
    for attempt in range(10):
        try:
            st, html = get(f"https://justhodl.ai/theses.html?t={int(time.time())}")
            shadow = "var b=d.spy_base_rates_pct" in html
            link = "watchlists.html" in html
            if not shadow and link:
                rep.ok("board-shadowing bug gone from the served page; "
                       "watchlist link present")
                break
            time.sleep(30)
        except Exception:
            time.sleep(30)
    else:
        warns.append("CDN still serving the pre-fix theses.html (max-age "
                     "self-heals within ~10 min)")

    rep.section("3. The feeds the pages read (via the proxy path)")
    for key, must in (("data/tv-watchlists.json", "lists"),
                      ("data/symbol-map.json", "map"),
                      ("data/thesis-engine.json", "theses")):
        try:
            st, body = get(f"{PROXY}/{key}?t={int(time.time())}", timeout=25)
            d = json.loads(body)
            n = len(d.get(must) or [])
            rep.kv(**{key.split("/")[-1].replace(".", "_"): n})
            if n:
                rep.ok(f"{key}: {n} {must}")
            else:
                fails.append(f"{key}: empty '{must}'")
        except Exception as e:
            fails.append(f"{key}: {str(e)[:70]}")

    rep.section("4. What he should SEE")
    try:
        st, body = get(f"{PROXY}/data/thesis-engine.json?t={int(time.time())}")
        d = json.loads(body)
        rows = d.get("theses") or []
        rep.log(f"theses.html should list {len(rows)} rows — e.g.:")
        for r in rows[:8]:
            e = (r.get("event_study") or {}).get("w13") or {}
            rep.log(f"  · {str(r.get('name'))[:44]:44s} "
                    f"act {str(r.get('activation_now')):>5}% "
                    f"t={str(e.get('t_stat')):>6} "
                    f"since {r.get('history_from')}")
        st, body = get(f"{PROXY}/data/tv-watchlists.json?t={int(time.time())}")
        w = json.loads(body)
        ls = [l for l in (w.get("lists") or [])
              if not str(l.get("id", "")).startswith("e2e-")]
        rep.log(f"watchlists.html should list {len(ls)} lists — e.g.:")
        for l in sorted(ls, key=lambda x: -len(x.get("symbols") or []))[:6]:
            rep.log(f"  · {str(l.get('name'))[:44]:44s} "
                    f"{len(l.get('symbols') or []):>3} indicators")
    except Exception as e:
        warns.append(f"preview: {str(e)[:70]}")

    for w in warns:
        rep.warn(w)
    for f in fails:
        rep.fail(f)
    rep.kv(n_fails=len(fails), n_warns=len(warns),
           verdict="PASS" if not fails else "FAIL")
    if fails:
        sys.exit(1)
    sys.exit(0)
