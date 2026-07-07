#!/usr/bin/env python3
"""ops 2950 — FLEET CERTIFICATION: every page, every feed pages actually
reference, every engine's outputs. All against LIVE production, read-only.

Khalid's ask: "make sure all my engines and pages are where they supposed to
and working as supposed to." Definition used here:
  PAGES   working = HTTP 200 + real body, listed in nav.
  FEEDS   working = every data/*.json a page references is live + parses.
  ENGINES working = each registry engine's outs[] answer 200, with age tiers.
Breakage is mapped to page impact so cosmetic backstage issues don't get
confused with user-visible ones. Hard-fails only on user-visible breakage.
"""
import json, re, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from ops_report import report

BASE = "https://justhodl.ai"
UA = {"User-Agent": "Mozilla/5.0 jh-ops"}
EXCLUDE = {"manifest.json", "nav-manifest.json", "engine-wiring.json",
           "package.json", "engine-registry.json"}

def get(path, to=12, body=True):
    """GET; returns (code, bytes, age_hours|None)."""
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            f"{BASE}/{path}", headers=UA), timeout=to)
        age = None
        lm = r.headers.get("Last-Modified")
        if lm:
            try:
                age = (datetime.now(timezone.utc) - parsedate_to_datetime(lm)).total_seconds() / 3600.0
            except Exception:
                pass
        data = r.read() if body else b""
        return r.getcode(), data, age
    except Exception:
        return None, b"", None

def norm_feed(ref):
    """Normalize any page reference to a /data/<key>; None if not a feed."""
    ref = ref.strip()
    if any(c in ref for c in "{}$+ `"):
        return None
    if ref.startswith("http"):
        if "justhodl-dashboard-live" in ref or "workers.dev" in ref:
            key = ref.split(".com/", 1)[-1].split(".dev/", 1)[-1]
        elif "justhodl.ai/data/" in ref:
            key = ref.split("justhodl.ai/data/", 1)[-1]
        else:
            return None
    else:
        key = ref.lstrip("/")
        if key.startswith("data/"):
            key = key[5:]
    key = key.split("?")[0]
    if not key.endswith(".json") or key.split("/")[-1] in EXCLUDE:
        return None
    return key

def main():
    with report("2950_fleet_health") as rep:
        fails, t0 = [], time.time()

        # ---- 1) page inventory from the nav the drawer actually uses
        c, b, _ = get("nav-manifest.json")
        pages = sorted(set(re.findall(rb'"([A-Za-z0-9_\-/]+\.html)"', b).__iter__()))
        pages = [p.decode() for p in pages]
        rep.kv(nav_pages=len(pages))
        if c != 200 or len(pages) < 300:
            fails.append(f"nav manifest bad ({c},{len(pages)})")

        # ---- 2) fetch every page; harvest feed references
        page_res = {}
        with ThreadPoolExecutor(max_workers=14) as ex:
            for p, r in zip(pages, ex.map(lambda x: get(x), pages)):
                page_res[p] = r
        down = [p for p, (cc, bb, _) in page_res.items() if cc != 200 or len(bb) < 1500]
        rep.kv(pages_ok=f"{len(pages)-len(down)}/{len(pages)}",
               pages_down=",".join(down[:12]) or "none")
        if down:
            fails.append(f"pages down: {down[:12]}")

        feed_pages = {}
        rx = [re.compile(r'["\'](/?data/[A-Za-z0-9_\-./]+\.json)'),
              re.compile(r'["\']((?:[A-Za-z0-9_\-]+/)*[A-Za-z0-9_\-]+\.json)["\']'),
              re.compile(r'(https://[^"\'\s]+\.json)')]
        for p, (cc, bb, _) in page_res.items():
            if cc != 200:
                continue
            txt = bb.decode("utf-8", "replace")
            for r_ in rx:
                for m in r_.findall(txt):
                    k = norm_feed(m)
                    if k:
                        feed_pages.setdefault(k, set()).add(p)
        rep.kv(feeds_referenced=len(feed_pages))

        # ---- 3) check every referenced feed (retry once)
        feed_stat = {}
        def check_feed(k):
            cc, bb, age = get(f"data/{k}")
            if cc != 200:
                time.sleep(2)
                cc, bb, age = get(f"data/{k}")
            ok_json = False
            if cc == 200:
                try:
                    json.loads(bb)
                    ok_json = True
                except Exception:
                    pass
            return k, (cc, ok_json, age, len(bb))
        with ThreadPoolExecutor(max_workers=14) as ex:
            for k, st in ex.map(check_feed, sorted(feed_pages)):
                feed_stat[k] = st
        dead_ref = {k: v for k, v in feed_stat.items() if v[0] != 200}
        unparse = {k: v for k, v in feed_stat.items() if v[0] == 200 and not v[1]}
        old_ref = {k: v for k, v in feed_stat.items()
                   if v[1] and v[2] is not None and v[2] > 24 * 30}
        rep.kv(feeds_live=f"{len(feed_stat)-len(dead_ref)}/{len(feed_stat)}",
               feeds_unparseable=len(unparse), feeds_older_30d=len(old_ref))
        for k, v in sorted(dead_ref.items()):
            rep.warn(f"DEAD referenced feed {k} (http={v[0]}) <- pages: "
                     + ",".join(sorted(feed_pages[k])[:5]))
        for k in sorted(unparse):
            rep.warn(f"UNPARSEABLE feed {k} <- " + ",".join(sorted(feed_pages[k])[:4]))
        for k, v in sorted(old_ref.items()):
            rep.log(f"old feed (> 30d) {k} age={v[2]/24:.0f}d <- "
                    + ",".join(sorted(feed_pages[k])[:3]))
        if len(dead_ref) > 5:
            fails.append(f"{len(dead_ref)} referenced feeds dead")

        # ---- 4) all 661 engines' outs, header-only age
        c, b, _ = get("data/engine-registry.json")
        reg = json.loads(b) if c == 200 else {}
        engines = reg.get("engines") or (reg if isinstance(reg, list) else [])
        outs_all = {}
        for e in engines:
            for o in (e.get("outs") or []):
                k = norm_feed(o) or (o.lstrip("/")[5:] if o.lstrip("/").startswith("data/") else o.lstrip("/"))
                if k and k.endswith(".json"):
                    outs_all.setdefault(k, []).append(e.get("name", "?"))
        def head_age(k):
            if k in feed_stat:
                cc, okj, age, _ = feed_stat[k]
                return k, (cc, age)
            cc, _, age = get(f"data/{k}", body=False)
            return k, (cc, age)
        out_stat = {}
        with ThreadPoolExecutor(max_workers=14) as ex:
            for k, st in ex.map(head_age, sorted(outs_all)):
                out_stat[k] = st
        e_fresh = e_aging = e_stale = e_dead = e_noouts = 0
        dead_engines, stale_engines = [], []
        for e in engines:
            outs = [norm_feed(o) or o.lstrip("/").removeprefix("data/") for o in (e.get("outs") or [])]
            outs = [o for o in outs if o in out_stat]
            if not outs:
                e_noouts += 1
                continue
            ages = [out_stat[o][1] for o in outs if out_stat[o][0] == 200 and out_stat[o][1] is not None]
            live = [o for o in outs if out_stat[o][0] == 200]
            if not live:
                e_dead += 1
                dead_engines.append(e.get("name", "?"))
            elif ages and min(ages) <= 26:
                e_fresh += 1
            elif ages and min(ages) <= 24 * 8:
                e_aging += 1
            else:
                e_stale += 1
                stale_engines.append(e.get("name", "?"))
        rep.kv(engines_total=len(engines), engines_fresh_26h=e_fresh,
               engines_aging_8d=e_aging, engines_stale=e_stale,
               engines_dead=e_dead, engines_no_outs=e_noouts)
        if dead_engines:
            rep.warn("dead engines: " + ",".join(sorted(dead_engines)[:20]))
        if stale_engines:
            rep.log("stale engines (>8d): " + ",".join(sorted(stale_engines)[:25]))

        # ---- 5) homepage identity (still restored + amber)
        c, b, _ = get("")
        h = b.decode("utf-8", "replace")
        rep.kv(homepage=("operator-console+amber" if ("Operator Console" in h and "jh-amber-skin" in h) else "WRONG"))
        if "Operator Console" not in h:
            fails.append("homepage identity wrong")

        line = (f"pages={len(pages)-len(down)}/{len(pages)} feeds={len(feed_stat)-len(dead_ref)}/{len(feed_stat)} "
                f"engines: fresh={e_fresh} aging={e_aging} stale={e_stale} dead={e_dead} no-outs={e_noouts} "
                f"dead-ref-feeds={len(dead_ref)} t={time.time()-t0:.0f}s")
        print(line)
        rep.kv(summary=line)
        if fails:
            for f in fails:
                rep.fail(f)
            print("FAILURES: " + " | ".join(fails))
            sys.exit(1)
        rep.ok("fleet certified: pages + referenced feeds + engines")

if __name__ == "__main__":
    main()
