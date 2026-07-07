#!/usr/bin/env python3
"""ops 2950 v2 — FLEET CERTIFICATION: pages, page-referenced feeds, all engines.
Live production, read-only. v2 fixes: registry dict-shape normalizer (per
bake_engine_directory), engines.html excluded from harvesting (it lists every
feed by design), worker-URL data/ prefix strip, throttled fetches with backoff
so Cloudflare stops eating the sweep (v1's http=None wall)."""
import json, re, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from ops_report import report

BASE = "https://justhodl.ai"
UA = {"User-Agent": "Mozilla/5.0 jh-ops"}
EXCLUDE = {"manifest.json", "nav-manifest.json", "engine-wiring.json",
           "package.json", "engine-registry.json"}

def get(path, to=12, body=True, tries=3):
    for i in range(tries):
        try:
            sep = "&" if "?" in path else "?"
            r = urllib.request.urlopen(urllib.request.Request(
                f"{BASE}/{path}{sep}_={int(time.time()*1000)}", headers=UA), timeout=to)
            age = None
            lm = r.headers.get("Last-Modified")
            if lm:
                try:
                    age = (datetime.now(timezone.utc) - parsedate_to_datetime(lm)).total_seconds() / 3600.0
                except Exception:
                    pass
            return r.getcode(), (r.read() if body else b""), age
        except Exception:
            if i < tries - 1:
                time.sleep(4 + 5 * i)
    return None, b"", None

def norm_feed(ref):
    ref = ref.strip()
    if any(c in ref for c in "{}$+ `") or ".." in ref:
        return None
    if ref.startswith("http"):
        if "justhodl-dashboard-live" in ref:
            key = ref.split(".com/", 1)[-1]
        elif "workers.dev" in ref:
            key = ref.split(".dev/", 1)[-1]
        elif "justhodl.ai/data/" in ref:
            key = ref.split("justhodl.ai/data/", 1)[-1]
        else:
            return None
        key = key.lstrip("/")
        if key.startswith("data/"):
            key = key[5:]
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

        c, b, _ = get("nav-manifest.json")
        pages = sorted({m.decode() for m in re.findall(rb'"([A-Za-z0-9_\-/]+\.html)"', b)})
        rep.kv(nav_pages=len(pages))
        print(f"nav pages: {len(pages)}")
        if c != 200 or len(pages) < 300:
            fails.append(f"nav manifest bad ({c},{len(pages)})")

        page_res = {}
        with ThreadPoolExecutor(max_workers=10) as ex:
            for p, r in zip(pages, ex.map(lambda x: get(x, tries=2), pages)):
                page_res[p] = r
        down = [p for p, (cc, bb, _) in page_res.items() if cc != 200 or len(bb) < 1500]
        print(f"pages ok: {len(pages)-len(down)}/{len(pages)} down={down[:12] or 'none'}")
        rep.kv(pages_ok=f"{len(pages)-len(down)}/{len(pages)}",
               pages_down=",".join(down[:12]) or "none")
        if down:
            fails.append(f"pages down: {down[:12]}")

        feed_pages = {}
        rx = [re.compile(r'["\'](/?data/[A-Za-z0-9_\-./]+\.json)'),
              re.compile(r'["\']((?:[A-Za-z0-9_\-]+/)*[A-Za-z0-9_\-]+\.json)["\']'),
              re.compile(r'(https://[^"\'\s]+\.json)')]
        for p, (cc, bb, _) in page_res.items():
            if cc != 200 or p.lstrip("/") == "engines.html":
                continue  # engines.html lists every feed by design
            txt = bb.decode("utf-8", "replace")
            for r_ in rx:
                for m in r_.findall(txt):
                    k = norm_feed(m)
                    if k:
                        feed_pages.setdefault(k, set()).add(p)
        print(f"feeds referenced by pages: {len(feed_pages)}")
        rep.kv(feeds_referenced=len(feed_pages))

        feed_stat = {}
        def check_feed(k):
            cc, bb, age = get(f"data/{k}")
            ok_json = False
            if cc == 200:
                try:
                    json.loads(bb)
                    ok_json = True
                except Exception:
                    pass
            return k, (cc, ok_json, age, len(bb))
        with ThreadPoolExecutor(max_workers=6) as ex:
            for k, st in ex.map(check_feed, sorted(feed_pages)):
                feed_stat[k] = st
        dead_ref = {k: v for k, v in feed_stat.items() if v[0] != 200}
        unparse = {k: v for k, v in feed_stat.items() if v[0] == 200 and not v[1]}
        print(f"feeds live: {len(feed_stat)-len(dead_ref)}/{len(feed_stat)} dead={len(dead_ref)} unparseable={len(unparse)}")
        rep.kv(feeds_live=f"{len(feed_stat)-len(dead_ref)}/{len(feed_stat)}",
               feeds_unparseable=len(unparse))
        for k, v in sorted(dead_ref.items()):
            rep.warn(f"DEAD referenced feed {k} (http={v[0]}) <- " + ",".join(sorted(feed_pages[k])[:5]))
        for k in sorted(unparse):
            rep.warn(f"UNPARSEABLE feed {k} <- " + ",".join(sorted(feed_pages[k])[:4]))
        if len(dead_ref) > 5:
            fails.append(f"{len(dead_ref)} referenced feeds dead")

        c, b, _ = get("data/engine-registry.json")
        reg = json.loads(b) if c == 200 else {}
        raw = reg.get("engines", reg) if isinstance(reg, dict) else reg
        entries = ({k: {**(v if isinstance(v, dict) else {}), "name": k} for k, v in raw.items()}
                   if isinstance(raw, dict) else {e.get("name", str(i)): e for i, e in enumerate(raw)})
        outs_all = set()
        for e in entries.values():
            for o in (e.get("outs") or []):
                k = norm_feed(o)
                if k:
                    outs_all.add(k)
        todo = sorted(outs_all - set(feed_stat))
        print(f"engines: {len(entries)} | unique outs: {len(outs_all)} (new to check: {len(todo)})")
        def head_age(k):
            cc, _, age = get(f"data/{k}", body=False)
            return k, (cc, True, age, 0)
        with ThreadPoolExecutor(max_workers=6) as ex:
            for k, st in ex.map(head_age, todo):
                feed_stat[k] = st
        e_fresh = e_aging = e_stale = e_dead = e_noouts = 0
        dead_engines, stale_engines = [], []
        for name, e in entries.items():
            outs = [norm_feed(o) for o in (e.get("outs") or [])]
            outs = [o for o in outs if o in feed_stat]
            if not outs:
                e_noouts += 1
                continue
            live = [o for o in outs if feed_stat[o][0] == 200]
            ages = [feed_stat[o][2] for o in live if feed_stat[o][2] is not None]
            if not live:
                e_dead += 1
                dead_engines.append(name)
            elif ages and min(ages) <= 26:
                e_fresh += 1
            elif ages and min(ages) <= 24 * 8:
                e_aging += 1
            else:
                e_stale += 1
                stale_engines.append(name)
        print(f"engines fresh={e_fresh} aging={e_aging} stale={e_stale} dead={e_dead} no-outs={e_noouts}")
        rep.kv(engines_total=len(entries), engines_fresh_26h=e_fresh,
               engines_aging_8d=e_aging, engines_stale=e_stale,
               engines_dead=e_dead, engines_no_outs=e_noouts)
        if dead_engines:
            rep.warn("dead engines: " + ",".join(sorted(dead_engines)[:24]))
        if stale_engines:
            rep.log("stale engines (>8d): " + ",".join(sorted(stale_engines)[:30]))

        c, b, _ = get("")
        h = b.decode("utf-8", "replace")
        rep.kv(homepage=("operator-console+amber" if ("Operator Console" in h and "jh-amber-skin" in h) else "WRONG"))
        if "Operator Console" not in h:
            fails.append("homepage identity wrong")

        line = (f"pages={len(pages)-len(down)}/{len(pages)} ref-feeds={len(feed_pages)-len(dead_ref)}/{len(feed_pages)} "
                f"engines fresh={e_fresh} aging={e_aging} stale={e_stale} dead={e_dead} no-outs={e_noouts} t={time.time()-t0:.0f}s")
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
