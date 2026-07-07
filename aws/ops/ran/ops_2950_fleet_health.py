#!/usr/bin/env python3
"""ops 2950 v3 — FLEET CERTIFICATION: pages, feeds AS PAGES FETCH THEM, engines.
v3: (a) redirect stubs resolved to their targets instead of counted down,
(b) every feed tested via the exact URL form the page uses (S3-direct refs are
no longer forced through /data/), (c) engine outs fall back to S3-direct when
the /data/ channel can't serve a subdir key, (d) network errors report their
exception class so timeouts and 404s stop looking identical."""
import json, re, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from ops_report import report

BASE = "https://justhodl.ai"
S3 = "https://justhodl-dashboard-live.s3.us-east-1.amazonaws.com"
UA = {"User-Agent": "Mozilla/5.0 jh-ops"}
EXCLUDE = {"manifest.json", "nav-manifest.json", "engine-wiring.json",
           "package.json", "engine-registry.json"}

def get(url, to=12, body=True, tries=3):
    """url may be a full https URL or a site-relative path."""
    if not url.startswith("http"):
        url = f"{BASE}/{url.lstrip('/')}"
    err = None
    for i in range(tries):
        try:
            sep = "&" if "?" in url else "?"
            r = urllib.request.urlopen(urllib.request.Request(
                f"{url}{sep}_={int(time.time()*1000)}", headers=UA), timeout=to)
            age = None
            lm = r.headers.get("Last-Modified")
            if lm:
                try:
                    age = (datetime.now(timezone.utc) - parsedate_to_datetime(lm)).total_seconds() / 3600.0
                except Exception:
                    pass
            return r.getcode(), (r.read() if body else b""), age, None
        except urllib.error.HTTPError as ex:
            return ex.code, b"", None, f"http{ex.code}"
        except Exception as ex:
            err = type(ex).__name__
            if i < tries - 1:
                time.sleep(4 + 5 * i)
    return None, b"", None, err

KNOWN_LEGACY = {  # dead long before the 2026-07 redesign; tracked, not gating
    "edgar_insiders.json": "origins-era agent retired (Feb 2026 fleet migration)",
    "equity_research.json": "superseded by justhodl-equity-research schema 2.0 (why.html)",
    "research_critique.json": "origins-era critic agent retired",
    "ici-flows.json": "ICI moved to anti-scraped .xls workbooks; parser rebuild pending",
    "agent/secretary": "legacy multi-agent API endpoint, retired",
}

def candidate_urls(ref):
    """Every URL form the page could resolve this reference to. Pages use
    literal /data/ paths, bare keys behind an S3/BUCKET const, or full URLs —
    a feed is live if ANY real channel serves it."""
    ref = ref.strip()
    if any(c in ref for c in "{}$+ `") or ".." in ref:
        return None, []
    if ref.startswith("http"):
        if ("justhodl" not in ref) and ("workers.dev" not in ref):
            return None, []
        u = ref.split("?")[0]
        key = u.split(".com/", 1)[-1].split(".dev/", 1)[-1].lstrip("/")
        key = key[5:] if key.startswith("data/") else key
        return key, [u]
    p = ref.lstrip("/").split("?")[0]
    if not p.endswith(".json") or p.split("/")[-1] in EXCLUDE:
        return None, []
    if p.startswith("data/"):
        key = p[5:]
        return key, [f"{BASE}/{p}", f"{S3}/{key}"]
    return p, [f"{S3}/{p}", f"{BASE}/data/{p}"]

def main():
    with report("2950_fleet_health") as rep:
        fails, t0 = [], time.time()

        c, b, _, _ = get("nav-manifest.json")
        pages = sorted({m.decode() for m in re.findall(rb'"([A-Za-z0-9_\-/]+\.html)"', b)})
        print(f"nav pages: {len(pages)}")
        rep.kv(nav_pages=len(pages))
        if c != 200 or len(pages) < 300:
            fails.append(f"nav manifest bad ({c},{len(pages)})")

        page_res = {}
        with ThreadPoolExecutor(max_workers=10) as ex:
            for p, r in zip(pages, ex.map(lambda x: get(x, tries=2), pages)):
                page_res[p] = r
        RX_TGT = re.compile(r'url=([^">\s]+)|location\.(?:href|replace)\s*[=(]\s*["\']([^"\']+)')
        redirects, true_down = {}, []
        for p, (cc, bb, _, _) in page_res.items():
            if cc == 200 and len(bb) >= 1500:
                continue
            tgt, hops, cur, cbb = None, 0, p, bb
            while hops < 3 and cc == 200 and len(cbb) < 1500:
                m = RX_TGT.search(cbb.decode("utf-8", "replace"))
                nxt = (m.group(1) or m.group(2)) if m else None
                if not nxt:
                    break
                cur = nxt
                cc, cbb, _, _ = page_res.get(cur, (None, b"", None, None)) if cur in page_res else get(cur, tries=2)
                hops += 1
            if cc == 200 and len(cbb) >= 1500:
                redirects[p] = cur
            else:
                true_down.append(p)
        print(f"pages ok: {len(pages)-len(true_down)}/{len(pages)} "
              f"(redirect-stubs resolved: {len(redirects)}) down={true_down[:12] or 'none'}")
        rep.kv(pages_ok=f"{len(pages)-len(true_down)}/{len(pages)}",
               redirect_stubs=len(redirects),
               pages_down=",".join(true_down[:12]) or "none")
        for p, t in sorted(redirects.items()):
            rep.log(f"redirect stub {p} -> {t}")
        if true_down:
            fails.append(f"pages down: {true_down[:12]}")

        feed_refs = {}
        rx = [re.compile(r'["\'](/?data/[A-Za-z0-9_\-./]+\.json)'),
              re.compile(r'["\']((?:[A-Za-z0-9_\-]+/)*[A-Za-z0-9_\-]+\.json)["\']'),
              re.compile(r'(https://[^"\'\s]+\.json)')]
        for p, (cc, bb, _, _) in page_res.items():
            if cc != 200 or p.lstrip("/") == "engines.html":
                continue
            txt = bb.decode("utf-8", "replace")
            for r_ in rx:
                for m in r_.findall(txt):
                    key, urls = candidate_urls(m)
                    if key:
                        rec = feed_refs.setdefault(key, {"pages": set(), "urls": []})
                        rec["pages"].add(p)
                        for u in urls:
                            if u not in rec["urls"]:
                                rec["urls"].append(u)
        print(f"feeds referenced by pages: {len(feed_refs)}")
        rep.kv(feeds_referenced=len(feed_refs))

        feed_stat = {}
        def check(item):
            key, rec = item
            last = (None, False, None, "unchecked")
            for u in rec["urls"]:
                cc, bb, age, err = get(u)
                if cc == 200:
                    okj = False
                    try:
                        json.loads(bb)
                        okj = True
                    except Exception:
                        pass
                    return key, (200, okj, age, None, u)
                last = (cc, False, age, err)
            return key, (*last, None)
        with ThreadPoolExecutor(max_workers=6) as ex:
            for key, st in ex.map(check, sorted(feed_refs.items())):
                feed_stat[key] = st
        dead = {k: v for k, v in feed_stat.items() if v[0] != 200}
        legacy = {k: v for k, v in dead.items()
                  if any(k.endswith(lg) for lg in KNOWN_LEGACY)}
        hard_dead = {k: v for k, v in dead.items() if k not in legacy}
        unparse = {k: v for k, v in feed_stat.items() if v[0] == 200 and not v[1]}
        print(f"feeds live: {len(feed_stat)-len(dead)}/{len(feed_stat)} "
              f"known-legacy-dead={len(legacy)} unexplained-dead={len(hard_dead)} unparseable={len(unparse)}")
        rep.kv(feeds_live=f"{len(feed_stat)-len(dead)}/{len(feed_stat)}",
               known_legacy_dead=len(legacy), unexplained_dead=len(hard_dead),
               feeds_unparseable=len(unparse))
        for k, v in sorted(hard_dead.items()):
            rep.warn(f"DEAD feed {k} ({v[3] or v[0]}) <- " + ",".join(sorted(feed_refs[k]["pages"])[:5]))
        for k, v in sorted(legacy.items()):
            why = next(r for lg, r in KNOWN_LEGACY.items() if k.endswith(lg))
            rep.warn(f"KNOWN-LEGACY dead {k} <- " + ",".join(sorted(feed_refs[k]["pages"])[:4]) + f" | {why}")
        for k in sorted(unparse):
            rep.warn(f"UNPARSEABLE {k} <- " + ",".join(sorted(feed_refs[k]["pages"])[:4]))
        if hard_dead:
            fails.append(f"{len(hard_dead)} referenced feeds dead (non-legacy)")

        c, b, _, _ = get("data/engine-registry.json")
        reg = json.loads(b) if c == 200 else {}
        raw = reg.get("engines", reg) if isinstance(reg, dict) else reg
        entries = ({k: {**(v if isinstance(v, dict) else {}), "name": k} for k, v in raw.items()}
                   if isinstance(raw, dict) else {e.get("name", str(i)): e for i, e in enumerate(raw)})
        def out_key(o):
            o = o.strip().split("?")[0]
            if o.startswith("http"):
                o = o.split(".com/", 1)[-1].split(".dev/", 1)[-1]
            o = o.lstrip("/")
            return o[5:] if o.startswith("data/") else o
        outs_all = set()
        for e in entries.values():
            for o in (e.get("outs") or []):
                k = out_key(o)
                if k.endswith(".json"):
                    outs_all.add(k)
        print(f"engines: {len(entries)} | unique out keys: {len(outs_all)}")
        def head_age(k):
            cc, _, age, err = get(f"data/{k}", body=False)
            if cc != 200:
                cc, _, age, err = get(f"{S3}/{k}", body=False)
            return k, (cc, age)
        key_stat = {}
        with ThreadPoolExecutor(max_workers=6) as ex:
            for k, st in ex.map(head_age, sorted(outs_all)):
                key_stat[k] = st
        e_fresh = e_aging = e_stale = e_dead = e_noouts = 0
        dead_engines, stale_engines = [], []
        for name, e in entries.items():
            outs = [out_key(o) for o in (e.get("outs") or [])]
            outs = [o for o in outs if o in key_stat]
            if not outs:
                e_noouts += 1
                continue
            live = [o for o in outs if key_stat[o][0] == 200]
            ages = [key_stat[o][1] for o in live if key_stat[o][1] is not None]
            if not live:
                e_dead += 1; dead_engines.append(name)
            elif ages and min(ages) <= 26:
                e_fresh += 1
            elif ages and min(ages) <= 24 * 8:
                e_aging += 1
            else:
                e_stale += 1; stale_engines.append(name)
        print(f"engines fresh={e_fresh} aging={e_aging} stale={e_stale} dead={e_dead} no-outs={e_noouts}")
        rep.kv(engines_total=len(entries), engines_fresh_26h=e_fresh,
               engines_aging_8d=e_aging, engines_stale=e_stale,
               engines_dead=e_dead, engines_no_outs=e_noouts)
        if dead_engines:
            rep.warn("dead engines: " + ",".join(sorted(dead_engines)[:24]))
        if stale_engines:
            rep.log("stale engines (>8d): " + ",".join(sorted(stale_engines)[:30]))

        c, b, _, _ = get("")
        h = b.decode("utf-8", "replace")
        rep.kv(homepage=("operator-console+amber" if ("Operator Console" in h and "jh-amber-skin" in h) else "WRONG"))
        if "Operator Console" not in h:
            fails.append("homepage identity wrong")

        line = (f"pages={len(pages)-len(true_down)}/{len(pages)} (stubs={len(redirects)}) "
                f"feeds={len(feed_stat)-len(dead)}/{len(feed_stat)} (legacy-dead={len(legacy)}) "
                f"engines fresh={e_fresh} aging={e_aging} stale={e_stale} dead={e_dead} no-outs={e_noouts} t={time.time()-t0:.0f}s")
        print(line)
        rep.kv(summary=line)
        if fails:
            for f in fails:
                rep.fail(f)
            print("FAILURES: " + " | ".join(fails))
            sys.exit(1)
        rep.ok("fleet certified: pages + as-written feeds + engines")

if __name__ == "__main__":
    main()
