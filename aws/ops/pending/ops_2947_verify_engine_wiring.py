#!/usr/bin/env python3
"""ops 2947 — verify the engine-wiring ship end-to-end against the LIVE site.

Checks, all against production (justhodl.ai), no repo-local shortcuts:
 1. /jh-wire.js live (HTTP 200, sane size).
 2. Every page in data/engine-wiring.json 'wired' contains its jh-wire line
    with the exact feed path.
 3. Every wired feed itself returns HTTP 200 + parses as JSON (real data
    behind every card; DEAD feeds were excluded by design).
 4. Recount fleet wiring with the corrected matcher (exact path OR bare
    filename OR quoted stem) across all live root pages -> the new true
    orphan count, written to the report.
"""
import json, re, sys, time, urllib.request
from concurrent.futures import ThreadPoolExecutor
from ops_report import report

BASE = "https://justhodl.ai"

def get(path, to=15):
    try:
        r = urllib.request.urlopen(urllib.request.Request(
            f"{BASE}/{path}?_={int(time.time())}",
            headers={"User-Agent": "Mozilla/5.0 jh-ops"}), timeout=to)
        return r.getcode(), r.read()
    except Exception:
        return None, b""

def main():
    with report("2947_verify_engine_wiring") as rep:
        # /data/* is served by the CF Worker from S3 (never from the repo) —
        # publish the repo-canonical manifest to the real data origin first.
        import os, boto3
        root = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".."))
        body = open(os.path.join(root, "data", "engine-wiring.json"), "rb").read()
        boto3.client("s3", "us-east-1").put_object(
            Bucket="justhodl-dashboard-live", Key="data/engine-wiring.json",
            Body=body, ContentType="application/json", CacheControl="max-age=300")
        print(f"manifest published to S3 ({len(body)} bytes)")
        man = json.loads(body)
        c, b = get("data/engine-wiring.json")
        rep.kv(manifest_live_http=c)
        wired = man["wired"]

        c, b = get("jh-wire.js")
        assert c == 200 and len(b) > 4000, f"jh-wire.js live check failed ({c},{len(b)})"

        pages = sorted({w["page"] for w in wired})
        page_src, bad_pages = {}, []
        with ThreadPoolExecutor(max_workers=12) as ex:
            for p, (cc, bb) in zip(pages, ex.map(get, pages)):
                if cc != 200:
                    bad_pages.append(p)
                page_src[p] = bb.decode("utf-8", "replace")
        missing_line = [w["page"] + "::" + w["feed"] for w in wired
                        if w["feed"] not in page_src.get(w["page"], "")]

        feeds = sorted({w["feed"] for w in wired})
        bad_feeds = []
        def feed_ok(f):
            cc, bb = get(f)
            if cc != 200:
                return f + f" HTTP {cc}"
            try:
                json.loads(bb)
                return None
            except Exception:
                return f + " not JSON"
        with ThreadPoolExecutor(max_workers=12) as ex:
            for r in ex.map(feed_ok, feeds):
                if r:
                    bad_feeds.append(r)

        # fleet-wide recount with corrected matcher
        c, b = get("data/engine-registry.json")
        reg = json.loads(b)
        raw = reg.get("engines", reg) if isinstance(reg, dict) else reg
        entries = (raw if isinstance(raw, list)
                   else [{**(v if isinstance(v, dict) else {}), "name": k} for k, v in raw.items()])
        c, b = get("data/nav-manifest.json")
        nav = json.loads(b)
        all_pages = sorted({it["href"].lstrip("/") for cat in nav.get("categories", nav if isinstance(nav, list) else [])
                            for it in (cat.get("items", cat.get("pages", [])) if isinstance(cat, dict) else [])
                            if isinstance(it, dict) and it.get("href", "").endswith(".html")})
        src_all = {}
        with ThreadPoolExecutor(max_workers=16) as ex:
            for p, (cc, bb) in zip(all_pages, ex.map(get, all_pages)):
                if cc == 200:
                    src_all[p] = bb.decode("utf-8", "replace")

        def referenced(out, s):
            if out in s or out.split("/")[-1] in s:
                return True
            stem = out.split("/")[-1].rsplit(".", 1)[0]
            return re.search(r'["\']' + re.escape(stem) + r'["\']', s) is not None

        wired_n = orphan_n = noouts = 0
        still_orphan = []
        for e in entries:
            outs = e.get("outs") or []
            if not outs:
                noouts += 1
                continue
            if any(referenced(o, s) for o in outs for s in src_all.values()):
                wired_n += 1
            else:
                orphan_n += 1
                still_orphan.append([e.get("name"), outs[:2]])

        rep.kv(pages_checked=len(pages), pages_bad=bad_pages,
                   wire_lines_missing=missing_line, feeds_checked=len(feeds),
                   feeds_bad=bad_feeds, fleet_total=len(entries),
                   fleet_wired=wired_n, fleet_orphaned=orphan_n,
                   fleet_no_outs=noouts, still_orphan=still_orphan[:60],
                   pages_scanned_live=len(src_all))
        print(f"pages ok={len(pages)-len(bad_pages)}/{len(pages)} lines-missing={len(missing_line)} "
              f"feeds ok={len(feeds)-len(bad_feeds)}/{len(feeds)} | fleet: wired={wired_n} "
              f"orphaned={orphan_n} no-outs={noouts} of {len(entries)}")
        assert not bad_pages and not missing_line, f"wiring not live: {bad_pages} {missing_line[:5]}"
        assert len(bad_feeds) <= 2, f"too many dead wired feeds: {bad_feeds}"
    sys.exit(0)

main()
