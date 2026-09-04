#!/usr/bin/env python3
"""scripts/build_section_registry.py — ops 5200. The fleet-wide section registry.

Renders every page in headless Chromium (Playwright), lets jh-sections.js
number the page's real blocks after its own JS has rendered, and merges the
result into config/section-registry.json APPEND-ONLY:

  * a key that already has a number keeps it forever;
  * a new key takes the next free number (top level: max+1; sub-panel:
    parent.n + "." + next free index);
  * a key that stops rendering keeps its number for RETAIN_DAYS (a page
    that failed to load once must not renumber everything) and is then
    dropped, its number never reused.

The homepage resolves `page#n` against this file (and against a live
discovery frame when a page is not indexed yet); bake_sections.py bakes each
page's own key→number map into the page so the numbers a user sees are the
registry's, not a fresh DOM-order count.

Usage:  build_section_registry.py [--base https://justhodl.ai] [--pages a,b,c]
        [--workers 6] [--registry config/section-registry.json] [--json out.json]
"""
import argparse, json, os, re, sys, time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RETAIN_DAYS = 14


def utcnow():
    return datetime.now(timezone.utc)


def load_pages(base_root):
    pages = []
    try:
        nav = json.load(open(os.path.join(base_root, "nav-manifest.json"), encoding="utf-8"))
        for c in nav.get("categories", []):
            for p in c.get("pages", []):
                href = p.get("href") or ""
                key = re.sub(r"\.html?$", "", re.sub(r"/index\.html?$", "", href)).strip("/")
                if key and key != "index" and not key.startswith("screener"):
                    pages.append((key, p.get("title") or key))
    except Exception as e:  # noqa: BLE001
        print("nav-manifest unavailable:", e)
    pages.append(("index", "Command Desk"))
    seen, out = set(), []
    for k, t in pages:
        if k not in seen:
            seen.add(k); out.append((k, t))
    return out


def crawl(base, pages, workers=4, settle_ms=4500, per_page_timeout=45000, log=print):
    """One Playwright + one Chromium per worker THREAD (the sync API is per-thread), each walking a slice."""
    try:
        import playwright  # noqa: F401
    except Exception:
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", "playwright"], check=True)
    from playwright.sync_api import sync_playwright
    import subprocess, threading
    results, lock = {}, threading.Lock()

    def launch(p):
        try:
            return p.chromium.launch(channel="chrome", headless=True)
        except Exception:
            subprocess.run([sys.executable, "-m", "playwright", "install", "chromium", "--with-deps"], check=False)
            return p.chromium.launch(headless=True)

    def one(browser, key, title):
        ctx = browser.new_context(viewport={"width": 1280, "height": 900})
        pg = ctx.new_page()
        pg.set_default_timeout(per_page_timeout)
        rec = {"title": title, "sections": [], "ok": False, "ms": 0, "error": ""}
        t0 = time.time()
        try:
            pg.goto(f"{base}/{key}.html?jhdiscover=1&nocache={int(t0)}", wait_until="domcontentloaded")
            try:
                pg.wait_for_load_state("networkidle", timeout=12000)
            except Exception:
                pass
            pg.wait_for_timeout(settle_ms)
            data = pg.evaluate("""() => { try { window.JustHodlSections && window.JustHodlSections.canonical(); } catch (e) {}
                return { title: document.title, sections: window.JH_SECTIONS || null, loaded: !!window.__jhSectionsLoaded }; }""")
            rec["title"] = (data.get("title") or title).strip()[:120]
            if data.get("sections") is None:
                rec["error"] = "jh-sections not loaded" if not data.get("loaded") else "no sections"
            else:
                rec["sections"] = data["sections"]; rec["ok"] = True
        except Exception as e:  # noqa: BLE001
            rec["error"] = str(e).splitlines()[0][:160]
        rec["ms"] = int((time.time() - t0) * 1000)
        try:
            ctx.close()
        except Exception:
            pass
        return rec

    def walk(items):
        with sync_playwright() as p:
            browser = launch(p)
            for key, title in items:
                rec = one(browser, key, title)
                with lock:
                    results[key] = rec
                    log(f"  {'ok ' if rec['ok'] else 'ERR'} {key:44s} {len(rec['sections']):3d} sections {rec['ms']:6d}ms {rec['error']}")
            browser.close()

    # make sure the browser binary exists before threads race to install it
    with sync_playwright() as p:
        launch(p).close()
    threads = [threading.Thread(target=walk, args=(pages[i::workers],), daemon=True) for i in range(max(1, workers))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results


def merge(registry, crawled, now=None):
    now = now or utcnow()
    pages = registry.setdefault("pages", {})
    stats = {"pages": 0, "new_sections": 0, "kept": 0, "dropped": 0, "failed": 0}
    for key, rec in crawled.items():
        if not rec.get("ok"):
            stats["failed"] += 1
            continue
        stats["pages"] += 1
        entry = pages.get(key) or {"title": rec.get("title") or key, "sections": [], "next": 1}
        entry["title"] = rec.get("title") or entry.get("title") or key
        by_key = {s["key"]: s for s in entry.get("sections", [])}
        seen_top = set()
        for s in rec["sections"]:
            k = s.get("key") or ""
            if not k:
                continue
            seen_top.add(k)
            ex = by_key.get(k)
            if ex is None:
                n = str(entry.get("next", 1))
                taken = {str(x["n"]) for x in entry["sections"]}
                while n in taken:
                    n = str(int(n) + 1)
                ex = {"n": n, "key": k, "title": s.get("title") or k, "id": s.get("id") or "", "sub": [], "first_seen": now.date().isoformat()}
                entry["sections"].append(ex); by_key[k] = ex
                entry["next"] = int(n) + 1
                stats["new_sections"] += 1
            else:
                stats["kept"] += 1
                ex["title"] = s.get("title") or ex.get("title") or k
                ex["id"] = s.get("id") or ex.get("id") or ""
            ex.pop("missing_since", None)
            sub_by = {x["key"]: x for x in ex.get("sub", [])}
            seen_sub = set()
            for x in s.get("sub", []) or []:
                sk = x.get("key") or ""
                if not sk:
                    continue
                seen_sub.add(sk)
                sx = sub_by.get(sk)
                if sx is None:
                    taken = {str(y["n"]) for y in ex["sub"]}
                    i = 1
                    while f"{ex['n']}.{i}" in taken:
                        i += 1
                    sx = {"n": f"{ex['n']}.{i}", "key": sk, "title": x.get("title") or sk, "id": x.get("id") or "", "first_seen": now.date().isoformat()}
                    ex["sub"].append(sx); sub_by[sk] = sx
                    stats["new_sections"] += 1
                else:
                    sx["title"] = x.get("title") or sx.get("title") or sk
                    sx["id"] = x.get("id") or sx.get("id") or ""
                sx.pop("missing_since", None)
            for sx in list(ex["sub"]):
                if sx["key"] not in seen_sub:
                    ms = sx.get("missing_since") or now.date().isoformat()
                    sx["missing_since"] = ms
                    if datetime.fromisoformat(ms).replace(tzinfo=timezone.utc) < now - timedelta(days=RETAIN_DAYS):
                        ex["sub"].remove(sx); stats["dropped"] += 1
        for ex in list(entry["sections"]):
            if ex["key"] not in seen_top:
                ms = ex.get("missing_since") or now.date().isoformat()
                ex["missing_since"] = ms
                if datetime.fromisoformat(ms).replace(tzinfo=timezone.utc) < now - timedelta(days=RETAIN_DAYS):
                    entry["sections"].remove(ex); stats["dropped"] += 1
        entry["sections"].sort(key=lambda x: int(str(x["n"]).split(".")[0]))
        for ex in entry["sections"]:
            ex["sub"].sort(key=lambda x: [int(t) for t in str(x["n"]).split(".")])
        entry["crawled_at"] = now.isoformat(timespec="seconds")
        pages[key] = entry
    registry["version"] = 1
    registry["generated_at"] = now.isoformat(timespec="seconds")
    registry["n_pages"] = len(pages)
    registry["n_sections"] = sum(len(p.get("sections", [])) for p in pages.values())
    registry["n_panels"] = sum(len(s.get("sub", [])) for p in pages.values() for s in p.get("sections", []))
    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="https://justhodl.ai")
    ap.add_argument("--pages", default="")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--registry", default=os.path.join(ROOT, "config", "section-registry.json"))
    ap.add_argument("--json", default="")
    a = ap.parse_args()
    pages = load_pages(ROOT)
    if a.pages.strip():
        want = set(x.strip() for x in a.pages.split(",") if x.strip())
        pages = [p for p in pages if p[0] in want] or [(k, k) for k in want]
    try:
        registry = json.load(open(a.registry, encoding="utf-8"))
    except Exception:
        registry = {"version": 1, "pages": {}}
    print(f"crawling {len(pages)} pages from {a.base}")
    crawled = crawl(a.base, pages, workers=a.workers)
    stats = merge(registry, crawled)
    os.makedirs(os.path.dirname(a.registry), exist_ok=True)
    json.dump(registry, open(a.registry, "w", encoding="utf-8"), indent=1, ensure_ascii=False)
    if a.json:
        json.dump({"stats": stats, "crawled": crawled}, open(a.json, "w", encoding="utf-8"), indent=1)
    print("registry:", json.dumps(stats), "->", a.registry)


if __name__ == "__main__":
    main()
