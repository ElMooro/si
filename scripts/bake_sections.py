#!/usr/bin/env python3
"""scripts/bake_sections.py — ops 5200. Fleet-wide section numbering.

Runs on the _site artifact in pages.yml (after build_workspaces, before
stamp_assets so /jh-sections.js gets its content hash):

  1. injects <script src="/jh-sections.js" defer> into every HTML page that
     does not carry it (idempotent; skips redirect stubs, /screener PROTECTED,
     partials, and pages opting out with data-jh-sections="off");
  2. bakes THIS page's key→number map from config/section-registry.json into
     window.JH_SECTION_MAP so numbers are stable across deploys and never
     shift when a section is added (append-only registry);
  3. ships config/section-registry.json and config/home-layout.json into
     _site/config/ for the homepage (config/ is otherwise not deployed).

Never fails the deploy: any per-file error is logged and skipped.
"""
import json, os, re, shutil, sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SITE = sys.argv[1] if len(sys.argv) > 1 else "_site"
TAG = '<script src="/jh-sections.js" defer></script>'
SKIP_DIRS = ("screener", "_partials", "node_modules")


def page_key(rel):
    rel = rel.replace(os.sep, "/")
    rel = re.sub(r"/index\.html?$", "", rel)
    rel = re.sub(r"\.html?$", "", rel)
    return rel or "index"


def main():
    reg_path = os.path.join(ROOT, "config", "section-registry.json")
    registry = {}
    try:
        registry = json.load(open(reg_path, encoding="utf-8")).get("pages", {})
    except Exception as e:  # noqa: BLE001
        print(f"bake_sections: registry unavailable ({e}); numbering will be DOM-order until the first crawl")
    os.makedirs(os.path.join(SITE, "config"), exist_ok=True)
    for name in ("section-registry.json", "home-layout.json"):
        src = os.path.join(ROOT, "config", name)
        if os.path.exists(src):
            shutil.copyfile(src, os.path.join(SITE, "config", name))
    injected = baked = skipped = 0
    for dirpath, dirnames, files in os.walk(SITE):
        rel_dir = os.path.relpath(dirpath, SITE).replace(os.sep, "/")
        if rel_dir == ".":
            rel_dir = ""
        if any(part in SKIP_DIRS for part in rel_dir.split("/") if part):
            continue
        for fn in files:
            if not fn.endswith((".html", ".htm")):
                continue
            path = os.path.join(dirpath, fn)
            try:
                text = open(path, encoding="utf-8", errors="replace").read()
            except Exception:
                continue
            low = text.lower()
            if len(text) < 1500 and ("http-equiv=\"refresh\"" in low or "location.replace" in low or "location.href" in low):
                skipped += 1
                continue
            if 'data-jh-sections="off"' in text or "<html" not in low:
                skipped += 1
                continue
            rel = (rel_dir + "/" + fn) if rel_dir else fn
            key = page_key(rel)
            changed = False
            pmap = {}
            entry = registry.get(key) or {}
            for s in entry.get("sections", []):
                if s.get("key") and s.get("n") is not None:
                    pmap[s["key"]] = str(s["n"])
                for x in s.get("sub", []) or []:
                    if x.get("key") and x.get("n") is not None:
                        pmap[x["key"]] = str(x["n"])
            map_tag = ("<script>window.JH_SECTION_MAP=" + json.dumps(pmap, separators=(",", ":")) + ";</script>") if pmap else ""
            # replace a previously baked map (idempotent re-bakes)
            text2 = re.sub(r"<script>window\.JH_SECTION_MAP=\{.*?\};</script>\n?", "", text, flags=re.S)
            if "jh-sections.js" not in text2:
                block = (map_tag + "\n" if map_tag else "") + TAG + "\n"
                if "</body>" in text2:
                    text2 = text2.replace("</body>", block + "</body>", 1)
                else:
                    text2 = text2 + "\n" + block
                injected += 1
                changed = True
            elif map_tag:
                text2 = text2.replace(TAG, map_tag + "\n" + TAG, 1) if TAG in text2 else re.sub(r'(<script src="/jh-sections\.js[^"]*"[^>]*></script>)', map_tag + r"\n\1", text2, count=1)
                changed = True
            if map_tag:
                baked += 1
            if changed and text2 != text:
                open(path, "w", encoding="utf-8").write(text2)
    print(f"bake_sections: injected={injected} baked_maps={baked} skipped={skipped} registry_pages={len(registry)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:  # noqa: BLE001
        print(f"bake_sections: non-fatal error {e}")
