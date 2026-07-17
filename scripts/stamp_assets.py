#!/usr/bin/env python3
"""Content-hash stamping for shared local assets (ops 3370, topo edition).

Local .js/.css get immutable ?v=<md5-8> URLs — content-addressed assets, the
pattern every large shop uses: new bytes ⇒ new URL ⇒ cold CDN/browser fetch;
unchanged bytes ⇒ cache hit. Kills the "deployed but no client ever fetched
it" class (drawer, ops 3369; auth.js was one bad cache away).

Versioning is TOPOLOGICAL on the reference DAG (a naive fixpoint never
converges: a file that both contains refs and is referenced rolls hashes
forever). Dependencies version first; a referrer's bytes are finalized (deps'
?v= baked in) BEFORE its own version is hashed — so a change anywhere
propagates transitively: auth-config edit ⇒ auth.js URL rolls ⇒ drawer URL
rolls ⇒ every page's stamp rolls. If a ref cycle ever appears, all assets
fall back to original-bytes hashing with a loud warning (deploy never bricks;
transitivity degrades until the cycle is removed).

/service-worker.js is never stamped (SW spec: stable registered URL;
byte-diff drives updates). Idempotent: rerunning on a stamped tree is a
no-op. Deterministic per checkout.

CLI: python3 scripts/stamp_assets.py <site_root>
Importable: compute_versions(root) → ({asset: ver8}, mode) — ops gates
replicate expected versions with the exact same logic. One logic, zero drift.
"""

import graphlib
import hashlib
import re
import sys
from pathlib import Path

EXCLUDE = {"/service-worker.js"}
EXT = (".js", ".css")


def _h(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()[:8]


def discover_assets(root: Path):
    out = set()
    for p in root.rglob("*"):
        if p.suffix in EXT and p.is_file():
            web = "/" + p.relative_to(root).as_posix()
            if web not in EXCLUDE and "/node_modules/" not in web:
                out.add(web)
    return out


def _pat(assets):
    alts = "|".join(re.escape(a) for a in sorted(assets, key=len, reverse=True))
    return re.compile('"(' + alts + r')(\?v=[A-Za-z0-9]+)?"')


def compute_versions(root: Path):
    """Version every asset; REWRITES js files in place (deps' ?v= baked in).

    Returns ({web_path: ver8}, mode) — mode "topo" or "flat-cycle-fallback".
    """
    assets = discover_assets(root)
    pat = _pat(assets)
    refs = {}
    for a in assets:
        if a.endswith(".js"):
            s = (root / a.lstrip("/")).read_text(errors="ignore")
            refs[a] = set(m.group(1) for m in pat.finditer(s)) - {a}
        else:
            refs[a] = set()
    try:
        ts = graphlib.TopologicalSorter()
        for a in sorted(assets):
            ts.add(a, *sorted(refs[a]))
        order = list(ts.static_order())
        # belt+braces: require dependencies-first regardless of stdlib nuance
        pos = {a: i for i, a in enumerate(order)}
        if any(pos[d] > pos[a] for a in assets for d in refs[a]):
            order.reverse()
            pos = {a: i for i, a in enumerate(order)}
        assert all(pos[d] < pos[a] for a in assets for d in refs[a]), "topo order broken"
        mode = "topo"
    except graphlib.CycleError as e:
        print("WARN: asset ref CYCLE — flat original-bytes fallback:", e)
        order, mode = sorted(assets), "flat-cycle-fallback"

    ver = {}
    for a in order:
        fp = root / a.lstrip("/")
        if mode == "topo" and a.endswith(".js") and refs[a]:
            s = fp.read_text(errors="ignore")
            # self-refs stay untouched: version isn't computable mid-file
            # (chicken-egg), and they're runtime string compares (e.g. auth.js
            # matching its own <script> src) that stamping would break.
            t = pat.sub(lambda m, _self=a: m.group(0) if m.group(1) == _self
                        else '"' + m.group(1) + "?v=" + ver[m.group(1)] + '"', s)
            if t != s:
                fp.write_text(t)
        ver[a] = _h(fp.read_bytes())
    return ver, mode


def rewrite_html(root: Path, ver):
    alts = "|".join(re.escape(a) for a in sorted(ver, key=len, reverse=True))
    pat = re.compile('(src|href)="(' + alts + r')(\?v=[A-Za-z0-9]+)?"')
    n_files = n_refs = 0
    for p in root.rglob("*.html"):
        s = p.read_text(errors="ignore")
        t, k = pat.subn(lambda m: m.group(1) + '="' + m.group(2) + "?v=" + ver[m.group(2)] + '"', s)
        if k and t != s:
            p.write_text(t)
            n_files += 1
        n_refs += k
    return n_files, n_refs


def main(site):
    root = Path(site)
    ver, mode = compute_versions(root)
    n_files, n_refs = rewrite_html(root, ver)
    print(f"[{mode}] {len(ver)} assets versioned; {n_refs} html refs across {n_files} changed pages")
    for a in ["/jh-nav-drawer.js", "/jh-page-ai.js", "/auth.js", "/auth-config.js"]:
        if a in ver:
            print("  ", a, ver[a])


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "_site")
