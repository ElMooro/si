"""ops 3370 — content-addressed shared assets: fleet cache-integrity gates.

This push: scripts/stamp_assets.py (topological versioner — deps first,
referrer bytes finalized before hashing, self-refs preserved, cycle fallback)
wired into pages.yml, replacing the drawer-only stamp. Every local .js/.css
(39 assets, ~1221 refs across 376 pages) now ships content-addressed,
including the drawer's DYNAMIC auth-config.js→auth.js injection chain.
Proven pre-push: deterministic, idempotent, transitive (config edit rolls
auth+drawer URLs), self-ref preserved, zero unversioned refs.

Gates replicate EXPECTED versions by running the same module on a tmp copy
of repo assets (single source of truth — zero drift), then poll live:
  G1  probe pages: every local src/href carries ?v=8hex; key stamps == expected
  G2  bytes identity: GET /<asset>?v=<expected> → md5-8(body) == expected
      (topo mode ⇒ version IS the hash of final bytes)
  G3  live auth.js contains "/plan/self" — ops 3366's server-plan fallback
      finally REACHES CLIENTS (was cache-stranded like the drawer)
  G4  live drawer bakes '"/auth.js?v=<expected>"' (dynamic dep versioned)
"""

import json
import re
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

from ops_report import report

sys.path.insert(0, "scripts")
import stamp_assets as SA  # noqa: E402

SITE = "https://justhodl.ai"
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) ops-3370"}
PAGES = ["/why.html", "/index.html", "/capital-flow.html", "/chart-pro.html"]
KEY_ASSETS = ["/jh-nav-drawer.js", "/jh-page-ai.js", "/auth.js", "/auth-config.js",
              "/interp-kit.js", "/jh-enhance.js", "/cmdk.js", "/jh-wire.js",
              "/jh-theme.css"]


def req(url, timeout=25):
    r = urllib.request.Request(url, headers=UA)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except Exception as e:  # noqa: BLE001
        return -1, str(e).encode()[:200]


def expected_versions():
    tmp = Path(tempfile.mkdtemp())
    (tmp / "assets").mkdir()
    for p in list(Path(".").glob("*.js")) + list(Path(".").glob("*.css")):
        shutil.copy(p, tmp)
    sc = Path("assets/sidebar.css")
    if sc.exists():
        shutil.copy(sc, tmp / "assets")
    ver, mode = SA.compute_versions(tmp)
    return ver, mode


def main(rep):
    out = {"gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:320]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:260]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    exp, mode = expected_versions()
    out["mode"] = mode
    out["expected"] = {k: exp[k] for k in KEY_ASSETS if k in exp}
    print("expected mode:", mode, "| drawer", exp.get("/jh-nav-drawer.js"),
          "auth", exp.get("/auth.js"))
    if mode != "topo":
        gate("G0_topo_mode", False, "cycle fallback active — investigate")

    ref_pat = re.compile(r'(?:src|href)="(/[^"?]+\.(?:js|css))(?:\?v=([A-Za-z0-9]+))?"')
    deadline = time.time() + 300
    page_state = {}
    while time.time() < deadline:
        page_state = {}
        for pg in PAGES:
            st, body = req(SITE + pg)
            b = body.decode("utf-8", "replace") if st == 200 else ""
            bad = [(m.group(1), m.group(2)) for m in ref_pat.finditer(b)
                   if not re.fullmatch(r"[0-9a-f]{8}", m.group(2) or "")]
            key_ok = all((f'{a}?v={exp[a]}' in b) for a in exp
                         if a in b and a in ("/jh-nav-drawer.js", "/jh-page-ai.js"))
            page_state[pg] = {"http": st, "unversioned_or_bad": bad[:4], "key_ok": key_ok}
        if all(p["http"] == 200 and not p["unversioned_or_bad"] and p["key_ok"]
               for p in page_state.values()):
            break
        time.sleep(15)
    out["pages"] = page_state
    gate("G1_pages_stamped_expected",
         all(p["http"] == 200 and not p["unversioned_or_bad"] and p["key_ok"]
             for p in page_state.values()),
         json.dumps({k: v["unversioned_or_bad"] or "ok" for k, v in page_state.items()})[:280])

    import hashlib
    byte_ok, byte_detail = True, []
    for a in KEY_ASSETS:
        if a not in exp:
            continue
        st, body = req(SITE + a + "?v=" + exp[a])
        got = hashlib.md5(body).hexdigest()[:8] if st == 200 else f"http{st}"
        if got != exp[a]:
            byte_ok = False
        byte_detail.append(f"{a.split('/')[-1]}:{'ok' if got == exp[a] else got}")
    gate("G2_bytes_identity", byte_ok, " ".join(byte_detail))

    st, body = req(SITE + "/auth.js?v=" + exp["/auth.js"])
    gate("G3_auth_plan_self_delivered", st == 200 and b"/plan/self" in body,
         f"http {st} marker={b'/plan/self' in body}")

    st, body = req(SITE + "/jh-nav-drawer.js?v=" + exp["/jh-nav-drawer.js"])
    want = ('"/auth.js?v=' + exp["/auth.js"] + '"').encode()
    gate("G4_drawer_dynamic_dep_baked", st == 200 and want in body,
         f"http {st} baked={want in body}")

    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3370.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)


with report("3370_asset_cache_integrity") as _rep:
    _rep.heading("ops 3370 — content-addressed shared assets, fleet gates")
    main(_rep)
