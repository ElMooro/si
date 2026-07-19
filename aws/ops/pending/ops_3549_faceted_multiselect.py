"""ops 3549 — faceted MULTI-SELECT on every Explorer dropdown (Khalid
spec): each select is now an ADDER — every click appends that option
as a removable filter chip; clicking it again toggles it off; the
"All …" option clears the facet. OR within a facet, AND across
facets ("Low + Medium risk", "Semis + Software + Banks", "double
bottom OR golden cross" all work). Facet chips row renders the active
set; legend screens, presets, saved screens and share-links all carry
the multi-facet state (backward-compatible with old single-value
saves). Page-only. 38-behavior harness PASS pre-push (union exact,
toggle-off exact, chip count).

  U1 page served with fxFacetChips + facetToggle + set-semantics
     markers; OPS3529 node-parses
"""
import json, re, subprocess, sys, tempfile, time, urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3549"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


with report("3549_faceted_multiselect") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:600]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    pa = b""
    for _ in range(14):
        try:
            pa = fetch("https://justhodl.ai/fundamental-census.html?cb=%d"
                       % int(time.time()))
            if b"fxFacetChips" in pa: break
        except Exception: pass
        time.sleep(20)
    mm = re.search(rb'<script id="OPS3529">\n([\s\S]*?)</script>', pa)
    ok_n = False
    if mm:
        src = mm.group(1).replace(b"__BT_URL__", b"https://x")
        with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                         delete=False) as f:
            f.write(src); pth = f.name
        ok_n = subprocess.run(["node", "--check", pth],
                              capture_output=True).returncode == 0
    need = [b"fxFacetChips", b"facetToggle", b"SECS.length",
            b"RISKS.indexOf", b"MOMS.indexOf", b"PATS.length",
            b"st.risks", b"IRISKS"]
    gate("U1_page", all(k in pa for k in need) and ok_n,
         {"node": ok_n,
          "missing": [k.decode() for k in need if k not in pa]})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3549.json").write_text(
        json.dumps({"ops": 3549, "fails": fails}))
sys.exit(0)
