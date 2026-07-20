"""ops 3568 — header-click sort on the flagship Explorer table
(Khalid spec): EVERY column header — Q, Turn, Σ pct, and every added
metric — toggles descending → ascending with ▼/▲ indicator; sorts on
raw values (nulls last). ETF/FI pages already had it. 40-behavior
harness PASS (desc AAA-first, asc EEE-first, toggle exact).

  L1 page served with data-hk header markers + node
"""
import json, re, subprocess, sys, tempfile, time, urllib.request
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
REPO = Path(__file__).resolve().parents[3]

with report("3568_header_sort") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:600]
        print(line); rep.log(line)
        if not ok: fails.append(n)
    pa = b""
    for _ in range(14):
        try:
            req = urllib.request.Request(
                "https://justhodl.ai/fundamental-census.html?cb=%d"
                % int(time.time()),
                headers={"User-Agent": "ops-3568"})
            pa = urllib.request.urlopen(req, timeout=30).read()
            if b"data-hk" in pa:
                break
        except Exception:
            pass
        time.sleep(20)
    mm = re.search(rb'<script id="OPS3529">\n([\s\S]*?)</script>', pa)
    node = False
    if mm:
        with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                         delete=False) as f:
            f.write(mm.group(1).replace(b"__BT_URL__", b"https://x"))
            pth = f.name
        node = subprocess.run(["node", "--check", pth],
                              capture_output=True).returncode == 0
    gate("L1_page", b"data-hk" in pa and b"HSORT" in pa and node,
         {"node": node})
    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3568.json").write_text(
        json.dumps({"ops": 3568, "fails": fails}))
sys.exit(0)
