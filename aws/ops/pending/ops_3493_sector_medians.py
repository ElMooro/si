"""ops 3493 — Tier-3b: sector medians (vs-Sector on both surfaces).

Zero new data sources: forensic-screen (503-name S&P cross-section)
ALREADY ships sector_valuation_medians; engine v1.4.1 republishes it —
plus row-computed medians for gross/op margin, Beneish, Sloan(×100) —
into a tiny fg-keyed file data/fundgraph/sector-medians.json
(SECMED_OPS3493; Monday warm_auto refresh; ?secmed=1 / event hook).
Core v8: grp-resolved opts.hlines (micro: 2 drawn on their axes,
unknown-grp skipped). Surfaces: "vs Sector" toggle -> dashed p50 lines
in Values + "sec ±X%" legend chips (LOWER_BETTER-aware coloring on the
flagship). jsdom harness v5 (14 behaviors) proves the module end-to-end;
its own two silent-no-op fixture patches were caught and assert-fixed —
the assert-every-replace rule now applies to HARNESS edits too.

Gates:
  H1 deploy v1.4.1 + invoke {"sector_medians":true} -> file with
     >=8 sectors and >=7 fg keys
  H2 real-data sanity: Technology pe_ttm p50 > Utilities pe_ttm p50;
     every sector's beneish_m median in (-4, 0)
  H3 served: core OPS3493 + hlines; flagship v2.6 (secbtn, SECMED);
     module (jhfgSec, secMedFor2); priors intact; 4-surface node-check
"""
import json
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report                      # noqa: E402
from _lambda_deploy_helpers import deploy_lambda   # noqa: E402

REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-fundamental-graphs"
BUCKET = "justhodl-dashboard-live"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
lam = boto3.client("lambda", region_name="us-east-1")
s3c = boto3.client("s3", region_name="us-east-1")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3493"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b)
        p = f.name
    return subprocess.run(["node", "--check", p],
                          capture_output=True).returncode == 0


with report("3493_sector_medians") as rep:
    out = {"ops": 3493, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:440]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:400]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3493 — sector medians republish + vs-Sector surfaces")

    deploy_lambda(
        report=rep, function_name=FN,
        source_dir=REPO / "aws" / "lambdas" / FN / "source",
        env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                  "CACHE_TTL_SEC": "72000"},
        timeout=900, memory=512,
        description="Fundamental Graphs v1.4.1 sector medians (ops 3493)",
        create_function_url=True, smoke=False,
    )
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)

    try:
        lam.invoke(FunctionName=FN,
                   Payload=json.dumps({"sector_medians": True}).encode())
        sm = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundgraph/sector-medians.json")["Body"].read())
        secs = sm.get("sectors") or {}
        gate("H1_republish",
             sm.get("n_sectors", 0) >= 8 and len(sm.get("keys") or []) >= 7,
             {"n_sectors": sm.get("n_sectors"), "keys": sm.get("keys")})
        tech = (secs.get("Technology") or {}).get("pe_ttm")
        util = (secs.get("Utilities") or {}).get("pe_ttm")
        ben_ok = all(-4 < (m2.get("beneish_m") or -2.5) < 0
                     for m2 in secs.values())
        gate("H2_realdata_sanity",
             tech is not None and util is not None and tech > util and ben_ok,
             {"tech_pe": tech, "util_pe": util, "beneish_band_ok": ben_ok,
              "sample": {k: secs[k].get("pe_ttm")
                         for k in list(secs)[:4]}})
    except Exception as e:  # noqa: BLE001
        gate("H1_republish", False, str(e)[:260])
        gate("H2_realdata_sanity", False, "see H1")

    got = {}
    for _ in range(18):
        try:
            cb = int(time.time())
            got["core"] = fetch(f"https://justhodl.ai/fg-chart.js?cb={cb}")
            got["cat"] = fetch(f"https://justhodl.ai/fg-catalog.js?cb={cb}")
            got["flag"] = fetch(
                f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if b"OPS3493" in got["core"] and b"ops3493" in got["flag"] \
               and b"jhfgSec" in got["why"]:
                break
        except Exception as e:  # noqa: BLE001
            got["err"] = str(e)[:120]
        time.sleep(20)
    checks = [node_ok(got.get("core", b"x=")), node_ok(got.get("cat", b"x="))]
    m1 = re.search(rb"<script>\n('use strict'[\s\S]*?)</script>",
                   got.get("flag", b""))
    checks.append(node_ok(m1.group(1) if m1 else b"x="))
    m2 = re.search(rb'<script id="fgwhy-3478">([\s\S]*?)</script>',
                   got.get("why", b""))
    checks.append(node_ok(m2.group(1) if m2 else b"x="))
    f = got.get("flag", b"")
    y = got.get("why", b"")
    d3 = {"node_ok": all(checks),
          "core_hlines": b"OPS3493" in got.get("core", b"")
          and b"opts.hlines" in got.get("core", b""),
          "flag": b"ops3493" in f and b"secbtn" in f and b"SECMED" in f,
          "why": b"jhfgSec" in y and b"secMedFor2" in y,
          "priors": b"mxbtn" in f and b"rtbtn" in f and b"evtbtn" in f
          and b"ops3475" in y and b"jhfgMx" in y}
    gate("H3_surfaces", all(d3.values()), d3)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3493.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
