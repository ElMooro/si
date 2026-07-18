"""ops 3467 — Fundamental Graphs research-desk pass.

Engine v1.1.2 adds a symbol-search proxy (?search= -> FMP /stable
search-symbol with search-name fallback) so the page gets typeahead
without exposing the FMP key. Page v1.4: analyst Lenses (7 curated preset
bundles), symbol typeahead dropdown, side-by-side snapshot Table with
direction-aware best-in-row highlighting. why.html gains a null-safe
ADDITIVE cross-link chip into #jhVitalsTop (nothing existing touched).

Gates:
  R1  deploy v1.1.2 + URL ?search=apple contains AAPL; ?search=micro
      contains MSFT (gzip path)
  R2  page v1.4 live (ops3467 + fgPresets + cmptbl + symsuggest markers)
  R3  why.html live carries fg-xlink AND retains jhVitalsTop + jhDollarFlows
      (additive-only proof)
"""
import gzip
import io
import json
import sys
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


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3467",
                                               "Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=45) as r:
        raw = r.read()
        if (r.headers.get("Content-Encoding") or "").lower() == "gzip":
            raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
        return r.status, raw


with report("3467_fundgraph_research_desk") as rep:
    out = {"ops": 3467, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:400]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:360]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3467 — lenses + typeahead + snapshot table + why.html link")

    deploy_lambda(
        report=rep, function_name=FN,
        source_dir=REPO / "aws" / "lambdas" / FN / "source",
        env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                  "CACHE_TTL_SEC": "72000"},
        timeout=900, memory=512,
        description="Fundamental Graphs API v1.1.2 + search proxy (ops 3467)",
        create_function_url=True, smoke=False,
    )
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)
    url = lam.get_function_url_config(FunctionName=FN)["FunctionUrl"].rstrip("/")

    try:
        st1, b1 = fetch(f"{url}/?search=apple")
        d1 = json.loads(b1)
        st2, b2 = fetch(f"{url}/?search=micro")
        d2 = json.loads(b2)
        syms1 = [r["symbol"] for r in d1.get("results", [])]
        syms2 = [r["symbol"] for r in d2.get("results", [])]
        gate("R1_search_proxy",
             st1 == 200 and d1.get("ok") and "AAPL" in syms1
             and st2 == 200 and "MSFT" in syms2,
             {"apple": syms1[:5], "micro": syms2[:5]})
    except Exception as e:  # noqa: BLE001
        gate("R1_search_proxy", False, str(e)[:220])

    page_ok, det = False, {}
    for _ in range(21):
        try:
            st, b = fetch(
                f"https://justhodl.ai/fundamental-graphs.html?cb={int(time.time())}")
            page_ok = all(m in b for m in (b"ops3467", b"fgPresets",
                                           b"cmptbl", b"symsuggest"))
            det = {"status": st, "markers": page_ok}
        except Exception as e:  # noqa: BLE001
            det = {"err": str(e)[:120]}
        if page_ok:
            break
        time.sleep(20)
    gate("R2_page_v14_live", page_ok, det)

    why_ok, det2 = False, {}
    for _ in range(12):
        try:
            st, b = fetch(f"https://justhodl.ai/why.html?cb={int(time.time())}")
            why_ok = (b"fg-xlink" in b and b"jhVitalsTop" in b
                      and b"jhDollarFlows" in b)
            det2 = {"status": st, "xlink": b"fg-xlink" in b,
                    "vitals_intact": b"jhVitalsTop" in b,
                    "dollarflows_intact": b"jhDollarFlows" in b}
        except Exception as e:  # noqa: BLE001
            det2 = {"err": str(e)[:120]}
        if why_ok:
            break
        time.sleep(20)
    gate("R3_why_additive_link", why_ok, det2)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3467.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
