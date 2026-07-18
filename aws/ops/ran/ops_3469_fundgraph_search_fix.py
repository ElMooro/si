"""ops 3469 — close R1 from ops 3467 (search relevance).

FMP search-symbol matches TICKERS ("micro" -> MICRO.BK, no Microsoft).
v1.1.3 merges search-name (company intent) ahead of search-symbol, ranks
exact ticker > US listing (no dot) > foreign, dedupes, caps 8.

Gates:
  R1b  ?search=apple -> AAPL in top 3
  R1c  ?search=micro -> MSFT present
  R1d  ?search=tesla -> TSLA in top 3
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3469",
                                               "Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=45) as r:
        raw = r.read()
        if (r.headers.get("Content-Encoding") or "").lower() == "gzip":
            raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
        return json.loads(raw)


with report("3469_fundgraph_search_rank") as rep:
    out = {"ops": 3469, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:380]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:340]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3469 — search relevance close (name-first ranked merge)")

    deploy_lambda(
        report=rep, function_name=FN,
        source_dir=REPO / "aws" / "lambdas" / FN / "source",
        env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                  "CACHE_TTL_SEC": "72000"},
        timeout=900, memory=512,
        description="Fundamental Graphs API v1.1.4 ranked search fix (ops 3469)",
        create_function_url=True, smoke=False,
    )
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)
    url = lam.get_function_url_config(FunctionName=FN)["FunctionUrl"].rstrip("/")

    def syms(q):
        d = fetch(f"{url}/?search={q}")
        return [r["symbol"] for r in d.get("results", [])]

    try:
        a = syms("apple")
        gate("S1_apple_top3", "AAPL" in a[:3], a[:5])
        m = syms("micro")
        gate("S2_micro_has_msft", "MSFT" in m, m[:6])
        t = syms("tesla")
        gate("S3_tesla_top3", "TSLA" in t[:3], t[:5])
    except Exception as e:  # noqa: BLE001
        gate("S1_apple_top3", False, str(e)[:220])

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3469.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
