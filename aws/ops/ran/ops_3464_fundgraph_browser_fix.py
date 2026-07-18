"""ops 3464 — fundamental-graphs "Failed to fetch" close.

Root cause: BOTH the Function URL Cors config AND the function response
emitted Access-Control-Allow-Origin. Browsers receive "*, *" (duplicate)
and hard-fail the fetch; curl/urllib take the first value, which is why
ops-3463's probes passed. Fix: v1.0.2 emits NO CORS headers — the URL
config is the single authority. Page also gains a baked FALLBACK_API.

Browser-faithful gates:
  B1  GET with Origin -> get_all('access-control-allow-origin') == ['*']
      (exactly one — the duplicate check browsers actually enforce)
  B2  worker-proxied data/fundgraph/config.json serves api_url (page boot path)
  B3  full boot-path emulation: PX config -> api_url -> CHTR fetch, single
      ACAO, ok:true, marker, gzip
  B4  page live with FALLBACK_API baked (polls pages.yml)
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
MARKER = "FUNDGRAPH_V1_OPS3462"
PXW = "https://justhodl-data-proxy.raafouis.workers.dev/"

lam = boto3.client("lambda", region_name="us-east-1")


def probe(url, hdrs=None):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3464",
                                               **(hdrs or {})})
    with urllib.request.urlopen(req, timeout=45) as r:
        h = r.headers
        acao_all = h.get_all("Access-Control-Allow-Origin") or []
        enc = (h.get("Content-Encoding") or "").lower()
        raw = r.read()
        body = gzip.GzipFile(fileobj=io.BytesIO(raw)).read() if enc == "gzip" else raw
        return r.status, acao_all, enc, body


with report("3464_fundgraph_browser_fix") as rep:
    out = {"ops": 3464, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:380]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:340]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3464 — browser CORS close (duplicate-ACAO fix)")

    # 1. redeploy v1.0.2 (no function-level ACAO)
    deploy_lambda(
        report=rep, function_name=FN,
        source_dir=REPO / "aws" / "lambdas" / FN / "source",
        env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                  "CACHE_TTL_SEC": "72000"},
        timeout=120, memory=512,
        description="Fundamental Graphs API v1.0.2 (single-authority CORS, ops 3464)",
        create_function_url=True, smoke=False,
    )
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)
    url = lam.get_function_url_config(FunctionName=FN)["FunctionUrl"].rstrip("/")

    # B1 — exactly ONE ACAO (the duplicate check browsers enforce)
    st, acao_all, enc, body = probe(
        f"{url}/?symbol=AAPL&period=quarter",
        {"Origin": "https://justhodl.ai", "Accept-Encoding": "gzip"})
    d = json.loads(body)
    gate("B1_single_acao",
         st == 200 and acao_all == ["*"] and d.get("ok"),
         {"acao_all": acao_all, "enc": enc, "version": d.get("version")})

    # B2 — worker-proxied config (the page's boot path)
    api_from_cfg = ""
    try:
        st2, _, _, cbody = probe(f"{PXW}data/fundgraph/config.json?ts={int(time.time())}")
        cfg = json.loads(cbody)
        api_from_cfg = (cfg.get("api_url") or "").rstrip("/")
        gate("B2_px_config", st2 == 200 and api_from_cfg == url,
             {"status": st2, "api_url": api_from_cfg})
    except Exception as e:  # noqa: BLE001
        gate("B2_px_config", False, str(e)[:200])

    # B3 — full boot-path emulation
    try:
        target = api_from_cfg or url
        st3, acao3, enc3, b3 = probe(
            f"{target}/?symbol=CHTR&period=quarter",
            {"Origin": "https://justhodl.ai", "Accept-Encoding": "gzip"})
        d3 = json.loads(b3)
        gate("B3_boot_path",
             st3 == 200 and acao3 == ["*"] and enc3 == "gzip"
             and d3.get("ok") and d3.get("marker") == MARKER,
             {"acao": acao3, "enc": enc3, "sym": d3.get("symbol"),
              "n": d3.get("n_periods")})
    except Exception as e:  # noqa: BLE001
        gate("B3_boot_path", False, str(e)[:200])

    # B4 — page live with baked fallback (pages.yml on this push)
    page_ok, det = False, {}
    for _ in range(21):
        try:
            stp, _, _, pb = probe(
                f"https://justhodl.ai/fundamental-graphs.html?cb={int(time.time())}")
            page_ok = stp == 200 and b"FALLBACK_API" in pb and b"ops3464" in pb
            det = {"status": stp, "has_fallback": b"FALLBACK_API" in pb}
        except Exception as e:  # noqa: BLE001
            det = {"err": str(e)[:120]}
        if page_ok:
            break
        time.sleep(20)
    gate("B4_page_fallback_live", page_ok, det)

    out["function_url"] = url
    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3464.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
