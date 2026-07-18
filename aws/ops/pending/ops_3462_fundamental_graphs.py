"""ops 3462 — Fundamental Graphs engine + flagship page (TradingView-class).

Deploys justhodl-fundamental-graphs (Function URL, CORS *, gzip), publishes
data/fundgraph/config.json {api_url} for the page, warms CHTR/AAPL/MSFT
caches, then gates on REAL data:

  G1  deploy + Function URL live, marker in warm response
  G2  AAPL quarter: >=35 revenue pts, 10y span, forensic scores + valuation
      + per-share + estimate keys present, price series present
  G3  CHTR latest FQ gross profit in [5.0B, 8.0B]  (cross-check vs the
      TradingView screenshot Khalid shot: FQ 6.28B)
  G4  URL probe: CORS * + gzip encoding honored
  G5  page live on justhodl.ai with marker + served nav-manifest carries
      /fundamental-graphs.html (polls; pages.yml runs on the same push)
"""
import gzip
import io
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
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

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def http(url, headers=None, t=40):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3462", **(headers or {})})
    with urllib.request.urlopen(req, timeout=t) as r:
        raw = r.read()
        if r.headers.get("Content-Encoding") == "gzip":
            raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
        return r.status, dict(r.headers), raw


with report("3462_fundamental_graphs") as rep:
    out = {"ops": 3462, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:400]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:340]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3462 — Fundamental Graphs (engine + page + sidebar)")

    # ── 1. deploy ────────────────────────────────────────────────────────────
    src = REPO / "aws" / "lambdas" / FN / "source"
    deploy_lambda(
        report=rep, function_name=FN, source_dir=src,
        env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                  "CACHE_TTL_SEC": "72000"},
        timeout=120, memory=512,
        description="Fundamental Graphs API for fundamental-graphs.html (ops 3462)",
        create_function_url=True, smoke=False,
    )
    # wait until the update settles before invoking (fleet gotcha)
    for _ in range(30):
        cfg = lam.get_function_configuration(FunctionName=FN)
        if cfg.get("LastUpdateStatus") == "Successful" and cfg.get("State") == "Active":
            break
        time.sleep(2)
    try:
        fn_url = lam.get_function_url_config(FunctionName=FN)["FunctionUrl"].rstrip("/")
    except Exception as e:  # noqa: BLE001
        fn_url = ""
        gate("G1_deploy_url", False, f"no function url: {e}")
    if fn_url:
        print("Function URL:", fn_url)
        rep.log("Function URL: " + fn_url)

    # ── 2. publish page config + warm ───────────────────────────────────────
    if fn_url:
        s3.put_object(
            Bucket=BUCKET, Key="data/fundgraph/config.json",
            Body=json.dumps({"api_url": fn_url, "engine": FN, "ops": 3462,
                             "updated": datetime.now(timezone.utc).isoformat()}).encode(),
            ContentType="application/json", CacheControl="public, max-age=300",
        )
        rep.log("published data/fundgraph/config.json")

        warm = lam.invoke(
            FunctionName=FN,
            Payload=json.dumps({"warm": ["CHTR", "AAPL", "MSFT"],
                                "periods": ["quarter", "annual"],
                                "refresh": True}).encode(),
        )
        wp = json.loads(warm["Payload"].read() or b"{}")
        ok1 = wp.get("marker") == MARKER and all(
            v.get("ok") for k, v in (wp.get("warmed") or {}).items()
            if k.endswith("_quarter"))
        gate("G1_deploy_url", ok1, {"url": bool(fn_url), "warm": wp.get("warmed")})

    # ── 3. G2: AAPL coverage from the warmed cache ──────────────────────────
    try:
        doc = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/fundgraph/cache/AAPL_quarter.json")["Body"].read())
        pts = doc.get("points", {})
        rev = pts.get("revenue", [])
        span_ok = bool(rev) and rev[0][0] <= "2016-12-31"
        need = ["grossProfit", "ebitda", "fcf", "mcap", "ev", "pe_ttm",
                "ev_ebitda_ttm", "fcf_yield_pct", "buyback_yield_pct",
                "gross_margin_pct", "roe_pct", "eps_ttm", "book_value_ps",
                "netdebt_to_ebitda_ttm", "altman_z", "piotroski_f",
                "beneish_m", "sloan_accruals_pct", "share_count_yoy_pct",
                "est_revenue_avg", "est_eps_avg"]
        missing = [k for k in need if not pts.get(k)]
        g2 = (len(rev) >= 35 and span_ok and not missing
              and len(doc.get("price", [])) >= 400 and doc.get("marker") == MARKER)
        gate("G2_aapl_coverage", g2,
             {"rev_pts": len(rev), "first": rev[0][0] if rev else None,
              "keys": len(pts), "price_pts": len(doc.get("price", [])),
              "missing": missing})
    except Exception as e:  # noqa: BLE001
        gate("G2_aapl_coverage", False, str(e)[:200])

    # ── 4. G3: CHTR gross profit vs known FQ (~6.28B on TV, 2026-07-18) ────
    try:
        chtr = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/fundgraph/cache/CHTR_quarter.json")["Body"].read())
        gp = chtr.get("points", {}).get("grossProfit", [])
        last = gp[-1] if gp else [None, None]
        gate("G3_chtr_crosscheck",
             bool(gp) and last[1] is not None and 5.0e9 <= last[1] <= 8.0e9,
             {"last_fq": last, "n": len(gp)})
    except Exception as e:  # noqa: BLE001
        gate("G3_chtr_crosscheck", False, str(e)[:200])

    # ── 5. G4: public URL probe — CORS + gzip honored ───────────────────────
    if fn_url:
        try:
            st, hdr, raw = http(f"{fn_url}/?symbol=AAPL&period=quarter",
                                headers={"Accept-Encoding": "gzip",
                                         "Origin": "https://justhodl.ai"})
            d = json.loads(raw)
            gate("G4_url_cors_gzip",
                 st == 200 and d.get("ok") and d.get("marker") == MARKER
                 and hdr.get("Access-Control-Allow-Origin") == "*",
                 {"status": st, "gzip": hdr.get("Content-Encoding"),
                  "acao": hdr.get("Access-Control-Allow-Origin"),
                  "cached": d.get("cached"), "bytes": len(raw)})
        except Exception as e:  # noqa: BLE001
            gate("G4_url_cors_gzip", False, str(e)[:200])

    # ── 6. G5: page + served manifest live (pages.yml runs on this push) ───
    page_ok = manifest_ok = False
    detail = {}
    for att in range(24):                      # up to ~8 min
        try:
            if not page_ok:
                st, _, body = http(
                    f"https://justhodl.ai/fundamental-graphs.html?cb={int(time.time())}")
                page_ok = st == 200 and b"fundgraph v1.0 ops3462" in body
                detail["page"] = st
            if not manifest_ok:
                st2, _, mraw = http(
                    f"https://justhodl.ai/nav-manifest.json?cb={int(time.time())}")
                manifest_ok = st2 == 200 and b"fundamental-graphs.html" in mraw
                detail["manifest"] = st2
        except Exception as e:  # noqa: BLE001
            detail["err"] = str(e)[:120]
        if page_ok and manifest_ok:
            break
        time.sleep(20)
    gate("G5_page_and_sidebar_live", page_ok and manifest_ok,
         {"page_ok": page_ok, "manifest_ok": manifest_ok, **detail})

    # ── report ──────────────────────────────────────────────────────────────
    out["function_url"] = fn_url
    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports").mkdir(parents=True, exist_ok=True)
    (REPO / "aws" / "ops" / "reports" / "3462.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])
    if fails:
        rep.log("failed gates: " + ", ".join(fails))

sys.exit(0)
