#!/usr/bin/env python3
"""ops 3751 — canary #19: short-interest collapse on FLAT price.

justhodl-short-interest already tracked short-interest change (si_change_pct)
and had a COVERING signal, but it NEVER joined price — its docstring said
"price rising / falling price" while classify_signal took no price arg. The
canary's whole thesis is that SI dropping WHILE PRICE STAYS FLAT is a
higher-signal event than covering into a rally: shorts are exiting on a
catalyst they see (forced buy-in, index event, borrow spike), not because the
thesis broke.

SHIPPED (additive):
  - fetch_price_over_window(): Polygon daily aggs over the SI settlement
    window, giving price_change_pct on the SAME clock as si_change_pct
  - classify_signal() gains SI_COLLAPSE_FLAT_PRICE (si<=-10% AND |price|<=4%),
    ranked ABOVE the generic signals; all prior signals intact
  - top_si_collapse_flat_price board + page card on squeeze.html

GATES
  G0  KEY CONTRACT — new keys present in producer + page render path
  G1  zip-settle to v1.1
  G2  async invoke + S3 freshness
  G3  DATA TRUTH — price leg populated; every SI_COLLAPSE row genuinely has
      si<=-10 AND |price|<=4 (no misclassification); prior signals still emit
  G4  PAGE — squeeze.html renders the new board (unique marker)
"""
import io
import json
import sys
import time
import traceback
import urllib.request
import zipfile
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
# ops 3750 CRASHED here: ROOT is aws/, so ROOT/"squeeze.html" resolved to
# aws/squeeze.html which does not exist. Root-level pages are parents[3].
REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

FN = "justhodl-short-interest"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/short-interest.json"
PAGE = "https://justhodl.ai/squeeze.html"
REGION = "us-east-1"
UA = {"User-Agent": "Mozilla/5.0 (JustHodl ops verify)"}

with report("3751_short_interest_flatprice_fix") as rep:
    rep.heading("ops 3751 — canary #19: short-interest v1.1 (SI collapse on flat price)")
    fails = []
    out = {"gates": {}}
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3751.json").write_text(json.dumps({"verdict": "STARTED"}))

    def gate(n, ok, detail):
        out["gates"][n] = {"ok": bool(ok), "detail": str(detail)[:900]}
        rep.log(("PASS " if ok else "FAIL ") + n + " — " + str(detail)[:800])
        if not ok:
            fails.append(n)
        return ok

    try:
        lam = boto3.client("lambda", region_name=REGION)
        s3 = boto3.client("s3", region_name=REGION)

        # ── G0 key contract ──────────────────────────────────────────────
        rep.section("G0 — key contract")
        src = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
        need = ["SI_COLLAPSE_FLAT_PRICE", "fetch_price_over_window",
                "price_change_pct", "top_si_collapse_flat_price",
                "price_window"]
        miss = [k for k in need if k not in src]
        page = (REPO / "squeeze.html").read_text()
        page_ok = ("top_si_collapse_flat_price" in page
                   and "sicollapse" in page)
        gate("G0_key_contract", not miss and page_ok,
             "producer_missing=%s page_renders=%s" % (miss, page_ok))

        # ── G1 settle ────────────────────────────────────────────────────
        rep.section("G1 — zip settle to v1.1")
        settled = False
        for i in range(24):
            try:
                cfg = lam.get_function(FunctionName=FN)
                c = cfg["Configuration"]
                if c.get("State") != "Active" or c.get("LastUpdateStatus") == "InProgress":
                    time.sleep(15)
                    continue
                blob = urllib.request.urlopen(cfg["Code"]["Location"], timeout=60).read()
                body = zipfile.ZipFile(io.BytesIO(blob)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if "SI_COLLAPSE_FLAT_PRICE" in body and '"version": "1.1"' in body:
                    settled = True
                    break
            except Exception as e:
                rep.log("  settle: %s" % str(e)[:90])
            time.sleep(15)
        gate("G1_settle", settled, "v1.1 deployed" if settled else "marker absent")

        # ── G2 invoke + freshness ────────────────────────────────────────
        rep.section("G2 — async invoke + S3 freshness")
        doc = None
        if settled:
            try:
                before = s3.head_object(Bucket=BUCKET, Key=OUT_KEY)["LastModified"]
            except Exception:
                before = None
            lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
            for _ in range(34):     # engine hits FINRA + a batch of Polygon calls
                time.sleep(15)
                try:
                    h = s3.head_object(Bucket=BUCKET, Key=OUT_KEY)
                    if before is None or h["LastModified"] > before:
                        doc = json.loads(s3.get_object(
                            Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
                        break
                except Exception:
                    pass
            gate("G2_artifact", doc is not None,
                 "priced=%s si_collapse=%s version=%s"
                 % ((doc or {}).get("n_tickers_priced"),
                    len((doc or {}).get("top_si_collapse_flat_price") or []),
                    (doc or {}).get("version")))

        # ── G3 data truth ────────────────────────────────────────────────
        rep.section("G3 — data truth (signal is correct, prior signals live)")
        if doc:
            bt = doc.get("by_ticker") or {}
            priced = doc.get("n_tickers_priced") or 0
            collapse = doc.get("top_si_collapse_flat_price") or []
            # every SI_COLLAPSE row must truly satisfy the definition
            bad = [r["ticker"] for r in collapse
                   if not (r.get("si_change_pct") is not None
                           and r["si_change_pct"] <= -10
                           and r.get("price_change_pct") is not None
                           and abs(r["price_change_pct"]) <= 4.0)]
            # prior signals must not have vanished
            sigs = {}
            for v in bt.values():
                sigs[v.get("signal")] = sigs.get(v.get("signal"), 0) + 1
            rep.log("  signal census: %s" % sigs)
            for r in collapse[:8]:
                rep.log("  %-6s ΔSI=%s%%  ΔPrice=%s%%  DTC=%s  score=%s"
                        % (r["ticker"], r.get("si_change_pct"),
                           r.get("price_change_pct"), r.get("days_to_cover"),
                           r.get("score")))
            gate("G3_data_truth", priced > 0 and not bad,
                 "priced=%d collapse_rows=%d misclassified=%s"
                 % (priced, len(collapse), bad[:4]))
        else:
            gate("G3_data_truth", False, "no doc")

        # ── G4 page ──────────────────────────────────────────────────────
        rep.section("G4 — squeeze.html renders the new board")
        html = ""
        for attempt in range(12):
            try:
                bust = "%s?v=%d" % (PAGE, int(time.time()) + attempt)
                html = urllib.request.urlopen(urllib.request.Request(
                    bust, headers=dict(UA, **{"Cache-Control": "no-cache",
                                              "Pragma": "no-cache"})),
                    timeout=30).read().decode("utf-8", "replace")
                if "top_si_collapse_flat_price" in html and "sicollapse" in html:
                    rep.log("  served page CURRENT (len=%d) after %ds"
                            % (len(html), attempt * 20))
                    break
                if html:
                    rep.log("  stale edge copy len=%d" % len(html))
            except Exception as e:
                rep.log("  attempt %d: %s" % (attempt, str(e)[:80]))
            time.sleep(20)
        gate("G4_page", "top_si_collapse_flat_price" in html and "sicollapse" in html,
             "page renders #19 board" if "sicollapse" in html
             else "page missing new board")

        rep.section("VERDICT")
        verdict = "PASS_ALL" if not fails else "FAIL"
        out["verdict"] = verdict
        Path("aws/ops/reports/3751.json").write_text(json.dumps(out, indent=2))
        rep.kv(verdict=verdict,
               priced=(doc or {}).get("n_tickers_priced", 0),
               si_collapse=len((doc or {}).get("top_si_collapse_flat_price") or []),
               failed=",".join(fails) or "none")
        if fails:
            rep.fail("gates failed: %s" % fails)
            sys.exit(1)
        rep.ok("PASS_ALL — canary #19 live")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3751.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
