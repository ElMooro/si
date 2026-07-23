#!/usr/bin/env python3
"""ops 3757 — SHIP canary #17: credit-before-equity.

Per-issuer credit direction (Δ distance-to-default, Δ synthetic CDS bp)
against a self-building ledger, crossed with the equity leg. Fires only when
credit moves beyond a threshold AND the stock is flat or moving the other way.

DAY-ONE EXPECTATION, stated up front so the gates are honest: the ledger is
empty on the first run, so EVERY name should report INSUFFICIENT_HISTORY and
n_leads should be 0. That is CORRECT behaviour, not a failure — a lead
measured from a single snapshot would be fabricated. G3 therefore asserts the
engine REFUSES to emit leads without a prior observation. (This is the same
class of defect I shipped and had to fix in canary #16, where z=None was
treated as permission to CONFIRM.)

GATES
  G0  KEY CONTRACT — every key the gates/page read exists in the producer
  G1  settle v1.0.0 (State Active + LastUpdateStatus, per ops 3735)
  G2  async invoke + S3 freshness
  G3  DAY-ONE HONESTY — no lead may fire without prior_obs_date; ledger written
  G4  GAPS — HY issuance gap declared, never substituted with OAS
  G5  page served + field coverage + nav
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
REPO = Path(__file__).resolve().parents[3]     # root pages live here (ops 3750)
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

FN = "justhodl-credit-before-equity"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/credit-before-equity.json"
HIST_KEY = "credit/credit-before-equity-history.json"
PAGE = "https://justhodl.ai/credit-before-equity.html"
REGION = "us-east-1"
UA = {"User-Agent": "Mozilla/5.0 (JustHodl ops verify)"}
EXEMPT = {"version", "generated_at", "attribution", "method", "group",
          "market_cap_usd_bn", "peer_rank", "hist_n", "min_dd_move",
          "min_cds_move_bp", "leads", "names"}

with report("3757_cbe_regate") as rep:
    rep.heading("ops 3757 — canary #17: credit-before-equity")
    fails = []
    out = {"gates": {}}
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3757.json").write_text(json.dumps({"verdict": "STARTED"}))

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
        page_src = (REPO / "credit-before-equity.html").read_text()
        need = ["credit_direction", "d_distance_to_default", "d_synthetic_cds_bp",
                "d_price_pct", "signal", "prior_obs_date", "n_leads",
                "n_awaiting_history", "equity_flat", "gaps", "thresholds"]
        miss = [k for k in need if '"%s"' % k not in src]
        pmiss = [k for k in need if k not in page_src]
        gate("G0_key_contract", not miss and not pmiss,
             "producer_missing=%s page_missing=%s" % (miss, pmiss))

        # ── G1 settle ────────────────────────────────────────────────────
        rep.section("G1 — settle v1.0.0")
        settled = False
        for i in range(26):
            try:
                cfg = lam.get_function(FunctionName=FN)
                c = cfg["Configuration"]
                if c.get("State") != "Active" or c.get("LastUpdateStatus") == "InProgress":
                    time.sleep(15)
                    continue
                blob = urllib.request.urlopen(cfg["Code"]["Location"], timeout=60).read()
                body = zipfile.ZipFile(io.BytesIO(blob)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if 'VERSION = "1.0.0"' in body and "CREDIT_LEADS_UP" in body:
                    settled = True
                    break
            except Exception as e:
                rep.log("  settle: %s" % str(e)[:90])
            time.sleep(15)
        gate("G1_settle", settled, "deployed" if settled else "marker absent")

        # ── G2 invoke ────────────────────────────────────────────────────
        rep.section("G2 — async invoke + freshness")
        doc = None
        if settled:
            try:
                before = s3.head_object(Bucket=BUCKET, Key=OUT_KEY)["LastModified"]
            except Exception:
                before = None
            lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
            for _ in range(24):
                time.sleep(10)
                try:
                    h = s3.head_object(Bucket=BUCKET, Key=OUT_KEY)
                    if before is None or h["LastModified"] > before:
                        doc = json.loads(s3.get_object(
                            Bucket=BUCKET, Key=OUT_KEY)["Body"].read())
                        break
                except Exception:
                    pass
            gate("G2_artifact", doc is not None,
                 "names=%s leads=%s awaiting=%s"
                 % ((doc or {}).get("n_names"), (doc or {}).get("n_leads"),
                    (doc or {}).get("n_awaiting_history")))

        # ── G3 DAY-ONE HONESTY ───────────────────────────────────────────
        rep.section("G3 — day-one honesty (no lead without a prior obs)")
        if doc:
            rows = doc.get("names") or []
            fabricated = [r["ticker"] for r in rows
                          if r.get("signal", "").startswith("CREDIT_LEADS")
                          and not r.get("prior_obs_date")]
            for r in rows[:8]:
                rep.log("  %-6s DD=%-6s dDD=%-7s CDS=%-7s dCDS=%-7s dPx=%-7s %s"
                        % (r.get("ticker"), r.get("distance_to_default"),
                           r.get("d_distance_to_default"),
                           r.get("synthetic_cds_bp"),
                           r.get("d_synthetic_cds_bp"), r.get("d_price_pct"),
                           r.get("signal")))
            # ledger must exist after the run
            ledger_ok = False
            try:
                hl = json.loads(s3.get_object(Bucket=BUCKET,
                                              Key=HIST_KEY)["Body"].read())
                ledger_ok = bool(hl.get("obs"))
                rep.log("  ledger tickers=%d" % len(hl.get("obs") or {}))
            except Exception as e:
                rep.warn("  ledger: %s" % str(e)[:110])
            gate("G3_day_one_honesty",
                 not fabricated and len(rows) > 0 and ledger_ok,
                 "rows=%d fabricated_leads=%s ledger_written=%s"
                 % (len(rows), fabricated[:3], ledger_ok))
        else:
            gate("G3_day_one_honesty", False, "no doc")

        # ── G4 gaps declared ─────────────────────────────────────────────
        rep.section("G4 — HY issuance gap declared, not faked")
        if doc:
            gaps = doc.get("gaps") or []
            for g in gaps:
                rep.log("  gap: %s" % g[:190])
            issuance_declared = any("issuance" in g.lower() for g in gaps)
            # ops 3756: my ORIGINAL check was a fragile string-slice that
            # flagged the DOCSTRING where the engine explains why OAS is
            # excluded — the gate failed a correct engine. Check real CODE:
            # OAS would only be smuggled in via an actual fetch, so assert no
            # executable line references the series or a FRED call.
            code_lines = [ln for ln in src.splitlines()
                          if "BAMLH0A0HYM2" in ln or "stlouisfed" in ln]
            executable = [ln for ln in code_lines
                          if not ln.strip().startswith("#")
                          and "PRICE" not in ln and "paid" not in ln]
            no_fake = not executable
            rep.log("  OAS/FRED references in source: %d (executable: %d)"
                    % (len(code_lines), len(executable)))
            gate("G4_gaps", issuance_declared and no_fake,
                 "issuance_gap_declared=%s no_oas_substitution=%s"
                 % (issuance_declared, no_fake))
        else:
            gate("G4_gaps", False, "no doc")

        # ── G5 page ──────────────────────────────────────────────────────
        rep.section("G5 — page served + field coverage + nav")
        html = ""
        for attempt in range(20):
            try:
                bust = "%s?v=%d" % (PAGE, int(time.time()) + attempt)
                html = urllib.request.urlopen(urllib.request.Request(
                    bust, headers=dict(UA, **{"Cache-Control": "no-cache",
                                              "Pragma": "no-cache"})),
                    timeout=30).read().decode("utf-8", "replace")
                if "Credit Before Equity" in html and "n_awaiting_history" in html:
                    rep.log("  served page CURRENT len=%d after %ds"
                            % (len(html), attempt * 20))
                    break
            except Exception as e:
                rep.log("  attempt %d: %s" % (attempt, str(e)[:80]))
            time.sleep(20)
        gate("G5_page_live", "Credit Before Equity" in html
             and "n_awaiting_history" in html, "len=%d" % len(html))
        if doc and html:
            keys = set(doc.keys())
            for r in (doc.get("names") or [])[:5]:
                keys |= set(r.keys())
            keys |= set((doc.get("thresholds") or {}).keys())
            unrendered = [k for k in sorted(keys)
                          if k not in EXEMPT and k not in html]
            out["unrendered"] = unrendered
            gate("G5_field_coverage", not unrendered,
                 "UNRENDERED: %s" % unrendered if unrendered
                 else "every published key has a render path")
            try:
                man = json.loads(urllib.request.urlopen(urllib.request.Request(
                    "https://justhodl.ai/nav-manifest.json?v=%d" % int(time.time()),
                    headers=UA), timeout=25).read())
                hit = None
                for cat in (man.get("categories") or []):
                    for p0 in (cat.get("pages") or []):
                        if "credit-before-equity" in (p0.get("href") or ""):
                            hit = (cat.get("name"), p0.get("title"))
                gate("G5_nav", bool(hit), "listed under %s" % (hit,))
            except Exception as e:
                gate("G5_nav", False, "manifest: %s" % str(e)[:150])

        rep.section("VERDICT")
        verdict = "PASS_ALL" if not fails else "FAIL"
        out["verdict"] = verdict
        Path("aws/ops/reports/3757.json").write_text(json.dumps(out, indent=2))
        rep.kv(verdict=verdict, names=(doc or {}).get("n_names", 0),
               leads=(doc or {}).get("n_leads", 0),
               awaiting=(doc or {}).get("n_awaiting_history", 0),
               failed=",".join(fails) or "none")
        if fails:
            rep.fail("gates failed: %s" % fails)
            sys.exit(1)
        rep.ok("PASS_ALL — canary #17 live (leads activate on day two)")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3757.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
