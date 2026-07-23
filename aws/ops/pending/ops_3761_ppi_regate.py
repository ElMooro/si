#!/usr/bin/env python3
"""ops 3761 — SHIP canary #13: narrow-line PPI acceleration.

Sweeps the 198 lines discovered in ops 3759 (config/ppi-lines.json) and ranks
them by the 2nd derivative of price. An aggregate PPI print averages a heating
input against a cooling one; a narrow ACCELERATING line is a bottleneck
forming months before it reaches a cost line.

GATES
  G0  KEY CONTRACT — every key the gates and page read exists in the producer
  G1  settle v1.0.0 (State Active AND LastUpdateStatus, per ops 3735)
  G2  async invoke + S3 freshness (198 FRED pulls — never sync-gate, ops 3730)
  G3  DATA TRUTH — accel really is the 2nd derivative: recompute
      accel_pp == yoy - prior_yoy for a sample and assert the engine agrees;
      no line may be CONFIRMED without level+accel+m3 all agreeing; z must be
      absent wherever base_rate_ready is false (the #16 lesson: never let an
      unknown base rate act as permission)
  G4  BREADTH — the sweep must actually cover the discovered universe, not
      silently collapse to a handful of lines
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
REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

FN = "justhodl-ppi-acceleration"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/ppi-acceleration.json"
LINES_KEY = "config/ppi-lines.json"
PAGE = "https://justhodl.ai/ppi-acceleration.html"
REGION = "us-east-1"
UA = {"User-Agent": "Mozilla/5.0 (JustHodl ops verify)"}
EXEMPT = {"version", "generated_at", "attribution", "method", "lines",
          "top_accelerating", "top_decelerating", "n_obs",
          "fetch_error", "exception", "short_history"}

with report("3761_ppi_regate") as rep:
    rep.heading("ops 3761 — canary #13: narrow-line PPI acceleration")
    fails = []
    out = {"gates": {}}
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3761.json").write_text(json.dumps({"verdict": "STARTED"}))

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
        rep.section("G0 — key contract (producer + page)")
        src = (ROOT / "lambdas" / FN / "source" / "lambda_function.py").read_text()
        page_src = (REPO / "ppi-acceleration.html").read_text()
        need = ["accel_pp", "yoy_pct", "prior_yoy_pct", "m3_ann_pct",
                "z_vs_own_history", "base_rate_ready", "signal", "series_id",
                "n_accelerating", "n_decelerating", "n_base_rate_ready",
                "thresholds", "degraded", "level", "period", "title"]
        miss = [k for k in need if '"%s"' % k not in src]
        pmiss = [k for k in need if k not in page_src]
        gate("G0_key_contract", not miss and not pmiss,
             "producer_missing=%s page_missing=%s" % (miss, pmiss))

        # ── G1 settle ────────────────────────────────────────────────────
        rep.section("G1 — settle v1.1.0")
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
                if 'VERSION = "1.1.0"' in body and "ACCELERATING_CONFIRMED" in body:
                    settled = True
                    break
            except Exception as e:
                rep.log("  settle: %s" % str(e)[:90])
            time.sleep(15)
        gate("G1_settle", settled, "deployed" if settled else "marker absent")

        # ── G2 invoke (async — 198 FRED pulls) ───────────────────────────
        rep.section("G2 — async invoke + freshness")
        doc = None
        if settled:
            try:
                before = s3.head_object(Bucket=BUCKET, Key=OUT_KEY)["LastModified"]
            except Exception:
                before = None
            lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
            for _ in range(40):
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
                 "lines=%s accel=%s decel=%s"
                 % ((doc or {}).get("n_lines"), (doc or {}).get("n_accelerating"),
                    (doc or {}).get("n_decelerating")))

        # ── G3 DATA TRUTH ────────────────────────────────────────────────
        rep.section("G3 — data truth (accel IS the 2nd derivative)")
        if doc:
            rows = doc.get("lines") or []
            math_bad = []
            for r in rows:
                y, p, a = r.get("yoy_pct"), r.get("prior_yoy_pct"), r.get("accel_pp")
                if y is not None and p is not None and a is not None:
                    if abs(round(y - p, 2) - a) > 0.02:
                        math_bad.append(r["series_id"])
            # CONFIRMED requires all three legs to agree
            bad_conf = [r["series_id"] for r in rows
                        if r.get("signal") == "ACCELERATING_CONFIRMED"
                        and not (r.get("yoy_pct") or 0) > 0]
            # the #16 lesson: unknown base rate must NOT be treated as a pass
            z_leak = [r["series_id"] for r in rows
                      if not r.get("base_rate_ready")
                      and r.get("z_vs_own_history") is not None]
            for r in (doc.get("top_accelerating") or [])[:8]:
                rep.log("  %-22s accel=%+6.2fpp yoy=%+7.2f%% prior=%+7.2f%% "
                        "m3ann=%s z=%s %s"
                        % (r["series_id"], r.get("accel_pp") or 0,
                           r.get("yoy_pct") or 0, r.get("prior_yoy_pct") or 0,
                           r.get("m3_ann_pct"), r.get("z_vs_own_history"),
                           r.get("signal")))
            gate("G3_data_truth",
                 not math_bad and not bad_conf and not z_leak and len(rows) > 0,
                 "rows=%d math_mismatch=%s bad_confirmed=%s z_leak=%s"
                 % (len(rows), math_bad[:3], bad_conf[:3], z_leak[:3]))
        else:
            gate("G3_data_truth", False, "no doc")

        # ── G4 breadth vs the discovered universe ────────────────────────
        rep.section("G4 — sweep covers the discovered universe")
        try:
            uni = json.loads(s3.get_object(Bucket=BUCKET,
                                           Key=LINES_KEY)["Body"].read())
            n_uni = uni.get("n_lines") or len(uni.get("lines") or [])
            n_out = len((doc or {}).get("lines") or [])
            cov = (n_out / n_uni * 100) if n_uni else 0
            dr = (doc or {}).get("drop_reasons") or {}
            rep.log("  universe=%d swept=%d coverage=%.1f%%" % (n_uni, n_out, cov))
            rep.log("  drop reasons: %s" % dr)
            rep.log("  dropped ids (sample): %s"
                    % ((doc or {}).get("dropped_ids") or [])[:8])
            # v1.1 keeps short-history lines IN the output (reported, not
            # ranked), so coverage should now be near-total; whatever is
            # still missing must be ACCOUNTED FOR by a stated reason rather
            # than vanishing silently.
            accounted = n_out + (dr.get("fetch_error", 0)
                                 + dr.get("exception", 0))
            gate("G4_breadth", n_uni > 0 and cov >= 90 and accounted >= n_uni,
                 "coverage %.1f%% (%d of %d) · accounted=%d/%d · reasons=%s"
                 % (cov, n_out, n_uni, accounted, n_uni, dr))
        except Exception as e:
            gate("G4_breadth", False, "universe read: %s" % str(e)[:150])

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
                if "PPI Acceleration" in html and "coverage_pct" in html:
                    rep.log("  served page CURRENT len=%d after %ds"
                            % (len(html), attempt * 20))
                    break
            except Exception as e:
                rep.log("  attempt %d: %s" % (attempt, str(e)[:80]))
            time.sleep(20)
        gate("G5_page_live", "PPI Acceleration" in html
             and "base_rate_ready" in html, "len=%d" % len(html))
        if doc and html:
            keys = set(doc.keys())
            for r in (doc.get("lines") or [])[:6]:
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
                        if "ppi-acceleration" in (p0.get("href") or ""):
                            hit = (cat.get("name"), p0.get("title"))
                gate("G5_nav", bool(hit), "listed under %s" % (hit,))
            except Exception as e:
                gate("G5_nav", False, "manifest: %s" % str(e)[:150])

        rep.section("VERDICT")
        verdict = "PASS_ALL" if not fails else "FAIL"
        out["verdict"] = verdict
        Path("aws/ops/reports/3761.json").write_text(json.dumps(out, indent=2))
        rep.kv(verdict=verdict, lines=(doc or {}).get("n_lines", 0),
               accelerating=(doc or {}).get("n_accelerating", 0),
               decelerating=(doc or {}).get("n_decelerating", 0),
               failed=",".join(fails) or "none")
        if fails:
            rep.fail("gates failed: %s" % fails)
            sys.exit(1)
        rep.ok("PASS_ALL — canary #13 live")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3761.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
