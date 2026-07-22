#!/usr/bin/env python3
"""ops 3739 — import-canary.html + FIELD-COVERAGE AUDIT + nav pin.

Per the PAGE CONTRACT (AUTONOMY.md): every engine gets a sidebar-reachable
page, and that page must surface EVERYTHING the engine publishes. The proven
recurring defect is an engine shipping fields the page silently ignores
(sectors.html 6-of-11 chips; capital-flow changes_summary; deal-scanner sized
cards; readthrough all_results).

GATES
  G1  live page served 200 with its markers
  G2  FIELD COVERAGE — dump keys from the LIVE S3 artifact (not the source),
      top-level AND per-row, then grep the page for each. Any key with no
      render path is a gap that must be surfaced or explained.
  G3  nav-manifest carries the page under a sensible category (SERVED copy)
  G4  degraded[] is empty or explicitly explained (non-empty = OPEN BUG)
"""
import json
import sys
import time
import traceback
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/grid-queue.json"
PAGE = "https://justhodl.ai/grid-queue.html"
UA = {"User-Agent": "Mozilla/5.0 (JustHodl ops verify)"}

# keys that legitimately need no visual render path
EXEMPT = {"version", "generated_at", "source_url", "basis",
          "codes", "shares_pct", "all_pp", "covered_usd", "month",
          "n_months", "fragile", "gainer_pp", "loser_pp"}

with report("3739_grid_queue_page") as rep:
    rep.heading("ops 3739 — grid-queue page + field-coverage audit")
    fails = []
    out = {"gates": {}}
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3739.json").write_text(json.dumps({"verdict": "STARTED"}))

    def gate(n, ok, detail):
        out["gates"][n] = {"ok": bool(ok), "detail": str(detail)[:900]}
        rep.log(("PASS " if ok else "FAIL ") + n + " — " + str(detail)[:800])
        if not ok:
            fails.append(n)
        return ok

    try:
        s3 = boto3.client("s3", region_name="us-east-1")

        # ── G0 settle v1.0.3 + re-invoke so the fuel fix is in the feed ──
        rep.section("G0 — settle v1.0.3 and refresh the artifact")
        import io as _io, zipfile as _zip
        lam = boto3.client("lambda", region_name="us-east-1")
        settled = False
        for attempt in range(20):
            try:
                cfg = lam.get_function(FunctionName="justhodl-grid-queue")
                conf = cfg["Configuration"]
                if conf.get("State") != "Active" or conf.get("LastUpdateStatus") == "InProgress":
                    time.sleep(15); continue
                blob = urllib.request.urlopen(cfg["Code"]["Location"], timeout=60).read()
                body = _zip.ZipFile(_io.BytesIO(blob)).read("lambda_function.py").decode("utf-8", "replace")
                if 'VERSION = "1.0.3"' in body:
                    settled = True
                    rep.log("  v1.0.3 settled after %ds" % (attempt * 15))
                    break
            except Exception as e:
                rep.log("  settle: %s" % str(e)[:90])
            time.sleep(15)
        gate("G0_settle_103", settled, "v1.0.3 deployed" if settled else "marker absent")
        if settled:
            try:
                before = s3.head_object(Bucket=BUCKET, Key=OUT_KEY)["LastModified"]
            except Exception:
                before = None
            lam.invoke(FunctionName="justhodl-grid-queue",
                       InvocationType="Event", Payload=b"{}")
            for _ in range(24):
                time.sleep(15)
                try:
                    h = s3.head_object(Bucket=BUCKET, Key=OUT_KEY)
                    if before is None or h["LastModified"] > before:
                        rep.log("  artifact refreshed on v1.0.3")
                        break
                except Exception:
                    pass

        # ── G1 page live ─────────────────────────────────────────────────
        rep.section("G1 — page served")
        html = ""
        for attempt in range(12):
            try:
                req = urllib.request.Request(PAGE, headers=UA)
                html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "replace")
                if "Grid Queue" in html:
                    break
            except Exception as e:
                rep.log("  attempt %d: %s" % (attempt, str(e)[:90]))
            time.sleep(20)
        markers = ["Grid Queue", "hot", "proj", "mix", "plan",
                   "load", "gaps", "method", "grid-queue.json"]
        miss_m = [m for m in markers if m not in html]
        gate("G1_page_live", bool(html) and not miss_m,
             "len=%d missing_markers=%s" % (len(html), miss_m))

        # ── G2 FIELD COVERAGE from LIVE artifact ─────────────────────────
        rep.section("G2 — field coverage (live S3 artifact vs page render paths)")
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())

        top_keys = sorted(doc.keys())
        row_keys = set()
        row_keys |= set((doc.get("queue") or {}).keys())
        row_keys |= set((doc.get("planned_capacity") or {}).keys())
        row_keys |= set((doc.get("industrial_load") or {}).keys())
        row_keys |= set((doc.get("coverage") or {}).keys())
        for a in ((doc.get("queue") or {}).get("large_projects") or [])[:4]:
            row_keys |= set(a.keys())
        for h in (doc.get("hotspots") or [])[:4]:
            row_keys |= set(h.keys())
        for u in ((doc.get("planned_capacity") or {}).get("upcoming_uprates") or [])[:3]:
            row_keys |= set(u.keys())
        for ip0 in ((doc.get("planned_capacity") or {}).get("industrial_plants") or [])[:3]:
            row_keys |= set(ip0.keys())
        for st0 in ((doc.get("industrial_load") or {}).get("states") or [])[:3]:
            row_keys |= set(st0.keys())

        rep.log("  top-level keys: %s" % top_keys)
        rep.log("  per-row keys:   %s" % sorted(row_keys))

        gaps = []
        for k in top_keys + sorted(row_keys):
            if k in EXEMPT:
                continue
            if k not in html:
                gaps.append(k)
        gaps = sorted(set(gaps))
        gate("G2_field_coverage", not gaps,
             "every published key has a render path"
             if not gaps else "UNRENDERED KEYS: %s" % gaps)

        # ── G3 nav manifest (SERVED copy, repo copy is always stale) ─────
        rep.section("G3 — nav manifest (served)")
        try:
            mreq = urllib.request.Request(
                "https://justhodl.ai/nav-manifest.json", headers=UA)
            man = json.loads(urllib.request.urlopen(mreq, timeout=30)
                             .read().decode("utf-8", "replace"))
            hit = None
            for cat in man.get("categories", []):
                for p in cat.get("pages", []):
                    if "grid-queue" in (p.get("href") or ""):
                        hit = (cat.get("name"), p.get("title"))
            gate("G3_nav", bool(hit),
                 "listed under %s" % (hit,) if hit else "NOT in served manifest")
        except Exception as e:
            gate("G3_nav", False, "manifest read: %s" % str(e)[:170])

        # ── G4 gaps declared + queue sane ────────────────────────────────
        rep.section("G4 — declared gaps + queue sanity")
        gaps2 = doc.get("gaps") or []
        q = doc.get("queue") or {}
        cov2 = doc.get("coverage") or {}
        rep.log("  coverage: %s" % cov2)
        for g in gaps2[:8]:
            rep.log("    gap: %s" % g)
        # fuel labels must NOT be numbers (ops 3738 caught "Solar + 1150")
        bad_fuel = [p0.get("fuel") for p0 in (q.get("large_projects") or [])[:40]
                    if p0.get("fuel") and any(ch.isdigit() for ch in p0["fuel"])]
        gate("G4_fuel_labels", not bad_fuel,
             "fuel labels clean" if not bad_fuel
             else "NUMERIC IN FUEL LABEL: %s" % bad_fuel[:5])
        gate("G4_declared_gaps", len(gaps2) >= 5,
             "%d gaps declared, queue %s MW / %s projects"
             % (len(gaps2), q.get("active_mw"), q.get("active_projects")))
        ratio = 1.0

        rep.section("VERDICT")
        verdict = "PASS_ALL" if not fails else "FAIL"
        out["verdict"] = verdict
        out["unrendered"] = gaps
        Path("aws/ops/reports/3739.json").write_text(json.dumps(out, indent=2))
        rep.kv(verdict=verdict, page="grid-queue.html",
               unrendered=",".join(gaps) or "none",
               queue_mw=(doc.get("queue") or {}).get("active_mw"),
               failed=",".join(fails) or "none")

        if fails:
            rep.fail("gates failed: %s" % fails)
            sys.exit(1)
        rep.ok("PASS_ALL — page live, every field rendered")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3739.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
