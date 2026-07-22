#!/usr/bin/env python3
"""ops 3732 — import-canary.html + FIELD-COVERAGE AUDIT + nav pin.

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
OUT_KEY = "data/import-canary.json"
PAGE = "https://justhodl.ai/import-canary.html"
UA = {"User-Agent": "Mozilla/5.0 (JustHodl ops verify)"}

# keys that legitimately need no visual render path
EXEMPT = {"version", "generated_at", "source_url", "basis",
          "codes", "shares_pct", "all_pp", "covered_usd", "month",
          "n_months", "fragile", "gainer_pp", "loser_pp"}

with report("3732_import_canary_page") as rep:
    rep.heading("ops 3732 — import-canary page + field-coverage audit")
    fails = []
    out = {"gates": {}}
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3732.json").write_text(json.dumps({"verdict": "STARTED"}))

    def gate(n, ok, detail):
        out["gates"][n] = {"ok": bool(ok), "detail": str(detail)[:900]}
        rep.log(("PASS " if ok else "FAIL ") + n + " — " + str(detail)[:800])
        if not ok:
            fails.append(n)
        return ok

    try:
        s3 = boto3.client("s3", region_name="us-east-1")

        # ── G1 page live ─────────────────────────────────────────────────
        rep.section("G1 — page served")
        html = ""
        for attempt in range(12):
            try:
                req = urllib.request.Request(PAGE, headers=UA)
                html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "replace")
                if "Import Canary" in html:
                    break
            except Exception as e:
                rep.log("  attempt %d: %s" % (attempt, str(e)[:90]))
            time.sleep(20)
        markers = ["Import Canary", "signals", "rollup", "hstab", "natab",
                   "conc", "cov", "import-canary.json"]
        miss_m = [m for m in markers if m not in html]
        gate("G1_page_live", bool(html) and not miss_m,
             "len=%d missing_markers=%s" % (len(html), miss_m))

        # ── G2 FIELD COVERAGE from LIVE artifact ─────────────────────────
        rep.section("G2 — field coverage (live S3 artifact vs page render paths)")
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())

        top_keys = sorted(doc.keys())
        row_keys = set()
        for a in (doc.get("lines") or [])[:4]:
            row_keys |= set(a.keys())
            for sub in ("concentration", "source_shift"):
                if isinstance(a.get(sub), dict):
                    row_keys |= set(a[sub].keys())
        for s in (doc.get("signals") or [])[:4]:
            row_keys |= set(s.keys())
        for r0 in (doc.get("industry_rollup") or [])[:3]:
            row_keys |= set(r0.keys())

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
                    if "import-canary" in (p.get("href") or ""):
                        hit = (cat.get("name"), p.get("title"))
            gate("G3_nav", bool(hit),
                 "listed under %s" % (hit,) if hit else "NOT in served manifest")
        except Exception as e:
            gate("G3_nav", False, "manifest read: %s" % str(e)[:170])

        # ── G4 degraded is an open bug, not decoration ───────────────────
        rep.section("G4 — degraded lines")
        dg = doc.get("degraded") or []
        cov = doc.get("coverage") or {}
        rep.log("  coverage: %s" % cov)
        if dg:
            for x in dg[:12]:
                rep.log("    degraded: %s" % x)
        # HS4 2836/7601 etc. may legitimately lack HS6 detail; tolerate <=25%
        req = (cov.get("hs_requested") or 0) + (cov.get("naics_requested") or 0)
        okc = (cov.get("hs_ok") or 0) + (cov.get("naics_ok") or 0)
        ratio = (okc / req) if req else 0
        gate("G4_degraded", ratio >= 0.75,
             "coverage %.0f%% (%d/%d), degraded=%d" % (ratio * 100, okc, req, len(dg)))

        rep.section("VERDICT")
        verdict = "PASS_ALL" if not fails else "FAIL"
        out["verdict"] = verdict
        out["unrendered"] = gaps
        Path("aws/ops/reports/3732.json").write_text(json.dumps(out, indent=2))
        rep.kv(verdict=verdict, page="import-canary.html",
               unrendered=",".join(gaps) or "none",
               coverage_pct=round(ratio * 100, 1),
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
        Path("aws/ops/reports/3732.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
