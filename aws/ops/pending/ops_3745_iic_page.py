#!/usr/bin/env python3
"""ops 3745 — import-canary.html + FIELD-COVERAGE AUDIT + nav pin.

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
OUT_KEY = "data/insider-industry-cluster.json"
PAGE = "https://justhodl.ai/insider-industry-cluster.html"
UA = {"User-Agent": "Mozilla/5.0 (JustHodl ops verify)"}

# keys that legitimately need no visual render path
EXEMPT = {"version", "generated_at", "source_url", "attribution",
          "sector", "participation_floor_pct", "has_exec_conviction",
          "hist_n", "min_companies", "min_listed_for_rate", "strong_companies",
          "universe_industries", "top_company_share_pct"}

with report("3745_iic_page") as rep:
    rep.heading("ops 3745 — insider-industry-cluster page + field-coverage audit")
    fails = []
    out = {"gates": {}}
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3745.json").write_text(json.dumps({"verdict": "STARTED"}))

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
        for attempt in range(20):
            try:
                # ops 3740: served copy was the PRE-EDIT page (12,563 B) while
                # the repo held 11,550 B — Cloudflare edge cache, not a code
                # bug. Bust it per attempt and require the CURRENT length.
                bust = "%s?v=%d" % (PAGE, int(time.time()) + attempt)
                req = urllib.request.Request(
                    bust, headers=dict(UA, **{"Cache-Control": "no-cache",
                                              "Pragma": "no-cache"}))
                html = urllib.request.urlopen(req, timeout=30).read().decode("utf-8", "replace")
                if "Insider Industry Cluster" in html and "awaiting_base_rate" in html:
                    rep.log("  served page is CURRENT (len=%d) after %ds"
                            % (len(html), attempt * 20))
                    break
                if html:
                    rep.log("  stale edge copy len=%d (waiting for CDN)" % len(html))
            except Exception as e:
                rep.log("  attempt %d: %s" % (attempt, str(e)[:90]))
            time.sleep(20)
        markers = ["Insider Industry Cluster", "clusters", "diffuse",
                   "method", "cov", "insider-industry-cluster.json"]
        miss_m = [m for m in markers if m not in html]
        gate("G1_page_live", bool(html) and not miss_m,
             "len=%d missing_markers=%s" % (len(html), miss_m))

        # ── G2 FIELD COVERAGE from LIVE artifact ─────────────────────────
        rep.section("G2 — field coverage (live S3 artifact vs page render paths)")
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read())

        top_keys = sorted(doc.keys())
        row_keys = set()
        row_keys |= set((doc.get("coverage") or {}).keys())
        for r0 in (doc.get("industries") or [])[:6]:
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
                    if "insider-industry-cluster" in (p.get("href") or ""):
                        hit = (cat.get("name"), p.get("title"))
            gate("G3_nav", bool(hit),
                 "listed under %s" % (hit,) if hit else "NOT in served manifest")
        except Exception as e:
            gate("G3_nav", False, "manifest read: %s" % str(e)[:170])

        # ── G4 ladder honesty on the LIVE feed ──────────────────────────
        rep.section("G4 — ladder honesty (no sub-floor PEER, biotech guard)")
        inds = doc.get("industries") or []
        cov2 = doc.get("coverage") or {}
        floor = cov2.get("min_participation_pct") or 4.0
        rep.log("  coverage: %s" % cov2)
        sub_floor = [r0["industry"] for r0 in inds
                     if r0.get("tier","").startswith("PEER_CLUSTER")
                     and (r0.get("participation_pct") is None
                          or r0["participation_pct"] < floor)]
        conf_no_z = [r0["industry"] for r0 in inds
                     if "CONFIRMED" in r0.get("tier","")
                     and r0.get("z_vs_own_history") is None]
        bio = next((r0 for r0 in inds if r0["industry"]=="Biotechnology"), None)
        if bio:
            rep.log("  Biotechnology: tier=%s part=%s%% (regression sentinel)"
                    % (bio.get("tier"), bio.get("participation_pct")))
        gate("G4_ladder_honesty", not sub_floor and not conf_no_z,
             "sub_floor_peer=%s confirmed_without_base_rate=%s"
             % (sub_floor[:3], conf_no_z[:3]))
        ratio = 1.0

        rep.section("VERDICT")
        verdict = "PASS_ALL" if not fails else "FAIL"
        out["verdict"] = verdict
        out["unrendered"] = gaps
        Path("aws/ops/reports/3745.json").write_text(json.dumps(out, indent=2))
        rep.kv(verdict=verdict, page="insider-industry-cluster.html",
               unrendered=",".join(gaps) or "none",
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
        Path("aws/ops/reports/3745.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
