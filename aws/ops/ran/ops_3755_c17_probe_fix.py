#!/usr/bin/env python3
"""ops 3755 — PROBE for canary #17 (credit moves BEFORE equity).

AUDIT (repo, 3754) — what already exists and why none of it is #17:
  · credit-equity-divergence = INDEX level (HYG vs SPY). A macro read: "is
    credit confirming the tape". It cannot name a company.
  · cds-monitor = CreditGrades structural model per name (DD, synthetic
    spread). Excellent, but POINT-IN-TIME: no history ledger, so it answers
    "who is risky today", never "whose credit improved this month while the
    stock hasn't noticed". The DIRECTION over time is the whole canary.
  · credit-composite / credit-stress / cds-proxy = aggregate + sovereign.
  · No engine anywhere tracks HY primary issuance windows.
Canary #17 = per-ISSUER credit improving (DD rising / synthetic spread
tightening) while the EQUITY is flat or down = the bond desk repricing risk
before the equity desk does. That is a genuine, buildable gap.

PROBE (build nothing until the shape is proven)
  A  data/cds-monitor.json — row shape, n names, DD + spread field names
  B  is there ANY history ledger key already accreting for these names?
  C  price change per name over a comparable window — reuse whatever the
     fleet already has (universe / census / short-interest price join)
     rather than adding a new price fetch
  D  HY issuance: is anything reachable free (FRED HY OAS is a PRICE not an
     issuance count; SIFMA/FINRA TRACE are the real issuance sources)
"""
import json
import ssl
import sys
import traceback
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
UA = {"User-Agent": "Mozilla/5.0 (compatible; JustHodl research)"}
CTX = ssl.create_default_context()
FRED_KEY = "2f057499936072679d8843d7fce99989"

with report("3755_c17_probe_fix") as rep:
    rep.heading("ops 3755 — canary #17 credit-before-equity probe")
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3755.json").write_text(json.dumps({"verdict": "STARTED"}))
    out = {}

    try:
        s3 = boto3.client("s3", region_name="us-east-1")

        def load(key):
            try:
                return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
            except Exception as e:
                rep.warn("  %s -> %s" % (key, str(e)[:100]))
                return None

        # ── A cds-monitor shape ──────────────────────────────────────────
        rep.section("A — data/cds-monitor.json shape")
        cm = load("data/cds-monitor.json")
        rows = []
        if cm:
            rep.ok("  top-level: %s" % sorted(cm.keys())[:16])
            # ops 3754 BUG: only scanned TOP-LEVEL lists, but cds-monitor
            # nests the names under single_name_cds / sovereign_cds — the
            # probe declared a healthy feed "unusable". Walk 2 levels.
            def walk(obj, path=""):
                hits = []
                if isinstance(obj, list) and obj and isinstance(obj[0], dict):
                    hits.append((path or "<root>", obj))
                elif isinstance(obj, dict):
                    for kk, vv in obj.items():
                        hits.extend(walk(vv, (path + "." + kk) if path else kk))
                return hits
            allists = walk(cm)
            for pth, lst in sorted(allists, key=lambda x: -len(x[1]))[:8]:
                rep.log("  list '%s' n=%d keys=%s"
                        % (pth, len(lst), sorted(lst[0].keys())[:10]))
            if allists:
                pth, rows = max(allists, key=lambda x: len(x[1]))
                rep.ok("  BIGGEST list = '%s' n=%d" % (pth, len(rows)))
                out["rows_path"] = pth
            if rows:
                rep.log("  row keys: %s" % sorted(rows[0].keys()))
                rep.log("  sample: %s" % json.dumps(rows[0])[:420])
                # which fields are the credit READ?
                cand = [k for k in rows[0]
                        if any(w in k.lower() for w in
                               ("dd", "distance", "spread", "prob", "pd",
                                "credit", "score", "rank", "tier"))]
                rep.log("  credit-ish fields: %s" % cand)
                out["cds_rows"] = len(rows)
                out["cds_fields"] = sorted(rows[0].keys())
                out["credit_fields"] = cand

        # ── B any accreting history for these names? ─────────────────────
        rep.section("B — existing history ledgers (do NOT rebuild)")
        for k in ("cds/cds-monitor-history.json",
                  "data/cds-monitor-history.json",
                  "credit/credit-history.json",
                  "data/credit-composite-history.json"):
            d = load(k)
            if d:
                rep.ok("  FOUND %s keys=%s" % (k, sorted(d.keys())[:8]))
                out.setdefault("history_found", []).append(k)
        if not out.get("history_found"):
            rep.log("  none — the #17 engine must build its OWN ledger "
                    "(same self-building pattern as import-canary / IIC)")

        # ── C price leg: reuse what the fleet already has ────────────────
        rep.section("C — price change per name (reuse, don't refetch)")
        for k in ("data/short-interest.json", "data/universe.json",
                  "data/fundamental-census-matrix.json"):
            d = load(k)
            if not d:
                continue
            best, bk = [], None
            for kk, vv in (d.items() if isinstance(d, dict) else []):
                if isinstance(vv, list) and vv and isinstance(vv[0], dict):
                    if len(vv) > len(best):
                        best, bk = vv, kk
            if best:
                pk = [c for c in best[0]
                      if any(w in c.lower() for w in
                             ("price", "chg", "change", "ret", "perf"))]
                rep.ok("  %s -> list '%s' n=%d price-ish=%s"
                       % (k, bk, len(best), pk[:8]))
                out.setdefault("price_sources", {})[k] = {"list": bk,
                                                          "n": len(best),
                                                          "fields": pk[:8]}

        # ── D HY issuance reachability ───────────────────────────────────
        rep.section("D — HY issuance (is it free-reachable at all?)")
        # FRED HY OAS is a PRICE, not issuance — check what issuance series exist
        try:
            u = ("https://api.stlouisfed.org/fred/series/search?search_text="
                 + "corporate+bond+issuance&api_key=" + FRED_KEY
                 + "&file_type=json&limit=10")
            j = json.loads(urllib.request.urlopen(
                urllib.request.Request(u, headers=UA), timeout=25,
                context=CTX).read())
            ss = j.get("seriess") or []
            rep.ok("  FRED 'corporate bond issuance' -> %d series" % len(ss))
            for s0 in ss[:8]:
                rep.log("    %-24s %s" % (s0.get("id"),
                                          s0.get("title", "")[:74]))
            out["fred_issuance"] = [s0.get("id") for s0 in ss[:8]]
        except Exception as e:
            rep.warn("  FRED issuance search: %s" % str(e)[:120])
        # the OAS we already have (contrast — price, not volume)
        for sid in ("BAMLH0A0HYM2", "BAMLC0A0CM"):
            try:
                u = ("https://api.stlouisfed.org/fred/series/observations?series_id="
                     + sid + "&api_key=" + FRED_KEY
                     + "&file_type=json&sort_order=desc&limit=2")
                obs = json.loads(urllib.request.urlopen(
                    urllib.request.Request(u, headers=UA), timeout=20,
                    context=CTX).read()).get("observations") or []
                rep.ok("  %s latest=%s (%s) [PRICE not issuance]"
                       % (sid, obs[0].get("value") if obs else "?",
                          obs[0].get("date") if obs else "?"))
            except Exception as e:
                rep.warn("  %s: %s" % (sid, str(e)[:90]))

        rep.section("VERDICT")
        buildable = bool(out.get("cds_rows"))
        rep.kv(cds_rows=out.get("cds_rows", 0),
               credit_fields=",".join(out.get("credit_fields", [])[:5]),
               history_exists=bool(out.get("history_found")),
               buildable=str(buildable))
        Path("aws/ops/reports/3755.json").write_text(
            json.dumps({"verdict": "PASS" if buildable else "BLOCKED",
                        "found": out}, indent=2, default=str))
        if not buildable:
            rep.fail("cds-monitor feed unusable — #17 needs a different base")
            sys.exit(1)
        rep.ok("PROBE COMPLETE")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3755.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
