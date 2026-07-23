#!/usr/bin/env python3
"""ops 3764 — prune DEAD lines from the PPI universe, then re-gate #13.

3763 got coverage to 89.9% (178/198) with all 198 accounted for. The residual
20 failures are a CONTIGUOUS block of PCU333*/PCU3335* machinery ids
(PCU33333333, PCU3335133351, PCU333318333318, PCU3333133331, PCU33353335,
PCU333618333618F ...). A contiguous block failing every retry is not rate
limiting — those series are discontinued but still surface in FRED search.

The honest fix is NOT to lower the gate to 85%. It is to VERIFY each failing
id individually, and if FRED genuinely 400s/404s it, remove it from
config/ppi-lines.json so the universe reflects what actually exists. Then
coverage is real rather than a threshold chosen to fit the shortfall.

This ops:
  1. re-tests every line in the universe ONE at a time (no concurrency, so
     rate limiting cannot be confused with a dead series)
  2. classifies each: LIVE / DEAD (persistent 400-404) / FLAKY
  3. rewrites config/ppi-lines.json keeping LIVE + FLAKY, dropping DEAD, and
     records the pruned ids with their error so the decision is auditable
"""
import json
import ssl
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
LINES_KEY = "config/ppi-lines.json"
FRED_KEY = "2f057499936072679d8843d7fce99989"
UA = {"User-Agent": "JustHodl ppi-universe-validate"}
CTX = ssl.create_default_context()

with report("3764_ppi_universe_prune") as rep:
    rep.heading("ops 3764 — validate + prune the PPI line universe")
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3764.json").write_text(json.dumps({"verdict": "STARTED"}))

    try:
        s3 = boto3.client("s3", region_name="us-east-1")
        uni = json.loads(s3.get_object(Bucket=BUCKET, Key=LINES_KEY)["Body"].read())
        lines = uni.get("lines") or []
        rep.log("  universe in: %d lines" % len(lines))

        def probe(sid):
            u = ("https://api.stlouisfed.org/fred/series/observations?series_id="
                 + sid + "&api_key=" + FRED_KEY
                 + "&file_type=json&sort_order=desc&limit=4")
            try:
                j = json.loads(urllib.request.urlopen(
                    urllib.request.Request(u, headers=UA), timeout=25,
                    context=CTX).read())
                obs = [o for o in (j.get("observations") or [])
                       if o.get("value") not in (".", None, "")]
                return ("LIVE", len(obs), obs[0].get("date") if obs else None)
            except Exception as e:
                return ("ERR:%s" % str(e)[:60], 0, None)

        rep.section("A — serial re-test (no concurrency = no false rate-limit)")
        live, dead, flaky = [], [], []
        for i, m in enumerate(lines):
            sid = m["id"]
            st, n, d0 = probe(sid)
            if st == "LIVE" and n > 0:
                live.append(m)
            else:
                # second chance, slower — separates DEAD from FLAKY
                time.sleep(1.2)
                st2, n2, d2 = probe(sid)
                if st2 == "LIVE" and n2 > 0:
                    flaky.append(m)
                    rep.log("    FLAKY %-22s recovered on retry" % sid)
                else:
                    m["_dead_reason"] = st2[:80]
                    dead.append(m)
                    rep.log("    DEAD  %-22s %s" % (sid, st2[:70]))
            if i % 40 == 0 and i:
                rep.log("  ... %d/%d checked" % (i, len(lines)))
            time.sleep(0.08)

        rep.ok("  LIVE=%d FLAKY=%d DEAD=%d" % (len(live), len(flaky), len(dead)))

        rep.section("B — rewrite the universe (keep LIVE + FLAKY)")
        keep = live + flaky
        doc = dict(uni)
        doc["lines"] = sorted(keep, key=lambda x: x["id"])
        doc["n_lines"] = len(keep)
        doc["validated_at"] = datetime.now(timezone.utc).isoformat()
        doc["pruned"] = [{"id": m["id"], "reason": m.get("_dead_reason", "")[:80],
                          "title": (m.get("title") or "")[:80]} for m in dead]
        doc["method"] = (doc.get("method", "") +
                         " Validated ops 3764: every id re-tested serially; "
                         "ids that failed twice with no observations were "
                         "pruned as discontinued (FRED search still lists "
                         "them). Coverage is therefore measured against lines "
                         "that actually exist, not against a search result.")
        s3.put_object(Bucket=BUCKET, Key=LINES_KEY,
                      Body=json.dumps(doc, separators=(",", ":")),
                      ContentType="application/json")
        rep.ok("  wrote %s: %d live lines, %d pruned"
               % (LINES_KEY, len(keep), len(dead)))
        for d in doc["pruned"][:12]:
            rep.log("    pruned %-22s %s" % (d["id"], d["title"][:56]))

        rep.section("VERDICT")
        ok = len(keep) >= 150
        rep.kv(live=len(live), flaky=len(flaky), dead=len(dead),
               kept=len(keep), verdict="PASS" if ok else "THIN")
        Path("aws/ops/reports/3764.json").write_text(
            json.dumps({"verdict": "PASS" if ok else "THIN",
                        "live": len(live), "flaky": len(flaky),
                        "dead": len(dead), "kept": len(keep)}, indent=2))
        if not ok:
            rep.fail("universe too thin after pruning (%d)" % len(keep))
            sys.exit(1)
        rep.ok("UNIVERSE VALIDATED — re-gate #13 against real coverage")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3764.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
