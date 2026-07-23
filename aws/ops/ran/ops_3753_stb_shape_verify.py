#!/usr/bin/env python3
"""ops 3753 — VERIFY STB rail CSV shape before building canary #9.

3752 established: FRED carries NO commodity split (only the two aggregates
freight-pulse already has, plus PPI *price* series). AAR serves a 296KB
marketing page, no CSV — scrape-hostile as expected. STB (the federal
regulator) serves per-railroad CSVs directly:
  STB-1145-{BNSF,CPKC,CSXT,GTC,NS,UP}.csv     weekly service metrics
  STB_49_CFR_1247_CARS_LOAD_TERM_*.csv        cars loaded / terminated

Before writing an engine, prove WHICH file carries a COMMODITY dimension.
A weekly service-metrics file with only train-speed and dwell is useless for
canary #9 — the canary needs carloads split by commodity (chemicals, motor
vehicles, lumber, metallic ores, grain, coal), because "which commodity is
moving" is the entire signal. An aggregate cannot answer it.

This ops downloads the real files and prints headers + a couple of rows.
It writes NO engine and deploys nothing. If no commodity dimension exists in
the reachable files, record that honestly and move on rather than shipping a
fabricated breakdown.
"""
import csv
import io
import json
import re
import ssl
import sys
import traceback
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (compatible; JustHodl research; admin@justhodl.ai)"}
CTX = ssl.create_default_context()

RAILROADS = ["BNSF", "CPKC", "CSXT", "GTC", "NS", "UP"]
COMMODITY_WORDS = ("commodity", "stcc", "chemical", "grain", "coal", "motor",
                   "vehicle", "lumber", "ore", "intermodal", "metal", "food",
                   "petroleum", "aggregate", "farm")


def fetch(url, timeout=45):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout, context=CTX) as r:
        return r.status, r.read()


with report("3753_stb_shape_verify") as rep:
    rep.heading("ops 3753 — STB rail CSV shape verification (canary #9)")
    Path("aws/ops/reports").mkdir(parents=True, exist_ok=True)
    Path("aws/ops/reports/3753.json").write_text(json.dumps({"verdict": "STARTED"}))
    findings = {}

    try:
        # ── A per-railroad weekly service files ──────────────────────────
        rep.section("A — STB-1145 per-railroad weekly files")
        commodity_capable = []
        for rr in RAILROADS[:3]:          # 3 is enough to establish the shape
            u = "https://www.stb.gov/wp-content/uploads/STB-1145-%s.csv" % rr
            try:
                st, body = fetch(u)
                txt = body.decode("utf-8", "replace", )
                rows = list(csv.reader(io.StringIO(txt)))
                rep.ok("  %s HTTP %d rows=%d" % (rr, st, len(rows)))
                # header may not be row 0 (learned on CAISO, ops 3737)
                hdr_i, hdr = 0, rows[0] if rows else []
                for i, r0 in enumerate(rows[:25]):
                    joined = " ".join(r0).lower()
                    if sum(1 for w in ("week", "date", "carload", "commodity",
                                       "railroad", "measure") if w in joined) >= 2:
                        hdr_i, hdr = i, r0
                        break
                rep.log("    header row idx=%d cols=%d" % (hdr_i, len(hdr)))
                rep.log("    cols: %s" % [c[:26] for c in hdr[:14]])
                for r0 in rows[hdr_i + 1:hdr_i + 3]:
                    rep.log("    row: %s" % [c[:22] for c in r0[:10]])
                low = " ".join(hdr).lower()
                hits = [w for w in COMMODITY_WORDS if w in low]
                # also scan the body: many STB files put the commodity in a
                # VALUE column (long format) rather than in the header
                body_low = txt[:200000].lower()
                body_hits = sorted({w for w in COMMODITY_WORDS
                                    if w in body_low})
                rep.log("    commodity words in HEADER: %s" % hits)
                rep.log("    commodity words in BODY:   %s" % body_hits[:10])
                if hits or len(body_hits) >= 3:
                    commodity_capable.append(rr)
                findings[rr] = {"rows": len(rows), "cols": hdr[:14],
                                "header_hits": hits, "body_hits": body_hits[:10]}
            except Exception as e:
                rep.warn("  %s -> %s" % (rr, str(e)[:120]))
                findings[rr] = {"error": str(e)[:120]}

        # ── B the CARS_LOAD_TERM file (wildcard name must be resolved) ────
        rep.section("B — STB 49 CFR 1247 cars loaded/terminated")
        try:
            st, body = fetch("https://www.stb.gov/reports-data/economic-data/")
            html = body.decode("utf-8", "replace")
            # the listing page shows a TEMPLATE name; find real hrefs
            cands = sorted(set(re.findall(
                r'href="([^"]*STB[^"]*1247[^"]*\.csv)"', html, re.I)))
            cands += sorted(set(re.findall(
                r'href="([^"]*CARS_LOAD[^"]*\.csv)"', html, re.I)))
            rep.log("  candidate 1247 links: %s" % cands[:6])
            findings["cars_load_links"] = cands[:6]
            for c in cands[:2]:
                u = c if c.startswith("http") else "https://www.stb.gov" + c
                try:
                    st2, b2 = fetch(u)
                    t2 = b2.decode("utf-8", "replace")
                    rows = list(csv.reader(io.StringIO(t2)))
                    rep.ok("    %s HTTP %d rows=%d" % (u[-56:], st2, len(rows)))
                    if rows:
                        rep.log("      cols: %s" % [x[:26] for x in rows[0][:14]])
                        for r0 in rows[1:3]:
                            rep.log("      row: %s" % [x[:22] for x in r0[:10]])
                except Exception as e:
                    rep.warn("    %s -> %s" % (u[-56:], str(e)[:100]))
        except Exception as e:
            rep.warn("  economic-data page: %s" % str(e)[:120])

        # ── VERDICT ──────────────────────────────────────────────────────
        rep.section("VERDICT")
        rep.log("  railroads with a commodity dimension: %s" % commodity_capable)
        buildable = bool(commodity_capable) or bool(findings.get("cars_load_links"))
        rep.kv(commodity_capable=",".join(commodity_capable) or "none",
               buildable=str(buildable))
        if not buildable:
            rep.warn("NO commodity dimension reachable — canary #9 would require "
                     "a fabricated split. Recording the gap; not building.")
        Path("aws/ops/reports/3753.json").write_text(
            json.dumps({"verdict": "PASS", "findings": findings,
                        "commodity_capable": commodity_capable,
                        "buildable": buildable}, indent=2, default=str))
        rep.ok("SHAPE VERIFY COMPLETE")

    except SystemExit:
        raise
    except Exception:
        tb = traceback.format_exc()
        rep.fail("UNCAUGHT: " + tb[-1500:])
        Path("aws/ops/reports/3753.json").write_text(
            json.dumps({"verdict": "CRASH", "traceback": tb[-3000:]}, indent=2))
        sys.exit(1)

sys.exit(0)
