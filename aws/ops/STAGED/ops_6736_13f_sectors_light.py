"""ops 6736 - first light of the 13F sector layer (book 1.2.0; Khalid 2026-11-04 "which specific sectors - semis, not just
tech - is smart money pouring money into and pulling out of"). Re-runnable.

After deploy-lambdas ships commit cea67dda: invoke justhodl-13f-book once (a root run under the trigger cap), then
print from the live book: version, run time, profiles source stats, the security master by source (the FMP profile
CUSIP bridge), the sector coverage, the money-in / money-out industries, the full sector table, the 40 industries with
the largest |net| flow at reported prices, and the accumulating / distributing names inside Semiconductors, Banks -
Regional, Software - Application, Biotechnology, Oil & Gas E&P, Gold, Aerospace & Defense and Utilities - for both
scopes. Embeds the sectors block as JSON. Writes nothing but this report.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
BOOK, OUT = "justhodl-13f-book", "data/13f-book.json"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=330)
WATCH = ["Semiconductors", "Banks - Regional", "Software - Application", "Software - Infrastructure", "Biotechnology", "Oil & Gas Exploration & Production",
         "Oil & Gas Midstream", "Gold", "Aerospace & Defense", "Regulated Electric", "Medical - Devices", "Semiconductor Equipment & Materials", "Internet Content & Information"]


def money(v):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "-"
    a, s = abs(v), "-" if v < 0 else "+" if v > 0 else ""
    if a >= 1e9:
        return "%s$%.2fB" % (s, a / 1e9)
    if a >= 1e6:
        return "%s$%.0fM" % (s, a / 1e6)
    if a >= 1e3:
        return "%s$%.0fK" % (s, a / 1e3)
    return "%s$%.0f" % (s, a)


def row_line(r):
    p = r.get("pairs") or {}
    return "%-42s %-22s value %10s (%5.2f%%) prior %10s dpp %+5.2f vs %6s | mgrs %2s names %4s<-%-4s | new %4s abs %4s more %4s fewer %4s tilt %6s %-13s | in %9s out %9s net %10s (%6s of prior) %-13s" % (
        r["name"][:42], (r.get("sector") or "")[:22], money(r.get("value_usd")).lstrip("+"), r.get("share_pct") or 0.0, money(r.get("value_prior_usd")).lstrip("+"), r.get("share_delta_pp") or 0.0,
        "%+.1f%%" % r["value_change_pct"] if r.get("value_change_pct") is not None else "-", r.get("n_managers"), r.get("n_identities"), r.get("n_identities_prior"),
        p.get("new"), p.get("absent"), p.get("more"), p.get("fewer"), "%+.1f%%" % r["tilt_pct"] if r.get("tilt_pct") is not None else "-", r.get("stance"),
        money(r.get("inflow_usd")).lstrip("+"), money(r.get("outflow_usd")).lstrip("+"), money(r.get("net_usd")), "%+.1f%%" % r["net_pct_of_prior"] if r.get("net_pct_of_prior") is not None else "-", r.get("money_stance"))


def names(rows):
    return ", ".join("%s %s (%d mgrs, +%d/-%d%s)" % (x.get("ticker") or (x.get("issuer") or "")[:14], money(x.get("net_usd")), x.get("n_holders") or 0, (x.get("votes") or {}).get("bull", 0), (x.get("votes") or {}).get("bear", 0),
                                                   " NCH" if x.get("no_current_holder") else "") for x in rows or [])


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    with report("ops_6736_13f_sectors_light") as r:
        r.heading("ops 6736 - 13F sector layer first light %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        r.section("1. deployed code + one run")
        cf = lam.get_function_configuration(FunctionName=BOOK)
        r.log("%s: modified %s, memory %s, timeout %s, state %s" % (BOOK, (cf.get("LastModified") or "")[:19], cf.get("MemorySize"), cf.get("Timeout"), cf.get("State")))
        t0 = time.time()
        resp = lam.invoke(FunctionName=BOOK, InvocationType="RequestResponse", Payload=json.dumps({"trigger": "ops_6736_sectors_light"}).encode())
        payload = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError"):
            r.fail("FunctionError %s: %s" % (resp.get("FunctionError"), json.dumps(payload)[:600]))
            raise SystemExit(1)
        book = json.loads(s3.get_object(Bucket=LIVE, Key=OUT)["Body"].read())
        b = book.get("book") or {}
        r.log("%.0fs wall | handler %s | version %s contract %s run %ss bytes %d" % (time.time() - t0, json.dumps(payload), book.get("version"), book.get("contract"), book.get("run_seconds"), len(json.dumps(book))))
        if book.get("version", "0") < "1.2.0":
            r.fail("the live book is %s - deploy-lambdas has not shipped 1.2.0 yet" % book.get("version"))
            raise SystemExit(1)
        r.log("profiles: %s" % json.dumps(b.get("profiles")))
        cov = (b.get("security_master") or {}).get("coverage") or {}
        rf = (b.get("security_master") or {}).get("refresh") or {}
        r.log("master: %s/%s resolved (%s%% of value) by source %s | typed-only %s | no_match %s ambiguous %s | this run: openfigi requests %s, fmp cusip bridge %s" % (
            cov.get("resolved"), cov.get("identities"), cov.get("resolved_value_pct"), json.dumps(cov.get("resolved_by_source")), cov.get("typed_only"), cov.get("no_match"), cov.get("ambiguous"), rf.get("requests"), rf.get("fmp_cusip_resolved")))
        r.log("unmapped held now: %s; largest: %s" % (((b.get("review") or {}).get("n_unmapped")), ", ".join("%s %s" % (u.get("issuer"), u.get("cusip")) for u in ((b.get("review") or {}).get("unmapped_top") or [])[:12])))
        for scope in ("cohort", "concentrated"):
            sc = (b.get("sectors") or {}).get(scope) or {}
            r.section("2. %s - coverage %s | %d industries" % (scope, json.dumps(sc.get("coverage")), sc.get("n_industries") or 0))
            r.log("MONEY IN (industries by net $ at reported prices):")
            for x in sc.get("top_inflow") or []:
                r.log("  %-40s %-22s net %10s (%6s of prior) in %9s out %9s tilt %6s %-13s mgrs %s value %s" % (x["name"][:40], (x.get("sector") or "")[:22], money(x.get("net_usd")), "%+.1f%%" % x["net_pct_of_prior"] if x.get("net_pct_of_prior") is not None else "-",
                                                                                                              money(x.get("inflow_usd")).lstrip("+"), money(x.get("outflow_usd")).lstrip("+"), "%+.1f%%" % x["tilt_pct"] if x.get("tilt_pct") is not None else "-", x.get("stance"), x.get("n_managers"), money(x.get("value_usd")).lstrip("+")))
            r.log("MONEY OUT:")
            for x in sc.get("top_outflow") or []:
                r.log("  %-40s %-22s net %10s (%6s of prior) in %9s out %9s tilt %6s %-13s mgrs %s value %s" % (x["name"][:40], (x.get("sector") or "")[:22], money(x.get("net_usd")), "%+.1f%%" % x["net_pct_of_prior"] if x.get("net_pct_of_prior") is not None else "-",
                                                                                                              money(x.get("inflow_usd")).lstrip("+"), money(x.get("outflow_usd")).lstrip("+"), "%+.1f%%" % x["tilt_pct"] if x.get("tilt_pct") is not None else "-", x.get("stance"), x.get("n_managers"), money(x.get("value_usd")).lstrip("+")))
            r.log("SECTORS:")
            for x in sc.get("sectors") or []:
                r.log("  " + row_line(x))
                r.log("      accumulating: %s" % (names(x.get("accumulating")) or "-"))
                r.log("      distributing: %s" % (names(x.get("distributing")) or "-"))
            inds = sorted(sc.get("industries") or [], key=lambda x: -abs(x.get("net_usd") or 0))
            r.log("INDUSTRIES by |net| (top 40 of %d kept):" % len(inds))
            for x in inds[:40]:
                r.log("  " + row_line(x))
            byname = {x["name"]: x for x in sc.get("industries") or []}
            r.log("WATCHED INDUSTRIES:")
            for w in WATCH:
                x = byname.get(w)
                if not x:
                    r.log("  %-40s not among the %d industries kept" % (w, len(byname)))
                    continue
                r.log("  " + row_line(x))
                r.log("      accumulating: %s" % (names(x.get("accumulating")) or "-"))
                r.log("      distributing: %s" % (names(x.get("distributing")) or "-"))
        r.section("3. sectors block (JSON, for offline reading)")
        r.log("```json\n%s\n```" % json.dumps({"profiles": b.get("profiles"), "sectors": b.get("sectors"), "generated_at": book.get("generated_at"), "version": book.get("version")}, separators=(",", ":"), default=str))
        sc = (b.get("sectors") or {}).get("cohort") or {}
        r.kv(version=book.get("version"), sector_value_pct=str((sc.get("coverage") or {}).get("with_sector_value_pct")), industries=str(sc.get("n_industries")),
             top_in=(sc.get("top_inflow") or [{}])[0].get("name") or "-", top_out=(sc.get("top_outflow") or [{}])[0].get("name") or "-", run_seconds=str(book.get("run_seconds")))
        r.ok("SECTOR LAYER LIVE: %s industries, sector known for %s%% of held stock value; money in %s, money out %s" % (
            sc.get("n_industries"), (sc.get("coverage") or {}).get("with_sector_value_pct"), (sc.get("top_inflow") or [{}])[0].get("name"), (sc.get("top_outflow") or [{}])[0].get("name")))


if __name__ == "__main__":
    main()
    sys.exit(0)
