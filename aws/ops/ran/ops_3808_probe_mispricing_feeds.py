#!/usr/bin/env python3
"""ops 3808 — probe candidate feeds for the mispricing thesis (audit before build).

THE QUESTION: capture-gap now identifies companies that are STRUCTURALLY
IMPORTANT (structural_importance, 2393/2393) and CHEAP RELATIVE TO PEERS
(capture_gap, catchup_pct). That is a value screen with a quality filter. What
it does NOT yet answer is the question a hedge fund actually underwrites:

  "This company is critical AND cheap. WHY is it cheap, WHO is on the other
   side, and WHAT will force the repricing?"

A value gap with no catalyst and no flow is a value trap. The three legs a
fundamental L/S desk adds on top of cheapness are:

  1. WHY CHEAP — is the market wrong, or does it know something? Earnings
     revisions falling while the stock is 'cheap' means the E in P/E is stale.
     (eps-revision-velocity, estimate-revisions)
  2. WHO IS POSITIONED — smart money accumulating into weakness is confirmation;
     insiders selling into it is a warning. Short interest tells you if the
     cheapness is a crowded bear thesis. (13f-price-divergence, insider-aggregate,
     finra-short, dark-pool)
  3. WHAT FORCES REPRICING — a catalyst with a clock. Post-earnings drift,
     guidance events, backlog conversion. (earnings-pead)

This ops does NOT build. It opens each candidate feed, reports the REAL schema,
per-ticker key, freshness and — critically — OVERLAP with the 2,393-name capture
ledger. Coverage decides which are worth joining: a signal covering 40 names
cannot inform a 2,393-name board, and I have already learned in this arc that
asserting coverage without opening the file wastes hours.
"""
import sys, json, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report
import boto3

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
FAILED = []


def gate(rep, n, ok, d=""):
    (rep.ok if ok else rep.fail)(f"{n} :: {d}")
    if not ok:
        FAILED.append(n)


CANDIDATES = [
    ("data/eps-revision-velocity.json", "WHY CHEAP — are estimates falling?"),
    ("data/estimate-revisions.json", "WHY CHEAP — analyst revision breadth"),
    ("data/earnings-pead.json", "CATALYST — post-earnings drift"),
    ("data/finra-short.json", "POSITIONING — short interest / crowded bear"),
    ("data/13f-price-divergence.json", "POSITIONING — smart money vs price"),
    ("data/insider-aggregate.json", "POSITIONING — insider buy/sell"),
    ("data/dark-pool.json", "POSITIONING — off-exchange accumulation"),
    ("data/insider-buyback-confluence.json", "POSITIONING — buyback + insider"),
    ("data/short-book.json", "POSITIONING — short book"),
    ("data/deal-scanner.json", "CATALYST — event classes"),
    ("data/readthrough.json", "CATALYST — beneficiary propagation"),
    ("data/industry-boom.json", "REGIME — which industries are booming"),
]


def harvest_tickers(o, depth=0, out=None):
    if out is None:
        out = set()
    if depth > 5:
        return out
    if isinstance(o, dict):
        for k, v in o.items():
            if k in ("ticker", "symbol") and isinstance(v, str):
                t = v.strip().upper()
                if 1 <= len(t) <= 6 and t.replace(".", "").replace("-", "").isalnum():
                    out.add(t)
            harvest_tickers(v, depth + 1, out)
        # dict keyed BY ticker
        ks = list(o.keys())[:50]
        up = [k for k in ks if isinstance(k, str) and 1 <= len(k) <= 6 and k.isupper()]
        if len(up) > len(ks) * 0.6 and len(ks) > 5:
            for k in o.keys():
                if isinstance(k, str) and 1 <= len(k) <= 6:
                    out.add(k.upper())
    elif isinstance(o, list):
        for x in o[:5000]:
            harvest_tickers(x, depth + 1, out)
    return out


def main():
    with report("3808_probe_mispricing_feeds") as rep:
        rep.heading("ops 3808 — which institutional signals can actually join?")

        ck = json.loads(s3.get_object(Bucket=BUCKET, Key="data/chokepoint.json")["Body"].read())
        cap = ck.get("capture_gap") or {}
        rows = cap.get("all_rows") or []
        led = {r.get("ticker") for r in rows if r.get("ticker")}
        rep.kv(capture_ledger=len(led), engine_version=ck.get("version"))

        rep.section("Candidate feeds — existence, freshness, schema, OVERLAP")
        results = []
        for key, why in CANDIDATES:
            try:
                h = s3.head_object(Bucket=BUCKET, Key=key)
                age = (time.time() - h["LastModified"].timestamp()) / 3600.0
                j = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
                syms = harvest_tickers(j)
                ov = syms & led
                topk = [k for k in list(j.keys())[:9]] if isinstance(j, dict) else "list"
                results.append((key, why, len(syms), len(ov), age, topk, h["ContentLength"]))
                rep.log("  %-42s %5dh  syms=%-6d overlap=%-6d  %s" % (
                    key, int(age), len(syms), len(ov), topk))
            except Exception as e:
                results.append((key, why, 0, 0, None, str(e)[:40], 0))
                rep.log("  %-42s  ABSENT/ERR  %s" % (key, str(e)[:60]))

        rep.section("Ranked by join value (overlap with the capture ledger)")
        usable = sorted([r for r in results if r[3] > 0], key=lambda x: -x[3])
        for key, why, ns, ov, age, topk, sz in usable:
            pct = 100.0 * ov / max(len(led), 1)
            rep.log("  %-42s overlap %-6d (%.1f%% of ledger)  %s" % (key, ov, pct, why))
        gate(rep, "PROBE.usable", len(usable) > 0, "%d feeds join non-trivially" % len(usable))

        rep.section("Feeds too thin to inform a 2,393-name board (<10%)")
        for key, why, ns, ov, age, topk, sz in results:
            if 0 < ov < len(led) * 0.10:
                rep.log("  %-42s overlap %d — informative for a subset only" % (key, ov))

        rep.section("Schema detail for the top joiners")
        for key, why, ns, ov, age, topk, sz in usable[:5]:
            try:
                j = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
                rep.log("  --- %s ---" % key)
                if isinstance(j, dict):
                    for k, v in list(j.items())[:12]:
                        rep.log("      %-26s %-7s %s" % (
                            k, type(v).__name__,
                            len(v) if isinstance(v, (list, dict, str)) else v))
                    for k, v in j.items():
                        if isinstance(v, dict) and v:
                            fk = list(v.keys())[0]
                            if isinstance(v[fk], dict):
                                rep.log("      %s['%s'] keys: %s" % (
                                    k, fk, sorted(v[fk].keys())[:12]))
                                break
                        if isinstance(v, list) and v and isinstance(v[0], dict):
                            rep.log("      %s[0] keys: %s" % (k, sorted(v[0].keys())[:12]))
                            break
            except Exception as e:
                rep.log("      schema read failed: %s" % str(e)[:70])

        rep.section("VERDICT")
        rep.log("Join priority is decided by OVERLAP, not by how good the idea sounds.")
        rep.log("Next ops wires only the feeds above ~15%% ledger coverage, each as an")
        rep.log("explicit leg with its own gate proving a non-zero join on live output.")
        if FAILED:
            rep.fail("FAILED: %s" % ", ".join(FAILED))
            sys.exit(1)
        rep.ok("PASS_ALL — probe complete")


if __name__ == "__main__":
    main()
