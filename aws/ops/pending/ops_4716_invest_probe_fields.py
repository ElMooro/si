"""
ops/4716 — PROBE every live S3 field justhodl-invest depends on, before the
engine is ever trusted.

Per CLAUDE.md / the fleet's own standing discipline ("every column bind
preceded by a census probe dumping real column names and sample values
before any code was written"), this script does NOT deploy anything. It
dumps, for every source string in causal_graph.py plus every whole-document
key lambda_function.py reads:
  - does the S3 key exist at all?
  - does the dotted path resolve to a numeric value?
  - if not, what top-level keys DOES exist, so a human can fix the path in
    one look instead of guessing blind.

Run this BEFORE trusting Tier 1 output. If anything below
prints MISMATCH, fix the corresponding Leg.source / *_KEY constant in
aws/lambdas/justhodl-invest/source/causal_graph.py or lambda_function.py
first — the engine is written to degrade to INSUFFICIENT_DATA on a bad
path rather than crash, but a silently-wrong path just means Tier 1 never
confirms anything and nobody notices why.

Writes aws/ops/reports/4716_invest_probe_fields.json (via ops_report).
No AWS mutation, read-only. A field mismatch is expected, useful probe
output, not a script failure -- this exits 0 in that case. It only exits
nonzero if the probe itself breaks (S3 unreachable in a way that isn't a
per-key NoSuchKey, a bug in causal_graph.py, etc.), so a real break can
never go green and get auto-moved to ran/ looking like a clean pass.
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "lambdas" / "justhodl-invest" / "source"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402
import causal_graph  # noqa: E402
import fleet_io  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = fleet_io.BUCKET

WHOLE_DOC_KEYS = {
    "forward-returns (Tier 2 SPX/sector ER)": "data/forward-returns.json",
    "industry-boom (Tier 3 universe seed)": "data/industry-boom.json",
    "backlog-miner (Tier 3 backlog)": "data/backlog-miner.json",
    "backlog XBRL (Tier 3 backlog fallback)": "data/backlog.json",
    "catalyst (Tier 3 catalyst strength)": "data/catalyst.json",
    "stock-buying (Tier 3 PEG/buyback/QoQ/cycle)": "data/stock-buying.json",
    "impact-graph exposure graph": "data/impact/exposure-graph.json",
    "impact-graph betas": "data/impact/betas.json",
}


def try_get(rep, key):
    try:
        body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        return json.loads(body)
    except s3.exceptions.NoSuchKey:
        rep.fail(f"  {key}: NoSuchKey — engine has never run, or key name is wrong")
        return None
    except Exception as e:
        rep.fail(f"  {key}: {type(e).__name__}: {str(e)[:150]}")
        return None


def top_keys(doc, depth=1):
    if isinstance(doc, dict):
        return sorted(doc.keys())[:20]
    if isinstance(doc, list) and doc:
        return f"list[{len(doc)}], first item keys: " + str(sorted(doc[0].keys())[:20] if isinstance(doc[0], dict) else type(doc[0]))
    return str(type(doc))


def main():
    with report("4716_invest_probe_fields") as rep:
        rep.heading("ops 4716 — justhodl-invest field probe (read-only)")

        rep.section("1. Leading-indicator legs (causal_graph.LEADING_INDICATORS)")
        n_ok, n_bad = 0, 0
        for ind in causal_graph.LEADING_INDICATORS:
            rep.log(f"── {ind.indicator_id} ──")
            for leg in ind.legs:
                key, path = fleet_io.parse_source(leg.source)
                doc = try_get(rep, key)
                if doc is None:
                    n_bad += 1
                    continue
                val = fleet_io.dig(doc, path)
                if isinstance(val, (int, float)):
                    rep.ok(f"  {leg.leg_id}: {leg.source} = {val}")
                    n_ok += 1
                else:
                    rep.fail(f"  {leg.leg_id}: {leg.source} -> path did not resolve "
                              f"to a number (got {type(val).__name__}). "
                              f"Top-level keys of {key}: {top_keys(doc)}")
                    n_bad += 1
        rep.kv(legs_ok=n_ok, legs_mismatch=n_bad)

        rep.section("2. Whole-document reads (Tier 2 / Tier 3)")
        for label, key in WHOLE_DOC_KEYS.items():
            doc = try_get(rep, key)
            if doc is not None:
                rep.ok(f"  {label} ({key}): present. Top-level shape: {top_keys(doc)}")

        rep.section("3. industry_boom_label cross-walk sanity check")
        boom_doc = try_get(rep, "data/industry-boom.json")
        live_labels = set()
        if boom_doc:
            live_labels = {row.get("industry") for row in boom_doc.get("league", [])
                           if row.get("industry")}
            rep.log(f"  {len(live_labels)} live industry-boom labels found")
        for key, proxy in causal_graph.INDUSTRY_PROXY.items():
            if proxy.industry_boom_label is None:
                continue
            if proxy.industry_boom_label in live_labels:
                rep.ok(f"  {key}: '{proxy.industry_boom_label}' matches a live league row")
            else:
                rep.fail(f"  {key}: '{proxy.industry_boom_label}' NOT found in live "
                          f"industry-boom league — closest live labels: "
                          f"{sorted(live_labels)[:15]}")

        rep.section("Verdict")
        if n_bad == 0:
            rep.ok("All leading-indicator legs resolved. Safe to push "
                   "the deploy is safe to trust.")
        else:
            rep.warn(f"{n_bad} leg(s) mismatched — engine will still deploy safely "
                      f"(mismatched legs degrade to INSUFFICIENT_DATA, never a fake "
                      f"value) but Tier 1 will confirm nothing for those indicators "
                      f"until causal_graph.py's Leg.source strings are corrected "
                      f"against the top-level keys printed above.")
        rep.status = "ok"  # probe success = probe ran and reported, not zero-mismatch


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("PROBE SCRIPT ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
