"""
ops/4727 — probe the real S3 output shape of every institutional-edge
engine selected to extend justhodl-invest, BEFORE writing any
causal_graph.py entries against them. Lesson from the first build (ops
4716-4726): guessing field paths costs hours; probing costs minutes.

Self-correcting: tries the conventional data/<name>.json key for each
engine; if that 404s, lists the data/ prefix for anything starting with
the engine's short name (catches prefix-style outputs like
etf-flows/*.json) so this resolves in one pass.
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

# (short label, guessed key)
CANDIDATES = [
    ("insider_industry_cluster", "data/insider-industry-cluster.json"),
    ("credit_before_equity", "data/credit-before-equity.json"),
    ("sector_flow_state", "data/sector-flow-state.json"),
    ("cftc_deep_view", "data/cftc-deep-view.json"),
    ("cot_extremes", "data/cot-extremes.json"),
    ("etf_fund_flows", "etf-flows/composite.json"),
    ("stealth_accumulation", "data/stealth-accumulation.json"),
    ("dealer_gex", "data/dealer-gex.json"),
    ("finra_short", "data/finra-short.json"),
    ("squeeze_fuel", "data/squeeze-fuel.json"),
    ("congress_direct", "data/congress-direct.json"),
    ("hiring_velocity", "data/hiring-velocity.json"),
    ("estimate_revisions", "data/estimate-revisions.json"),
    ("smart_money_13f", "data/smart-money-13f.json"),
]


def shape(obj, depth=3, max_items=5):
    if depth <= 0:
        return f"<{type(obj).__name__}>"
    if isinstance(obj, dict):
        return {k: shape(v, depth - 1, max_items) for k, v in list(obj.items())[:20]}
    if isinstance(obj, list):
        return [shape(v, depth - 1, max_items) for v in obj[:max_items]] + (
            [f"...+{len(obj) - max_items} more"] if len(obj) > max_items else [])
    if isinstance(obj, str):
        return f"str: {obj[:60]!r}"
    if isinstance(obj, (int, float, bool)) or obj is None:
        return obj
    return f"<{type(obj).__name__}>"


def find_alternates(rep, short_name, guessed_key):
    """List the data/ (and bare) prefix for anything matching the engine's
    short name, when the guessed key doesn't exist."""
    stem = short_name.replace("_", "-")
    prefixes_to_try = [f"data/{stem}", f"{stem}/", f"data/{stem.split('-')[0]}"]
    found = set()
    for pfx in prefixes_to_try:
        try:
            resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=pfx, MaxKeys=15)
            for obj in resp.get("Contents", []):
                found.add(obj["Key"])
        except Exception:
            pass
    return sorted(found)


def main():
    with report("4727_invest_institutional_edge_probe") as rep:
        rep.heading("ops 4727 — probe institutional-edge engine outputs")

        for short_name, guessed_key in CANDIDATES:
            rep.log(f"── {short_name} (guessed: {guessed_key}) ──")
            try:
                doc = json.loads(s3.get_object(Bucket=BUCKET, Key=guessed_key)["Body"].read())
                rep.ok(f"  {guessed_key}: FOUND. Shape:\n" +
                       json.dumps(shape(doc), indent=2, default=str)[:3500])
                continue
            except s3.exceptions.NoSuchKey:
                rep.warn(f"  {guessed_key}: NoSuchKey -- searching for alternates")
            except Exception as e:
                rep.fail(f"  {guessed_key}: {type(e).__name__}: {str(e)[:150]}")
                continue

            alts = find_alternates(rep, short_name, guessed_key)
            if not alts:
                rep.fail(f"  no alternate keys found under data/{short_name.replace('_','-')}* "
                         f"or {short_name.replace('_','-')}/* -- engine may not have run yet, "
                         f"or writes somewhere unconventional")
                continue
            rep.log(f"  candidates found: {alts}")
            # peek the first alternate
            try:
                doc = json.loads(s3.get_object(Bucket=BUCKET, Key=alts[0])["Body"].read())
                rep.ok(f"  {alts[0]}: shape:\n" +
                       json.dumps(shape(doc), indent=2, default=str)[:3500])
            except Exception as e:
                rep.fail(f"  {alts[0]}: could not load ({e})")

        rep.section("Done")
        rep.ok("probe complete -- wire causal_graph.py from the real shapes above, not guesses")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("INSTITUTIONAL EDGE PROBE ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
