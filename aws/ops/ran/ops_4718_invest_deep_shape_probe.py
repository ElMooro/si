"""
ops/4718 — deep structural probe for justhodl-invest field fixes.

ops_4716 found 15/16 legs mismatched and identified the right FILES but not
the right PATHS (it only prints top-level keys). ops_4717's smoke invoke
then hit a real crash in get_spx_er() -- 'str' object has no attribute
'get', meaning data/forward-returns.json's assets field isn't shaped the
way lambda_function.py assumed either.

This script dumps the actual nested shape of every field this engine reads,
bounded to depth 3 and truncated arrays, so the next round of fixes is
grounded in real data -- per the fleet's own "probe-then-wire, probe writes
NO code" doctrine. Read-only. No AWS mutation beyond writing its own report.
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


def shape(obj, depth=3, max_items=4):
    """Bounded structural summary: dict -> {key: shape(value)}, list -> [shape(first N)], else -> type+preview."""
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


def dump(rep, key, subpaths):
    """subpaths: list of dotted paths (or '' for the whole doc) to shape-dump."""
    try:
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception as e:
        rep.fail(f"  {key}: could not load ({type(e).__name__}: {str(e)[:150]})")
        return
    for sp in subpaths:
        node = doc
        ok = True
        if sp:
            for part in sp.split("."):
                if isinstance(node, dict) and part in node:
                    node = node[part]
                else:
                    ok = False
                    break
        if ok:
            rep.log(f"  {key} :: {sp or '(root)'} =\n" +
                     json.dumps(shape(node), indent=2, default=str)[:3000])
        else:
            rep.warn(f"  {key} :: {sp} -- path not found")


def main():
    with report("4718_invest_deep_shape_probe") as rep:
        rep.heading("ops 4718 — deep structural probe for justhodl-invest fixes")

        rep.section("forward-returns.json — the get_spx_er() crash site")
        dump(rep, "data/forward-returns.json", ["assets", "rankings"])

        rep.section("canary-grid.json — signals + sub_grids (copper, lumber, korea)")
        dump(rep, "data/canary-grid.json", ["signals", "sub_grids"])

        rep.section("portwatch.json — where does Korea live?")
        dump(rep, "data/portwatch.json", ["ports", "exporters", "chokepoints"])

        rep.section("asia-leads.json — taiwan_orders full shape (korea_exports already proven working)")
        dump(rep, "data/asia-leads.json", ["taiwan_orders", "taiwan_exports", "korea_exports"])

        rep.section("china-liquidity.json — tsf")
        dump(rep, "data/china-liquidity.json", ["tsf", "credit_impulse"])

        rep.section("port-cargo.json — global_pulse")
        dump(rep, "data/port-cargo.json", ["global_pulse"])

        rep.section("freight-pulse.json — composite")
        dump(rep, "data/freight-pulse.json", ["composite"])

        rep.section("grid-queue.json — national, queue_velocity, planned_capacity")
        dump(rep, "data/grid-queue.json", ["national", "queue_velocity", "planned_capacity"])

        rep.section("pjm-grid.json — load, forecast, ai_demand_read")
        dump(rep, "data/pjm-grid.json", ["load", "forecast", "ai_demand_read"])

        rep.section("construction-housing.json — signals, cycle_score")
        dump(rep, "data/construction-housing.json", ["signals", "cycle_score"])

        rep.section("taiwan-moea.json — export_orders, semiconductor")
        dump(rep, "data/taiwan-moea.json", ["export_orders", "semiconductor"])

        rep.section("industry-boom.json — full label list (need Homebuilding/Industrial-Machinery equivalents)")
        try:
            doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/industry-boom.json")["Body"].read())
            labels = sorted({row.get("industry") for row in doc.get("league", []) if row.get("industry")})
            rep.log(f"  {len(labels)} labels:\n" + "\n".join(labels))
        except Exception as e:
            rep.fail(f"  could not load industry-boom.json: {e}")

        rep.section("Done")
        rep.ok("deep shape probe complete — fix causal_graph.py / lambda_function.py from the Log above")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("DEEP PROBE ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
