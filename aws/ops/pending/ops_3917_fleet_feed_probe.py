"""
ops_3917 — PROBE (writes no code): dump the REAL live shape of every fleet
feed Khalid approved for Risk Gate v2 consumption, plus targeted field
hunts for the exact metrics each leg needs. Also verifies JPLG coverage in
boj-detail's output. The #1 recurring bug class is key-guessing; this is
the G0 contract for all ~15 wires at once.
"""
import json, sys
from datetime import datetime, timezone
from pathlib import Path
import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"

FEEDS = {
  "data/nyfed-primary-dealer.json": ["fail", "net_short", "financing", "corporate"],
  "data/ofr-stfm.json": ["fail", "dvp", "tri", "gcf", "volume"],
  "data/auction-tail.json": ["tail", "10", "grade", "bid_to_cover"],
  "data/auction-grades.json": ["tail", "10y", "grade"],
  "data/crisis-plumbing.json": ["soma", "tga", "basis", "cross", "rrp"],
  "data/credit-composite.json": ["composite", "tightening", "lens", "score"],
  "data/credit-stress.json": ["stress", "score", "spread"],
  "data/sovereign-distress.json": ["composite", "distress", "hub", "score"],
  "data/euro-fragmentation.json": ["btp", "bund", "fragmentation", "spread"],
  "data/ciss-stress.json": ["ciss", "stress", "level"],
  "data/bis-crossborder.json": ["flow", "yoy", "cross"],
  "data/yen-carry.json": ["composite", "score", "unwind", "funding"],
  "data/eurodollar-plumbing.json": ["composite", "layer", "score", "stress"],
  "data/china-liquidity.json": ["impulse", "tsf", "yoy", "credit"],
  "data/asia-leads.json": ["taiwan", "export", "yoy", "kr"],
  "data/portwatch.json": ["volume", "yoy", "global"],
  "data/freight-pulse.json": ["index", "yoy", "score"],
  "data/air-cargo.json": ["yoy", "index", "cargo"],
  "data/bond-vol.json": ["z", "regime", "posture", "move"],
  "data/fifx-vol-history.json": ["breadth", "spill", "as", "gb"],
  "data/cftc-deep-view.json": ["net_spec", "z", "leverage", "n_reports"],
  "data/etf-true-flows.json": ["net_flow", "inflow", "outflow", "risk"],
  "data/boj-detail.json": ["loan", "jplg", "growth"],
  "data/global-business-cycle.json": ["chile", "peru", "terms"],
}


def walk_keys(obj, depth=0, prefix="", out=None, cap=60):
    if out is None: out = []
    if len(out) >= cap or depth > 3: return out
    if isinstance(obj, dict):
        for k, v in list(obj.items())[:25]:
            t = type(v).__name__
            if isinstance(v, list): t = f"list[{len(v)}]"
            out.append(f"{prefix}{k}:{t}")
            if isinstance(v, (dict,)) or (isinstance(v, list) and v and isinstance(v[0], dict)):
                walk_keys(v[0] if isinstance(v, list) else v, depth+1, prefix+k+".", out, cap)
    return out


def hunt(obj, terms, path="", hits=None, cap=8):
    if hits is None: hits = []
    if len(hits) >= cap: return hits
    if isinstance(obj, dict):
        for k, v in obj.items():
            kl = str(k).lower()
            if any(t in kl for t in terms) and not isinstance(v, (dict, list)):
                hits.append(f"{path}.{k}={str(v)[:60]}")
            hunt(v, terms, f"{path}.{k}", hits, cap)
    elif isinstance(obj, list):
        for v in obj[:8]:
            hunt(v, terms, path+"[]", hits, cap)
    return hits


def main():
    with report("3917_fleet_feed_probe") as rep:
        rep.heading("ops 3917 — real shapes of all Risk Gate v2 donor feeds")
        n_ok = 0
        for key, terms in FEEDS.items():
            try:
                o = s3.get_object(Bucket=BUCKET, Key=key)
                doc = json.loads(o["Body"].read())
                age_h = round((datetime.now(timezone.utc) - o["LastModified"]).total_seconds()/3600, 1)
                rep.section(f"{key} ({age_h}h old)")
                rep.log("  KEYS: " + " | ".join(walk_keys(doc)[:35]))
                rep.log("  HUNT: " + " | ".join(hunt(doc, terms)))
                n_ok += 1
            except Exception as e:
                rep.section(key)
                rep.fail(f"  UNREADABLE: {str(e)[:120]}")
        rep.kv(feeds_readable=f"{n_ok}/{len(FEEDS)}")
        if n_ok < 15:
            rep.fail("too many donor feeds unreadable"); sys.exit(1)
        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
