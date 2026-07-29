"""ops_4107 — attribution coverage: how much of TV's sourcing do we know?"""
import json
import re
import sys
from collections import Counter
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
PFX = re.compile(r"^(?:source|provider|country)/")


def gj(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def main():
    with report("4107_attr_coverage") as rep:
        rep.heading("ops 4107 — attribution coverage right now")
        sr = gj("data/tv-sources.json") or {}
        m = sr.get("sources") or {}
        d = sr.get("last_harvest_diag") or {}
        rep.log("  DIAG: " + json.dumps(d)[:320])
        norm = Counter()
        econ = 0
        agency = Counter()
        for k, v in m.items():
            n1 = PFX.sub("", str(v.get("source") or "").strip())
            if not n1:
                continue
            norm[n1] += 1
            if str(k).startswith("ECONOMICS"):
                econ += 1
                agency[n1] += 1
        rep.kv(attributed=len(m), of_universe=10319,
               pct=round(len(m) * 100 / 10319, 1),
               distinct=len(norm), economics_attributed=econ)
        rep.section("distinct sources seen (normalized)")
        for s2, n in norm.most_common(25):
            rep.log(f"  {n:5d}  {s2[:60]}")
        rep.section("ECONOMICS agencies seen")
        for s2, n in agency.most_common(15):
            rep.log(f"  {n:4d}  {s2[:60]}")
        sm = gj("data/source-map.json") or {}
        rep.kv(map_new=len(sm.get("new_sources") or []),
               map_marker=sm.get("marker"))
        rep.ok(f"COVERAGE — {len(m)}/10319 attributed "
               f"({round(len(m)*100/10319,1)}%), {len(norm)} distinct, "
               f"{econ} economics rows")


if __name__ == "__main__":
    main()
