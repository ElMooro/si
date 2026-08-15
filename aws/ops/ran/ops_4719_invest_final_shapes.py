"""
ops/4719 — last two unknowns before every leg in causal_graph.py is grounded
in confirmed real data: the exact canary-grid signal `key` values (to find
copper/lumber precisely; ops_4718 showed the list is 65 long and only
sampled 4) and portwatch.json's chile_trace shape (Khalid's own worked
example -- Chile copper -- currently points at a nonexistent file).
Read-only.
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


def load(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def main():
    with report("4719_invest_final_shapes") as rep:
        rep.heading("ops 4719 — canary-grid signal keys + portwatch chile_trace")

        rep.section("All canary-grid signal keys")
        doc = load("data/canary-grid.json")
        sigs = doc.get("signals", [])
        rows = [(s.get("key"), s.get("name"), s.get("available"), s.get("value"),
                 s.get("unit")) for s in sigs]
        rep.kv(n_signals=len(rows))
        for key, name, avail, val, unit in rows:
            rep.log(f"  {key!r:32s} avail={avail!s:5s} value={val!s:10s} unit={unit!s:8s} name={name}")

        rep.section("portwatch.json chile_trace")
        pw = load("data/portwatch.json")
        ct = pw.get("chile_trace")
        rep.log("  chile_trace =\n" + json.dumps(ct, indent=2, default=str)[:4000])

        rep.section("Done")
        rep.ok("final shapes captured")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("FINAL SHAPES PROBE ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
