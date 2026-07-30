"""ops_4108 — midpoint check: walk progress + econ_probe UNSLICED (the
agency verdict I owe Khalid) + coverage."""
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


def main():
    with report("4108_midpoint") as rep:
        rep.heading("ops 4108 — midpoint: progress + econ_probe verdict")
        sr = json.loads(s3.get_object(Bucket=BUCKET,
                                      Key="data/tv-sources.json")["Body"].read())
        m = sr.get("sources") or {}
        d = sr.get("last_harvest_diag") or {}

        rep.section("A. walk progress")
        rep.kv(done=d.get("done"), total=d.get("total"),
               pct=round((d.get("done") or 0) * 100 /
                         max(1, d.get("total") or 1), 1),
               rate=d.get("rate_per_min"), delay_ms=d.get("delay_ms"),
               sc_ok=d.get("sc_ok"), sc_err=d.get("sc_err"),
               wall_events=d.get("wall_events"),
               recoveries=d.get("recoveries"),
               paused_s=d.get("paused_s"), elapsed_s=d.get("elapsed_s"))

        rep.section("B. econ_probe — UNSLICED, the agency verdict")
        ep = d.get("econ_probe")
        txt = json.dumps(ep, indent=None)
        if not ep:
            rep.log("  econ_probe EMPTY — no ECONOMICS symbol sampled yet")
        else:
            for i in range(0, len(txt), 180):
                rep.log("  " + txt[i:i + 180])

        rep.section("C. coverage")
        norm, agency, econ = Counter(), Counter(), 0
        for k, v in m.items():
            n1 = PFX.sub("", str(v.get("source") or "").strip())
            if not n1:
                continue
            norm[n1] += 1
            if str(k).startswith("ECONOMICS"):
                econ += 1
                agency[n1] += 1
        rep.kv(attributed=len(m), pct_universe=round(len(m) * 100 / 10319, 1),
               distinct=len(norm), economics_rows=econ)
        for s2, n in agency.most_common(15):
            rep.log(f"  AGENCY {n:4d}  {s2[:60]}")
        econ_keys = [k for k in m if str(k).startswith("ECONOMICS")][:10]
        for k in econ_keys:
            rep.log(f"  ECON {k}: {json.dumps(m[k])[:120]}")
        rep.ok(f"MIDPOINT — {d.get('done')}/{d.get('total')} walked, "
               f"{len(m)} attributed, {econ} economics, "
               f"econ_probe {'PRESENT' if ep else 'EMPTY'}")


if __name__ == "__main__":
    main()
