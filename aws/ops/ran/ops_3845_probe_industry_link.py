"""
ops_3845 — PROBE: can we attach industries to ports with a REAL percentage?

Khalid wants each port to carry the industries most likely affected, how, and by
what percentage. The naive version — "Shanghai is down 50.7%, so Chinese
electronics are down 50.7%" — is false: port throughput is total tonnage/TEU
across all cargo and maps onto no single industry.

A defensible number needs country x industry trade composition. justhodl-import-
canary builds exactly that from US Census (HS6/NAICS x country x month, 18
industry labels, concentration + source_shift). The question this probe settles:

  Does the LIVE feed publish PER-COUNTRY SHARES per industry line, or only the
  single top_source? Full shares -> share-weighted exposure arithmetic is
  possible. Top source only -> the honest output is a RANKED EXPOSURE, not a
  percentage, and I will say so rather than invent one.

Writes no engine code.
"""
import json
import sys
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("3845_probe_industry_link") as rep:
        rep.heading("ops 3845 — PROBE: port -> industry percentage feasibility")

        rep.section("1. import-canary live shape")
        try:
            d = json.loads(s3.get_object(
                Bucket=BUCKET, Key="data/import-canary.json")["Body"].read())
        except Exception as e:
            rep.fail(f"  unreadable: {str(e)[:120]}"); sys.exit(1)
        rep.log(f"  generated_at={d.get('generated_at')} top-level={list(d)[:14]}")
        lines = d.get("lines") or []
        rep.ok(f"  lines={len(lines)}")
        if not lines:
            rep.fail("  no lines[] — cannot build an industry link"); sys.exit(1)

        rep.log(f"  line keys: {list(lines[0])}")
        rep.log(f"  sample: {json.dumps(lines[0], default=str)[:600]}")

        rep.section("2. Is a PER-COUNTRY SHARE published?")
        share_found, examples = False, []
        for ln in lines[:8]:
            con = ln.get("concentration") or {}
            rep.log(f"  [{ln.get('industry')}] {ln.get('label')}")
            rep.log(f"    concentration keys: {list(con)}")
            for k, v in con.items():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    rep.ok(f"    '{k}' is a LIST of {len(v)} rows -> {list(v[0])}")
                    rep.log(f"      sample: {json.dumps(v[0], default=str)[:220]}")
                    share_found = True
                    examples.append((ln.get("industry"), k, v[0]))
                elif isinstance(v, dict) and v:
                    rep.log(f"    '{k}' dict -> {list(v)[:8]}")
            ss = ln.get("source_shift")
            if isinstance(ss, (list, dict)):
                rep.log(f"    source_shift: {json.dumps(ss, default=str)[:220]}")

        rep.section("3. Industry universe available")
        inds = sorted({ln.get("industry") for ln in lines if ln.get("industry")})
        rep.log(f"  {len(inds)} industries: {inds}")

        rep.section("4. Countries named in the trade data")
        ctys = set()

        def walk(o):
            if isinstance(o, dict):
                for k, v in o.items():
                    if k.lower() in ("country", "cty", "cty_name", "source",
                                     "top_source", "name") and isinstance(v, str):
                        ctys.add(v)
                    walk(v)
            elif isinstance(o, list):
                for v in o[:40]:
                    walk(v)
        walk(lines)
        rep.log(f"  {len(ctys)} distinct country strings: {sorted(ctys)[:26]}")

        rep.section("5. Verdict — what percentage is honestly computable?")
        if share_found:
            rep.ok("  PER-COUNTRY SHARES AVAILABLE -> share-weighted exposure is")
            rep.ok("  computable: port_yoy_pct x country_share_of_industry_imports,")
            rep.ok("  labelled as EXPOSURE ARITHMETIC, not a forecast.")
        else:
            rep.warn("  Only a single top_source per line is published. Honest")
            rep.warn("  output is then a RANKED EXPOSURE (which industries this")
            rep.warn("  country dominates) with the DIRECTION of impact — and NO")
            rep.warn("  fabricated percentage. Widening the country split would")
            rep.warn("  require extending import-canary to publish full shares.")

        rep.kv(lines=len(lines), industries=len(inds),
               countries=len(ctys), per_country_shares=share_found)
        rep.ok("PROBE COMPLETE — no code written")


if __name__ == "__main__":
    main()
