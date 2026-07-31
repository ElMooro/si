"""ops_4151 — the COT wire decision: full weekly-row columns, the 29
cftc_codes, overlap vs his distinct market codes, group-column presence."""
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


def main():
    with report("4151_cot_decision") as rep:
        rep.heading("ops 4151 — COT wire decision")
        c = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/cftc-all-cache.json")["Body"].read())
        inner = next(v for v in c.values()
                     if isinstance(v, dict) and len(v) > 20)
        codes = {}
        for tk, r in inner.items():
            if isinstance(r, dict) and r.get("cftc_code"):
                codes[str(r["cftc_code"])] = tk
        rep.kv(store_contracts=len(inner), with_cftc_code=len(codes))
        rep.log("  cftc_codes: " + json.dumps(
            sorted(codes.items())[:29])[:420])
        r0 = next(iter(inner.values()))
        wr = (r0.get("weekly_reports") or [{}])[0]
        rep.log("  weekly row columns: " + json.dumps(list(wr))[:420])
        tff = [k for k in wr if re.search(
            r"dealer|asset|lev|manager|other_rept|spread", str(k), re.I)]
        rep.kv(tff_group_columns=len(tff))
        rep.log("  tff-ish: " + json.dumps(tff)[:300])

        wl = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tv-watchlists.json")["Body"].read())
        his = Counter()
        fields = Counter()
        for l in (wl.get("lists") or []):
            for sy in l.get("symbols") or []:
                sy = str(sy)
                if sy.split(":")[0] in ("COT", "COT3"):
                    bare = sy.split(":", 1)[1]
                    m = re.match(r"(\d{6})_([A-Z]+)_(.+)", bare)
                    if m:
                        his[m.group(1)] += 1
                        fields[m.group(3)] += 1
        rep.kv(his_markets=len(his))
        rep.log("  his codes: " + json.dumps(
            sorted(his.items())[:24])[:360])
        hit = [c2 for c2 in his if c2 in codes]
        rep.kv(code_overlap=len(hit),
               symbols_servable_if_columns=sum(his[c2] for c2 in hit))
        rep.log("  overlap: " + json.dumps(
            [(c2, codes[c2]) for c2 in hit])[:300])
        rep.log("  his field suffixes: " + json.dumps(
            dict(fields.most_common(16)))[:360])
        rep.ok(f"DECISION DATA — overlap {len(hit)} markets, "
               f"tff cols {len(tff)}")


if __name__ == "__main__":
    main()
