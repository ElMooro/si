"""ops_4202 — the overlap diagnosis: why 7,664 TE keys flipped ~zero
NFS rows. Suffix censuses both sides, intersection, verbatim probes."""
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
    with report("4202_overlap_diag") as rep:
        rep.heading("ops 4202 — TE overlap diagnosis")
        v = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        td = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/te-feed.json")["Body"].read())
        tp = td.get("prices") or {}

        nfs_syms = []
        nfs_sfx = Counter()
        for r in v.get("symbols") or []:
            if r.get("status") != "NO_FREE_SOURCE":
                continue
            note = str(r.get("resolution_note") or "")
            if "TV/TradingEconomics" in note or \
                    "country-indicator" in note:
                sy = str(r.get("symbol"))
                nfs_syms.append(sy)
                m = re.match(r"^([A-Z]{2})([A-Z0-9]{1,14})$", sy)
                if m:
                    nfs_sfx[m.group(2)] += 1
        te_sfx = Counter()
        for k in tp:
            m = re.match(r"^([A-Z]{2})([A-Z0-9]{1,14})$", k)
            if m:
                te_sfx[m.group(2)] += 1
        inter = sum(1 for s in nfs_syms if s in tp)
        rep.kv(nfs_pool=len(nfs_syms), te_keys=len(tp),
               direct_hits=inter)
        rep.log("  NFS suffixes: " + json.dumps(
            dict(nfs_sfx.most_common(28)))[:600])
        rep.log("  TE suffixes:  " + json.dumps(
            dict(te_sfx.most_common(28)))[:600])
        both = [s for s in nfs_sfx if s in te_sfx][:20]
        rep.log("  suffixes on BOTH sides: " + json.dumps(both))
        rep.section("verbatim probes")
        for sy in nfs_syms[:12]:
            rep.log(f"  {sy}: te={json.dumps(tp.get(sy))[:80]}")
        for sy in ("USIPYY", "USFER", "JPINTR", "DEIRYY", "USCARREG"):
            rep.log(f"  known {sy}: in_te={sy in tp} "
                    f"artifact_status="
                    + str(next((r.get("status") for r in
                                v.get("symbols") or []
                                if r.get("symbol") == sy), "ABSENT")))
        rep.ok(f"DIAG — hits={inter}/{len(nfs_syms)}")


if __name__ == "__main__":
    main()
