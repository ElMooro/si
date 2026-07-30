"""ops_4109 — THE ECONOMICS GAP: which ECONOMICS:* symbols Khalid watches
that the fleet does not yet serve LIVE. Computable without TV's blessing."""
import json
import sys
from collections import Counter
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def gj(key):
    return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())


def main():
    with report("4109_econ_gap") as rep:
        rep.heading("ops 4109 — the ECONOMICS gap list")
        wl = gj("data/tv-watchlists.json")
        lists = wl.get("lists") or wl.get("watchlists") or []
        v = gj("data/tradingview.json")
        idx = {}
        for row in v.get("symbols") or []:
            idx[row.get("symbol")] = row
            idx.setdefault(str(row.get("symbol")).split(":")[-1], row)
        econ = {}
        for l in lists:
            for sy in l.get("symbols") or []:
                if str(sy).startswith("ECONOMICS:"):
                    econ.setdefault(sy, set()).add(l.get("name"))
        st = Counter()
        gaps = []
        for sy in sorted(econ):
            row = idx.get(sy) or idx.get(str(sy).split(":")[-1])
            s2 = (row or {}).get("status") or "NOT-IN-VAULT"
            st[s2] += 1
            if s2 != "LIVE":
                gaps.append((sy, s2, sorted(econ[sy])[:2]))
        rep.kv(economics_watched=len(econ), **dict(st.most_common()))
        rep.section(f"GAPS — {len(gaps)} not yet LIVE")
        for sy, s2, ls in gaps[:80]:
            rep.log(f"  {s2:20s} {sy[:40]}   in: {', '.join(map(str,ls))[:50]}")
        rep.ok(f"ECON GAP — {len(econ)} watched, "
               f"{st.get('LIVE',0)} LIVE, {len(gaps)} gaps")


if __name__ == "__main__":
    main()
