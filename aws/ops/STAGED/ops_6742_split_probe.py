"""ops 6742 - READ-ONLY: why the strength engine's inferred-split guard did not catch ETHA (still +156% 1M in the 1.0.4
first light). Prints, for a handful of names with known or suspected splits in the 520-session window, every session
whose close ratio leaves [0.6, 1.7] with the five sessions of volume on each side (from the Polygon EOD bank, exactly
the rows the engine reads), the corporate-actions index entry and artifact events for each, and the index's coverage of
the strength universe (how many of its names have a split record, how old the records are). Nothing is written.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from ops_report import report  # noqa: E402
import polygon_bankfirst as pb  # noqa: E402

LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 4, "mode": "standard"}, read_timeout=60)
NAMES = ["ETHA", "IBIT", "ORLY", "FAST", "IBKR", "TSLA", "NVDA", "SOXL", "TQQQ", "NFLX", "WMT", "SMCI"]


def bars(s3, t):
    raw = s3.get_object(Bucket=LIVE, Key=pb.ROOT + t + ".json.gz")["Body"].read()
    doc = json.loads(gzip.decompress(raw))
    out = []
    for r in doc.get("rows") or []:
        if isinstance(r, list) and len(r) >= 6 and r[0] is not None and r[4] is not None:
            out.append({"date": pb._row_date(r[0]).isoformat(), "o": r[1], "h": r[2], "l": r[3], "c": r[4], "v": r[5]})
    return out[-520:]


def main():
    s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
    with report("ops_6742_split_probe") as r:
        r.heading("ops 6742 - split guard probe (read-only) %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        idx = json.loads(s3.get_object(Bucket=LIVE, Key="data/corporate-actions-index.json")["Body"].read())
        tk = idx.get("tickers") or {}
        r.log("corporate-actions index: generated %s | universe %s | covered %s | with splits %d" % (idx.get("generated_at"), idx.get("universe"), idx.get("covered"), sum(1 for v in tk.values() if isinstance(v, dict) and v.get("n_splits"))))
        art = json.loads(s3.get_object(Bucket=LIVE, Key="data/strength.json")["Body"].read())
        uni = [x["ticker"] for x in art.get("rows") or []]
        cov = [t for t in uni if isinstance(tk.get(t), dict)]
        splits = [t for t in cov if tk[t].get("n_splits")]
        ages = sorted((tk[t].get("generated_at") or "")[:10] for t in cov)
        r.log("strength universe %d: in the index %d, with a split record %d (%s), record dates %s .. %s" % (len(uni), len(cov), len(splits), splits[:40], ages[0] if ages else "-", ages[-1] if ages else "-"))
        r.log("engine rows carrying splits: %s" % [(x["ticker"], x["splits"]) for x in art["rows"] if x.get("splits")][:30])
        for t in NAMES:
            r.section(t)
            r.log("index entry: %s" % json.dumps(tk.get(t)))
            try:
                doc = json.loads(s3.get_object(Bucket=LIVE, Key="data/corporate-actions/%s.json" % t)["Body"].read())
                r.log("artifact: generated %s events %s" % (doc.get("generated_at"), json.dumps([e for e in doc.get("events") or [] if e.get("type") == "split"])))
            except Exception as e:  # noqa: BLE001
                r.log("artifact: none (%s)" % type(e).__name__)
            try:
                b = bars(s3, t)
            except Exception as e:  # noqa: BLE001
                r.log("bank: %s" % type(e).__name__)
                continue
            r.log("bank: %d sessions %s..%s last close %s" % (len(b), b[0]["date"], b[-1]["date"], b[-1]["c"]))
            for i in range(1, len(b)):
                ratio = b[i]["c"] / b[i - 1]["c"] if b[i - 1]["c"] else None
                if ratio is None or 0.6 < ratio < 1.7:
                    continue
                before = b[max(0, i - 5):i]
                after = b[i:i + 5]
                vb = sum(x["v"] or 0 for x in before) / max(1, len(before))
                va = sum(x["v"] or 0 for x in after) / max(1, len(after))
                r.log("  JUMP %s: close %s -> %s (x%.3f) | vol before avg %.0f after avg %.0f (before/after %.2f) | rows: %s" % (
                    b[i]["date"], b[i - 1]["c"], b[i]["c"], ratio, vb, va, (vb / va) if va else 0, json.dumps(before[-2:] + after[:3])))
        r.ok("ops 6742 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
