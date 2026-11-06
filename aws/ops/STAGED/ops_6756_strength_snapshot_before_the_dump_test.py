"""ops 6756 - keep the strength desk's ranks AS OF THE SESSION BEFORE THE DUMP (Khalid 2026-11-05 19:25 CT: "Today would be a
good day to test strong stocks engine and thesis. The strongest stocks should have held up and either stayed flat went a
little bit up or went down less than sp500 dump today. Test that engine.").

The engine overwrites data/strength.json every run (next: 03:40 UTC, with the 2026-11-05 session folded in), so the
ranks the desk showed BEFORE today's session - bars through 2026-11-04 - must be kept first:
  1. copy data/strength.json -> data/strength/history/<as_of>.json.gz (and data/leaders.json -> data/leaders/history/)
     unless a snapshot for that as_of already exists;
  2. embed in this report, as JSON, the pieces the thesis test needs offline: every ranked row's ticker / strength /
     decile / sleeve / scopes / kind / close, the leaders / laggards / coil boards, the benchmark block.
Writes only the two history copies. Re-runnable (idempotent).
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
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def exists(s3, key):
    try:
        s3.head_object(Bucket=LIVE, Key=key)
        return True
    except Exception:  # noqa: BLE001
        return False


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    with report("ops_6756_strength_snapshot_before_the_dump_test") as r:
        r.heading("ops 6756 - strength / leaders snapshot before the 2026-11-05 dump test %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        kept = {}
        for key, prefix in (("data/strength.json", "data/strength/history/"), ("data/leaders.json", "data/leaders/history/")):
            obj = s3.get_object(Bucket=LIVE, Key=key)
            raw = obj["Body"].read()
            doc = json.loads(raw)
            as_of = doc.get("as_of") or doc.get("session") or (doc.get("bank") or {}).get("last_session")
            hk = "%s%s.json.gz" % (prefix, as_of)
            r.log("%s: %d bytes, modified %s, version %s, as_of %s, generated %s" % (key, len(raw), obj["LastModified"].isoformat(timespec="seconds"), doc.get("version"), as_of, doc.get("generated_at")))
            if not as_of:
                r.fail("%s has no as_of" % key)
                raise SystemExit(1)
            if exists(s3, hk):
                r.log("  history %s already exists - kept" % hk)
            else:
                s3.put_object(Bucket=LIVE, Key=hk, Body=gzip.compress(raw), ContentType="application/json", ContentEncoding="gzip", CacheControl="max-age=31536000")
                r.log("  wrote %s (%d bytes gz)" % (hk, len(gzip.compress(raw))))
            kept[key] = (doc, hk, as_of)
        doc = kept["data/strength.json"][0]
        rows = doc.get("rows") or []
        ranked = [x for x in rows if x.get("strength") is not None]
        r.section("the ranks as of %s (%d rows, %d ranked)" % (kept["data/strength.json"][2], len(rows), len(ranked)))
        b = doc.get("benchmark") or {}
        r.log("benchmark %s px %s as_of %s h %s" % (b.get("ticker"), b.get("px"), b.get("as_of"), json.dumps(b.get("h"))))
        slim = [{"t": x["ticker"], "s": x["strength"], "sl": x.get("sleeve"), "sc": x.get("scopes"), "k": x.get("kind"), "px": x.get("px"), "as_of": x.get("as_of"), "stale": x.get("stale"),
                 "vs": x.get("vs"), "z": x.get("z"), "coil": ((x.get("coil") or {}).get("tier")), "name": (x.get("name") or "")[:40]} for x in rows]
        bd = doc.get("boards") or {}
        r.log("leaders: %s" % ", ".join("%s %s" % (x["ticker"], x["strength"]) for x in (bd.get("leaders") or [])[:40]))
        r.log("laggards: %s" % ", ".join("%s %s" % (x["ticker"], x["strength"]) for x in (bd.get("laggards") or [])[:40]))
        r.log("coil: %s" % ", ".join("%s %s/%s" % (x["ticker"], (x.get("coil") or {}).get("tier"), (x.get("coil") or {}).get("score")) for x in (bd.get("coil") or [])[:40]))
        r.lines.append("")
        r.lines.append("<!-- STRENGTH_RANKS_JSON -->")
        r.lines.append(json.dumps({"as_of": kept["data/strength.json"][2], "version": doc.get("version"), "generated_at": doc.get("generated_at"), "benchmark": b, "rows": slim,
                                   "boards": {k: [x["ticker"] for x in (bd.get(k) or [])] for k in ("leaders", "laggards", "coil", "rs_new_highs")}}, separators=(",", ":")))
        r.lines.append("<!-- /STRENGTH_RANKS_JSON -->")
        r.lines.append("")
        r.kv(as_of=str(kept["data/strength.json"][2]), rows=str(len(rows)), ranked=str(len(ranked)), strength_history=kept["data/strength.json"][1], leaders_history=kept["data/leaders.json"][1])
        r.ok("ops 6756 complete: the pre-dump ranks are kept in S3 history and in this report")


if __name__ == "__main__":
    main()
    sys.exit(0)
