"""ops 6843 - READ-ONLY dry run of the campaign scan over the whole warehouse (re-runnable).

Khalid 2026-11-19: "accumulation, bottom, distribution engines should be built for the icons there it should show all
the corresponding stocks".  Before the engine is deployed, its own build() is run here on the runner against the real
bank: how many symbols it scans, how long that takes, how many names land on each board, where they stand against
their marks.  Nothing is written - build() only reads; the handler's put is not called.  The result is embedded so the
desk and its sheets are designed and tested on the real boards.
"""
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
import base64
import gzip
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-campaign-scan" / "source"))
from ops_report import report  # noqa: E402

sys.modules.setdefault("chain_guard", SimpleNamespace(begin=lambda event, context=None: 0))   # build() never calls it; the import must resolve off-Lambda
import lambda_function as LF  # noqa: E402


class ReadOnly:
    """The real S3 client with every write refused: a dry run cannot publish."""

    def __init__(self, s3):
        self._s3 = s3

    def get_object(self, **kw):
        return self._s3.get_object(**kw)

    def head_object(self, **kw):
        return self._s3.head_object(**kw)

    def put_object(self, **kw):
        raise RuntimeError("dry run: put refused for %s" % kw.get("Key"))


def main():
    now = datetime.now(timezone.utc)
    s3 = ReadOnly(boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 4, "mode": "standard"}, connect_timeout=5, read_timeout=30, max_pool_connections=40)))
    with report("ops_6843_campaign_scan_dry_run") as r:
        r.heading("ops 6843 - campaign scan, dry run over the warehouse %s (read-only)" % now.isoformat(timespec="minutes"))
        logs = []
        t0 = time.time()
        out = LF.build(s3, now, log=logs.append, remaining_s=1500.0)
        took = time.time() - t0
        for line in logs:
            r.log(line)
        r.log("build() %.1fs on the runner; engine %s %s; as of %s" % (took, out["engine"], out["version"], out["as_of"]))
        r.section("universe")
        r.log(json.dumps(out["universe"]))
        r.section("boards")
        for b in LF.BOARDS:
            rows = out["boards"][b]
            r.log("%s: %s" % (b, json.dumps(out["counts"][b])))
            src = {}
            for x in rows:
                src[x["source"]] = src.get(x["source"], 0) + 1
            ages = sorted(x["ago"] for x in rows)
            r.log("  sources %s; age in sessions min %s median %s max %s; rows with a sector %d, a market cap %d" % (
                json.dumps(src), ages[0] if ages else None, ages[len(ages) // 2] if ages else None, ages[-1] if ages else None,
                len([x for x in rows if x.get("sector")]), len([x for x in rows if x.get("mcap")])))
            for x in sorted([x for x in rows if x["kind"] == "stock"], key=lambda x: -(x.get("mcap") or 0))[:12]:
                r.log("  %s" % json.dumps({k: v for k, v in x.items() if k not in ("industry", "adv_usd")}))
        r.section("by sector (stocks)")
        r.log(json.dumps(out["by_sector"]))
        r.section("prints by session, last 30")
        for p in out["prints"][-30:]:
            r.log(json.dumps(p))
        r.log("errors %d: %s" % (out["n_errors"], json.dumps(out["errors"][:12])))
        body = json.dumps(out, separators=(",", ":"), default=str).encode("utf-8")
        r.log("feed would be %d bytes (%d gzipped)" % (len(body), len(gzip.compress(body, 6))))
        blob = base64.b64encode(gzip.compress(body, 9)).decode("ascii")
        r.lines.append("<!-- FEED-PACK-GZ-B64-BEGIN campaign-scan")
        r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
        r.lines.append("FEED-PACK-GZ-B64-END -->")
        c = out["counts"]
        r.kv(scanned=str(out["universe"]["scanned"]), seconds="%.0f" % took, accumulation=str(c["accumulation"]["n"]), bottom=str(c["bottom"]["n"]), distribution=str(c["distribution"]["n"]), errors=str(out["n_errors"]))
        r.ok("DRY RUN: %d symbols scanned in %.0fs - accumulation %d, bottom %d, distribution %d; nothing written" % (out["universe"]["scanned"], took, c["accumulation"]["n"], c["bottom"]["n"], c["distribution"]["n"]))


if __name__ == "__main__":
    main()
    sys.exit(0)
