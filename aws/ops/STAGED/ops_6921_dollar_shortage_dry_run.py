"""ops 6921 - DRY RUN: the dollar-shortage document, compiled on the runner from the live stores, nothing written.

Khalid 2026-11-22 13:52 ET: "add these 3 to it under their own tab called dollar shortage and also add it to the dxy
tab" - the euro's reference rate against the dollar and the Eurosystem's two foreign-currency claims.

The CISS engine gains a module (dollar_shortage.py) that publishes data/ciss-dollar-shortage.json: the three measures
of the sovereign desk document with their complete histories, read again from the ECB responses the desk document
names.  It makes no request to the ECB.  This runs it exactly as the Lambda would, except that the write stays in
memory.  It also asks the site's data worker for a /data document the way the Dollar desk's cards will, because those
cards read their feeds through the worker.  It can be run again at any time.

The compiled document is written to aws/ops/reports/latest/ops_6921_fixture.md (base64 gzip) for the pages' tests.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import io
import json
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared"), str(ROOT / "aws" / "lambdas" / "justhodl-ciss-stress" / "source")]
LIVE = "justhodl-dashboard-live"
WORKER = "https://justhodl-data-proxy.raafouis.workers.dev"


class Overlay:
    """Reads fall through to the bucket once and are then kept; writes stay here."""

    def __init__(self, real):
        self.real, self.mem, self.head, self.writes, self.reads = real, {}, {}, [], []

    def get_object(self, Bucket, Key, **kw):
        if Key not in self.mem:
            obj = self.real.get_object(Bucket=LIVE, Key=Key)           # a missing key raises the bucket's own error
            self.reads.append(Key)
            self.mem[Key] = obj["Body"].read()
            self.head[Key] = {k: v for k, v in obj.items() if k != "Body"}
        return {**self.head.get(Key, {"LastModified": datetime.now(timezone.utc), "Metadata": {}}), "Body": io.BytesIO(self.mem[Key])}

    def put_object(self, Bucket, Key, Body, **kw):
        self.mem[Key] = Body if isinstance(Body, bytes) else Body.encode()
        self.writes.append(Key)


def changes(d, v):
    """Latest minus the reading a week / a month / a year of calendar days earlier, each taken at or before that day."""
    from datetime import date, timedelta
    last = date.fromisoformat(d[-1])
    out = {}
    for name, days in (("7 days", 7), ("30 days", 30), ("365 days", 365)):
        target = (last - timedelta(days=days)).isoformat()
        at = max((i for i, x in enumerate(d) if x <= target), default=None)
        out[name] = None if at is None else round(v[-1] - v[at], 4)
    return out


def selftest():
    assert changes(["2026-01-01", "2026-01-05", "2026-01-12"], [1.0, 2.0, 5.0]) == {"7 days": 3.0, "30 days": None, "365 days": None}
    import dollar_shortage as dollar
    assert dollar.CURRENT == "data/ciss-dollar-shortage.json" and [s[0] for s in dollar.SERIES] == ["AUX_EURUSD", "AUX_FX_EA", "AUX_FX_NONEA"]


def main():
    selftest()
    import boto3
    import ciss_source_model as model
    import ciss_source_store as store
    import dollar_shortage as dollar
    from ops_report import report
    with report("ops_6921_dollar_shortage_dry_run") as r:
        r.heading("ops 6921 - the dollar-shortage document, dry run (nothing written), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        fails = []

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.fail(msg)
            return cond
        overlay = Overlay(boto3.client("s3", region_name="us-east-1"))

        r.section("1. The run")
        t0 = time.time()
        out = dollar.run(overlay, LIVE, store)
        secs = time.time() - t0
        r.log("receipt: %s" % json.dumps(out)[:500])
        raw = overlay.mem[dollar.CURRENT]
        doc = json.loads(raw)
        r.log("%.1fs | read from the bucket: %s | kept in memory, not written: %s" % (secs, [k.split("/")[-1][:28] for k in overlay.reads], overlay.writes))
        r.log("document %s %s | %d bytes, %d gzip | stands on the desk document of %s" % (doc["contract"], doc["version"], len(raw), len(gzip.compress(raw, 6)), doc["desk"]["generated_at"][:16]))
        ck(overlay.writes == [dollar.CURRENT] and doc["contract"] == dollar.CONTRACT, "contract or writes: %s" % overlay.writes)
        body = dict(doc)
        ck(body.pop("sha256", None) == model.digest(body), "the document does not carry its own digest")

        r.section("2. The three measures")
        desk_doc = json.loads(overlay.mem["data/ciss-sovereign.json"])
        hist = json.loads(overlay.get_object(Bucket=LIVE, Key="data/ciss-sovereign-history.json")["Body"].read())
        for mid in doc["order"]:
            m, s, d = doc["measures"][mid], doc["series"][mid], desk_doc["measures"][mid]
            weekly = hist["series"][mid]
            r.log("%-13s %s | %s %s on %s | %d observations %s .. %s (the desk's history keeps %d) | high %s, low %s" % (
                mid, m["label"], m["value"], m["unit"], m["date"], len(s["d"]), s["d"][0], s["d"][-1], len(weekly["d"]), m["high"], m["low"]))
            r.log("              changes over %s | the desk's own: %s" % (json.dumps(changes(s["d"], s["v"])), json.dumps(d["change"])))
            r.log("              %s" % m["what"])
            ck(all(m[k] == d[k] for k in ("value", "date", "n", "high", "low", "change", "pctile", "peaks", "key", "evidence")), "%s differs from the desk document" % mid)
            ck(len(s["d"]) == len(s["v"]) == m["n"] and s["d"] == sorted(set(s["d"])) and s["d"][-1] == m["date"] and s["v"][-1] == m["value"], "%s: series and measure disagree" % mid)
            # the desk's weekly history is a subset of the complete series, value for value
            full = dict(zip(s["d"], s["v"]))
            ck(all(full.get(day) == value for day, value in zip(weekly["d"], weekly["v"])), "%s: the desk's history is not a subset of the complete series" % mid)
            hi = max(zip(s["v"], s["d"]))
            ck(hi[0] == m["high"][0], "%s: the series' highest value %s is not the measure's %s" % (mid, hi, m["high"]))
        eur, ea = doc["series"]["AUX_EURUSD"], doc["measures"]["AUX_FX_EA"]
        ck(len(eur["d"]) > 6500 and eur["d"][0] == "1999-01-04", "the reference rate is not daily since 1999: %d from %s" % (len(eur["d"]), eur["d"][0]))
        r.log("foreign-currency lending to euro-area banks, highest readings: %s" % " | ".join("%s bn on %s" % (v, d) for v, d in ea["tops"][:5]))
        ck(ea["high"] == [248.854, "2008-12-05"] and ea["peaks"]["covid2020"]["hi"][0] > 100, "the 2008 and 2020 dollar operations are not where the ECB's series has them")
        for s in doc["sources"]:
            r.log("  source %-16s %s | %d observations | received %s | %s" % (s["name"], s["path"], s["observations"], str(s["acquired_at"])[:16], s["evidence"]["key"].split("/")[-1][:20]))

        r.section("3. The worker the Dollar desk's cards read through")
        for path in ("/data/ciss-sovereign.json", "/data/ciss-dollar-shortage.json"):
            try:
                req = urllib.request.Request(WORKER + path + "?t=%d" % (time.time() // 300), headers={"Origin": "https://justhodl.ai", "User-Agent": "jh-ops-probe"})
                with urllib.request.urlopen(req, timeout=60) as resp:
                    got = resp.read()
                    info = (resp.status, resp.headers.get("content-type"), resp.headers.get("access-control-allow-origin"), len(got))
                r.log("  %s -> HTTP %s, %s, allow-origin %s, %d bytes, JSON %s" % (path, info[0], info[1], info[2], info[3], isinstance(json.loads(got), dict)))
                if path.endswith("sovereign.json"):
                    ck(info[0] == 200 and info[2] in ("*", "https://justhodl.ai"), "the worker does not hand a /data document to the site's pages: %s" % (info,))
            except Exception as e:  # noqa: BLE001
                r.log("  %s -> %s: %s" % (path, type(e).__name__, str(e)[:120]))
                if path.endswith("sovereign.json"):
                    ck(False, "the worker refused %s" % path)

        blob = base64.b64encode(gzip.compress(json.dumps(doc, separators=(",", ":")).encode(), 9)).decode()
        Path(ROOT / "aws" / "ops" / "reports" / "latest" / "ops_6921_fixture.md").write_text(blob + "\n")
        r.kv(failed_checks=len(fails), measures=len(doc["measures"]), observations=doc["coverage"]["observations"], bytes=len(raw), seconds=round(secs, 1))
        if fails:
            raise SystemExit(1)
        r.ok("the dollar-shortage document compiles from the retained ECB responses and equals the desk document; nothing was written")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
        print("selftest ok")
    else:
        main()
