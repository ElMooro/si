"""ops 6919 - DRY RUN: the World tab's histories, compiled on the runner from the live stores, nothing written.

Khalid 2026-11-22 12:57 ET: "add a chart to world cds and yields and have it show all and also have it the chart show
only the ones i tagged same as you have on crisis peak".

The CISS engine gains a module (world_history.py) that publishes data/ciss-sovereign-world.json: for each of the World
tab's countries, the 10-year yield (monthly, ECB or OECD), the 5-year CDS the CDS desk measured, and the table's own
daily figures recovered from the world document's chain of past publications.  This runs that module exactly as the
Lambda would, with one difference: every write stays in memory.  Reads go to the live bucket and to the site's series
resolver (one monthly series at a time, with a pause).  It can be run again at any time.

The compiled document is written to aws/ops/reports/latest/ops_6919_fixture.md (base64 gzip) for the page's tests.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import io
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared"), str(ROOT / "aws" / "lambdas" / "justhodl-ciss-stress" / "source")]
LIVE = "justhodl-dashboard-live"


class Overlay:
    """Reads fall through to the bucket once and are then kept, so every later read sees the same bytes; writes stay here."""

    def __init__(self, real):
        self.real, self.mem, self.stamp, self.writes, self.reads = real, {}, {}, [], []

    def get_object(self, Bucket, Key, **kw):
        if Key not in self.mem:
            obj = self.real.get_object(Bucket=LIVE, Key=Key)           # a missing key raises the bucket's own error
            self.reads.append(Key)
            self.mem[Key], self.stamp[Key] = obj["Body"].read(), obj["LastModified"]
        return {"Body": io.BytesIO(self.mem[Key]), "LastModified": self.stamp.get(Key) or datetime.now(timezone.utc)}

    def put_object(self, Bucket, Key, Body, **kw):
        self.mem[Key] = Body if isinstance(Body, bytes) else Body.encode()
        self.writes.append(Key)


def selftest():
    class Real:
        def get_object(self, Bucket, Key):
            return {"Body": io.BytesIO(b"live"), "LastModified": datetime(2026, 1, 1, tzinfo=timezone.utc)}
    o = Overlay(Real())
    assert o.get_object(Bucket="x", Key="a")["Body"].read() == b"live" and o.reads == ["a"]
    o.put_object(Bucket="x", Key="a", Body=b"mine")
    assert o.get_object(Bucket="x", Key="a")["Body"].read() == b"mine" and o.reads == ["a"] and o.writes == ["a"]
    import world_history as world
    assert world.slug("Czech Republic") == "CZECHIA" and world.CURRENT == "data/ciss-sovereign-world.json"


def main():
    selftest()
    import boto3
    import world_history as world
    from ops_report import report
    with report("ops_6919_world_history_dry_run") as r:
        r.heading("ops 6919 - the World tab's histories, dry run (nothing written), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        store = Overlay(boto3.client("s3", region_name="us-east-1"))
        asked = []

        def fetch(url):
            t0 = time.time()
            try:
                out = world.http_json(url)
                asked.append((url.split("id=")[-1], "ok", round(time.time() - t0, 1), len(out[1])))
                return out
            except Exception as e:  # noqa: BLE001
                asked.append((url.split("id=")[-1], type(e).__name__, round(time.time() - t0, 1), 0))
                raise
        t0 = time.time()
        out = world.run(store, LIVE, budget_seconds=240, fetch=fetch)
        secs = time.time() - t0
        r.section("1. The run")
        r.log("receipt: %s" % json.dumps(out)[:600])
        r.log("%.0fs | objects read from the bucket %d (of which the world document's kept publications and receipts: %d) | writes kept in memory: %s" % (
            secs, len(store.reads), sum(1 for k in store.reads if k.startswith(world.RETAINED)), store.writes))
        r.log("monthly series asked of the resolver: %d | %s" % (len(asked), " ".join("%s:%s/%ss" % (a[0].replace("fred%3AIRLTLT01", "").replace("M156N", ""), a[1], a[2]) for a in asked)))
        doc = json.loads(store.mem[world.CURRENT])
        state = json.loads(store.mem[world.STATE])
        raw = store.mem[world.CURRENT]
        r.log("document %s %s | %d bytes, %d gzip | state %d bytes | errors %s" % (doc["contract"], doc["version"], len(raw), len(gzip.compress(raw, 6)), len(store.mem[world.STATE]), json.dumps(doc["errors"])[:500]))

        r.section("2. The four sets")
        fails = []
        M, S = doc["measures"], doc["series"]
        for s in doc["sets"]:
            ms = [M[c["ids"][s["id"]]] for c in doc["countries"] if s["id"] in c["ids"]]
            by = {}
            for m in ms:
                by[m["source"]] = by.get(m["source"], 0) + 1
            status = {}
            for m in ms:
                status[m["status"]] = status.get(m["status"], 0) + 1
            r.log("%-15s %2d of %d countries | sources %s | status %s | earliest start %s | newest %s" % (
                s["id"], s["n"], len(doc["countries"]), by, status, min((m["start"] for m in ms), default="-"), max((m["date"] for m in ms), default="-")))
            r.log("                not held for: %s" % (", ".join(doc["missing"][s["id"]]) or "nobody"))
        for c in doc["countries"]:
            parts = []
            for s in doc["sets"]:
                m = M.get(c["ids"].get(s["id"]))
                parts.append("%s %s" % (s["id"][:1] + ("r" if "reported" in s["id"] else ""), ("%s %s..%s n%d last %s" % (m["source"], m["start"][:7], m["date"], m["n"], m["value"])) if m else "-"))
            r.log("  %-15s %s" % (c["country"], " | ".join(parts)))

        r.section("3. Checks")
        table = json.loads(store.get_object(Bucket=LIVE, Key=world.TABLE)["Body"].read())
        rows = {row["country"]: row for row in table["countries"]}

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.fail(msg)
            return cond
        ck(doc["contract"] == world.CONTRACT and store.writes == [world.STATE, world.CURRENT], "contract or write order: %s" % store.writes)
        ck([c["country"] for c in doc["countries"]] == [row["country"] for row in table["countries"]], "the document's countries are not the table's")
        ck(set(M) == set(S) and all(len(S[k]["d"]) == len(S[k]["v"]) == M[k]["n"] and S[k]["d"] == sorted(S[k]["d"]) and len(set(S[k]["d"])) == len(S[k]["d"]) for k in M), "a series and its measure disagree, or dates are not ascending and unique")
        ck(all(S[k]["d"][-1] == M[k]["date"] and S[k]["v"][-1] == M[k]["value"] and S[k]["d"][0] == M[k]["start"] for k in M), "a measure's latest reading is not its series' last point")
        # the table's own figures of today are the last point of the reported series
        off = []
        for c in doc["countries"]:
            row = rows[c["country"]]
            for sid, field in (("yield_reported", "yield_10y_pct"), ("cds_reported", "cds_bp")):
                mid = c["ids"].get(sid)
                if mid and isinstance(row.get(field), (int, float)) and M[mid]["date"] == world.provider_day(row.get("as_of")) and abs(M[mid]["value"] - row[field]) > 1e-9:
                    off.append((c["country"], sid, M[mid]["value"], row[field]))
        ck(not off, "reported series do not end on the table's figure: %s" % off[:4])
        reported = [M[c["ids"]["yield_reported"]] for c in doc["countries"] if "yield_reported" in c["ids"]]
        ck(len(reported) >= 40 and min(m["n"] for m in reported) >= 20, "the day-by-day record is thin: %d countries, shortest %s" % (len(reported), min((m["n"] for m in reported), default=0)))
        ck(doc["sets"][0]["n"] >= 28 and doc["sets"][1]["n"] >= 20, "long yields for %d countries, measured CDS for %d" % (doc["sets"][0]["n"], doc["sets"][1]["n"]))
        # an OECD series against the audit of ops 6917 (United States from April 1953)
        us = M.get("WY_UNITED_STATES")
        ck(us and us["source"] == "oecd" and us["start"] == "1953-04-30" and us["freq"] == "M", "United States monthly yield: %s" % ((us and (us["source"], us["start"])),))
        tr = M.get("WC_TURKEY")
        ck(tr and tr["source"] == "dtcc" and tr["start"].startswith("2024-09") and tr["n"] > 400, "Turkey's measured CDS: %s" % ((tr and (tr["source"], tr["start"], tr["n"])),))
        # the second compile of the same inputs, inside run(), already agreed; a second whole run must not ask the resolver again
        asked_before = len(asked)
        out2 = world.run(store, LIVE, budget_seconds=120, fetch=fetch)
        r.log("a second run on what the first kept: folded %s publication(s), asked %d series more, %ss" % (out2["publications_folded"], len(asked) - asked_before, out2["seconds"]))
        failed_requests = sum(1 for a in asked[:asked_before] if a[1] != "ok")
        ck(len(asked) - asked_before <= failed_requests and out2["publications_folded"] == 1, "the second run asked again for series it already holds")
        ck(json.loads(store.mem[world.CURRENT])["series"] == S, "the second run changed the series")

        blob = base64.b64encode(gzip.compress(json.dumps(doc, separators=(",", ":")).encode(), 9)).decode()
        Path(ROOT / "aws" / "ops" / "reports" / "latest" / "ops_6919_fixture.md").write_text(blob + "\n")
        r.kv(failed_checks=len(fails), countries=len(doc["countries"]), measures=len(M), yield_monthly=doc["sets"][0]["n"], cds_measured=doc["sets"][1]["n"],
             yield_reported=doc["sets"][2]["n"], cds_reported=doc["sets"][3]["n"], seconds=round(secs), errors=len(doc["errors"]))
        if fails:
            raise SystemExit(1)
        r.ok("the World tab's histories compile from the live stores; nothing was written")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
        print("selftest ok")
    else:
        main()
