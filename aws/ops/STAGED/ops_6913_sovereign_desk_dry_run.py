"""ops 6913 - DRY RUN of the sovereign-stress desk document the CISS engine is about to compile.

The engine's new module (aws/lambdas/justhodl-ciss-stress/source/sovereign_desk.py) is run here exactly as the
Lambda will run it - the live warehouse, the retained ECB originals, seven real requests to the ECB Data Portal -
against a storage overlay: every read falls through to the bucket, every write stays in memory.  Nothing in the
bucket changes and no engine is invoked.

What must hold before this may go live:
  * the seven added ECB responses arrive and parse; no measure group is empty; nothing is reported in `errors`;
  * every SovCISS / CISS measure equals the warehouse's own row for the same series (value, date, percentile);
  * the two compiles from separate reads agree (the module raises otherwise);
  * the documents are small enough to open in the chart's overlay.

The two compiled documents are written to aws/ops/reports/latest/ops_6913_fixture.md (base64 gzip) so the desk page
is built and tested on the real numbers before the engine publishes anything.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import hashlib
import io
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "aws" / "ops"), str(ROOT / "aws" / "shared"), str(ROOT / "aws" / "lambdas" / "justhodl-ciss-stress" / "source")]
LIVE = "justhodl-dashboard-live"
GROUPS = {"sovciss": 11, "euro_area": 5, "contribution": 6, "ciss": 10, "clifs": 20, "curve": 4, "yield": 20, "spread": 19, "target": 15, "debt": 20, "auxiliary": 3}
SHOW_PEAKS = ("EA_SOV_GDP", "EA_SOV_RANGE", "SOV_GR", "SOV_IT", "SOV_ES", "SOV_FR", "EA_CURVE_10Y", "S10_GR", "S10_IT", "S10_FR", "TGT_IT", "TGT_DE")


class Refused(Exception):
    def __init__(self, code):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class Overlay:
    """Reads fall through to the bucket; writes stay here.  Conditional writes behave as S3's do."""

    def __init__(self, real):
        self.real, self.mem, self.meta, self.writes, self.real_reads = real, {}, {}, [], 0

    def _load(self, key):
        if key not in self.mem:
            obj = self.real.get_object(Bucket=LIVE, Key=key)           # a missing key raises the bucket's own error
            self.real_reads += 1
            self.mem[key], self.meta[key] = obj["Body"].read(), obj.get("Metadata", {})
        return self.mem[key]

    def _exists(self, key):
        if key in self.mem:
            return True
        try:
            self.real.head_object(Bucket=LIVE, Key=key)
            return True
        except Exception:  # noqa: BLE001
            return False

    def get_object(self, Bucket, Key, **kw):
        raw = self._load(Key)
        return {"Body": io.BytesIO(raw), "ETag": hashlib.sha256(raw).hexdigest(), "Metadata": self.meta.get(Key, {})}

    def put_object(self, Bucket, Key, Body, **kw):
        if kw.get("IfNoneMatch") == "*" and self._exists(Key):
            raise Refused("PreconditionFailed")
        self.mem[Key], self.meta[Key] = Body, kw.get("Metadata", {})
        self.writes.append(Key)


class Timed:
    """The engine's own store, with each request to the ECB timed and logged."""

    def __init__(self, store, log):
        self._store, self._log, self.requests = store, log, []

    def __getattr__(self, name):
        return getattr(self._store, name)

    def request(self, url, deadline):
        t0 = time.time()
        try:
            raw, received = self._store.request(url, deadline)
        except Exception as e:  # noqa: BLE001
            self._log("  ECB %s -> %s after %.1fs" % (url.split("/service/data/")[1][:70], self._store.safe_error(e), time.time() - t0))
            raise
        self.requests.append((url, len(raw), time.time() - t0))
        self._log("  ECB %s -> %d bytes in %.1fs" % (url.split("/service/data/")[1][:70], len(raw), time.time() - t0))
        return raw, received


def fmt(m, key="value"):
    if not m or m.get(key) is None:
        return "-"
    return "%s%s" % (m[key], {"bp": " bp", "pct": "%", "eur_bn": " bn", "pct_gdp": "% GDP"}.get(m["unit"], ""))


def selftest():
    class Real:
        def __init__(self):
            self.puts = 0

        def get_object(self, Bucket, Key):
            if Key != "have":
                raise Refused("NoSuchKey")
            return {"Body": io.BytesIO(b"x"), "Metadata": {}}

        def head_object(self, Bucket, Key):
            if Key != "have":
                raise Refused("404")

        def put_object(self, **kw):
            self.puts += 1

    real = Real()
    o = Overlay(real)
    o.put_object(Bucket=LIVE, Key="new", Body=b"1", IfNoneMatch="*")
    try:
        o.put_object(Bucket=LIVE, Key="have", Body=b"2", IfNoneMatch="*")
        raise AssertionError("a conditional write over an existing object must be refused")
    except Refused:
        pass
    assert o.get_object(Bucket=LIVE, Key="new")["Body"].read() == b"1" and real.puts == 0 and o.writes == ["new"]
    assert fmt({"value": 59.81, "unit": "bp"}) == "59.81 bp" and fmt(None) == "-"


def main():
    selftest()
    import boto3
    from ops_report import report
    import ciss_source_model as model
    import ciss_source_store as store
    import sovereign_desk as desk
    real = boto3.client("s3", region_name="us-east-1")
    now = datetime.now(timezone.utc)
    fails = []

    with report("ops_6913_sovereign_desk_dry_run") as r:
        r.heading("ops 6913 - DRY RUN: the sovereign-stress desk document, compiled from live originals (nothing written), %s" % now.isoformat(timespec="minutes"))
        overlay = Overlay(real)
        timed = Timed(store, r.log)

        r.section("1. Compile, as the Lambda will")
        t0 = time.time()
        result = desk.run(overlay, LIVE, timed, budget_seconds=900)
        took = time.time() - t0
        doc, hist = json.loads(overlay.mem[desk.CURRENT]), json.loads(overlay.mem[desk.HISTORY])
        raw_doc, raw_hist = overlay.mem[desk.CURRENT], overlay.mem[desk.HISTORY]
        r.log("published (to the overlay) in %.0fs: %s" % (took, json.dumps(result)[:300]))
        r.log("document %d bytes (%d gzip) | history %d bytes (%d gzip) | %d objects read from the bucket | writes held back: %d" % (
            len(raw_doc), len(gzip.compress(raw_doc)), len(raw_hist), len(gzip.compress(raw_hist)), overlay.real_reads, len(overlay.writes)))
        kinds = {}
        for key in overlay.writes:
            kind = "evidence" if key.startswith("data/evidence/") else "compiler" if "/compilers/" in key else "run receipt" if "/sovereign/runs/" in key else key
            kinds[kind] = kinds.get(kind, 0) + 1
        r.log("what a live run would write: %s" % json.dumps(kinds))
        r.log("errors: %s | coverage %s | ended at source: %d" % (json.dumps(doc["errors"]), json.dumps(doc["coverage"]), len(doc["ended_at_source"])))
        for e in doc["ended_at_source"]:
            r.log("    ended: %-10s %s last %s" % (e["id"], e["key"], e["last"]))
        if doc["errors"]:
            fails.append("the compile reports errors: %s" % json.dumps(doc["errors"])[:300])
        if len(gzip.compress(raw_doc)) > 400_000 or len(gzip.compress(raw_hist)) > 1_500_000:
            fails.append("the documents are too large for a desk in the overlay")

        r.section("2. The sources")
        for s in doc["sources"]:
            r.log("  %-16s %s | %s series, %s rows, newest %s%s" % (s["name"], s["label"], s.get("series"), s.get("rows", "-"), s.get("newest", "-"), " | CARRIED" if s.get("carried") else ""))
        if len(timed.requests) != len(desk.AUX):
            fails.append("%d of %d ECB responses arrived" % (len(timed.requests), len(desk.AUX)))

        r.section("3. The measures, by group")
        m = doc["measures"]
        groups = {}
        for x in m.values():
            groups.setdefault(x["group"], []).append(x)
        for g, need in GROUPS.items():
            have = groups.get(g, [])
            state = {}
            for x in have:
                state[x["status"]] = state.get(x["status"], 0) + 1
            r.log("  %-13s %3d measures (%s) | newest %s, oldest %s" % (g, len(have), json.dumps(state), max((x["date"] for x in have), default="-"), min((x["date"] for x in have), default="-")))
            if len(have) < need:
                fails.append("group %s has %d measures, at least %d expected" % (g, len(have), need))
        missing_hist = [k for k in m if k not in hist["series"] or not hist["series"][k]["d"]]
        if missing_hist:
            fails.append("measures without a history: %s" % missing_hist[:8])

        r.section("4. Against the warehouse's own rows")
        ware = json.loads(overlay.mem[desk.WAREHOUSE])
        rows = {x["key"]: x for x in ware["series"]}
        checked = differ = 0
        for x in m.values():
            row = rows.get(x["key"] or "")
            if not row or row["quality"]["status"] != "fresh":
                continue
            checked += 1
            bad = []
            if x["date"] != row["latest_date"] or abs(x["value"] - row["latest"]) > 6e-6:
                bad.append("value %s on %s against %s on %s" % (x["value"], x["date"], row["latest"], row["latest_date"]))
            if row.get("pctile") is not None and abs(x["pctile"] - row["pctile"]) > 0.06:
                bad.append("percentile %s against %s" % (x["pctile"], row["pctile"]))
            if row.get("percentile_5y") is not None and x["pctile_5y"] is not None and abs(x["pctile_5y"] - row["percentile_5y"]) > 0.6:
                bad.append("5-year percentile %s against %s" % (x["pctile_5y"], row["percentile_5y"]))
            for name in ("1w", "1m", "3m", "12m"):
                theirs = ((row.get("comparisons") or {}).get(name) or {}).get("value")
                if theirs is not None and x["change"][name] is not None and abs(x["change"][name] - theirs) > 6e-6:
                    bad.append("%s change %s against %s" % (name, x["change"][name], theirs))
                if (theirs is None) != (x["change"][name] is None):
                    bad.append("%s change %s against %s" % (name, x["change"][name], theirs))
            if abs(x["high"][0] - row["max"]) > 6e-6:
                bad.append("high %s against %s" % (x["high"][0], row["max"]))
            if bad:
                differ += 1
                r.log("  DIFFERS %-12s %s" % (x["id"], "; ".join(bad)))
        r.log("  %d measures checked against their warehouse rows, %d differ" % (checked, differ))
        if differ or checked < 40:
            fails.append("%d of %d measures differ from the warehouse's own rows" % (differ, checked))
        blank = [x for x in m.values() if x.get("blank_after")]
        for x in blank:
            r.log("  blank newest row at the ECB: %-10s shows %s on %s (the %s row is blank)" % (x["id"], x["value"], x["date"], x["blank_after"]))
        left = {x["id"]: x["left_out"] for x in m.values() if x.get("left_out")}
        r.log("  numeric rows left out for their status: %s" % (json.dumps(left)[:600] or "none"))

        r.section("5. The desk in words")
        for mid in doc["euro_area"] + doc["contributions"][1:]:
            x = m[mid]
            r.log("  %-12s %-62s %10s  %s  %s  1w %s  1m %s  12m %s  | lower than %s%% of its history | high %s on %s" % (
                mid, x["label"][:62], fmt(x), x["date"], x["status"], x["change"]["1w"], x["change"]["1m"], x["change"]["12m"], round(100 - x["pctile"], 1), x["high"][0], x["high"][1]))
        cross = doc["cross_section"]
        r.log("  cross-section %s: %s | median %s, range %s" % (cross["date"], " ".join("%s %s" % (c["area"], c["value"]) for c in cross["ranking"]), cross["median"], cross["range"]))
        r.log("  %-4s %-14s %-4s %9s %6s %9s %9s %10s %10s %8s %8s" % ("", "country", "euro", "SovCISS", "pct", "10y", "spread", "TARGET bn", "debt", "CLIFS", "CISS"))
        for c in doc["countries"]:
            g = lambda kind: m.get(c["ids"].get(kind, ""))
            r.log("  %-4s %-14s %-4s %9s %6s %9s %9s %10s %10s %8s %8s" % (c["area"], c["country"][:14], "yes" if c["euro"] else "no", fmt(g("sovciss")), (g("sovciss") or {}).get("pctile", "-"), fmt(g("yield")),
                                                                         fmt(g("spread")), fmt(g("target")), fmt(g("debt")), fmt(g("clifs")), fmt(g("ciss"))))
        r.log("  auxiliaries: %s" % " | ".join("%s %s on %s (exact %s)" % (m[a]["label"], m[a]["value"], m[a]["date"], m[a]["exact"]) for a in doc["auxiliaries"]))
        r.log("  crisis peaks (highest reading inside each window; TARGET shows the lowest):")
        r.log("    %-28s %s" % ("", " ".join("%-12s" % mid for mid in SHOW_PEAKS)))
        for ep in doc["episodes"]:
            cells = []
            for mid in SHOW_PEAKS:
                p = (m.get(mid) or {}).get("peaks", {}).get(ep["id"])
                cells.append("%-12s" % ("-" if not p else p["lo" if mid.startswith("TGT_") else "hi"][0]))
            r.log("    %-28s %s" % (ep["name"][:28], " ".join(cells)))
        for mid in ("EA_SOV_GDP", "SOV_FR", "EA_CURVE_10Y"):
            r.log("    highest readings of %s by the data: %s" % (mid, "  ".join("%s on %s" % (v, d) for v, d in m[mid]["tops"])))

        r.section("6. For the page's tests")
        blob = base64.b64encode(gzip.compress(json.dumps({"desk": doc, "history": hist}, separators=(",", ":")).encode(), 9)).decode()
        Path(ROOT / "aws" / "ops" / "reports" / "latest" / "ops_6913_fixture.md").write_text(blob + "\n")
        r.kv(seconds=round(took), measures=len(m), countries=len(doc["countries"]), document_gzip=len(gzip.compress(raw_doc)), history_gzip=len(gzip.compress(raw_hist)),
             checked_against_warehouse=checked, differ=differ, failed_checks=len(fails))
        if fails:
            r.fail(" | ".join(fails))
            sys.exit(1)
        r.ok("the document compiles from live originals and seven ECB responses, agrees with the warehouse, and replays; nothing was written")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
