"""ops 6925 - DRY RUN: the dollar-shortage document with the Federal Reserve's series, compiled on the runner, nothing written.

Khalid 2026-11-22 16:20 ET: "are you sure about Claims on non-euro-area residents in foreign currency and
Foreign-currency claims abroad charts and data? verify again and add whatever dollar shortage metrics such as dxy or
other".

dollar_shortage.py 1.1.0 keeps the three ECB measurements exactly as before (read again from the retained ECB
responses the desk document names; no request to the ECB) and adds twelve measures from the Federal Reserve's series
that ops 6923 and ops 6924 found in the site's own warehouse: swap lines, repos with foreign central banks, DXY
rebuilt from the H.10 rates, the Fed's three dollar indexes, Treasuries in custody and their 13-week change, the foreign repo pool, SOFR against the
rate on reserves, the TED spread, and foreign banks' net position with their offices abroad.

This runs the module exactly as the Lambda would - the same 18 requests to the site's resolver, one at a time with a
pause - except that both writes (the kept series and the document) stay in memory.  It then runs it a second time to
show that nothing is asked for twice in a day.  It can be run again at any time.

The compiled document is written to aws/ops/reports/latest/ops_6925_fixture.md (base64 gzip) for the pages' tests.
"""
from datetime import datetime, timedelta, timezone
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
ECB = ("AUX_EURUSD", "AUX_FX_EA", "AUX_FX_NONEA")
# what each series is known to have done, from the Federal Reserve's own published history; (measure, field, test, words)
KNOWN = (
    ("FED_SWAPS", lambda m: m["high"][0] > 500 and m["high"][1].startswith("2008-12"), "swap lines peaked above $500 bn in December 2008"),
    ("FED_SWAPS", lambda m: m["peaks"]["covid2020"]["hi"][0] > 300, "swap lines were drawn above $300 bn inside the COVID window"),
    ("FED_FIMA", lambda m: m["high"][0] > 20 and m["high"][1].startswith("2023-03"), "repos with foreign central banks peaked in March 2023"),
    ("USD_TED", lambda m: m["high"] == [458.0, "2008-10-10"] and m["status"] == "ended" and m["date"] == "2022-01-21", "the TED spread peaked at 458 bp on 10 October 2008 and ended on 21 January 2022"),
    ("USD_SOFR_IOR", lambda m: m["high"] == [315.0, "2019-09-17"], "SOFR stood 315 bp over the rate on reserves on 17 September 2019"),
    ("H8_NET_DUE", lambda m: m["low"] == [-615.423, "2008-05-28"], "foreign banks' US offices were owed $615 bn by their offices abroad in May 2008"),
    ("USD_DXY", lambda m: 70 < m["low"][0] < 75 and m["low"][1].startswith("2008") and 118 < m["high"][0] < 122 and m["high"][1][:4] in ("2001", "2002"), "DXY's low was near 71 in 2008 and its high near 120 in 2001-02"),
    ("USD_BROAD", lambda m: m["start"] == "2006-01-02", "the broad dollar index begins on 2 January 2006"),
    ("FED_CUSTODY_UST", lambda m: m["value"] > 2000 and m["low"][0] > 1000 and m["start"].startswith("2007") and m["worse"] is None, "Treasuries in custody are above $2,000 bn and the line has no reading below $1,000 bn since it began in 2007"),
    ("FED_CUSTODY_13W", lambda m: m["peaks"]["covid2020"]["lo"][0] < -100 and m["start"].startswith("2007-10") and m["worse"] == "lower", "foreign central banks' Treasuries in custody fell by more than $100 bn over 13 weeks inside the COVID window"),
    ("FED_FIMA", lambda m: m["start"].startswith("2020"), "the repo line with foreign central banks begins in 2020"),
    ("FED_SWAPS", lambda m: m["start"].startswith("2007-12"), "the swap line begins with its first drawing in December 2007"),
)


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


def biggest_moves(d, v, count=3):
    """The largest one-observation changes of a series: [(change, date, before, after)], largest first by size."""
    steps = [(round(v[i] - v[i - 1], 4), d[i], v[i - 1], v[i]) for i in range(1, len(v))]
    return sorted(steps, key=lambda s: -abs(s[0]))[:count]


def selftest():
    assert biggest_moves(["a", "b", "c", "d"], [1.0, 2.0, 12.0, 5.0], 2) == [(10.0, "c", 2.0, 12.0), (-7.0, "d", 12.0, 5.0)]
    import dollar_shortage as dollar
    assert dollar.VERSION == "1.1.0" and len(dollar.US) == 12 and len(dollar.FRED_IDS) == 18 and [s[0] for s in dollar.SERIES] == list(ECB)
    assert {name for name, _, _ in KNOWN} <= {spec["id"] for spec in dollar.US}


def main():
    selftest()
    import boto3
    import ciss_source_model as model
    import ciss_source_store as store
    import dollar_shortage as dollar
    from ops_report import report
    with report("ops_6925_dollar_shortage_fed_dry_run") as r:
        r.heading("ops 6925 - the dollar-shortage document with the Federal Reserve's series, dry run (nothing written), %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        fails = []

        def ck(cond, msg):
            if not cond:
                fails.append(msg)
                r.fail(msg)
            return cond
        overlay = Overlay(boto3.client("s3", region_name="us-east-1"))
        asked = []

        def fetch(url):
            t = time.time()
            out = dollar.ask(url)
            asked.append((url.split("?id=")[1], round(time.time() - t, 1), len(out[1]), str(out[0].get("source"))[:60]))
            return out

        r.section("1. The run")
        t0 = time.time()
        out = dollar.run(overlay, LIVE, store, budget_seconds=240, fetch=fetch)
        secs = time.time() - t0
        r.log("receipt: %s" % json.dumps(out)[:900])
        for sid, took, size, via in asked:
            r.log("  asked %-28s %5.1fs %8d bytes | %s" % (sid, took, size, via))
        raw, state_raw = overlay.mem[dollar.CURRENT], overlay.mem.get(dollar.STATE, b"")
        doc = json.loads(raw)
        r.log("%.1fs | read from the bucket: %s | kept in memory, not written: %s" % (secs, [k.split("/")[-1][:28] for k in overlay.reads], overlay.writes))
        r.log("document %s %s | %d bytes, %d gzip | kept series %d bytes, %d gzip | stands on the desk document of %s" % (
            doc["contract"], doc["version"], len(raw), len(gzip.compress(raw, 6)), len(state_raw), len(gzip.compress(state_raw, 6)), doc["desk"]["generated_at"][:16]))
        ck(overlay.writes == [dollar.STATE, dollar.CURRENT] and doc["contract"] == dollar.CONTRACT and doc["version"] == "1.1.0", "contract, version or writes: %s" % overlay.writes)
        body = dict(doc)
        ck(body.pop("sha256", None) == model.digest(body), "the document does not carry its own digest")
        ck(len(asked) == 18 and out["series_asked"] == 18, "18 series were to be asked for, %d were" % len(asked))
        ck(doc["errors"] == {} and doc["coverage"]["status"] == "measured", "errors: %s" % json.dumps(doc["errors"])[:400])
        ck(doc["coverage"]["measures"] == 15 and doc["coverage"]["fed"] == 12 and doc["coverage"]["ecb"] == 3, "coverage: %s" % doc["coverage"])

        r.section("2. The three ECB measures are the desk document's own, as before")
        desk_doc = json.loads(overlay.mem["data/ciss-sovereign.json"])
        for mid in ECB:
            m, s, d = doc["measures"][mid], doc["series"][mid], desk_doc["measures"][mid]
            ck(all(m[k] == d[k] for k in ("value", "date", "n", "high", "low", "change", "pctile", "peaks", "key", "evidence", "label", "unit")), "%s differs from the desk document" % mid)
            ck(len(s["d"]) == len(s["v"]) == m["n"] and s["d"][-1] == m["date"] and s["v"][-1] == m["value"], "%s: series and measure disagree" % mid)
            r.log("%-13s %s %s on %s | %d observations since %s | part %s | called \"%s\"" % (mid, m["value"], m["unit"], m["date"], m["n"], m["start"], m["part"], m["short"]))
        nonea = doc["series"]["AUX_FX_NONEA"]
        r.log("the reserves item's largest one-week changes: %s" % " | ".join("%+.3f bn in the week to %s (%s -> %s)" % mv for mv in biggest_moves(nonea["d"], nonea["v"], 6)))
        aug = dict(zip(nonea["d"], nonea["v"]))
        ck(aug.get("2021-08-27") is not None and aug.get("2021-08-20") is not None and 115 < aug["2021-08-27"] - aug["2021-08-20"] < 125,
           "the August 2021 SDR allocation is not the step the document's note describes: %s -> %s" % (aug.get("2021-08-20"), aug.get("2021-08-27")))

        r.section("3. The twelve Federal Reserve measures")
        for part in doc["parts"]:
            r.log("%s - %s" % (part["name"], part["note"]))
            for mid in part["ids"]:
                m, s = doc["measures"][mid], doc["series"][mid]
                r.log("  %-16s %s" % (mid, m["label"]))
                r.log("      %s %s on %s (%s, %d days old) | %d obs since %s | high %s, low %s | 1w %s, 1m %s, 12m %s | at or below %s%% of its history | %s" % (
                    m["value"], m["unit"], m["date"], m["status"], m["age_days"], m["n"], m["start"], m["high"], m["low"], m["change"]["1w"], m["change"]["1m"], m["change"]["12m"], m["pctile"],
                    m.get("key")))
                r.log("      highest separate readings: %s" % " | ".join("%s on %s" % (v, d) for v, d in (m.get("tops") or [])[:5]))
                wins = [(e["name"], m["peaks"][e["id"]]) for e in doc["episodes"] if e["id"] in (m.get("peaks") or {}) and e["id"] in ("gfc2008", "italy2011", "covid2020", "banks2023", "tariffs2025")]
                r.log("      inside windows: %s" % " | ".join("%s lo %s hi %s" % (n, p["lo"][0], p["hi"][0]) for n, p in wins))
                ck(len(s["d"]) == len(s["v"]) == m["n"] and s["d"] == sorted(set(s["d"])) and s["d"][-1] == m["date"] and s["v"][-1] == m["value"] and s["d"][0] >= "1999-01-01", "%s: series and measure disagree" % mid)
                ck(m["source"] in ("ecb", "fred") and m["what"] and m["short"], "%s lacks its description" % mid)
        M = doc["measures"]
        for mid, test, words in KNOWN:
            try:
                good = bool(test(M[mid]))
            except Exception as e:  # noqa: BLE001
                good = False
                r.log("  %s: %s" % (mid, type(e).__name__))
            ck(good, "%s - not so here: %s, high %s, low %s" % (words, mid, M.get(mid, {}).get("high"), M.get(mid, {}).get("low")))
            if good:
                r.log("  as the record has it: %s" % words)
        live = [mid for mid in doc["order"] if M[mid]["status"] != "current"]
        r.log("not current: %s" % [(mid, M[mid]["status"], M[mid]["date"]) for mid in live])
        ck(live == ["USD_TED"], "only the TED spread should be other than current: %s" % live)
        # the foreign banks' series and the swap lines move in March 2020 as the record has them
        swaps = dict(zip(doc["series"]["FED_SWAPS"]["d"], doc["series"]["FED_SWAPS"]["v"]))
        r.log("swap lines through March-May 2020: %s" % " | ".join("%s %s" % (d, swaps[d]) for d in sorted(swaps) if "2020-03-04" <= d <= "2020-06-03"))
        cust = dict(zip(doc["series"]["FED_CUSTODY_UST"]["d"], doc["series"]["FED_CUSTODY_UST"]["v"]))
        c13 = doc["series"]["FED_CUSTODY_13W"]
        r.log("the 13-week change of Treasuries in custody, its ten lowest readings: %s" % " | ".join("%s on %s" % (v, d) for v, d in sorted(zip(c13["v"], c13["d"]))[:10]))
        whole = dict(zip(c13["d"], c13["v"]))
        ck(all(abs(whole[d] - (cust[d] - cust[e])) < 0.0015 for d, e in (("2020-04-08", "2020-01-08"), (c13["d"][-1], sorted(cust)[-14]))), "the 13-week change is not the level less the level 13 weeks earlier")
        pool = dict(zip(doc["series"]["FED_FOREIGN_RRP"]["d"], doc["series"]["FED_FOREIGN_RRP"]["v"]))
        r.log("March 2020, Treasuries in custody and the foreign repo pool: %s" % " | ".join("%s %s / %s" % (d, cust[d], pool.get(d)) for d in sorted(cust) if "2020-02-26" <= d <= "2020-04-29"))

        r.section("4. A second run in the same day asks for nothing")
        asked_first = list(asked)
        asked.clear()
        before = len(overlay.writes)
        again = dollar.run(overlay, LIVE, store, budget_seconds=60, fetch=fetch, now=datetime.fromisoformat(doc["generated_at"]) + timedelta(hours=8))
        r.log("receipt: series_asked %s, measures %s, wrote %s" % (again["series_asked"], again["coverage"]["measures"], overlay.writes[before:]))
        ck(asked == [] and again["series_asked"] == 0 and overlay.writes[before:] == [dollar.CURRENT] and again["coverage"]["measures"] == 15, "the second run asked for %d series" % len(asked))

        blob = base64.b64encode(gzip.compress(json.dumps(doc, separators=(",", ":")).encode(), 9)).decode()
        Path(ROOT / "aws" / "ops" / "reports" / "latest" / "ops_6925_fixture.md").write_text(blob + "\n")
        r.kv(failed_checks=len(fails), measures=len(doc["measures"]), observations=doc["coverage"]["observations"], bytes=len(raw), gzip=len(gzip.compress(raw, 6)), seconds=round(secs, 1), requests=len(asked_first))
        if fails:
            raise SystemExit(1)
        r.ok("the dollar-shortage document compiles with 3 ECB and 12 Federal Reserve measures; nothing was written")


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
        print("selftest ok")
    else:
        main()
