"""ops 6893 - READ-ONLY probe: why the repo board's venue data stops on 2026-08-04, and what the sources hold today.

ops 6892 found the master board's OFR-fed groups frozen: reference rates and volumes, tri-party, GCF and DVP end
2026-08-03/04, primary-dealer fails end 2026-07-22, FICC sponsored repo ends 2026-09-18, the tri-party haircuts end
2026-07-09 - while the NY Fed percentiles the repo engine pulls itself are current.  justhodl-ofr-stfm never fetches a
mnemonic again once it is in state.done (the code path exists only for the hedge-fund catalog, 10 a run).

This measures, without writing anything:
  1. the OFR bank: state file, and every series file by the day of its last write;
  2. the OFR API today: what one dataset call returns (shape, size, time, newest date per mnemonic) for the five
     short-term-funding datasets, and the per-mnemonic call the bank stores;
  3. per mnemonic: banked newest date against the source's newest date;
  4. the hedge-fund catalog's sponsored-repo series, bank against source;
  5. the engine's own runs over 14 days;
  6. the other feeders named by the board: haircut index, DTCC fails, NY Fed primary-dealer API.
"""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
import gzip
import json
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
LIVE = "justhodl-dashboard-live"
OFR = "https://data.financialresearch.gov/v1"
HFM = "https://data.financialresearch.gov/hf/v1"
DATASETS = ("fnyr", "repo", "nypd", "mmf", "tyld")
UA = {"User-Agent": "JustHodl research (ops probe)"}


def newest(doc):
    """The newest ISO date found in the longest [date, value] list anywhere in a JSON document."""
    best = []

    def walk(o, depth=0):
        nonlocal best
        if depth > 8:
            return
        if isinstance(o, list):
            if o and all(isinstance(r, (list, tuple)) and len(r) >= 2 and isinstance(r[0], str) and len(r[0]) >= 10 and r[0][4:5] == "-" for r in o[:5]):
                if len(o) > len(best):
                    best = o
            else:
                for r in o[:40]:
                    walk(r, depth + 1)
        elif isinstance(o, dict):
            for v in o.values():
                walk(v, depth + 1)

    walk(doc)
    rows = [r for r in best if r[1] is not None]
    return (rows[-1][0][:10] if rows else None), len(rows), (rows[-1][1] if rows else None)


def series_of(dataset_doc):
    """{mnemonic: per-series document} out of a dataset answer, whichever of the known envelopes it uses."""
    if not isinstance(dataset_doc, dict):
        return {}
    inner = dataset_doc.get("timeseries") if isinstance(dataset_doc.get("timeseries"), dict) else dataset_doc
    return {k: v for k, v in inner.items() if isinstance(v, dict)}


def selftest():
    doc = {"X": {"timeseries": {"aggregation": [["2026-11-18", 3.9], ["2026-11-19", 3.91], ["2026-11-20", None]]}, "metadata": {"a": 1}}}
    assert newest(doc) == ("2026-11-19", 2, 3.91), newest(doc)
    assert newest({"a": 1}) == (None, 0, None)
    assert set(series_of({"timeseries": {"A": {"timeseries": {}}, "B": {"timeseries": {}}}})) == {"A", "B"}
    assert set(series_of({"A": {"timeseries": {}}, "note": "x"})) == {"A"} and series_of([]) == {}


def http(url, timeout=120):
    t0 = time.time()
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, raw, time.time() - t0, dict(resp.headers)
    except urllib.error.HTTPError as e:
        return e.code, b"", time.time() - t0, {}
    except Exception as e:  # noqa: BLE001
        return type(e).__name__, b"", time.time() - t0, {}


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}, max_pool_connections=48))
    cw = boto3.client("cloudwatch", region_name="us-east-1")

    def jget(key):
        try:
            o = s3.get_object(Bucket=LIVE, Key=key)
            raw = o["Body"].read()
            if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return json.loads(raw), o["LastModified"]
        except Exception as e:  # noqa: BLE001
            return {"_unreadable": type(e).__name__}, None

    def listing(prefix):
        rows, token = [], None
        while True:
            kw = {"Bucket": LIVE, "Prefix": prefix, "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            page = s3.list_objects_v2(**kw)
            rows += [(o["Key"], o["LastModified"], o["Size"]) for o in page.get("Contents", [])]
            token = page.get("NextContinuationToken")
            if not token:
                return rows

    with report("ops_6893_repo_feeders_probe") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6893 - the repo board's feeders: bank against source (read-only), %s" % now.isoformat(timespec="minutes"))
        facts = {}

        r.section("1. The OFR bank")
        state, when = jget("data/warm/ofr/state.json")
        cat, done = state.get("catalog") or [], set(state.get("done") or [])
        r.log("state written %s | status %s | as_of %s | catalog %d | done %d | failures %d | catalog source %s | new last run %s" % (
            when.strftime("%Y-%m-%d %H:%M") if when else "-", state.get("status"), state.get("as_of"), len(cat), len(done),
            len(state.get("failures") or {}), state.get("catalog_source"), (state.get("catalog_new_last_run") or [])[:6]))
        files = listing("data/warm/ofr/series/")
        days = Counter(w.strftime("%Y-%m-%d") for _, w, _ in files)
        r.log("series files: %d, %.1f MB | by day of last write: %s" % (len(files), sum(s for _, _, s in files) / 1e6, ", ".join("%s %d" % d for d in sorted(days.items(), reverse=True)[:10])))
        facts["ofr_files"] = len(files)
        facts["ofr_newest_write"] = max(days) if days else None

        r.section("2. The OFR API today")
        code, raw, secs, _ = http(OFR + "/metadata/mnemonics")
        live_cat = []
        try:
            d = json.loads(raw)
            live_cat = sorted({(x if isinstance(x, str) else x.get("mnemonic")) for x in d if x})
        except Exception:  # noqa: BLE001
            pass
        r.log("/metadata/mnemonics: %s, %d bytes, %.1f s -> %d mnemonics | not in the bank's catalog: %s" % (code, len(raw), secs, len(live_cat), sorted(set(live_cat) - set(cat))[:12]))
        code, raw, secs, _ = http(OFR + "/series/full?mnemonic=FNYR-SOFR-A")
        try:
            full = json.loads(raw)
            inner = full.get("FNYR-SOFR-A") if isinstance(full, dict) else None
            r.log("/series/full?mnemonic=FNYR-SOFR-A: %s, %d bytes, %.1f s | top keys %s | inner keys %s | newest %s" % (
                code, len(raw), secs, list(full)[:4] if isinstance(full, dict) else type(full).__name__, list(inner)[:6] if isinstance(inner, dict) else None, newest(full)))
        except Exception as e:  # noqa: BLE001
            r.log("/series/full: %s (%s)" % (code, type(e).__name__))
        source = {}
        for ds in DATASETS:
            code, raw, secs, _ = http(OFR + "/series/dataset?dataset=" + ds, timeout=240)
            try:
                doc = json.loads(raw)
            except Exception:  # noqa: BLE001
                doc = None
            ser = series_of(doc)
            for m, body in ser.items():
                source[m] = newest(body)
            sample = next(iter(ser.items()), (None, {}))
            r.log("/series/dataset?dataset=%s: %s, %.1f MB, %.1f s | top keys %s | %d series | sample %s keys %s | newest dates: %s" % (
                ds, code, len(raw) / 1e6, secs, list(doc)[:5] if isinstance(doc, dict) else type(doc).__name__, len(ser), sample[0],
                list(sample[1])[:5], ", ".join("%s %d" % d for d in sorted(Counter(v[0] or "-" for m, v in source.items() if m in ser).items(), reverse=True)[:5])))
        facts["source_series"] = len(source)

        r.section("3. Bank against source, per mnemonic")

        def banked(m):
            doc, w = jget("data/warm/ofr/series/%s.json.gz" % m)
            return m, newest(doc.get("payload") if isinstance(doc, dict) and "payload" in doc else doc), (doc.get("as_of") if isinstance(doc, dict) else None)

        with ThreadPoolExecutor(max_workers=32) as pool:
            bank = {m: (n, a) for m, n, a in pool.map(banked, sorted(source))}
        behind, same, unbanked = [], 0, []
        for m, (sdate, sn, _) in sorted(source.items()):
            (bdate, bn, _), as_of = bank.get(m, ((None, 0, None), None))
            if bdate is None:
                unbanked.append(m)
            elif sdate and sdate > bdate:
                behind.append((m, bdate, sdate, sn - bn, as_of))
            else:
                same += 1
        by_family = Counter(m.split("-")[0] for m, *_ in behind)
        r.log("source series %d | bank current %d | bank behind %d (%s) | not banked %d %s" % (len(source), same, len(behind), dict(by_family), len(unbanked), unbanked[:8]))
        for m, bdate, sdate, more, as_of in behind[:14] + behind[len(behind) // 2:len(behind) // 2 + 6] + behind[-6:]:
            r.log("  %-30s bank %s | source %s | %4d observations behind | banked %s" % (m, bdate, sdate, more, (as_of or "")[:10]))
        facts["bank_behind"] = len(behind)
        facts["bank_current"] = same
        live_nypd = sorted(m for m, v in source.items() if m.startswith("NYPD") and (v[0] or "") >= (now - timedelta(days=30)).strftime("%Y-%m-%d"))
        ended_nypd = sorted(m for m, v in source.items() if m.startswith("NYPD") and (v[0] or "") < "2022-06-01")
        r.log("primary-dealer mnemonics at the source: %d still printing (%s ...), %d ended before mid-2022" % (len(live_nypd), ", ".join(live_nypd[:10]), len(ended_nypd)))
        inv, _ = jget("data/repo-master-inventory.json")
        inv_ids = {e.get("mnemonic") for e in (inv.get("series") or [])}
        r.log("board inventory: %d series | source series the inventory does not carry: %d (%s ...)" % (
            len(inv_ids), len(set(source) - inv_ids), ", ".join(sorted(set(source) - inv_ids)[:14])))
        r.log("  of those, printing in the last 30 days: %s" % ", ".join(sorted(m for m in set(source) - inv_ids if (source[m][0] or "") >= (now - timedelta(days=30)).strftime("%Y-%m-%d"))[:40]))
        facts["source_not_on_board"] = len(set(source) - inv_ids)

        r.section("4. Hedge-fund catalog: sponsored repo")
        hf, when = jget("data/warm/ofr-hfm/state.json")
        r.log("state written %s | status %s | catalog %d | refresh_cursor %s" % (when.strftime("%Y-%m-%d %H:%M") if when else "-", hf.get("status"), len(hf.get("catalog") or []), hf.get("refresh_cursor")))
        for m in ("FICC-SPONSORED_REPO_VOL", "FICC-SPONSORED_REVREPO_VOL"):
            doc, w = jget("data/warm/ofr-hfm/series/%s.json.gz" % m)
            code, raw, secs, _ = http(HFM + "/series/full?mnemonic=" + m)
            try:
                src = newest(json.loads(raw))
            except Exception:  # noqa: BLE001
                src = (None, 0, None)
            r.log("  %-28s bank newest %s (file written %s) | source %s newest %s (%d obs, %.1f s)" % (m, newest(doc)[0], w.strftime("%Y-%m-%d") if w else "-", code, src[0], src[1], secs))
        code, raw, secs, _ = http(HFM + "/series/dataset?dataset=ficc")
        try:
            ser = series_of(json.loads(raw))
        except Exception:  # noqa: BLE001
            ser = {}
        r.log("  /hf/v1/series/dataset?dataset=ficc: %s, %d bytes, %.1f s, %d series %s" % (code, len(raw), secs, len(ser), sorted(ser)[:6]))

        r.section("5. The engine's own runs, 14 days")
        for fn in ("justhodl-ofr-stfm", "justhodl-repo", "justhodl-repo-market", "justhodl-nyfed-markets-full", "justhodl-nyfed-pd"):
            row = {}
            for metric, stat in (("Invocations", "Sum"), ("Errors", "Sum"), ("Duration", "Average")):
                pts = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=metric, Dimensions=[{"Name": "FunctionName", "Value": fn}],
                                               StartTime=now - timedelta(days=14), EndTime=now, Period=86400, Statistics=[stat]).get("Datapoints") or []
                row[metric] = sorted((p["Timestamp"].strftime("%m-%d"), round(p[stat], 1)) for p in pts)
            r.log("%-28s runs/day %s | errors %d | mean duration %s s" % (
                fn, " ".join("%s:%d" % (d, v) for d, v in row["Invocations"][-8:]), sum(v for _, v in row["Errors"]),
                round(sum(v for _, v in row["Duration"]) / max(1, len(row["Duration"])) / 1000, 1)))

        r.section("6. The other feeders")
        hix, when = jget("data/warm/nyfed-research/haircuts-series/_index.json")
        r.log("haircut index written %s | %d series | keys %s" % (when.strftime("%Y-%m-%d") if when else "-", len(hix.get("series") or []), [k for k in hix if k != "series"][:8]))
        for url in ("https://www.newyorkfed.org/medialibrary/media/banking/pdf/tpr_stats_historical.xlsx",
                    "https://www.newyorkfed.org/medialibrary/media/research/banking_research/tri-party-repo/tpr-statistics-historical.xlsx",
                    "https://www.dtcc.com/charts/daily-total-us-treasury-trade-fails.json",
                    "https://www.dtcc.com/-/media/Files/Downloads/Charts/daily-treasury-fails.csv",
                    "https://markets.newyorkfed.org/api/pd/list/timeseries.json",
                    "https://markets.newyorkfed.org/api/pd/get/PDSORA-UTSETTOT.json",
                    "https://markets.newyorkfed.org/api/pd/latest/SBN2024.json"):
            code, raw, secs, hdr = http(url, timeout=45)
            r.log("  %s -> %s, %d bytes, %.1f s, last-modified %s | %s" % (url.split("//")[1][:96], code, len(raw), secs, hdr.get("Last-Modified") or hdr.get("last-modified"), raw[:110].decode("utf-8", "replace").replace("\n", " ") if raw[:1] in (b"{", b"[") else ""))
        pd, when = jget("data/settlement-fails.json")
        r.log("data/settlement-fails.json written %s | %s" % (when.strftime("%Y-%m-%d %H:%M") if when else "-", json.dumps({k: (v if not isinstance(v, (dict, list)) else "{%d}" % len(v)) for k, v in pd.items()})[:400]))
        r.kv(**facts)
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
