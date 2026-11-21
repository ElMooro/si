"""ops 6896 - ACTING: the repo board rebuilt on the refreshed OFR bank by engine 3.0, read back, and the real numbers
the new desk page is tested against cut out of it.

Before: ops 6892 (the board's venue groups ended 2026-08-04; 19 groups, 1,658 series).  The OFR bank was refreshed by
ops 6895 (442 of 442 level with the source).  The engine's whole handler ran end to end on a stub bucket in its own
suite before this deploy (test_board_build).

  1. The deployed package must carry this checkout's repo_scope.py byte for byte, or nothing is invoked.
  2. One run of justhodl-repo (a repair run; its schedule is daily 06:10 UTC and is unchanged).  If it fails, the
     published board is the previous one - the engine writes data/repo.json last.
  3. Read back: version, groups with their newest dates against ops 6892, the new groups, the derived spreads on the
     days history knows (2019-09-17, 2008-09-15 ...), how long the run took against the ~598 s of the last 14 days.
  4. A fixture of real numbers for the page's tests is printed at the end (public market data, gzip + base64).
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
import zipfile

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
LIVE = "justhodl-dashboard-live"
FN = "justhodl-repo"
BEFORE = {"DVP bilateral cleared": "2026-08-04", "GCF": "2026-08-04", "Reference rates & volumes": "2026-08-04", "Tri-party (TRIV1, ex-Fed)": "2026-08-03",
          "Tri-party (TRI legacy)": "2026-08-03", "Primary dealer FAILS": "2026-07-22", "FICC sponsored repo": "2026-09-18",
          "MMF repo counterparties (OFR)": "2026-06-30", "Derived stress indicators": "2026-11-19", "Rate percentiles (NY Fed)": "2026-11-19"}
CHECKS = (("D_SOFR_ADMIN", "2019-09-17"), ("D_SOFR_EFFR", "2019-09-17"), ("D_SOFR_TAIL", "2019-09-17"), ("D_SOFR_TAIL", "2018-12-31"),
          ("D_SOFR_ADMIN", "2025-10-31"), ("D_EFFR_TARGET", "2008-09-15"), ("D_EFFR_TARGET", "2008-09-30"), ("D_FED_REPO", "2019-09-17"),
          ("D_FED_REPO", "2020-03-17"), ("TEDRATE", "2008-10-10"), ("DFF", "2008-09-15"), ("NYPD-PD_AFtD_T-A", "2008-10-22"), ("SRF_TAKEUP", "2025-10-31"))
FIXTURE_SERIES = ("SOFR", "SOFR_P99", "SOFR_P1", "D_SOFR_TAIL", "D_SOFR_EFFR", "D_SOFR_ADMIN", "D_EFFR_TARGET", "TEDRATE", "D_FED_REPO", "SRF_TAKEUP",
                  "RRPONTSYD", "NYPD-PD_AFtD_T-A", "NYPD-PD_AFtD_AG_MBS-A", "D_GCF_TRI", "D_DVP_TRI", "REPO-TRIV1_AR_OO-P", "FNYR-EFFR-A", "WRESBAL", "WLCFLPCL", "SWPT")
FIXTURE_FROM = {"D_EFFR_TARGET": "2000-01-01", "TEDRATE": "2000-01-01", "WRESBAL": "2006-01-01", "WLCFLPCL": "2006-01-01", "SWPT": "2006-01-01"}


def newest_by_group(groups):
    return {g.get("name"): max((s.get("last") or "" for s in g.get("series") or []), default="") for g in groups}


def cut(dates, values, start):
    rows = [(d, v) for d, v in zip(dates, values) if d >= start]
    return [d for d, _ in rows], [v for _, v in rows]


def selftest():
    assert newest_by_group([{"name": "A", "series": [{"last": "2026-11-19"}, {"last": "2026-08-04"}]}, {"name": "B", "series": []}]) == {"A": "2026-11-19", "B": ""}
    assert cut(["2007-01-01", "2008-01-01"], [1, 2], "2007-06-01") == (["2008-01-01"], [2])
    assert set(BEFORE) and all(len(d) == 10 for _, d in CHECKS)


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    lam = boto3.client("lambda", region_name="us-east-1", config=Config(retries={"max_attempts": 1, "mode": "standard"}, connect_timeout=10, read_timeout=930))
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}, max_pool_connections=32))

    def jget(key):
        try:
            o = s3.get_object(Bucket=LIVE, Key=key)
            raw = o["Body"].read()
            if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
                raw = gzip.decompress(raw)
            return json.loads(raw), o["LastModified"]
        except Exception as e:  # noqa: BLE001
            return {"_unreadable": type(e).__name__}, None

    with report("ops_6896_repo_board_rebuild") as r:
        r.heading("ops 6896 - repo board 3.0 on the refreshed bank, %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))

        r.section("1. What is deployed")
        g = lam.get_function(FunctionName=FN)
        with urllib.request.urlopen(g["Code"]["Location"], timeout=120) as resp:
            z = zipfile.ZipFile(io.BytesIO(resp.read()))
        hits = [x for x in z.namelist() if x == "repo_scope.py" or x.endswith("/repo_scope.py")]
        same = bool(hits) and z.read(hits[0]) == (ROOT / "aws/lambdas/justhodl-repo/source/repo_scope.py").read_bytes()
        r.log("%s updated %s | repo_scope.py as in this checkout: %s | timeout %s s, %s MB" % (
            FN, (g["Configuration"].get("LastModified") or "")[:19], same, g["Configuration"].get("Timeout"), g["Configuration"].get("MemorySize")))
        if not same:
            r.fail("the deployed package is not engine 3.0; nothing was invoked")
            sys.exit(1)

        r.section("2. One run")
        old, when0 = jget("data/repo.json")
        r.log("before: written %s | engine %s | %d groups, %s series" % (when0.strftime("%Y-%m-%d %H:%M") if when0 else "-", old.get("engine_v"), len(old.get("groups") or []), (old.get("counts") or {}).get("series")))
        t0 = time.time()
        res = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", Payload=b"{}")
        raw = res["Payload"].read().decode("utf-8", "replace")
        took = time.time() - t0
        try:
            body = json.loads(json.loads(raw)["body"])
        except Exception:  # noqa: BLE001
            body = {}
        r.log("invoke: status %s%s in %.0f s | %s" % (res.get("StatusCode"), " FunctionError %s" % res.get("FunctionError") if res.get("FunctionError") else "", took, json.dumps(body) if body else raw[:400]))

        r.section("3. Read back")
        board, when = jget("data/repo.json")
        groups = board.get("groups") or []
        rows = {s["id"]: dict(s, group=g_["name"]) for g_ in groups for s in g_.get("series") or []}
        r.log("after: written %s | engine %s | %d groups, %d series | counts %s | diag.dtcc %s" % (
            when.strftime("%Y-%m-%d %H:%M") if when else "-", board.get("engine_v"), len(groups), len(rows), json.dumps(board.get("counts")), json.dumps((board.get("diag") or {}).get("dtcc"))[:200]))
        newest = newest_by_group(groups)
        for g_ in groups:
            name = g_["name"]
            was = BEFORE.get(name) or BEFORE.get("MMF repo counterparties (OFR)" if name == "Money market funds (OFR)" else "")
            r.log("  %-48s %4d series | newest %s%s" % (name, len(g_.get("series") or []), newest[name], (" (was %s)" % was) if was else ""))
        buckets = {}
        for s in rows.values():
            if s["group"] == "FRED repo & SOFR complex":
                buckets[s.get("bucket")] = buckets.get(s.get("bucket"), 0) + 1
        r.log("FRED group buckets: %s" % json.dumps(buckets))
        r.log("skipped %d: %s" % (len(board.get("skipped") or []), json.dumps(board.get("skipped"))[:500]))
        for sid in ("FNYR-SOFR-A", "SOFR", "REPO-TRIV1_AR_OO-P", "REPO-GCF_AR_OO-P", "REPO-DVP_AR_OO-P", "FICC-SPONSORED_REPO_VOL", "NYPD-PD_AFtD_T-A", "MMF-MMF_RP_TOT-M",
                    "MMF-MY_MMF_RP_T_OO-M", "NYPD-PD_SB_T_TOT-A", "DFF", "IOER", "TEDRATE", "WRESBAL", "WLCFLPCL", "SWPT", "DTCC-TREASURY-FAILS",
                    "D_SOFR_EFFR", "D_SOFR_TAIL", "D_SOFR_ADMIN", "D_EFFR_TARGET", "D_FED_REPO"):
            s = rows.get(sid)
            r.log("  %-26s %s" % (sid, ("%s | %s..%s n=%s | last %s | %s" % (s["group"][:34], s.get("first"), s.get("last"), s.get("n_obs"), s.get("last_value"), (s.get("label") or "")[:70])) if s else "NOT ON THE BOARD"))

        hist = {}

        def history(sid):
            if sid not in hist:
                doc, _ = jget("data/repo-history/%s.json" % rows[sid]["sid"]) if sid in rows else ({}, None)
                hist[sid] = (doc.get("dates") or [], doc.get("values") or [])
            return hist[sid]

        r.log("the days history knows:")
        for sid, day in CHECKS:
            dates, values = history(sid)
            at = dict(zip(dates, values)).get(day)
            top = max(zip(values, dates), default=(None, None)) if values else (None, None)
            r.log("  %-18s on %s = %s | series maximum %s on %s | %d observations from %s" % (sid, day, at, top[0], top[1], len(dates), dates[0] if dates else "-"))

        stale = sorted(n for n, was in BEFORE.items() if n in newest and newest[n] <= was and n not in ("Derived stress indicators", "Rate percentiles (NY Fed)"))
        need = ("Money market funds (OFR)", "Primary dealer securities borrowed & lent")
        ok = board.get("engine_v") == "3.0" and not stale and all(n in newest for n in need) and all(s in rows for s in ("D_SOFR_EFFR", "D_SOFR_TAIL", "D_SOFR_ADMIN", "D_EFFR_TARGET", "D_FED_REPO", "DFF"))
        if ok:
            r.ok("the board is engine 3.0, its venue groups moved past their August dates, the new groups and spreads are on it; the run took %.0f s" % took)
        else:
            r.fail("board not as expected: engine %s, groups still at their old date %s, missing groups %s" % (board.get("engine_v"), stale, [n for n in need if n not in newest]))

        r.section("4. Sibling feeds the desk page reads")
        for key in ("data/repo-market.json", "data/nyfed-primary-dealer.json"):
            doc, w = jget(key)
            small = {k: (v if not isinstance(v, (dict, list)) else ("{%d}" % len(v) if isinstance(v, dict) else "[%d]" % len(v))) for k, v in doc.items()}
            r.log("%s written %s | %s" % (key, w.strftime("%Y-%m-%d %H:%M") if w else "-", json.dumps(small)[:900]))
        pd, _ = jget("data/nyfed-primary-dealer.json")
        r.log("primary dealer financing block: %s" % json.dumps(pd.get("financing"))[:900])
        desk, _ = jget("data/repo-market.json")

        r.section("5. Fixture for the page's tests (real numbers; gzip + base64)")
        fixture = {"cut_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "source": "ops 6896, data/repo.json + data/repo-history/* + data/repo-market.json",
                   "board": {"as_of": board.get("as_of"), "engine_v": board.get("engine_v"), "counts": board.get("counts"), "barometer": board.get("barometer"),
                             "groups": [{"name": g_["name"], "n": len(g_.get("series") or [])} for g_ in groups]},
                   "rows": {sid: {k: v for k, v in s.items()} for sid, s in rows.items()
                            if s.get("tier") == 1 or sid in FIXTURE_SERIES or sid.startswith(("FNYR-", "MMF-MMF_RP_w", "FICC-", "SRF_", "D_"))
                            or sid in ("REPO-GCF_AR_OO-P", "REPO-DVP_AR_OO-P", "REPO-GCF_TV_TOT-P", "REPO-DVP_TV_TOT-P", "REPO-DVP_OV_TOT-P", "MMF-MMF_RP_TOT-M", "MMF-MY_MMF_RP_T_OO-M",
                                       "NYPD-PD_AFtR_T-A", "NYPD-PD_AFtD_TOT-A", "NYPD-PD_SB_T_TOT-A", "NYPD-PD_SL_T_TOT-A", "IORB", "DFEDTARU", "DFEDTARL", "RRPONTSYAWARD",
                                       "WTREGEN", "WALCL", "WREPOFOR", "DTCC-TREASURY-FAILS", "EFFR", "OBFR", "DFF", "SOFR30DAYAVG", "SOFR90DAYAVG", "SOFR180DAYAVG")},
                   "history": {}, "desk": desk, "primary_dealer_financing": pd.get("financing")}
        for sid in FIXTURE_SERIES:
            if sid in rows:
                dates, values = history(sid)
                d2, v2 = cut(dates, values, FIXTURE_FROM.get(sid, "1900-01-01"))
                fixture["history"][sid] = {"label": rows[sid].get("label"), "dates": d2, "values": v2}
        blob = base64.b64encode(gzip.compress(json.dumps(fixture, separators=(",", ":")).encode(), 9)).decode()
        r.log("fixture: %d rows, %d histories, %d bytes of base64 in %d lines" % (len(fixture["rows"]), len(fixture["history"]), len(blob), -(-len(blob) // 6000)))
        for i in range(0, len(blob), 6000):
            r.log("FIXTURE %04d %s" % (i // 6000, blob[i:i + 6000]))
        r.kv(engine=board.get("engine_v"), groups=len(groups), series=len(rows), run_seconds=round(took), groups_still_old=len(stale),
             history_written=(board.get("counts") or {}).get("history_written"), history_unchanged=(board.get("counts") or {}).get("history_unchanged"))
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
