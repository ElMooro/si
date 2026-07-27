"""
ops_3984 — PURE VERIFIER. No invoke, no pending-branch, nothing long.

Pass-3 of ops 3983 failed with no report — the third silent kill of this
arc. But the thing being verified is independent of that: the v1.5 engine
was invoked at 20:33 under enforced 900s/3008MB and, at fleet pace, wrote
data/data-census.json around 20:45. This op only READS and GATES:

  * artifact marker/freshness (is it v1.5, when did it write)
  * totals, truncation, metric directory
  * by_source — Khalid's locating scheme, with FRED/US10Y as the acid test
  * detectors, schedule state, page v3 at the edge

Every line of the body runs inside an explicit try/except that writes the
full traceback INTO the report before exiting — whatever killed pass-3
cannot kill this one silently.
"""
import json
import sys
import time
import traceback
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
sch = boto3.client("scheduler", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT = "data/data-census.json"
MARK = "data-census v1.6 ops3985 deterministic"
PAGE = "https://justhodl.ai/data-census.html"
PAGE_MARKS = ["v3-ops3982", "Browse by data source", "pulled from",
              "Metric directory", "JPEXPYY"]


def body(rep, checks):
    now = datetime.now(timezone.utc)

    rep.section("A. the artifact (short poll for the v1.6 write)")
    doc = None
    for i in range(12):
        doc = json.loads(s3.get_object(Bucket=BUCKET, Key=OUT)["Body"].read())
        if doc.get("marker") == MARK:
            break
        rep.log(f"  [{i}] marker={str(doc.get('marker'))[:40]} — waiting 30s")
        time.sleep(30)
    gen = doc.get("generated_at")
    rep.kv(marker=doc.get("marker"), generated_at=gen,
           elapsed_s=doc.get("elapsed_s"))
    age_min = None
    try:
        age_min = round((now - datetime.fromisoformat(gen)).total_seconds() / 60, 1)
    except Exception:
        pass
    rep.kv(age_min=age_min)
    checks.append(("artifact is v1.5", doc.get("marker") == MARK))
    checks.append(("written after the v1.6 invoke",
                   bool(gen and gen >= "2026-07-27T21:1")))

    rep.section("B. totals + directory")
    t = doc.get("totals") or {}
    rep.kv(**t)
    md = doc.get("metric_directory") or []
    full = [m for m in md if m.get("name") and m.get("pulled_from") and m.get("engine")]
    keyed = any("[" in (m.get("path") or "") and "[0]" not in (m.get("path") or "")
                for m in md)
    rep.kv(metric_directory_n=len(md), fully_attributed=len(full), keyed_paths=keyed)
    for m in md[:6]:
        rep.log(f"  DIR {str(m.get('name'))[:36]:36s} = {m.get('value')} "
                f"live={m.get('live')} from={str(m.get('pulled_from'))[:28]} "
                f"eng={str(m.get('engine'))[:30]}")
    checks += [
        (">=100 artifacts walked", (t.get("artifacts") or 0) >= 100),
        (">=2000 scalar paths", (t.get("scalar_paths") or 0) >= 2000),
        ("metric directory >=50", len(md) >= 50),
        (">=50 fully attributed (name+source+engine)", len(full) >= 50),
        ("keyed-list walk landed", keyed),
    ]

    rep.section("C. by_source — the locating scheme")
    bs = doc.get("by_source") or {}
    rep.kv(source_families=len(bs),
           families=json.dumps({k: (v or {}).get("n")
                                for k, v in list(bs.items())[:14]}))
    fred = ((bs.get("FRED") or {}).get("metrics")) or []
    for m in fred[:6]:
        rep.log(f"  FRED: {m['name']} = {m['value']} live={m['live']} "
                f"pulled from {str(m['pulled_from'])[:32]} through {m['engine']}")
    us10 = next((m for m in fred if "US10" in str(m.get("name")).upper()
                 or "DGS10" in str(m.get("pulled_from")).upper()), None)
    rep.kv(fred_us10y=json.dumps(us10) if us10 else None)
    checks += [
        ("by_source >=5 families", len(bs) >= 5),
        ("FRED >=40 metrics", len(fred) >= 40),
        ("US 10Y locatable under FRED", bool(us10)),
    ]

    rep.section("D. detectors (first run with a fed keyed walk)")
    mis = doc.get("mislabel_candidates") or []
    con = doc.get("measure_conflicts") or []
    gap = doc.get("gap_fill_candidates") or []
    rep.kv(mislabels=len(mis), conflicts=len(con), gap_candidates=len(gap))
    for m in mis[:5]:
        rep.log(f"  MISLABEL v={m['value']} {m['countries_claimed']} "
                f"{[p['artifact'].split('/')[-1]+':'+p['path'][:40] for p in m['paths'][:2]]}")
    for g in gap[:6]:
        rep.log(f"  GAP {g['symbol']:9s} notes={g['n_notes']} -> "
                f"{[(c.get('name') or c['path'])[:36] for c in g['candidates'][:2]]}")
    checks.append(("detectors present", all(k in doc for k in
                   ("mislabel_candidates", "measure_conflicts",
                    "gap_fill_candidates"))))

    rep.section("E. schedule + page")
    st = sch.get_schedule(Name="data-census-daily")
    rep.kv(schedule_state=st.get("State"), cron=st.get("ScheduleExpression"))
    checks.append(("schedule ENABLED", st.get("State") == "ENABLED"))
    html, got = "", 0
    for i in range(6):
        try:
            req = urllib.request.Request(PAGE + f"?cb={int(time.time())}",
                                         headers={"User-Agent": "Mozilla/5.0",
                                                  "Cache-Control": "no-cache"})
            html = urllib.request.urlopen(req, timeout=25).read().decode("utf8", "ignore")
            got = sum(1 for m in PAGE_MARKS if m in html)
            if got == len(PAGE_MARKS):
                break
        except Exception:
            pass
        time.sleep(15)
    rep.kv(page_bytes=len(html), page_markers=f"{got}/{len(PAGE_MARKS)}")
    checks.append(("page v3 live at edge", got == len(PAGE_MARKS)))
    return doc, t, md, full, bs, fred, mis, con, gap


def main():
    with report("3984_census_verify") as rep:
        rep.heading("ops 3984 — pure verifier: is the census finally whole?")
        checks = []
        try:
            doc, t, md, full, bs, fred, mis, con, gap = body(rep, checks)
        except Exception:
            rep.fail("EXCEPTION IN BODY — full traceback follows")
            for line in traceback.format_exc().splitlines():
                rep.log(f"  {line[:200]}")
            sys.exit(1)
        failed = [l for l, ok in checks if not ok]
        for l, ok in checks:
            (rep.ok if ok else rep.fail)(f"  {l}")
        if failed:
            rep.fail(f"FAILED: {failed}")
            sys.exit(1)
        rep.ok(f"PASS_ALL — census v1.5 whole: {t.get('artifacts')} artifacts / "
               f"{t.get('scalar_paths')} values / truncated "
               f"{t.get('artifacts_truncated_by_time_budget')}; directory {len(md)} "
               f"({len(full)} attributed); {len(bs)} source families, FRED {len(fred)}; "
               f"mislabels {len(mis)}, conflicts {len(con)}, gaps {len(gap)}")


if __name__ == "__main__":
    main()
