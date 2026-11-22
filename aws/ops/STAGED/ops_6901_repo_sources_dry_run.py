"""ops 6901 - DRY RUN from the checkout: the repo board's three new source readers against the real sources and bank.

Nothing is written.  aws/lambdas/justhodl-repo/source/repo_sources.py is imported from the checkout and run with a
`put` that only records:
  1. the dealer survey rate workbook parsed (count, span, the days of known crises);
  2. the two tri-party workbooks parsed and compared with the 81 banked haircut series - which banked ids the parse
     reproduces, and for the months both workbooks cover (December 2024 - October 2025) which workbook the bank's
     numbers came from; then refresh_haircuts() as the engine will run it (not forced) and forced;
  3. DTCC's moved fails file parsed and compared with the banked history (overlap equal, days added, no gap).
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import sys
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-repo" / "source"))
LIVE = "justhodl-dashboard-live"


def fetch(url, headers=None):
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers or {"User-Agent": "Mozilla/5.0 JustHodl research"}), timeout=90) as resp:
        return resp.read()


def compare(banked, source):
    """(equal, different, only in bank, only in source) over two {day: value} maps."""
    both = set(banked) & set(source)
    eq = sum(1 for d in both if banked[d] == source[d])
    return eq, len(both) - eq, len(set(banked) - set(source)), len(set(source) - set(banked))


def selftest():
    assert compare({"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 5, "d": 4}) == (1, 1, 1, 1)


def main():
    selftest()
    import boto3
    from botocore.config import Config
    from ops_report import report
    import repo_sources as S
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}, max_pool_connections=32))
    cache = {}

    def read(key):
        if key not in cache:
            try:
                raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
                cache[key] = json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)
            except Exception:  # noqa: BLE001
                cache[key] = None
        return cache[key]

    fetched = {}

    def get(url):
        if url not in fetched:
            fetched[url] = fetch(url)
        return fetched[url]

    with report("ops_6901_repo_sources_dry_run") as r:
        now = datetime.now(timezone.utc)
        r.heading("ops 6901 - the repo board's new source readers, dry run from the checkout, %s" % now.isoformat(timespec="minutes"))
        facts = {}

        r.section("1. Primary dealer survey rate, 1998-2018")
        puts = []
        out = S.bank_survey_rate(read, get, lambda k, d: puts.append((k, d)), now.isoformat(timespec="seconds"))
        r.log("bank_survey_rate: %s | would write %s" % (json.dumps(out), [(k, len(d["observations"]), d["source_sha256"][:16]) for k, d in puts]))
        pairs = S.survey_rate(get(S.SURVEY_URL))
        by = dict(pairs)
        r.log("%d observations %s to %s | min %.2f max %.2f | years %s" % (len(pairs), pairs[0][0], pairs[-1][0], min(by.values()), max(by.values()),
                                                                         " ".join("%s:%d" % kv for kv in sorted(Counter(d[:4] for d in by).items()))))
        for day in ("1998-08-17", "1998-10-08", "1998-12-31", "1999-12-31", "2001-09-10", "2001-09-13", "2001-09-18", "2003-06-25", "2007-08-09", "2007-08-20", "2008-03-14", "2008-03-17",
                    "2008-09-12", "2008-09-15", "2008-09-16", "2008-09-17", "2008-09-18", "2008-09-30", "2008-10-01", "2008-12-16", "2008-12-31", "2010-11-30", "2014-08-22", "2016-12-30", "2018-02-28"):
            r.log("  %s  %s" % (day, by.get(day)))
        facts["survey_n"] = len(pairs)

        r.section("2. Tri-party haircuts")
        old_rows, new_rows = S.triparty_rows(get(S.TRIPARTY_OLD)), S.triparty_rows(get(S.TRIPARTY_NOW))
        od, nd = sorted({x["date"] for x in old_rows}), sorted({x["date"] for x in new_rows})
        r.log("old workbook: %d rows, %d snapshots %s to %s, %d groups | new workbook: %d rows, %d snapshots %s to %s, %d groups" % (
            len(old_rows), len(od), od[0], od[-1], len({x["group"] for x in old_rows}), len(new_rows), len(nd), nd[0], nd[-1], len({x["group"] for x in new_rows})))
        shared = sorted(set(od) & set(nd))
        r.log("snapshots in both workbooks: %d (%s ... %s) | last 6 of the new one: %s" % (len(shared), shared[0] if shared else None, shared[-1] if shared else None, nd[-6:]))
        r.log("groups only in the old: %s | only in the new: %s" % (sorted({x["group"] for x in old_rows} - {x["group"] for x in new_rows}), sorted({x["group"] for x in new_rows} - {x["group"] for x in old_rows})))
        index = read(S.HAIRCUT_INDEX) or {}
        banked_ids = [e["id"] for e in index.get("series") or []]
        only_old, only_new, both = S.haircut_series(old_rows, []), S.haircut_series([], new_rows), S.haircut_series(old_rows, new_rows)
        r.log("banked ids %d | parse makes %d ids | banked ids the parse does not make: %s | ids the parse makes that are not banked: %s" % (
            len(banked_ids), len(both), sorted(set(banked_ids) - set(both)), sorted(set(both) - set(banked_ids))))
        tot = {"old": [0, 0, 0, 0], "new": [0, 0, 0, 0], "both": [0, 0, 0, 0]}
        overlap = {"equals_old": 0, "equals_new": 0, "equals_both": 0, "equals_neither": 0, "samples": []}
        titles = 0
        for sid in banked_ids:
            doc = read(S.HAIRCUT_PREFIX + sid + ".json") or {}
            banked = {o["date"]: o["value"] for o in doc.get("observations") or []}
            titles += int(doc.get("title") == (both.get(sid) or {}).get("title"))
            for name, src in (("old", only_old), ("new", only_new), ("both", both)):
                c = compare(banked, (src.get(sid) or {}).get("observations") or {})
                for i in range(4):
                    tot[name][i] += c[i]
            o, n = (only_old.get(sid) or {}).get("observations") or {}, (only_new.get(sid) or {}).get("observations") or {}
            for d in set(banked) & set(o) & set(n):
                key = "equals_both" if banked[d] == o[d] == n[d] else "equals_old" if banked[d] == o[d] else "equals_new" if banked[d] == n[d] else "equals_neither"
                overlap[key] += 1
                if key != "equals_both" and len(overlap["samples"]) < 10:
                    overlap["samples"].append([sid, d, banked[d], o[d], n[d]])
        for name in ("old", "new", "both"):
            r.log("bank against %-4s workbook(s): %d observations equal, %d different, %d only in the bank, %d only in the source" % ((name,) + tuple(tot[name])))
        r.log("months both workbooks cover, per banked observation: %s" % json.dumps({k: v for k, v in overlap.items() if k != "samples"}))
        for s in overlap["samples"]:
            r.log("  %s %s: bank %s | old workbook %s | new workbook %s" % tuple(s))
        r.log("banked titles the parse reproduces: %d of %d" % (titles, len(banked_ids)))
        for label, kw in (("as the engine runs it", {}), ("forced (both workbooks)", {"force": True})):
            puts = []
            st = S.refresh_haircuts(read, get, lambda k, d: puts.append(k), now.isoformat(timespec="seconds"), now.date(), dry_run=True, **kw)
            r.log("refresh_haircuts dry run, %s: %s" % (label, json.dumps({k: v for k, v in st.items() if k != "revised_sample"})))
            for s in st.get("revised_sample") or []:
                r.log("    would revise %s %s: %s -> %s" % tuple(s))
            assert not puts, "a dry run wrote"
            facts["haircut_%s" % ("forced" if kw else "normal")] = "%s written, +%s obs, %s revised" % (st.get("written"), st.get("observations_added"), st.get("observations_revised"))
        for sid in ("us-treasuries-excluding-strips--median_haircut", "agency-mbs--median_haircut", "equities--median_haircut", "corporates-non-investment-grade--median_haircut", "total--collateral_value", "sponsored-gc--collateral_value"):
            obs = (both.get(sid) or {}).get("observations") or {}
            r.log("  %s: %s" % (sid, ", ".join("%s %s" % (d, obs[d]) for d in sorted(obs)[-5:])))

        r.section("3. DTCC daily fails")
        for base in S.DTCC_FAILS_URLS:
            try:
                text = fetch(S.dtcc_url(base, int(time.time() * 1000)), S.DTCC_CLIENT).decode("utf-8", "replace")
                tsy, agy = S.dtcc_fails(text)
            except Exception as e:  # noqa: BLE001
                r.log("%s: %s %s" % (base, type(e).__name__, str(e)[:120]))
                continue
            days = sorted(tsy)
            r.log("%s: %d days %s to %s | last three Treasury: %s" % (base, len(days), days[0], days[-1], ", ".join("%s $%.1fB" % (d, tsy[d] / 1e9) for d in days[-3:])))
            for name, src in (("treasury", tsy), ("agency", agy)):
                doc = read("data/warm/dtcc-fails/%s.json" % name) or {}
                banked = {o["date"]: o["value"] for o in doc.get("observations") or []}
                eq, diff, only_bank, only_src = compare(banked, src)
                merged = sorted(set(banked) | set(src))
                gaps = [(a, b) for a, b in zip(merged, merged[1:]) if (datetime.fromisoformat(b) - datetime.fromisoformat(a)).days > 5]
                r.log("  %s: bank %d days to %s | shared days equal %d, different %d | new days %d | merged %d days %s to %s | gaps longer than 5 days: %s" % (
                    name, len(banked), max(banked) if banked else None, eq, diff, only_src, len(merged), merged[0], merged[-1], gaps))
                facts["dtcc_%s_new_days" % name] = only_src
            break
        r.kv(**facts)
        r.ok("dry run; nothing written")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
