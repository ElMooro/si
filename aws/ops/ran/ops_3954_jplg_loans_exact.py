"""
ops_3954 — the EXACT loans series for JPLG: filter MD11's 792 metadata rows
by NAME containing 'Loans and Bills Discounted' (3953's grep matched the db
title in every row; deposits sorted first). Print all with unit/freq; flag
any native '%' YoY variant; confirm the Average-Amounts-Outstanding Total
series via getDataCode with 14 months so YoY is computable. Report the
final code + computed YoY; wire (vault boj: kind + risk-gate carry leg)
next ops.
"""
import json, re, sys, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0", "Accept-Encoding": "identity"}
BASE = "https://www.stat-search.boj.or.jp/api/v1"


def fetch_json(url, timeout=35):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def main():
    with report("3954_jplg_loans_exact") as rep:
        rep.heading("ops 3954 — the exact JPLG loans series in MD11")
        meta = fetch_json(f"{BASE}/getMetadata?format=json&lang=en&db=MD11", 45)
        rs = meta.get("RESULTSET") or []
        rep.kv(n_series=len(rs))

        loans = []
        for e in rs:
            name = str(e.get("NAME_OF_TIME_SERIES", ""))
            code = str(e.get("SERIES_CODE", ""))
            if code and re.search(r"Loans and Bills Discounted|Loans/", name, re.I):
                loans.append((code, name, str(e.get("UNIT", "")),
                              str(e.get("FREQUENCY", ""))))
        # broaden if the db-title text polluted names differently
        if not loans:
            for e in rs:
                name = str(e.get("NAME_OF_TIME_SERIES", ""))
                code = str(e.get("SERIES_CODE", ""))
                if code and "loan" in name.lower() and "deposit" not in name.lower():
                    loans.append((code, name, str(e.get("UNIT", "")),
                                  str(e.get("FREQUENCY", ""))))
        rep.section(f"loans-named series ({len(loans)})")
        for c, n, u, f in loans[:40]:
            rep.log(f"  {c:16s} unit={u[:14]:14s} freq={f[:8]:8s} {n[:95]}")

        pct = [(c, n) for c, n, u, f in loans if "%" in u]
        rep.section("native % variants")
        for c, n in pct[:10]:
            rep.log(f"  {c}  {n[:95]}")

        # rank: Average > End of Period; Total > sector splits; Banking Accounts
        def score(t):
            c, n, u, f = t
            s = 0
            nl = n.lower()
            s += 4 if "average" in nl else 0
            s += 3 if "total" in nl else 0
            s += 2 if "banking account" in nl else 0
            s += 1 if "domestically licensed" in nl else 0
            s -= 2 if "prefecture" in nl else 0
            return -s
        loans.sort(key=score)

        rep.section("confirm top candidates (14 months -> computed YoY)")
        final = None
        for c, n, u, f in loans[:4]:
            try:
                d = fetch_json(f"{BASE}/getDataCode?format=json&lang=en&db=MD11"
                               f"&startDate=202505&endDate=202606&code={c}", 30)
                rs0 = (d.get("RESULTSET") or [{}])[0]
                vals = ((rs0.get("VALUES") or {}).get("VALUES")) or []
                dates = ((rs0.get("VALUES") or {}).get("SURVEY_DATES")) or []
                yoy = None
                if len(vals) >= 13 and vals[-1] and vals[-13]:
                    yoy = round((float(vals[-1]) / float(vals[-13]) - 1) * 100, 2)
                line = (f"{c} :: {rs0.get('NAME_OF_TIME_SERIES','')[:70]} :: "
                        f"unit={rs0.get('UNIT')} n={len(vals)} "
                        f"last_date={dates[-1] if dates else '?'} "
                        f"last={vals[-1] if vals else '?'} computed_yoy={yoy}%")
                if vals:
                    rep.ok("  " + line)
                    if final is None and yoy is not None:
                        final = (c, yoy, rs0.get("NAME_OF_TIME_SERIES", ""))
                else:
                    rep.log("  " + line)
            except Exception as e:
                rep.log(f"  {c}: {str(e)[:90]}")

        if final:
            rep.ok(f"JPLG FINAL: code={final[0]} computed_yoy={final[1]}% "
                   f"({final[2][:70]}) — wire vault boj: + gate carry leg next ops")
        else:
            rep.log("no candidate yielded 13+ monthly values — list above steers")
        rep.ok("DONE")
        if False: sys.exit(1)


if __name__ == "__main__":
    main()
