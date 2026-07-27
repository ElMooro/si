"""
ops_3953 — the direct route to JPLG's series code: /getMetadata for db=MD11
("Deposits, Vault Cash, and Loans and Bills Discounted" — the loans db per
the manual's now-readable table) -> full series list -> grep the YoY loans
series (unit %, 'change from the previous year' / total banks) -> CONFIRM
top candidates via getDataCode with real values. Also getDataLayer with the
real param name (layer) + db. Report the exact code; wire next ops.
"""
import json, re, sys, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0",
      "Accept-Encoding": "identity"}
BASE = "https://www.stat-search.boj.or.jp/api/v1"


def fetch_json(url, timeout=30):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def main():
    with report("3953_boj_md11_series") as rep:
        rep.heading("ops 3953 — MD11 metadata -> the JPLG series code")

        rep.section("1. getMetadata db=MD11 (+ param fallbacks)")
        meta = None
        for q in ("format=json&lang=en&db=MD11",
                  "format=json&lang=en&db=MD11&startPosition=",
                  "format=json&lang=en&code=MD11"):
            try:
                d = fetch_json(f"{BASE}/getMetadata?{q}", 40)
                rep.log(f"  {q}: STATUS={d.get('STATUS')} "
                        f"msg={str(d.get('MESSAGE'))[:60]} "
                        f"n={len(d.get('RESULTSET') or [])}")
                if d.get("STATUS") == 200 and d.get("RESULTSET"):
                    meta = d
                    break
            except Exception as e:
                rep.log(f"  {q}: {str(e)[:110]}")

        cands = []
        if meta:
            rs = meta.get("RESULTSET") or []
            rep.section(f"2. MD11 series list ({len(rs)} entries) — loans YoY hunt")
            sample = json.dumps(rs[0], ensure_ascii=False)[:400] if rs else ""
            rep.log(f"  entry shape: {sample}")
            for e in rs:
                blob = json.dumps(e, ensure_ascii=False)
                name = next((v for k, v in e.items()
                             if isinstance(v, str) and "NAME" in k.upper()), "")
                code = next((v for k, v in e.items()
                             if isinstance(v, str) and "CODE" in k.upper()), "")
                if re.search(r"loan", name, re.I) and \
                   re.search(r"year|y/y|yoy|change", blob, re.I):
                    cands.append((code, name))
            for c, n in cands[:15]:
                rep.log(f"  cand {c[:30]:30s} {n[:90]}")
            if not cands:
                loans_any = [(next((v for k, v in e.items()
                                    if isinstance(v, str) and "CODE" in k.upper()), ""),
                              next((v for k, v in e.items()
                                    if isinstance(v, str) and "NAME" in k.upper()), ""))
                             for e in rs
                             if re.search(r"loan", json.dumps(e, ensure_ascii=False), re.I)]
                for c, n in loans_any[:15]:
                    rep.log(f"  loans-any {c[:30]:30s} {n[:90]}")
                cands = loans_any
        else:
            rep.section("2b. getDataLayer with real params (layer + db)")
            for q in ("format=json&lang=en&db=MD11&layer=",
                      "format=json&lang=en&db=MD11&layer=0",
                      "format=json&lang=en&db=MD11&layer=1",
                      "format=json&lang=en&layer=MD11"):
                try:
                    d = fetch_json(f"{BASE}/getDataLayer?{q}", 25)
                    rep.log(f"  {q}: STATUS={d.get('STATUS')} "
                            f"head={json.dumps(d, ensure_ascii=False)[:300]}")
                    if d.get("STATUS") == 200:
                        break
                except Exception as e:
                    rep.log(f"  {q}: {str(e)[:100]}")

        rep.section("3. confirm candidates via getDataCode (real values)")
        confirmed = []
        for code, name in cands[:6]:
            if not code:
                continue
            try:
                d = fetch_json(f"{BASE}/getDataCode?format=json&lang=en&db=MD11"
                               f"&startDate=202501&endDate=202506&code={code}", 25)
                rs0 = (d.get("RESULTSET") or [{}])[0]
                vals = (rs0.get("VALUES") or {})
                line = (f"{code} :: {rs0.get('NAME_OF_TIME_SERIES','')[:80]} :: "
                        f"unit={rs0.get('UNIT')} freq={rs0.get('FREQUENCY')} "
                        f"last={str(vals.get('VALUES'))[-40:]}")
                if rs0.get("VALUES"):
                    confirmed.append((code, rs0))
                    rep.ok("  " + line)
                else:
                    rep.log("  " + line)
            except Exception as e:
                rep.log(f"  {code}: {str(e)[:90]}")
        if confirmed:
            best = confirmed[0]
            rep.ok(f"JPLG CANDIDATE CONFIRMED: {best[0]} — wire next ops")
        rep.ok("MD11 HUNT COMPLETE")
        if False: sys.exit(1)


if __name__ == "__main__":
    main()
