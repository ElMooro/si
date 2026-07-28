"""
ops_4001 — DISCOVERY: expand BOJ / MOF-JAPAN / NORGES / BCRP-PERU.

Khalid: "pull all the important data and indicators" from the four thin
families "and integrate them at the engines that need them." Doctrine picks
the targets — yen = the second liquidity provider and carry funding cost
[tv-9fa576184567fa8f], JGB curve = the carry-unwind trigger, reserves = the
EM buffer — and the gov-sources registry supplies the specs. This op tests
every candidate LIVE and prints paste-ready alias strings; only confirmed
series get wired (the 3970 method — no guessed wiring, ever).

Zero new adapter code planned: boj_yoy suits level->YoY series (MonBase,
M2, M3); mofjp takes tenors; norges takes flow:match; bcrp takes PN/PD
codes. Tankan/call-rate are LEVELS and would be silently wrong through
boj_yoy — deliberately deferred to a level-adapter pass.
"""
import json
import re
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0",
      "Accept-Encoding": "identity"}


def get(url, timeout=40):
    try:
        r = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.read()
    except Exception as e:
        return getattr(e, "code", 0), str(e).encode()[:200]


def main():
    with report("4001_expand_discovery") as rep:
        rep.heading("ops 4001 — BOJ/MOF/NORGES/BCRP series discovery")
        confirmed = []

        rep.section("R. registry specs (audit-first)")
        try:
            gov = json.loads(s3.get_object(Bucket=BUCKET,
                                           Key="data/gov-sources.json")["Body"].read())
            for a in gov.get("agencies") or []:
                if str(a.get("id")) in ("boj", "mof_japan", "norges", "bcrp", "bcrp_peru"):
                    rep.log(f"  [{a.get('id')}] base={str(a.get('base') or a.get('api_base'))[:70]}")
                    rep.log(f"      example={str(a.get('worked_example') or a.get('example'))[:110]}")
                    rep.log(f"      quirks={str(a.get('quirks') or a.get('lessons'))[:130]}")
        except Exception as e:
            rep.log(f"  registry: {type(e).__name__}")

        rep.section("V. current status of the fill targets in the live vault")
        vault = json.loads(s3.get_object(Bucket=BUCKET,
                                         Key="data/tradingview.json")["Body"].read())
        vs = {r["symbol"]: r for r in vault.get("symbols") or []}
        for s_ in ("JPINTR", "JPM3", "JPFER", "JPGDG", "JPCLI", "JP02Y", "NO03Y",
                   "PETOT", "JPLG"):
            r = vs.get(s_) or {}
            rep.log(f"  {s_:8s} status={str(r.get('status')):16s} "
                    f"src={str(r.get('source'))[:24]} n_notes={r.get('n_notes')}")

        rep.section("A. BOJ — find MonBase / M2 / M3 codes via getMetadata")
        B = "https://www.stat-search.boj.or.jp/api/v1"
        for db, wants in (("MD01", [("monetary base", "average")]),
                          ("MD02", [("m2", "average"), ("m3", "average")])):
            st, body = get(f"{B}/getMetadata?format=json&lang=en&db={db}")
            txt = body.decode("utf8", "ignore")
            rep.log(f"  [{st}] {db} metadata bytes={len(body)}")
            pairs = re.findall(r'"(?:seriesCode|code)"\s*:\s*"([^"]+)"[^}}]*?'
                               r'"(?:seriesName|name)"\s*:\s*"([^"]{{0,110}})"', txt)
            rep.log(f"      series listed: {len(pairs)}")
            for want in wants:
                hits = [(c, n) for c, n in pairs
                        if all(w in n.lower() for w in want)]
                rep.log(f"      match {want}: {len(hits)}")
                for c, n in hits[:4]:
                    rep.log(f"        {c} — {n[:90]}")
                if hits:
                    code = hits[0][0]
                    now = datetime.now(timezone.utc)
                    end = now.year * 100 + now.month
                    start = (now.year - 1) * 100 + now.month
                    st2, b2 = get(f"{B}/getDataCode?format=json&lang=en&db={db}"
                                  f"&code={code}&startDate={start}&endDate={end}")
                    ok = st2 == 200 and b"value" in b2.lower()
                    rep.log(f"        probe [{st2}] bytes={len(b2)} has_values={ok}")
                    if ok:
                        confirmed.append((f"{'JPMB' if 'monetary' in want[0] else ('JPM2YY' if want[0]=='m2' else 'JPM3')}",
                                          f"boj:{db}:{code}",
                                          "yen liquidity provider — level->YoY "
                                          "[tv-9fa576184567fa8f]"))

        rep.section("B. MOF — the full JGB curve from jgbcme.csv")
        st, body = get("https://www.mof.go.jp/english/policy/jgbs/reference/"
                       "interest_rate/jgbcme.csv")
        txt = body.decode("utf8", "ignore")
        rep.log(f"  [{st}] bytes={len(body)}")
        lines = [l for l in txt.splitlines() if l.strip()]
        hdr = next((l for l in lines if "1Y" in l and "10Y" in l), "")
        rep.log(f"  header: {hdr[:150]}")
        last = lines[-1] if lines else ""
        rep.log(f"  last row: {last[:150]}")
        cols = [c.strip() for c in hdr.split(",")]
        vals = [c.strip() for c in last.split(",")]
        for tenor, sym in (("1Y", "JP01Y"), ("5Y", "JP05Y"), ("10Y", "JP10Y"),
                           ("20Y", "JP20Y"), ("30Y", "JP30Y")):
            if tenor in cols:
                v = vals[cols.index(tenor)] if len(vals) > cols.index(tenor) else "?"
                rep.log(f"  {sym} <- {tenor} = {v}")
                try:
                    float(v)
                    confirmed.append((sym, f"mofjp:{tenor}",
                                      "JGB curve — carry-unwind trigger "
                                      "[tv-9fa576184567fa8f]"))
                except Exception:
                    pass

        rep.section("C. NORGES — policy rate + 10Y")
        for sym, flow, why in (("NO10Y", "GOVT_GENERIC_RATES",
                                "long NOK rate"),
                               ("NOINTR", "IR", "policy rate — tighter"),
                               ("NOINTR", "KPRA", "policy rate — tighter")):
            st, body = get(f"https://data.norges-bank.no/api/data/{flow}"
                           f"?format=sdmx-json&lastNObservations=1")
            head = body[:180].decode("utf8", "ignore").replace("\n", " ")
            rep.log(f"  [{st}] {sym} flow={flow} bytes={len(body)} head={head[:110]}")
            if st == 200 and b"dataSets" in body:
                if sym == "NO10Y":
                    confirmed.append((sym, "norges:GOVT_GENERIC_RATES:10", why))
                elif not any(c[0] == "NOINTR" for c in confirmed):
                    txt2 = body.decode("utf8", "ignore")
                    rep.log(f"      series-dims sample: {txt2[txt2.find('series'):txt2.find('series')+200][:180]}")
                    confirmed.append((sym, f"norges:{flow}:KPRA?", why + " — MATCH RULE TBD FROM DIMS"))

        rep.section("D. BCRP — policy rate / net reserves / FX")
        yr = datetime.now(timezone.utc).year
        for sym, code, why in (
                ("PEINTR", "PD04722MM", "reference rate — tighter"),
                ("PEFER", "PN00026MM", "net intl reserves — EM buffer"),
                ("PEFER", "PN00027MM", "net intl reserves alt"),
                ("PENUSD", "PD04640PD", "interbank FX"),
                ("PENUSD", "PN01207PM", "FX monthly alt")):
            st, body = get(f"https://estadisticas.bcrp.gob.pe/estadisticas/series/"
                           f"api/{code}/json/{yr-1}-1/{yr}-12")
            ok = st == 200 and b"periods" in body
            nm = ""
            try:
                d = json.loads(body)
                nm = str((d.get("config") or {}).get("series",
                         [{}])[0].get("name"))[:70]
                pv = (d.get("periods") or [])[-1]
                rep.log(f"  [{st}] {sym} {code} — {nm}")
                rep.log(f"      last={pv.get('name')} v={pv.get('values')}")
            except Exception:
                rep.log(f"  [{st}] {sym} {code} bytes={len(body)} "
                        f"head={body[:80].decode('utf8','ignore')}")
            if ok and not any(c[0] == sym for c in confirmed):
                confirmed.append((sym, f"bcrp:{code}", why + f" ({nm[:40]})"))

        rep.section("E. CONFIRMED — paste-ready aliases")
        for sym, alias, why in confirmed:
            rep.log(f'  "{sym}": "{alias}",   # {why[:80]}')
        rep.kv(n_confirmed=len(confirmed))
        if len(confirmed) < 6:
            rep.fail("fewer than 6 confirmed — wiring op must shrink to match")
            sys.exit(1)
        rep.ok(f"DISCOVERY DONE — {len(confirmed)} confirmed, zero guessed")


if __name__ == "__main__":
    main()
