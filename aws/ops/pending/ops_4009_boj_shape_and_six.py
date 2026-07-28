"""
ops_4009 — DISCOVERY: BOJ response-shape + six-country expansion.

Khalid: grow BOJ past JPLG (M-aggregates + Tankan + call rate) and add
gov/CB/finance-ministry coverage for liquidity, risk, manufacturing and
industrial output: Finland, Spain, Italy, Switzerland, Taiwan, S. Korea,
Brazil. Audit-first result: FRED's OECD-MEI family already carries IP and
long rates for five of the six through the EXISTING yoy:/fred_alias:
adapters — confirmed here keylessly via fredgraph.csv. Only Brazil's BCB
needs one small fetcher, and BOJ needs the response shape this op dumps RAW.
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
CODE_RX = re.compile(r"^[A-Z0-9@'/._-]{4,26}$")


def get(url, timeout=45):
    try:
        r = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.read()
    except Exception as e:
        return getattr(e, "code", 0), str(e).encode()[:200]


def pairs_from(obj, out):
    if isinstance(obj, dict):
        code = name = None
        for v in obj.values():
            if isinstance(v, str):
                if CODE_RX.match(v) and code is None:
                    code = v
                elif len(v) > 12 and name is None:
                    name = v
        if code and name:
            out.append((code, name))
        for v in obj.values():
            pairs_from(v, out)
    elif isinstance(obj, list):
        for v in obj:
            pairs_from(v, out)


def fredgraph(series):
    st, body = get(f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series}")
    txt = body.decode("utf8", "ignore")
    rows = [l.split(",") for l in txt.strip().splitlines()[1:] if "," in l]
    rows = [(d, v) for d, v in rows if v not in (".", "")]
    return st, (rows[-1] if rows else None), len(rows)


def main():
    with report("4009_boj_shape_and_six") as rep:
        rep.heading("ops 4009 — BOJ shape + FI/ES/IT/CH/TW/KR/BR discovery")
        confirmed, deferred = [], []
        now = datetime.now(timezone.utc)
        B = "https://www.stat-search.boj.or.jp/api/v1"

        rep.section("V. collision check — candidate symbols in the live vault")
        vault = json.loads(s3.get_object(Bucket=BUCKET,
                                         Key="data/tradingview.json")["Body"].read())
        vs = {r["symbol"]: r for r in vault.get("symbols") or []}
        cands = ("JPMB", "JPM2YY", "JPM3", "JPTANKAN", "JPCALLR", "FIIPYY",
                 "ESIPYY", "ITIPYY", "CHIPYY", "KRIPYY", "BRIPYY", "BRINTR",
                 "CHINTR", "KRINTR", "IT10Y", "ES10Y", "FI10Y", "CH10Y",
                 "KR10Y", "TWINTR")
        for c in cands:
            r = vs.get(c)
            if r:
                rep.log(f"  {c:9s} EXISTS status={r.get('status')} "
                        f"src={str(r.get('source'))[:18]}")

        rep.section("A. BOJ getDataCode RAW — the shape work")
        end = now.year * 100 + now.month
        start = (now.year - 1) * 100 + max(1, now.month - 2)
        st, body = get(f"{B}/getDataCode?format=json&lang=en&db=MD01"
                       f"&code=MABS1AN11&startDate={start}&endDate={end}")
        rep.log(f"  MD01/MABS1AN11 [{st}] bytes={len(body)}")
        rep.log("  HEAD: " + body[:400].decode("utf8", "ignore").replace("\n", " "))
        rep.log("  TAIL: " + body[-300:].decode("utf8", "ignore").replace("\n", " "))
        try:
            d = json.loads(body)
            def walk(o, pre="", depth=0):
                if depth > 4:
                    return
                if isinstance(o, dict):
                    for k, v in list(o.items())[:8]:
                        rep.log(f"    {pre}.{k}: {type(v).__name__} "
                                f"{str(v)[:60] if not isinstance(v,(dict,list)) else ('n='+str(len(v)))}")
                        walk(v, f"{pre}.{k}", depth + 1)
                elif isinstance(o, list) and o:
                    rep.log(f"    {pre}[0]: {str(o[0])[:120]}")
                    walk(o[0], f"{pre}[0]", depth + 1)
            walk(d)
        except Exception as e:
            rep.log(f"  json: {type(e).__name__}")

        rep.section("A2. BOJ Tankan + call-rate code discovery")
        for db, wants in (("CO", ["business conditions", "tankan", "large"]),
                          ("FM08", ["call rate", "uncollateralized"]),
                          ("FM01", ["call rate"]), ("IR01", ["call"])):
            st, body = get(f"{B}/getMetadata?format=json&lang=en&db={db}")
            try:
                ps = []
                pairs_from(json.loads(body), ps)
            except Exception:
                ps = []
            hits = [(c, n) for c, n in ps
                    if any(w in n.lower() for w in wants)][:5]
            rep.log(f"  [{st}] {db}: pairs={len(ps)} hits={len(hits)}")
            for c, n in hits[:4]:
                rep.log(f"      {c} — {n[:92]}")

        rep.section("B. FRED MEI — keyless fredgraph confirms")
        F = [("FIIPYY", "yoy:FINPROINDMISMEI", "Finland IP", 60, 140),
             ("ESIPYY", "yoy:ESPPROINDMISMEI", "Spain IP", 60, 140),
             ("ITIPYY", "yoy:ITAPROINDMISMEI", "Italy IP", 60, 140),
             ("CHIPYY", "yoy:CHEPROINDMISMEI", "Swiss IP", 60, 160),
             ("KRIPYY", "yoy:KORPROINDMISMEI", "Korea IP", 60, 180),
             ("BRIPYY", "yoy:BRAPROINDMISMEI", "Brazil IP", 60, 160),
             ("IT10Y", "fred_alias:IRLTLT01ITM156N", "Italy 10Y", 1, 8),
             ("ES10Y", "fred_alias:IRLTLT01ESM156N", "Spain 10Y", 1, 8),
             ("FI10Y", "fred_alias:IRLTLT01FIM156N", "Finland 10Y", 1, 8),
             ("CH10Y", "fred_alias:IRLTLT01CHM156N", "Swiss 10Y", 0, 4),
             ("KR10Y", "fred_alias:IRLTLT01KRM156N", "Korea 10Y", 1, 8),
             ("CHINTR", "fred_alias:IRSTCI01CHM156N", "Swiss immediate rate",
              -1, 4),
             ("KRINTR", "fred_alias:INTDSRKRM193N", "Korea discount", 0, 8)]
        for sym, alias, nm, lo, hi in F:
            series = alias.split(":")[1]
            st, last, n = fredgraph(series)
            rep.log(f"  {sym:7s} {series:20s} [{st}] n={n} last={last}")
            if st == 200 and last and n > 24:
                try:
                    v = float(last[1])
                    fresh = last[0] >= f"{now.year - 1}-"
                    ok = (lo <= v <= hi) and fresh
                    rep.log(f"      v={v} fresh={fresh} sane={ok}")
                    if ok:
                        confirmed.append((sym, alias, nm + f" (FRED {series})"))
                    else:
                        deferred.append((sym, f"{series} v={v} fresh={fresh}"))
                except Exception:
                    deferred.append((sym, "parse"))
            else:
                deferred.append((sym, f"st={st} n={n}"))

        rep.section("C. BCB Brazil — SGS open API")
        for sym, code, nm, lo, hi in (
                ("BRINTR", "432", "Selic target", 5, 20),
                ("BRFER", "13621", "intl reserves USD mn", 200000, 500000),
                ("BRLUSD", "1", "USD/BRL", 3, 8)):
            st, body = get(f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{code}"
                           f"/dados/ultimos/2?formato=json")
            try:
                d = json.loads(body)
                last = d[-1]
                v = float(str(last.get("valor")).replace(",", "."))
                rep.log(f"  {sym:7s} sgs.{code} [{st}] {last.get('data')} v={v}")
                if lo <= v <= hi:
                    confirmed.append((sym, f"bcb:{code}", nm))
                else:
                    deferred.append((sym, f"v={v} out of [{lo},{hi}]"))
            except Exception as e:
                rep.log(f"  {sym} sgs.{code} [{st}] {type(e).__name__} "
                        f"{body[:90].decode('utf8','ignore')}")
                deferred.append((sym, f"st={st}"))

        rep.section("E. PASTE-READY + DEFERRED")
        for sym, alias, why in confirmed:
            rep.log(f'  "{sym}": "{alias}",  # {why[:70]}')
        for sym, why in deferred:
            rep.log(f"  DEFER {sym}: {why}")
        rep.kv(n_confirmed=len(confirmed), n_deferred=len(deferred))
        if len(confirmed) < 8:
            rep.fail("under 8 — read the evidence before wiring")
            sys.exit(1)
        rep.ok(f"DISCOVERY DONE — {len(confirmed)} confirmed; BOJ shape dumped "
               f"for the adapter design")


if __name__ == "__main__":
    main()
