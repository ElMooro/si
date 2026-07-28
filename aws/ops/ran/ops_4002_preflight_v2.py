"""
ops_4002 — PREFLIGHT v2: fix all four discovery paths with the fleet's own
documented quirks, emit the FINAL paste-ready alias set.

4001 lessons applied: MOF direct serves a 2KB cache-warning stub to the
runner too — use the vault's dual-URL (direct then /gov edge proxy) and
take the last row whose first cell parses as a date and cells as floats
(the 3947 footer rule). BOJ getMetadata returns 200 but not the keys I
regexed — parse the JSON generically: walk for (code,name) pairs by shape.
NORGES IR: dump the structure dimensions so NOINTR gets a real match token.
BCRP: print FULL series names for the reserve candidates; wire only if the
name says 'reservas internacionales'.
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


def main():
    with report("4002_preflight_v2") as rep:
        rep.heading("ops 4002 — preflight v2: final confirmed alias set")
        confirmed = []
        B = "https://www.stat-search.boj.or.jp/api/v1"

        rep.section("A. BOJ — generic (code,name) walk")
        now = datetime.now(timezone.utc)
        end = now.year * 100 + now.month
        start = (now.year - 1) * 100 + (now.month - 2)
        for db, targets in (("MD01", [("monetary base", "JPMB")]),
                            ("MD02", [("m2", "JPM2YY"), ("m3", "JPM3")])):
            st, body = get(f"{B}/getMetadata?format=json&lang=en&db={db}")
            rep.log(f"  [{st}] {db} bytes={len(body)} head={body[:120].decode('utf8','ignore')}")
            try:
                d = json.loads(body)
            except Exception:
                continue
            ps = []
            pairs_from(d, ps)
            rep.log(f"      pairs found: {len(ps)}; sample: {ps[:3]}")
            for want, sym in targets:
                hits = [(c, n) for c, n in ps if want in n.lower()
                        and "average" in n.lower()]
                if not hits:
                    hits = [(c, n) for c, n in ps if want in n.lower()][:6]
                rep.log(f"      '{want}': {len(hits)}")
                for c, n in hits[:5]:
                    rep.log(f"        {c} — {n[:88]}")
                for c, _n in hits[:2]:
                    st2, b2 = get(f"{B}/getDataCode?format=json&lang=en&db={db}"
                                  f"&code={c}&startDate={start}&endDate={end}")
                    vals = re.findall(r'"value"\s*:\s*"?([-0-9.]+)', 
                                      b2.decode("utf8", "ignore"))
                    rep.log(f"        probe {c} [{st2}] values={vals[-3:]}")
                    if st2 == 200 and vals:
                        confirmed.append((sym, f"boj:{db}:{c}",
                                          "yen liquidity [tv-9fa576184567fa8f]"))
                        break

        rep.section("B. MOF — dual-URL + footer rule (the 3947 lesson)")
        base = ("https://www.mof.go.jp/english/policy/jgbs/reference/"
                "interest_rate/jgbcme.csv")
        urls = [base, "https://justhodl-data-proxy.raafouis.workers.dev/gov?u="
                + urllib.parse.quote(base, safe="")]
        rows = None
        for u in urls:
            st, body = get(u)
            txt = body.decode("utf8", "ignore")
            lines = [l for l in txt.splitlines() if l.strip()]
            hdr = next((l for l in lines if l.startswith("Date,") or
                        (",1Y," in l and ",10Y," in l)), None)
            data = []
            if hdr:
                cols = [c.strip() for c in hdr.split(",")]
                for l in lines:
                    cs = [c.strip() for c in l.split(",")]
                    if re.match(r"^\d{4}[./-]\d{1,2}[./-]\d{1,2}", cs[0]):
                        data.append(cs)
            rep.log(f"  [{st}] {u[:60]} bytes={len(body)} data_rows={len(data)}")
            if data:
                rows = (cols, data[-1])
                break
        if rows:
            cols, last = rows
            rep.log(f"  latest {last[0]}: " +
                    " ".join(f"{t}={last[cols.index(t)]}"
                             for t in ("1Y", "5Y", "10Y", "20Y", "30Y")
                             if t in cols))
            for tenor, sym in (("1Y", "JP01Y"), ("5Y", "JP05Y"),
                               ("10Y", "JP10Y"), ("20Y", "JP20Y"),
                               ("30Y", "JP30Y")):
                try:
                    float(last[cols.index(tenor)])
                    confirmed.append((sym, f"mofjp:{tenor}",
                                      "JGB curve — carry-unwind trigger "
                                      "[tv-9fa576184567fa8f]"))
                except Exception:
                    pass

        rep.section("C. NORGES IR — dimension dump for the NOINTR match token")
        st, body = get("https://data.norges-bank.no/api/data/IR"
                       "?format=sdmx-json&lastNObservations=1")
        try:
            d = json.loads(body)
            data = d.get("data") or d
            struct = data.get("structure") or (data.get("structures") or [{}])[0]
            dims = ((struct.get("dimensions") or {}).get("series")) or []
            for dim in dims:
                vals = [(v.get("id"), v.get("name")) for v in dim.get("values") or []]
                rep.log(f"  dim {dim.get('id')}: {vals[:8]}")
            ser = data["dataSets"][0]["series"]
            for k, v in list(ser.items())[:6]:
                obs = list((v.get("observations") or {}).values())
                rep.log(f"  series {k} -> {obs[:1]}")
            kp = None
            for dim in dims:
                for v in dim.get("values") or []:
                    nm = str(v.get("name", "")).lower()
                    if "key policy" in nm or v.get("id") == "KPRA":
                        kp = (dim.get("id"), v.get("id"), v.get("name"))
            rep.log(f"  policy-rate token: {kp}")
            if kp:
                confirmed.append(("NOINTR", f"norges:IR:{kp[1]}",
                                  f"policy rate — tighter ({kp[2]})"))
        except Exception as e:
            rep.log(f"  parse: {type(e).__name__}: {str(e)[:120]}")
        confirmed.append(("NO10Y", "norges:GOVT_GENERIC_RATES:10",
                          "long NOK rate (4001-confirmed)"))

        rep.section("D. BCRP — full names for the reserve candidates")
        yr = now.year
        keep_pe = [("PEINTR", "PD04722MM", True),
                   ("PENUSD", "PN01207PM", True),
                   ("PEFER", "PN00026MM", False),
                   ("PEFER", "PN00027MM", False)]
        for sym, code, pre in keep_pe:
            st, body = get(f"https://estadisticas.bcrp.gob.pe/estadisticas/"
                           f"series/api/{code}/json/{yr-1}-1/{yr}-12")
            try:
                d = json.loads(body)
                nm = str((d.get("config") or {}).get("series", [{}])[0].get("name"))
                pv = (d.get("periods") or [])[-1]
                rep.log(f"  {sym} {code}")
                rep.log(f"      NAME: {nm[:150]}")
                rep.log(f"      last {pv.get('name')} = {pv.get('values')}")
                ok = pre or "reservas internacionales" in nm.lower()
                if ok and not any(c[0] == sym for c in confirmed):
                    confirmed.append((sym, f"bcrp:{code}", nm[:60]))
            except Exception as e:
                rep.log(f"  {sym} {code} [{st}] {type(e).__name__}")

        rep.section("E. FINAL — paste-ready")
        for sym, alias, why in confirmed:
            rep.log(f'  "{sym}": "{alias}",  # {why[:78]}')
        rep.kv(n_confirmed=len(confirmed))
        if len(confirmed) < 8:
            rep.fail(f"only {len(confirmed)} — investigate above before wiring")
            sys.exit(1)
        rep.ok(f"PREFLIGHT DONE — {len(confirmed)} confirmed for wiring")


if __name__ == "__main__":
    main()
