"""
ops_3970 — DISCOVERY (read-only): actually CALL candidate official endpoints
for the highest-value dead vault symbols. No code changes, no guessing.

ops 3969 routed 51 dead symbols to agencies. But routing is not resolution —
the registry's own quirks already record that GB30Y has no BoE par series
(20Y IUDLNPY is the long end), Chile needs Khalid's BCCh account, and the
SNB windowed parse is an open bug. And a large slice of the "missing" list
(USMPMI, USCPMI, USNMPR, EUMPMI, JPMPMI, USHMI) are S&P Global / ISM / NAHB
products — licensed, not government-published. No agency serves them, so
they stay NO_FREE_SOURCE and I will not substitute a different series under
their symbol and call it fixed.

This probe tests the ones that plausibly DO exist behind a proven adapter,
and reports per symbol: the exact URL, HTTP status, and the parsed value.
Only confirmed hits get wired in the op that follows.
"""
import json
import re
import sys
import urllib.request
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0",
      "Accept-Encoding": "identity"}


def get(url, timeout=30):
    try:
        r = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.read()
    except Exception as e:
        return getattr(e, "code", 0), str(e).encode()[:300]


def fred_key():
    try:
        import boto3 as b
        lam = b.client("lambda", region_name="us-east-1")
        env = lam.get_function_configuration(
            FunctionName="justhodl-signal-backtest")["Environment"]["Variables"]
        return env.get("FRED_KEY")
    except Exception:
        return None


def main():
    with report("3970_gov_discovery") as rep:
        rep.heading("ops 3970 — DISCOVERY: call the candidate official endpoints")
        hits, misses = [], []
        FK = fred_key()
        rep.kv(fred_key_present=bool(FK))

        # ── 1. ECB Data Portal: same YC adapter the vault already proves ──
        rep.section("A. ECB — German 10Y (EUBUND) via the YC curve")
        u = ("https://data-api.ecb.europa.eu/service/data/YC/"
             "B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y?format=jsondata&lastNObservations=1")
        st, body = get(u)
        val = None
        try:
            d = json.loads(body)
            ser = ((d.get("dataSets") or [{}])[0].get("series") or {})
            for _k, v in ser.items():
                obs = v.get("observations") or {}
                for _i, o in obs.items():
                    val = o[0]
        except Exception as e:
            rep.log(f"  parse: {type(e).__name__}")
        rep.log(f"  [{st}] EUBUND -> {val}  {u[:96]}")
        (hits if val is not None else misses).append(("EUBUND", "ecb:YC 10Y", val))

        # ── 2. FRED: Japan CPI YoY (JPIRYY) and China PPI (CNPPIYY) ──
        rep.section("B. FRED — JPIRYY / CNPPIYY / USHMI candidates")
        if FK:
            for sym, sid in (("JPIRYY", "JPNCPIALLMINMEI"),
                             ("CNPPIYY", "CHNPPIALLMINMEI"),
                             ("CNPPIYY_alt", "CHNCPIALLMINMEI"),
                             ("USHMI", "NAHBHMI"),
                             ("USHMI_alt", "HMI"),
                             ("USMPMI", "NAPM"),
                             ("USNMPR", "NMFCI")):
                u = (f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}"
                     f"&api_key={FK}&file_type=json&sort_order=desc&limit=2")
                st, body = get(u)
                v = None
                try:
                    obs = json.loads(body).get("observations") or []
                    v = [o for o in obs if o.get("value") not in (".", "", None)]
                    v = v[0] if v else None
                except Exception:
                    pass
                rep.log(f"  [{st}] {sym:12s} {sid:20s} -> {v}")
                (hits if v else misses).append((sym, f"fred:{sid}", v))
        else:
            rep.log("  no FRED key — skipped")

        # ── 3. Eurostat: DENO (German new orders), EUESI (sentiment) ──
        rep.section("C. Eurostat — DENO / EUESI")
        for sym, ds, q in (
            ("DENO", "sts_intv_m", "?geo=DE&s_adj=SCA&indic_bt=IS-INO&nace_r2=C&lastTimePeriod=1"),
            ("EUESI", "ei_bssi_m_r2", "?geo=EA20&s_adj=SA&indic=BS-ESI-I&lastTimePeriod=1"),
        ):
            u = ("https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
                 + ds + q + "&format=JSON")
            st, body = get(u)
            v = None
            try:
                d = json.loads(body)
                vals = d.get("value") or {}
                v = list(vals.values())[0] if vals else None
            except Exception:
                pass
            rep.log(f"  [{st}] {sym:8s} {ds:14s} -> {v}")
            (hits if v is not None else misses).append((sym, f"eurostat:{ds}", v))

        # ── 4. BOJ: short JGB / 3M rate for JP03MY ──
        rep.section("D. BOJ — hunt a 3M rate series for JP03MY")
        u = ("https://www.stat-search.boj.or.jp/api/v1/getMetadata?"
             "format=json&lang=en&db=IR01")
        st, body = get(u)
        cands = []
        try:
            txt = body.decode("utf8", "ignore")
            for m in re.finditer(r'"(?:code|seriesCode)"\s*:\s*"([^"]+)"[^}]*?'
                                 r'"(?:name|seriesName)"\s*:\s*"([^"]{0,90})"', txt):
                nm = m.group(2).lower()
                if "3" in nm and ("month" in nm or "mo" in nm):
                    cands.append((m.group(1), m.group(2)))
        except Exception as e:
            rep.log(f"  parse: {type(e).__name__}")
        rep.log(f"  [{st}] IR01 metadata bytes={len(body)} 3-month candidates={len(cands)}")
        for c in cands[:8]:
            rep.log(f"      {c[0]} — {c[1][:70]}")
        (hits if cands else misses).append(("JP03MY", "boj:IR01", len(cands)))

        # ── 5. SNB: the known-open windowed parse, with a RAW dump ──
        rep.section("E. SNB — CH02Y/CH03Y (registry records this parse as an open bug)")
        u = "https://data.snb.ch/api/cube/rendoblid/data/json/en"
        st, body = get(u)
        rep.log(f"  [{st}] bytes={len(body)}")
        try:
            d = json.loads(body)
            ts = d.get("timeseries") or []
            rep.log(f"  timeseries n={len(ts)}")
            for t in ts[:4]:
                vals = t.get("values") or []
                rep.log(f"      header={str(t.get('header'))[:80]} n_values={len(vals)} "
                        f"last={vals[-1] if vals else None}")
        except Exception as e:
            rep.log(f"  parse FAILED: {type(e).__name__}: {str(e)[:120]}")
            rep.log(f"  raw head: {body[:200]}")

        # ── 6. ONS: GBGDG (UK govt debt to GDP) ──
        rep.section("F. ONS — GBGDG")
        u = ("https://api.beta.ons.gov.uk/v1/datasets/gross-domestic-product-year-on-year"
             "/editions/time-series/versions/1/observations?time=*&geography=K02000001")
        st, body = get(u)
        rep.log(f"  [{st}] bytes={len(body)} head={body[:150]}")

        rep.section("G. verdict")
        rep.kv(confirmed=len([h for h in hits if h[2] is not None]),
               failed=len(misses))
        rep.log("  CONFIRMED (wire these):")
        for s_, src, v in hits:
            if v is not None:
                rep.log(f"    {s_:14s} {src:24s} {str(v)[:70]}")
        rep.log("  NOT RESOLVED (leave NO_FREE_SOURCE, documented):")
        for s_, src, v in misses:
            rep.log(f"    {s_:14s} {src}")
        rep.log("")
        rep.log("  NOT GOVERNMENT DATA AT ALL — will NOT be faked with a substitute:")
        rep.log("    USMPMI / USCPMI / USNMPR / EUMPMI / JPMPMI / TWMPMI = S&P Global "
                "and ISM licensed PMIs; USHMI = NAHB. No agency publishes them.")
        rep.ok("DISCOVERY COMPLETE — evidence recorded; wiring op follows for confirmed only")


if __name__ == "__main__":
    main()
