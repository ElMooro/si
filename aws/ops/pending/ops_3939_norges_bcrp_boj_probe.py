"""
ops_3939 — PROBE (no code): (1) dump the REAL Norges GOVT_GENERIC_RATES
JSON skeleton — twice blind-patched, now inspect: top keys, structure(s)
location, series-dim values, first series key+obs; (2) BCRP: fetch candidate
Peru ToT INDEX codes adjacent to the discovered components (PN38910BM..14BM)
and print each API config.title so the headline index is identified by
TITLE not guess; (3) BOJ: fetch the download index DECODED AS SHIFT_JIS and
list loan/lending anchors for the JPLG flat file.
"""
import json, re, sys, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/126.0"}


def main():
    with report("3939_norges_bcrp_boj_probe") as rep:
        rep.heading("ops 3939 — norges skeleton + BCRP titles + BOJ shift_jis")

        rep.section("1. Norges GOVT_GENERIC_RATES raw skeleton")
        try:
            url = ("https://data.norges-bank.no/api/data/GOVT_GENERIC_RATES"
                   "?format=sdmx-json&lastNObservations=1")
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA),
                                        timeout=25) as r:
                d = json.loads(r.read())
            rep.log(f"  top keys: {sorted(d.keys())}")
            data = d.get("data") or d
            rep.log(f"  data keys: {sorted(data.keys()) if isinstance(data, dict) else type(data)}")
            ds = (data.get("dataSets") or [{}])[0]
            rep.log(f"  dataSets[0] keys: {sorted(ds.keys())}")
            series = ds.get("series") or {}
            k0 = list(series.keys())[0] if series else None
            rep.log(f"  n_series={len(series)} first_key={k0} "
                    f"first_val={json.dumps(series.get(k0))[:150]}")
            for loc in ("structure", "structures"):
                st = data.get(loc) or d.get(loc)
                if st is not None:
                    st0 = st[0] if isinstance(st, list) else st
                    dims = ((st0.get("dimensions") or {}).get("series")) or []
                    rep.log(f"  {loc}: present; series dims:")
                    for di, dim in enumerate(dims):
                        vals = [(v.get("id"), v.get("name")) for v in (dim.get("values") or [])][:10]
                        rep.log(f"    dim[{di}] id={dim.get('id')} values={vals}")
        except Exception as e:
            rep.log(f"  norges: {str(e)[:150]}")

        rep.section("2. BCRP candidate ToT index codes — identify by TITLE")
        for code in ("PN38910BM", "PN38911BM", "PN38912BM", "PN38913BM", "PN38914BM"):
            try:
                url = f"https://estadisticas.bcrp.gob.pe/estadisticas/series/api/{code}/json/2026-1/2026-5"
                with urllib.request.urlopen(urllib.request.Request(url, headers=UA),
                                            timeout=20) as r:
                    d = json.loads(r.read())
                title = (d.get("config") or {}).get("title", "")
                periods = d.get("periods") or []
                last = periods[-1] if periods else {}
                rep.log(f"  {code}: {title[:95]} | last={json.dumps(last)[:70]}")
            except Exception as e:
                rep.log(f"  {code}: {str(e)[:90]}")

        rep.section("3. BOJ index decoded as Shift_JIS — loan links")
        try:
            url = "https://www.stat-search.boj.or.jp/info/dload_en.html"
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA),
                                        timeout=25) as r:
                html = r.read().decode("shift_jis", "ignore")
            links = re.findall(r'href="([^"]+)"[^>]*>([^<]{3,90})</a>', html)
            loanish = [(h, t) for h, t in links
                       if re.search(r"loan|lend|deposit|money stock|DL", t, re.I)][:15]
            for h, t in loanish:
                rep.log(f"  {t.strip()[:70]} -> {h[:110]}")
            if not loanish:
                rep.log(f"  {len(links)} anchors total; first 8:")
                for h, t in links[:8]:
                    rep.log(f"    {t.strip()[:60]} -> {h[:100]}")
        except Exception as e:
            rep.log(f"  boj: {str(e)[:150]}")

        rep.ok("PROBE COMPLETE")
        if False: sys.exit(1)


if __name__ == "__main__":
    main()
