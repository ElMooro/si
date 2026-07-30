"""ops_4111 — discovery round 2: IMF dataflow enumeration, IRFCL bulk
truth, BIS CBPOL for INTR, WB family breadth."""
import json
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0"}


def fetch(url, timeout=45):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "ignore")[:400]
    except Exception as e:
        return -1, str(e)[:200]


def series_map(xml, dim="REF_AREA|COUNTRY"):
    out = {}
    for blk in re.split(r"<Series[ >]", xml)[1:]:
        area = re.search(r'(?:%s)="([A-Z0-9]{2,3})"' % dim, blk)
        val = re.findall(r'OBS_VALUE="([\d\.eE\+\-]+)"', blk)
        if area and val:
            out[area.group(1)] = float(val[-1])
    return out


def main():
    with report("4111_discovery2") as rep:
        rep.heading("ops 4111 — discovery round 2")

        rep.section("A. IMF dataflow enumeration (what exists now?)")
        st, x = fetch("https://api.imf.org/external/sdmx/2.1/dataflow")
        rep.kv(status=st, bytes=len(x))
        if st == 200:
            ids = re.findall(r'Dataflow[^>]*\bid="([A-Za-z0-9_\-]+)"', x)
            rep.kv(n_flows=len(ids))
            mon = [i for i in ids if re.search(
                r"MFS|IFS|FSI|MON|CPI|IR|BOP|GFS|RES", i, re.I)]
            rep.log("  monetary-ish flows: " + ", ".join(sorted(set(mon))[:40]))
            rep.log("  first 40 all: " + ", ".join(ids[:40]))
        else:
            rep.log("  ERR: " + x[:250])

        rep.section("B. IRFCL bulk — explicit status this time")
        st2, x2 = fetch("https://api.imf.org/external/sdmx/2.1/data/IRFCL/"
                        "M..RAF_USD?lastNObservations=1")
        rep.kv(irfcl_status=st2, irfcl_bytes=len(x2))
        if st2 != 200:
            rep.log("  ERR: " + x2[:250])
        else:
            m = series_map(x2)
            rep.kv(irfcl_areas=len(m))
            rep.log(f"  spots BR={m.get('BR')} PE={m.get('PE')} "
                    f"JP={m.get('JP')} US={m.get('US')}")

        rep.section("C. BIS CBPOL — policy rates, all countries, daily")
        for url in (
            "https://stats.bis.org/api/v2/data/dataflow/BIS/WS_CBPOL/1.0/"
            "D..?lastNObservations=1&format=xml",
            "https://stats.bis.org/api/v1/data/WS_CBPOL/D../all"
            "?lastNObservations=1",
        ):
            st3, x3 = fetch(url)
            rep.log(f"  {url[:70]}... -> {st3} ({len(x3)}B)")
            if st3 == 200 and "OBS_VALUE" in x3:
                m3 = series_map(x3)
                rep.kv(bis_areas=len(m3))
                rep.log(f"  spots BR={m3.get('BR')} PE={m3.get('PE')} "
                        f"NO={m3.get('NO')} JP={m3.get('JP')} "
                        f"US={m3.get('US')}")
                break
            rep.log("    body: " + x3[:180])

        rep.section("D. WB breadth across doctrine families")
        WB = {"FER": "FI.RES.TOTL.CD", "GDG": "GC.DOD.TOTL.GD.ZS",
              "BM": "FM.LBL.BMNY.CN", "LEND": "FR.INR.LEND",
              "GDPYY": "NY.GDP.MKTP.KD.ZG", "IRYY": "FP.CPI.TOTL.ZG",
              "UR": "SL.UEM.TOTL.ZS"}
        for fam, code in WB.items():
            st4, x4 = fetch("https://api.worldbank.org/v2/country/all/"
                            f"indicator/{code}?format=json&mrnev=1"
                            "&per_page=400")
            n = 0
            yr = ""
            try:
                j = json.loads(x4)
                rows = [r for r in (j[1] or [])
                        if r.get("value") is not None]
                n = len(rows)
                if rows:
                    yr = rows[0].get("date")
            except Exception:
                pass
            rep.log(f"  {fam:6s} {code:22s} -> {n:3d} countries, "
                    f"latest~{yr}")

        rep.ok("DISCOVERY2 DONE — flows enumerated, BIS + WB measured")


if __name__ == "__main__":
    main()
