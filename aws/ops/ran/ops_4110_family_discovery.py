"""ops_4110 — FAMILY-ADAPTER DISCOVERY: bulk IMF probes for FER/INTR,
candidate probes for LG/CBBS/M0, WB breadth check. Runner-exec ground
truth; cross-checked against fleet-LIVE values (BR/PE/NO/JP)."""
import json
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0"}
TRUTH = {"FER": {"BR": 368899, "PE": 325836},
         "INTR": {"BR": 14.25, "PE": 4.25, "NO": 4.29}}


def fetch(url, timeout=40):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "ignore")[:400]
    except Exception as e:
        return -1, str(e)[:200]


def series_map(xml):
    out = {}
    for blk in re.split(r"<Series[ >]", xml)[1:]:
        area = re.search(r'(?:REF_AREA|COUNTRY)="([A-Z0-9]{2,3})"', blk)
        val = re.findall(r'OBS_VALUE="([\d\.eE\+\-]+)"', blk)
        tp = re.findall(r'TIME_PERIOD="([^"]+)"', blk)
        if area and val:
            out[area.group(1)] = (float(val[-1]), tp[-1] if tp else "?")
    return out


def main():
    with report("4110_family_discovery") as rep:
        rep.heading("ops 4110 — family discovery")

        rep.section("A. FER — bulk IRFCL M..RAF_USD (proven code, "
                    "bulk shape unproven)")
        st, x = fetch("https://api.imf.org/external/sdmx/2.1/data/IRFCL/"
                      "M..RAF_USD?lastNObservations=1")
        rep.kv(status=st, bytes=len(x))
        fer = series_map(x) if st == 200 else {}
        rep.kv(fer_areas=len(fer))
        if fer:
            i = x.find("<Series")
            rep.log("  SHAPE: " + x[i:i + 260].replace("\n", " "))
        for cc, want in TRUTH["FER"].items():
            got = fer.get(cc)
            rep.log(f"  spot {cc}: imf={got} fleet={want}")

        rep.section("B. INTR — bulk IFS M..FPOLM_PA")
        st2, x2 = fetch("https://api.imf.org/external/sdmx/2.1/data/IFS/"
                        "M..FPOLM_PA?lastNObservations=1")
        rep.kv(status=st2, bytes=len(x2))
        intr = series_map(x2) if st2 == 200 else {}
        rep.kv(intr_areas=len(intr))
        if st2 != 200:
            rep.log("  ERR BODY: " + x2[:300])
        for cc, want in TRUTH["INTR"].items():
            rep.log(f"  spot {cc}: imf={intr.get(cc)} fleet={want}")

        rep.section("C. LG / CBBS / M0 candidates (JP + BR, per-country)")
        for fam, codes in (("LG", ["FDSAOP_XDC", "FOSAOP_XDC"]),
                           ("CBBS+M0", ["FASMB_XDC", "FACB_XDC"])):
            for code in codes:
                for cc in ("JP", "BR"):
                    st3, x3 = fetch(
                        f"https://api.imf.org/external/sdmx/2.1/data/IFS/"
                        f"M.{cc}.{code}?lastNObservations=2")
                    got = series_map(x3) if st3 == 200 else {}
                    rep.log(f"  {fam} {code} {cc}: status={st3} "
                            f"val={got.get(cc)}"
                            + ("" if st3 == 200 else
                               f" err={x3[:140]}"))

        rep.section("D. WB breadth — bulk reserves, all countries")
        st4, x4 = fetch("https://api.worldbank.org/v2/country/all/indicator/"
                        "FI.RES.TOTL.CD?format=json&mrnev=1&per_page=400")
        n4 = 0
        try:
            j = json.loads(x4)
            n4 = sum(1 for r in (j[1] or [])
                     if r.get("value") is not None)
        except Exception:
            pass
        rep.kv(wb_status=st4, wb_countries_with_value=n4)

        rep.ok(f"DISCOVERY — FER bulk {len(fer)} areas, INTR bulk "
               f"{len(intr)} areas, WB {n4} countries")


if __name__ == "__main__":
    main()
