"""ops/4806 -- daily EU sovereign 10Y: free-source probe battery.
TE = 403 (plan wall). Candidates, evidence verbatim per probe:
 A. Bundesbank SDMX csv -- daily Bund 10Y (two candidate keys)
 B. ECB YC dataset -- euro-area AAA 10Y spot (context row)
 C. Banca d'Italia SDMX candidates
 D. MTS markets page -- hunt embedded JSON/XHR (per-country yields
    + spreads shown live on their public screen)
 E. Borsa Italiana BTP-Bund page -- same hunt
 F. stooq daily CSVs 10YDE/10YIT/10YFR/10YES (unofficial mirror --
    provenance-labeled fallback only)
No banking here; findings drive the v2.9 wiring."""
import gzip
import re
import sys
import urllib.request
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0 (research; raafouis@gmail.com)"}


def fetch(url, timeout=45, hdrs=None):
    h = dict(UA)
    if hdrs:
        h.update(hdrs)
    req = urllib.request.Request(url, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw


def probe(rep, tag, url, hdrs=None):
    try:
        raw = fetch(url, hdrs=hdrs)
        head = raw[:220].decode("utf-8", "replace").replace("\n", "|")
        rep.ok(f"[{tag}] {len(raw)}B :: {head[:200]}")
        return raw
    except Exception as e:
        rep.log(f"[{tag}] {type(e).__name__}: {str(e)[:90]}")
        return None


def main():
    with report("4806_eu_daily_probe") as rep:
        rep.heading("ops 4806 -- daily EU 10Y free-source probes")

        rep.section("A. Bundesbank (official, daily Bund)")
        probe(rep, "BBK-BBSIS-R10",
               "https://api.statistiken.bundesbank.de/rest/data/BBSIS/"
               "D.I.ZST.ZI.EUR.S1311.B.A604.R10XX.R.A.A._Z._Z.A"
               "?format=csv&lastNObservations=8")
        probe(rep, "BBK-BBSIS-alt",
               "https://api.statistiken.bundesbank.de/rest/data/BBSIS/"
               "D.I.ZST.ZI.EUR.S1311.B.A604.R10XX.R.A.A._Z._Z.A"
               "?format=sdmx-json&lastNObservations=3",
               {"Accept": "application/vnd.sdmx.data+json"})

        rep.section("B. ECB YC (euro-area AAA 10Y)")
        probe(rep, "ECB-YC-10Y",
               "https://data-api.ecb.europa.eu/service/data/YC/"
               "B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y"
               "?format=csvdata&lastNObservations=8")

        rep.section("C. Banca d'Italia SDMX candidates")
        probe(rep, "BDI-a2a",
               "https://a2a.bancaditalia.it/stat/rest/dataflow/all")
        probe(rep, "BDI-infostat",
               "https://infostat.bancaditalia.it/inquiry/api/open/"
               "dataflow")

        rep.section("D. MTS live screen -- embedded data hunt")
        raw = probe(rep, "MTS-page", "https://www.mtsmarkets.com/")
        if raw:
            txt = raw[:400000].decode("utf-8", "replace")
            for x in sorted(set(re.findall(
                    r"[\"'](https?://[^\"']*(?:json|api|data|yield)"
                    r"[^\"']*)[\"']", txt)))[:10]:
                rep.log("  mts url: " + x[:150])
            for a in re.findall(r"(\[\{[^\]]{40,300}?\}\])", txt)[:2]:
                rep.log("  mts inline: " + a[:180])

        rep.section("E. Borsa Italiana spread page")
        raw = probe(rep, "BIT-page",
                     "https://www.borsaitaliana.it/obbligazioni/"
                     "spread/overview.en.htm")
        if raw:
            txt = raw[:400000].decode("utf-8", "replace")
            for x in sorted(set(re.findall(
                    r"[\"'](/[^\"']*(?:json|api|Spread|spread)"
                    r"[^\"']*)[\"']", txt)))[:8]:
                rep.log("  bit path: " + x[:150])
            m = re.search(r"(?i)(btp[^<]{0,40}bund[^<]{0,80})", txt)
            if m:
                rep.log("  bit text: " + m.group(1)[:120])

        rep.section("F. stooq daily CSVs (unofficial mirror)")
        for cc, sym in (("DE", "10yde_b"), ("IT", "10yit_b"),
                          ("FR", "10yfr_b"), ("ES", "10yes_b")):
            probe(rep, f"stooq-{cc}",
                   f"https://stooq.com/q/d/l/?s={sym}&i=d")
        for cc, sym in (("DE2", "10ydey_b"), ("IT2", "10yity_b")):
            probe(rep, f"stooq-{cc}",
                   f"https://stooq.com/q/d/l/?s={sym}&i=d")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
