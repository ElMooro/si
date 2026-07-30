"""ops_4138 — MFS DISCOVERY: key grammar for MFS_DC (Loan Growth) and
MFS_CBS (CB balance sheet) via structure + codelist grep + live JP/BR spots."""
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0"}
BASE = "https://api.imf.org/external/sdmx/2.1"


def fetch(url, timeout=45):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "ignore")[:500]
    except Exception as e:
        return -1, str(e)[:200]


def main():
    with report("4138_mfs_discovery") as rep:
        rep.heading("ops 4138 — MFS_DC / MFS_CBS discovery")

        for flow in ("MFS_DC", "MFS_CBS"):
            rep.section(f"{flow}: dataflow -> DSD -> dimensions")
            st, x = fetch(f"{BASE}/dataflow/IMF/{flow}"
                          "?references=datastructure")
            rep.kv(**{f"{flow}_struct_status": st,
                      f"{flow}_bytes": len(x)})
            if st != 200:
                rep.log("  ERR: " + x[:250])
                continue
            dims = re.findall(
                r'<str:Dimension[^>]*\bid="([A-Za-z0-9_]+)"[^>]*'
                r'position="(\d+)"', x)
            if not dims:
                dims = re.findall(
                    r'Dimension[^>]*\bid="([A-Za-z0-9_]+)"', x)
                rep.log("  dims(no-pos): " + ", ".join(dims[:10]))
            else:
                dims.sort(key=lambda d: int(d[1]))
                rep.log("  KEY ORDER: " + ".".join(d[0] for d in dims))
            cls = re.findall(r'codelist[^"]*id="([A-Za-z0-9_\-]+)"', x)
            rep.log("  codelists: " + ", ".join(sorted(set(cls))[:8]))

        rep.section("codelist grep — the LG / CBBS concepts by name")
        for cl, pats in (
            ("CL_INDICATOR_MFS_DC",
             ("Private Sector", "Loans", "Claims on Other Sectors")),
            ("CL_INDICATOR_MFS_CBS",
             ("Total Assets", "Monetary Base", "Claims")),
        ):
            st, x = fetch(f"{BASE}/codelist/IMF/{cl}")
            rep.log(f"  {cl}: status={st} bytes={len(x)}")
            if st != 200:
                rep.log("    ERR: " + x[:200])
                continue
            codes = re.findall(
                r'<str:Code[^>]*id="([A-Za-z0-9_\-]+)"[\s\S]{0,300}?'
                r'<com:Name[^>]*>([^<]{4,90})</com:Name>', x)
            hits = [(cid, nm) for cid, nm in codes
                    if any(p.lower() in nm.lower() for p in pats)]
            for cid, nm in hits[:12]:
                rep.log(f"    {cid}: {nm[:70]}")

        rep.section("live spots — key-shape trial for JP/BR")
        trials = [
            ("MFS_DC", "M.JPN"), ("MFS_DC", "M.JP"),
            ("MFS_CBS", "M.JPN"), ("MFS_CBS", "M.BRA"),
        ]
        for flow, key in trials:
            st, x = fetch(f"{BASE}/data/{flow}/{key}"
                          "?lastNObservations=1", timeout=50)
            n_series = x.count("<Series")
            rep.log(f"  {flow}/{key}: status={st} series={n_series} "
                    f"bytes={len(x)}")
            if st != 200:
                rep.log("    ERR: " + x[:220])
            elif n_series:
                i = x.find("<Series")
                rep.log("    SHAPE: " + x[i:i + 300].replace("\n", " "))
        rep.ok("MFS DISCOVERY DONE")


if __name__ == "__main__":
    main()
