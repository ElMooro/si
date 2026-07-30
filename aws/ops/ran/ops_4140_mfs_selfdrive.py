"""ops_4140 — SELF-DRIVING MFS resolver: structure(references=all) ->
dimension order + indicator codes + area form -> build keys -> live spots."""
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0"}
BASE = "https://api.imf.org/external/sdmx/2.1"


def fetch(url, timeout=60):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", "ignore")[:400]
    except Exception as e:
        return -1, str(e)[:200]


def main():
    with report("4140_mfs_selfdrive") as rep:
        rep.heading("ops 4140 — self-driving MFS resolver")
        FLOWS = {"MFS_DC": ("Loans", "Private Sector",
                            "Claims on Other Sectors"),
                 "MFS_CBS": ("Total Assets", "Monetary Base",
                             "Central Bank")}
        for flow, pats in FLOWS.items():
            rep.section(flow)
            st, x = fetch(f"{BASE}/dataflow/IMF.STA/{flow}"
                          "?references=all", timeout=90)
            rep.kv(**{f"{flow}_status": st, f"{flow}_MB":
                      round(len(x) / 1e6, 2)})
            if st != 200:
                rep.log("  ERR: " + x[:250])
                continue
            dims = re.findall(
                r'<[A-Za-z]*:?Dimension\b[^>]*?id="([A-Za-z0-9_]+)"'
                r'[^>]*?position="(\d+)"', x)
            dims = sorted(set(dims), key=lambda d: int(d[1]))
            order = [d[0] for d in dims]
            rep.log("  KEY ORDER: " + ".".join(order))
            jpn = 'id="JPN"' in x
            jp2 = re.search(r'<[A-Za-z]*:?Code\b[^>]*id="JP"', x)
            rep.log(f"  area form: JPN={jpn} JP={bool(jp2)}")
            codes = re.findall(
                r'<[A-Za-z]*:?Code\b[^>]*id="([A-Za-z0-9_\-]+)"'
                r'[\s\S]{0,400}?<[A-Za-z]*:?Name[^>]*>([^<]{4,100})<', x)
            hits = [(c, n) for c, n in codes
                    if any(p.lower() in n.lower() for p in pats)]
            rep.log(f"  candidate indicators ({len(hits)}):")
            for c, n in hits[:10]:
                rep.log(f"    {c}: {n[:72]}")
            area = "JPN" if jpn else "JP"
            nd = len(order)
            for c, n in hits[:3]:
                for ar in (area, "BRA" if jpn else "BR"):
                    parts = []
                    for d in order:
                        du = d.upper()
                        if du == "FREQ":
                            parts.append("M")
                        elif "AREA" in du or du in ("COUNTRY",
                                                    "REF_AREA",
                                                    "JURISDICTION"):
                            parts.append(ar)
                        elif "INDICATOR" in du:
                            parts.append(c)
                        else:
                            parts.append("")
                    key = ".".join(parts)
                    st2, x2 = fetch(f"{BASE}/data/{flow}/{key}"
                                    "?lastNObservations=2", timeout=60)
                    ns = x2.count("<Series")
                    ov = re.findall(r'OBS_VALUE="([\d\.eE\+\-]+)"', x2)
                    rep.log(f"  SPOT {flow}/{key} -> {st2} "
                            f"series={ns} vals={ov[:3]}")
                    if ns:
                        i = x2.find("<Series")
                        rep.log("    SHAPE: "
                                + x2[i:i + 240].replace("\n", " "))
                        break
                if ns:
                    break
        rep.ok("SELF-DRIVE DONE")


if __name__ == "__main__":
    main()
