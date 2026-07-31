"""ops_4152 — CFTC Socrata discovery: catalog, column censuses, filtered
latest-week spots for 6E (TFF) and CL (Disagg)."""
import json
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "Mozilla/5.0"}
DOM = "https://publicreporting.cftc.gov"


def fetch(url, timeout=45):
    try:
        req = urllib.request.Request(url, headers=UA)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, r.read().decode("utf-8", "ignore")
    except Exception as e:
        return -1, str(e)[:200]


def main():
    with report("4152_cftc_socrata") as rep:
        rep.heading("ops 4152 — CFTC Socrata discovery")

        rep.section("A. catalog — dataset ids by name")
        st, x = fetch("https://api.us.socrata.com/api/catalog/v1"
                      "?domains=publicreporting.cftc.gov&limit=60")
        ids = []
        try:
            for r in json.loads(x).get("results", []):
                res = r.get("resource") or {}
                nm = str(res.get("name"))
                if any(k in nm for k in ("Financial", "Disaggregated",
                                         "Legacy")):
                    ids.append((res.get("id"), nm[:70]))
        except Exception:
            rep.log("  catalog parse miss: " + x[:200])
        for i, nm in ids[:12]:
            rep.log(f"  {i}  {nm}")

        rep.section("B. column census per candidate (1 row)")
        cands = [i for i, nm in ids] or ["gpe5-46if", "yw9f-hn96",
                                         "72hh-3qpy", "kh3c-gbw2"]
        good = {}
        for i in cands[:8]:
            st2, x2 = fetch(f"{DOM}/resource/{i}.json?$limit=1")
            try:
                row = json.loads(x2)[0]
                cols = list(row)
                good[i] = cols
                grp = [c for c in cols if any(
                    g in c for g in ("dealer", "asset_mgr", "lev_money",
                                     "m_money", "prod_merc", "swap",
                                     "other_rept", "nonrept",
                                     "comm_positions",
                                     "noncomm_positions"))]
                rep.log(f"  {i}: {len(cols)} cols; groups={len(grp)}; "
                        f"sample={json.dumps(grp[:6])[:200]}")
            except Exception:
                rep.log(f"  {i}: {st2} {x2[:120]}")

        rep.section("C. filtered spots — 099741 & 067651 latest")
        for i in list(good)[:6]:
            for code in ("099741", "067651"):
                st3, x3 = fetch(
                    f"{DOM}/resource/{i}.json"
                    f"?cftc_contract_market_code={code}"
                    "&$order=report_date_as_yyyy_mm_dd%20DESC&$limit=1")
                try:
                    row = json.loads(x3)[0]
                    keys = [k for k in row if "long_all" in k][:3]
                    rep.log(f"  {i}/{code}: "
                            f"{row.get('report_date_as_yyyy_mm_dd', '?')[:10]} "
                            + " ".join(f"{k}={row.get(k)}"
                                       for k in keys)[:150])
                except Exception:
                    rep.log(f"  {i}/{code}: {st3} {x3[:100]}")
        rep.ok(f"SOCRATA DISCOVERY — {len(good)} datasets read")


if __name__ == "__main__":
    main()
