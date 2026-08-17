"""ops/4843 -- IMF BOP structure probe + BIS v2 catalog
(report-only, ZERO writes).  Claimed 4843-4846 S-A#k7q2.

4837 (S-B) found dataflow IMF.STA/BOP v21.0.0 alive; a wire needs
three more facts, resolved here and printed verbatim:
 (1) the BOP datastructure: dimension ids IN ORDER (the SDMX key),
     via 2.1 and 3.0 candidates;
 (2) the indicator codelist entries for what Khalid's hot-money
     doctrine needs: portfolio liabilities (total/equity/debt) and
     other-investment liabilities (banking/short-term legs) --
     grepped from the codelists, never recalled from memory;
 (3) ONE working sample data URL: Korea quarterly portfolio
     liabilities, several URL shapes tried, winner printed with its
     first bytes so the wire op copies it exactly.
 (4) BIS: /api/v2 dataflow catalog; LBS-ish flows listed + one
     structure peek (banking layer, wired later).
Hard-fail only if the BOP datastructure resolves on neither host.
"""
import json
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

UA = {"User-Agent": "justhodl-ops-4843",
      "Accept": "application/json;q=0.9,*/*;q=0.5"}
FAILED = []


def get(url, timeout=60):
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.status, r.read()


def main():
    with report("ops 4843 -- IMF BOP structure + BIS catalog "
                "probe") as rep:
        rep.heading("1. BOP datastructure -- dimension order")
        dsd = None
        for url in (
            "https://api.imf.org/external/sdmx/2.1/datastructure/"
            "IMF.STA/DSD_BOP?references=children",
            "https://api.imf.org/external/sdmx/2.1/datastructure/"
            "IMF.STA/BOP?references=children",
            "https://api.imf.org/external/sdmx/3.0/structure/"
            "datastructure/IMF.STA/DSD_BOP?references=children",
        ):
            try:
                st, raw = get(url, 90)
                if st == 200 and len(raw) > 2000:
                    try:
                        dsd = json.loads(raw)
                        rep.ok("DSD via %s (%d bytes)"
                               % (url.split("external/")[1][:40],
                                  len(raw)))
                        break
                    except ValueError:
                        rep.warn("  %s non-json head=%s"
                                 % (url[-50:], raw[:60]))
                else:
                    rep.warn("  %s -> HTTP %d bytes=%d"
                             % (url[-50:], st, len(raw)))
            except Exception as e:  # noqa: BLE001
                rep.warn("  %s dead: %s" % (url[-50:],
                                            str(e)[:60]))
            time.sleep(0.4)
        dims = []
        codelists = {}
        if dsd:
            d = dsd.get("data") or dsd
            try:
                dsds = (d.get("dataStructures")
                        or d.get("dataStructureDefinitions")
                        or [])
                comp = (dsds[0].get("dataStructureComponents")
                        or {})
                for dim in ((comp.get("dimensionList") or {})
                            .get("dimensions") or []):
                    dims.append((dim.get("id"),
                                 ((dim.get("localRepresentation")
                                   or {}).get("enumeration")
                                  or "")))
                rep.ok("dimension order: %s"
                       % [x[0] for x in dims])
                for cl in (d.get("codelists") or []):
                    codelists[cl.get("id")] = cl.get("codes") or []
                rep.ok("codelists shipped: %d (%s...)"
                       % (len(codelists),
                          sorted(codelists)[:5]))
            except Exception as e:  # noqa: BLE001
                rep.warn("DSD parse issue: %s" % str(e)[:80])
                rep.log("  raw head: %s"
                        % json.dumps(d, default=str)[:400])
        if not dims:
            rep.fail("no dimension order resolved on any host")
            FAILED.append("dsd")
            sys.exit(1)

        rep.heading("2. indicator codes -- portfolio + other-"
                    "investment LIABILITIES")
        ind_cl = None
        for cid, codes in codelists.items():
            if "INDICATOR" in (cid or "").upper() and codes:
                ind_cl = (cid, codes)
                break
        picks = {}
        if ind_cl:
            cid, codes = ind_cl
            rep.ok("indicator codelist %s: %d codes"
                   % (cid, len(codes)))
            wants = {
                "pf_liab_total": ("portfolio", "liabilit"),
                "pf_liab_equity": ("portfolio", "liabilit",
                                   "equity"),
                "pf_liab_debt": ("portfolio", "liabilit", "debt"),
                "oth_liab": ("other investment", "liabilit"),
            }
            for tag, terms in wants.items():
                cands = []
                for c in codes:
                    nm = json.dumps(c.get("name") or c.get(
                        "names") or "", ensure_ascii=False).lower()
                    if all(t in nm for t in terms):
                        cands.append(c)
                if cands:
                    picks[tag] = cands[0].get("id")
                    rep.ok("  %-15s %d cand; PICK %s"
                           % (tag, len(cands), cands[0].get("id")))
                    for c in cands[:4]:
                        rep.log("    %s | %s"
                                % (c.get("id"),
                                   json.dumps(c.get("name")
                                              or c.get("names"),
                                              ensure_ascii=False)
                                   [:90]))
                else:
                    rep.warn("  %-15s no candidates" % tag)
        else:
            rep.warn("no INDICATOR codelist in DSD payload; ids "
                     "seen: %s" % sorted(codelists)[:12])

        rep.heading("3. sample data call -- Korea quarterly "
                    "portfolio liabilities")
        key_dims = [x[0] for x in dims]
        rep.log("  building keys over dims: %s" % key_dims)
        ind = picks.get("pf_liab_total") or "BFPL_BP6_USD"
        shapes = []
        if len(key_dims) >= 3:
            base21 = ("https://api.imf.org/external/sdmx/2.1/data/"
                      "IMF.STA,BOP/")
            for key in ("Q.KR.%s" % ind, "KR.%s.Q" % ind,
                        "%s.KR.Q" % ind):
                shapes.append(base21 + key
                              + "?lastNObservations=6"
                              "&format=sdmx-json")
            shapes.append("https://api.imf.org/external/sdmx/3.0/"
                          "data/dataflow/IMF.STA/BOP/~/Q.KR.%s"
                          "?lastNObservations=6" % ind)
        winner = None
        for u in shapes:
            try:
                st, raw = get(u, 75)
                head = raw[:160].decode("utf-8", "replace")
                ok = st == 200 and (b"observations" in raw
                                    or b"\"series\"" in raw
                                    or b"dataSets" in raw)
                (rep.ok if ok else rep.warn)(
                    "  %s HTTP %d bytes=%d head=%s"
                    % (u.split("/data/")[1][:52], st, len(raw),
                       head.replace("\n", " ")[:110]))
                if ok and not winner:
                    winner = u
            except Exception as e:  # noqa: BLE001
                rep.warn("  %s dead: %s"
                         % (u.split("/data/")[1][:52],
                            str(e)[:70]))
            time.sleep(0.5)
        if winner:
            rep.ok("WINNER data URL shape: %s" % winner)
        else:
            rep.warn("no data shape answered -- wire blocked "
                     "until a follow-up cracks the key (dims + "
                     "codes above are the map)")

        rep.heading("4. BIS /api/v2 catalog + LBS peek")
        try:
            st, raw = get("https://stats.bis.org/api/v2/structure/"
                          "dataflow", 60)
            rep.ok("BIS dataflow HTTP %d bytes=%d" % (st, len(raw)))
            try:
                j = json.loads(raw)
                flows = ((j.get("data") or {}).get("dataflows")
                         or j.get("dataflows") or [])
                lbs = [f for f in flows
                       if "LBS" in json.dumps(f)[:400].upper()]
                rep.ok("  flows=%d, LBS-ish=%d" % (len(flows),
                                                   len(lbs)))
                for f in lbs[:4]:
                    rep.log("   %s | %s"
                            % (f.get("id"),
                               json.dumps(f.get("name")
                                          or f.get("names"),
                                          ensure_ascii=False)[:80]))
            except ValueError:
                rep.log("  head: %s" % raw[:200])
        except Exception as e:  # noqa: BLE001
            rep.warn("BIS v2 dead: %s" % str(e)[:80])

        rep.heading("5. verdict")
        rep.ok("structure resolved; wire op binds ONLY what is "
               "printed above")


if __name__ == "__main__":
    main()
